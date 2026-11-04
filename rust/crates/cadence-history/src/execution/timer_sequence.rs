//! Ordered timer generation from Go `execution/timer_sequence.go`.
use super::timer::TimerInfo;
use crate::mutable_state_activity::ActivityInfo;

pub const TIMER_TASK_STATUS_NONE: i32 = 0;
pub const TIMER_TASK_STATUS_CREATED: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum TimerType { StartToClose = 0, ScheduleToStart = 1, ScheduleToClose = 2, Heartbeat = 3 }
impl TimerType {
    pub fn mask(self) -> i32 { 1 << self as i32 }
    pub fn reason(self) -> String {
        let name = match self { Self::StartToClose => "START_TO_CLOSE", Self::ScheduleToStart => "SCHEDULE_TO_START", Self::ScheduleToClose => "SCHEDULE_TO_CLOSE", Self::Heartbeat => "HEARTBEAT" };
        format!("cadenceInternal:Timeout {name}")
    }
    pub fn from_internal(value: i32) -> Self {
        match value { 0 => Self::StartToClose, 1 => Self::ScheduleToStart, 2 => Self::ScheduleToClose, 3 => Self::Heartbeat, _ => panic!("invalid timeout type: {value}") }
    }
    pub fn to_internal(self) -> i32 { self as i32 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerSequenceId {
    pub event_id: i64,
    pub timestamp_unix_nanos: i64,
    pub timer_type: TimerType,
    pub timer_created: bool,
    pub attempt: i32,
}
impl TimerSequenceId {
    /// Go deliberately compares whole Unix seconds, including pre-epoch times.
    pub fn is_expired(self, reference_unix_nanos: i64) -> (i64, bool) {
        let fire = self.timestamp_unix_nanos.div_euclid(1_000_000_000);
        let reference = reference_unix_nanos.div_euclid(1_000_000_000);
        if fire <= reference { ((reference - fire) * 1_000_000_000, true) } else { (0, false) }
    }
}

/// Activity persistence fields not yet carried by the base activity port.
#[derive(Debug, Clone)]
pub struct ActivityTimerInfo {
    pub activity: ActivityInfo,
    pub timer_task_status: i32,
    pub last_heartbeat_timeout_visibility_in_seconds: i64,
}

/// State and task sink shared by active task generation and refresh. The sink
/// adds workflow identity/version/task-list from its execution record; shard
/// allocation supplies the durable task ID later, just as in Go.
pub trait TimerSequenceState {
    type Error;
    fn user_timers(&self) -> Vec<TimerInfo>;
    fn activities(&self) -> Vec<ActivityTimerInfo>;
    fn user_timer(&self, event_id: i64) -> Option<TimerInfo>;
    fn activity(&self, event_id: i64) -> Option<ActivityTimerInfo>;
    fn missing_activity_info(&self, event_id: i64) -> Self::Error;
    fn update_user_timer(&mut self, timer: TimerInfo) -> Result<(), Self::Error>;
    fn update_activity_timer(&mut self, activity: ActivityTimerInfo) -> Result<(), Self::Error>;
    fn add_user_timer_task(&mut self, timer: TimerSequenceId);
    fn add_activity_timeout_task(&mut self, timer: TimerSequenceId, task_list: String);
}

pub struct TimerSequence<'a, S> { pub state: &'a mut S }
impl<S: TimerSequenceState> TimerSequence<'_, S> {
    pub fn load_and_sort_user_timers(&self) -> Vec<TimerSequenceId> {
        let mut timers = self.state.user_timers().into_iter().map(|info| TimerSequenceId {
            event_id: info.started_id, timestamp_unix_nanos: info.expiry_time_unix_nanos,
            timer_type: TimerType::StartToClose, timer_created: info.task_status == TIMER_TASK_STATUS_CREATED, attempt: 0,
        }).collect::<Vec<_>>();
        sort(&mut timers);
        timers
    }
    pub fn load_and_sort_activity_timers(&self) -> Vec<TimerSequenceId> {
        let mut timers = Vec::new();
        for info in self.state.activities() {
            let a = &info.activity;
            if a.schedule_id == 0 { continue; }
            let mut add = |timer_type: TimerType, base: i64, seconds: i32| timers.push(TimerSequenceId {
                event_id: a.schedule_id, timestamp_unix_nanos: base + i64::from(seconds) * 1_000_000_000,
                timer_type, timer_created: info.timer_task_status & timer_type.mask() > 0, attempt: a.attempt,
            });
            add(TimerType::ScheduleToClose, a.scheduled_time, a.schedule_to_close_timeout);
            if a.started_id == 0 { add(TimerType::ScheduleToStart, a.scheduled_time, a.schedule_to_start_timeout); }
            else {
                add(TimerType::StartToClose, a.started_time, a.start_to_close_timeout);
                if a.heartbeat_timeout > 0 { add(TimerType::Heartbeat, a.started_time.max(a.last_heartbeat_updated_time), a.heartbeat_timeout); }
            }
        }
        sort(&mut timers);
        timers
    }
    pub fn create_next_user_timer(&mut self) -> Result<bool, S::Error> {
        let Some(first) = self.load_and_sort_user_timers().first().copied() else { return Ok(false); };
        if first.timer_created { return Ok(false); }
        let mut info = self.state.user_timer(first.event_id).ok_or_else(|| self.state.missing_activity_info(first.event_id))?;
        info.task_status = TIMER_TASK_STATUS_CREATED;
        self.state.update_user_timer(info)?;
        self.state.add_user_timer_task(first);
        Ok(true)
    }
    pub fn create_next_activity_timer(&mut self) -> Result<bool, S::Error> {
        let Some(first) = self.load_and_sort_activity_timers().first().copied() else { return Ok(false); };
        if first.timer_created { return Ok(false); }
        let mut info = self.state.activity(first.event_id).ok_or_else(|| self.state.missing_activity_info(first.event_id))?;
        info.timer_task_status |= first.timer_type.mask();
        if first.timer_type == TimerType::Heartbeat { info.last_heartbeat_timeout_visibility_in_seconds = first.timestamp_unix_nanos.div_euclid(1_000_000_000); }
        let task_list = info.activity.task_list.clone();
        self.state.update_activity_timer(info)?;
        self.state.add_activity_timeout_task(first, task_list);
        Ok(true)
    }
}
fn sort(timers: &mut [TimerSequenceId]) { timers.sort_by_key(|t| (t.timestamp_unix_nanos, t.event_id, t.timer_type)); }
