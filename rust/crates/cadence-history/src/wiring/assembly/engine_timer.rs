//! group: assembly/engine_timer
//!
//! Timer support of `EngineMutableState` for the timer active task executor: Go `timer_sequence.go`
//! (user/activity timer ordering and next-timer creation), `AddTimerFiredEvent`,
//! `AddActivityTaskTimedOutEvent`, `AddDecisionTaskTimedOutEvent`, `AddTimeoutWorkflowEvent`,
//! `GetRetryBackoffDuration` and `GetCronBackoffDuration`.
//! Not ported here: `retry_workflow` / `update_workflow_execution_with_new_as_active`
//! (continue-as-new run creation), see memory/history-gap-timermut-20261107.md.

use std::collections::HashSet;
use std::time::{Duration, UNIX_EPOCH};

use cadence_api::enums::{EventType, TimeoutType};
use cadence_api::tasks::*;

use crate::engine::deps::{
    ActivityInfo, HResult, HistoryError, EMPTY_EVENT_ID, WORKFLOW_CLOSE_STATUS_TIMED_OUT, WORKFLOW_STATE_COMPLETED,
};
use crate::execution::retry::{backoff_interval, should_retry};
use crate::wiring::assembly::engine_state::{EngineMutableState, Inner, TRANSIENT_EVENT_ID};

const NANOS: i64 = 1_000_000_000;
const TIMER_USER_TIMER: i32 = 2;
const TRANSFER_UPSERT_SEARCH_ATTRIBUTES: i32 = 8;
/// Go `TimerTaskStatusCreated`.
const TIMER_TASK_STATUS_CREATED: i64 = 1;

/// One ordered timer of the Go `TimerSequenceID`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqTimer {
    pub event_id: i64,
    pub expiry_nanos: i64,
    /// 0 start-to-close, 1 schedule-to-start, 2 schedule-to-close, 3 heartbeat.
    pub timeout_type: i32,
    pub attempt: i64,
}

impl Inner {
    /// Go `timerSequence.LoadAndSortUserTimers`.
    pub(super) fn sorted_user_timers(&self) -> Vec<SeqTimer> {
        let mut v: Vec<SeqTimer> = self
            .timers
            .values()
            .map(|t| SeqTimer { event_id: t.started_id, expiry_nanos: t.expiry_time_unix_nanos, timeout_type: 0, attempt: 0 })
            .collect();
        v.sort_by_key(|t| (t.expiry_nanos, t.event_id, t.timeout_type));
        v
    }

    /// Go `timerSequence.createNextUserTimer`.
    pub(super) fn create_next_user_timer(&mut self) {
        let Some(first) = self.sorted_user_timers().first().copied() else { return };
        let Some((id, info)) = self.timers.iter_mut().find(|(_, t)| t.started_id == first.event_id) else { return };
        if info.task_id & TIMER_TASK_STATUS_CREATED != 0 {
            return;
        }
        info.task_id |= TIMER_TASK_STATUS_CREATED;
        let (id, version) = (id.clone(), info.version);
        self.upsert_timers.insert(id);
        let mut tt = self.timer_base(TIMER_USER_TIMER, version, first.expiry_nanos);
        tt.event_id = first.event_id;
        self.timer_tasks.push(tt);
        self.dirty = true;
    }

    /// Go `timerSequence.LoadAndSortActivityTimers`.
    pub(super) fn sorted_activity_timers(&self) -> Vec<SeqTimer> {
        let mut v = Vec::new();
        for (sid, a) in &self.pc_acts {
            let attempt = a.attempt as i64;
            let mut add = |timeout_type: i32, base: i64, secs: i32| {
                if secs > 0 {
                    v.push(SeqTimer { event_id: *sid, expiry_nanos: base + secs as i64 * NANOS, timeout_type, attempt });
                }
            };
            add(2, a.scheduled_time_unix_nanos, a.schedule_to_close_timeout_seconds);
            if a.started_id == EMPTY_EVENT_ID {
                add(1, a.scheduled_time_unix_nanos, a.schedule_to_start_timeout_seconds);
            } else {
                add(0, a.started_time_unix_nanos, a.start_to_close_timeout_seconds);
                let hb = self.heartbeats.get(sid).map(|h| h.updated_time_unix_nanos).unwrap_or(0);
                add(3, a.started_time_unix_nanos.max(hb), a.heartbeat_timeout_seconds);
            }
        }
        v.sort_by_key(|t| (t.expiry_nanos, t.event_id, t.timeout_type));
        v
    }

    /// Go `GenerateWorkflowSearchAttrTasks`.
    pub(super) fn gen_search_attr_task(&mut self) {
        let version = self.current_version;
        let mut t = self.transfer_base(TRANSFER_UPSERT_SEARCH_ATTRIBUTES, version);
        t.target_domain_id = self.exec.domain_id.clone();
        t.task_list = self.exec.task_list.clone();
        self.transfer_tasks.push(t);
        self.dirty = true;
    }
}

/// Activity fields the engine `ActivityInfo` does not carry.
#[derive(Debug, Clone, Copy)]
pub struct ActivityTimerFields {
    pub schedule_to_start_timeout_seconds: i32,
    pub timer_task_status: i32,
}

impl EngineMutableState {
    pub fn seq_user_timers(&self) -> Vec<SeqTimer> {
        self.inner.lock().unwrap().sorted_user_timers()
    }

    pub fn seq_activity_timers(&self) -> Vec<SeqTimer> {
        self.inner.lock().unwrap().sorted_activity_timers()
    }

    /// `(timer id, started event id)` of the user timer started by `event_id`.
    pub fn user_timer_by_started_id(&self, event_id: i64) -> Option<(String, i64)> {
        let g = self.inner.lock().unwrap();
        g.timers.iter().find(|(_, t)| t.started_id == event_id).map(|(id, t)| (id.clone(), t.started_id))
    }

    pub fn activity_timer_fields(&self, schedule_id: i64) -> Option<ActivityTimerFields> {
        let g = self.inner.lock().unwrap();
        g.pc_acts.get(&schedule_id).map(|a| ActivityTimerFields {
            schedule_to_start_timeout_seconds: a.schedule_to_start_timeout_seconds,
            timer_task_status: a.timer_task_status,
        })
    }

    /// Go `UpdateActivity` for the timer task status written back by the executor.
    pub fn set_activity_timer_status(&self, schedule_id: i64, timer_task_status: i32) -> HResult<()> {
        let mut g = self.inner.lock().unwrap();
        let Some(a) = g.pc_acts.get_mut(&schedule_id) else {
            return Err(HistoryError::internal(format!("unable to find activity with schedule event id: {schedule_id}")));
        };
        a.timer_task_status = timer_task_status;
        g.refresh_activity(schedule_id);
        Ok(())
    }

    /// Go `DeleteUserTimer`.
    pub fn delete_user_timer(&self, timer_id: &str) -> HResult<()> {
        let mut g = self.inner.lock().unwrap();
        if g.timers.remove(timer_id).is_none() {
            return Err(HistoryError::internal(format!("unable to find user timer {timer_id} in mutable state")));
        }
        g.upsert_timers.remove(timer_id);
        g.delete_timers.insert(timer_id.to_string());
        g.dirty = true;
        Ok(())
    }

    /// Go `AddTimerFiredEvent`.
    pub fn add_timer_fired(&self, timer_id: &str) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let Some(t) = g.timers.get(timer_id).cloned() else {
            return Err(HistoryError::internal(format!("invalid history action TimerFired (timer {timer_id} not found)")));
        };
        let id = timer_id.to_string();
        g.push_event(EventType::TimerFired, ms, |e| {
            e.timer_fired_event_attributes = Some(TimerFiredEventAttributes { timer_id: id, started_event_id: t.started_id });
        });
        g.timers.remove(timer_id);
        g.upsert_timers.remove(timer_id);
        g.delete_timers.insert(timer_id.to_string());
        Ok(())
    }

    /// Go `GetResurrectedTimers`: pending timers whose Fired/Canceled event already exists in history.
    pub async fn resurrected_timers(&self) -> HResult<HashSet<String>> {
        let (pending, next): (HashSet<String>, i64) = {
            let g = self.inner.lock().unwrap();
            (g.timers.keys().cloned().collect(), g.next_event_id)
        };
        if pending.is_empty() {
            return Ok(HashSet::new());
        }
        let events = self.read_events(1, next).await?;
        let mut out = HashSet::new();
        for e in events {
            let id = match e.event_type {
                Some(EventType::TimerFired) => e.timer_fired_event_attributes.map(|a| a.timer_id),
                Some(EventType::TimerCanceled) => e.timer_canceled_event_attributes.map(|a| a.timer_id),
                _ => None,
            };
            if let Some(id) = id {
                if pending.contains(&id) {
                    out.insert(id);
                }
            }
        }
        Ok(out)
    }

    /// Go `AddActivityTaskTimedOutEvent` (+ replicate).
    pub fn add_activity_timed_out(&self, schedule_id: i64, started_id: i64, timeout_type: TimeoutType) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let Some(a) = g.pc_acts.get(&schedule_id).cloned() else {
            return Err(HistoryError::internal(format!(
                "invalid history action ActivityTaskTimedOut (next event id {})",
                g.next_event_id
            )));
        };
        if a.started_id != started_id {
            return Err(HistoryError::internal(format!(
                "invalid history action ActivityTaskTimedOut (next event id {})",
                g.next_event_id
            )));
        }
        let started = g.add_transient_started(schedule_id)?;
        let details = g.heartbeats.get(&schedule_id).map(|h| h.details.clone()).unwrap_or_default();
        let reason = (!a.retry_last_failure_reason.is_empty()).then(|| a.retry_last_failure_reason.clone());
        let last_details = a.retry_last_failure_details.clone();
        g.push_event(EventType::ActivityTaskTimedOut, ms, |e| {
            e.activity_task_timed_out_event_attributes = Some(ActivityTaskTimedOutEventAttributes {
                details,
                scheduled_event_id: schedule_id,
                started_event_id: started,
                timeout_type: Some(timeout_type),
                last_failure_reason: reason,
                last_failure_details: last_details,
                last_failure_options: None,
            });
        });
        g.remove_activity(schedule_id)
    }

    /// Go `AddDecisionTaskTimedOutEvent` (start-to-close).
    pub fn add_decision_timed_out(&self, schedule_id: i64, started_id: i64) -> HResult<()> {
        let now = self.now();
        let mut g = self.inner.lock().unwrap();
        if g.pc_exec.decision_schedule_id != schedule_id || g.pc_exec.decision_started_id != started_id {
            return Err(HistoryError::internal(format!(
                "invalid history action DecisionTaskTimedOut (next event id {})",
                g.next_event_id
            )));
        }
        if g.pc_exec.decision_attempt == 0 {
            g.push_event(EventType::DecisionTaskTimedOut, now / 1_000_000, |e| {
                e.decision_task_timed_out_event_attributes = Some(DecisionTaskTimedOutEventAttributes {
                    scheduled_event_id: schedule_id,
                    started_event_id: started_id,
                    timeout_type: Some(TimeoutType::StartToClose),
                    ..Default::default()
                });
            });
        }
        g.fail_decision(true, now);
        Ok(())
    }

    /// Go `AddTimeoutWorkflowEvent`.
    pub fn add_timeout_workflow(&self, first_event_id: i64) -> HResult<()> {
        let ms = self.now_millis();
        let params = self.params.clone();
        let mut g = self.inner.lock().unwrap();
        if !g.exec.is_running() {
            return Err(HistoryError::internal(format!(
                "invalid history action WorkflowExecutionTimedOut (next event id {})",
                g.next_event_id
            )));
        }
        let ev = g.push_event(EventType::WorkflowExecutionTimedOut, ms, |e| {
            e.workflow_execution_timed_out_event_attributes =
                Some(WorkflowExecutionTimedOutEventAttributes { timeout_type: Some(TimeoutType::StartToClose) });
        });
        g.exec.state = WORKFLOW_STATE_COMPLETED;
        g.exec.close_status = WORKFLOW_CLOSE_STATUS_TIMED_OUT;
        g.pc_exec.state = WORKFLOW_STATE_COMPLETED;
        g.pc_exec.close_status = WORKFLOW_CLOSE_STATUS_TIMED_OUT;
        g.pc_exec.completion_event_batch_id = first_event_id;
        g.clear_stickyness();
        g.gen_workflow_close_tasks(&params, ms * 1_000_000, ev.version);
        Ok(())
    }

    /// Go `GenerateWorkflowSearchAttrTasks`.
    pub fn add_search_attributes_task(&self) {
        self.inner.lock().unwrap().gen_search_attr_task();
    }

    /// Go `GetRetryBackoffDuration` (workflow retry policy on the execution info).
    pub fn workflow_retry_backoff(&self, reason: &str) -> Option<Duration> {
        let now = self.now();
        let g = self.inner.lock().unwrap();
        let e = &g.pc_exec;
        if !e.has_retry_policy {
            return None;
        }
        let backoff = backoff_interval(
            e.attempt,
            (e.initial_interval_nanos / NANOS) as i32,
            (e.maximum_interval_nanos / NANOS) as i32,
            e.backoff_coefficient,
            0.0,
            0.0,
        )?;
        let next = now.saturating_add(backoff.as_nanos() as i64);
        let expiration = (e.expiration_time_unix_nanos > 0)
            .then(|| UNIX_EPOCH + Duration::from_nanos(e.expiration_time_unix_nanos as u64));
        should_retry(
            UNIX_EPOCH + Duration::from_nanos(next.max(0) as u64),
            e.attempt,
            e.maximum_attempts,
            expiration,
            reason,
            &e.non_retriable_errors,
            false,
        )
        .then_some(backoff)
    }

    /// Go `GetCronBackoffDuration`. Jitter start seconds are not applied (start event not read).
    pub fn workflow_cron_backoff(&self) -> HResult<Option<Duration>> {
        let now = self.now();
        let g = self.inner.lock().unwrap();
        if g.pc_exec.cron_schedule.is_empty() {
            return Ok(None);
        }
        let to_dt = |nanos: i64| {
            chrono::DateTime::from_timestamp(nanos.div_euclid(NANOS), nanos.rem_euclid(NANOS) as u32)
                .map(|d| d.naive_utc())
                .unwrap_or_default()
        };
        let secs = cadence_common::cron::get_backoff_for_next_schedule_in_seconds(
            &g.pc_exec.cron_schedule,
            to_dt(g.pc_exec.start_time_unix_nanos),
            to_dt(now),
            Some(g.pc_exec.cron_overlap_policy),
            to_dt(now),
            Duration::ZERO,
        )
        .map_err(HistoryError::internal)?;
        Ok(Some(Duration::from_secs(secs.max(0) as u64)))
    }

    /// Engine activity view for `retry_activity` from the executor.
    pub fn engine_activity_info(&self, schedule_id: i64) -> Option<ActivityInfo> {
        self.inner.lock().unwrap().activities.get(&schedule_id).cloned()
    }

    /// Whether the transient-started marker applies (used by the adapter for retry decisions).
    pub fn is_transient_started(&self, schedule_id: i64) -> bool {
        self.inner.lock().unwrap().pc_acts.get(&schedule_id).is_some_and(|a| a.started_id == TRANSIENT_EVENT_ID)
    }
}
