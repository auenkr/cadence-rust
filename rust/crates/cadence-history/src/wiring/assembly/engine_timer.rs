//! group: assembly/engine_timer
//!
//! Timer support of `EngineMutableState` for the timer active task executor: Go `timer_sequence.go`
//! (user/activity timer ordering and next-timer creation), `AddTimerFiredEvent`,
//! `AddActivityTaskTimedOutEvent`, `AddDecisionTaskTimedOutEvent`, `AddTimeoutWorkflowEvent`,
//! `GetRetryBackoffDuration` and `GetCronBackoffDuration`.
//! `retry_workflow_new_run` is Go `executeWorkflowTimeoutTask` + `retryWorkflow` (continue-as-new run creation).

use std::collections::HashSet;
use std::sync::Arc;
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
        // Go DeleteUserTimer: log a data inconsistency and continue.
        if g.timers.remove(timer_id).is_none() {
            tracing::error!("unable to find timer ID: {timer_id} in mutable state");
            g.log_data_inconsistency();
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
        g.push_event(EventType::TimerFired, ms * 1_000_000, |e| {
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
        let reason = Some(a.retry_last_failure_reason.clone());
        let last_details = a.retry_last_failure_details.clone();
        let options = Some(cadence_api::types::FailureOptions {
            failure_category: crate::execution::retry::failure_category_from_go_i32(a.retry_last_failure_category),
            next_retry_interval_seconds: Some(a.retry_last_retry_interval_seconds),
        });
        g.push_event(EventType::ActivityTaskTimedOut, ms * 1_000_000, |e| {
            e.activity_task_timed_out_event_attributes = Some(ActivityTaskTimedOutEventAttributes {
                details,
                scheduled_event_id: schedule_id,
                started_event_id: started,
                timeout_type: Some(timeout_type),
                last_failure_reason: reason,
                last_failure_details: last_details,
                last_failure_options: options,
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
            g.push_event(EventType::DecisionTaskTimedOut, now, |e| {
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

    /// Go `execution.FailDecision(ms, decision, ForceCloseDecision)` for an in-flight decision.
    pub(super) fn force_close_in_flight_decision(&self) -> HResult<()> {
        let pending = {
            let g = self.inner.lock().unwrap();
            g.in_flight()
                .then(|| (g.pc_exec.decision_schedule_id, g.pc_exec.decision_started_id))
        };
        match pending {
            Some((schedule_id, started_id)) => self.add_decision_task_failed(
                schedule_id,
                started_id,
                Some(cadence_api::enums::DecisionTaskFailedCause::ForceCloseDecision),
                &[],
                "history-service",
                "",
            ),
            None => Ok(()),
        }
    }

    /// Go `executeWorkflowTimeoutTask` (retry / cron branch) + `retryWorkflow`: builds the
    /// continue-as-new attributes from the start event, force-closes the in-flight decision, closes
    /// this run with `WorkflowExecutionContinuedAsNew` and returns the new run's mutable state.
    pub(super) fn retry_workflow_new_run(
        &self,
        first_event_id: i64,
        backoff: Duration,
        cron: bool,
        reason: &str,
    ) -> HResult<Arc<EngineMutableState>> {
        let start = self
            .inner
            .lock()
            .unwrap()
            .prev_start_event
            .clone()
            .and_then(|e| e.workflow_execution_started_event_attributes)
            .ok_or_else(|| HistoryError::internal("workflow start event is missing"))?;
        let initiator = if cron {
            cadence_api::enums::ContinueAsNewInitiator::CronSchedule
        } else {
            cadence_api::enums::ContinueAsNewInitiator::RetryPolicy
        };
        let seconds = i32::try_from(backoff.as_secs()).unwrap_or(i32::MAX);
        let attrs = crate::decision::task_handler::retry_cron_continue_as_new_attributes(
            &start,
            seconds,
            initiator,
            Some(reason.to_string()),
            Vec::new(),
            Vec::new(),
            start.active_cluster_selection_policy.clone(),
        );
        let parent_domain = start.parent_workflow_domain.clone().unwrap_or_default();
        self.force_close_in_flight_decision()?;
        self.add_continue_as_new_event(first_event_id, EMPTY_EVENT_ID, &parent_domain, &attrs)?;
        self.take_new_run().ok_or_else(|| HistoryError::internal("continue-as-new did not produce a new run"))
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
        let ev = g.push_event(EventType::WorkflowExecutionTimedOut, ms * 1_000_000, |e| {
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
            e.jitter_coefficient,
            rand::random::<f64>(),
        )?;
        // Go: round up so a sub-second jittered interval doesn't become an immediate retry.
        let backoff = Duration::from_secs(backoff.as_secs() + u64::from(backoff.subsec_nanos() > 0));
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

    /// Go `GetCronBackoffDuration`: execution time = start timestamp + first decision backoff;
    /// jitter start seconds come from the start event (requires it to be loaded, as in Go).
    pub fn workflow_cron_backoff(&self) -> HResult<Option<Duration>> {
        let now = self.now();
        let g = self.inner.lock().unwrap();
        if g.pc_exec.cron_schedule.is_empty() {
            return Ok(None);
        }
        let start_attrs = g
            .prev_start_event
            .clone()
            .and_then(|e| e.workflow_execution_started_event_attributes)
            .ok_or_else(|| HistoryError::internal("unable to find workflow start event"))?;
        let first_backoff = start_attrs.first_decision_task_backoff_seconds.unwrap_or(0) as i64 * NANOS;
        let jitter = start_attrs.jitter_start_seconds.unwrap_or(0);
        let jitter = if jitter > 0 {
            Duration::from_secs(rand::Rng::gen_range(&mut rand::thread_rng(), 0..=jitter as u64))
        } else {
            Duration::ZERO
        };
        let to_dt = |nanos: i64| {
            chrono::DateTime::from_timestamp(nanos.div_euclid(NANOS), nanos.rem_euclid(NANOS) as u32)
                .map(|d| d.naive_utc())
                .unwrap_or_default()
        };
        let secs = cadence_common::cron::get_backoff_for_next_schedule_in_seconds(
            &g.pc_exec.cron_schedule,
            to_dt(g.pc_exec.start_time_unix_nanos.saturating_add(first_backoff)),
            to_dt(now),
            Some(g.pc_exec.cron_overlap_policy),
            to_dt(now),
            jitter,
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

impl EngineMutableState {
    /// Go `timerSequence.CreateNextActivityTimer`: true when a timer task was created.
    pub fn create_next_activity_timer_task(&self) -> bool {
        let mut g = self.inner.lock().unwrap();
        let before = g.timer_tasks.len();
        g.create_next_activity_timer();
        g.timer_tasks.len() > before
    }

    /// Go `mutableStateBuilder.UpdateCurrentVersion`.
    pub fn update_current_version(&self, version: i64, force_update: bool) -> HResult<()> {
        let mut g = self.inner.lock().unwrap();
        if !g.exec.is_running() {
            // Completed workflows always take the last write version.
            let last = match &g.version_histories {
                Some(vh) => vh
                    .histories
                    .get(vh.current_version_history_index as usize)
                    .and_then(|h| h.items.last().map(|i| i.version))
                    .unwrap_or(-24),
                None => -24,
            };
            g.current_version = last;
            return Ok(());
        }
        let Some(vh) = g.version_histories.clone() else {
            g.current_version = -24;
            return Ok(());
        };
        let current = vh
            .histories
            .get(vh.current_version_history_index as usize)
            .ok_or_else(|| HistoryError::internal("current version history index out of range"))?;
        if let Some(last) = current.items.last() {
            g.current_version = last.version;
        }
        if version > g.current_version || force_update {
            g.current_version = version;
        }
        Ok(())
    }
}

#[cfg(test)]
mod retry_workflow_tests {
    use super::*;
    use crate::engine::deps::{DomainCacheEntry, DomainInfo};
    use crate::engine::deps_ms::MutableState;
    use cadence_api::enums::ContinueAsNewInitiator;
    use cadence_api::service_types::{HistoryEvent, WorkflowExecutionStartedEventAttributes};
    use cadence_api::types::{RetryPolicy, TaskList, WorkflowType};
    use cadence_persistence::history_contract as pc;

    fn state() -> EngineMutableState {
        let domain = DomainCacheEntry {
            info: DomainInfo { id: "d".into(), name: "n".into(), status: 0 },
            ..Default::default()
        };
        let mut snapshot = pc::WorkflowSnapshot::default();
        snapshot.execution_info.domain_id = "d".into();
        snapshot.execution_info.workflow_id = "wf".into();
        snapshot.execution_info.run_id = "run".into();
        snapshot.execution_info.task_list = "tl".into();
        snapshot.execution_info.next_event_id = 5;
        snapshot.execution_info.decision_schedule_id = EMPTY_EVENT_ID;
        snapshot.execution_info.decision_started_id = EMPTY_EVENT_ID;
        let s = EngineMutableState::from_snapshot(snapshot, domain, 0, Arc::new(|| 1_700_000_000_000_000_000)).unwrap();
        s.inner.lock().unwrap().prev_start_event = Some(HistoryEvent {
            event_id: 1,
            event_type: Some(EventType::WorkflowExecutionStarted),
            workflow_execution_started_event_attributes: Some(WorkflowExecutionStartedEventAttributes {
                workflow_type: Some(WorkflowType { name: "wt".into() }),
                task_list: Some(TaskList { name: "tl".into(), ..Default::default() }),
                execution_start_to_close_timeout_seconds: Some(10),
                task_start_to_close_timeout_seconds: Some(5),
                retry_policy: Some(RetryPolicy { maximum_attempts: 3, ..Default::default() }),
                ..Default::default()
            }),
            ..Default::default()
        });
        s
    }

    fn event_types(s: &EngineMutableState) -> Vec<EventType> {
        s.inner.lock().unwrap().hist.iter().filter_map(|e| e.event_type).collect()
    }

    #[test]
    fn workflow_timeout_with_retry_policy_continues_as_new_with_retry_initiator() {
        let s = state();
        let new_run = s.retry_workflow_new_run(5, Duration::from_secs(30), false, "START_TO_CLOSE").unwrap();
        assert!(!s.is_workflow_execution_running());
        assert_eq!(event_types(&s), vec![EventType::WorkflowExecutionContinuedAsNew]);
        let closed = s.inner.lock().unwrap().hist[0].workflow_execution_continued_as_new_event_attributes.clone().unwrap();
        assert_eq!(closed.initiator, Some(ContinueAsNewInitiator::RetryPolicy));
        assert_eq!(closed.backoff_start_interval_in_seconds, Some(30));
        assert_eq!(closed.failure_reason.as_deref(), Some("START_TO_CLOSE"));
        let started = new_run.inner.lock().unwrap().hist[0].workflow_execution_started_event_attributes.clone().unwrap();
        assert_eq!(started.initiator, Some(ContinueAsNewInitiator::RetryPolicy));
        assert_eq!(started.continued_execution_run_id, "run");
        assert_eq!(started.attempt, 1);
        assert!(new_run.is_workflow_execution_running());
    }

    #[test]
    fn workflow_timeout_with_cron_uses_cron_initiator_and_first_decision_backoff() {
        let s = state();
        let new_run = s.retry_workflow_new_run(5, Duration::from_secs(60), true, "START_TO_CLOSE").unwrap();
        let started = new_run.inner.lock().unwrap().hist[0].workflow_execution_started_event_attributes.clone().unwrap();
        assert_eq!(started.initiator, Some(ContinueAsNewInitiator::CronSchedule));
        assert_eq!(started.first_decision_task_backoff_seconds, Some(60));
        assert_eq!(started.attempt, 0);
    }

    #[test]
    fn retry_force_closes_in_flight_decision_first() {
        let s = state();
        {
            let mut g = s.inner.lock().unwrap();
            g.pc_exec.decision_schedule_id = 3;
            g.pc_exec.decision_started_id = 4;
            g.mirror_decision();
        }
        s.retry_workflow_new_run(5, Duration::from_secs(1), false, "START_TO_CLOSE").unwrap();
        assert_eq!(
            event_types(&s),
            vec![EventType::DecisionTaskFailed, EventType::WorkflowExecutionContinuedAsNew]
        );
    }

    #[test]
    fn retry_without_loaded_start_event_fails() {
        let s = state();
        s.inner.lock().unwrap().prev_start_event = None;
        assert!(s.retry_workflow_new_run(5, Duration::from_secs(1), false, "START_TO_CLOSE").is_err());
        assert!(s.is_workflow_execution_running());
    }

    #[test]
    fn retry_backoff_rounds_up_to_whole_seconds_with_jitter() {
        let s = state();
        {
            let mut g = s.inner.lock().unwrap();
            g.pc_exec.has_retry_policy = true;
            g.pc_exec.initial_interval_nanos = 10 * NANOS;
            g.pc_exec.backoff_coefficient = 2.0;
            g.pc_exec.jitter_coefficient = 0.5;
            g.pc_exec.maximum_attempts = 5;
        }
        for _ in 0..50 {
            let d = s.workflow_retry_backoff("x").unwrap();
            assert_eq!(d.subsec_nanos(), 0);
            assert!(d.as_secs() >= 5 && d.as_secs() <= 10, "{d:?}");
        }
    }

    #[test]
    fn cron_backoff_uses_first_decision_backoff_and_requires_start_event() {
        let s = state();
        {
            let mut g = s.inner.lock().unwrap();
            g.pc_exec.cron_schedule = "0 * * * *".into();
            // now = 1_700_000_000s = 22:13:20 UTC; started at 21:00:00 with 2h first-decision backoff
            g.pc_exec.start_time_unix_nanos = 1_699_995_600 * NANOS;
            g.pc_exec.cron_overlap_policy = 0;
            g.prev_start_event.as_mut().unwrap().workflow_execution_started_event_attributes.as_mut().unwrap().first_decision_task_backoff_seconds = Some(7200);
        }
        // execution time 23:00 -> next 00:00 (> close 22:13:20) => 6400s... computed from schedule
        let d = s.workflow_cron_backoff().unwrap().unwrap();
        assert_eq!(d, Duration::from_secs(6400));
        s.inner.lock().unwrap().prev_start_event = None;
        assert!(s.workflow_cron_backoff().is_err());
    }
}
