//! group: assembly/engine_events
//!
//! The history-event family of `EngineMutableState`: Go `historyBuilder`
//! (event creation, buffering, flushing) plus the `AddXxxEvent` /
//! `ReplicateXxxEvent` methods of `mutable_state_builder_methods_*.go`.

use std::collections::HashSet;
use std::time::{Duration, UNIX_EPOCH};

use cadence_api::enums::{EventType, FailureCategory};
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::*;
use cadence_api::types::{FailureOptions, WorkflowExecution, WorkflowType};
use cadence_persistence::history_reader::{read_history_branch, ReadHistoryBranchRequest};

use crate::engine::deps::{
    ActivityInfo, HResult, HistoryError, EMPTY_EVENT_ID, WORKFLOW_CLOSE_STATUS_TERMINATED, WORKFLOW_STATE_COMPLETED,
};
use crate::engine::types::*;
use crate::execution::retry::{backoff_interval, should_retry};
use crate::wiring::assembly::engine_state::{
    EngineMutableState, Inner, BUFFERED_EVENT_ID, EMPTY_EVENT_TASK_ID, TRANSIENT_EVENT_ID,
};

/// Go `shouldBufferEvent` (only consulted while a decision is in flight): every event
/// type is buffered except workflow state changes, decision events and the events
/// generated directly from decisions.
fn is_bufferable(ty: EventType) -> bool {
    !matches!(
        ty,
        EventType::WorkflowExecutionStarted
            | EventType::WorkflowExecutionCompleted
            | EventType::WorkflowExecutionFailed
            | EventType::WorkflowExecutionTimedOut
            | EventType::WorkflowExecutionTerminated
            | EventType::WorkflowExecutionContinuedAsNew
            | EventType::WorkflowExecutionCanceled
            | EventType::DecisionTaskScheduled
            | EventType::DecisionTaskStarted
            | EventType::DecisionTaskCompleted
            | EventType::DecisionTaskFailed
            | EventType::DecisionTaskTimedOut
            | EventType::ActivityTaskScheduled
            | EventType::ActivityTaskCancelRequested
            | EventType::TimerStarted
            | EventType::TimerCanceled
            | EventType::CancelTimerFailed
            | EventType::RequestCancelExternalWorkflowExecutionInitiated
            | EventType::MarkerRecorded
            | EventType::StartChildWorkflowExecutionInitiated
            | EventType::SignalExternalWorkflowExecutionInitiated
            | EventType::UpsertWorkflowSearchAttributes
            | EventType::SemaphoreAcquireInitiated
            | EventType::SemaphoreReleased
    )
}

#[cfg(test)]
mod buffer_tests {
    use super::*;

    #[test]
    fn async_results_are_buffered_like_go() {
        for ty in [
            EventType::TimerFired,
            EventType::StartChildWorkflowExecutionFailed,
            EventType::ExternalWorkflowExecutionSignaled,
            EventType::SignalExternalWorkflowExecutionFailed,
            EventType::ExternalWorkflowExecutionCancelRequested,
            EventType::RequestCancelExternalWorkflowExecutionFailed,
            EventType::SemaphoreAcquired,
            EventType::ActivityTaskCompleted,
            EventType::WorkflowExecutionSignaled,
        ] {
            assert!(is_bufferable(ty), "{ty:?}");
        }
        for ty in [EventType::TimerStarted, EventType::DecisionTaskCompleted, EventType::WorkflowExecutionCompleted] {
            assert!(!is_bufferable(ty), "{ty:?}");
        }
    }
}

fn is_close_event(ty: Option<EventType>) -> bool {
    matches!(
        ty,
        Some(
            EventType::WorkflowExecutionCompleted
                | EventType::WorkflowExecutionFailed
                | EventType::WorkflowExecutionTimedOut
                | EventType::WorkflowExecutionTerminated
                | EventType::WorkflowExecutionContinuedAsNew
                | EventType::WorkflowExecutionCanceled
        )
    )
}

impl Inner {
    /// Go `checkAndClearTimerFiredEvent`: removes and returns a TimerFired event for `timer_id`
    /// from the persisted buffered events, the update-buffered events, then the new history.
    pub(super) fn check_and_clear_timer_fired_event(&mut self, timer_id: &str) -> Option<HistoryEvent> {
        fn take(events: &mut Vec<HistoryEvent>, timer_id: &str) -> Option<HistoryEvent> {
            let idx = events.iter().position(|e| {
                e.event_type == Some(EventType::TimerFired)
                    && e.timer_fired_event_attributes.as_ref().is_some_and(|a| a.timer_id == timer_id)
            })?;
            Some(events.remove(idx))
        }
        let found = take(&mut self.buffered, timer_id)
            .or_else(|| take(&mut self.update_buffered, timer_id))
            .or_else(|| take(&mut self.hist, timer_id));
        if found.is_some() {
            self.dirty = true;
        }
        found
    }

    /// Go `historyBuilder.addEventToHistory` + `CreateNewHistoryEvent`.
    pub(super) fn push_event(
        &mut self,
        ty: EventType,
        now_ms: i64,
        fill: impl FnOnce(&mut HistoryEvent),
    ) -> HistoryEvent {
        let buffer = self.in_flight() && is_bufferable(ty);
        let mut ev = HistoryEvent {
            timestamp: Some(now_ms),
            event_type: Some(ty),
            version: self.current_version,
            task_id: EMPTY_EVENT_TASK_ID,
            ..Default::default()
        };
        if buffer {
            ev.event_id = BUFFERED_EVENT_ID;
        } else {
            ev.event_id = self.next_event_id;
            self.next_event_id += 1;
        }
        fill(&mut ev);
        self.hist.push(ev.clone());
        self.dirty = true;
        ev
    }

    /// Go `FlushBufferedEvents`: commits buffered events when no decision is in
    /// flight, assigns ids to them and patches the started ids of later events.
    pub(super) fn flush_buffered(&mut self) {
        let in_flight = self.in_flight();
        let mut committed = Vec::new();
        let mut new_buffered = Vec::new();
        for e in std::mem::take(&mut self.hist) {
            if e.event_id == BUFFERED_EVENT_ID {
                new_buffered.push(e);
            } else {
                committed.push(e);
            }
        }
        if in_flight {
            self.update_buffered.extend(new_buffered);
            self.hist = committed;
            return;
        }
        committed.extend(std::mem::take(&mut self.buffered));
        if self.has_buffered_in_db {
            self.clear_buffered = true;
            self.has_buffered_in_db = false;
        }
        committed.extend(std::mem::take(&mut self.update_buffered));
        committed.extend(new_buffered);
        // trimEventsAfterWorkflowClose
        if let Some(pos) = committed.iter().position(|e| is_close_event(e.event_type)) {
            committed.truncate(pos + 1);
        }
        self.assign_event_ids(&mut committed);
        self.hist = committed;
        self.dirty = true;
    }

    /// Go `assignEventIDToBufferedEvents`.
    fn assign_event_ids(&mut self, events: &mut [HistoryEvent]) {
        let mut scheduled_to_started = std::collections::HashMap::<i64, i64>::new();
        for e in events.iter_mut() {
            if e.event_id != BUFFERED_EVENT_ID {
                continue;
            }
            let id = self.next_event_id;
            e.event_id = id;
            self.next_event_id += 1;
            match e.event_type {
                Some(EventType::ActivityTaskStarted) => {
                    if let Some(a) = &e.activity_task_started_event_attributes {
                        scheduled_to_started.insert(a.scheduled_event_id, id);
                        let sid = a.scheduled_event_id;
                        if let Some(ai) = self.pc_acts.get_mut(&sid) {
                            ai.started_id = id;
                            self.refresh_activity(sid);
                        }
                    }
                }
                Some(EventType::ChildWorkflowExecutionStarted) => {
                    if let Some(a) = &e.child_workflow_execution_started_event_attributes {
                        scheduled_to_started.insert(a.initiated_event_id, id);
                        let iid = a.initiated_event_id;
                        if let Some(ci) = self.pc_children.get_mut(&iid) {
                            ci.started_id = id;
                        }
                        if let Some(ci) = self.children.get_mut(&iid) {
                            ci.started_id = id;
                        }
                        self.upsert_children.insert(iid);
                    }
                }
                Some(EventType::ActivityTaskCompleted) => {
                    if let Some(a) = e.activity_task_completed_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.scheduled_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ActivityTaskFailed) => {
                    if let Some(a) = e.activity_task_failed_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.scheduled_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ActivityTaskTimedOut) => {
                    if let Some(a) = e.activity_task_timed_out_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.scheduled_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ActivityTaskCanceled) => {
                    if let Some(a) = e.activity_task_canceled_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.scheduled_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ChildWorkflowExecutionCompleted) => {
                    if let Some(a) = e.child_workflow_execution_completed_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.initiated_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ChildWorkflowExecutionFailed) => {
                    if let Some(a) = e.child_workflow_execution_failed_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.initiated_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ChildWorkflowExecutionTimedOut) => {
                    if let Some(a) = e.child_workflow_execution_timed_out_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.initiated_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ChildWorkflowExecutionCanceled) => {
                    if let Some(a) = e.child_workflow_execution_canceled_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.initiated_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                Some(EventType::ChildWorkflowExecutionTerminated) => {
                    if let Some(a) = e.child_workflow_execution_terminated_event_attributes.as_mut() {
                        if let Some(s) = scheduled_to_started.get(&a.initiated_event_id) {
                            a.started_event_id = *s;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Go `addTransientActivityStartedEvent`: materialises the Started event of a
    /// retried activity right before its close event. Returns the started id.
    pub(super) fn add_transient_started(&mut self, schedule_id: i64) -> HResult<i64> {
        let Some(mut a) = self.pc_acts.get(&schedule_id).cloned() else {
            return Err(HistoryError::internal(format!("activity {schedule_id} not found")));
        };
        if a.started_id != TRANSIENT_EVENT_ID {
            return Ok(a.started_id);
        }
        let started_ms = a.started_time_unix_nanos / 1_000_000;
        let (attempt, request_id, identity) = (a.attempt, a.request_id.clone(), a.started_identity.clone());
        let reason = if a.retry_last_failure_reason.is_empty() { None } else { Some(a.retry_last_failure_reason.clone()) };
        let details = a.retry_last_failure_details.clone();
        let options = if a.retry_last_failure_category != 0 || a.retry_last_retry_interval_seconds != 0 {
            Some(FailureOptions {
                failure_category: category_from_i32(a.retry_last_failure_category),
                next_retry_interval_seconds: Some(a.retry_last_retry_interval_seconds),
            })
        } else {
            None
        };
        let ev = self.push_event(EventType::ActivityTaskStarted, started_ms, |e| {
            e.activity_task_started_event_attributes = Some(ActivityTaskStartedEventAttributes {
                scheduled_event_id: schedule_id,
                identity,
                request_id,
                attempt,
                last_failure_reason: reason,
                last_failure_details: details,
                last_failure_options: options,
            });
        });
        a.started_id = ev.event_id;
        self.pc_acts.insert(schedule_id, a);
        self.refresh_activity(schedule_id);
        Ok(ev.event_id)
    }
}

fn category_from_i32(v: i32) -> Option<FailureCategory> {
    match v {
        0 => Some(FailureCategory::Poll),
        1 => Some(FailureCategory::Standard),
        2 => Some(FailureCategory::Fatal),
        _ => None,
    }
}

impl EngineMutableState {
    fn invalid_action(what: &str, next_event_id: i64) -> HistoryError {
        HistoryError::internal(format!("invalid history action {what} (next event id {next_event_id})"))
    }

    /// Go `AddActivityTaskStartedEvent`.
    pub(super) fn ev_activity_started(
        &self,
        _info: &ActivityInfo,
        schedule_id: i64,
        request_id: &str,
        identity: &str,
    ) -> HResult<ActivityInfo> {
        let now = self.now();
        let mut g = self.inner.lock().unwrap();
        let Some(mut a) = g.pc_acts.get(&schedule_id).cloned() else {
            return Err(Self::invalid_action("ActivityTaskStarted", g.next_event_id));
        };
        if a.has_retry_policy {
            a.started_id = TRANSIENT_EVENT_ID;
        } else {
            let attempt = a.attempt;
            let (rid, ident) = (request_id.to_string(), identity.to_string());
            let ev = g.push_event(EventType::ActivityTaskStarted, now / 1_000_000, |e| {
                e.activity_task_started_event_attributes = Some(ActivityTaskStartedEventAttributes {
                    scheduled_event_id: schedule_id,
                    identity: ident,
                    request_id: rid,
                    attempt,
                    ..Default::default()
                });
            });
            a.started_id = ev.event_id;
        }
        a.request_id = request_id.to_string();
        a.started_time_unix_nanos = now;
        a.started_identity = identity.to_string();
        g.pc_acts.insert(schedule_id, a);
        g.refresh_activity(schedule_id);
        Ok(g.activities[&schedule_id].clone())
    }

    fn check_activity_close(g: &Inner, schedule_id: i64, started_id: i64, what: &str) -> HResult<()> {
        match g.pc_acts.get(&schedule_id) {
            Some(a) if a.started_id == started_id => Ok(()),
            _ => Err(Self::invalid_action(what, g.next_event_id)),
        }
    }

    /// Go `AddActivityTaskCompletedEvent`.
    pub(super) fn ev_activity_completed(
        &self,
        schedule_id: i64,
        started_id: i64,
        request: &RespondActivityTaskCompletedRequest,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        Self::check_activity_close(&g, schedule_id, started_id, "ActivityTaskCompleted")?;
        let started = g.add_transient_started(schedule_id)?;
        let (result, identity) = (request.result.clone(), request.identity.clone());
        g.push_event(EventType::ActivityTaskCompleted, ms, |e| {
            e.activity_task_completed_event_attributes = Some(ActivityTaskCompletedEventAttributes {
                result,
                scheduled_event_id: schedule_id,
                started_event_id: started,
                identity,
            });
        });
        g.remove_activity(schedule_id)
    }

    /// Go `AddActivityTaskFailedEvent`.
    pub(super) fn ev_activity_failed(
        &self,
        schedule_id: i64,
        started_id: i64,
        request: &RespondActivityTaskFailedRequest,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        Self::check_activity_close(&g, schedule_id, started_id, "ActivityTaskFailed")?;
        let started = g.add_transient_started(schedule_id)?;
        let r = request.clone();
        g.push_event(EventType::ActivityTaskFailed, ms, |e| {
            e.activity_task_failed_event_attributes = Some(ActivityTaskFailedEventAttributes {
                reason: r.reason,
                details: r.details,
                failure_options: r.failure_options,
                scheduled_event_id: schedule_id,
                started_event_id: started,
                identity: r.identity,
            });
        });
        g.remove_activity(schedule_id)
    }

    /// Go `AddActivityTaskCanceledEvent`.
    pub(super) fn ev_activity_canceled(
        &self,
        schedule_id: i64,
        started_id: i64,
        cancel_request_id: i64,
        details: &[u8],
        identity: &str,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        Self::check_activity_close(&g, schedule_id, started_id, "ActivityTaskCanceled")?;
        if !g.pc_acts[&schedule_id].cancel_requested {
            return Err(Self::invalid_action("ActivityTaskCanceled (cancel not requested)", g.next_event_id));
        }
        let started = g.add_transient_started(schedule_id)?;
        let (details, identity) = (details.to_vec(), identity.to_string());
        g.push_event(EventType::ActivityTaskCanceled, ms, |e| {
            e.activity_task_canceled_event_attributes = Some(ActivityTaskCanceledEventAttributes {
                details,
                latest_cancel_requested_event_id: cancel_request_id,
                scheduled_event_id: schedule_id,
                started_event_id: started,
                identity,
            });
        });
        g.remove_activity(schedule_id)
    }

    /// Go `RetryActivity`.
    pub(super) fn ev_retry_activity(
        &self,
        info: &ActivityInfo,
        reason: &str,
        details: &[u8],
        options: Option<&FailureOptions>,
    ) -> HResult<bool> {
        let now = self.now();
        let mut g = self.inner.lock().unwrap();
        let Some(mut a) = g.pc_acts.get(&info.schedule_id).cloned() else {
            return Err(Self::invalid_action("RetryActivity", g.next_event_id));
        };
        if !a.has_retry_policy || a.cancel_requested {
            return Ok(false);
        }
        let override_secs = options.and_then(|o| o.next_retry_interval_seconds).unwrap_or(0);
        let backoff = if override_secs > 0 {
            Some(Duration::from_secs(override_secs as u64))
        } else {
            backoff_interval(
                a.attempt,
                a.retry_initial_interval_seconds,
                a.retry_maximum_interval_seconds,
                a.retry_backoff_coefficient,
                a.retry_jitter_coefficient,
                rand::random::<f64>(),
            )
        };
        let Some(backoff) = backoff else { return Ok(false) };
        let next = now.saturating_add(backoff.as_nanos() as i64);
        let expiration = (a.retry_expiration_time_unix_nanos > 0)
            .then(|| UNIX_EPOCH + Duration::from_nanos(a.retry_expiration_time_unix_nanos as u64));
        let category = options.and_then(|o| o.failure_category);
        let fatal = category == Some(FailureCategory::Fatal);
        if !should_retry(
            UNIX_EPOCH + Duration::from_nanos(next.max(0) as u64),
            a.attempt,
            a.retry_maximum_attempts,
            expiration,
            reason,
            &a.retry_non_retriable_errors,
            fatal,
        ) {
            return Ok(false);
        }
        a.version = g.current_version;
        a.attempt += 1;
        a.scheduled_time_unix_nanos = next;
        a.started_id = EMPTY_EVENT_ID;
        a.request_id.clear();
        a.started_time_unix_nanos = 0;
        a.timer_task_status = 0;
        a.retry_last_failure_reason = reason.to_string();
        a.retry_last_worker_identity = a.started_identity.clone();
        a.retry_last_failure_details = details.to_vec();
        a.retry_last_failure_category = category.map(|c| c as i32).unwrap_or(0);
        a.retry_last_retry_interval_seconds = options.and_then(|o| o.next_retry_interval_seconds).unwrap_or(0);
        let (version, attempt) = (a.version, a.attempt as i64);
        g.pc_acts.insert(info.schedule_id, a);
        g.refresh_activity(info.schedule_id);
        g.gen_activity_retry_timer(info.schedule_id, next, attempt, version);
        Ok(true)
    }

    /// Go `AddWorkflowExecutionSignaled` + `ReplicateWorkflowExecutionSignaled`.
    pub(super) fn ev_signaled(&self, name: &str, input: &[u8], identity: &str, request_id: &str) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let (n, i, id, r) = (name.to_string(), input.to_vec(), identity.to_string(), request_id.to_string());
        g.push_event(EventType::WorkflowExecutionSignaled, ms, |e| {
            e.workflow_execution_signaled_event_attributes =
                Some(cadence_api::service_types::WorkflowExecutionSignaledEventAttributes {
                    signal_name: n,
                    input: i,
                    identity: id,
                    request_id: r,
                });
        });
        g.pc_exec.signal_count += 1;
        g.exec.signal_count += 1;
        Ok(())
    }

    /// Go `AddWorkflowExecutionCancelRequestedEvent`.
    pub(super) fn ev_cancel_requested(
        &self,
        cause: &str,
        request: &HistoryRequestCancelWorkflowExecutionRequest,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        if g.pc_exec.cancel_requested {
            return Err(Self::invalid_action("WorkflowExecutionCancelRequested", g.next_event_id));
        }
        let cancel = request.cancel_request.clone().unwrap_or_default();
        let (cause, ext_id, ext_exec) =
            (cause.to_string(), request.external_initiated_event_id, request.external_workflow_execution.clone());
        let (identity, request_id) = (cancel.identity.clone(), cancel.request_id.clone());
        g.push_event(EventType::WorkflowExecutionCancelRequested, ms, |e| {
            e.workflow_execution_cancel_requested_event_attributes =
                Some(cadence_api::service_types::WorkflowExecutionCancelRequestedEventAttributes {
                    cause,
                    external_initiated_event_id: ext_id,
                    external_workflow_execution: ext_exec,
                    identity,
                    request_id: request_id.clone(),
                });
        });
        g.pc_exec.cancel_requested = true;
        g.pc_exec.cancel_request_id = request_id;
        Ok(())
    }

    /// Go `AddWorkflowExecutionTerminatedEvent` + replicate.
    pub(super) fn ev_terminate(&self, first_event_id: i64, reason: &str, details: &[u8], identity: &str) -> HResult<()> {
        let ms = self.now_millis();
        let params = self.params.clone();
        let mut g = self.inner.lock().unwrap();
        if !g.exec.is_running() {
            return Err(Self::invalid_action("WorkflowExecutionTerminated", g.next_event_id));
        }
        let (reason, details, identity) = (reason.to_string(), details.to_vec(), identity.to_string());
        let ev = g.push_event(EventType::WorkflowExecutionTerminated, ms, |e| {
            e.workflow_execution_terminated_event_attributes =
                Some(cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes { reason, details, identity });
        });
        g.exec.state = WORKFLOW_STATE_COMPLETED;
        g.exec.close_status = WORKFLOW_CLOSE_STATUS_TERMINATED;
        g.pc_exec.completion_event_batch_id = first_event_id;
        g.clear_stickyness();
        g.gen_workflow_close_tasks(&params, ms * 1_000_000, ev.version);
        Ok(())
    }

    // ---- child workflow close events ------------------------------------

    fn child_info(&self, g: &Inner, initiated_id: i64, what: &str) -> HResult<(String, WorkflowType, i64)> {
        match (g.children.get(&initiated_id), g.pc_children.get(&initiated_id)) {
            (Some(ci), Some(pci)) if ci.started_id != EMPTY_EVENT_ID => {
                let domain = if pci.domain_name.is_empty() { self.domain.info.name.clone() } else { pci.domain_name.clone() };
                Ok((domain, WorkflowType { name: pci.workflow_type_name.clone() }, ci.started_id))
            }
            _ => Err(Self::invalid_action(what, g.next_event_id)),
        }
    }

    pub(super) fn ev_child_completed(
        &self,
        initiated_id: i64,
        execution: &WorkflowExecution,
        attrs: Option<&WorkflowExecutionCompletedEventAttributes>,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let (domain, wt, started) = self.child_info(&g, initiated_id, "ChildWorkflowExecutionCompleted")?;
        let result = attrs.map(|a| a.result.clone()).unwrap_or_default();
        let exec = execution.clone();
        g.push_event(EventType::ChildWorkflowExecutionCompleted, ms, |e| {
            e.child_workflow_execution_completed_event_attributes = Some(ChildWorkflowExecutionCompletedEventAttributes {
                result,
                domain,
                workflow_execution: Some(exec),
                workflow_type: Some(wt),
                initiated_event_id: initiated_id,
                started_event_id: started,
            });
        });
        g.remove_child(initiated_id)
    }

    pub(super) fn ev_child_failed(
        &self,
        initiated_id: i64,
        execution: &WorkflowExecution,
        attrs: Option<&WorkflowExecutionFailedEventAttributes>,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let (domain, wt, started) = self.child_info(&g, initiated_id, "ChildWorkflowExecutionFailed")?;
        let (reason, details) = attrs.map(|a| (a.reason.clone(), a.details.clone())).unwrap_or_default();
        let exec = execution.clone();
        g.push_event(EventType::ChildWorkflowExecutionFailed, ms, |e| {
            e.child_workflow_execution_failed_event_attributes = Some(ChildWorkflowExecutionFailedEventAttributes {
                reason,
                details,
                domain,
                workflow_execution: Some(exec),
                workflow_type: Some(wt),
                initiated_event_id: initiated_id,
                started_event_id: started,
            });
        });
        g.remove_child(initiated_id)
    }

    pub(super) fn ev_child_canceled(
        &self,
        initiated_id: i64,
        execution: &WorkflowExecution,
        attrs: Option<&WorkflowExecutionCanceledEventAttributes>,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let (domain, wt, started) = self.child_info(&g, initiated_id, "ChildWorkflowExecutionCanceled")?;
        let details = attrs.map(|a| a.details.clone()).unwrap_or_default();
        let exec = execution.clone();
        g.push_event(EventType::ChildWorkflowExecutionCanceled, ms, |e| {
            e.child_workflow_execution_canceled_event_attributes = Some(ChildWorkflowExecutionCanceledEventAttributes {
                details,
                domain,
                workflow_execution: Some(exec),
                workflow_type: Some(wt),
                initiated_event_id: initiated_id,
                started_event_id: started,
            });
        });
        g.remove_child(initiated_id)
    }

    pub(super) fn ev_child_terminated(
        &self,
        initiated_id: i64,
        execution: &WorkflowExecution,
        _attrs: Option<&cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes>,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let (domain, wt, started) = self.child_info(&g, initiated_id, "ChildWorkflowExecutionTerminated")?;
        let exec = execution.clone();
        g.push_event(EventType::ChildWorkflowExecutionTerminated, ms, |e| {
            e.child_workflow_execution_terminated_event_attributes = Some(ChildWorkflowExecutionTerminatedEventAttributes {
                domain,
                workflow_execution: Some(exec),
                workflow_type: Some(wt),
                initiated_event_id: initiated_id,
                started_event_id: started,
            });
        });
        g.remove_child(initiated_id)
    }

    pub(super) fn ev_child_timed_out(
        &self,
        initiated_id: i64,
        execution: &WorkflowExecution,
        attrs: Option<&WorkflowExecutionTimedOutEventAttributes>,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let (domain, wt, started) = self.child_info(&g, initiated_id, "ChildWorkflowExecutionTimedOut")?;
        let timeout_type = attrs.and_then(|a| a.timeout_type);
        let exec = execution.clone();
        g.push_event(EventType::ChildWorkflowExecutionTimedOut, ms, |e| {
            e.child_workflow_execution_timed_out_event_attributes = Some(ChildWorkflowExecutionTimedOutEventAttributes {
                timeout_type,
                domain,
                workflow_execution: Some(exec),
                workflow_type: Some(wt),
                initiated_event_id: initiated_id,
                started_event_id: started,
            });
        });
        g.remove_child(initiated_id)
    }

    // ---- history reads ----------------------------------------------------

    /// Reads events `[min, max)` of the current branch.
    pub(super) async fn read_events(&self, min: i64, max: i64) -> HResult<Vec<HistoryEvent>> {
        use crate::engine::deps_ms::MutableState;
        let branch = self.get_current_branch_token()?;
        let store = self
            .params
            .history_nodes
            .clone()
            .ok_or_else(|| HistoryError::internal("history node store is not configured on the mutable state"))?;
        let shard_id = self.params.shard_id;
        tokio::task::spawn_blocking(move || -> Result<Vec<HistoryEvent>, String> {
            let mut out = Vec::new();
            let mut token = Vec::new();
            loop {
                let resp = read_history_branch(
                    &*store,
                    &ReadHistoryBranchRequest {
                        branch_token: branch.clone(),
                        min_event_id: min,
                        max_event_id: max,
                        page_size: 1000,
                        next_page_token: token.clone(),
                        shard_id,
                    },
                )
                .map_err(|e| format!("{e:?}"))?;
                for b in resp.batches {
                    out.extend(b);
                }
                if resp.next_page_token.is_empty() {
                    break;
                }
                token = resp.next_page_token;
            }
            Ok(out)
        })
        .await
        .map_err(|e| HistoryError::internal(format!("history read task: {e}")))?
        .map_err(|e| HistoryError::internal(format!("read history branch: {e}")))
    }

    /// Go `GetStartEvent`.
    pub(super) async fn ev_start_event(&self) -> HResult<HistoryEvent> {
        let events = self.read_events(1, 2).await?;
        events
            .into_iter()
            .find(|e| e.event_id == 1)
            .ok_or_else(|| HistoryError::internal("workflow start event is missing"))
    }

    /// Go `GetCompletionEvent`: the last event of the closed workflow.
    pub(super) async fn ev_completion_event(&self) -> HResult<HistoryEvent> {
        let (batch, next) = {
            let g = self.inner.lock().unwrap();
            if g.exec.is_running() {
                return Err(HistoryError::internal("workflow execution is still running, no completion event"));
            }
            (g.pc_exec.completion_event_batch_id, g.next_event_id)
        };
        let events = self.read_events(batch, next).await?;
        events
            .into_iter()
            .find(|e| e.event_id == next - 1)
            .ok_or_else(|| HistoryError::internal("workflow completion event is missing"))
    }

    /// Go `GetActivityScheduledEvent`.
    pub(super) async fn ev_activity_scheduled_event(&self, schedule_id: i64) -> HResult<HistoryEvent> {
        let batch = {
            let g = self.inner.lock().unwrap();
            match g.pc_acts.get(&schedule_id) {
                Some(a) => a.scheduled_event_batch_id,
                None => return Err(HistoryError::internal(format!("missing activity info for schedule id {schedule_id}"))),
            }
        };
        let events = self.read_events(batch, schedule_id + 1).await?;
        events
            .into_iter()
            .find(|e| e.event_id == schedule_id)
            .ok_or_else(|| HistoryError::internal(format!("activity scheduled event {schedule_id} not found")))
    }

    /// Go `execution.GetResurrectedActivities`.
    pub(super) async fn ev_resurrected_activities(&self) -> HResult<HashSet<i64>> {
        let (pending, next): (HashSet<i64>, i64) = {
            let g = self.inner.lock().unwrap();
            (g.activities.keys().copied().collect(), g.next_event_id)
        };
        if pending.is_empty() {
            return Ok(HashSet::new());
        }
        let events = self.read_events(1, next).await?;
        let mut out = HashSet::new();
        for e in events {
            let sid = match e.event_type {
                Some(EventType::ActivityTaskCompleted) => e.activity_task_completed_event_attributes.map(|a| a.scheduled_event_id),
                Some(EventType::ActivityTaskFailed) => e.activity_task_failed_event_attributes.map(|a| a.scheduled_event_id),
                Some(EventType::ActivityTaskTimedOut) => e.activity_task_timed_out_event_attributes.map(|a| a.scheduled_event_id),
                Some(EventType::ActivityTaskCanceled) => e.activity_task_canceled_event_attributes.map(|a| a.scheduled_event_id),
                _ => None,
            };
            if let Some(sid) = sid {
                if sid != 0 && pending.contains(&sid) {
                    out.insert(sid);
                }
            }
        }
        Ok(out)
    }

    /// Go `CopyToPersistence` (debug / describe view of the loaded state).
    pub(super) fn ev_copy_to_persistence(&self) -> HResult<serde_json::Value> {
        let g = self.inner.lock().unwrap();
        Ok(serde_json::json!({
            "executionInfo": {
                "domainId": g.exec.domain_id,
                "workflowId": g.exec.workflow_id,
                "runId": g.exec.run_id,
                "state": g.exec.state,
                "closeStatus": g.exec.close_status,
                "nextEventId": g.next_event_id,
                "lastFirstEventId": g.last_first_event_id,
                "decisionScheduleId": g.pc_exec.decision_schedule_id,
                "decisionStartedId": g.pc_exec.decision_started_id,
                "decisionAttempt": g.pc_exec.decision_attempt,
                "signalCount": g.exec.signal_count,
                "cancelRequested": g.pc_exec.cancel_requested,
            },
            // Go `VersionHistories` (CLI reads the current branch token from it).
            "versionHistories": g.version_histories,
            "pendingActivities": g.pc_acts.keys().collect::<Vec<_>>(),
            "pendingChildExecutions": g.pc_children.keys().collect::<Vec<_>>(),
            // Go `BufferedEvents []*types.HistoryEvent`: the CLI unmarshals this
            // into persistence.WorkflowMutableState, so it must be an event list.
            "bufferedEvents": g.buffered.iter().chain(g.update_buffered.iter()).collect::<Vec<_>>(),
        }))
    }
}
