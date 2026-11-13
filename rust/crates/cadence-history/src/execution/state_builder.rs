//! Event replay into mutable state, corresponding to Go's `state_builder.go`.

use cadence_api::{enums::EventType, service_types::HistoryEvent};
use cadence_api::types::WorkflowExecution;
use crate::mutable_state::{MutableStateBuilder, MutableStateError};
use super::mutable_state_builder::{TypedMutableState, TypedMutableStateError};
use super::mutable_state_methods_started::StartedEnv;
use super::mutable_state_methods_decision::DecisionEnv;
use super::{child_workflow::ChildWorkflowError, timer::TimerError};

#[derive(Debug, thiserror::Error)]
pub enum StateBuilderError {
    #[error(transparent)] State(#[from] MutableStateError),
    #[error(transparent)] Timer(#[from] TimerError),
    #[error(transparent)] Activity(#[from] crate::mutable_state_activity::ActivityTransitionError),
    #[error(transparent)] Child(#[from] ChildWorkflowError),
    #[error("encounter history size being zero")] HistorySizeZero,
    #[error("version histories are missing")] MissingVersionHistories,
    #[error("continued-as-new run does not match new-run history")] NewRunLinkage,
    #[error("continued-as-new branch version does not match new-run history")] NewRunBranchVersion,
    #[error("invalid version history item")] VersionHistoryItem,
    #[error("Unknown event type")] UnknownEventType,
    /// Go `ErrMissingSemaphoreInfo` (InternalServiceError).
    #[error("unable to get semaphore info")] MissingSemaphoreInfo,
    #[error("unsupported or malformed replicated event {event_type:?} ({event_id})")]
    InvalidEvent { event_type: Option<EventType>, event_id: i64 },
    #[error("event type {event_type:?} is not yet supported in typed replay")]
    TypedReplayNotYetSupported { event_type: Option<EventType> },
    #[error(transparent)] Typed(#[from] TypedMutableStateError),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StateBuilder { pub mutable_state: MutableStateBuilder }

/// Wire/blob-decoded history events carry millisecond timestamps (`history_blob::decode`), while
/// typed replay (like Go's `event.Timestamp`) works in nanoseconds.
#[must_use]
pub fn events_millis_to_nanos(events: &[HistoryEvent]) -> Vec<HistoryEvent> {
    events
        .iter()
        .map(|e| {
            let mut e = e.clone();
            e.timestamp = e.timestamp.map(|ms| ms.saturating_mul(1_000_000));
            e
        })
        .collect()
}

impl StateBuilder {
    /// Go `ReplicateSemaphoreAcquireInitiatedEvent`: the hold derives only from the event and run.
    fn replicate_semaphore_acquire_initiated(&mut self, event: &HistoryEvent) -> Result<(), StateBuilderError> {
        let attributes = event.semaphore_acquire_initiated_event_attributes.clone().unwrap_or_default();
        let info = &self.mutable_state.execution_info;
        let workflow_id = info.get("workflow_id").and_then(|v| v.as_str()).unwrap_or_default();
        let run_id = info.get("run_id").and_then(|v| v.as_str()).unwrap_or_default();
        // Same encoding as `execution::semaphore` (Go commonsemaphore.Owner.String()).
        let owner_id = format!("{}:{}:{}:{}", workflow_id.len(), workflow_id, run_id, event.event_id);
        let wait_nanos = i64::from(attributes.wait_timeout_seconds.unwrap_or_default()) * 1_000_000_000;
        self.mutable_state.semaphore_state.upsert_semaphore_info(super::semaphore::SemaphoreInfo {
            version: event.version,
            initiated_id: event.event_id,
            semaphore_name: attributes.semaphore_name,
            owner_id,
            // Measured from the event's own timestamp so replay gets the same deadline.
            acquire_deadline_unix_nanos: event.timestamp.unwrap_or_default() + wait_nanos,
            token_id: 0,
        });
        Ok(())
    }
    /// Go `ReplicateSemaphoreAcquiredEvent`.
    fn replicate_semaphore_acquired(&mut self, event: &HistoryEvent) -> Result<(), StateBuilderError> {
        let attributes = event.semaphore_acquired_event_attributes.clone().unwrap_or_default();
        let mut info = self.mutable_state.semaphore_state.get_semaphore_info(attributes.initiated_event_id).cloned().ok_or(StateBuilderError::MissingSemaphoreInfo)?;
        info.version = event.version;
        info.token_id = attributes.token_id;
        self.mutable_state.semaphore_state.upsert_semaphore_info(info);
        Ok(())
    }
    /// Go `ReplicateSemaphoreReleasedEvent` -> `DeleteSemaphoreInfo`.
    fn replicate_semaphore_released(&mut self, event: &HistoryEvent) -> Result<(), StateBuilderError> {
        let id = event.semaphore_released_event_attributes.clone().unwrap_or_default().initiated_event_id;
        let state = &mut self.mutable_state.semaphore_state;
        state.pending.remove(&id);
        state.updates.remove(&id);
        state.deletes.insert(id);
        Ok(())
    }
    pub fn new(mutable_state: MutableStateBuilder) -> Self { Self { mutable_state } }

    /// Replay history families with typed Replicate* transitions.
    /// Incoming events are metadata and are never appended or assigned new IDs.
    pub fn apply_events_typed(
        state: &mut TypedMutableState,
        domain_id: &str,
        request_id: &str,
        workflow_execution: &WorkflowExecution,
        events: &[HistoryEvent],
    ) -> Result<(), StateBuilderError> {
        Self::apply_events_typed_with_new_run(state, domain_id, request_id, workflow_execution, events, None, &[]).map(|_| ())
    }

    pub fn apply_events_typed_with_new_run(
        state: &mut TypedMutableState,
        domain_id: &str,
        request_id: &str,
        workflow_execution: &WorkflowExecution,
        events: &[HistoryEvent],
        new_run_execution: Option<&WorkflowExecution>,
        new_run_events: &[HistoryEvent],
    ) -> Result<Option<TypedMutableState>, StateBuilderError> {
        let (Some(first), Some(last)) = (events.first(), events.last()) else {
            return Err(StateBuilderError::HistorySizeZero);
        };
        let requests_before_replay = state.insert_workflow_requests.len();
        state.clear_stickyness();
        let old_suppression = state.suppress_task_generation;
        state.suppress_task_generation = true;
        let result = (|| {
            for event in events {
                state.update_current_version(event.version, true)?;
                let histories = state.version_histories.as_mut().ok_or(StateBuilderError::MissingVersionHistories)?;
                let idx = histories.current_version_history_index as usize;
                let history = histories.histories.get_mut(idx).ok_or(StateBuilderError::MissingVersionHistories)?;
                add_or_update_version_item(&mut history.items, event.event_id, event.version)?;
                state.execution_info.last_event_task_id = event.task_id;

                match event.event_type {
                    Some(EventType::WorkflowExecutionStarted) => {
                        let parent_domain_id = event.workflow_execution_started_event_attributes.as_ref()
                            .and_then(|a| a.parent_workflow_domain_id.as_deref());
                        state.replicate_workflow_execution_started_event(
                            &mut StartedEnv { domain_id: domain_id.to_string(), ..Default::default() },
                            parent_domain_id,
                            &workflow_execution.workflow_id,
                            &workflow_execution.run_id,
                            request_id,
                            event,
                            false,
                        )?;
                    }
                    Some(EventType::DecisionTaskScheduled) => {
                        let a = event.decision_task_scheduled_event_attributes.as_ref().ok_or(StateBuilderError::InvalidEvent { event_type: event.event_type, event_id: event.event_id })?;
                        state.replicate_decision_task_scheduled_event(
                            event.version, event.event_id,
                            a.task_list.as_ref().map(|t| t.name.as_str()).unwrap_or_default(),
                            a.start_to_close_timeout_seconds.unwrap_or_default(), a.attempt,
                            event.timestamp.unwrap_or_default(), event.timestamp.unwrap_or_default(), true,
                        )?;
                    }
                    Some(EventType::DecisionTaskStarted) => {
                        let a = event.decision_task_started_event_attributes.as_ref().ok_or(StateBuilderError::InvalidEvent { event_type: event.event_type, event_id: event.event_id })?;
                        state.replicate_decision_task_started_event(None, event.version, a.scheduled_event_id, event.event_id, &a.request_id, event.timestamp.unwrap_or_default())?;
                    }
                    Some(EventType::DecisionTaskCompleted) => {
                        let mut env = DecisionEnv { now_nanos: event.timestamp.unwrap_or_default(), ..Default::default() };
                        state.replicate_decision_task_completed_event(&mut env, event)?;
                    }
                    Some(EventType::DecisionTaskTimedOut) => {
                        let env = DecisionEnv { now_nanos: event.timestamp.unwrap_or_default(), ..Default::default() };
                        state.replicate_decision_task_timed_out_event(&env, event)?;
                        state.replicate_transient_decision_task_scheduled(&env)?;
                    }
                    Some(EventType::DecisionTaskFailed) => {
                        let env = DecisionEnv { now_nanos: event.timestamp.unwrap_or_default(), ..Default::default() };
                        state.replicate_decision_task_failed_event(&env, Some(event))?;
                        state.replicate_transient_decision_task_scheduled(&env)?;
                    }
                    Some(EventType::ActivityTaskScheduled) => {
                        state.replicate_activity_task_scheduled_event(first.event_id, event, domain_id)?;
                    }
                    Some(EventType::ActivityTaskStarted) => state.replicate_activity_task_started_event(event)?,
                    Some(EventType::ActivityTaskCompleted) => state.replicate_activity_task_completed_event(event)?,
                    Some(EventType::ActivityTaskFailed) => state.replicate_activity_task_failed_event(event)?,
                    Some(EventType::ActivityTaskTimedOut) => state.replicate_activity_task_timed_out_event(event)?,
                    Some(EventType::ActivityTaskCancelRequested) => state.replicate_activity_task_cancel_requested_event(event)?,
                    Some(EventType::ActivityTaskCanceled) => state.replicate_activity_task_canceled_event(event)?,
                    Some(EventType::RequestCancelActivityTaskFailed) => {}
                    Some(EventType::TimerStarted) => { state.replicate_timer_started_event(event)?; }
                    Some(EventType::TimerFired) => state.replicate_timer_fired_event(event)?,
                    Some(EventType::TimerCanceled) => state.replicate_timer_canceled_event(event)?,
                    Some(EventType::StartChildWorkflowExecutionInitiated) => {
                        state.replicate_start_child_workflow_execution_initiated_event(first.event_id, event, request_id, domain_id)?;
                    }
                    Some(EventType::ChildWorkflowExecutionStarted) => state.replicate_child_workflow_execution_started_event(event)?,
                    Some(EventType::StartChildWorkflowExecutionFailed) => state.replicate_start_child_workflow_execution_failed_event(event)?,
                    Some(EventType::ChildWorkflowExecutionCompleted) => state.replicate_child_workflow_execution_completed_event(event)?,
                    Some(EventType::ChildWorkflowExecutionFailed) => state.replicate_child_workflow_execution_failed_event(event)?,
                    Some(EventType::ChildWorkflowExecutionCanceled) => state.replicate_child_workflow_execution_canceled_event(event)?,
                    Some(EventType::ChildWorkflowExecutionTimedOut) => state.replicate_child_workflow_execution_timed_out_event(event)?,
                    Some(EventType::ChildWorkflowExecutionTerminated) => state.replicate_child_workflow_execution_terminated_event(event)?,
                    Some(EventType::RequestCancelExternalWorkflowExecutionInitiated) => {
                        state.replicate_request_cancel_external_workflow_execution_initiated_event(first.event_id, event, request_id)?;
                    }
                    Some(EventType::ExternalWorkflowExecutionCancelRequested) => state.replicate_external_workflow_execution_cancel_requested(event)?,
                    Some(EventType::RequestCancelExternalWorkflowExecutionFailed) => state.replicate_request_cancel_external_workflow_execution_failed_event(event)?,
                    Some(EventType::WorkflowExecutionSignaled) => state.replicate_workflow_execution_signaled(event)?,
                    Some(EventType::SignalExternalWorkflowExecutionInitiated) => {
                        state.replicate_signal_external_workflow_execution_initiated_event(first.event_id, event, request_id)?;
                    }
                    Some(EventType::SignalExternalWorkflowExecutionFailed) => state.replicate_signal_external_workflow_execution_failed_event(event)?,
                    Some(EventType::ExternalWorkflowExecutionSignaled) => state.replicate_external_workflow_execution_signaled(event)?,
                    Some(EventType::WorkflowExecutionCancelRequested) => state.replicate_workflow_execution_cancel_requested_event(event)?,
                    Some(EventType::MarkerRecorded) => {}
                    Some(EventType::UpsertWorkflowSearchAttributes) => state.replicate_upsert_workflow_search_attributes_event(event)?,
                    Some(EventType::WorkflowExecutionCompleted) => state.replicate_workflow_execution_completed_event(first.event_id, event)?,
                    Some(EventType::WorkflowExecutionFailed) => state.replicate_workflow_execution_failed_event(first.event_id, event)?,
                    Some(EventType::WorkflowExecutionTimedOut) => state.replicate_workflow_execution_timedout_event(first.event_id, event)?,
                    Some(EventType::WorkflowExecutionCanceled) => state.replicate_workflow_execution_canceled_event(first.event_id, event)?,
                    Some(EventType::WorkflowExecutionTerminated) => state.replicate_workflow_execution_terminated_event(first.event_id, event)?,
                    Some(EventType::WorkflowExecutionContinuedAsNew) => state.replicate_workflow_execution_continued_as_new_event(first.event_id, event)?,
                    Some(EventType::SemaphoreAcquireInitiated) => {
                        // Go `ReplicateSemaphoreAcquireInitiatedEvent`: the hold derives only from the event and run.
                        let attributes = event.semaphore_acquire_initiated_event_attributes.clone().unwrap_or_default();
                        let workflow_id = state.execution_info.workflow_id.clone();
                        let owner_id = format!("{}:{}:{}:{}", workflow_id.len(), workflow_id, state.execution_info.run_id, event.event_id);
                        let wait_nanos = i64::from(attributes.wait_timeout_seconds.unwrap_or_default()) * 1_000_000_000;
                        state.semaphore_state.upsert_semaphore_info(super::semaphore::SemaphoreInfo {
                            version: event.version,
                            initiated_id: event.event_id,
                            semaphore_name: attributes.semaphore_name,
                            owner_id,
                            acquire_deadline_unix_nanos: event.timestamp.unwrap_or_default().saturating_add(wait_nanos),
                            token_id: 0,
                        });
                    }
                    Some(EventType::SemaphoreAcquired) => {
                        let attributes = event.semaphore_acquired_event_attributes.clone().unwrap_or_default();
                        let mut info = state
                            .semaphore_state
                            .get_semaphore_info(attributes.initiated_event_id)
                            .cloned()
                            .ok_or(StateBuilderError::MissingSemaphoreInfo)?;
                        info.version = event.version;
                        info.token_id = attributes.token_id;
                        state.semaphore_state.upsert_semaphore_info(info);
                    }
                    Some(EventType::SemaphoreReleased) => {
                        let id = event.semaphore_released_event_attributes.clone().unwrap_or_default().initiated_event_id;
                        state.semaphore_state.pending.remove(&id);
                        state.semaphore_state.updates.remove(&id);
                        state.semaphore_state.deletes.insert(id);
                    }
                    None => return Err(StateBuilderError::UnknownEventType),
                    other => return Err(StateBuilderError::TypedReplayNotYetSupported { event_type: other }),
                }
            }
            state.execution_info.last_first_event_id = first.event_id;
            state.execution_info.next_event_id = last.event_id.saturating_add(1);
            // Go only persists these rows when strong idempotency is enabled.
            // This API has no domain policy input, so never retain fabricated payloads.
            state.insert_workflow_requests.truncate(requests_before_replay);
            Ok(())
        })();
        state.suppress_task_generation = old_suppression;
        result?;
        let Some(continued) = events.iter().rev().find(|e| e.event_type == Some(EventType::WorkflowExecutionContinuedAsNew)) else {
            return Ok(None);
        };
        if new_run_events.is_empty() { return Ok(None); }
        let expected_run_id = continued.workflow_execution_continued_as_new_event_attributes.as_ref()
            .map(|a| a.new_execution_run_id.as_str()).unwrap_or_default();
        let Some(new_start) = new_run_events.first() else { return Ok(None); };
        let Some(new_execution) = new_run_execution else { return Err(StateBuilderError::NewRunLinkage); };
        if expected_run_id.is_empty() || new_start.event_type != Some(EventType::WorkflowExecutionStarted)
            || new_execution.run_id != expected_run_id
            || new_execution.workflow_id != workflow_execution.workflow_id
            || new_start.workflow_execution_started_event_attributes.as_ref().map(|a| a.continued_execution_run_id.as_str()).unwrap_or_default() != workflow_execution.run_id
        { return Err(StateBuilderError::NewRunLinkage); }
        if new_start.version != continued.version { return Err(StateBuilderError::NewRunBranchVersion); }
        let mut new_state = TypedMutableState::new();
        new_state.version_histories = Some(cadence_api::types::VersionHistories {
            current_version_history_index: 0,
            histories: vec![cadence_api::types::VersionHistory { branch_token: Vec::new(), items: Vec::new() }],
        });
        Self::apply_events_typed(&mut new_state, domain_id, request_id, new_execution, new_run_events)?;
        Ok(Some(new_state))
    }
    /// Go `ApplyEvents`: replicates a batch onto mutable state.
    pub fn apply_events(&mut self, events: &[HistoryEvent]) -> Result<(), StateBuilderError> {
        // Go: errMessageHistorySizeZero
        let (Some(first_event), Some(last_event)) = (events.first(), events.last()) else {
            return Err(StateBuilderError::HistorySizeZero);
        };
        // need to clear the stickiness since workflow turned to passive
        self.mutable_state.execution_info["sticky_task_list"] = String::new().into();
        self.mutable_state.execution_info["sticky_schedule_to_start_timeout_seconds"] = 0.into();
        for event in events {
            // Go: UpdateCurrentVersion(event.Version, true) + version history AddOrUpdateItem
            let histories = self.mutable_state.version_histories.as_mut().ok_or(StateBuilderError::MissingVersionHistories)?;
            let idx = histories.current_version_history_index as usize;
            let history = histories.histories.get_mut(idx).ok_or(StateBuilderError::MissingVersionHistories)?;
            add_or_update_version_item(&mut history.items, event.event_id, event.version)?;
            self.mutable_state.execution_info["current_version"] = event.version.into();
            self.mutable_state.execution_info["last_event_task_id"] = event.task_id.into();
            self.mutable_state.replicate_event(event.clone())?;
            match event.event_type.as_ref() {
                Some(EventType::WorkflowExecutionStarted) => {
                    // Go: prefer ParentWorkflowDomainID (absent on older histories).
                    if let Some(id) = event.workflow_execution_started_event_attributes.as_ref().and_then(|a| a.parent_workflow_domain_id.clone()) {
                        self.mutable_state.execution_info["parent_domain_id"] = id.into();
                    }
                    self.mutable_state.replicate_workflow_execution_started_event(event);
                }
                Some(EventType::WorkflowExecutionSignaled) => self.mutable_state.replicate_workflow_execution_signaled_event(event),
                Some(EventType::WorkflowExecutionCancelRequested) => { self.mutable_state.replicate_workflow_execution_cancel_requested_event(event); }
                Some(EventType::SignalExternalWorkflowExecutionInitiated) => { self.mutable_state.replicate_signal_external_workflow_execution_initiated_event(event.event_id, event, ""); }
                Some(EventType::SignalExternalWorkflowExecutionFailed) => { self.mutable_state.replicate_signal_external_workflow_execution_failed_event(event); }
                Some(EventType::ExternalWorkflowExecutionSignaled) => { self.mutable_state.replicate_external_workflow_execution_signaled_event(event); }
                Some(EventType::DecisionTaskScheduled) => {
                    let timestamp = event.timestamp.unwrap_or_default();
                    self.mutable_state.replicate_decision_scheduled(event, event.version, timestamp, timestamp)
                        .ok_or(StateBuilderError::InvalidEvent { event_type: event.event_type, event_id: event.event_id })?;
                }
                Some(EventType::DecisionTaskStarted) => {
                    self.mutable_state.replicate_decision_started(event, event.version, event.timestamp.unwrap_or_default())
                        .ok_or(StateBuilderError::InvalidEvent { event_type: event.event_type, event_id: event.event_id })?;
                }
                Some(EventType::DecisionTaskCompleted) => {
                    if !self.mutable_state.replicate_decision_completed(event) { return Err(StateBuilderError::InvalidEvent { event_type: event.event_type, event_id: event.event_id }); }
                }
                Some(EventType::DecisionTaskTimedOut) => self.mutable_state.replicate_decision_timed_out(event),
                Some(EventType::DecisionTaskFailed) => self.mutable_state.fail_decision(true),
                Some(EventType::ActivityTaskScheduled) => {
                    self.mutable_state.replicate_activity_task_scheduled(events[0].event_id, event, String::new())
                        .ok_or(StateBuilderError::InvalidEvent { event_type: event.event_type, event_id: event.event_id })?;
                }
                Some(EventType::ActivityTaskStarted) => self.mutable_state.replicate_activity_task_started(event)?,
                Some(EventType::ActivityTaskCompleted) => self.mutable_state.replicate_activity_task_completed(event),
                Some(EventType::ActivityTaskFailed) => self.mutable_state.replicate_activity_task_failed(event),
                Some(EventType::ActivityTaskTimedOut) => self.mutable_state.replicate_activity_task_timed_out(event),
                Some(EventType::ActivityTaskCanceled) => self.mutable_state.replicate_activity_task_canceled(event),
                Some(EventType::ActivityTaskCancelRequested) => self.mutable_state.replicate_activity_task_cancel_requested(event),
                Some(EventType::TimerStarted) => { self.mutable_state.replicate_timer_started_event(event)?; }
                Some(EventType::TimerFired) => self.mutable_state.replicate_timer_fired_event(event)?,
                Some(EventType::TimerCanceled) => self.mutable_state.replicate_timer_canceled_event(event)?,
                Some(EventType::StartChildWorkflowExecutionInitiated) => { self.mutable_state.replicate_start_child_workflow_execution_initiated_event(event.event_id, event, "")?; }
                Some(EventType::ChildWorkflowExecutionStarted) => self.mutable_state.replicate_child_workflow_execution_started_event(event)?,
                Some(EventType::StartChildWorkflowExecutionFailed | EventType::ChildWorkflowExecutionCompleted | EventType::ChildWorkflowExecutionFailed | EventType::ChildWorkflowExecutionCanceled | EventType::ChildWorkflowExecutionTimedOut | EventType::ChildWorkflowExecutionTerminated) => self.mutable_state.replicate_child_workflow_terminal_event(event)?,
                Some(EventType::RequestCancelExternalWorkflowExecutionInitiated) => { self.mutable_state.replicate_request_cancel_external_workflow_execution_initiated_event(event.event_id, event, ""); }
                Some(EventType::ExternalWorkflowExecutionCancelRequested) => { self.mutable_state.replicate_external_workflow_execution_cancel_requested(event); }
                Some(EventType::RequestCancelExternalWorkflowExecutionFailed) => { self.mutable_state.replicate_request_cancel_external_workflow_execution_failed_event(event); }
                Some(EventType::MarkerRecorded) => { self.mutable_state.replicate_marker_recorded_event(event); }
                Some(EventType::UpsertWorkflowSearchAttributes) => { self.mutable_state.replicate_upsert_workflow_search_attributes_event(event); }
                Some(EventType::WorkflowExecutionCompleted | EventType::WorkflowExecutionFailed | EventType::WorkflowExecutionTimedOut | EventType::WorkflowExecutionCanceled | EventType::WorkflowExecutionTerminated | EventType::WorkflowExecutionContinuedAsNew) => { self.mutable_state.replicate_workflow_close_event(event.event_id, event); }
                // Go: no mutable state action is needed
                Some(EventType::RequestCancelActivityTaskFailed | EventType::CancelTimerFailed) => {}
                Some(EventType::SemaphoreAcquireInitiated) => self.replicate_semaphore_acquire_initiated(event)?,
                Some(EventType::SemaphoreAcquired) => self.replicate_semaphore_acquired(event)?,
                Some(EventType::SemaphoreReleased) => self.replicate_semaphore_released(event)?,
                // Go: BadRequestError "Unknown event type"
                _ => return Err(StateBuilderError::UnknownEventType),
            }
        }
        self.mutable_state.execution_info["last_first_event_id"] = first_event.event_id.into();
        self.mutable_state.execution_info["next_event_id"] = (last_event.event_id + 1).into();
        Ok(())
    }
}

/// Go `VersionHistory.AddOrUpdateItem`.
fn add_or_update_version_item(items: &mut Vec<cadence_api::types::VersionHistoryItem>, event_id: i64, version: i64) -> Result<(), StateBuilderError> {
    let item = cadence_api::types::VersionHistoryItem { event_id, version };
    match items.last_mut() {
        None => items.push(item),
        Some(last) if last.version > version => return Err(StateBuilderError::VersionHistoryItem),
        Some(last) if last.version == version => {
            if event_id < last.event_id { return Err(StateBuilderError::VersionHistoryItem); }
            last.event_id = event_id;
        }
        Some(last) => {
            if event_id <= last.event_id { return Err(StateBuilderError::VersionHistoryItem); }
            items.push(item);
        }
    }
    Ok(())
}

#[cfg(test)]
mod typed_replay_tests {
    use super::*;
    use cadence_api::{enums::EventType, service_types::WorkflowExecutionStartedEventAttributes, tasks::{ActivityTaskFailedEventAttributes, ActivityTaskStartedEventAttributes, DecisionTaskScheduledEventAttributes, DecisionTaskStartedEventAttributes, DecisionTaskTimedOutEventAttributes}, types::{VersionHistories, VersionHistory, TaskList}};

    fn state() -> TypedMutableState {
        let mut state = TypedMutableState::new();
        state.version_histories = Some(VersionHistories {
            current_version_history_index: 0,
            histories: vec![VersionHistory { branch_token: vec![1], items: vec![] }],
        });
        state
    }

    #[test]
    fn typed_replay_keeps_batch_ids_indexes_and_does_not_append_or_generate_tasks() {
        let mut state = state();
        let workflow = WorkflowExecution::new("wf", "run");
        let batch_prefix = HistoryEvent { event_id: 9, timestamp: Some(90), event_type: Some(EventType::RequestCancelActivityTaskFailed), version: 3, task_id: 40, ..Default::default() };
        let scheduled = HistoryEvent {
            event_id: 10, timestamp: Some(100), event_type: Some(EventType::ActivityTaskScheduled), version: 3, task_id: 41,
            activity_task_scheduled_event_attributes: Some(cadence_api::tasks::ActivityTaskScheduledEventAttributes {
                activity_id: "activity-a".into(), task_list: Some(TaskList { name: "activities".into(), ..Default::default() }), ..Default::default()
            }), ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "domain", "request", &workflow, &[batch_prefix, scheduled]).unwrap();
        let started = HistoryEvent {
            event_id: 11, timestamp: Some(110), event_type: Some(EventType::ActivityTaskStarted), version: 3, task_id: 42,
            activity_task_started_event_attributes: Some(ActivityTaskStartedEventAttributes { scheduled_event_id: 10, request_id: "activity-request".into(), ..Default::default() }),
            ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "domain", "request", &workflow, &[started]).unwrap();
        let activity = &state.pending_activity_info_ids[&10];
        assert_eq!(activity.scheduled_event_batch_id, 9);
        assert_eq!(activity.started_id, 11);
        assert_eq!(state.execution_info.last_event_task_id, 42);
        assert_eq!(state.execution_info.last_first_event_id, 11);
        assert_eq!(state.execution_info.next_event_id, 12);

        let failed = HistoryEvent {
            event_id: 12, timestamp: Some(120), event_type: Some(EventType::ActivityTaskFailed), version: 3, task_id: 43,
            activity_task_failed_event_attributes: Some(ActivityTaskFailedEventAttributes { scheduled_event_id: 10, started_event_id: 11, ..Default::default() }),
            ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "domain", "request", &workflow, &[failed]).unwrap();
        assert!(!state.pending_activity_info_ids.contains_key(&10));

        let retry_prefix = HistoryEvent { event_id: 13, timestamp: Some(130), event_type: Some(EventType::RequestCancelActivityTaskFailed), version: 3, task_id: 44, ..Default::default() };
        let retry_schedule = HistoryEvent {
            event_id: 14, timestamp: Some(140), event_type: Some(EventType::ActivityTaskScheduled), version: 3, task_id: 45,
            activity_task_scheduled_event_attributes: Some(cadence_api::tasks::ActivityTaskScheduledEventAttributes {
                activity_id: "activity-a".into(), task_list: Some(TaskList { name: "activities".into(), ..Default::default() }), ..Default::default()
            }), ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "domain", "request", &workflow, &[retry_prefix, retry_schedule]).unwrap();
        assert!(state.pending_activity_info_ids.contains_key(&14));
        assert_eq!(state.pending_activity_id_to_event_id.get("activity-a"), Some(&14));
        assert_eq!(state.pending_activity_info_ids[&14].scheduled_event_batch_id, 13);
        assert!(state.history_events.is_empty());
        assert!(state.transfer_tasks.is_empty());
        assert!(state.timer_tasks.is_empty());
    }

    #[test]
    fn typed_replay_marker_is_noop_and_upsert_merges_search_attributes() {
        let mut state = state();
        let marker = HistoryEvent { event_id: 1, version: 1, event_type: Some(EventType::MarkerRecorded), marker_recorded_event_attributes: Some(cadence_api::tasks::MarkerRecordedEventAttributes::default()), ..Default::default() };
        StateBuilder::apply_events_typed(&mut state, "d", "r", &WorkflowExecution::default(), &[marker]).unwrap();
        let upsert = HistoryEvent {
            event_id: 2, version: 1, event_type: Some(EventType::UpsertWorkflowSearchAttributes),
            upsert_workflow_search_attributes_event_attributes: Some(cadence_api::tasks::UpsertWorkflowSearchAttributesEventAttributes {
                search_attributes: Some(cadence_api::types::SearchAttributes { indexed_fields: Some(std::collections::HashMap::from([("CustomKeywordField".into(), b"value".to_vec())])) }), ..Default::default()
            }), ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "d", "r", &WorkflowExecution::default(), &[upsert]).unwrap();
        assert_eq!(state.execution_info.search_attributes.get("CustomKeywordField").unwrap(), b"value");
        assert!(state.insert_workflow_requests.is_empty());
    }

    #[test]
    fn typed_replay_continued_as_new_returns_independent_new_run_state() {
        let mut old = state();
        let can = HistoryEvent { event_id: 1, version: 7, event_type: Some(EventType::WorkflowExecutionContinuedAsNew),
            workflow_execution_continued_as_new_event_attributes: Some(cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes { new_execution_run_id: "new-run".into(), ..Default::default() }), ..Default::default() };
        let new_start = HistoryEvent { event_id: 1, version: 7, event_type: Some(EventType::WorkflowExecutionStarted),
            workflow_execution_started_event_attributes: Some(WorkflowExecutionStartedEventAttributes { continued_execution_run_id: "old-run".into(), request_id: "new-request".into(), ..Default::default() }), ..Default::default() };
        let workflow = WorkflowExecution::new("wf", "old-run");
        let new_execution = WorkflowExecution::new("wf", "new-run");
        let new = StateBuilder::apply_events_typed_with_new_run(&mut old, "d", "req", &workflow, &[can], Some(&new_execution), std::slice::from_ref(&new_start)).unwrap().unwrap();
        assert_eq!(old.execution_info.run_id, "");
        assert_eq!(old.execution_info.state, 2);
        assert_eq!(new.execution_info.run_id, "new-run");
        assert_eq!(new.execution_info.next_event_id, 2);
        assert!(new.insert_workflow_requests.is_empty());
        let mut changed_new = new.clone();
        changed_new.execution_info.search_attributes.insert("only-new".into(), b"value".to_vec());
        assert!(!old.execution_info.search_attributes.contains_key("only-new"));
        assert!(matches!(StateBuilder::apply_events_typed_with_new_run(&mut state(), "d", "req", &workflow,
            &[HistoryEvent { event_id: 1, version: 7, event_type: Some(EventType::WorkflowExecutionContinuedAsNew), workflow_execution_continued_as_new_event_attributes: Some(cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes { new_execution_run_id: "new-run".into(), ..Default::default() }), ..Default::default() }], Some(&new_execution),
            &[HistoryEvent { event_id: 1, version: 8, event_type: Some(EventType::WorkflowExecutionStarted), workflow_execution_started_event_attributes: Some(WorkflowExecutionStartedEventAttributes { continued_execution_run_id: "old-run".into(), ..Default::default() }), ..Default::default() }]), Err(StateBuilderError::NewRunBranchVersion)));
        assert!(matches!(StateBuilder::apply_events_typed_with_new_run(&mut state(), "d", "req", &workflow,
            &[HistoryEvent { event_id: 1, version: 7, event_type: Some(EventType::WorkflowExecutionContinuedAsNew), workflow_execution_continued_as_new_event_attributes: Some(cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes { new_execution_run_id: "different".into(), ..Default::default() }), ..Default::default() }], Some(&new_execution), &[new_start]), Err(StateBuilderError::NewRunLinkage)));
    }

    #[test]
    fn typed_replay_rejects_missing_timer_attributes_and_unknown_event_types() {
        let mut typed_state = state();
        let missing_attributes = HistoryEvent { event_type: Some(EventType::TimerStarted), ..Default::default() };
        assert!(matches!(StateBuilder::apply_events_typed(&mut typed_state, "d", "r", &WorkflowExecution::default(), &[missing_attributes]), Err(StateBuilderError::Typed(_))));

        let mut typed_state = state();
        let unknown = HistoryEvent { event_type: None, ..Default::default() };
        assert!(matches!(StateBuilder::apply_events_typed(&mut typed_state, "d", "r", &WorkflowExecution::default(), &[unknown]), Err(StateBuilderError::UnknownEventType)));
    }

    #[test]
    fn typed_replay_start_decision_timeout_retry_spans_batches_without_active_tasks() {
        let mut state = state();
        let workflow = WorkflowExecution::new("wf", "run");
        let start = HistoryEvent {
            event_id: 1, timestamp: Some(100), event_type: Some(EventType::WorkflowExecutionStarted), version: 7, task_id: 90,
            workflow_execution_started_event_attributes: Some(WorkflowExecutionStartedEventAttributes { request_id: "from-history".into(), ..Default::default() }),
            ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "domain-id", "request", &workflow, &[start]).unwrap();
        assert_eq!(state.execution_info.domain_id, "domain-id");
        assert_eq!(state.execution_info.create_request_id, "from-history");
        assert_eq!(state.execution_info.run_id, "run");

        let scheduled = HistoryEvent {
            event_id: 2, timestamp: Some(110), event_type: Some(EventType::DecisionTaskScheduled), version: 7, task_id: 91,
            decision_task_scheduled_event_attributes: Some(DecisionTaskScheduledEventAttributes {
                task_list: Some(TaskList { name: "decisions".into(), ..Default::default() }),
                start_to_close_timeout_seconds: Some(30), attempt: 2,
            }), ..Default::default()
        };
        let started = HistoryEvent {
            event_id: 3, timestamp: Some(120), event_type: Some(EventType::DecisionTaskStarted), version: 7, task_id: 92,
            decision_task_started_event_attributes: Some(DecisionTaskStartedEventAttributes { scheduled_event_id: 2, request_id: "decision-request".into(), ..Default::default() }),
            ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "domain-id", "request", &workflow, &[scheduled, started]).unwrap();
        assert_eq!(state.execution_info.decision_schedule_id, 2);
        assert_eq!(state.execution_info.decision_started_id, 3);
        assert_eq!(state.execution_info.decision_attempt, 0);

        let timeout = HistoryEvent {
            event_id: 4, timestamp: Some(130), event_type: Some(EventType::DecisionTaskTimedOut), version: 7, task_id: 93,
            decision_task_timed_out_event_attributes: Some(DecisionTaskTimedOutEventAttributes::default()),
            ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "domain-id", "request", &workflow, &[timeout]).unwrap();
        assert_eq!(state.execution_info.decision_attempt, 1);
        assert_eq!(state.execution_info.decision_schedule_id, 4); // transient retry uses the current next event ID
        assert!(state.history_events.is_empty());
        assert!(state.transfer_tasks.is_empty());
        assert!(state.timer_tasks.is_empty());
    }

    #[test]
    fn typed_replay_supports_semaphore_events() {
        use cadence_api::tasks::{SemaphoreAcquireInitiatedEventAttributes, SemaphoreAcquiredEventAttributes, SemaphoreReleasedEventAttributes};
        let mut state = state();
        let workflow = WorkflowExecution::new("wf", "run");
        let initiated = HistoryEvent {
            event_id: 5, timestamp: Some(1_000), event_type: Some(EventType::SemaphoreAcquireInitiated), version: 1,
            semaphore_acquire_initiated_event_attributes: Some(SemaphoreAcquireInitiatedEventAttributes {
                semaphore_name: "s".into(), wait_timeout_seconds: Some(2), decision_task_completed_event_id: 4,
            }),
            ..Default::default()
        };
        let acquired = HistoryEvent {
            event_id: 6, event_type: Some(EventType::SemaphoreAcquired), version: 1,
            semaphore_acquired_event_attributes: Some(SemaphoreAcquiredEventAttributes { token_id: 3, initiated_event_id: 5 }),
            ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "d", "r", &workflow, &[initiated, acquired]).unwrap();
        let info = state.semaphore_state.get_semaphore_info(5).unwrap();
        assert_eq!((info.token_id, info.acquire_deadline_unix_nanos), (3, 1_000 + 2_000_000_000));
        let released = HistoryEvent {
            event_id: 7, event_type: Some(EventType::SemaphoreReleased), version: 1,
            semaphore_released_event_attributes: Some(SemaphoreReleasedEventAttributes { token_id: 3, initiated_event_id: 5, decision_task_completed_event_id: 4 }),
            ..Default::default()
        };
        StateBuilder::apply_events_typed(&mut state, "d", "r", &workflow, &[released]).unwrap();
        assert!(state.semaphore_state.get_semaphore_info(5).is_none());
    }

    #[test]
    fn millis_to_nanos_scales_event_timestamps() {
        let e = HistoryEvent { timestamp: Some(1_800_000_000_000), ..Default::default() };
        assert_eq!(events_millis_to_nanos(&[e])[0].timestamp, Some(1_800_000_000_000_000_000));
    }
}
