//! Event replay into mutable state, corresponding to Go's `state_builder.go`.

use cadence_api::{enums::EventType, service_types::HistoryEvent};
use crate::mutable_state::{MutableStateBuilder, MutableStateError};
use super::{child_workflow::ChildWorkflowError, timer::TimerError};

#[derive(Debug, thiserror::Error)]
pub enum StateBuilderError {
    #[error(transparent)] State(#[from] MutableStateError),
    #[error(transparent)] Timer(#[from] TimerError),
    #[error(transparent)] Activity(#[from] crate::mutable_state_activity::ActivityTransitionError),
    #[error(transparent)] Child(#[from] ChildWorkflowError),
    #[error("encounter history size being zero")] HistorySizeZero,
    #[error("version histories are missing")] MissingVersionHistories,
    #[error("invalid version history item")] VersionHistoryItem,
    #[error("Unknown event type")] UnknownEventType,
    #[error("unsupported or malformed replicated event {event_type:?} ({event_id})")]
    InvalidEvent { event_type: Option<EventType>, event_id: i64 },
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StateBuilder { pub mutable_state: MutableStateBuilder }

impl StateBuilder {
    pub fn new(mutable_state: MutableStateBuilder) -> Self { Self { mutable_state } }
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
                Some(EventType::WorkflowExecutionStarted) => { self.mutable_state.replicate_workflow_execution_started_event(event); }
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
                // GAP: Semaphore* replication needs cadence-api attribute types (missing); see memory.
                Some(EventType::SemaphoreAcquireInitiated | EventType::SemaphoreAcquired | EventType::SemaphoreReleased) => {}
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
