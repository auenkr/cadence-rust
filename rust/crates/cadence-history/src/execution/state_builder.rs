//! Event replay into mutable state, corresponding to Go's `state_builder.go`.

use cadence_api::{service_types::HistoryEvent, types::EventType};
use crate::mutable_state::{MutableStateBuilder, MutableStateError};
use super::{child_workflow::ChildWorkflowError, timer::TimerError};

#[derive(Debug, thiserror::Error)]
pub enum StateBuilderError {
    #[error(transparent)] State(#[from] MutableStateError),
    #[error(transparent)] Timer(#[from] TimerError),
    #[error(transparent)] Child(#[from] ChildWorkflowError),
    #[error("unsupported or malformed replicated event {event_type:?} ({event_id})")]
    InvalidEvent { event_type: Option<EventType>, event_id: i64 },
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StateBuilder { pub mutable_state: MutableStateBuilder }

impl StateBuilder {
    pub fn new(mutable_state: MutableStateBuilder) -> Self { Self { mutable_state } }
    pub fn apply_events(&mut self, events: &[HistoryEvent]) -> Result<(), StateBuilderError> {
        for event in events {
            self.mutable_state.replicate_event(event.clone())?;
            match event.event_type.as_ref() {
                Some(EventType::WorkflowExecutionStarted) => { self.mutable_state.replicate_workflow_execution_started_event(event); }
                Some(EventType::WorkflowExecutionSignaled) => self.mutable_state.replicate_workflow_execution_signaled_event(event),
                Some(EventType::WorkflowExecutionCancelRequested) => { self.mutable_state.replicate_workflow_execution_cancel_requested_event(event); }
                Some(EventType::SignalExternalWorkflowExecutionInitiated) => { self.mutable_state.replicate_signal_external_workflow_execution_initiated_event(event.event_id, event, ""); }
                Some(EventType::SignalExternalWorkflowExecutionFailed) => { self.mutable_state.replicate_signal_external_workflow_execution_failed_event(event); }
                Some(EventType::ExternalWorkflowExecutionSignaled) => { self.mutable_state.replicate_external_workflow_execution_signaled_event(event); }
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
                _ => {}
            }
        }
        Ok(())
    }
}
