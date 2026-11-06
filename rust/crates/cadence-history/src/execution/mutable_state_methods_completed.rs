//! `mutable_state_builder_methods_completed.go` on [`TypedMutableState`].
//! `GetCompletionEvent` needs the events cache and is not ported yet.

use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::{
    CompleteWorkflowExecutionDecisionAttributes, FailWorkflowExecutionDecisionAttributes,
    WorkflowExecutionCompletedEventAttributes, WorkflowExecutionFailedEventAttributes,
};

use super::mutable_state_builder::TypedMutableState;
use super::mutable_state_methods_common::{
    Result, CLOSE_STATUS_COMPLETED, CLOSE_STATUS_FAILED, WORKFLOW_STATE_COMPLETED,
};

impl TypedMutableState {
    /// Go `IsWorkflowCompleted`.
    pub fn is_workflow_completed(&self) -> bool {
        self.execution_info.state == WORKFLOW_STATE_COMPLETED
    }

    /// Go `AddCompletedWorkflowEvent`.
    pub fn add_completed_workflow_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        attributes: &CompleteWorkflowExecutionDecisionAttributes,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::WorkflowExecutionCompleted, |e| {
            e.workflow_execution_completed_event_attributes = Some(WorkflowExecutionCompletedEventAttributes {
                result: attributes.result.clone(),
                decision_task_completed_event_id: decision_completed_event_id,
            });
        })?;
        self.replicate_workflow_execution_completed_event(decision_completed_event_id, &event)?;
        Ok(event)
    }

    /// Go `ReplicateWorkflowExecutionCompletedEvent` (close tasks / cache not wired).
    pub fn replicate_workflow_execution_completed_event(
        &mut self,
        first_event_id: i64,
        _event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(WORKFLOW_STATE_COMPLETED, CLOSE_STATUS_COMPLETED)?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        Ok(())
    }

    /// Go `AddFailWorkflowEvent`.
    pub fn add_fail_workflow_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        attributes: &FailWorkflowExecutionDecisionAttributes,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::WorkflowExecutionFailed, |e| {
            e.workflow_execution_failed_event_attributes = Some(WorkflowExecutionFailedEventAttributes {
                reason: attributes.reason.clone(),
                details: attributes.details.clone(),
                decision_task_completed_event_id: decision_completed_event_id,
            });
        })?;
        self.replicate_workflow_execution_failed_event(decision_completed_event_id, &event)?;
        Ok(event)
    }

    /// Go `ReplicateWorkflowExecutionFailedEvent` (close tasks / cache not wired).
    pub fn replicate_workflow_execution_failed_event(
        &mut self,
        first_event_id: i64,
        _event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(WORKFLOW_STATE_COMPLETED, CLOSE_STATUS_FAILED)?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        Ok(())
    }
}
