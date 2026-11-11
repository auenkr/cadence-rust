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

    /// Go `ReplicateWorkflowExecutionCompletedEvent` (close tasks generated; Go `writeEventToCache` is perf-only, the executor falls back to the history reader).
    pub fn replicate_workflow_execution_completed_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(WORKFLOW_STATE_COMPLETED, CLOSE_STATUS_COMPLETED)?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        // Go: taskGenerator.GenerateWorkflowCloseTasks (close tasks generated; Go `writeEventToCache` is perf-only, the executor falls back to the history reader)
        self.generate_workflow_close_tasks(event)
    }

    /// Go `AddWorkflowExecutionTerminatedEvent` (logging/metrics not wired).
    pub fn add_workflow_execution_terminated_event(
        &mut self,
        timestamp_unix_nanos: i64,
        first_event_id: i64,
        reason: &str,
        details: Vec<u8>,
        identity: &str,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::WorkflowExecutionTerminated, |e| {
            e.workflow_execution_terminated_event_attributes = Some(
                cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes {
                    reason: reason.to_string(),
                    details,
                    identity: identity.to_string(),
                },
            );
        })?;
        self.replicate_workflow_execution_terminated_event(first_event_id, &event)?;
        Ok(event)
    }

    /// Go `ReplicateWorkflowExecutionTerminatedEvent` (close tasks generated; Go `writeEventToCache` is perf-only, the executor falls back to the history reader).
    pub fn replicate_workflow_execution_terminated_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(WORKFLOW_STATE_COMPLETED, super::mutable_state_methods_common::CLOSE_STATUS_TERMINATED)?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        self.generate_workflow_close_tasks(event)
    }

    /// Go `ReplicateWorkflowExecutionContinuedAsNewEvent` (writeEventToCache not
    /// wired; `AddContinueAsNewEvent` itself is not ported).
    pub fn replicate_workflow_execution_continued_as_new_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(
            WORKFLOW_STATE_COMPLETED,
            super::mutable_state_methods_common::CLOSE_STATUS_CONTINUED_AS_NEW,
        )?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        self.generate_workflow_close_tasks(event)
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

    /// Go `ReplicateWorkflowExecutionFailedEvent` (close tasks generated; Go `writeEventToCache` is perf-only, the executor falls back to the history reader).
    pub fn replicate_workflow_execution_failed_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(WORKFLOW_STATE_COMPLETED, CLOSE_STATUS_FAILED)?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        // Go: taskGenerator.GenerateWorkflowCloseTasks (close tasks generated; Go `writeEventToCache` is perf-only, the executor falls back to the history reader)
        self.generate_workflow_close_tasks(event)
    }
}

#[cfg(test)]
mod close_task_tests {
    use super::super::mutable_state_builder::TypedMutableState;
    use super::super::mutable_state_task_generator::TaskKind;
    use cadence_api::service_types::HistoryEvent;

    fn assert_close_tasks(ms: &TypedMutableState) {
        assert!(ms.transfer_tasks.iter().any(|t| matches!(t.kind, TaskKind::CloseExecution)));
        assert!(ms.timer_tasks.iter().any(|t| matches!(t.kind, TaskKind::DeleteHistoryEvent)));
    }

    fn ev() -> HistoryEvent {
        HistoryEvent { timestamp: Some(1), ..Default::default() }
    }

    #[test]
    fn replicate_completed_generates_close_tasks() {
        let mut ms = TypedMutableState::new();
        ms.execution_info.state = 1; // running
        ms.replicate_workflow_execution_completed_event(3, &ev()).unwrap();
        assert_close_tasks(&ms);
    }

    #[test]
    fn replicate_failed_generates_close_tasks() {
        let mut ms = TypedMutableState::new();
        ms.execution_info.state = 1; // running
        ms.replicate_workflow_execution_failed_event(3, &ev()).unwrap();
        assert_close_tasks(&ms);
    }

    #[test]
    fn replicate_terminated_generates_close_tasks() {
        let mut ms = TypedMutableState::new();
        ms.execution_info.state = 1; // running
        ms.replicate_workflow_execution_terminated_event(3, &ev()).unwrap();
        assert_close_tasks(&ms);
    }

    #[test]
    fn replicate_canceled_generates_close_tasks() {
        let mut ms = TypedMutableState::new();
        ms.execution_info.state = 1; // running
        ms.replicate_workflow_execution_canceled_event(3, &ev()).unwrap();
        assert_close_tasks(&ms);
    }

    #[test]
    fn replicate_timedout_generates_close_tasks() {
        let mut ms = TypedMutableState::new();
        ms.execution_info.state = 1; // running
        ms.replicate_workflow_execution_timedout_event(3, &ev()).unwrap();
        assert_close_tasks(&ms);
    }
}
