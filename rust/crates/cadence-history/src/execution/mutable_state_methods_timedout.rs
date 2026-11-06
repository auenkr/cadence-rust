//! `mutable_state_builder_methods_timedout.go` on [`TypedMutableState`].

use cadence_api::enums::{EventType, TimeoutType};
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::WorkflowExecutionTimedOutEventAttributes;

use super::mutable_state_builder::TypedMutableState;
use super::mutable_state_methods_common::{
    Result, CLOSE_STATUS_TIMED_OUT, WORKFLOW_STATE_COMPLETED,
};

impl TypedMutableState {
    /// Go `AddTimeoutWorkflowEvent`.
    pub fn add_timeout_workflow_event(
        &mut self,
        timestamp_unix_nanos: i64,
        first_event_id: i64,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::WorkflowExecutionTimedOut, |e| {
            e.workflow_execution_timed_out_event_attributes = Some(WorkflowExecutionTimedOutEventAttributes {
                timeout_type: Some(TimeoutType::StartToClose),
            });
        })?;
        self.replicate_workflow_execution_timedout_event(first_event_id, &event)?;
        Ok(event)
    }

    /// Go `ReplicateWorkflowExecutionTimedoutEvent`. Close-task generation
    /// (`taskGenerator.GenerateWorkflowCloseTasks`) and `writeEventToCache`
    /// are not wired yet.
    pub fn replicate_workflow_execution_timedout_event(
        &mut self,
        first_event_id: i64,
        _event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(WORKFLOW_STATE_COMPLETED, CLOSE_STATUS_TIMED_OUT)?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        Ok(())
    }
}
