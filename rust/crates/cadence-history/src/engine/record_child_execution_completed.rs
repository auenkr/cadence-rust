//! Port of engineimpl/record_child_execution_completed.go.
use super::{deps::*, runtime::*, types::*};
use cadence_api::enums::EventType;
impl HistoryEngine {
    pub async fn record_child_execution_completed(
        &self,
        request: &RecordChildExecutionCompletedRequest,
    ) -> HResult<()> {
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let domain = self
            .get_active_domain_by_workflow(
                &request.domain_uuid,
                &execution.workflow_id,
                &execution.run_id,
            )
            .await?;
        self.updater.update(&domain.info.id, &execution, false, self.time_source.now(), Box::new(|_, state| Box::pin(async move {
            if !state.is_workflow_execution_running() { return Err(HistoryError::entity_not_exists("workflow execution already completed")); }
            let initiated = request.initiated_id;
            let child = state.get_child_execution_info(initiated);
            if child.is_none() {
                if initiated >= state.get_next_event_id() {
                    self.metrics.stale_mutable_state("RecordChildExecutionCompleted");
                    tracing::error!(initiated_id = initiated, started_id = request.started_id, "Encounter stale mutable state in RecordChildExecutionCompleted");
                    return Err(HistoryError::StaleState);
                }
                return Err(HistoryError::entity_not_exists("Pending child execution not found."));
            }
            let child = child.unwrap();
            if child.started_id == EMPTY_EVENT_ID {
                self.metrics.stale_mutable_state("RecordChildExecutionCompleted");
                tracing::error!(initiated_id = initiated, started_id = request.started_id, "Encounter stale mutable state in RecordChildExecutionCompleted");
                return Err(HistoryError::StaleState);
            }
            let completed = request.completed_execution.clone().unwrap_or_default();
            if child.started_workflow_id != completed.workflow_id { return Err(HistoryError::entity_not_exists("Pending child execution workflowID mismatch.")); }
            let event = request.completion_event.clone().unwrap_or_default();
            match event.event_type {
                Some(EventType::WorkflowExecutionCompleted) => state.add_child_workflow_execution_completed_event(initiated, &completed, event.workflow_execution_completed_event_attributes.as_ref())?,
                Some(EventType::WorkflowExecutionFailed) => state.add_child_workflow_execution_failed_event(initiated, &completed, event.workflow_execution_failed_event_attributes.as_ref())?,
                Some(EventType::WorkflowExecutionCanceled) => state.add_child_workflow_execution_canceled_event(initiated, &completed, event.workflow_execution_canceled_event_attributes.as_ref())?,
                Some(EventType::WorkflowExecutionTerminated) => state.add_child_workflow_execution_terminated_event(initiated, &completed, event.workflow_execution_terminated_event_attributes.as_ref())?,
                Some(EventType::WorkflowExecutionTimedOut) => state.add_child_workflow_execution_timed_out_event(initiated, &completed, event.workflow_execution_timed_out_event_attributes.as_ref())?,
                _ => {}
            }
            Ok(UpdateAction { create_decision: true, ..Default::default() })
        }))).await
    }
}
