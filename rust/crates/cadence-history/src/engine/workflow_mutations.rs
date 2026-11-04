//! Port of engineimpl/{remove_signal_mutable_state,reset_sticky_tasklist,
//! request_cancel_workflow_execution,terminate_workflow_execution}.go.
use std::sync::Arc;
use cadence_api::{errors::{CancellationAlreadyRequestedError, WorkflowExecutionAlreadyCompletedError}, types::WorkflowExecution};
use super::{common_util::validate_domain_uuid, deps::*, runtime::*, types::*};
pub fn already_completed() -> HistoryError {
    WorkflowExecutionAlreadyCompletedError { message: "workflow execution already completed".into() }.into()
}
impl HistoryEngine {
    pub async fn get_active_domain_by_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<Arc<DomainCacheEntry>> {
        let active = self.active_cluster_manager.active_cluster_by_workflow(domain_id, workflow_id, run_id).await?;
        let domain = self.domain_cache.get_domain_by_id(domain_id)?;
        let current = self.cluster_metadata.get_current_cluster_name();
        if active == current { Ok(domain) } else { Err(domain.new_domain_not_active_error(&current, &active)) }
    }
    pub fn new_domain_not_active_error(&self, domain: &DomainCacheEntry, version: i64) -> HistoryError {
        let active = self.cluster_metadata.cluster_name_for_failover_version(version).unwrap_or_else(|_| "_unknown_".into());
        domain.new_domain_not_active_error(&self.current_cluster_name, &active)
    }
    pub async fn remove_signal_mutable_state(&self, request: &RemoveSignalMutableStateRequest) -> HResult<()> {
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let domain = self.get_active_domain_by_workflow(&request.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        let request_id = request.request_id.clone();
        self.updater.update(&domain.info.id, &execution, false, self.time_source.now(), Box::new(move |_, state| {
            let request_id = request_id.clone();
            Box::pin(async move {
                if !state.is_workflow_execution_running() { return Err(HistoryError::entity_not_exists("workflow execution already completed")); }
                state.delete_signal_requested(&request_id);
                Ok(UpdateAction::default())
            })
        })).await
    }
    pub async fn reset_sticky_task_list(&self, request: &HistoryResetStickyTaskListRequest) -> HResult<HistoryResetStickyTaskListResponse> {
        validate_domain_uuid(&request.domain_uuid)?;
        let execution = request.execution.clone().unwrap_or_default();
        self.updater.update(&request.domain_uuid, &execution, false, self.time_source.now(), Box::new(|_, state| Box::pin(async move {
            if !state.is_workflow_execution_running() { return Err(already_completed()); }
            state.clear_stickyness();
            Ok(UpdateAction::default())
        }))).await?;
        Ok(HistoryResetStickyTaskListResponse::default())
    }
    pub async fn request_cancel_workflow_execution(&self, req: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()> {
        let request = req.cancel_request.clone().unwrap_or_default();
        let mut execution = request.workflow_execution.clone().unwrap_or_default();
        if !request.first_execution_run_id.is_empty() { execution.run_id.clear(); }
        let domain = self.get_active_domain_by_workflow(&req.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        let req = req.clone();
        self.updater.update(&domain.info.id, &execution, true, self.time_source.now(), Box::new(move |_, state| {
            let req = req.clone(); let request = request.clone();
            Box::pin(async move {
                let (cancel_requested, cancel_id) = state.is_cancel_requested();
                if !state.is_workflow_execution_running() {
                    if cancel_requested && state.get_execution_info().close_status == WORKFLOW_CLOSE_STATUS_CANCELED
                        && !request.request_id.is_empty() && request.request_id == cancel_id {
                        return Ok(UpdateAction { noop: true, ..Default::default() });
                    }
                    return Err(already_completed());
                }
                check_execution(&*state, &request.first_execution_run_id, req.child_workflow_only, req.external_workflow_execution.as_ref()).await?;
                if cancel_requested {
                    if !request.request_id.is_empty() && request.request_id == cancel_id {
                        return Ok(UpdateAction { create_decision: true, ..Default::default() });
                    }
                    return Err(CancellationAlreadyRequestedError { message: "cancellation already requested for this workflow execution".into() }.into());
                }
                state.add_workflow_execution_cancel_requested_event(&request.cause, &req)
                    .map_err(|_| HistoryError::internal("Unable to cancel workflow execution."))?;
                Ok(UpdateAction { create_decision: true, ..Default::default() })
            })
        })).await
    }
    pub async fn terminate_workflow_execution(&self, req: &HistoryTerminateWorkflowExecutionRequest) -> HResult<()> {
        let request = req.terminate_request.clone().unwrap_or_default();
        let mut execution = request.workflow_execution.clone().unwrap_or_default();
        if !request.first_execution_run_id.is_empty() { execution.run_id.clear(); }
        let domain = self.get_active_domain_by_workflow(&req.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        let req = req.clone();
        self.updater.update(&domain.info.id, &execution, true, self.time_source.now(), Box::new(move |_, state| {
            let req = req.clone(); let request = request.clone();
            Box::pin(async move {
                if !state.is_workflow_execution_running() { return Err(already_completed()); }
                check_execution(&*state, &request.first_execution_run_id, req.child_workflow_only, req.external_workflow_execution.as_ref()).await?;
                state.terminate_workflow(state.get_next_event_id(), &request.reason, &request.details, &request.identity)?;
                Ok(UpdateAction::default())
            })
        })).await
    }
}
async fn check_execution(state: &dyn MutableState, first_run_id: &str, child_only: bool, parent: Option<&WorkflowExecution>) -> HResult<()> {
    let info = state.get_execution_info();
    if !first_run_id.is_empty() {
        let mut first = info.first_execution_run_id;
        if first.is_empty() {
            // Backwards compatibility with workflows created on <= v0.25.0.
            first = state.get_start_event().await?.workflow_execution_started_event_attributes
                .map(|a| a.first_execution_run_id).unwrap_or_default();
        }
        if first_run_id != first { return Err(HistoryError::entity_not_exists("Workflow execution not found")); }
    }
    if child_only {
        let parent = parent.cloned().unwrap_or_default();
        if parent.workflow_id != info.parent_workflow_id || parent.run_id != info.parent_run_id {
            return Err(HistoryError::entity_not_exists("workflow parent does not match"));
        }
    }
    Ok(())
}
pub fn create_shard_name_from_shard_id(shard_id: i32) -> String { format!("history-engine-{shard_id}") }
pub fn get_schedule_id(activity_id: &str, state: &dyn MutableState) -> HResult<i64> {
    if activity_id.is_empty() { return Err(HistoryError::bad_request("Neither ActivityID nor ScheduleID is provided")); }
    state.get_activity_by_activity_id(activity_id).map(|a| a.schedule_id)
        .ok_or_else(|| HistoryError::bad_request("Cannot locate Activity ScheduleID"))
}
