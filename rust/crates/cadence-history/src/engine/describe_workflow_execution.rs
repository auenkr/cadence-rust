//! Port of engineimpl/describe_workflow_execution.go.
use cadence_api::{enums::*, service_types::{DescribeWorkflowExecutionResponse, HistoryEvent, WorkflowExecutionConfiguration}, tasks::{PendingActivityInfo, PendingChildExecutionInfo, PendingDecisionInfo}, types::{Memo, SearchAttributes, TaskList, WorkflowExecution, WorkflowType}};
use super::{common_util::validate_domain_uuid, deps::*, deps_ms::{ChildExecutionInfo, PendingDecision}, runtime::HistoryEngine, types::HistoryDescribeWorkflowExecutionRequest};

impl HistoryEngine {
    pub async fn describe_workflow_execution(&self, request: &HistoryDescribeWorkflowExecutionRequest) -> HResult<DescribeWorkflowExecutionResponse> {
        validate_domain_uuid(&request.domain_uuid)?;
        let execution = request.request.as_ref().and_then(|r| r.execution.clone()).unwrap_or_default();
        let (context, release) = self.execution_cache.get_or_create_workflow_execution(&request.domain_uuid, &execution).await?;
        let result = async {
            let state = context.load_workflow_execution().await?;
            if self.check_for_history_corruptions(&*state).await? {
                return Err(HistoryError::entity_not_exists("Workflow execution corrupted."));
            }
            create_describe_workflow_execution_response(&*state, &*self.domain_cache).await
        }.await;
        release(result.as_ref().err());
        result
    }
}

pub async fn create_describe_workflow_execution_response(state: &dyn MutableState, domains: &dyn DomainCache) -> HResult<DescribeWorkflowExecutionResponse> {
    let info = state.get_execution_info();
    let mut result = DescribeWorkflowExecutionResponse {
        execution_configuration: Some(WorkflowExecutionConfiguration {
            task_list: Some(map_decision_info_to_task_list(&info)),
            execution_start_to_close_timeout_seconds: Some(info.workflow_timeout),
            task_start_to_close_timeout_seconds: Some(info.decision_start_to_close_timeout),
        }), ..Default::default()
    };
    let start = state.get_start_event().await?;
    let completion = if info.state == WORKFLOW_STATE_COMPLETED { Some(state.get_completion_event().await?) } else { None };
    result.workflow_execution_info = Some(map_workflow_execution_info(&info, &start, domains,
        state.get_next_event_id() - FIRST_EVENT_ID, completion.as_ref())?);
    for activity in state.get_pending_activity_infos() {
        let scheduled = state.get_activity_scheduled_event(activity.schedule_id).await?;
        result.pending_activities.push(map_pending_activity_info(&activity, &scheduled));
    }
    let domain = state.get_domain_entry();
    for child in state.get_pending_child_execution_infos() {
        result.pending_children.push(map_pending_child_execution_info(&child, &domain, domains)?);
    }
    result.pending_decision = state.get_pending_decision().map(|d| map_pending_decision_info(&d));
    Ok(result)
}

pub fn map_decision_info_to_task_list(info: &WorkflowExecutionInfo) -> TaskList {
    TaskList { name: info.task_list.clone(), kind: Some(info.task_list_kind), ..Default::default() }
}

pub fn map_workflow_execution_info(info: &WorkflowExecutionInfo, start: &HistoryEvent, domains: &dyn DomainCache,
    history_length: i64, completion: Option<&HistoryEvent>) -> HResult<cadence_api::service_types::WorkflowExecutionInfo> {
    let backoff = start.workflow_execution_started_event_attributes.as_ref()
        .and_then(|a| a.first_decision_task_backoff_seconds).unwrap_or_default() as i64;
    let mut result = cadence_api::service_types::WorkflowExecutionInfo {
        execution: Some(WorkflowExecution { workflow_id: info.workflow_id.clone(), run_id: info.run_id.clone() }),
        task_list: Some(map_decision_info_to_task_list(info)),
        workflow_type: Some(WorkflowType { name: info.workflow_type_name.clone() }),
        start_time: Some(info.start_timestamp), history_length, auto_reset_points: info.auto_reset_points.clone(),
        memo: Some(Memo { fields: info.memo.clone() }), is_cron: !info.cron_schedule.is_empty(),
        update_time: Some(info.last_updated_timestamp), search_attributes: Some(SearchAttributes { indexed_fields: info.search_attributes.clone() }),
        partition_config: Some(info.partition_config.clone()), cron_overlap_policy: Some(info.cron_overlap_policy),
        active_cluster_selection_policy: info.active_cluster_selection_policy.clone(),
        execution_time: Some(info.start_timestamp.wrapping_add(backoff.wrapping_mul(1_000_000_000))),
        ..Default::default()
    };
    if !info.parent_run_id.is_empty() {
        result.parent_execution = Some(WorkflowExecution { workflow_id: info.parent_workflow_id.clone(), run_id: info.parent_run_id.clone() });
        result.parent_domain_id = Some(info.parent_domain_id.clone()); result.parent_initiated_id = Some(info.initiated_id);
        result.parent_domain = Some(domains.get_domain_name(&info.parent_domain_id)?);
    }
    if info.state == WORKFLOW_STATE_COMPLETED {
        result.close_status = match info.close_status {
            1 => Some(WorkflowExecutionCloseStatus::Completed), 2 => Some(WorkflowExecutionCloseStatus::Failed),
            3 => Some(WorkflowExecutionCloseStatus::Canceled), 4 => Some(WorkflowExecutionCloseStatus::Terminated),
            5 => Some(WorkflowExecutionCloseStatus::ContinuedAsNew), 6 => Some(WorkflowExecutionCloseStatus::TimedOut), _ => None,
        };
        result.close_time = completion.map(|e| e.timestamp.unwrap_or_default());
    }
    Ok(result)
}

pub fn map_pending_activity_info(info: &ActivityInfo, scheduled: &HistoryEvent) -> PendingActivityInfo {
    let state = if info.cancel_requested { PendingActivityState::CancelRequested }
        else if info.started_id != EMPTY_EVENT_ID { PendingActivityState::Started } else { PendingActivityState::Scheduled };
    let mut result = PendingActivityInfo {
        activity_id: info.activity_id.clone(), schedule_id: info.schedule_id, state: Some(state),
        activity_type: scheduled.activity_task_scheduled_event_attributes.as_ref().and_then(|a| a.activity_type.clone()),
        ..Default::default()
    };
    if info.last_heartbeat_updated_time > 0 {
        result.last_heartbeat_timestamp = Some(info.last_heartbeat_updated_time); result.heartbeat_details = info.details.clone();
    }
    if state == PendingActivityState::Scheduled { result.scheduled_timestamp = Some(info.scheduled_time); }
    else { result.last_started_timestamp = Some(info.started_time); }
    if info.has_retry_policy {
        result.attempt = info.attempt; result.expiration_timestamp = info.expiration_time;
        result.maximum_attempts = info.maximum_attempts;
        if !info.last_failure_reason.is_empty() {
            result.last_failure_reason = Some(info.last_failure_reason.clone()); result.last_failure_details = info.last_failure_details.clone();
            result.last_failure_options = Some(cadence_api::types::FailureOptions {
                failure_category: Some(info.last_failure_category), next_retry_interval_seconds: Some(info.last_retry_interval_seconds),
            });
        }
        result.last_worker_identity = info.last_worker_identity.clone(); result.started_worker_identity = info.started_identity.clone();
    }
    result
}

pub fn map_pending_child_execution_info(child: &ChildExecutionInfo, parent: &DomainCacheEntry, domains: &dyn DomainCache) -> HResult<PendingChildExecutionInfo> {
    let domain = if !child.domain_id.is_empty() {
        match domains.get_domain_name(&child.domain_id) {
            Ok(name) => name, Err(error) if error.is_entity_not_exists() => child.domain_id.clone(), Err(error) => return Err(error),
        }
    } else if !child.domain_name_deprecated.is_empty() { child.domain_name_deprecated.clone() } else { parent.info.name.clone() };
    Ok(PendingChildExecutionInfo { domain, workflow_id: child.started_workflow_id.clone(), run_id: child.started_run_id.clone(),
        workflow_type_name: child.workflow_type_name.clone(), initiated_id: child.initiated_id, parent_close_policy: Some(child.parent_close_policy) })
}

pub fn map_pending_decision_info(info: &PendingDecision) -> PendingDecisionInfo {
    let started = info.started_id != EMPTY_EVENT_ID;
    PendingDecisionInfo {
        state: Some(if started { PendingDecisionState::Started } else { PendingDecisionState::Scheduled }),
        scheduled_timestamp: Some(info.scheduled_timestamp), attempt: info.attempt,
        original_scheduled_timestamp: Some(info.original_scheduled_timestamp), schedule_id: info.schedule_id,
        started_timestamp: started.then_some(info.started_timestamp),
    }
}
