//! Port of engineimpl/describe_workflow_execution.go.
use cadence_api::enums::*;
use cadence_api::service_types::{DescribeWorkflowExecutionResponse, WorkflowExecutionConfiguration, WorkflowExecutionInfo as ApiWorkflowExecutionInfo};
use cadence_api::tasks::{PendingActivityInfo, PendingChildExecutionInfo, PendingDecisionInfo};
use cadence_api::types::{FailureOptions, Memo, SearchAttributes, TaskList, WorkflowType};
use super::{common_util::validate_domain_uuid, deps::*, deps_ms::PendingChildInfo, runtime::*, types::*};
use crate::mutable_state::DecisionInfo;

impl HistoryEngine {
    /// `DescribeWorkflowExecution`
    pub async fn describe_workflow_execution(&self, request: &HistoryDescribeWorkflowExecutionRequest) -> HResult<DescribeWorkflowExecutionResponse> {
        validate_describe_workflow_execution_request(request)?;
        let domain_id = request.domain_uuid.clone();
        let wf_execution = request.request.as_ref().and_then(|r| r.execution.clone()).unwrap_or_default();
        let (wf_context, release) = self.execution_cache.get_or_create_workflow_execution(&domain_id, &wf_execution).await?;
        let result = async {
            let mutable_state = wf_context.load_workflow_execution().await?;
            // If history is corrupted, return an error to the end user
            if self.check_for_history_corruptions(&*mutable_state).await? {
                return Err(HistoryError::entity_not_exists("Workflow execution corrupted."));
            }
            create_describe_workflow_execution_response(&*mutable_state, &*self.domain_cache).await
        }.await;
        release(result.as_ref().err());
        result
    }
}

/// `validateDescribeWorkflowExecutionRequest`
fn validate_describe_workflow_execution_request(request: &HistoryDescribeWorkflowExecutionRequest) -> HResult<()> {
    validate_domain_uuid(&request.domain_uuid)
}

/// `createDescribeWorkflowExecutionResponse`
pub async fn create_describe_workflow_execution_response(mutable_state: &dyn MutableState, domain_cache: &dyn DomainCache) -> HResult<DescribeWorkflowExecutionResponse> {
    let execution_info = mutable_state.get_execution_info();
    let mut result = DescribeWorkflowExecutionResponse {
        execution_configuration: Some(map_workflow_execution_configuration(&execution_info)),
        ..Default::default()
    };
    // TODO: we need to consider adding execution time to mutable state
    // For now execution time will be calculated based on start time and cron schedule/retry policy
    // each time DescribeWorkflowExecution is called.
    let start_event = mutable_state.get_start_event().await?;
    let mut completion_event = None;
    if execution_info.state == WORKFLOW_STATE_COMPLETED {
        completion_event = Some(mutable_state.get_completion_event().await?);
    }
    let history_length = mutable_state.get_next_event_id() - FIRST_EVENT_ID;
    result.workflow_execution_info = Some(map_workflow_execution_info(&execution_info, &start_event, domain_cache, history_length, completion_event.as_ref())?);

    for ai in mutable_state.get_pending_activity_infos() {
        let scheduled_event = mutable_state.get_activity_scheduled_event(ai.schedule_id).await?;
        result.pending_activities.push(map_pending_activity_info(&ai, &scheduled_event));
    }
    let domain_entry = mutable_state.get_domain_entry();
    for child in mutable_state.get_pending_child_execution_infos() {
        result.pending_children.push(map_pending_child_execution_info(&child, &domain_entry, domain_cache)?);
    }
    if let Some(di) = mutable_state.get_pending_decision() {
        result.pending_decision = Some(map_pending_decision_info(&di));
    }
    Ok(result)
}

fn map_workflow_execution_configuration(info: &WorkflowExecutionInfo) -> WorkflowExecutionConfiguration {
    WorkflowExecutionConfiguration {
        task_list: Some(map_decision_info_to_task_list(info)),
        execution_start_to_close_timeout_seconds: Some(info.workflow_timeout),
        task_start_to_close_timeout_seconds: Some(info.decision_start_to_close_timeout),
    }
}

fn map_workflow_execution_info(
    info: &WorkflowExecutionInfo,
    start_event: &HistoryEvent,
    domain_cache: &dyn DomainCache,
    history_length: i64,
    completion_event: Option<&HistoryEvent>,
) -> HResult<ApiWorkflowExecutionInfo> {
    let mut result = ApiWorkflowExecutionInfo {
        execution: Some(WorkflowExecution { workflow_id: info.workflow_id.clone(), run_id: info.run_id.clone() }),
        task_list: Some(map_decision_info_to_task_list(info)),
        workflow_type: Some(WorkflowType { name: info.workflow_type_name.clone() }),
        start_time: Some(info.start_timestamp),
        history_length,
        auto_reset_points: info.auto_reset_points.clone(),
        memo: Some(Memo { fields: info.memo.clone() }),
        is_cron: !info.cron_schedule.is_empty(),
        update_time: Some(info.last_updated_timestamp),
        search_attributes: Some(SearchAttributes { indexed_fields: info.search_attributes.clone() }),
        partition_config: info.partition_config.clone(),
        cron_overlap_policy: info.cron_overlap_policy,
        active_cluster_selection_policy: info.active_cluster_selection_policy.clone(),
        ..Default::default()
    };
    let backoff_seconds = start_event.workflow_execution_started_event_attributes.as_ref()
        .and_then(|a| a.first_decision_task_backoff_seconds).unwrap_or(0) as i64;
    result.execution_time = Some(info.start_timestamp + backoff_seconds * 1_000_000_000);

    if !info.parent_run_id.is_empty() {
        result.parent_execution = Some(WorkflowExecution { workflow_id: info.parent_workflow_id.clone(), run_id: info.parent_run_id.clone() });
        result.parent_domain_id = Some(info.parent_domain_id.clone());
        result.parent_initiated_id = Some(info.initiated_id);
        result.parent_domain = Some(domain_cache.get_domain_name(&info.parent_domain_id)?);
    }
    if info.state == WORKFLOW_STATE_COMPLETED {
        result.close_status = to_internal_workflow_execution_close_status(info.close_status);
        if let Some(ev) = completion_event {
            result.close_time = ev.timestamp;
        }
    }
    Ok(result)
}

/// `persistence.ToInternalWorkflowExecutionCloseStatus`
fn to_internal_workflow_execution_close_status(status: i32) -> Option<WorkflowExecutionCloseStatus> {
    match status {
        WORKFLOW_CLOSE_STATUS_COMPLETED => Some(WorkflowExecutionCloseStatus::Completed),
        WORKFLOW_CLOSE_STATUS_FAILED => Some(WorkflowExecutionCloseStatus::Failed),
        WORKFLOW_CLOSE_STATUS_CANCELED => Some(WorkflowExecutionCloseStatus::Canceled),
        WORKFLOW_CLOSE_STATUS_TERMINATED => Some(WorkflowExecutionCloseStatus::Terminated),
        WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW => Some(WorkflowExecutionCloseStatus::ContinuedAsNew),
        WORKFLOW_CLOSE_STATUS_TIMED_OUT => Some(WorkflowExecutionCloseStatus::TimedOut),
        _ => None,
    }
}

fn map_pending_activity_info(ai: &ActivityInfo, scheduled_event: &HistoryEvent) -> PendingActivityInfo {
    let mut p = PendingActivityInfo { activity_id: ai.activity_id.clone(), schedule_id: ai.schedule_id, ..Default::default() };
    let mut state = PendingActivityState::Scheduled;
    if ai.cancel_requested {
        state = PendingActivityState::CancelRequested;
    } else if ai.started_id != EMPTY_EVENT_ID {
        state = PendingActivityState::Started;
    }
    p.state = Some(state);
    let last_heartbeat = ai.last_heart_beat_updated_time;
    if last_heartbeat > 0 {
        p.last_heartbeat_timestamp = Some(last_heartbeat);
        p.heartbeat_details = ai.details.clone();
    }
    p.activity_type = scheduled_event.activity_task_scheduled_event_attributes.as_ref().and_then(|a| a.activity_type.clone());
    if state == PendingActivityState::Scheduled {
        p.scheduled_timestamp = Some(ai.scheduled_time);
    } else {
        p.last_started_timestamp = Some(ai.started_time);
    }
    if ai.has_retry_policy {
        p.attempt = ai.attempt;
        if ai.expiration_time != 0 {
            p.expiration_timestamp = Some(ai.expiration_time);
        }
        if ai.maximum_attempts != 0 {
            p.maximum_attempts = ai.maximum_attempts;
        }
        if !ai.last_failure_reason.is_empty() {
            p.last_failure_reason = Some(ai.last_failure_reason.clone());
            p.last_failure_details = ai.last_failure_details.clone();
            p.last_failure_options = Some(FailureOptions {
                failure_category: ai.last_failure_category,
                next_retry_interval_seconds: Some(ai.last_retry_interval_seconds),
            });
        }
        if !ai.last_worker_identity.is_empty() {
            p.last_worker_identity = ai.last_worker_identity.clone();
        }
        if !ai.started_identity.is_empty() {
            p.started_worker_identity = ai.started_identity.clone();
        }
    }
    p
}

fn map_pending_child_execution_info(child: &PendingChildInfo, domain_entry: &DomainCacheEntry, domain_cache: &dyn DomainCache) -> HResult<PendingChildExecutionInfo> {
    // execution.GetChildExecutionDomainName
    let child_domain_name = if !child.domain_id.is_empty() {
        domain_cache.get_domain_name(&child.domain_id)
    } else {
        Ok(domain_entry.get_info().name.clone())
    };
    let child_domain_name = match child_domain_name {
        Ok(n) => n,
        Err(err) if err.is_entity_not_exists() => {
            // child domain already deleted, instead of failing the request,
            // return domainID instead since this field is only for information purpose
            child.domain_id.clone()
        }
        Err(err) => return Err(err),
    };
    Ok(PendingChildExecutionInfo {
        domain: child_domain_name,
        workflow_id: child.started_workflow_id.clone(),
        run_id: child.started_run_id.clone(),
        workflow_type_name: child.workflow_type_name.clone(),
        initiated_id: child.initiated_id,
        parent_close_policy: child.parent_close_policy,
    })
}

fn map_pending_decision_info(di: &DecisionInfo) -> PendingDecisionInfo {
    let mut pending = PendingDecisionInfo {
        state: Some(PendingDecisionState::Scheduled),
        scheduled_timestamp: Some(di.schedule_timestamp),
        attempt: di.attempt,
        original_scheduled_timestamp: Some(di.original_schedule_timestamp),
        schedule_id: di.schedule_id,
        ..Default::default()
    };
    if di.started_id != EMPTY_EVENT_ID {
        pending.state = Some(PendingDecisionState::Started);
        pending.started_timestamp = Some(di.started_timestamp);
    }
    pending
}

fn map_decision_info_to_task_list(info: &WorkflowExecutionInfo) -> TaskList {
    TaskList { name: info.task_list.clone(), kind: info.task_list_kind, ..Default::default() }
}
