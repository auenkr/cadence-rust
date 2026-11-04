//! Validation and reuse policy from engineimpl/start_workflow_execution.go.
//! TODO(link): start transaction orchestration and persistence adapters.
use cadence_api::{errors::WorkflowExecutionAlreadyStartedError, types::StartWorkflowExecutionRequest};
use super::{common_util::is_valid_id_length, deps::{HResult, HistoryError}};

/// TODO(link): engine configuration and metrics adapter. Lengths count UTF-8 bytes, as Go does.
pub trait StartValidationConfig {
    fn max_id_length_warn_limit(&self) -> usize;
    fn domain_name_max_length(&self, domain: &str) -> usize;
    fn workflow_id_max_length(&self, domain: &str) -> usize;
    fn task_list_name_max_length(&self, domain: &str) -> usize;
    fn workflow_type_max_length(&self, domain: &str) -> usize;
    fn id_length_warning(&self, domain: &str, id_type: &str, value: &str);
    fn decision_timeout_overridden(&self, domain: &str);
}

pub fn validate_start_workflow_execution_request(
    request: &StartWorkflowExecutionRequest,
    config: &dyn StartValidationConfig,
) -> HResult<()> {
    if request.request_id.is_empty() { return Err(HistoryError::bad_request("Missing request ID.")); }
    if request.execution_start_to_close_timeout_seconds.unwrap_or_default() <= 0 {
        return Err(HistoryError::bad_request("Missing or invalid ExecutionStartToCloseTimeoutSeconds."));
    }
    if request.task_start_to_close_timeout_seconds.unwrap_or_default() <= 0 {
        return Err(HistoryError::bad_request("Missing or invalid TaskStartToCloseTimeoutSeconds."));
    }
    let task_list = request.task_list.as_ref().map(|t| t.name.as_str()).unwrap_or_default();
    if task_list.is_empty() { return Err(HistoryError::bad_request("Missing Tasklist.")); }
    let workflow_type = request.workflow_type.as_ref().map(|t| t.name.as_str()).unwrap_or_default();
    if workflow_type.is_empty() { return Err(HistoryError::bad_request("Missing WorkflowType.")); }
    let domain = &request.domain;
    for (value, limit, id_type, message) in [
        (domain.as_str(), config.domain_name_max_length(domain), "domain-name", "Domain exceeds length limit."),
        (request.workflow_id.as_str(), config.workflow_id_max_length(domain), "workflow-id", "WorkflowId exceeds length limit."),
        (task_list, config.task_list_name_max_length(domain), "task-list-name", "TaskList exceeds length limit."),
        (workflow_type, config.workflow_type_max_length(domain), "workflow-type", "WorkflowType exceeds length limit."),
    ] {
        let (valid, warn) = is_valid_id_length(value, config.max_id_length_warn_limit(), limit);
        if warn { config.id_length_warning(domain, id_type, value); }
        if !valid { return Err(HistoryError::bad_request(message)); }
    }
    cadence_api::validation::validate_retry_policy(request.retry_policy.as_ref())?;
    Ok(())
}

pub fn override_task_start_to_close_timeout_seconds(
    domain: &str,
    request: &mut StartWorkflowExecutionRequest,
    max_decision_start_to_close_seconds: i32,
    metrics: &dyn StartValidationConfig,
) {
    let original = request.task_start_to_close_timeout_seconds.unwrap_or_default();
    let timeout = original.min(max_decision_start_to_close_seconds)
        .min(request.execution_start_to_close_timeout_seconds.unwrap_or_default());
    if timeout != original {
        request.task_start_to_close_timeout_seconds = Some(timeout);
        metrics.decision_timeout_overridden(domain);
    }
}

/// The policy is an integer to retain Go's unknown-value error path.
pub fn apply_workflow_id_reuse_policy(
    previous_request_id: &str, previous_run_id: &str, previous_state: i32,
    previous_close_state: i32, workflow_id: &str, policy: i32,
) -> HResult<()> {
    let already_started = |message: String| -> HistoryError {
        WorkflowExecutionAlreadyStartedError {
            message, start_request_id: previous_request_id.into(), run_id: previous_run_id.into(),
        }.into()
    };
    match previous_state {
        0 | 1 => return Err(already_started(format!("Workflow execution is already running. WorkflowId: {workflow_id}, RunId: {previous_run_id}."))),
        2 => {},
        5 => return Ok(()), // corrupted executions ignore the reuse policy
        other => return Err(HistoryError::internal(format!("Failed to process workflow, workflow has invalid state: {other}."))),
    }
    match policy {
        0 => {
            if !matches!(previous_close_state, 2 | 3 | 4 | 6) {
                return Err(already_started(format!("Workflow execution already finished successfully. WorkflowId: {workflow_id}, RunId: {previous_run_id}. Workflow ID reuse policy: allow duplicate workflow ID if last run failed.")));
            }
        }
        1 | 3 => {},
        2 => return Err(already_started(format!("Workflow execution already finished. WorkflowId: {workflow_id}, RunId: {previous_run_id}. Workflow ID reuse policy: reject duplicate workflow ID."))),
        _ => return Err(HistoryError::internal("Failed to process start workflow reuse policy.")),
    }
    Ok(())
}

pub fn should_terminate_and_start(policy: i32, state: i32) -> bool {
    policy == 3 && matches!(state, 0 | 1)
}

pub fn get_terminate_if_running_details(new_run_id: &str) -> Vec<u8> {
    format!("New runID: {new_run_id}").into_bytes()
}
