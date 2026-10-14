//! Request validation ported from `go/service/frontend/validate/errors.go`,
//! `go/service/frontend/api/handler.go` (`validateStartWorkflowExecutionRequest`),
//! and `go/common/util.go` (`ValidateRetryPolicy`, `IsValidIDLength`).

use crate::errors::BadRequestError;
use crate::types::{RetryPolicy, StartWorkflowExecutionRequest, WorkflowExecution};

// ---------------------------------------------------------------------------
// Sentinel validation errors (messages match Go exactly)
// ---------------------------------------------------------------------------

pub fn err_domain_not_set() -> BadRequestError {
    BadRequestError::new("Domain not set on request.")
}
pub fn err_request_not_set() -> BadRequestError {
    BadRequestError::new("Request is nil.")
}
pub fn err_workflow_id_not_set() -> BadRequestError {
    BadRequestError::new("WorkflowId is not set on request.")
}
pub fn err_workflow_type_not_set() -> BadRequestError {
    BadRequestError::new("WorkflowType is not set on request.")
}
pub fn err_execution_not_set() -> BadRequestError {
    BadRequestError::new("Execution is not set on request.")
}
pub fn err_signal_name_not_set() -> BadRequestError {
    BadRequestError::new("SignalName is not set on request.")
}
pub fn err_invalid_run_id() -> BadRequestError {
    BadRequestError::new("Invalid RunId.")
}
pub fn err_task_list_not_set() -> BadRequestError {
    BadRequestError::new("TaskList is not set on request.")
}
pub fn err_invalid_execution_timeout() -> BadRequestError {
    BadRequestError::new("A valid ExecutionStartToCloseTimeoutSeconds is not set on request.")
}
pub fn err_invalid_task_timeout() -> BadRequestError {
    BadRequestError::new("A valid TaskStartToCloseTimeoutSeconds is not set on request.")
}

// ---------------------------------------------------------------------------
// Retry policy validation (mirrors `common.ValidateRetryPolicy`)
// ---------------------------------------------------------------------------

pub fn validate_retry_policy(policy: Option<&RetryPolicy>) -> Result<(), BadRequestError> {
    let p = match policy {
        None => return Ok(()),
        Some(p) => p,
    };
    if p.initial_interval_in_seconds <= 0 {
        return Err(BadRequestError::new(
            "InitialIntervalInSeconds must be greater than 0 on retry policy.",
        ));
    }
    if p.backoff_coefficient < 1.0 {
        return Err(BadRequestError::new(
            "BackoffCoefficient cannot be less than 1 on retry policy.",
        ));
    }
    if p.maximum_interval_in_seconds < 0 {
        return Err(BadRequestError::new(
            "MaximumIntervalInSeconds cannot be less than 0 on retry policy.",
        ));
    }
    if p.maximum_interval_in_seconds > 0
        && p.maximum_interval_in_seconds < p.initial_interval_in_seconds
    {
        return Err(BadRequestError::new(
            "MaximumIntervalInSeconds cannot be less than InitialIntervalInSeconds on retry policy.",
        ));
    }
    if p.maximum_attempts < 0 {
        return Err(BadRequestError::new(
            "MaximumAttempts cannot be less than 0 on retry policy.",
        ));
    }
    if p.expiration_interval_in_seconds < 0 {
        return Err(BadRequestError::new(
            "ExpirationIntervalInSeconds cannot be less than 0 on retry policy.",
        ));
    }
    if p.jitter_coefficient.is_nan() || p.jitter_coefficient < 0.0 || p.jitter_coefficient > 1.0 {
        return Err(BadRequestError::new(
            "JitterCoefficient must be between 0 and 1 on retry policy.",
        ));
    }
    if p.maximum_attempts == 0 && p.expiration_interval_in_seconds == 0 {
        return Err(BadRequestError::new(
            "MaximumAttempts and ExpirationIntervalInSeconds are both 0. At least one of them must be specified.",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Execution check (mirrors `validate.CheckExecution`)
// ---------------------------------------------------------------------------

pub fn check_execution(execution: Option<&WorkflowExecution>) -> Result<(), BadRequestError> {
    let e = match execution {
        None => return Err(err_execution_not_set()),
        Some(e) => e,
    };
    if e.workflow_id.is_empty() {
        return Err(err_workflow_id_not_set());
    }
    if !e.run_id.is_empty() && !is_valid_uuid(&e.run_id) {
        return Err(err_invalid_run_id());
    }
    Ok(())
}

fn is_valid_uuid(s: &str) -> bool {
    // Accept canonical 8-4-4-4-12 hex without pulling the `uuid` crate here.
    let parts: Vec<&str> = s.split('-').collect();
    const EXPECTED: [usize; 5] = [8, 4, 4, 4, 12];
    if parts.len() != 5 {
        return false;
    }
    parts
        .iter()
        .zip(EXPECTED)
        .all(|(p, n)| p.len() == n && p.chars().all(|c| c.is_ascii_hexdigit()))
}

// ---------------------------------------------------------------------------
// StartWorkflowExecution validation (core subset, no metrics/config deps)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct IdLengthLimits {
    pub domain_max: usize,
    pub workflow_id_max: usize,
    pub workflow_type_max: usize,
    pub task_list_max: usize,
    pub request_id_max: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self::cadence_default()
    }
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub ids: IdLengthLimits,
}

impl Limits {
    #[must_use]
    pub fn cadence_default() -> Self {
        Self {
            ids: IdLengthLimits {
                domain_max: 256,
                workflow_id_max: 256,
                workflow_type_max: 256,
                task_list_max: 256,
                request_id_max: 256,
            },
        }
    }
}

pub fn validate_start_workflow_execution_request(
    req: Option<&StartWorkflowExecutionRequest>,
    limits: &Limits,
) -> Result<(), BadRequestError> {
    let r = match req {
        None => return Err(err_request_not_set()),
        Some(r) => r,
    };
    if r.domain.is_empty() {
        return Err(err_domain_not_set());
    }
    if r.workflow_id.is_empty() {
        return Err(err_workflow_id_not_set());
    }
    if !is_valid_uuid(&r.request_id) {
        return Err(BadRequestError::new(format!(
            "requestId {:?} is not a valid UUID",
            r.request_id
        )));
    }
    match &r.workflow_type {
        None => return Err(err_workflow_type_not_set()),
        Some(t) if t.name.is_empty() => return Err(err_workflow_type_not_set()),
        _ => {}
    }
    if r.domain.len() > limits.ids.domain_max {
        return Err(BadRequestError::new("Domain length exceeds limit."));
    }
    if r.workflow_id.len() > limits.ids.workflow_id_max {
        return Err(BadRequestError::new("WorkflowID length exceeds limit."));
    }
    validate_retry_policy(r.retry_policy.as_ref())?;
    match &r.task_list {
        None => return Err(err_task_list_not_set()),
        Some(t) if t.name.is_empty() => return Err(err_task_list_not_set()),
        Some(t) if t.name.len() > limits.ids.task_list_max => {
            return Err(BadRequestError::new("TaskList length exceeds limit."))
        }
        _ => {}
    }
    match r.execution_start_to_close_timeout_seconds {
        None => return Err(err_invalid_execution_timeout()),
        Some(v) if v <= 0 => return Err(err_invalid_execution_timeout()),
        _ => {}
    }
    match r.task_start_to_close_timeout_seconds {
        None => return Err(err_invalid_task_timeout()),
        Some(v) if v <= 0 => return Err(err_invalid_task_timeout()),
        _ => {}
    }
    match r.delay_start_seconds {
        Some(v) if v < 0 => return Err(err_invalid_delay_start_seconds()),
        _ => {}
    }
    match r.jitter_start_seconds {
        Some(v) if v < 0 => return Err(err_invalid_jitter_start_seconds()),
        _ => {}
    }
    Ok(())
}

fn err_invalid_delay_start_seconds() -> BadRequestError {
    BadRequestError::new("A valid DelayStartSeconds is not set on request.")
}

fn err_invalid_jitter_start_seconds() -> BadRequestError {
    BadRequestError::new("A valid JitterStartSeconds is not set on request (negative).")
}

/// Core of Go `validateSignalWithStartWorkflowExecutionRequest` (without
/// metrics/config): same field requirements as start plus a signal name.
pub fn validate_signal_with_start_request(
    req: Option<&crate::service_types::SignalWithStartWorkflowExecutionRequest>,
    _limits: &Limits,
) -> Result<(), BadRequestError> {
    let r = match req {
        None => return Err(err_request_not_set()),
        Some(r) => r,
    };
    if r.domain.is_empty() {
        return Err(err_domain_not_set());
    }
    if r.workflow_id.is_empty() {
        return Err(err_workflow_id_not_set());
    }
    if !is_valid_uuid(&r.request_id) {
        return Err(BadRequestError::new(format!(
            "requestId {:?} is not a valid UUID",
            r.request_id
        )));
    }
    match &r.workflow_type {
        None => return Err(err_workflow_type_not_set()),
        Some(t) if t.name.is_empty() => return Err(err_workflow_type_not_set()),
        _ => {}
    }
    match &r.task_list {
        None => return Err(err_task_list_not_set()),
        Some(t) if t.name.is_empty() => return Err(err_task_list_not_set()),
        _ => {}
    }
    if r.signal_name.is_empty() {
        return Err(err_signal_name_not_set());
    }
    match r.execution_start_to_close_timeout_seconds {
        None => return Err(err_invalid_execution_timeout()),
        Some(v) if v <= 0 => return Err(err_invalid_execution_timeout()),
        _ => {}
    }
    match r.task_start_to_close_timeout_seconds {
        None => return Err(err_invalid_task_timeout()),
        Some(v) if v <= 0 => return Err(err_invalid_task_timeout()),
        _ => {}
    }
    validate_retry_policy(r.retry_policy.as_ref())?;
    match r.delay_start_seconds {
        Some(v) if v < 0 => return Err(err_invalid_delay_start_seconds()),
        _ => {}
    }
    match r.jitter_start_seconds {
        Some(v) if v < 0 => return Err(err_invalid_jitter_start_seconds()),
        _ => {}
    }
    Ok(())
}

/// Poll requests require a domain and a task list, mirroring Go's
/// `ErrDomainNotSet` / `ErrTaskListNotSet` checks.
pub fn validate_poll_request(
    domain: &str,
    task_list: Option<&crate::types::TaskList>,
) -> Result<(), BadRequestError> {
    if domain.is_empty() {
        return Err(err_domain_not_set());
    }
    match task_list {
        None => Err(err_task_list_not_set()),
        Some(t) if t.name.is_empty() => Err(err_task_list_not_set()),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TaskList, WorkflowType};

    fn valid_request() -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: "test-domain".into(),
            workflow_id: "wf-1".into(),
            workflow_type: Some(WorkflowType { name: "wt".into() }),
            task_list: Some(TaskList {
                name: "tl".into(),
                kind: None,
                base_name: String::new(),
            }),
            request_id: "123e4567-e89b-12d3-a456-426614174000".into(),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }
    }

    #[test]
    fn start_request_happy_path() {
        assert!(validate_start_workflow_execution_request(
            Some(&valid_request()),
            &Limits::cadence_default()
        )
        .is_ok());
    }

    #[test]
    fn start_request_rejects_missing_fields() {
        let limits = Limits::cadence_default();
        assert_eq!(
            validate_start_workflow_execution_request(None, &limits).unwrap_err(),
            err_request_not_set()
        );
        let mut r = valid_request();
        r.domain.clear();
        assert_eq!(
            validate_start_workflow_execution_request(Some(&r), &limits).unwrap_err(),
            err_domain_not_set()
        );
        let mut r = valid_request();
        r.workflow_id.clear();
        assert_eq!(
            validate_start_workflow_execution_request(Some(&r), &limits).unwrap_err(),
            err_workflow_id_not_set()
        );
        let mut r = valid_request();
        r.request_id = "not-a-uuid".into();
        assert!(validate_start_workflow_execution_request(Some(&r), &limits).is_err());
        let mut r = valid_request();
        r.execution_start_to_close_timeout_seconds = Some(0);
        assert_eq!(
            validate_start_workflow_execution_request(Some(&r), &limits).unwrap_err(),
            err_invalid_execution_timeout()
        );
    }

    #[test]
    fn retry_policy_validation_matches_go_messages() {
        assert!(validate_retry_policy(None).is_ok());
        let err = validate_retry_policy(Some(&RetryPolicy::default())).unwrap_err();
        assert_eq!(
            err.message,
            "InitialIntervalInSeconds must be greater than 0 on retry policy."
        );
        let err = validate_retry_policy(Some(&RetryPolicy {
            initial_interval_in_seconds: 1,
            backoff_coefficient: 0.5,
            maximum_attempts: 1,
            ..Default::default()
        }))
        .unwrap_err();
        assert_eq!(
            err.message,
            "BackoffCoefficient cannot be less than 1 on retry policy."
        );
        let err = validate_retry_policy(Some(&RetryPolicy {
            initial_interval_in_seconds: 10,
            backoff_coefficient: 2.0,
            maximum_interval_in_seconds: 5,
            maximum_attempts: 1,
            ..Default::default()
        }))
        .unwrap_err();
        assert_eq!(
            err.message,
            "MaximumIntervalInSeconds cannot be less than InitialIntervalInSeconds on retry policy."
        );
        let err = validate_retry_policy(Some(&RetryPolicy {
            initial_interval_in_seconds: 1,
            backoff_coefficient: 2.0,
            ..Default::default()
        }))
        .unwrap_err();
        assert_eq!(
            err.message,
            "MaximumAttempts and ExpirationIntervalInSeconds are both 0. At least one of them must be specified."
        );
    }

    #[test]
    fn check_execution_matches_go() {
        assert_eq!(check_execution(None).unwrap_err(), err_execution_not_set());
        assert_eq!(
            check_execution(Some(&WorkflowExecution::default())).unwrap_err(),
            err_workflow_id_not_set()
        );
        let bad_run = WorkflowExecution::new("w", "not-a-uuid");
        assert_eq!(
            check_execution(Some(&bad_run)).unwrap_err(),
            err_invalid_run_id()
        );
        let ok = WorkflowExecution::new("w", "123e4567-e89b-12d3-a456-426614174000");
        assert!(check_execution(Some(&ok)).is_ok());
    }
}
