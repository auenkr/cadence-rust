//! Pure decision logic of the diagnostics invariants (retry, timeoutrisk,
//! failure) from `go/service/worker/diagnostics/invariant/*`. History event
//! traversal is left to the caller; these functions take the already-extracted
//! attributes so they stay faithful to the Go branches.

use crate::diagnostics::root_cause;

// ---- retry ----
pub const WORKFLOW_RETRY_ISSUE: &str = "Workflow Retry configured but invalid";
pub const ACTIVITY_RETRY_ISSUE: &str = "Activity Retry configured but invalid";
pub const ACTIVITY_HEARTBEAT_ISSUE: &str = "Activity Heartbeat configured but invalid";
pub const RETRY_POLICY_VALIDATION_MAX_ATTEMPTS: &str =
    "MaximumAttempts set to 1 will not retry since maximum attempts includes the first attempt.";
pub const RETRY_POLICY_VALIDATION_EXP_INTERVAL: &str =
    "ExpirationIntervalInSeconds less than  InitialIntervalInSeconds  will not retry.";
pub const HEARTBEAT_TIMEOUT_EQUAL_TO_START_TO_CLOSE: &str =
    "Heartbeat timeout being equal or higher than StartToClose timeout will not provide any benefit.";

/// The fields of `types.RetryPolicy` that `checkRetryPolicy` reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryPolicy {
    pub initial_interval_in_seconds: i32,
    pub maximum_attempts: i32,
    pub expiration_interval_in_seconds: i32,
}

/// Port of `checkRetryPolicy`: `None` policy or no issue gives `None`.
pub fn check_retry_policy(policy: Option<&RetryPolicy>) -> Option<&'static str> {
    let p = policy?;
    if p.expiration_interval_in_seconds == 0 && p.maximum_attempts == 1 {
        return Some(RETRY_POLICY_VALIDATION_MAX_ATTEMPTS);
    }
    if p.maximum_attempts == 0 && p.expiration_interval_in_seconds < p.initial_interval_in_seconds {
        return Some(RETRY_POLICY_VALIDATION_EXP_INTERVAL);
    }
    None
}

/// Go checks `StartToClose <= Heartbeat` (so a zero heartbeat only triggers
/// when StartToClose is also zero or negative).
pub fn heartbeat_not_below_start_to_close(start_to_close: i32, heartbeat: i32) -> bool {
    start_to_close <= heartbeat
}

// ---- timeoutrisk ----
pub const ACTIVITY_START_TO_CLOSE_AT_WORKFLOW_TIMEOUT_CAP: &str =
    "Activity StartToClose timeout at the workflow execution timeout cap";
pub const ACTIVITY_MISSING_HEARTBEAT_TIMEOUT: &str = "Long-running activity missing heartbeat timeout";
pub const START_TO_CLOSE_AT_WORKFLOW_TIMEOUT_CAP: &str = "Activity StartToClose timeout may have been silently capped at the workflow execution timeout by the server, leaving no headroom for retrying.";
pub const MISSING_HEARTBEAT_TIMEOUT_FOR_LONG_ACTIVITY: &str = "Long-running activity without a HeartbeatTimeout will leave worker failures undetected until the activity times out.";
pub const LONG_RUNNING_ACTIVITY_THRESHOLD_SECONDS: i32 = 10 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutRisk {
    StartToCloseAtWorkflowTimeoutCap,
    MissingHeartbeatTimeout,
}

/// Risks for one ActivityTaskScheduled event, in Go emission order.
pub fn timeout_risks(
    workflow_timeout_seconds: i32,
    start_to_close_seconds: i32,
    heartbeat_seconds: i32,
) -> Vec<TimeoutRisk> {
    let mut out = Vec::new();
    if workflow_timeout_seconds > 0 && start_to_close_seconds == workflow_timeout_seconds {
        out.push(TimeoutRisk::StartToCloseAtWorkflowTimeoutCap);
    }
    if start_to_close_seconds >= LONG_RUNNING_ACTIVITY_THRESHOLD_SECONDS && heartbeat_seconds == 0 {
        out.push(TimeoutRisk::MissingHeartbeatTimeout);
    }
    out
}

// ---- failure ----
pub const CUSTOM_ERROR: &str = "The failure is caused by a specific custom error returned from the service code";
pub const GENERIC_ERROR: &str = "The failure is because of an error returned from the service code";
pub const PANIC_ERROR: &str = "The failure is caused by a panic in the service code";
pub const TIMEOUT_ERROR: &str = "The failure is caused by a timeout during the execution";
pub const HEARTBEAT_BLOB_SIZE_LIMIT: &str = "Heartbeat details has exceeded the blob size limit";
pub const ACTIVITY_OUTPUT_BLOB_SIZE_LIMIT: &str = "Activity output has exceeded the blob size limit";
pub const DECISION_BLOB_SIZE_LIMIT: &str = "Decision result caused to exceed blob size limit";
pub const HISTORY_SIZE_EXCEEDS_LIMIT: &str =
    "The workflow history size or event count exceeded the configured limit";
pub const ACTIVITY_FAILED: &str = "Activity Failed";
pub const WORKFLOW_FAILED: &str = "Workflow Failed";
pub const DECISION_CAUSED_FAILURE: &str = "Decision caused failure";

pub const FAILURE_REASON_COMPLETE_RESULT_EXCEEDS_LIMIT: &str = "COMPLETE_RESULT_EXCEEDS_LIMIT";
pub const FAILURE_REASON_HEARTBEAT_EXCEEDS_LIMIT: &str = "HEARTBEAT_EXCEEDS_LIMIT";
pub const FAILURE_REASON_DECISION_BLOB_SIZE_EXCEEDS_LIMIT: &str = "DECISION_BLOB_SIZE_EXCEEDS_LIMIT";
pub const FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT: &str = "HISTORY_EXCEEDS_LIMIT";

/// Port of `ErrorTypeFromReason` (substring checks in Go order).
pub fn error_type_from_reason(reason: &str) -> &'static str {
    if reason.contains("Generic") {
        GENERIC_ERROR
    } else if reason.contains("Panic") {
        PANIC_ERROR
    } else if reason.contains("Timeout") {
        TIMEOUT_ERROR
    } else {
        CUSTOM_ERROR
    }
}

/// (invariant type, reason) for a WorkflowExecutionFailed event reason.
pub fn workflow_failure_issue(reason: &str) -> (&'static str, &'static str) {
    match reason {
        FAILURE_REASON_DECISION_BLOB_SIZE_EXCEEDS_LIMIT => {
            (DECISION_CAUSED_FAILURE, DECISION_BLOB_SIZE_LIMIT)
        }
        FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT => (WORKFLOW_FAILED, HISTORY_SIZE_EXCEEDS_LIMIT),
        r => (WORKFLOW_FAILED, error_type_from_reason(r)),
    }
}

/// (invariant type, reason) for an ActivityTaskFailed event reason.
pub fn activity_failure_issue(reason: &str) -> (&'static str, &'static str) {
    match reason {
        FAILURE_REASON_HEARTBEAT_EXCEEDS_LIMIT => (ACTIVITY_FAILED, HEARTBEAT_BLOB_SIZE_LIMIT),
        FAILURE_REASON_COMPLETE_RESULT_EXCEEDS_LIMIT => {
            (ACTIVITY_FAILED, ACTIVITY_OUTPUT_BLOB_SIZE_LIMIT)
        }
        r => (ACTIVITY_FAILED, error_type_from_reason(r)),
    }
}

/// Failure invariant `RootCause` mapping for an issue reason; other reasons
/// (timeouts, blob limits) produce no root cause.
pub fn failure_root_cause(issue_reason: &str) -> Option<&'static str> {
    match issue_reason {
        GENERIC_ERROR => Some(root_cause::SERVICE_SIDE_ISSUE),
        PANIC_ERROR => Some(root_cause::SERVICE_SIDE_PANIC),
        CUSTOM_ERROR => Some(root_cause::SERVICE_SIDE_CUSTOM_ERROR),
        HISTORY_SIZE_EXCEEDS_LIMIT => Some(root_cause::HISTORY_SIZE_EXCEEDS_LIMIT),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_checks() {
        assert_eq!(check_retry_policy(None), None);
        let one = RetryPolicy { maximum_attempts: 1, ..Default::default() };
        assert_eq!(check_retry_policy(Some(&one)), Some(RETRY_POLICY_VALIDATION_MAX_ATTEMPTS));
        let exp = RetryPolicy {
            initial_interval_in_seconds: 10,
            expiration_interval_in_seconds: 5,
            maximum_attempts: 0,
        };
        assert_eq!(check_retry_policy(Some(&exp)), Some(RETRY_POLICY_VALIDATION_EXP_INTERVAL));
        let ok = RetryPolicy {
            initial_interval_in_seconds: 1,
            expiration_interval_in_seconds: 100,
            maximum_attempts: 3,
        };
        assert_eq!(check_retry_policy(Some(&ok)), None);
        // max attempts 1 with nonzero expiration is not flagged.
        let p = RetryPolicy { maximum_attempts: 1, expiration_interval_in_seconds: 5, initial_interval_in_seconds: 1 };
        assert_eq!(check_retry_policy(Some(&p)), None);
        assert!(heartbeat_not_below_start_to_close(10, 10));
        assert!(!heartbeat_not_below_start_to_close(10, 0));
    }

    #[test]
    fn timeout_risk_rules() {
        assert_eq!(
            timeout_risks(600, 600, 0),
            vec![TimeoutRisk::StartToCloseAtWorkflowTimeoutCap, TimeoutRisk::MissingHeartbeatTimeout]
        );
        assert!(timeout_risks(0, 0, 0).is_empty());
        assert_eq!(timeout_risks(1000, 599, 0), vec![]);
        assert_eq!(timeout_risks(1000, 600, 5), vec![]);
        assert_eq!(timeout_risks(1000, 1000, 5), vec![TimeoutRisk::StartToCloseAtWorkflowTimeoutCap]);
    }

    #[test]
    fn failure_classification() {
        assert_eq!(error_type_from_reason("cadenceInternal:Generic"), GENERIC_ERROR);
        assert_eq!(error_type_from_reason("cadenceInternal:Panic"), PANIC_ERROR);
        assert_eq!(error_type_from_reason("cadenceInternal:Timeout START_TO_CLOSE"), TIMEOUT_ERROR);
        assert_eq!(error_type_from_reason("custom"), CUSTOM_ERROR);
        assert_eq!(
            workflow_failure_issue("DECISION_BLOB_SIZE_EXCEEDS_LIMIT"),
            (DECISION_CAUSED_FAILURE, DECISION_BLOB_SIZE_LIMIT)
        );
        assert_eq!(
            workflow_failure_issue("HISTORY_EXCEEDS_LIMIT"),
            (WORKFLOW_FAILED, HISTORY_SIZE_EXCEEDS_LIMIT)
        );
        assert_eq!(workflow_failure_issue("x"), (WORKFLOW_FAILED, CUSTOM_ERROR));
        assert_eq!(
            activity_failure_issue("HEARTBEAT_EXCEEDS_LIMIT"),
            (ACTIVITY_FAILED, HEARTBEAT_BLOB_SIZE_LIMIT)
        );
        assert_eq!(
            activity_failure_issue("COMPLETE_RESULT_EXCEEDS_LIMIT"),
            (ACTIVITY_FAILED, ACTIVITY_OUTPUT_BLOB_SIZE_LIMIT)
        );
        assert_eq!(activity_failure_issue("Panic"), (ACTIVITY_FAILED, PANIC_ERROR));
    }

    #[test]
    fn failure_root_causes() {
        assert_eq!(failure_root_cause(GENERIC_ERROR), Some(root_cause::SERVICE_SIDE_ISSUE));
        assert_eq!(failure_root_cause(PANIC_ERROR), Some(root_cause::SERVICE_SIDE_PANIC));
        assert_eq!(failure_root_cause(CUSTOM_ERROR), Some(root_cause::SERVICE_SIDE_CUSTOM_ERROR));
        assert_eq!(
            failure_root_cause(HISTORY_SIZE_EXCEEDS_LIMIT),
            Some(root_cause::HISTORY_SIZE_EXCEEDS_LIMIT)
        );
        assert_eq!(failure_root_cause(TIMEOUT_ERROR), None);
        assert_eq!(failure_root_cause(HEARTBEAT_BLOB_SIZE_LIMIT), None);
    }
}
