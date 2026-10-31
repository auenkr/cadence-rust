//! Non-retryable error reasons and batch defaults from Go
//! `domaindeprecation/helpers.go`.

use std::time::Duration;

/// Cadence non-retryable error reason used when the domain does not exist.
pub const ERR_DOMAIN_DOES_NOT_EXIST_NON_RETRYABLE: &str = "domain does not exist";
/// Permission error that needs manual intervention.
pub const ERR_ACCESS_DENIED_NON_RETRYABLE: &str = "AccessDeniedError";
/// Error indicating the workflow execution already completed.
pub const ERR_WORKFLOW_ALREADY_COMPLETED_NON_RETRYABLE: &str =
    "WorkflowExecutionAlreadyCompletedError";
/// The domain has active pollers and cannot be deprecated without force.
pub const ERR_ACTIVE_POLLERS_NON_RETRYABLE: &str = "domain has active pollers";

pub const DEFAULT_RPS: i32 = 50;
pub const DEFAULT_CONCURRENCY: i32 = 5;
pub const DEFAULT_PAGE_SIZE: i32 = 1000;
pub const DEFAULT_ATTEMPTS_ON_RETRYABLE_ERROR: i32 = 50;
pub const DEFAULT_ACTIVITY_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_MAX_ACTIVITY_RETRIES: i32 = 4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_go_error_reasons_and_defaults() {
        assert_eq!(ERR_DOMAIN_DOES_NOT_EXIST_NON_RETRYABLE, "domain does not exist");
        assert_eq!(ERR_ACCESS_DENIED_NON_RETRYABLE, "AccessDeniedError");
        assert_eq!(
            ERR_WORKFLOW_ALREADY_COMPLETED_NON_RETRYABLE,
            "WorkflowExecutionAlreadyCompletedError"
        );
        assert_eq!(ERR_ACTIVE_POLLERS_NON_RETRYABLE, "domain has active pollers");
        assert_eq!(DEFAULT_RPS, 50);
        assert_eq!(DEFAULT_CONCURRENCY, 5);
        assert_eq!(DEFAULT_PAGE_SIZE, 1000);
        assert_eq!(DEFAULT_ATTEMPTS_ON_RETRYABLE_ERROR, 50);
        assert_eq!(DEFAULT_ACTIVITY_HEARTBEAT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(DEFAULT_MAX_ACTIVITY_RETRIES, 4);
    }
}
