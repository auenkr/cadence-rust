//! Port of `go/service/history/constants/{constants,test_constants}.go`.
//! Go package-level error values become constructor functions (Rust errors are
//! owned values); messages are byte-identical to Go.

use cadence_api::errors::{BadRequestError, InternalServiceError, ServiceBusyError};

pub fn err_domain_not_set() -> BadRequestError {
    BadRequestError::new("Domain not set on request.")
}
pub fn err_workflow_execution_not_set() -> BadRequestError {
    BadRequestError::new("WorkflowExecution not set on request.")
}
pub fn err_task_list_not_set() -> BadRequestError {
    BadRequestError::new("Tasklist not set.")
}
pub fn err_run_id_not_valid() -> BadRequestError {
    BadRequestError::new("RunID is not valid UUID.")
}
pub fn err_workflow_id_not_set() -> BadRequestError {
    BadRequestError::new("WorkflowId is not set on request.")
}
pub fn err_source_cluster_not_set() -> BadRequestError {
    BadRequestError::new("Source Cluster not set on request.")
}
pub fn err_timestamp_not_set() -> BadRequestError {
    BadRequestError::new("Timestamp not set on request.")
}
pub fn err_invalid_task_type() -> BadRequestError {
    BadRequestError::new("Invalid task type")
}
pub fn err_history_host_throttle() -> ServiceBusyError {
    ServiceBusyError {
        message: "History host rps exceeded".into(),
        reason: None,
    }
}
pub fn err_shutting_down() -> InternalServiceError {
    InternalServiceError::new("Shutting down")
}

/// `HistoryTaskDLQModeEnabled` enables writing tasks to the DLQ.
pub const HISTORY_TASK_DLQ_MODE_ENABLED: &str = "enabled";
/// `HistoryTaskDLQModeDisabled` disables writing tasks to the DLQ.
pub const HISTORY_TASK_DLQ_MODE_DISABLED: &str = "disabled";
/// `HistoryTaskDLQModeShadow` enables writing tasks to the DLQ but does not process the task.
pub const HISTORY_TASK_DLQ_MODE_SHADOW: &str = "shadow";

// ---- test_constants.go (scalar values; domain cache entries live with the
// domain cache port and are tracked in memory/history-port-shard-20261105.md)

/// `TestCurrentClusterInitialFailoverVersion` (common/cluster).
pub const TEST_CURRENT_CLUSTER_INITIAL_FAILOVER_VERSION: i64 = 0;
/// `TestFailoverVersionIncrement` (common/cluster).
pub const TEST_FAILOVER_VERSION_INCREMENT: i64 = 10;
/// `TestVersion` is the workflow version for test.
pub const TEST_VERSION: i64 =
    TEST_CURRENT_CLUSTER_INITIAL_FAILOVER_VERSION + TEST_FAILOVER_VERSION_INCREMENT * 5;
pub const TEST_DOMAIN_ID: &str = "deadbeef-0123-4567-890a-bcdef0123456";
pub const TEST_ACTIVE_ACTIVE_DOMAIN_ID: &str = "965c78b7-1f6f-4122-9ba7-af6b7a55b27f";
pub const TEST_ACTIVE_ACTIVE_DOMAIN_NAME: &str = "active-active-domain";
pub const TEST_DOMAIN_NAME: &str = "some random domain name";
pub const TEST_RATE_LIMITED_DOMAIN_NAME: &str = "rate-limited-domain-0123-4567-890a-bcdef0123456";
pub const TEST_RATE_LIMITED_DOMAIN_ID: &str = "rate-limited-domain";
pub const TEST_UNKNOWN_DOMAIN_ID: &str = "unknown-domain-id";
pub const TEST_WORKFLOW_ID: &str = "random-workflow-id";
pub const TEST_RUN_ID: &str = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6";
pub const TEST_REQUEST_ID: &str = "143b22cd-dfac-4d59-9398-893f89d89df6";
/// `TestActiveClusterSelectionPolicy` cluster attribute (scope, name).
pub const TEST_ACTIVE_CLUSTER_ATTRIBUTE: (&str, &str) = ("region", "us-east");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_messages_and_values() {
        assert_eq!(err_domain_not_set().message, "Domain not set on request.");
        assert_eq!(err_history_host_throttle().message, "History host rps exceeded");
        assert_eq!(TEST_VERSION, 50);
    }
}
