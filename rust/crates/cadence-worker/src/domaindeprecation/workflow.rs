//! Workflow planning from Go `domaindeprecation/workflow.go`.
//!
//! The SDK-driven workflow body (activity scheduling, the child batch
//! execution and the visibility sleep) is not ported yet. This module holds
//! its Go-equal timeouts, retry policy, batch request and attempt decisions so
//! that body can be wired without re-deriving them.

use std::time::Duration;

use crate::batcher::{BatchParams, TerminateParams};
use crate::domaindeprecation::helpers::{
    DEFAULT_ACTIVITY_HEARTBEAT_TIMEOUT, DEFAULT_ATTEMPTS_ON_RETRYABLE_ERROR, DEFAULT_CONCURRENCY,
    DEFAULT_MAX_ACTIVITY_RETRIES, DEFAULT_PAGE_SIZE, DEFAULT_RPS, ERR_ACCESS_DENIED_NON_RETRYABLE,
    ERR_ACTIVE_POLLERS_NON_RETRYABLE, ERR_DOMAIN_DOES_NOT_EXIST_NON_RETRYABLE,
    ERR_WORKFLOW_ALREADY_COMPLETED_NON_RETRYABLE,
};
use crate::domaindeprecation::BATCH_WORKFLOW_ID_PREFIX;

/// Go `batcher.BatchTypeTerminate`.
const BATCH_TYPE_TERMINATE: &str = "terminate";
/// Go `workflowStartToCloseTimeout` (30 days).
pub const WORKFLOW_START_TO_CLOSE_TIMEOUT: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// Go `workflowTaskStartToCloseTimeout`.
pub const WORKFLOW_TASK_START_TO_CLOSE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Go `MaxBatchWorkflowAttempts`.
pub const MAX_BATCH_WORKFLOW_ATTEMPTS: u32 = 3;
/// Go `VisibilityRefreshWaitTime`.
pub const VISIBILITY_REFRESH_WAIT_TIME: Duration = Duration::from_secs(3 * 60);

/// Go `activityOptions` shared by every domain-deprecation activity.
pub const ACTIVITY_SCHEDULE_TO_START_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const ACTIVITY_START_TO_CLOSE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const ACTIVITY_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// Go `retryPolicy`. The backoff coefficient is 1.7, stored in thousandths.
pub const RETRY_INITIAL_INTERVAL: Duration = Duration::from_secs(10);
pub const RETRY_BACKOFF_COEFFICIENT_MILLI: u32 = 1_700;
pub const RETRY_MAXIMUM_INTERVAL: Duration = Duration::from_secs(5 * 60);
pub const RETRY_EXPIRATION_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Go `retryPolicy.NonRetriableErrorReasons`, in Go's order.
pub const NON_RETRIABLE_ERROR_REASONS: [&str; 3] = [
    ERR_DOMAIN_DOES_NOT_EXIST_NON_RETRYABLE,
    ERR_ACCESS_DENIED_NON_RETRYABLE,
    ERR_ACTIVE_POLLERS_NON_RETRYABLE,
];

/// Go `CheckActivePollers` runs only when the request is not forced.
pub fn requires_active_poller_check(force: bool) -> bool {
    !force
}

/// Go's child batch request: terminate open workflows, excluding closed ones,
/// with the Go batch defaults and the two non-retryable errors.
pub fn batch_params(domain_name: &str) -> BatchParams {
    BatchParams {
        domain_name: domain_name.to_owned(),
        query: "CloseTime = missing".to_owned(),
        reason: "domain is deprecated".to_owned(),
        batch_type: BATCH_TYPE_TERMINATE.to_owned(),
        terminate_params: TerminateParams {
            terminate_children: Some(true),
        },
        rps: DEFAULT_RPS,
        concurrency: DEFAULT_CONCURRENCY,
        page_size: DEFAULT_PAGE_SIZE,
        attempts_on_retryable_error: DEFAULT_ATTEMPTS_ON_RETRYABLE_ERROR,
        activity_heartbeat_timeout_nanos: DEFAULT_ACTIVITY_HEARTBEAT_TIMEOUT.as_nanos() as i64,
        max_activity_retries: DEFAULT_MAX_ACTIVITY_RETRIES,
        non_retryable_errors: Some(vec![
            ERR_ACCESS_DENIED_NON_RETRYABLE.to_owned(),
            ERR_WORKFLOW_ALREADY_COMPLETED_NON_RETRYABLE.to_owned(),
        ]),
        ..Default::default()
    }
}

/// Go child workflow ID `domain-deprecation-batch-<domain>-<uuid>-<attempt>`.
/// The caller supplies the UUID, as Go generates it with `uuid.New()`.
pub fn batch_child_workflow_id(domain_name: &str, workflow_uuid: &str, attempt: u32) -> String {
    format!("{BATCH_WORKFLOW_ID_PREFIX}-{domain_name}-{workflow_uuid}-{attempt}")
}

/// Outcome of checking open workflows after one batch attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptOutcome {
    /// No open workflows remain; the deprecation finishes.
    Completed,
    /// Open workflows remain and attempts are left; run another batch.
    RetryBatch,
    /// Open workflows remain after the final attempt; the workflow fails.
    Failed,
}

/// Go loop decision after `CheckOpenWorkflows` on attempt `attempt` (1-based).
pub fn after_open_workflow_check(attempt: u32, has_open_workflows: bool) -> AttemptOutcome {
    if !has_open_workflows {
        AttemptOutcome::Completed
    } else if attempt == MAX_BATCH_WORKFLOW_ATTEMPTS {
        AttemptOutcome::Failed
    } else {
        AttemptOutcome::RetryBatch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batcher::{validate_and_default, V2_WORKFLOW_TYPE};

    #[test]
    fn preserves_go_timeouts_and_retry_policy() {
        assert_eq!(
            WORKFLOW_START_TO_CLOSE_TIMEOUT,
            Duration::from_secs(30 * 24 * 60 * 60)
        );
        assert_eq!(
            WORKFLOW_TASK_START_TO_CLOSE_TIMEOUT,
            Duration::from_secs(300)
        );
        assert_eq!(MAX_BATCH_WORKFLOW_ATTEMPTS, 3);
        assert_eq!(VISIBILITY_REFRESH_WAIT_TIME, Duration::from_secs(180));
        assert_eq!(ACTIVITY_SCHEDULE_TO_START_TIMEOUT, Duration::from_secs(300));
        assert_eq!(ACTIVITY_START_TO_CLOSE_TIMEOUT, Duration::from_secs(300));
        assert_eq!(ACTIVITY_HEARTBEAT_TIMEOUT, Duration::from_secs(30));
        assert_eq!(RETRY_INITIAL_INTERVAL, Duration::from_secs(10));
        assert_eq!(RETRY_BACKOFF_COEFFICIENT_MILLI, 1_700);
        assert_eq!(RETRY_MAXIMUM_INTERVAL, Duration::from_secs(300));
        assert_eq!(RETRY_EXPIRATION_INTERVAL, Duration::from_secs(86_400));
        assert_eq!(
            NON_RETRIABLE_ERROR_REASONS,
            [
                "domain does not exist",
                "AccessDeniedError",
                "domain has active pollers",
            ]
        );
    }

    #[test]
    fn skips_active_poller_check_only_when_forced() {
        assert!(requires_active_poller_check(false));
        assert!(!requires_active_poller_check(true));
    }

    #[test]
    fn batch_request_matches_go_terminate_request_and_validates() {
        let params = batch_params("orders");
        assert_eq!(params.batch_type, "terminate");
        assert_eq!(params.query, "CloseTime = missing");
        assert_eq!(params.reason, "domain is deprecated");
        assert_eq!(params.terminate_params.terminate_children, Some(true));
        assert_eq!(params.activity_heartbeat_timeout_nanos, 10_000_000_000);
        assert_eq!(
            params.non_retryable_errors,
            Some(vec![
                "AccessDeniedError".to_owned(),
                "WorkflowExecutionAlreadyCompletedError".to_owned(),
            ])
        );
        let validated = validate_and_default(params.clone()).expect("Go batch request validates");
        assert_eq!(validated.0, params);
        assert_eq!(V2_WORKFLOW_TYPE, "cadence-sys-batch-workflow-v2");
    }

    #[test]
    fn child_workflow_id_matches_go_format() {
        assert_eq!(
            batch_child_workflow_id("orders", "0f1e", 2),
            "domain-deprecation-batch-orders-0f1e-2"
        );
    }

    #[test]
    fn attempt_loop_decisions_match_go() {
        assert_eq!(
            after_open_workflow_check(1, false),
            AttemptOutcome::Completed
        );
        assert_eq!(
            after_open_workflow_check(3, false),
            AttemptOutcome::Completed
        );
        assert_eq!(
            after_open_workflow_check(1, true),
            AttemptOutcome::RetryBatch
        );
        assert_eq!(
            after_open_workflow_check(2, true),
            AttemptOutcome::RetryBatch
        );
        assert_eq!(after_open_workflow_check(3, true), AttemptOutcome::Failed);
    }
}
