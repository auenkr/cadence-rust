//! Port of Go `TaskListScannerWorkflow` and `HistoryScannerWorkflow`: each
//! runs one long-lived scavenger activity with `activityOptions`.

use std::time::Duration;

use crate::sdk::core::{ActivityOptions, RetryPolicy, WorkflowContext, WorkflowError, WorkflowResult};
use crate::sdk::registry::Registry;

const INFINITE_SECONDS: u64 = 20 * 365 * 24 * 60 * 60;

/// Go scanner `activityOptions`.
pub fn activity_options() -> ActivityOptions {
    ActivityOptions {
        schedule_to_start: Duration::from_secs(5 * 60),
        start_to_close: Duration::from_secs(INFINITE_SECONDS),
        heartbeat: Duration::from_secs(5 * 60),
        retry_policy: Some(RetryPolicy {
            initial_interval: Duration::from_secs(10),
            backoff_coefficient: 1.7,
            maximum_interval: Duration::from_secs(5 * 60),
            maximum_attempts: 0,
            expiration_interval: Duration::from_secs(INFINITE_SECONDS),
            non_retriable_error_reasons: Vec::new(),
        }),
        ..Default::default()
    }
}

async fn run_activity(ctx: &WorkflowContext, activity: &str) -> WorkflowResult {
    ctx.execute_activity(activity, &activity_options(), &())
        .await
        .map_err(WorkflowError::from)?;
    Ok(None)
}

pub async fn task_list_scanner_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    run_activity(&ctx, super::SCAVENGER_ACTIVITY_TYPE).await
}

pub async fn history_scanner_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    run_activity(&ctx, super::HISTORY_SCAVENGER_ACTIVITY_TYPE).await
}

pub fn register_sdk(registry: &mut Registry) {
    registry.register_workflow(super::TASK_LIST_WORKFLOW_TYPE, task_list_scanner_workflow);
    registry.register_workflow(super::HISTORY_WORKFLOW_TYPE, history_scanner_workflow);
}
