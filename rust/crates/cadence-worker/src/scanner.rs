//! Scanner and fixer system workflow names from Go `scanner`.
pub const TASK_LIST: &str = "cadence-sys-tl-scanner-tasklist-0";
pub const WORKFLOW_ID: &str = "cadence-sys-tl-scanner";
pub const SCAVENGER_ACTIVITY_TYPE: &str = "cadence-sys-tl-scanner-scvg-activity";
pub const HISTORY_TASK_LIST: &str = "cadence-sys-history-scanner-tasklist-0";
pub const HISTORY_WORKFLOW_ID: &str = "cadence-sys-history-scanner";
pub const HISTORY_SCAVENGER_ACTIVITY_TYPE: &str = "cadence-sys-history-scanner-scvg-activity";
pub const CONCRETE_EXECUTIONS_TASK_LIST: &str = "cadence-sys-executions-scanner-tasklist-0";
pub const CONCRETE_EXECUTIONS_FIXER_TASK_LIST: &str = "cadence-sys-executions-fixer-tasklist-0";
pub const CURRENT_EXECUTIONS_TASK_LIST: &str =
    "cadence-sys-current-executions-scanner-tasklist-0";
pub const CURRENT_EXECUTIONS_FIXER_TASK_LIST: &str =
    "cadence-sys-current-executions-fixer-tasklist-0";
pub const TIMERS_TASK_LIST: &str = "cadence-sys-timers-scanner-tasklist-0";
pub const TIMERS_FIXER_TASK_LIST: &str = "cadence-sys-timers-fixer-tasklist-0";
pub const TASK_LIST_WORKFLOW_TYPE: &str = "cadence-sys-tl-scanner-workflow";
pub const HISTORY_WORKFLOW_TYPE: &str = "cadence-sys-history-scanner-workflow";
pub const CONCRETE_EXECUTIONS_WORKFLOW_TYPE: &str = "cadence-sys-executions-scanner-workflow";
pub const CONCRETE_EXECUTIONS_FIXER_WORKFLOW_TYPE: &str = "cadence-sys-executions-fixer-workflow";
pub const CURRENT_EXECUTIONS_WORKFLOW_TYPE: &str =
    "cadence-sys-current-executions-scanner-workflow";
pub const CURRENT_EXECUTIONS_FIXER_WORKFLOW_TYPE: &str =
    "cadence-sys-current-executions-fixer-workflow";
pub const TIMERS_WORKFLOW_TYPE: &str = "cadence-sys-timers-scanner-workflow";
pub const TIMERS_FIXER_WORKFLOW_TYPE: &str = "cadence-sys-timers-fixer-workflow";

/// Start policy used by the Go worker's periodic scanner workflows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartOptions {
    pub workflow_id: &'static str,
    pub task_list: &'static str,
    pub execution_timeout_seconds: u64,
    pub cron_schedule: &'static str,
    /// Cadence's WorkflowIDReusePolicyAllowDuplicate wire value.
    pub allow_duplicate: bool,
}

pub const TASK_LIST_START: StartOptions = StartOptions {
    workflow_id: WORKFLOW_ID,
    task_list: TASK_LIST,
    execution_timeout_seconds: 5 * 24 * 60 * 60,
    cron_schedule: "0 */12 * * *",
    allow_duplicate: true,
};
pub const HISTORY_START: StartOptions = StartOptions {
    workflow_id: HISTORY_WORKFLOW_ID,
    task_list: HISTORY_TASK_LIST,
    execution_timeout_seconds: 20 * 365 * 24 * 60 * 60,
    cron_schedule: "0 */12 * * *",
    allow_duplicate: true,
};

pub const EXECUTIONS_CRON_SCHEDULE: &str = "*/5 * * * *";
pub const ACTIVITY_SCHEDULE_TO_START_TIMEOUT_SECONDS: u64 = 5 * 60;
pub const ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS: u64 = 20 * 365 * 24 * 60 * 60;
pub const ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS: u64 = 5 * 60;
pub const ACTIVITY_RETRY_INITIAL_INTERVAL_SECONDS: u64 = 10;
pub const ACTIVITY_RETRY_BACKOFF_COEFFICIENT_MILLI: u64 = 1_700;
pub const ACTIVITY_RETRY_MAXIMUM_INTERVAL_SECONDS: u64 = 5 * 60;

#[cfg(test)]
mod tests {
    #[test]
    fn scanner_workflow_types_match_go() {
        assert_eq!(super::TASK_LIST, "cadence-sys-tl-scanner-tasklist-0");
        assert_eq!(super::WORKFLOW_ID, "cadence-sys-tl-scanner");
        assert_eq!(
            super::SCAVENGER_ACTIVITY_TYPE,
            "cadence-sys-tl-scanner-scvg-activity"
        );
        assert_eq!(super::HISTORY_TASK_LIST, "cadence-sys-history-scanner-tasklist-0");
        assert_eq!(super::HISTORY_WORKFLOW_ID, "cadence-sys-history-scanner");
        assert_eq!(
            super::HISTORY_SCAVENGER_ACTIVITY_TYPE,
            "cadence-sys-history-scanner-scvg-activity"
        );
        assert_eq!(super::TASK_LIST_WORKFLOW_TYPE, "cadence-sys-tl-scanner-workflow");
        assert_eq!(super::HISTORY_WORKFLOW_TYPE, "cadence-sys-history-scanner-workflow");
        assert_eq!(
            super::CONCRETE_EXECUTIONS_WORKFLOW_TYPE,
            "cadence-sys-executions-scanner-workflow"
        );
        assert_eq!(
            super::CURRENT_EXECUTIONS_WORKFLOW_TYPE,
            "cadence-sys-current-executions-scanner-workflow"
        );
        assert_eq!(
            super::TIMERS_WORKFLOW_TYPE,
            "cadence-sys-timers-scanner-workflow"
        );
        assert_eq!(super::EXECUTIONS_CRON_SCHEDULE, "*/5 * * * *");
        assert_eq!(super::TASK_LIST_START.execution_timeout_seconds, 432_000);
        assert_eq!(super::HISTORY_START.execution_timeout_seconds, 630_720_000);
        assert_eq!(super::TASK_LIST_START.cron_schedule, "0 */12 * * *");
        assert!(super::TASK_LIST_START.allow_duplicate && super::HISTORY_START.allow_duplicate);
        assert_eq!(super::ACTIVITY_RETRY_BACKOFF_COEFFICIENT_MILLI, 1_700);
    }
}
