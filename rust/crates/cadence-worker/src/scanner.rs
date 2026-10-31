//! Scanner and fixer system workflow names from Go `scanner`.
pub const TASK_LIST: &str = "cadence-sys-tl-scanner-tasklist-0";
pub const HISTORY_TASK_LIST: &str = "cadence-sys-history-scanner-tasklist-0";
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

#[cfg(test)]
mod tests {
    #[test]
    fn scanner_workflow_types_match_go() {
        assert_eq!(super::TASK_LIST, "cadence-sys-tl-scanner-tasklist");
        assert_eq!(super::TASK_LIST_WORKFLOW_TYPE, "cadence-sys-tl-scanner-workflow");
        assert_eq!(super::HISTORY_WORKFLOW_TYPE, "cadence-sys-history-scanner-workflow");
        assert_eq!(super::CONCRETE_EXECUTIONS_WORKFLOW_TYPE, "cadence-sys-executions-scanner-workflow");
        assert_eq!(super::CURRENT_EXECUTIONS_WORKFLOW_TYPE, "cadence-sys-current-executions-scanner-workflow");
        assert_eq!(super::TIMERS_WORKFLOW_TYPE, "cadence-sys-timers-scanner-workflow");
    }
}
