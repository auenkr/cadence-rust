//! Scheduler worker workflow names and task-list contract.
pub const TASK_LIST: &str = "cadence-scheduler";
pub const WORKFLOW_TYPE: &str = "cadence-scheduler";
pub const SIGNAL_PAUSE: &str = "scheduler-pause";
pub const SIGNAL_UNPAUSE: &str = "scheduler-unpause";
pub const SIGNAL_UPDATE: &str = "scheduler-update";
pub const SIGNAL_BACKFILL: &str = "scheduler-backfill";
pub const SIGNAL_DELETE: &str = "scheduler-delete";
pub const QUERY_DESCRIBE: &str = "scheduler-describe";
pub const WATCH_WORKFLOW_ACTIVITY: &str = "watchWorkflowActivity";

pub fn registration() -> crate::runtime::WorkflowRegistration {
    crate::runtime::WorkflowRegistration {
        workflow_type: WORKFLOW_TYPE,
        task_list: TASK_LIST,
        activities: &[WATCH_WORKFLOW_ACTIVITY],
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn scheduler_registration_matches_go_worker() {
        let registration = super::registration();
        assert_eq!(registration.workflow_type, "cadence-scheduler");
        assert_eq!(registration.task_list, "cadence-scheduler");
        assert_eq!(registration.activities, &["watchWorkflowActivity"]);
    }
}
