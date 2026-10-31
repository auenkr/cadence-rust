//! Domain deprecation workflow names; source port follows in this module.
pub const TASK_LIST: &str = "domain-deprecation-tasklist";
pub const WORKFLOW_TYPE: &str = "domain-deprecation-workflow";
pub const BATCH_WORKFLOW_ID_PREFIX: &str = "domain-deprecation-batch";
pub const CHECK_ACTIVE_POLLERS_ACTIVITY: &str = "checkActivePollers";
pub const DISABLE_ARCHIVAL_ACTIVITY: &str = "disableArchival";
pub const DEPRECATE_DOMAIN_ACTIVITY: &str = "deprecateDomain";
pub const CHECK_OPEN_WORKFLOWS_ACTIVITY: &str = "checkOpenWorkflows";

pub fn registration() -> crate::runtime::WorkflowRegistration {
    crate::runtime::WorkflowRegistration {
        workflow_type: WORKFLOW_TYPE,
        task_list: TASK_LIST,
        activities: &[
            CHECK_ACTIVE_POLLERS_ACTIVITY,
            DISABLE_ARCHIVAL_ACTIVITY,
            DEPRECATE_DOMAIN_ACTIVITY,
            CHECK_OPEN_WORKFLOWS_ACTIVITY,
        ],
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn registration_matches_go_domain_deprecation_worker() {
        let registration = super::registration();
        assert_eq!(registration.workflow_type, "domain-deprecation-workflow");
        assert_eq!(registration.task_list, "domain-deprecation-tasklist");
        assert_eq!(
            registration.activities,
            &[
                "checkActivePollers",
                "disableArchival",
                "deprecateDomain",
                "checkOpenWorkflows",
            ]
        );
    }
}
