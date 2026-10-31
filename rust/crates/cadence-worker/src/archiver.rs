//! Archival workflow wire names from Go `archiver`.
pub const TASK_LIST: &str = "cadence-archival-tl";
pub const WORKFLOW_TYPE: &str = "archivalWorkflow";
pub const SIGNAL_NAME: &str = "cadence-archival-signal";
pub const UPLOAD_ACTIVITY_TYPE: &str = "uploadHistoryActivity";
pub const DELETE_ACTIVITY_TYPE: &str = "deleteHistoryActivity";
pub const ARCHIVE_VISIBILITY_ACTIVITY_TYPE: &str = "archiveVisibilityActivity";

pub fn registrations() -> crate::runtime::WorkflowRegistration {
    crate::runtime::WorkflowRegistration {
        workflow_type: WORKFLOW_TYPE,
        task_list: TASK_LIST,
        activities: &[
            UPLOAD_ACTIVITY_TYPE,
            DELETE_ACTIVITY_TYPE,
            ARCHIVE_VISIBILITY_ACTIVITY_TYPE,
        ],
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn preserves_archival_history_wire_names() {
        assert_eq!(super::TASK_LIST, "cadence-archival-tl");
        assert_eq!(super::WORKFLOW_TYPE, "archivalWorkflow");
        assert_eq!(super::SIGNAL_NAME, "cadence-archival-signal");
        assert_eq!(super::UPLOAD_ACTIVITY_TYPE, "uploadHistoryActivity");
    }

    #[test]
    fn registration_matches_go_archiver_worker() {
        let registration = super::registrations();
        assert_eq!(registration.workflow_type, "archivalWorkflow");
        assert_eq!(registration.task_list, "cadence-archival-tl");
        assert_eq!(
            registration.activities,
            &[
                "uploadHistoryActivity",
                "deleteHistoryActivity",
                "archiveVisibilityActivity",
            ]
        );
    }
}
