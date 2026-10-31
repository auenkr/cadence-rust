//! Archival workflow wire names from Go `archiver`.
pub const TASK_LIST: &str = "cadence-archival-tl";
pub const WORKFLOW_TYPE: &str = "archivalWorkflow";
pub const SIGNAL_NAME: &str = "cadence-archival-signal";
pub const UPLOAD_ACTIVITY_TYPE: &str = "uploadHistoryActivity";
pub const DELETE_ACTIVITY_TYPE: &str = "deleteHistoryActivity";
pub const ARCHIVE_VISIBILITY_ACTIVITY_TYPE: &str = "archiveVisibilityActivity";

#[cfg(test)]
mod tests {
    #[test]
    fn preserves_archival_history_wire_names() {
        assert_eq!(super::TASK_LIST, "cadence-archival-tl");
        assert_eq!(super::WORKFLOW_TYPE, "archivalWorkflow");
        assert_eq!(super::SIGNAL_NAME, "cadence-archival-signal");
        assert_eq!(super::UPLOAD_ACTIVITY_TYPE, "uploadHistoryActivity");
    }
}
