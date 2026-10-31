//! Batch system workflow wire names from Go `batcher`.
pub const TASK_LIST: &str = "cadence-sys-batcher-tasklist";
pub const V1_WORKFLOW_TYPE: &str = "cadence-sys-batch-workflow";
pub const V2_WORKFLOW_TYPE: &str = "cadence-sys-batch-workflow-v2";
pub const ACTIVITY_TYPE: &str = "cadence-sys-batch-activity";

pub fn registrations() -> [crate::runtime::WorkflowRegistration; 2] {
    [
        crate::runtime::WorkflowRegistration {
            workflow_type: V1_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[ACTIVITY_TYPE],
        },
        crate::runtime::WorkflowRegistration {
            workflow_type: V2_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[ACTIVITY_TYPE],
        },
    ]
}

#[cfg(test)]
mod tests {
    #[test]
    fn preserves_legacy_and_current_workflow_names() {
        assert_eq!(super::TASK_LIST, "cadence-sys-batcher-tasklist");
        assert_eq!(super::V1_WORKFLOW_TYPE, "cadence-sys-batch-workflow");
        assert_eq!(super::V2_WORKFLOW_TYPE, "cadence-sys-batch-workflow-v2");
        assert_eq!(super::ACTIVITY_TYPE, "cadence-sys-batch-activity");
    }
}
