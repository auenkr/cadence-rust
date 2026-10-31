//! Parent-close-policy system workflow wire names from Go `parentclosepolicy`.
pub const TASK_LIST: &str = "cadence-sys-processor-parent-close-policy";
pub const WORKFLOW_TYPE: &str = "cadence-sys-parent-close-policy-workflow";
pub const ACTIVITY_TYPE: &str = "cadence-sys-parent-close-policy-activity";
pub const SIGNAL_NAME: &str = "ParentClosePolicyProcessorChannelName";

pub fn registration() -> crate::runtime::WorkflowRegistration {
    crate::runtime::WorkflowRegistration {
        workflow_type: WORKFLOW_TYPE,
        task_list: TASK_LIST,
        activities: &[ACTIVITY_TYPE],
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn go_registration_names_are_preserved() {
        assert_eq!(super::TASK_LIST, "cadence-sys-processor-parent-close-policy");
        assert_eq!(super::WORKFLOW_TYPE, "cadence-sys-parent-close-policy-workflow");
        assert_eq!(super::ACTIVITY_TYPE, "cadence-sys-parent-close-policy-activity");
        assert_eq!(super::SIGNAL_NAME, "ParentClosePolicyProcessorChannelName");
    }
}
