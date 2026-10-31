//! Parent-close-policy system workflow wire names from Go `parentclosepolicy`.
pub const TASK_LIST: &str = "cadence-sys-processor-parent-close-policy";
pub const WORKFLOW_TYPE: &str = "cadence-sys-parent-close-policy-workflow";
pub const ACTIVITY_TYPE: &str = "cadence-sys-parent-close-policy-activity";
pub const SIGNAL_NAME: &str = "ParentClosePolicyProcessorChannelName";

/// Cadence workflow execution identifiers use Go's exported-field JSON names.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WorkflowExecution {
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,
}

/// A child execution and the policy inherited from its parent workflow.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RequestDetail {
    #[serde(rename = "DomainID")]
    pub domain_id: String,
    #[serde(rename = "DomainName")]
    pub domain_name: String,
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,
    /// Go's `types.ParentClosePolicy` is an integer enum in JSON payloads.
    #[serde(rename = "Policy")]
    pub policy: i32,
}

/// Signal payload consumed by the parent-close-policy system workflow.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Request {
    #[serde(rename = "ParentExecution")]
    pub parent_execution: WorkflowExecution,
    #[serde(rename = "Executions")]
    pub executions: Vec<RequestDetail>,
    /// Deprecated Go field retained for old payloads that omit per-execution domains.
    #[serde(rename = "DomainName")]
    pub domain_name: String,
}

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

    #[test]
    fn request_json_field_names_and_order_match_go_structs() {
        let request = super::Request {
            parent_execution: super::WorkflowExecution {
                workflow_id: "parent".to_owned(),
                run_id: "parent-run".to_owned(),
            },
            executions: vec![super::RequestDetail {
                domain_id: "domain-id".to_owned(),
                domain_name: "domain".to_owned(),
                workflow_id: "child".to_owned(),
                run_id: "child-run".to_owned(),
                policy: 1,
            }],
            domain_name: String::new(),
        };
        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"ParentExecution":{"WorkflowID":"parent","RunID":"parent-run"},"Executions":[{"DomainID":"domain-id","DomainName":"domain","WorkflowID":"child","RunID":"child-run","Policy":1}],"DomainName":""}"#
        );
    }
}
