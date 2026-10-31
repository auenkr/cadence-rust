//! Parent-close-policy system workflow wire names from Go `parentclosepolicy`.
pub const TASK_LIST: &str = "cadence-sys-processor-parent-close-policy";
pub const WORKFLOW_TYPE: &str = "cadence-sys-parent-close-policy-workflow";
pub const ACTIVITY_TYPE: &str = "cadence-sys-parent-close-policy-activity";
pub const SIGNAL_NAME: &str = "ParentClosePolicyProcessorChannelName";
pub const POLICY_ABANDON: i32 = 0;
pub const POLICY_REQUEST_CANCEL: i32 = 1;
pub const POLICY_TERMINATE: i32 = 2;
pub const TERMINATE_REASON: &str = "by parent close policy";

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    RequestCancel,
    Terminate,
}

/// History operation produced by the Go ProcessorActivity semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation {
    pub action: Action,
    pub domain_id: String,
    pub domain_name: String,
    pub workflow_id: String,
    pub first_execution_run_id: String,
    pub identity: &'static str,
    pub reason: Option<&'static str>,
    pub child_workflow_only: bool,
    pub external_workflow_execution: Option<WorkflowExecution>,
}

/// Convert a signal payload into the ordered History calls used by the Go
/// activity. Missing per-child domain names fall back to the legacy request
/// field; Abandon entries are intentionally omitted.
pub fn operations(request: &Request) -> Result<Vec<Operation>, String> {
    let child_workflow_only = !request.parent_execution.workflow_id.is_empty()
        && !request.parent_execution.run_id.is_empty();
    request
        .executions
        .iter()
        .filter_map(|execution| {
            let action = match execution.policy {
                POLICY_ABANDON => return None,
                POLICY_REQUEST_CANCEL => Action::RequestCancel,
                POLICY_TERMINATE => Action::Terminate,
                policy => {
                    return Some(Err(format!("unknown parent close policy: {policy}")));
                }
            };
            Some(Ok(Operation {
                action,
                domain_id: execution.domain_id.clone(),
                domain_name: if execution.domain_name.is_empty() {
                    request.domain_name.clone()
                } else {
                    execution.domain_name.clone()
                },
                workflow_id: execution.workflow_id.clone(),
                first_execution_run_id: execution.run_id.clone(),
                identity: WORKFLOW_TYPE,
                reason: (action == Action::Terminate).then_some(TERMINATE_REASON),
                child_workflow_only,
                external_workflow_execution: child_workflow_only
                    .then(|| request.parent_execution.clone()),
            }))
        })
        .collect()
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

    #[test]
    fn processor_preserves_domain_fallback_child_scope_and_abandon_noop() {
        let request = super::Request {
            parent_execution: super::WorkflowExecution {
                workflow_id: "parent".to_owned(),
                run_id: "parent-run".to_owned(),
            },
            executions: vec![
                super::RequestDetail {
                    domain_id: "id-1".to_owned(),
                    domain_name: String::new(),
                    workflow_id: "abandoned".to_owned(),
                    run_id: "run-0".to_owned(),
                    policy: super::POLICY_ABANDON,
                },
                super::RequestDetail {
                    domain_id: "id-2".to_owned(),
                    domain_name: String::new(),
                    workflow_id: "child".to_owned(),
                    run_id: "run-1".to_owned(),
                    policy: super::POLICY_TERMINATE,
                },
            ],
            domain_name: "legacy-domain".to_owned(),
        };
        let operations = super::operations(&request).unwrap();
        assert_eq!(operations.len(), 1);
        let operation = &operations[0];
        assert_eq!(operation.action, super::Action::Terminate);
        assert_eq!(operation.domain_name, "legacy-domain");
        assert_eq!(operation.reason, Some("by parent close policy"));
        assert_eq!(operation.first_execution_run_id, "run-1");
        assert!(operation.child_workflow_only);
        assert_eq!(operation.external_workflow_execution, Some(request.parent_execution));
    }
}
