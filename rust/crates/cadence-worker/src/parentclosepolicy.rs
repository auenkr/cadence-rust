//! Parent-close-policy system workflow wire names from Go `parentclosepolicy`.
pub const TASK_LIST: &str = "cadence-sys-processor-parent-close-policy";
pub const WORKFLOW_TYPE: &str = "cadence-sys-parent-close-policy-workflow";
pub const ACTIVITY_TYPE: &str = "cadence-sys-parent-close-policy-activity";
pub const SIGNAL_NAME: &str = "ParentClosePolicyProcessorChannelName";
pub const POLICY_ABANDON: i32 = 0;
pub const POLICY_REQUEST_CANCEL: i32 = 1;
pub const POLICY_TERMINATE: i32 = 2;
pub const TERMINATE_REASON: &str = "by parent close policy";
pub const SIGNAL_TIMEOUT_MILLIS: u64 = 400;
pub const WORKFLOW_ID_PREFIX: &str = "parent-close-policy-workflow";
pub const WORKFLOW_EXECUTION_TIMEOUT_SECONDS: i32 = 20 * 365 * 24 * 60 * 60;
pub const WORKFLOW_TASK_TIMEOUT_SECONDS: i32 = 60;
pub const REMOTE_WORKFLOW_IDENTITY: &str = "cadence-worker";

pub fn workflow_id(shard: usize) -> String {
    format!("{WORKFLOW_ID_PREFIX}-{shard}")
}

/// Internal History RPC adapter used by Go's parent-close processor activity.
/// This preserves the resolved domain UUID and parent-child validation fields
/// that are absent from the public Frontend cancel/terminate APIs.
#[derive(Clone)]
pub struct HistoryClient {
    inner: cadence_proto::history::history_api_client::HistoryApiClient<tonic::transport::Channel>,
}

impl HistoryClient {
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            inner: cadence_proto::history::history_api_client::HistoryApiClient::new(channel),
        }
    }

    // Match generated tonic client RPC methods, which return Status directly.
    #[allow(clippy::result_large_err)]
    pub async fn apply(&mut self, operation: &Operation) -> Result<(), tonic::Status> {
        use cadence_proto::{api, history};

        let execution = api::WorkflowExecution {
            workflow_id: operation.workflow_id.clone(),
            run_id: String::new(),
        };
        match operation.action {
            Action::Terminate => {
                let request = history::TerminateWorkflowExecutionRequest {
                    request: Some(api::TerminateWorkflowExecutionRequest {
                        domain: operation.domain_name.clone(),
                        workflow_execution: Some(execution),
                        reason: operation.reason.unwrap_or_default().to_owned(),
                        details: None,
                        identity: operation.identity.to_owned(),
                        first_execution_run_id: operation.first_execution_run_id.clone(),
                    }),
                    domain_id: operation.domain_id.clone(),
                    external_workflow_execution: operation.external_workflow_execution.as_ref().map(
                        |parent| api::WorkflowExecution {
                            workflow_id: parent.workflow_id.clone(),
                            run_id: parent.run_id.clone(),
                        },
                    ),
                    child_workflow_only: operation.child_workflow_only,
                };
                self.inner
                    .terminate_workflow_execution(tonic::Request::new(request))
                    .await?;
            }
            Action::RequestCancel => {
                let request = history::RequestCancelWorkflowExecutionRequest {
                    domain_id: operation.domain_id.clone(),
                    cancel_request: Some(api::RequestCancelWorkflowExecutionRequest {
                        domain: operation.domain_name.clone(),
                        workflow_execution: Some(execution),
                        identity: operation.identity.to_owned(),
                        request_id: String::new(),
                        cause: String::new(),
                        first_execution_run_id: operation.first_execution_run_id.clone(),
                    }),
                    external_execution_info: operation.external_workflow_execution.as_ref().map(
                        |parent| api::ExternalExecutionInfo {
                            workflow_execution: Some(api::WorkflowExecution {
                                workflow_id: parent.workflow_id.clone(),
                                run_id: parent.run_id.clone(),
                            }),
                            initiated_id: 0,
                        },
                    ),
                    child_workflow_only: operation.child_workflow_only,
                };
                self.inner
                    .request_cancel_workflow_execution(tonic::Request::new(request))
                    .await?;
            }
        }
        Ok(())
    }
}

/// Cadence workflow execution identifiers use Go's exported-field JSON names.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct WorkflowExecution {
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,
}

/// A child execution and the policy inherited from its parent workflow.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
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
#[serde(default)]
pub struct Request {
    #[serde(rename = "ParentExecution")]
    pub parent_execution: WorkflowExecution,
    #[serde(rename = "Executions")]
    pub executions: Vec<RequestDetail>,
    /// Deprecated Go field retained for old payloads that omit per-execution domains.
    #[serde(rename = "DomainName")]
    pub domain_name: String,
}

/// Build Go's remote SignalWithStartWorkflowExecution envelope. The caller
/// supplies the random request ID and shard selected by the Go client so this
/// function remains deterministic and testable.
pub fn remote_signal_request(
    shard: usize,
    request_id: impl Into<String>,
    request: &Request,
) -> anyhow::Result<cadence_proto::api::SignalWithStartWorkflowExecutionRequest> {
    use cadence_proto::api::{
        Payload, SignalWithStartWorkflowExecutionRequest, StartWorkflowExecutionRequest, TaskList,
        TaskListKind, WorkflowType,
    };

    let input = crate::workercommon::encode_json(request)?;
    Ok(SignalWithStartWorkflowExecutionRequest {
        start_request: Some(StartWorkflowExecutionRequest {
            domain: crate::workercommon::SYSTEM_DOMAIN.to_owned(),
            workflow_id: workflow_id(shard),
            workflow_type: Some(WorkflowType {
                name: WORKFLOW_TYPE.to_owned(),
            }),
            task_list: Some(TaskList {
                name: TASK_LIST.to_owned(),
                kind: TaskListKind::Normal as i32,
                base_name: String::new(),
            }),
            execution_start_to_close_timeout: Some(prost_types::Duration {
                seconds: WORKFLOW_EXECUTION_TIMEOUT_SECONDS as i64,
                nanos: 0,
            }),
            task_start_to_close_timeout: Some(prost_types::Duration {
                seconds: WORKFLOW_TASK_TIMEOUT_SECONDS as i64,
                nanos: 0,
            }),
            identity: REMOTE_WORKFLOW_IDENTITY.to_owned(),
            request_id: request_id.into(),
            workflow_id_reuse_policy: 2,
            ..Default::default()
        }),
        signal_name: SIGNAL_NAME.to_owned(),
        signal_input: Some(Payload { data: input }),
        control: Vec::new(),
    })
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

/// Errors returned by the History calls made by Go's processor activity.
/// The Cadence Go client exposes these as concrete error types; keeping the
/// cases explicit avoids converting terminal races into retryable failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryError {
    EntityNotExists,
    WorkflowAlreadyCompleted,
    CancellationAlreadyRequested,
    DomainNotActive,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteBatch {
    pub cluster: String,
    pub executions: Vec<RequestDetail>,
}

/// Run the ordered History portion of Go's `ProcessorActivity` and collect
/// executions that must be forwarded to their domain's active cluster.
/// Terminal workflow races count as success; a domain-not-active response is
/// forwarded only after resolving that domain's current active cluster.
pub fn process_history_outcomes<R, H, C, E>(
    request: &Request,
    mut resolve_domain_id: R,
    mut apply_history: H,
    mut active_cluster_for_domain: C,
) -> Result<Vec<RemoteBatch>, String>
where
    R: FnMut(&str) -> Result<String, DomainLookupError>,
    H: FnMut(&Operation) -> Result<(), HistoryError>,
    C: FnMut(&str) -> Result<String, E>,
    E: std::fmt::Display,
{
    let mut batches: std::collections::BTreeMap<String, Vec<RequestDetail>> =
        std::collections::BTreeMap::new();

    let child_workflow_only = !request.parent_execution.workflow_id.is_empty()
        && !request.parent_execution.run_id.is_empty();
    for execution in &request.executions {
        let domain_name = if execution.domain_name.is_empty() {
            request.domain_name.as_str()
        } else {
            execution.domain_name.as_str()
        };
        let domain_id = if execution.domain_id.is_empty() {
            match resolve_domain_id(domain_name) {
                Ok(domain_id) => domain_id,
                Err(DomainLookupError::EntityNotExists) => continue,
                Err(DomainLookupError::Other(error)) => return Err(error),
            }
        } else {
            execution.domain_id.clone()
        };
        let action = match execution.policy {
            POLICY_ABANDON => continue,
            POLICY_REQUEST_CANCEL => Action::RequestCancel,
            POLICY_TERMINATE => Action::Terminate,
            policy => return Err(format!("unknown parent close policy: {policy}")),
        };
        let operation = Operation {
            action,
            domain_id,
            domain_name: domain_name.to_owned(),
            workflow_id: execution.workflow_id.clone(),
            first_execution_run_id: execution.run_id.clone(),
            identity: WORKFLOW_TYPE,
            reason: (action == Action::Terminate).then_some(TERMINATE_REASON),
            child_workflow_only,
            external_workflow_execution: child_workflow_only
                .then(|| request.parent_execution.clone()),
        };
        match apply_history(&operation) {
            Ok(())
            | Err(HistoryError::EntityNotExists)
            | Err(HistoryError::WorkflowAlreadyCompleted)
            | Err(HistoryError::CancellationAlreadyRequested) => {}
            Err(HistoryError::DomainNotActive) => {
                let cluster = active_cluster_for_domain(&operation.domain_id)
                    .map_err(|error| error.to_string())?;
                batches.entry(cluster).or_default().push(execution.clone());
            }
            Err(HistoryError::Other(error)) => return Err(error),
        }
    }

    Ok(batches
        .into_iter()
        .map(|(cluster, executions)| RemoteBatch { cluster, executions })
        .collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomainLookupError {
    EntityNotExists,
    Other(String),
}

/// Convert a signal payload into the ordered History calls used by the Go
/// activity. Missing per-child domain names fall back to the legacy request
/// field; Abandon entries are intentionally omitted.
pub fn operations(request: &Request) -> Result<Vec<Operation>, String> {
    operations_with_domain_resolution(request, |_| {
        Err(DomainLookupError::Other(
            "domain ID is absent and no domain cache resolver was provided".to_owned(),
        ))
    })
}

/// Apply Go's domain-cache lookup ordering before interpreting the policy.
/// Missing domains are successful skips; other lookup errors stop processing.
pub fn operations_with_domain_resolution<F>(
    request: &Request,
    mut resolve_domain_id: F,
) -> Result<Vec<Operation>, String>
where
    F: FnMut(&str) -> Result<String, DomainLookupError>,
{
    let child_workflow_only = !request.parent_execution.workflow_id.is_empty()
        && !request.parent_execution.run_id.is_empty();
    let mut operations = Vec::with_capacity(request.executions.len());
    for execution in &request.executions {
        let domain_name = if execution.domain_name.is_empty() {
            request.domain_name.as_str()
        } else {
            execution.domain_name.as_str()
        };
        let domain_id = if execution.domain_id.is_empty() {
            match resolve_domain_id(domain_name) {
                Ok(domain_id) => domain_id,
                Err(DomainLookupError::EntityNotExists) => continue,
                Err(DomainLookupError::Other(error)) => return Err(error),
            }
        } else {
            execution.domain_id.clone()
        };
        let action = match execution.policy {
            POLICY_ABANDON => continue,
            POLICY_REQUEST_CANCEL => Action::RequestCancel,
            POLICY_TERMINATE => Action::Terminate,
            policy => return Err(format!("unknown parent close policy: {policy}")),
        };
        operations.push(Operation {
            action,
            domain_id,
            domain_name: domain_name.to_owned(),
            workflow_id: execution.workflow_id.clone(),
            first_execution_run_id: execution.run_id.clone(),
            identity: WORKFLOW_TYPE,
            reason: (action == Action::Terminate).then_some(TERMINATE_REASON),
            child_workflow_only,
            external_workflow_execution: child_workflow_only
                .then(|| request.parent_execution.clone()),
        });
    }
    Ok(operations)
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
        assert_eq!(super::workflow_id(7), "parent-close-policy-workflow-7");
        assert_eq!(super::SIGNAL_TIMEOUT_MILLIS, 400);
        assert_eq!(super::WORKFLOW_EXECUTION_TIMEOUT_SECONDS, 630_720_000);
        assert_eq!(super::WORKFLOW_TASK_TIMEOUT_SECONDS, 60);
        assert_eq!(super::REMOTE_WORKFLOW_IDENTITY, "cadence-worker");
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
    fn remote_signal_with_start_envelope_matches_go_fields() {
        let request = super::Request {
            executions: vec![super::RequestDetail {
                domain_id: "domain-id".to_owned(),
                domain_name: "domain".to_owned(),
                workflow_id: "child".to_owned(),
                run_id: "run".to_owned(),
                policy: super::POLICY_REQUEST_CANCEL,
            }],
            ..Default::default()
        };
        let rpc = super::remote_signal_request(3, "request-id", &request).unwrap();
        let start = rpc.start_request.unwrap();
        assert_eq!(start.domain, "cadence-system");
        assert_eq!(start.workflow_id, "parent-close-policy-workflow-3");
        assert_eq!(start.workflow_type.unwrap().name, super::WORKFLOW_TYPE);
        assert_eq!(start.task_list.unwrap().name, super::TASK_LIST);
        assert_eq!(start.execution_start_to_close_timeout.unwrap().seconds, 630_720_000);
        assert_eq!(start.task_start_to_close_timeout.unwrap().seconds, 60);
        assert_eq!(start.identity, "cadence-worker");
        assert_eq!(start.request_id, "request-id");
        assert_eq!(start.workflow_id_reuse_policy, 2);
        assert_eq!(rpc.signal_name, super::SIGNAL_NAME);
        assert_eq!(
            rpc.signal_input.unwrap().data,
            br#"{"ParentExecution":{"WorkflowID":"","RunID":""},"Executions":[{"DomainID":"domain-id","DomainName":"domain","WorkflowID":"child","RunID":"run","Policy":1}],"DomainName":""}"#
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

    #[test]
    fn processor_resolves_domain_before_abandon_and_skips_missing_domains() {
        let request = super::Request {
            domain_name: "legacy-domain".to_owned(),
            executions: vec![super::RequestDetail {
                policy: super::POLICY_ABANDON,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut requested_domain = String::new();
        let operations = super::operations_with_domain_resolution(&request, |domain| {
            requested_domain = domain.to_owned();
            Err(super::DomainLookupError::EntityNotExists)
        })
        .unwrap();
        assert_eq!(requested_domain, "legacy-domain");
        assert!(operations.is_empty());
    }

    #[test]
    fn processor_activity_suppresses_terminal_races_and_routes_inactive_domains() {
        let request = super::Request {
            parent_execution: super::WorkflowExecution {
                workflow_id: "parent".into(),
                run_id: "parent-run".into(),
            },
            executions: vec![
                super::RequestDetail {
                    domain_id: "local-domain-id".into(),
                    domain_name: "local-domain".into(),
                    workflow_id: "already-gone".into(),
                    policy: super::POLICY_REQUEST_CANCEL,
                    ..Default::default()
                },
                super::RequestDetail {
                    domain_id: "remote-domain-id".into(),
                    domain_name: "remote-domain".into(),
                    workflow_id: "remote-child".into(),
                    policy: super::POLICY_TERMINATE,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut applied = Vec::new();
        let routed = super::process_history_outcomes(
            &request,
            |_| unreachable!("both domain IDs are present"),
            |operation| {
                applied.push(operation.workflow_id.clone());
                if operation.workflow_id == "already-gone" {
                    Err(super::HistoryError::WorkflowAlreadyCompleted)
                } else {
                    assert_eq!(operation.action, super::Action::Terminate);
                    assert!(operation.child_workflow_only);
                    Err(super::HistoryError::DomainNotActive)
                }
            },
            |domain_id| {
                assert_eq!(domain_id, "remote-domain-id");
                Ok::<_, &'static str>("cluster-b".into())
            },
        )
        .unwrap();
        assert_eq!(applied, ["already-gone", "remote-child"]);
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].cluster, "cluster-b");
        assert_eq!(routed[0].executions, [request.executions[1].clone()]);
    }

    #[test]
    fn processor_activity_retries_unclassified_history_errors() {
        let request = super::Request {
            executions: vec![super::RequestDetail {
                domain_id: "domain-id".into(),
                policy: super::POLICY_REQUEST_CANCEL,
                ..Default::default()
            }],
            ..Default::default()
        };
        let error = super::process_history_outcomes(
            &request,
            |_| unreachable!("domain ID is present"),
            |_| Err(super::HistoryError::Other("history unavailable".into())),
            |_| Ok::<_, &'static str>("cluster-a".into()),
        )
        .unwrap_err();
        assert_eq!(error, "history unavailable");
    }
}
