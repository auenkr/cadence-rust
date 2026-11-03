//! Production installation of Go's cluster-redirection wrapper
//! (`wrappers/clusterredirection`): request->domain resolver, config-driven
//! runtime knobs, metrics and the remote-Frontend forwarding backend.
//!
//! Not ported (documented gaps): active-active domains (the Rust API has no
//! ActiveClusterSelectionPolicy), strong-consistency auto-forwarding header
//! (always eventual), schedule APIs (served by handler_schedule) and the gRPC
//! remote transport.

#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cadence_api::errors::{CadenceError, InternalServiceError};
use cadence_api::service_types::DescribeDomainRequest;
use cadence_common::cluster::GroupMetadata;
use cadence_persistence::FrontendDomainManager;
use cadence_service_core::frontend_remote::{RemoteFrontendClient, DEFAULT_REMOTE_TIMEOUT};
use cadence_service_core::matching_metrics::{
    HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue,
};
use serde_json::Value;

use super::clusterredirection::{
    ActiveClusterManager, Consistency, DomainState, ForwardRecord, RedirectMetrics,
    RedirectRequest, RedirectRequestResolver, RuntimeConfig, WorkflowRef,
};
use crate::handler_workflow::{WorkflowBackend, WorkflowFuture, WorkflowOperation};

/// APIs Go's `clusterRedirectionHandler` wraps with `Redirect`. Everything
/// else (domain admin, health, search attributes, diagnose...) is local.
const REDIRECTED: &[&str] = &[
    "CountWorkflowExecutions",
    "DescribeTaskList",
    "DescribeWorkflowExecution",
    "GetTaskListsByDomain",
    "GetWorkflowExecutionHistory",
    "ListArchivedWorkflowExecutions",
    "ListClosedWorkflowExecutions",
    "ListOpenWorkflowExecutions",
    "ListTaskListPartitions",
    "ListWorkflowExecutions",
    "PollForActivityTask",
    "PollForDecisionTask",
    "QueryWorkflow",
    "RecordActivityTaskHeartbeat",
    "RecordActivityTaskHeartbeatByID",
    "RefreshWorkflowTasks",
    "RequestCancelWorkflowExecution",
    "ResetStickyTaskList",
    "ResetWorkflowExecution",
    "RespondActivityTaskCanceled",
    "RespondActivityTaskCanceledByID",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskCompletedByID",
    "RespondActivityTaskFailed",
    "RespondActivityTaskFailedByID",
    "RespondDecisionTaskCompleted",
    "RespondDecisionTaskFailed",
    "RespondQueryTaskCompleted",
    "RestartWorkflowExecution",
    "ScanWorkflowExecutions",
    "SignalWithStartWorkflowExecution",
    "SignalWithStartWorkflowExecutionAsync",
    "SignalWorkflowExecution",
    "StartWorkflowExecution",
    "StartWorkflowExecutionAsync",
    "TerminateWorkflowExecution",
];

/// Operations whose domain comes from a task token (`GetDomainByID`).
const TOKEN_OPERATIONS: &[&str] = &[
    "RecordActivityTaskHeartbeat",
    "RespondActivityTaskCanceled",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskFailed",
    "RespondDecisionTaskCompleted",
    "RespondDecisionTaskFailed",
    "RespondQueryTaskCompleted",
];

const EXECUTION_OPERATIONS: &[&str] = &[
    "GetWorkflowExecutionHistory",
    "RequestCancelWorkflowExecution",
    "ResetWorkflowExecution",
    "RestartWorkflowExecution",
    "SignalWorkflowExecution",
    "TerminateWorkflowExecution",
];

#[derive(serde::Deserialize)]
struct EncodedToken {
    #[serde(
        rename = "taskToken",
        default,
        deserialize_with = "cadence_api::b64::deserialize_bytes"
    )]
    task_token: Vec<u8>,
}

/// Go `DomainCache` lookups backed by the persistence domain manager.
pub struct DomainCacheResolver {
    manager: Arc<dyn FrontendDomainManager>,
}

impl DomainCacheResolver {
    pub fn new(manager: Arc<dyn FrontendDomainManager>) -> Self {
        Self { manager }
    }

    fn domain_id_from_token(operation: &str, request: &Value) -> Result<String, CadenceError> {
        let token: EncodedToken = serde_json::from_value(request.clone())
            .map_err(|error| CadenceError::Unknown(error.to_string()))?;
        let id = if operation == "RespondQueryTaskCompleted" {
            cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
                &token.task_token,
            )
            .map(|t| t.domain_id)
        } else {
            cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(&token.task_token)
                .map(|t| t.domain_id)
        };
        id.map_err(|error| CadenceError::Unknown(error.to_string()))
    }
}

fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

impl RedirectRequestResolver for DomainCacheResolver {
    fn resolve(
        &self,
        operation: &str,
        request: &Value,
    ) -> Result<Option<RedirectRequest>, CadenceError> {
        if !REDIRECTED.contains(&operation) {
            return Ok(None);
        }
        let fields = if matches!(
            operation,
            "StartWorkflowExecutionAsync" | "SignalWithStartWorkflowExecutionAsync"
        ) {
            request.get("request").unwrap_or(request)
        } else {
            request
        };
        let lookup = if TOKEN_OPERATIONS.contains(&operation) {
            DescribeDomainRequest {
                name: None,
                uuid: Some(Self::domain_id_from_token(operation, request)?),
            }
        } else {
            DescribeDomainRequest {
                name: Some(str_field(fields, "domain").to_owned()),
                uuid: None,
            }
        };
        let described = self
            .manager
            .describe(&lookup)
            .map_err(CadenceError::EntityNotExists)?;
        let info = described.domain_info.unwrap_or_default();
        let replication = described.replication_configuration.unwrap_or_default();
        let deprecated_or_deleted = info
            .status
            .is_some_and(|status| status != cadence_api::enums::DomainStatus::Registered);
        let domain = DomainState {
            id: info.uuid,
            name: info.name,
            global: described.is_global_domain,
            deprecated_or_deleted,
            active_cluster: replication.active_cluster_name,
            active_active: false,
        };
        let execution = if EXECUTION_OPERATIONS.contains(&operation) {
            fields
                .get("workflowExecution")
                .map(|execution| WorkflowRef {
                    workflow_id: str_field(execution, "workflowId").to_owned(),
                    run_id: str_field(execution, "runId").to_owned(),
                })
        } else if matches!(
            operation,
            "SignalWithStartWorkflowExecution" | "SignalWithStartWorkflowExecutionAsync"
        ) {
            Some(WorkflowRef {
                workflow_id: str_field(fields, "workflowId").to_owned(),
                run_id: String::new(),
            })
        } else {
            None
        };
        Ok(Some(RedirectRequest {
            domain,
            execution,
            requested_cluster_attribute: None,
            consistency: Consistency::Eventual,
        }))
    }
}

/// Dynamic-config lookup `system.enableDomainNotActiveAutoForwarding`.
pub struct FnRuntimeConfig(pub Arc<dyn Fn(&str) -> bool + Send + Sync>);

impl RuntimeConfig for FnRuntimeConfig {
    fn domain_auto_forwarding_enabled(&self, domain: &str) -> bool {
        (self.0)(domain)
    }
}

/// Active-active lookups are unavailable in the Rust API; Go treats lookup
/// errors as "use the current cluster", which this reproduces.
pub struct UnsupportedActiveClusterManager;

impl ActiveClusterManager for UnsupportedActiveClusterManager {
    fn active_cluster_for_workflow(&self, _: &str, _: &str, _: &str) -> Result<String, String> {
        Err("active-active domains are not supported".into())
    }
    fn active_cluster_for_cluster_attribute(&self, _: &str, _: &str) -> Result<String, String> {
        Err("active-active domains are not supported".into())
    }
    fn current_workflow_selection(
        &self,
        _: &str,
        _: &str,
    ) -> Result<(bool, Option<String>), String> {
        Err("active-active domains are not supported".into())
    }
}

/// Emits Go's `cluster_forwarding_policy_requests` and the
/// `cadence_client_*_redirection` client metrics.
pub struct SinkRedirectMetrics {
    sink: Arc<dyn MetricSink>,
}

impl SinkRedirectMetrics {
    pub fn new(sink: Arc<dyn MetricSink>) -> Self {
        Self { sink }
    }

    fn emit(
        &self,
        name: &'static str,
        kind: MetricKind,
        value: MetricValue,
        tags: std::collections::BTreeMap<String, String>,
        histogram: Option<HistogramSpec>,
    ) {
        self.sink.record(MetricEvent {
            name,
            kind,
            value,
            tags,
            histogram,
        });
    }
}

impl RedirectMetrics for SinkRedirectMetrics {
    fn record_forward(&self, record: &ForwardRecord) {
        let mut policy_tags = std::collections::BTreeMap::new();
        policy_tags.insert(
            "operation".to_owned(),
            "DCRedirectionForwardingPolicy".to_owned(),
        );
        policy_tags.insert("cadence_role".to_owned(), "dc_redirection".to_owned());
        policy_tags.insert("domain".to_owned(), record.domain.clone());
        policy_tags.insert("source_cluster".to_owned(), record.source_cluster.clone());
        policy_tags.insert("target_cluster".to_owned(), record.target_cluster.clone());
        policy_tags.insert(
            "is_active_active_domain".to_owned(),
            record.active_active.to_string(),
        );
        policy_tags.insert(
            "query_consistency_level".to_owned(),
            if record.strong_consistency {
                "strong"
            } else {
                "eventual"
            }
            .to_owned(),
        );
        if let Some(active) = &record.active_cluster {
            policy_tags.insert("active_cluster".to_owned(), active.clone());
        }
        self.emit(
            "cluster_forwarding_policy_requests",
            MetricKind::Counter,
            MetricValue::Count(1),
            policy_tags,
            None,
        );

        let mut tags = std::collections::BTreeMap::new();
        tags.insert(
            "operation".to_owned(),
            format!("DCRedirection{}", record.operation),
        );
        tags.insert("cadence_role".to_owned(), "dc_redirection".to_owned());
        tags.insert("domain".to_owned(), record.domain.clone());
        tags.insert("target_cluster".to_owned(), record.target_cluster.clone());
        let elapsed =
            Duration::from_micros(u64::try_from(record.elapsed_micros).unwrap_or(u64::MAX));
        self.emit(
            "cadence_client_requests_redirection",
            MetricKind::Counter,
            MetricValue::Count(1),
            tags.clone(),
            None,
        );
        self.emit(
            "cadence_client_latency_redirection",
            MetricKind::Timer,
            MetricValue::Duration(elapsed),
            tags.clone(),
            None,
        );
        self.emit(
            "cadence_client_latency_redirection_ns",
            MetricKind::Histogram,
            MetricValue::Duration(elapsed),
            tags.clone(),
            Some(HistogramSpec::GoLow1ms100s),
        );
        if !record.succeeded {
            self.emit(
                "cadence_client_errors_redirection",
                MetricKind::Counter,
                MetricValue::Count(1),
                tags,
                None,
            );
        }
    }
}

/// Remote Frontend clients for every enabled remote cluster in
/// `clusterGroupMetadata.clusterGroup` (Go client bean).
#[derive(Clone, Default)]
pub struct RemoteFrontends {
    clients: Arc<HashMap<String, RemoteFrontendClient>>,
}

impl RemoteFrontends {
    pub fn from_metadata(metadata: &GroupMetadata) -> Self {
        let current = &metadata.current_cluster_name;
        let clients = metadata
            .cluster_group
            .iter()
            .filter(|(name, info)| info.enabled && *name != current)
            .map(|(name, info)| {
                (
                    name.clone(),
                    RemoteFrontendClient {
                        cluster: name.clone(),
                        address: info.rpc_address.clone(),
                        service: info.rpc_name.clone(),
                        transport: info.rpc_transport.clone(),
                        caller: "cadence-frontend".to_owned(),
                        forwarding_cluster: current.clone(),
                    },
                )
            })
            .collect();
        Self {
            clients: Arc::new(clients),
        }
    }

    pub fn get(&self, cluster: &str) -> Result<&RemoteFrontendClient, CadenceError> {
        self.clients.get(cluster).ok_or_else(|| {
            CadenceError::Internal(InternalServiceError::new(format!(
                "Cannot find remote Frontend client for cluster {cluster}"
            )))
        })
    }
}

/// Calls the local backend for the current cluster (or no cluster) and the
/// remote Frontend for any other target, like Go's `targetDC ==
/// currentClusterName` switch in the generated wrapper.
#[derive(Clone)]
pub struct ForwardingBackend<B> {
    inner: B,
    current_cluster: String,
    remotes: RemoteFrontends,
    timeout: Duration,
}

impl<B> ForwardingBackend<B> {
    pub fn new(inner: B, current_cluster: String, remotes: RemoteFrontends) -> Self {
        Self {
            inner,
            current_cluster,
            remotes,
            timeout: DEFAULT_REMOTE_TIMEOUT,
        }
    }
}

impl<B: WorkflowBackend> WorkflowBackend for ForwardingBackend<B> {
    fn invoke<'a>(&'a self, operation: WorkflowOperation, request: Value) -> WorkflowFuture<'a> {
        self.inner.invoke(operation, request)
    }

    fn invoke_on_cluster<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: Value,
        target_cluster: Option<String>,
    ) -> WorkflowFuture<'a> {
        Box::pin(async move {
            match target_cluster {
                Some(cluster) if cluster != self.current_cluster => {
                    let client = self.remotes.get(&cluster)?;
                    client.call(operation.name(), request, self.timeout).await
                }
                _ => self.inner.invoke_on_cluster(operation, request, None).await,
            }
        })
    }

    fn invoke_remote_public<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: &Value,
        target_cluster: Option<&str>,
    ) -> Option<WorkflowFuture<'a>> {
        let cluster = target_cluster.filter(|c| *c != self.current_cluster)?.to_owned();
        let request = request.clone();
        Some(Box::pin(async move {
            let client = self.remotes.get(&cluster)?;
            client.call(operation.name(), request, self.timeout).await
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wrappers::clusterredirection::{ForwardingPolicy, WorkflowRedirector};
    use std::sync::Mutex;

    #[test]
    fn redirected_list_matches_go_generated_wrapper_count() {
        assert_eq!(REDIRECTED.len(), 36);
        for operation in TOKEN_OPERATIONS.iter().chain(EXECUTION_OPERATIONS) {
            assert!(REDIRECTED.contains(operation), "{operation}");
        }
    }

    #[test]
    fn remote_clients_cover_enabled_remote_clusters_only() {
        let mut metadata = GroupMetadata {
            current_cluster_name: "a".into(),
            ..Default::default()
        };
        for (name, enabled) in [("a", true), ("b", true), ("c", false)] {
            metadata.cluster_group.insert(
                name.into(),
                cadence_common::cluster::ClusterInformation {
                    enabled,
                    rpc_address: format!("{name}:7933"),
                    rpc_name: "cadence-frontend".into(),
                    rpc_transport: "tchannel".into(),
                    ..Default::default()
                },
            );
        }
        let remotes = RemoteFrontends::from_metadata(&metadata);
        assert!(remotes.get("b").is_ok());
        assert_eq!(remotes.get("b").unwrap().forwarding_cluster, "a");
        assert!(remotes.get("a").is_err());
        assert!(remotes.get("c").is_err());
    }

    struct Recording(Arc<Mutex<Vec<Option<String>>>>);
    impl WorkflowBackend for Recording {
        fn invoke<'a>(&'a self, _: WorkflowOperation, _: Value) -> WorkflowFuture<'a> {
            Box::pin(async { Ok(Value::Null) })
        }
        fn invoke_on_cluster<'a>(
            &'a self,
            _: WorkflowOperation,
            _: Value,
            target: Option<String>,
        ) -> WorkflowFuture<'a> {
            Box::pin(async move {
                self.0.lock().unwrap().push(target);
                Ok(Value::Null)
            })
        }
    }

    #[tokio::test]
    async fn current_cluster_stays_local_and_unknown_remote_errors() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let backend = ForwardingBackend::new(
            Recording(calls.clone()),
            "a".into(),
            RemoteFrontends::default(),
        );
        backend
            .invoke_on_cluster(
                WorkflowOperation::TerminateWorkflowExecution,
                Value::Null,
                Some("a".into()),
            )
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), [None]);
        let err = backend
            .invoke_on_cluster(
                WorkflowOperation::TerminateWorkflowExecution,
                Value::Null,
                Some("b".into()),
            )
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Cannot find remote Frontend client"));
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[test]
    fn metrics_emit_policy_and_client_series() {
        use cadence_service_core::matching_metrics::RecordingMetricSink;
        let sink = Arc::new(RecordingMetricSink::default());
        SinkRedirectMetrics::new(sink.clone()).record_forward(&ForwardRecord {
            operation: "SignalWorkflowExecution".into(),
            domain: "d".into(),
            source_cluster: "a".into(),
            target_cluster: "b".into(),
            active_cluster: Some("b".into()),
            active_active: false,
            strong_consistency: false,
            elapsed_micros: 1500,
            succeeded: false,
        });
        let names: Vec<_> = sink.events().iter().map(|e| e.name).collect();
        assert_eq!(
            names,
            [
                "cluster_forwarding_policy_requests",
                "cadence_client_requests_redirection",
                "cadence_client_latency_redirection",
                "cadence_client_latency_redirection_ns",
                "cadence_client_errors_redirection"
            ]
        );
        assert_eq!(sink.events()[0].tags["active_cluster"], "b");
    }

    // Keep the policy/dispatch types referenced so a fake-remote end to end
    // test lives next to the production wiring.
    #[tokio::test]
    async fn global_domain_signal_is_sent_to_remote_target() {
        struct Resolver;
        impl RedirectRequestResolver for Resolver {
            fn resolve(&self, _: &str, _: &Value) -> Result<Option<RedirectRequest>, CadenceError> {
                Ok(Some(RedirectRequest {
                    domain: DomainState {
                        id: "id".into(),
                        name: "d".into(),
                        global: true,
                        active_cluster: "b".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                }))
            }
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let backend = ForwardingBackend::new(
            Recording(calls.clone()),
            "a".into(),
            RemoteFrontends::default(),
        );
        let redirector = WorkflowRedirector {
            policy: ForwardingPolicy::SelectedV1,
            current_cluster: "a".into(),
            configured_target: String::new(),
            config: FnRuntimeConfig(Arc::new(|_| true)),
            manager: UnsupportedActiveClusterManager,
            metrics: Arc::new(SinkRedirectMetrics::new(Arc::new(
                cadence_service_core::matching_metrics::NoopMetricSink,
            ))),
            requests: Resolver,
        };
        let handler = crate::handler_workflow::Handler::new(backend)
            .with_cluster_redirection(Arc::new(redirector));
        // No remote client is configured, so reaching cluster "b" proves the
        // request was routed away from the local backend.
        let err = handler
            .dispatch(
                "SignalWorkflowExecution",
                serde_json::json!({
                    "domain": "d",
                    "workflowExecution": {"workflowId": "w"},
                    "signalName": "s",
                    "identity": "i",
                    "requestId": "r"
                }),
            )
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("remote Frontend client for cluster b"));
        assert!(calls.lock().unwrap().is_empty());
    }
}
