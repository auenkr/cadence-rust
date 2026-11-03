//! Production installation of Go's cluster-redirection wrapper
//! (`wrappers/clusterredirection`): request->domain resolver, config-driven
//! runtime knobs, metrics and the remote-Frontend forwarding backend.
//!
//! Active-active per-workflow policy lookups read the execution tables via
//! `HistoryNodeStore`. The requested consistency level comes from the inbound
//! `cadence-client-feature-flags` header (Go `getRequestedConsistencyLevelFromContext`);
//! Go never sends a consistency header outbound, only `cadence-forwarding-cluster`.

#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cadence_api::errors::{CadenceError, InternalServiceError};
use cadence_api::service_types::DescribeDomainRequest;
use cadence_common::cluster::GroupMetadata;
use cadence_persistence::history_reader::{
    decode_active_cluster_selection_policy, is_workflow_running, ClusterSelectionPolicy,
    HistoryNodeStore,
};
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
    "BackfillSchedule",
    "CountWorkflowExecutions",
    "CreateSchedule",
    "DeleteSchedule",
    "DescribeSchedule",
    "ListSchedules",
    "PauseSchedule",
    "UnpauseSchedule",
    "UpdateSchedule",
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
            active_active: is_active_active(replication.active_clusters.as_ref()),
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
            requested_cluster_attribute: requested_cluster_attribute(operation, fields),
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

/// Go `DomainReplicationConfig.IsActiveActive`: cluster attribute scopes (or
/// the legacy region map) are configured.
pub fn is_active_active(active_clusters: Option<&cadence_api::types::ActiveClusters>) -> bool {
    active_clusters.is_some_and(|clusters| {
        clusters
            .active_clusters_by_cluster_attribute
            .as_ref()
            .is_some_and(|scopes| !scopes.is_empty())
            || clusters
                .active_clusters_by_region
                .as_ref()
                .is_some_and(|regions| !regions.is_empty())
    })
}

/// Encodes `ClusterAttribute{scope,name}` for the string-typed manager
/// interface. An empty string is a nil attribute.
pub fn encode_cluster_attribute(scope: &str, name: &str) -> String {
    format!("{scope}:{name}")
}

/// `ActiveClusterSelectionPolicy.ClusterAttribute` of a start /
/// signal-with-start request (Go `requestedActiveClusterSelectionPolicy`).
fn requested_cluster_attribute(operation: &str, request: &Value) -> Option<String> {
    if !matches!(
        operation,
        "StartWorkflowExecution"
            | "StartWorkflowExecutionAsync"
            | "SignalWithStartWorkflowExecution"
            | "SignalWithStartWorkflowExecutionAsync"
    ) {
        return None;
    }
    let attribute = request
        .get("activeClusterSelectionPolicy")?
        .get("clusterAttribute")?;
    Some(encode_cluster_attribute(
        str_field(attribute, "scope"),
        str_field(attribute, "name"),
    ))
}

/// Go `activecluster.Manager` backed by the domain cache entry. Cluster
/// attribute lookups follow `DomainCacheEntry.GetActiveClusterInfoByClusterAttribute`.
/// Per-workflow policy lookups read the execution tables through the
/// persistence store (`with_executions`), as Go's `ExecutionManager` does.
pub struct DomainActiveClusterManager {
    manager: Arc<dyn FrontendDomainManager>,
    executions: Option<(Arc<dyn HistoryNodeStore>, i32)>,
}

impl DomainActiveClusterManager {
    pub fn new(manager: Arc<dyn FrontendDomainManager>) -> Self {
        Self {
            manager,
            executions: None,
        }
    }

    /// Execution store + `numHistoryShards` for persisted policy reads.
    pub fn with_executions(mut self, store: Arc<dyn HistoryNodeStore>, num_shards: i32) -> Self {
        self.executions = Some((store, num_shards));
        self
    }

    /// Go `getClusterSelectionPolicy`. `Ok(None)` is Go's EntityNotExistsError
    /// (the caller treats it as an empty policy).
    fn cluster_selection_policy(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<Option<ClusterSelectionPolicy>, String> {
        let Some((store, num_shards)) = &self.executions else {
            // No execution store configured: behave like Go's
            // EntityNotExists (empty policy).
            return Ok(None);
        };
        let domain = parse_uuid(domain_id)?;
        let shard = crate::workflow_matching::workflow_id_to_history_shard(workflow_id, *num_shards);
        let run = if run_id.is_empty() {
            match store
                .select_current_execution(shard, domain, workflow_id)
                .map_err(|e| e.to_string())?
            {
                Some(current) => current.run_id,
                None => return Ok(None),
            }
        } else {
            parse_uuid(run_id)?
        };
        match store
            .select_active_cluster_policy(shard, domain, workflow_id, run)
            .map_err(|e| e.to_string())?
        {
            None => Ok(None),
            Some((data, encoding)) => decode_active_cluster_selection_policy(&data, &encoding)
                .map(Some)
                .map_err(|e| e.to_string()),
        }
    }
}

fn parse_uuid(value: &str) -> Result<[u8; 16], String> {
    uuid::Uuid::parse_str(value)
        .map(|u| *u.as_bytes())
        .map_err(|e| format!("invalid uuid {value}: {e}"))
}

fn policy_attribute(policy: Option<&ClusterSelectionPolicy>) -> String {
    policy
        .and_then(|p| p.cluster_attribute.as_ref())
        .map(|(scope, name)| encode_cluster_attribute(scope, name))
        .unwrap_or_default()
}

/// Go `GetActiveClusterInfoByClusterAttribute` on a domain's replication config.
pub fn active_cluster_for_attribute(
    domain_active_cluster: &str,
    active_clusters: Option<&cadence_api::types::ActiveClusters>,
    attribute: &str,
) -> Result<String, String> {
    if attribute.is_empty() {
        return Ok(domain_active_cluster.to_owned());
    }
    let (scope, name) = attribute.split_once(':').unwrap_or((attribute, ""));
    active_clusters
        .and_then(|clusters| clusters.active_clusters_by_cluster_attribute.as_ref())
        .and_then(|scopes| scopes.get(scope))
        .and_then(|scope| scope.cluster_attributes.as_ref())
        .and_then(|attributes| attributes.get(name))
        .map(|info| info.active_cluster_name.clone())
        .ok_or_else(|| format!("cluster attribute {attribute} not found"))
}

impl ActiveClusterManager for DomainActiveClusterManager {
    fn active_cluster_for_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<String, String> {
        let described = self
            .manager
            .describe(&DescribeDomainRequest {
                name: None,
                uuid: Some(domain_id.to_owned()),
            })
            .map_err(|error| error.to_string())?;
        let replication = described.replication_configuration.unwrap_or_default();
        if !is_active_active(replication.active_clusters.as_ref()) {
            // Go: not active-active, ActiveClusterName from the domain entry.
            return Ok(replication.active_cluster_name);
        }
        // Go `GetActiveClusterInfoByWorkflow`: EntityNotExists from the
        // execution store yields an empty policy (domain active cluster).
        let policy = self.cluster_selection_policy(domain_id, workflow_id, run_id)?;
        active_cluster_for_attribute(
            &replication.active_cluster_name,
            replication.active_clusters.as_ref(),
            &policy_attribute(policy.as_ref()),
        )
    }

    fn active_cluster_for_cluster_attribute(
        &self,
        domain_id: &str,
        cluster_attribute: &str,
    ) -> Result<String, String> {
        let described = self
            .manager
            .describe(&DescribeDomainRequest {
                name: None,
                uuid: Some(domain_id.to_owned()),
            })
            .map_err(|error| error.to_string())?;
        let replication = described.replication_configuration.unwrap_or_default();
        active_cluster_for_attribute(
            &replication.active_cluster_name,
            replication.active_clusters.as_ref(),
            cluster_attribute,
        )
    }

    fn current_workflow_selection(
        &self,
        domain_id: &str,
        workflow_id: &str,
    ) -> Result<(bool, Option<String>), String> {
        let described = self
            .manager
            .describe(&DescribeDomainRequest {
                name: None,
                uuid: Some(domain_id.to_owned()),
            })
            .map_err(|error| error.to_string())?;
        let replication = described.replication_configuration.unwrap_or_default();
        if !is_active_active(replication.active_clusters.as_ref()) {
            // Go: not active-active returns (nil, false, nil).
            return Ok((false, None));
        }
        // Go `GetActiveClusterSelectionPolicyForCurrentWorkflow`.
        let Some((store, num_shards)) = &self.executions else {
            return Err("execution store is not configured".into());
        };
        let shard = crate::workflow_matching::workflow_id_to_history_shard(workflow_id, *num_shards);
        let domain = parse_uuid(domain_id)?;
        let current = store
            .select_current_execution(shard, domain, workflow_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Workflow execution not found.  WorkflowId: {workflow_id}"))?;
        if !is_workflow_running(current.state) {
            return Ok((false, None));
        }
        let run = uuid::Uuid::from_bytes(current.run_id).to_string();
        // Go returns an error when the policy row is missing here.
        let policy = self
            .cluster_selection_policy(domain_id, workflow_id, &run)?
            .ok_or_else(|| "active cluster selection policy not found".to_string())?;
        Ok((true, Some(policy_attribute(Some(&policy)))))
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
                    RemoteFrontendClient::new(
                        name.clone(),
                        info.rpc_address.clone(),
                        info.rpc_name.clone(),
                        info.rpc_transport.clone(),
                        "cadence-frontend".to_owned(),
                        current.clone(),
                        info.tls
                            .clone()
                            .and_then(|tls| serde_json::from_value(tls).ok()),
                    )
                    .with_authorization_provider_if(
                        info.authorization_provider.enable,
                        &info.authorization_provider.private_key,
                    ),
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

/// The production redirector (`policy.go` + `DomainCacheResolver`).
pub type ProductionRedirector = super::clusterredirection::WorkflowRedirector<
    FnRuntimeConfig,
    DomainActiveClusterManager,
    DomainCacheResolver,
>;

/// Go's generated wrapper for the schedule APIs, which Rust serves from the
/// core schedule handlers: run the redirection policy, forward to the remote
/// Frontend for another cluster, or return `None` to serve locally.
#[derive(Clone)]
pub struct ScheduleRedirect {
    pub redirector: Arc<ProductionRedirector>,
    pub remotes: RemoteFrontends,
    pub timeout: Duration,
}

impl ScheduleRedirect {
    pub async fn route(
        &self,
        operation: &str,
        request: &Value,
        headers: &HashMap<String, String>,
    ) -> Result<Option<Value>, CadenceError> {
        let current = self.redirector.current_cluster.as_str();
        self.redirector
            .route(
                operation,
                request,
                Some(super::clusterredirection::consistency_from_headers(headers)),
                |cluster: Option<String>| {
                    let request = request.clone();
                    async move {
                        match cluster {
                            Some(cluster) if cluster != current => {
                                let client = self.remotes.get(&cluster)?;
                                client
                                    .call(operation, request, self.timeout)
                                    .await
                                    .map(Some)
                            }
                            _ => Ok(None),
                        }
                    }
                },
            )
            .await
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
        let cluster = target_cluster
            .filter(|c| *c != self.current_cluster)?
            .to_owned();
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

    struct TestManager;

    impl ActiveClusterManager for TestManager {
        fn active_cluster_for_workflow(&self, _: &str, _: &str, _: &str) -> Result<String, String> {
            Err("unavailable".into())
        }
        fn active_cluster_for_cluster_attribute(&self, _: &str, _: &str) -> Result<String, String> {
            Err("unavailable".into())
        }
        fn current_workflow_selection(
            &self,
            _: &str,
            _: &str,
        ) -> Result<(bool, Option<String>), String> {
            Err("unavailable".into())
        }
    }

    fn active_clusters() -> cadence_api::types::ActiveClusters {
        use cadence_api::types::{ActiveClusterInfo, ClusterAttributeScope};
        let info = ActiveClusterInfo {
            active_cluster_name: "b".into(),
            failover_version: 3,
        };
        cadence_api::types::ActiveClusters {
            active_clusters_by_region: None,
            active_clusters_by_cluster_attribute: Some(HashMap::from([(
                "region".to_owned(),
                ClusterAttributeScope {
                    cluster_attributes: Some(HashMap::from([("us".to_owned(), info)])),
                },
            )])),
        }
    }

    /// Thriftrw `ActiveClusterSelectionPolicy{clusterAttribute{scope:"region",name:"us"}}`.
    fn policy_blob() -> Vec<u8> {
        let mut blob = vec![0x0c, 0x00, 0x01];
        for (id, text) in [(1u8, "region"), (2u8, "us")] {
            blob.extend([0x0b, 0x00, id]);
            blob.extend((text.len() as u32).to_be_bytes());
            blob.extend(text.as_bytes());
        }
        blob.extend([0x00, 0x00]);
        blob
    }

    #[derive(Debug, Default)]
    struct FakeExecutions {
        current: Option<cadence_persistence::history_reader::CurrentExecutionRow>,
        policy: Option<(Vec<u8>, String)>,
        calls: Mutex<Vec<String>>,
    }
    impl HistoryNodeStore for FakeExecutions {
        fn select_nodes(
            &self,
            _: i32,
            _: [u8; 16],
            _: [u8; 16],
            _: i64,
            _: i64,
            _: i64,
        ) -> Result<
            Vec<cadence_persistence::history_reader::HistoryNodeRow>,
            cadence_persistence::history_reader::HistoryReadError,
        > {
            Ok(vec![])
        }
        fn select_current_execution(
            &self,
            shard: i32,
            _: [u8; 16],
            workflow_id: &str,
        ) -> Result<
            Option<cadence_persistence::history_reader::CurrentExecutionRow>,
            cadence_persistence::history_reader::HistoryReadError,
        > {
            self.calls.lock().unwrap().push(format!("current {workflow_id} {shard}"));
            Ok(self.current.clone())
        }
        fn select_active_cluster_policy(
            &self,
            _: i32,
            _: [u8; 16],
            workflow_id: &str,
            run_id: [u8; 16],
        ) -> Result<Option<(Vec<u8>, String)>, cadence_persistence::history_reader::HistoryReadError>
        {
            self.calls
                .lock()
                .unwrap()
                .push(format!("policy {workflow_id} {}", uuid::Uuid::from_bytes(run_id)));
            Ok(self.policy.clone())
        }
    }

    fn manager_with(store: FakeExecutions) -> DomainActiveClusterManager {
        DomainActiveClusterManager::new(Arc::new(
            cadence_persistence::domains::InMemoryDomainStore::default(),
        ))
        .with_executions(Arc::new(store), 4)
    }

    const DOMAIN: &str = "11111111-1111-1111-1111-111111111111";
    const RUN: &str = "22222222-2222-2222-2222-222222222222";

    #[test]
    fn persisted_policy_is_decoded_for_explicit_run() {
        let manager = manager_with(FakeExecutions {
            policy: Some((policy_blob(), "thriftrw".into())),
            ..Default::default()
        });
        let policy = manager.cluster_selection_policy(DOMAIN, "wf", RUN).unwrap();
        assert_eq!(policy_attribute(policy.as_ref()), "region:us");
    }

    #[test]
    fn empty_run_id_resolves_current_run_first_and_missing_rows_are_empty_policy() {
        let current_run = *uuid::Uuid::parse_str(RUN).unwrap().as_bytes();
        let store = FakeExecutions {
            current: Some(cadence_persistence::history_reader::CurrentExecutionRow {
                run_id: current_run,
                state: 1,
            }),
            policy: None,
            ..Default::default()
        };
        let manager = manager_with(store);
        // Missing policy row == Go EntityNotExists == empty attribute.
        let policy = manager.cluster_selection_policy(DOMAIN, "wf", "").unwrap();
        assert_eq!(policy_attribute(policy.as_ref()), "");
        let none = manager_with(FakeExecutions::default());
        assert!(none.cluster_selection_policy(DOMAIN, "wf", "").unwrap().is_none());
    }

    #[test]
    fn snappy_policy_blob_and_bad_encoding() {
        let compressed = snap::raw::Encoder::new().compress_vec(&policy_blob()).unwrap();
        let decoded = decode_active_cluster_selection_policy(&compressed, "thriftrw_snappy").unwrap();
        assert_eq!(
            decoded.cluster_attribute,
            Some(("region".to_owned(), "us".to_owned()))
        );
        assert!(decode_active_cluster_selection_policy(&policy_blob(), "json").is_err());
        // Nil attribute -> empty policy.
        let empty = decode_active_cluster_selection_policy(&[0x00], "thriftrw").unwrap();
        assert!(empty.cluster_attribute.is_none());
    }

    #[test]
    fn active_active_requires_attribute_scopes_or_regions() {
        assert!(!is_active_active(None));
        assert!(!is_active_active(Some(&Default::default())));
        assert!(is_active_active(Some(&active_clusters())));
    }

    #[test]
    fn cluster_attribute_lookup_matches_domain_cache_entry() {
        let clusters = active_clusters();
        // Nil attribute: the domain's ActiveClusterName.
        assert_eq!(
            active_cluster_for_attribute("a", Some(&clusters), "").unwrap(),
            "a"
        );
        assert_eq!(
            active_cluster_for_attribute("a", Some(&clusters), "region:us").unwrap(),
            "b"
        );
        assert!(active_cluster_for_attribute("a", Some(&clusters), "region:eu").is_err());
        assert!(active_cluster_for_attribute("a", Some(&clusters), "zone:us").is_err());
        assert!(active_cluster_for_attribute("a", None, "region:us").is_err());
    }

    #[test]
    fn requested_attribute_comes_from_start_requests_only() {
        let request = serde_json::json!({
            "activeClusterSelectionPolicy": {"clusterAttribute": {"scope": "region", "name": "us"}}
        });
        assert_eq!(
            requested_cluster_attribute("StartWorkflowExecution", &request).as_deref(),
            Some("region:us")
        );
        assert_eq!(
            requested_cluster_attribute("SignalWithStartWorkflowExecutionAsync", &request)
                .as_deref(),
            Some("region:us")
        );
        assert_eq!(
            requested_cluster_attribute("SignalWorkflowExecution", &request),
            None
        );
        assert_eq!(
            requested_cluster_attribute("StartWorkflowExecution", &serde_json::json!({})),
            None
        );
    }

    #[test]
    fn active_active_start_routes_to_cluster_attribute_cluster() {
        struct Attr;
        impl ActiveClusterManager for Attr {
            fn active_cluster_for_workflow(
                &self,
                _: &str,
                _: &str,
                _: &str,
            ) -> Result<String, String> {
                Err("x".into())
            }
            fn active_cluster_for_cluster_attribute(
                &self,
                _: &str,
                a: &str,
            ) -> Result<String, String> {
                active_cluster_for_attribute("a", Some(&active_clusters()), a)
            }
            fn current_workflow_selection(
                &self,
                _: &str,
                _: &str,
            ) -> Result<(bool, Option<String>), String> {
                Err("x".into())
            }
        }
        let domain = DomainState {
            id: "id".into(),
            name: "d".into(),
            global: true,
            deprecated_or_deleted: false,
            active_cluster: "a".into(),
            active_active: true,
        };
        let metrics = SinkRedirectMetrics::new(Arc::new(
            cadence_service_core::matching_metrics::NoopMetricSink,
        ));
        let mut targets = Vec::new();
        crate::wrappers::clusterredirection::redirect(
            ForwardingPolicy::AllDomainV3,
            "a",
            "",
            &domain,
            None,
            Some("region:us"),
            "StartWorkflowExecution",
            Consistency::Eventual,
            &FnRuntimeConfig(Arc::new(|_| true)),
            &Attr,
            &metrics,
            |cluster: &str| {
                targets.push(cluster.to_owned());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(targets, ["b"]);
    }

    #[test]
    fn redirected_list_matches_go_generated_wrapper_count() {
        // 44 `redirectionPolicy.Redirect(` call sites in Go api_generated.go.
        assert_eq!(REDIRECTED.len(), 44);
        for operation in TOKEN_OPERATIONS
            .iter()
            .chain(EXECUTION_OPERATIONS)
            .chain(cadence_service_core::workflow_api::REDIRECTED_SCHEDULE_OPERATIONS)
        {
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
            manager: TestManager,
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
