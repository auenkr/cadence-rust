//! Replication `AdminClient` / `FrontendClient` / `ClientBean` over gRPC to the
//! remote clusters in `ClusterMetadata` (Go `client.Bean.GetRemoteAdminClient` /
//! `GetRemoteFrontendClient`; remote clients use the cluster's `rpcAddress`).
//! The local-history client (`HistoryClient`) is supplied by the caller because
//! it routes by shard ownership (assembly unit).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::admin::{
    GetDLQReplicationMessagesRequest, GetReplicationMessagesRequest, GetReplicationMessagesResponse,
};
use cadence_api::errors::CadenceError;
use cadence_api::types::ReplicationTask;
use cadence_common::cluster::{ClusterInformation, Metadata};
use cadence_service_core::frontend_remote_grpc::error_from_status;
use cadence_service_core::replication_convert as rc;
use tonic::transport::Channel;

use crate::replication::deps::{
    AdminClient, ClientBean, FrontendClient, HistoryClient, ReplicationError,
};

type Result<T> = std::result::Result<T, ReplicationError>;

pub fn replication_error(e: CadenceError) -> ReplicationError {
    match e {
        CadenceError::BadRequest(e) => ReplicationError::BadRequest(e.to_string()),
        CadenceError::EntityNotExists(e) => ReplicationError::EntityNotExists(e.to_string()),
        CadenceError::ServiceBusy(e) => ReplicationError::ServiceBusy(e.to_string()),
        CadenceError::ShardOwnershipLost(e) => ReplicationError::ShardOwnershipLost(e.to_string()),
        CadenceError::DomainNotActive(e) => ReplicationError::DomainNotActive(e.to_string()),
        CadenceError::WorkflowAlreadyStarted(e) => {
            ReplicationError::WorkflowExecutionAlreadyStarted(e.to_string())
        }
        CadenceError::WorkflowAlreadyCompleted(e) => {
            ReplicationError::WorkflowExecutionAlreadyCompleted(e.to_string())
        }
        CadenceError::LimitExceeded(e) => ReplicationError::LimitExceeded(e.to_string()),
        CadenceError::RetryTaskV2(e) => ReplicationError::RetryTaskV2(e),
        other => ReplicationError::Other(other.to_string()),
    }
}

fn status_error(s: tonic::Status) -> ReplicationError {
    if s.code() == tonic::Code::DeadlineExceeded {
        return ReplicationError::DeadlineExceeded(s.message().to_string());
    }
    replication_error(error_from_status(&s))
}

fn decorate<M>(message: M, info: &ClusterInformation, caller: &str, timeout: Duration) -> tonic::Request<M> {
    let mut r = tonic::Request::new(message);
    r.set_timeout(timeout);
    for (k, v) in [
        ("rpc-service", info.rpc_name.as_str()),
        ("rpc-caller", caller),
        ("rpc-encoding", "proto"),
    ] {
        if let Ok(v) = tonic::metadata::MetadataValue::try_from(v) {
            r.metadata_mut().insert(k, v);
        }
    }
    r
}

/// Go replication fetcher uses the remote admin client's default RPC timeout.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

pub struct GrpcAdminClient {
    channel: Channel,
    info: ClusterInformation,
    caller: String,
}

#[async_trait]
impl AdminClient for GrpcAdminClient {
    async fn get_replication_messages(
        &self,
        req: GetReplicationMessagesRequest,
        timeout: Duration,
    ) -> Result<GetReplicationMessagesResponse> {
        let mut c = cadence_proto::admin::admin_api_client::AdminApiClient::new(self.channel.clone());
        let proto = cadence_proto::admin::GetReplicationMessagesRequest {
            tokens: req.tokens.into_iter().map(rc::replication_token_out).collect(),
            cluster_name: req.cluster_name,
        };
        let resp = c
            .get_replication_messages(decorate(proto, &self.info, &self.caller, timeout))
            .await
            .map_err(status_error)?
            .into_inner();
        let mut out = HashMap::new();
        for (shard, msgs) in resp.shard_messages {
            out.insert(shard, rc::replication_messages_in(msgs).map_err(ReplicationError::Other)?);
        }
        Ok(GetReplicationMessagesResponse { messages_by_shard: Some(out) })
    }

    async fn get_dlq_replication_messages(
        &self,
        req: GetDLQReplicationMessagesRequest,
    ) -> Result<Vec<ReplicationTask>> {
        let mut c = cadence_proto::admin::admin_api_client::AdminApiClient::new(self.channel.clone());
        let proto = cadence_proto::admin::GetDlqReplicationMessagesRequest {
            task_infos: req.task_infos.into_iter().map(rc::replication_task_info_out).collect(),
        };
        let resp = c
            .get_dlq_replication_messages(decorate(proto, &self.info, &self.caller, DEFAULT_RPC_TIMEOUT))
            .await
            .map_err(status_error)?
            .into_inner();
        rc::replication_tasks_in(resp.replication_tasks).map_err(ReplicationError::Other)
    }
}

pub struct GrpcFrontendClient {
    channel: Channel,
    info: ClusterInformation,
    caller: String,
}

/// `reconciliation.CheckDataCorruptionWorkflow*` / `constants.SystemLocalDomainName`.
const SYSTEM_LOCAL_DOMAIN_NAME: &str = "cadence-system";
const CHECK_DATA_CORRUPTION_WORKFLOW_ID: &str = "check-data-corruption-workflow-id";
const CHECK_DATA_CORRUPTION_WORKFLOW_TYPE: &str = "check-data-corruption-workflow";
const CHECK_DATA_CORRUPTION_WORKFLOW_TASK_LIST: &str = "check-data-corruption-workflow-tl";
const CHECK_DATA_CORRUPTION_WORKFLOW_SIGNAL_NAME: &str = "check-data-corruption-workflow-signal";
const CHECK_DATA_CORRUPTION_WORKFLOW_TIMEOUT_SECONDS: i32 = 24 * 60 * 60;
const CHECK_DATA_CORRUPTION_WORKFLOW_TASK_TIMEOUT_SECONDS: i32 = 60;

#[async_trait]
impl FrontendClient for GrpcFrontendClient {
    async fn start_data_corruption_scan(&self, signal_input: Vec<u8>) -> Result<()> {
        use cadence_api::{enums::WorkflowIDReusePolicy, service_types as st, types as t};
        let request = st::SignalWithStartWorkflowExecutionRequest {
            domain: SYSTEM_LOCAL_DOMAIN_NAME.into(),
            workflow_id: CHECK_DATA_CORRUPTION_WORKFLOW_ID.into(),
            workflow_type: Some(t::WorkflowType { name: CHECK_DATA_CORRUPTION_WORKFLOW_TYPE.into() }),
            task_list: Some(t::TaskList {
                name: CHECK_DATA_CORRUPTION_WORKFLOW_TASK_LIST.into(),
                ..Default::default()
            }),
            execution_start_to_close_timeout_seconds: Some(CHECK_DATA_CORRUPTION_WORKFLOW_TIMEOUT_SECONDS),
            task_start_to_close_timeout_seconds: Some(CHECK_DATA_CORRUPTION_WORKFLOW_TASK_TIMEOUT_SECONDS),
            identity: "cadence-history-replication".into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::AllowDuplicate),
            signal_name: CHECK_DATA_CORRUPTION_WORKFLOW_SIGNAL_NAME.into(),
            signal_input,
            ..Default::default()
        };
        let proto = cadence_proto::convert::client::signal_with_start_request_out(request);
        let mut c = cadence_proto::api::workflow_api_client::WorkflowApiClient::new(self.channel.clone());
        c.signal_with_start_workflow_execution(decorate(proto, &self.info, &self.caller, DEFAULT_RPC_TIMEOUT))
            .await
            .map_err(status_error)?;
        Ok(())
    }
}

/// `client.Bean` for replication: lazy per-cluster gRPC channels built from the
/// cluster metadata (`rpcAddress`; only gRPC remotes are supported).
pub struct ReplicationClientBean {
    history: Arc<dyn HistoryClient>,
    clusters: Metadata,
    caller: String,
    channels: Mutex<HashMap<String, Channel>>,
}

impl ReplicationClientBean {
    pub fn new(history: Arc<dyn HistoryClient>, clusters: Metadata, caller: impl Into<String>) -> Self {
        Self { history, clusters, caller: caller.into(), channels: Mutex::new(HashMap::new()) }
    }

    #[allow(clippy::result_large_err)]
    fn remote(&self, cluster: &str) -> Result<(Channel, ClusterInformation)> {
        let info = self
            .clusters
            .clusters
            .get(cluster)
            .filter(|_| cluster != self.clusters.current_cluster_name)
            .ok_or(ReplicationError::UnknownCluster)?;
        if info.rpc_transport == "tchannel" {
            return Err(ReplicationError::Other(format!(
                "remote cluster {cluster:?} uses tchannel; only grpc remotes are supported"
            )));
        }
        let mut map = self.channels.lock().expect("channel map");
        if let Some(c) = map.get(cluster) {
            return Ok((c.clone(), info.clone()));
        }
        let channel = cadence_service_core::grpc_tls::endpoint_channel(&info.rpc_address, None)
            .map_err(|e| ReplicationError::Other(e.to_string()))?;
        map.insert(cluster.to_string(), channel.clone());
        Ok((channel, info.clone()))
    }
}

impl ClientBean for ReplicationClientBean {
    fn history_client(&self) -> Arc<dyn HistoryClient> {
        self.history.clone()
    }
    fn remote_admin_client(&self, cluster: &str) -> Result<Arc<dyn AdminClient>> {
        let (channel, info) = self.remote(cluster)?;
        Ok(Arc::new(GrpcAdminClient { channel, info, caller: self.caller.clone() }))
    }
    fn remote_frontend_client(&self, cluster: &str) -> Result<Arc<dyn FrontendClient>> {
        let (channel, info) = self.remote(cluster)?;
        Ok(Arc::new(GrpcFrontendClient { channel, info, caller: self.caller.clone() }))
    }
}
