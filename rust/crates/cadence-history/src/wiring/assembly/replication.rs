//! Assembly helpers for the replication package (go/service/history/replication).
//!
//! Real adapters: `ThriftPayloadSerializer`, `ReplicationShardAdapter`,
//! `build_task_ack_manager`.
//!
//! `LocalReplicationEngine` delegates `SyncShardStatus`, `SyncActivity` and
//! `ReplicateEventsV2` to the shard's `HistoryEngine` through the engine slot
//! of `ShardBuildCtx`. The replication builders live in `replication_builders.rs`
//! and the history resender in `resender.rs`.
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::errors::CadenceError;
use cadence_api::types::{DataBlob, FailoverMarkerAttributes};

use crate::engine::deps::HistoryError;
use crate::engine::runtime::HistoryEngine;
use crate::engine::types as et;

use crate::replication::deps::{
    ClientBean, DomainCache, Engine, ExecutionManager, PayloadSerializer, ReplicationError,
    Result as ReplResult, ShardContext as ReplShard, SyncActivityRequest, SyncShardStatusRequest, TimeSource,
    ReplicateEventsV2Request,
};
use crate::shard_context::{FailoverMarker, HistoryTaskCategory, HistoryTaskKey, ShardContext};
use crate::wiring::assembly::failover::marker_stale_by_cache;
use crate::wiring::resources::domain_cache::HistoryDomainCache;

/// `persistence.PayloadSerializer.DeserializeBatchEvents` over the ThriftRW blob.
pub struct ThriftPayloadSerializer;

impl PayloadSerializer for ThriftPayloadSerializer {
    fn deserialize_batch_events(&self, blob: &DataBlob) -> ReplResult<Vec<(i64, i64)>> {
        let events = cadence_thrift::history_blob::decode(blob)
            .map_err(|e| ReplicationError::InternalDataInconsistency(e.to_string()))?;
        Ok(events.into_iter().map(|e| (e.event_id, e.version)).collect())
    }
}

/// Everything `replication::ShardContext` needs besides the real shard.
pub struct ReplicationShardDeps {
    pub shard: Arc<ShardContext>,
    pub current_cluster: String,
    pub remote_clusters: Vec<String>,
    pub time_source: Arc<dyn TimeSource>,
    /// Replication-module view of the domain cache.
    pub repl_domain_cache: Arc<dyn DomainCache>,
    /// Cache used for failover marker staleness checks.
    pub domains: Arc<HistoryDomainCache>,
    pub execution_manager: Arc<dyn ExecutionManager>,
    pub client_bean: Arc<dyn ClientBean>,
    pub cluster_name_for_version: Arc<dyn Fn(i64) -> ReplResult<String> + Send + Sync>,
}

/// `replication::ShardContext` over the real shard context.
pub struct ReplicationShardAdapter {
    deps: ReplicationShardDeps,
}

impl ReplicationShardAdapter {
    pub fn new(deps: ReplicationShardDeps) -> Arc<Self> {
        Arc::new(Self { deps })
    }
}

#[async_trait]
impl ReplShard for ReplicationShardAdapter {
    fn shard_id(&self) -> i32 {
        self.deps.shard.shard_id()
    }
    fn current_cluster_name(&self) -> String {
        self.deps.current_cluster.clone()
    }
    fn remote_cluster_names(&self) -> Vec<String> {
        self.deps.remote_clusters.clone()
    }
    fn time_source(&self) -> Arc<dyn TimeSource> {
        self.deps.time_source.clone()
    }
    fn domain_cache(&self) -> Arc<dyn DomainCache> {
        self.deps.repl_domain_cache.clone()
    }
    fn execution_manager(&self) -> Arc<dyn ExecutionManager> {
        self.deps.execution_manager.clone()
    }
    fn client_bean(&self) -> Arc<dyn ClientBean> {
        self.deps.client_bean.clone()
    }
    fn cluster_name_for_failover_version(&self, version: i64) -> ReplResult<String> {
        (self.deps.cluster_name_for_version)(version)
    }
    fn get_queue_cluster_ack_level(&self, cluster: &str) -> i64 {
        self.deps.shard.get_queue_cluster_ack_level(HistoryTaskCategory::Replication, cluster).task_id
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, cluster: &str) -> i64 {
        self.deps
            .shard
            .update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Replication, cluster)
            .task_id
    }
    fn update_queue_cluster_ack_level(&self, cluster: &str, ack_level: i64) -> ReplResult<()> {
        self.deps
            .shard
            .update_queue_cluster_ack_level(HistoryTaskCategory::Replication, cluster, HistoryTaskKey::immediate(ack_level))
            .map_err(|e| ReplicationError::Other(e.to_string()))
    }
    fn adding_pending_failover_marker(&self, marker: FailoverMarkerAttributes) -> ReplResult<()> {
        let (Some(domain_id), Some(version)) = (marker.domain_id, marker.failover_version) else {
            return Err(ReplicationError::empty_failover_marker_attributes());
        };
        let m = FailoverMarker { domain_id, failover_version: version };
        // Go AddingPendingFailoverMarker: drop markers that are already stale.
        match marker_stale_by_cache(&self.deps.domains, &self.deps.current_cluster, &m) {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => return Err(ReplicationError::Other(e.to_string())),
        }
        self.deps
            .shard
            .add_pending_failover_marker(m)
            .map_err(|e| ReplicationError::Other(e.to_string()))
    }
}

/// `engine.Engine` for the replication package over the shard's `HistoryEngine`.
pub struct LocalReplicationEngine {
    shard: Arc<ShardContext>,
    slot: Arc<OnceLock<Weak<HistoryEngine>>>,
}

impl LocalReplicationEngine {
    pub fn new(shard: Arc<ShardContext>, slot: Arc<OnceLock<Weak<HistoryEngine>>>) -> Self {
        Self { shard, slot }
    }

    fn engine(&self) -> ReplResult<Arc<HistoryEngine>> {
        self.slot
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| ReplicationError::Other("history engine is not ready".into()))
    }
}

/// Engine `SyncActivityRequest` from the replication-layer attributes.
pub fn to_engine_sync_activity(req: SyncActivityRequest) -> et::SyncActivityRequest {
    let t = req.task;
    et::SyncActivityRequest {
        domain_id: t.domain_id.unwrap_or_default(),
        workflow_id: t.workflow_id.unwrap_or_default(),
        run_id: t.run_id.unwrap_or_default(),
        version: t.version.unwrap_or_default(),
        scheduled_id: t.scheduled_id.unwrap_or_default(),
        scheduled_time: t.scheduled_time,
        started_id: t.started_id.unwrap_or_default(),
        started_time: t.started_time,
        last_heartbeat_time: t.last_heartbeat_time,
        details: t.details.unwrap_or_default(),
        attempt: t.attempt.unwrap_or_default(),
        last_failure_reason: t.last_failure_reason,
        last_worker_identity: t.last_worker_identity.unwrap_or_default(),
        last_failure_details: t.last_failure_details.unwrap_or_default(),
        last_failure_options: t.last_failure_options,
        version_history: t.version_history,
    }
}

/// Engine `ReplicateEventsV2Request` from the replication-layer request.
pub fn to_engine_replicate_events(req: ReplicateEventsV2Request) -> et::ReplicateEventsV2Request {
    et::ReplicateEventsV2Request {
        domain_uuid: req.domain_uuid.unwrap_or_default(),
        workflow_execution: Some(req.workflow_execution),
        version_history_items: req.version_history_items.unwrap_or_default(),
        events: req.events,
        new_run_events: req.new_run_events,
    }
}

/// Engine errors back into the replication error union (keeps `RetryTaskV2`,
/// `EntityNotExists`, `ServiceBusy`, ... distinguishable for the task executor).
pub fn history_error_to_replication(e: HistoryError) -> ReplicationError {
    match e {
        HistoryError::Cadence(c) => match *c {
            CadenceError::BadRequest(x) => ReplicationError::BadRequest(x.to_string()),
            CadenceError::EntityNotExists(x) => ReplicationError::EntityNotExists(x.to_string()),
            CadenceError::ServiceBusy(x) => ReplicationError::ServiceBusy(x.to_string()),
            CadenceError::RetryTaskV2(x) => ReplicationError::RetryTaskV2(x),
            CadenceError::DomainNotActive(x) => ReplicationError::DomainNotActive(x.to_string()),
            CadenceError::WorkflowAlreadyStarted(x) => {
                ReplicationError::WorkflowExecutionAlreadyStarted(x.to_string())
            }
            CadenceError::WorkflowAlreadyCompleted(x) => {
                ReplicationError::WorkflowExecutionAlreadyCompleted(x.to_string())
            }
            CadenceError::LimitExceeded(x) => ReplicationError::LimitExceeded(x.to_string()),
            CadenceError::ShardOwnershipLost(x) => ReplicationError::ShardOwnershipLost(x.to_string()),
            other => ReplicationError::Other(other.to_string()),
        },
        HistoryError::MissingVersionHistories => ReplicationError::MissingVersionHistories,
        HistoryError::PersistenceShardOwnershipLost(m) => ReplicationError::ShardOwnershipLost(m),
        HistoryError::DeadlineExceeded => ReplicationError::DeadlineExceeded("context deadline exceeded".into()),
        other => ReplicationError::Other(other.to_string()),
    }
}

pub(crate) async fn with_timeout<T>(
    timeout: Duration,
    fut: impl std::future::Future<Output = Result<T, HistoryError>>,
) -> ReplResult<T> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(r) => r.map_err(history_error_to_replication),
        Err(_) => Err(ReplicationError::DeadlineExceeded("context deadline exceeded".into())),
    }
}

#[async_trait]
impl Engine for LocalReplicationEngine {
    async fn sync_shard_status(&self, req: SyncShardStatusRequest, timeout: Duration) -> ReplResult<()> {
        match self.engine() {
            Ok(engine) => {
                let r = et::SyncShardStatusRequest {
                    source_cluster: req.source_cluster,
                    shard_id: req.shard_id,
                    timestamp: req.timestamp,
                };
                with_timeout(timeout, async move { engine.sync_shard_status(&r).await }).await
            }
            Err(_) => {
                // Engine not built yet: still advance the remote cluster clock.
                if let Some(ts) = req.timestamp {
                    self.shard.set_current_time(&req.source_cluster, ts);
                }
                Ok(())
            }
        }
    }
    async fn sync_activity(&self, req: SyncActivityRequest, timeout: Duration) -> ReplResult<()> {
        let engine = self.engine()?;
        let r = to_engine_sync_activity(req);
        with_timeout(timeout, async move { engine.sync_activity(&r).await }).await
    }
    async fn replicate_events_v2(&self, req: ReplicateEventsV2Request, timeout: Duration) -> ReplResult<()> {
        let engine = self.engine()?;
        let r = to_engine_replicate_events(req);
        with_timeout(timeout, async move { engine.replicate_events_v2(&r).await }).await
    }
}
