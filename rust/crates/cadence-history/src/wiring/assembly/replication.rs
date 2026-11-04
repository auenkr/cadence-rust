//! Assembly helpers for the replication package (go/service/history/replication).
//!
//! Real adapters: `ThriftPayloadSerializer`, `ReplicationShardAdapter`,
//! `build_task_ack_manager`.
//!
//! Documented gaps (memory/history-port-wiring-20261105.md, Unit D2/D3):
//! * `UnwiredHistoryResender` returns an error: the remote admin client based
//!   `ndc.HistoryResender` is not ported to the wiring layer yet.
//! * `LocalReplicationEngine` only implements `sync_shard_status`; `sync_activity`
//!   and `replicate_events_v2` delegate to an injected engine (`inner`) and fail
//!   when none is supplied.
//! * No SQL `MutableStateProvider`; the hydrator is built deferred by the caller.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::types::{DataBlob, FailoverMarkerAttributes};

use crate::replication::deps::{
    ClientBean, DomainCache, Engine, ExecutionManager, HistoryResender, PayloadSerializer, ReplicationError,
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

/// Placeholder for `ndc.HistoryResender` (see module docs).
pub struct UnwiredHistoryResender;

#[async_trait]
impl HistoryResender for UnwiredHistoryResender {
    async fn send_single_workflow_history(
        &self,
        _remote_cluster: &str,
        _domain_id: &str,
        _workflow_id: &str,
        _run_id: &str,
        _start_event_id: Option<i64>,
        _start_event_version: Option<i64>,
        _end_event_id: Option<i64>,
        _end_event_version: Option<i64>,
    ) -> ReplResult<()> {
        Err(ReplicationError::Other("history resender is not wired".into()))
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

/// `engine.Engine` for the replication package: `SyncShardStatus` is local, the
/// rest is delegated to `inner` (the real engine adapter).
pub struct LocalReplicationEngine {
    pub shard: Arc<ShardContext>,
    pub inner: Option<Arc<dyn Engine>>,
}

#[async_trait]
impl Engine for LocalReplicationEngine {
    async fn sync_shard_status(&self, req: SyncShardStatusRequest, _timeout: Duration) -> ReplResult<()> {
        if let Some(ts) = req.timestamp {
            self.shard.set_current_time(&req.source_cluster, ts);
        }
        Ok(())
    }
    async fn sync_activity(&self, req: SyncActivityRequest, timeout: Duration) -> ReplResult<()> {
        match &self.inner {
            Some(e) => e.sync_activity(req, timeout).await,
            None => Err(ReplicationError::Other("replication engine is not wired".into())),
        }
    }
    async fn replicate_events_v2(&self, req: ReplicateEventsV2Request, timeout: Duration) -> ReplResult<()> {
        match &self.inner {
            Some(e) => e.replicate_events_v2(req, timeout).await,
            None => Err(ReplicationError::Other("replication engine is not wired".into())),
        }
    }
}
