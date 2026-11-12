//! group: assembly/env
//!
//! `HistoryEnv`: every shared, process-wide dependency of the History service
//! (persistence bundle, resources, config), built once at start-up and shared by
//! all shards. Persistence is opened from the same `PersistenceConfig` the
//! Matching launcher uses (`open_configured_default_store`).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use cadence_common::cluster::Metadata;
use cadence_common::membership::Host;
use cadence_matching::matching_domain_cache::MatchingDomainCache;
use cadence_matching::matching_task_store::open_configured_default_store;
use cadence_persistence::history_contract::{GoExecutionStore, GoHistoryV2Store};
use cadence_persistence::history_reader::HistoryNodeStore;
use cadence_persistence::history_sql::ShardStore as RowShardStore;
use cadence_persistence::sql_history_store::SqlHistoryV2Store;
use cadence_persistence::sql_workflow_store::SqlWorkflowStore;
use cadence_persistence::sqlblobs::Encoding;
use cadence_service_core::config::PersistenceConfig;

use crate::config::Config;
use crate::config_collection::DynamicClient;
use crate::engine::deps::HistoryTaskCategory;
use crate::engine::runtime::{
    HistoryEngine, QueueProcessor, ReplicationAckManager, ReplicationDLQHandler,
    ReplicationHydrator, Replicator,
};
use crate::shard_context::ShardContext;
use crate::wiring::persistence::shard_events::{SqlShardPersistence, SqlShardStore};
use crate::wiring::resources::client_checker::VersionChecker;
use crate::wiring::resources::clock::SystemClock;
use crate::wiring::resources::cluster::ClusterMetadataImpl;
use crate::wiring::resources::config_adapters::ConfigAdapter;
use crate::wiring::resources::domain_cache::HistoryDomainCache;
use crate::wiring::resources::membership::MembershipResolver;
use crate::wiring::resources::metrics::{DomainMetricsScopeCacheImpl, MetricsClientImpl, MetricsRegistry};
use crate::wiring::resources::query_matching::{ChannelResolver, GrpcQueryMatchingClient};

/// SQL persistence bundle shared by all shards (MySQL / PostgreSQL).
#[derive(Clone)]
pub struct HistoryPersistence {
    pub shard_rows: Arc<dyn RowShardStore>,
    /// `request.range_id` / `request.shard_id` are passed per call, so one
    /// instance serves every shard.
    pub executions: Arc<dyn GoExecutionStore>,
    pub history_v2: Arc<dyn GoHistoryV2Store>,
    pub history_nodes: Arc<dyn HistoryNodeStore>,
    pub shard_store: Arc<SqlShardStore>,
    pub shard_persistence: Arc<SqlShardPersistence>,
}

/// Inputs to [`HistoryEnv::build`].
pub struct HistoryEnvParams {
    pub config: Arc<Config>,
    pub persistence: PersistenceConfig,
    pub cluster: Metadata,
    /// Membership identity of this host (`Host::identity`).
    pub who_am_i: String,
    pub hosts: Vec<Host>,
    /// Resolves a Matching address to a gRPC channel (query forwarding).
    pub matching_resolver: ChannelResolver,
    pub dynamic_client: Option<Arc<dyn DynamicClient>>,
}

/// All process-wide shared dependencies.
pub struct HistoryEnv {
    pub config: Arc<Config>,
    pub config_adapter: Arc<ConfigAdapter>,
    pub persistence: HistoryPersistence,
    pub clock: Arc<SystemClock>,
    pub cluster: Arc<ClusterMetadataImpl>,
    pub matching_domain_cache: Arc<MatchingDomainCache>,
    pub domain_cache: Arc<HistoryDomainCache>,
    pub metrics_registry: Arc<MetricsRegistry>,
    pub metrics: Arc<MetricsClientImpl>,
    pub domain_scope_cache: Arc<DomainMetricsScopeCacheImpl>,
    pub membership: Arc<MembershipResolver>,
    pub client_checker: Arc<VersionChecker>,
    pub query_matching: Arc<GrpcQueryMatchingClient>,
    /// Visibility (`executions_visibility`) writer shared by the transfer / timer executors.
    pub visibility: Arc<dyn crate::wiring::persistence::visibility::VisibilityWriter>,
    /// Matching channel resolver (add-task RPCs of the transfer executors).
    pub matching_resolver: ChannelResolver,
    pub dynamic_client: Option<Arc<dyn DynamicClient>>,
    pub identity: String,
    /// Go `failover.NewCoordinator` (handler-level; started by the handler).
    pub failover_coordinator: Arc<crate::failover::CoordinatorImpl>,
    /// Go handler `dlqHostLimiter`: `taskdlq.NewHostLimiter(config.HistoryTaskDLQProcessorHostConcurrency())`,
    /// shared by every shard's task DLQ processor.
    pub dlq_host_limiter: crate::taskdlq::HostLimiter,
}

impl HistoryEnv {
    /// Opens persistence and builds every shared resource.
    pub async fn build(params: HistoryEnvParams) -> anyhow::Result<Arc<Self>> {
        let HistoryEnvParams { config, persistence, cluster, who_am_i, hosts, matching_resolver, dynamic_client } =
            params;
        let active = cluster.current_cluster_name.clone();

        let stores = tokio::task::spawn_blocking(move || {
            open_configured_default_store(
                &persistence,
                &active,
                Arc::new(|| false),
                Arc::new(|| "thriftrw".to_string()),
                false,
            )
        })
        .await??;

        let shard_rows = stores
            .history_shard_store
            .clone()
            .ok_or_else(|| anyhow::anyhow!("History requires a shard store"))?;
        let history_nodes = stores
            .history_node_store
            .clone()
            .ok_or_else(|| anyhow::anyhow!("History requires a history_node store"))?;
        let source = stores
            .domain_metadata_source
            .clone()
            .ok_or_else(|| anyhow::anyhow!("configured default store has no domain metadata source"))?;

        let (executions, history_v2, visibility): (
            Arc<dyn GoExecutionStore>,
            Arc<dyn GoHistoryV2Store>,
            Arc<dyn crate::wiring::persistence::visibility::VisibilityWriter>,
        ) = if let Some(c) = stores.history_cassandra.clone() {
            (
                c.executions,
                c.history_v2,
                Arc::new(crate::wiring::persistence::visibility::CassandraVisibilityWriter::new(c.visibility)),
            )
        } else {
            let backend = stores
                .history_sql_backend
                .clone()
                .ok_or_else(|| anyhow::anyhow!("History requires a MySQL, PostgreSQL or Cassandra default store"))?;
            let visibility_backend = stores.history_visibility_sql_backend.clone().unwrap_or_else(|| backend.clone());
            (
                Arc::new(SqlWorkflowStore::new(backend.clone(), Encoding::ThriftRW, 0, 0)),
                Arc::new(SqlHistoryV2Store::new(backend, Encoding::ThriftRW)),
                Arc::new(crate::wiring::persistence::visibility::SqlVisibilityWriter::new(visibility_backend)),
            )
        };
        let persistence = HistoryPersistence {
            shard_store: Arc::new(SqlShardStore::new(shard_rows.clone())),
            shard_persistence: Arc::new(SqlShardPersistence::new(shard_rows.clone())),
            shard_rows,
            executions,
            history_v2,
            history_nodes,
        };

        let matching_domain_cache = Arc::new(MatchingDomainCache::new(source));
        {
            let cache = matching_domain_cache.clone();
            tokio::task::spawn_blocking(move || cache.start())
                .await?
                .map_err(|e| anyhow::anyhow!("start domain cache: {e:?}"))?;
        }

        let domain_persistence = stores
            .domain_metadata_persistence
            .clone()
            .ok_or_else(|| anyhow::anyhow!("configured default store has no domain metadata persistence"))?;
        let metrics_registry = MetricsRegistry::new();
        let config_adapter = Arc::new(ConfigAdapter::new(config.clone()));
        let clock = Arc::new(SystemClock);
        let domain_cache = Arc::new(HistoryDomainCache::new(matching_domain_cache.clone()));
        let metrics = Arc::new(MetricsClientImpl::new(metrics_registry.clone()));
        let failover_coordinator = super::failover::build_failover_coordinator(super::failover::FailoverDeps {
            domain_manager: Arc::new(crate::wiring::resources::failover_domain_manager::PersistenceDomainManager::new(
                domain_persistence,
            )),
            time_source: clock.clone(),
            domain_cache: domain_cache.clone(),
            config: config_adapter.clone(),
            metrics: metrics.clone(),
            history_client: None,
        })
        .coordinator;
        let dlq_host_limiter = crate::taskdlq::new_host_limiter(
            (config.history_task_dlq_processor_host_concurrency)().clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
        );
        Ok(Arc::new(Self {
            dlq_host_limiter,
            config_adapter,
            config,
            persistence,
            clock,
            cluster: Arc::new(ClusterMetadataImpl::new(cluster)),
            domain_cache,
            matching_domain_cache,
            metrics,
            metrics_registry,
            domain_scope_cache: Arc::new(DomainMetricsScopeCacheImpl::default()),
            membership: Arc::new(MembershipResolver::new(who_am_i.clone(), hosts)),
            client_checker: Arc::new(VersionChecker),
            query_matching: Arc::new(GrpcQueryMatchingClient::new(matching_resolver.clone())),
            visibility,
            matching_resolver,
            dynamic_client,
            identity: who_am_i,
            failover_coordinator,
        }))
    }
}

/// Per-shard context handed to the queue and replication factories.
#[derive(Clone)]
pub struct ShardBuildCtx {
    pub env: Arc<HistoryEnv>,
    pub shard: Arc<ShardContext>,
    /// Filled with the engine once it is built, so queue processors that need the
    /// engine can resolve it lazily.
    pub engine_slot: Arc<OnceLock<Weak<HistoryEngine>>>,
    /// Execution core and cache of the shard, shared with the engine.
    pub core: Arc<super::engine_cache::EngineCore>,
    pub cache: Arc<super::engine_cache::EngineExecutionCache>,
}

/// Implemented by the queue unit (D2): builds the per-shard transfer / timer
/// (and cross-cluster) queue processors.
pub trait QueueFactory: Send + Sync {
    fn build_queue_processors(
        &self,
        ctx: &ShardBuildCtx,
    ) -> anyhow::Result<HashMap<HistoryTaskCategory, Arc<dyn QueueProcessor>>>;
}

/// The replication components of one engine.
pub struct ReplicationParts {
    pub dlq_handler: Arc<dyn ReplicationDLQHandler>,
    pub ack_manager: Arc<dyn ReplicationAckManager>,
    pub hydrator: Arc<dyn ReplicationHydrator>,
    pub replicator: Arc<dyn Replicator>,
    /// `replication.TaskStore` of the shard (engine pre-hydration cache).
    pub task_store: Arc<dyn crate::engine::runtime::ReplicationTaskStore>,
    /// Go `replicationDLQHandler` Start/Stop.
    pub dlq_daemon: Arc<dyn crate::replication::deps::Daemon>,
    /// Go `replicationMetricsEmitter` Start/Stop.
    pub metrics_emitter: Arc<dyn crate::replication::deps::Daemon>,
    /// Go `replicationTaskProcessors` Start/Stop.
    pub task_processors: Vec<Arc<dyn crate::replication::deps::Daemon>>,
}

/// Implemented by the replication unit (D3).
pub trait ReplicationFactory: Send + Sync {
    fn build_replication(&self, ctx: &ShardBuildCtx) -> anyhow::Result<ReplicationParts>;
}
