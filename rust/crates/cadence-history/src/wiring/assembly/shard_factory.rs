//! group: assembly/shard_factory
//!
//! Per-shard `ShardContext` creation over the real SQL shard store (range-id
//! fencing through `ShardPersistence`) plus the adapters that let one
//! `shard_context::ShardContext` satisfy the engine, task and replication
//! shard-facing traits. Queue (queue / queuev2) adapters belong to unit D2.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::ShardClock;
use crate::shard::ShardStore;
use crate::shard_context::{
    ClusterInfo, HistoryTaskCategory as ShardCategory, ShardContext, ShardContextError,
};
use crate::task::deps as task;
use crate::taskdlq::interface::TaskReinjector;
use crate::wiring::assembly::env::HistoryEnv;
use crate::wiring::resources::domain_cache::HistoryDomainCache;

/// Maps a `ShardContextError` to the error History returns to callers.
pub fn shard_error_to_history(e: ShardContextError) -> HistoryError {
    match e {
        ShardContextError::ShardClosed { .. } => {
            cadence_api::errors::ShardOwnershipLostError::new("shard closed").into()
        }
        ShardContextError::ShardOwnershipLost(m) => cadence_api::errors::ShardOwnershipLostError::new(m).into(),
        other => HistoryError::internal(other.to_string()),
    }
}

/// `cluster.Metadata` subset used by the shard context.
pub struct ShardClusterInfo {
    current: String,
    enabled: Vec<String>,
}

impl ShardClusterInfo {
    pub fn from_env(env: &HistoryEnv) -> Self {
        let md = env.cluster.metadata();
        Self {
            current: md.current_cluster_name.clone(),
            enabled: md.enabled_clusters().map(|(n, _)| n.clone()).collect(),
        }
    }
}

impl ClusterInfo for ShardClusterInfo {
    fn current_cluster_name(&self) -> String {
        self.current.clone()
    }
    fn enabled_cluster_names(&self) -> Vec<String> {
        self.enabled.clone()
    }
}

/// Builds shard contexts: ensure the shard row exists, read it, then acquire
/// (renew range id with stealing) through `ShardContext::acquire`.
pub struct ShardFactory {
    env: Arc<HistoryEnv>,
}

impl ShardFactory {
    pub fn new(env: Arc<HistoryEnv>) -> Self {
        Self { env }
    }

    /// Blocking (the persistence bridge is sync); call from `spawn_blocking`.
    pub fn new_shard_context(
        &self,
        shard_id: i32,
        close_callback: Arc<dyn Fn(i32) + Send + Sync>,
    ) -> HResult<Arc<ShardContext>> {
        let env = &self.env;
        let id = shard_id as u32;
        let store = &env.persistence.shard_store;
        let existing = store.get_shard(id).map_err(|e| HistoryError::internal(e.to_string()))?;
        if existing.is_none() {
            store
                .create_shard(id, &env.identity)
                .map_err(|e| HistoryError::internal(e.to_string()))?;
        }
        let info = env.persistence.shard_persistence.get_shard(shard_id).map_err(shard_error_to_history)?;
        let cfg = &env.config;
        let clock = env.clock.clone();
        let time_source: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
            crate::engine::deps::TimeSource::now(&*clock)
        });
        ShardContext::acquire(
            info,
            &env.identity,
            cfg.range_size_bits,
            (cfg.shard_update_min_interval)().as_nanos() as i64,
            (cfg.timer_processor_max_time_shift)().as_nanos() as i64,
            env.persistence.shard_persistence.clone(),
            Arc::new(ShardClusterInfo::from_env(env)),
            time_source,
            close_callback,
        )
        .map_err(shard_error_to_history)
    }
}

// ---------------------------------------------------------------------------
// engine::ShardClock
// ---------------------------------------------------------------------------

pub struct ShardClockAdapter(pub Arc<ShardContext>);

impl ShardClock for ShardClockAdapter {
    fn set_current_time(&self, cluster: &str, timestamp: i64) {
        self.0.set_current_time(cluster, timestamp);
    }
}

// ---------------------------------------------------------------------------
// Active cluster manager (engine + task traits)
// ---------------------------------------------------------------------------

/// Non active-active domains only: the active cluster of a workflow is the
/// domain's active cluster.
pub struct DomainActiveClusterManager {
    domains: Arc<HistoryDomainCache>,
}

impl DomainActiveClusterManager {
    pub fn new(domains: Arc<HistoryDomainCache>) -> Self {
        Self { domains }
    }
}

#[async_trait]
impl crate::engine::runtime::ActiveClusterManager for DomainActiveClusterManager {
    async fn active_cluster_by_workflow(&self, domain_id: &str, _workflow_id: &str, _run_id: &str) -> HResult<String> {
        let entry = crate::engine::deps::DomainCache::get_domain_by_id(&*self.domains, domain_id)?;
        entry
            .active_clusters
            .first()
            .map(|(_, name)| name.clone())
            .ok_or_else(|| HistoryError::internal(format!("domain {domain_id} has no active cluster")))
    }
}

#[async_trait]
impl task::ActiveClusterManager for DomainActiveClusterManager {
    async fn get_active_cluster_info_by_workflow(
        &self,
        domain_id: &str,
        _workflow_id: &str,
        _run_id: &str,
    ) -> task::Result<task::ActiveClusterInfo> {
        let entry = task::DomainCache::get_domain_by_id(&*self.domains, domain_id)?;
        Ok(task::ActiveClusterInfo {
            active_cluster_name: entry.replication_config.active_cluster_name.clone(),
            failover_version: entry.failover_version,
        })
    }

    async fn get_active_cluster_selection_policy_for_workflow(
        &self,
        _domain_id: &str,
        _workflow_id: &str,
        _run_id: &str,
    ) -> task::Result<Option<task::ActiveClusterSelectionPolicy>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// task::deps::ShardContext
// ---------------------------------------------------------------------------

/// `TaskReinjector` that D2 binds once the queue processors exist. Until then
/// reinjection fails explicitly.
#[derive(Default)]
pub struct ReinjectorSlot {
    inner: OnceLock<Arc<dyn TaskReinjector>>,
}

impl ReinjectorSlot {
    pub fn bind(&self, reinjector: Arc<dyn TaskReinjector>) -> bool {
        self.inner.set(reinjector).is_ok()
    }
}

#[async_trait]
impl TaskReinjector for ReinjectorSlot {
    async fn reinject_history_tasks(
        &self,
        ctx: &CancellationToken,
        tasks: Vec<Arc<dyn task::PersistenceTask>>,
    ) -> Result<(), task::TaskError> {
        match self.inner.get() {
            Some(r) => r.reinject_history_tasks(ctx, tasks).await,
            None => Err(task::TaskError::InternalService("task reinjector not bound to queue processors".into())),
        }
    }
}

/// Port of Go `shard.Context.ReinjectHistoryTasks` for the SQL persistence
/// backend: closed check, group by (domain, workflow), resolve domain entries
/// before the write path, then `ExecutionManager.CreateHistoryTasks`, which
/// SQL does not implement (Go `errHistoryTasksNotImplemented`). The task-id
/// allocation Go performs before that call is not observable on SQL because
/// the persistence call always fails.
pub struct ShardTaskReinjector {
    shard: Arc<ShardContext>,
    domain_cache: Arc<dyn task::DomainCache>,
}

impl ShardTaskReinjector {
    pub fn new(shard: Arc<ShardContext>, domain_cache: Arc<dyn task::DomainCache>) -> Arc<Self> {
        Arc::new(Self { shard, domain_cache })
    }
}

#[async_trait]
impl TaskReinjector for ShardTaskReinjector {
    async fn reinject_history_tasks(
        &self,
        _ctx: &CancellationToken,
        tasks: Vec<Arc<dyn task::PersistenceTask>>,
    ) -> Result<(), task::TaskError> {
        if let Some(ShardContextError::ShardClosed { closed_at_nanos }) = self.shard.closed_error() {
            return Err(task::TaskError::ShardClosed { closed_at: task::Time(closed_at_nanos) });
        }
        let mut domains = std::collections::HashSet::new();
        for t in &tasks {
            domains.insert(t.get_domain_id().to_owned());
        }
        for d in &domains {
            self.domain_cache.get_domain_by_id(d)?;
        }
        Err(task::TaskError::InternalService("create history tasks not implemented for SQL".into()))
    }
}

struct ShardService {
    scopes: Arc<dyn task::DomainMetricsScopeCache>,
    dlq: Arc<dyn crate::taskdlq::processor::HistoryTaskDLQManager>,
}

impl task::Service for ShardService {
    fn get_domain_metrics_scope_cache(&self) -> Arc<dyn task::DomainMetricsScopeCache> {
        self.scopes.clone()
    }
    fn get_history_task_dlq_manager(&self) -> Arc<dyn crate::taskdlq::processor::HistoryTaskDLQManager> {
        self.dlq.clone()
    }
}

/// Adapter making a `shard_context::ShardContext` a `task::deps::ShardContext`.
pub struct TaskShardContext {
    shard: Arc<ShardContext>,
    env: Arc<HistoryEnv>,
    service: Arc<ShardService>,
    active: Arc<DomainActiveClusterManager>,
    pub reinjector: Arc<ReinjectorSlot>,
}

impl TaskShardContext {
    pub fn new(env: Arc<HistoryEnv>, shard: Arc<ShardContext>) -> Arc<Self> {
        let service = Arc::new(ShardService {
            scopes: env.domain_scope_cache.clone(),
            dlq: Arc::new(crate::wiring::persistence::shard_events::SqlHistoryTaskDLQManager),
        });
        Arc::new(Self {
            active: Arc::new(DomainActiveClusterManager::new(env.domain_cache.clone())),
            reinjector: Arc::new(ReinjectorSlot::default()),
            shard,
            env,
            service,
        })
    }
}

fn to_shard_category(c: task::HistoryTaskCategory) -> ShardCategory {
    match c {
        task::HistoryTaskCategory::Transfer | task::HistoryTaskCategory::CrossCluster => ShardCategory::Transfer,
        task::HistoryTaskCategory::Timer => ShardCategory::Timer,
        task::HistoryTaskCategory::Replication => ShardCategory::Replication,
    }
}

impl task::ShardContext for TaskShardContext {
    fn get_shard_id(&self) -> i32 {
        self.shard.shard_id()
    }
    fn get_time_source(&self) -> Arc<dyn task::TimeSource> {
        self.env.clock.clone()
    }
    fn get_config(&self) -> Arc<dyn task::HistoryConfig> {
        self.env.config_adapter.clone()
    }
    fn get_domain_cache(&self) -> Arc<dyn task::DomainCache> {
        self.env.domain_cache.clone()
    }
    fn get_cluster_metadata(&self) -> Arc<dyn task::ClusterMetadata> {
        self.env.cluster.clone()
    }
    fn get_active_cluster_manager(&self) -> Arc<dyn task::ActiveClusterManager> {
        self.active.clone()
    }
    fn get_metrics_client(&self) -> Arc<dyn task::MetricsClient> {
        self.env.metrics.clone()
    }
    fn get_service(&self) -> Arc<dyn task::Service> {
        self.service.clone()
    }
    fn get_logger(&self) -> task::Logger {
        task::Logger::new()
    }
    fn update_if_needed_and_get_queue_max_read_level(
        &self,
        category: task::HistoryTaskCategory,
        cluster: &str,
    ) -> task::HistoryTaskKey {
        let k = self.shard.update_if_needed_and_get_queue_max_read_level(to_shard_category(category), cluster);
        task::HistoryTaskKey { scheduled_time: task::Time(k.scheduled_time_nanos), task_id: k.task_id }
    }
    fn as_task_reinjector(&self) -> Arc<dyn TaskReinjector> {
        self.reinjector.clone()
    }
}
