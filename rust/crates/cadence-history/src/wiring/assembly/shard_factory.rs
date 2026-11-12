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
use cadence_persistence::history_contract::{self as pc, GoExecutionStore};
use cadence_persistence::sqlblobs;

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

/// Go `activecluster.Manager` over the History domain cache and execution
/// persistence (see `active_cluster_manager`).
pub struct DomainActiveClusterManager {
    inner: crate::wiring::assembly::active_cluster_manager::ActiveClusterManagerImpl,
}

impl DomainActiveClusterManager {
    pub fn new(domains: Arc<HistoryDomainCache>, executions: Arc<dyn GoExecutionStore>, num_shards: i32) -> Self {
        let domain_fn: crate::wiring::assembly::active_cluster_manager::DomainIdToDomainFn = Arc::new(move |id: &str| {
            let entry = crate::engine::deps::DomainCache::get_domain_by_id(&*domains, id)?;
            let active_active = domains
                .inner()
                .get_domain_by_id(id)
                .map_err(|e| HistoryError::internal(e.to_string()))?
                .replication
                .active_clusters
                .as_ref()
                .and_then(|ac| ac.active_clusters_by_cluster_attribute.as_ref())
                .is_some_and(|scopes| !scopes.is_empty());
            Ok((entry, active_active))
        });
        Self {
            inner: crate::wiring::assembly::active_cluster_manager::ActiveClusterManagerImpl::new(domain_fn, Arc::new(executions), num_shards),
        }
    }
}

fn history_to_task_error(e: HistoryError) -> task::TaskError {
    if e.is_entity_not_exists() {
        task::TaskError::EntityNotExists(e.to_string())
    } else {
        task::TaskError::InternalService(e.to_string())
    }
}

#[async_trait]
impl crate::engine::runtime::ActiveClusterManager for DomainActiveClusterManager {
    async fn active_cluster_by_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<String> {
        self.inner.get_active_cluster_info_by_workflow(domain_id, workflow_id, run_id).await.map(|i| i.active_cluster_name)
    }

    async fn failover_version_by_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<Option<i64>> {
        self.inner.get_active_cluster_info_by_workflow(domain_id, workflow_id, run_id).await.map(|i| Some(i.failover_version))
    }

    async fn active_cluster_selection_policy_for_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> HResult<Option<cadence_api::types::ActiveClusterSelectionPolicy>> {
        self.inner.get_active_cluster_selection_policy_for_workflow(domain_id, workflow_id, run_id).await
    }
}

#[async_trait]
impl task::ActiveClusterManager for DomainActiveClusterManager {
    async fn get_active_cluster_info_by_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> task::Result<task::ActiveClusterInfo> {
        self.inner
            .get_active_cluster_info_by_workflow(domain_id, workflow_id, run_id)
            .await
            .map(|i| task::ActiveClusterInfo { active_cluster_name: i.active_cluster_name, failover_version: i.failover_version })
            .map_err(history_to_task_error)
    }

    async fn get_active_cluster_selection_policy_for_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> task::Result<Option<task::ActiveClusterSelectionPolicy>> {
        self.inner
            .get_active_cluster_selection_policy_for_workflow(domain_id, workflow_id, run_id)
            .await
            .map(|p| {
                p.map(|p| task::ActiveClusterSelectionPolicy {
                    cluster_attribute: p.cluster_attribute.map(|a| task::ClusterAttribute { scope: a.scope, name: a.name }),
                })
            })
            .map_err(history_to_task_error)
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

/// Port of Go `shard.Context.ReinjectHistoryTasks`: closed check, group by
/// (domain, workflow), resolve domain entries before taking the write gate,
/// allocate task ids (transfer/timer, replication last), then
/// `ExecutionManager.CreateHistoryTasks`. SQL stores return Go's
/// `errHistoryTasksNotImplemented`; Cassandra performs the write.
pub struct ShardTaskReinjector {
    shard: Arc<ShardContext>,
    domain_cache: Arc<dyn task::DomainCache>,
    executions: Arc<dyn GoExecutionStore>,
    current_cluster: String,
}

impl ShardTaskReinjector {
    pub fn new(
        shard: Arc<ShardContext>,
        domain_cache: Arc<dyn task::DomainCache>,
        executions: Arc<dyn GoExecutionStore>,
        current_cluster: String,
    ) -> Arc<Self> {
        Arc::new(Self { shard, domain_cache, executions, current_cluster })
    }
}

fn kind_i32(k: task::TaskListKind) -> i32 {
    match k {
        task::TaskListKind::Normal => 0,
        task::TaskListKind::Sticky => 1,
        task::TaskListKind::Ephemeral => 2,
    }
}

/// Typed history task -> persistence contract rows (the inverse of the queue decode).
enum PcTask {
    Transfer(pc::TransferTask),
    Timer(pc::TimerTask),
}

fn to_pc_task(t: &dyn task::PersistenceTask) -> Result<PcTask, task::TaskError> {
    use task as d;
    let any = t.as_any();
    let mut tr = pc::TransferTask {
        domain_id: t.get_domain_id().to_owned(),
        workflow_id: t.get_workflow_id().to_owned(),
        run_id: t.get_run_id().to_owned(),
        visibility_time_unix_nanos: t.get_visibility_timestamp().0,
        task_id: t.get_task_id(),
        task_type: t.get_task_type(),
        version: t.get_version(),
        ..Default::default()
    };
    let mut tm = pc::TimerTask {
        domain_id: tr.domain_id.clone(),
        workflow_id: tr.workflow_id.clone(),
        run_id: tr.run_id.clone(),
        visibility_timestamp_unix_nanos: tr.visibility_time_unix_nanos,
        task_id: tr.task_id,
        task_type: tr.task_type,
        version: tr.version,
        ..Default::default()
    };
    if let Some(x) = any.downcast_ref::<d::ActivityTask>() {
        tr.target_domain_id = x.target_domain_id.clone();
        tr.task_list = x.task_list.clone();
        tr.schedule_id = x.schedule_id;
        tr.original_task_list = x.original_task_list.clone();
        tr.original_task_list_kind = kind_i32(x.original_task_list_kind);
    } else if let Some(x) = any.downcast_ref::<d::DecisionTask>() {
        tr.target_domain_id = x.target_domain_id.clone();
        tr.task_list = x.task_list.clone();
        tr.schedule_id = x.schedule_id;
        tr.original_task_list = x.original_task_list.clone();
        tr.original_task_list_kind = kind_i32(x.original_task_list_kind);
    } else if let Some(x) = any.downcast_ref::<d::CancelExecutionTask>() {
        tr.target_domain_id = x.target_domain_id.clone();
        tr.target_workflow_id = x.target_workflow_id.clone();
        tr.target_run_id = x.target_run_id.clone();
        tr.target_child_workflow_only = x.target_child_workflow_only;
        tr.schedule_id = x.initiated_id;
    } else if let Some(x) = any.downcast_ref::<d::SignalExecutionTask>() {
        tr.target_domain_id = x.target_domain_id.clone();
        tr.target_workflow_id = x.target_workflow_id.clone();
        tr.target_run_id = x.target_run_id.clone();
        tr.target_child_workflow_only = x.target_child_workflow_only;
        tr.schedule_id = x.initiated_id;
    } else if let Some(x) = any.downcast_ref::<d::StartChildExecutionTask>() {
        tr.target_domain_id = x.target_domain_id.clone();
        tr.target_workflow_id = x.target_workflow_id.clone();
        tr.schedule_id = x.initiated_id;
        tr.task_list = x.task_list.clone();
    } else if let Some(x) = any.downcast_ref::<d::UpsertWorkflowSearchAttributesTask>() {
        tr.task_list = x.task_list.clone();
    } else if let Some(x) = any.downcast_ref::<d::RecordChildExecutionCompletedTask>() {
        tr.target_domain_id = x.target_domain_id.clone();
        tr.target_workflow_id = x.target_workflow_id.clone();
        tr.target_run_id = x.target_run_id.clone();
    } else if let Some(x) = any.downcast_ref::<d::ApplyParentClosePolicyTask>() {
        tr.target_domain_ids = x.target_domain_ids.clone();
    } else if let Some(x) = any.downcast_ref::<d::DecisionTimeoutTask>() {
        tm.event_id = x.event_id;
        tm.schedule_attempt = x.schedule_attempt;
        tm.timeout_type = x.timeout_type;
    } else if let Some(x) = any.downcast_ref::<d::ActivityTimeoutTask>() {
        tm.event_id = x.event_id;
        tm.schedule_attempt = x.attempt;
        tm.timeout_type = x.timeout_type;
    } else if let Some(x) = any.downcast_ref::<d::UserTimerTask>() {
        tm.event_id = x.event_id;
    } else if let Some(x) = any.downcast_ref::<d::ActivityRetryTimerTask>() {
        tm.event_id = x.event_id;
        tm.schedule_attempt = x.attempt;
    } else if let Some(x) = any.downcast_ref::<d::WorkflowBackoffTimerTask>() {
        tm.event_id = x.event_id;
        tm.timeout_type = x.timeout_type;
    }
    match t.get_task_category() {
        task::HistoryTaskCategory::Transfer => Ok(PcTask::Transfer(tr)),
        task::HistoryTaskCategory::Timer => Ok(PcTask::Timer(tm)),
        other => Err(task::TaskError::InternalService(format!(
            "CreateHistoryTasks only supports transfer and timer tasks, got category: {other:?}"
        ))),
    }
}

fn blob(r: Result<Vec<u8>, impl std::fmt::Display>) -> Result<pc::DataBlob, task::TaskError> {
    r.map(|data| pc::DataBlob { encoding: "thriftrw".to_string(), data })
        .map_err(|e| task::TaskError::InternalService(format!("encode task: {e}")))
}

#[async_trait]
impl TaskReinjector for ShardTaskReinjector {
    async fn reinject_history_tasks(
        &self,
        _ctx: &CancellationToken,
        tasks: Vec<Arc<dyn task::PersistenceTask>>,
    ) -> Result<(), task::TaskError> {
        let closed = |s: &ShardContext| match s.closed_error() {
            Some(ShardContextError::ShardClosed { closed_at_nanos }) => {
                Some(task::TaskError::ShardClosed { closed_at: task::Time(closed_at_nanos) })
            }
            _ => None,
        };
        if let Some(e) = closed(&self.shard) {
            return Err(e);
        }
        // Group by (domain, workflow); Go iterates a map, order is not significant.
        let mut by_exec: std::collections::BTreeMap<(String, String), Vec<PcTask>> = Default::default();
        let mut domains = std::collections::HashMap::new();
        for t in &tasks {
            by_exec
                .entry((t.get_domain_id().to_owned(), t.get_workflow_id().to_owned()))
                .or_default()
                .push(to_pc_task(t.as_ref())?);
        }
        for (d, _) in by_exec.keys() {
            if !domains.contains_key(d) {
                domains.insert(d.clone(), self.domain_cache.get_domain_by_id(d)?);
            }
        }

        // Go takes the shard lock from id allocation through the write.
        let _gate = self.shard.lock_task_writes().await;
        let shard_err = |e: ShardContextError| task::TaskError::InternalService(e.to_string());
        let mut max_read_level = 0i64;
        let mut transfer = Vec::new();
        let mut timer = Vec::new();
        for ((domain_id, _wf), items) in by_exec {
            let entry = &domains[&domain_id];
            let mut tr: Vec<pc::TransferTask> = Vec::new();
            let mut tm: Vec<pc::TimerTask> = Vec::new();
            for i in items {
                match i {
                    PcTask::Transfer(t) => tr.push(t),
                    PcTask::Timer(t) => tm.push(t),
                }
            }
            let mut slots: Vec<(i64, i64)> = tr.iter().map(|t| (0, t.visibility_time_unix_nanos)).collect();
            self.shard.allocate_transfer_ids(&mut slots, &mut max_read_level).map_err(shard_err)?;
            for (t, s) in tr.iter_mut().zip(&slots) {
                t.task_id = s.0;
                t.visibility_time_unix_nanos = s.1;
            }
            // Timer ids after transfer ids (Cadence indexer relies on the order).
            for t in tm.iter_mut() {
                let cluster = if t.version != task::EMPTY_VERSION {
                    entry.replication_config.active_cluster_name.clone()
                } else {
                    self.current_cluster.clone()
                };
                let mut slot = [(0i64, t.visibility_timestamp_unix_nanos)];
                self.shard.allocate_timer_ids(&cluster, &mut slot).map_err(shard_err)?;
                t.task_id = slot[0].0;
                t.visibility_timestamp_unix_nanos = slot[0].1;
            }
            transfer.extend(tr);
            timer.extend(tm);
        }

        let mut tasks_by_category = std::collections::BTreeMap::new();
        let mut rows = Vec::new();
        for t in &transfer {
            rows.push(pc::Task {
                visibility_timestamp_unix_nanos: t.visibility_time_unix_nanos,
                task_type: t.task_type,
                task_id: t.task_id,
                data: blob(
                    sqlblobs::transfer_task_to_struct(t)
                        .and_then(|s| sqlblobs::Encoding::ThriftRW.encode(&s)),
                )?,
            });
        }
        if !rows.is_empty() {
            tasks_by_category.insert(pc::HistoryTaskCategory { category_type: 1, category_id: 1 }, rows);
        }
        let mut rows = Vec::new();
        for t in &timer {
            rows.push(pc::Task {
                visibility_timestamp_unix_nanos: t.visibility_timestamp_unix_nanos,
                task_type: t.task_type,
                task_id: t.task_id,
                data: blob(
                    sqlblobs::timer_task_to_struct(t).and_then(|s| sqlblobs::Encoding::ThriftRW.encode(&s)),
                )?,
            });
        }
        if !rows.is_empty() {
            tasks_by_category.insert(pc::HistoryTaskCategory { category_type: 2, category_id: 2 }, rows);
        }

        if let Some(e) = closed(&self.shard) {
            return Err(e);
        }
        let res = self
            .executions
            .create_history_tasks(pc::CreateHistoryTasksRequest {
                shard_id: self.shard.shard_id(),
                range_id: self.shard.range_id(),
                current_timestamp_unix_nanos: self.shard.now(),
                tasks_by_category,
            })
            .await;
        match res {
            Ok(()) => {
                self.shard.update_max_read_level(max_read_level);
                Ok(())
            }
            Err(pc::PersistenceError::ShardOwnershipLost { .. }) => {
                tracing::warn!("Closing shard: ReinjectHistoryTasks failed due to stolen shard.");
                self.shard.close_shard(None);
                Err(task::TaskError::ShardOwnershipLost("ReinjectHistoryTasks: shard ownership lost".into()))
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to re-inject history DLQ tasks into the executions table.");
                Err(task::TaskError::InternalService(e.to_string()))
            }
        }
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
            active: env.active_cluster_manager.clone(),
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
