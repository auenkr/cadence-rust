//! Port of the replication wiring in `go/service/history/engine/engineimpl/history_engine.go`
//! (NewEngineWithShardContext, ~L163-303) and `go/service/history/replication/factory.go`.
//!
//! `ReplicationFactoryImpl` builds, per shard: TaskHydrator (deferred), TaskStore,
//! DynamicTaskBatchSizer, TaskAckManager, MetricsEmitter, and per remote cluster a
//! HistoryResender + TaskExecutor + TaskProcessor, plus the DLQ handler. The
//! process-level `TaskFetchers` are shared by all shards and created lazily.
//!
//! Deviations (documented in memory/history-port-wiring-20261105.md, Unit D8):
//! * No engine lifecycle exists, so processors / DLQ handler / metrics emitter are
//!   started when built and stopped by a watcher once the shard is closed.
//! * `Replicator` (NDC apply-events / sync-activity) is not wired: the execution
//!   cache is still `Unwired*`, so `UnwiredReplicator` returns an internal error.
//! * `MutableStateProvider` reads the persisted snapshot directly (no execution
//!   cache lock, so the release fn is a no-op).
//! * `ReplicationMessagesSizeFn` uses the JSON length, not the protobuf size.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::types::{ReplicationTask, VersionHistories};
use cadence_persistence::history_contract::{ActivityInfo as PActivityInfo, GoExecutionStore};
use cadence_persistence::sqlblobs::{activity_info_from_struct, Encoding};

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::{HistoryEngine, ReplicationHydrator, Replicator, Task};
use crate::engine::types as et;
use crate::mutable_state_activity::ActivityInfo;
use crate::replication::deps::{
    ClientBean, Daemon, DomainCache as ReplDomainCache, Engine as ReplEngine, ExecutionManager, HistoryClient,
    HistoryManager, HistoryResender, HistoryTask, Limiter, MutableStateProvider, MutableStateView,
    PayloadSerializer, RealTimeSource, ReleaseFn, ReplicateEventsV2Request, ReplicationConfig, ReplicationError,
    ReplicationMessagesSizeFn, ReplicationMetrics, Result as ReplResult, ShardContext as ReplShard,
    SimpleRateLimiter, SyncActivityRequest, TaskInfo, TimeSource as ReplTimeSource,
};
use crate::replication::task_hydrator::TaskHydrator;
use crate::replication::task_reader::TaskReaderApi;
use crate::replication::task_store::TaskHydratorApi;
use crate::replication::{
    DlqHandlerImpl, DynamicTaskBatchSizerImpl, MetricsEmitterImpl, TaskAckManager, TaskExecutor, TaskExecutorImpl,
    TaskFetchers, TaskFetchersImpl, TaskProcessorImpl, TaskStore,
};
use crate::wiring::assembly::controller::ShardRegistry;
use crate::wiring::assembly::env::{ReplicationFactory, ReplicationParts, ShardBuildCtx};
use crate::wiring::assembly::replication::{
    history_error_to_replication, to_engine_replicate_events, to_engine_sync_activity, with_timeout,
    LocalReplicationEngine, ReplicationShardAdapter, ReplicationShardDeps, ThriftPayloadSerializer,
};
use crate::wiring::assembly::resender::HistoryResenderImpl;
use crate::wiring::persistence::replication::{
    map_persistence_error, replication_error_to_history_error, ShardAckLevelStore, SqlExecutionManager,
    SqlHistoryManager, SqlReplicationAckManager, SqlReplicationDlqHandler, SqlTaskReader,
};
use crate::wiring::resources::replication_clients::ReplicationClientBean;

// ---------------------------------------------------------------- mutable state

struct SnapshotView {
    running: bool,
    activities: HashMap<i64, ActivityInfo>,
    version_histories: Option<VersionHistories>,
}

impl MutableStateView for SnapshotView {
    fn is_workflow_execution_running(&self) -> bool {
        self.running
    }
    fn get_activity_info(&self, id: i64) -> Option<ActivityInfo> {
        self.activities.get(&id).cloned()
    }
    fn get_version_histories(&self) -> Option<VersionHistories> {
        self.version_histories.clone()
    }
}

fn inconsistent(e: impl std::fmt::Display) -> ReplicationError {
    ReplicationError::InternalDataInconsistency(e.to_string())
}

fn to_activity_info(
    domain_id: &str,
    schedule_id: i64,
    p: PActivityInfo,
    heartbeat: Option<&cadence_persistence::history_contract::ActivityHeartbeat>,
) -> ActivityInfo {
    ActivityInfo {
        version: p.version,
        schedule_id,
        scheduled_event_batch_id: p.scheduled_event_batch_id,
        scheduled_time: p.scheduled_time_unix_nanos,
        started_id: p.started_id,
        started_time: p.started_time_unix_nanos,
        activity_id: p.activity_id,
        domain_id: domain_id.to_string(),
        schedule_to_start_timeout: p.schedule_to_start_timeout_seconds,
        schedule_to_close_timeout: p.schedule_to_close_timeout_seconds,
        start_to_close_timeout: p.start_to_close_timeout_seconds,
        heartbeat_timeout: p.heartbeat_timeout_seconds,
        cancel_requested: p.cancel_requested,
        cancel_request_id: p.cancel_request_id,
        task_list: p.task_list,
        attempt: p.attempt,
        request_id: p.request_id,
        started_identity: p.started_identity,
        last_heartbeat_updated_time: heartbeat.map(|h| h.updated_time_unix_nanos).unwrap_or_default(),
        details: heartbeat.map(|h| h.details.clone()).unwrap_or_default(),
        has_retry_policy: p.has_retry_policy,
        initial_interval: p.retry_initial_interval_seconds,
        backoff_coefficient: p.retry_backoff_coefficient,
        jitter_coefficient: p.retry_jitter_coefficient,
        maximum_interval: p.retry_maximum_interval_seconds,
        maximum_attempts: p.retry_maximum_attempts,
        expiration_time: p.retry_expiration_time_unix_nanos,
        non_retriable_errors: p.retry_non_retriable_errors,
        last_failure_reason: p.retry_last_failure_reason,
        last_failure_details: p.retry_last_failure_details,
        last_retry_interval_seconds: p.retry_last_retry_interval_seconds,
        last_worker_identity: p.retry_last_worker_identity,
    }
}

/// SQL-backed `MutableStateProvider` (reads the persisted snapshot).
pub struct SqlMutableStateProvider {
    shard_id: i32,
    store: Arc<dyn GoExecutionStore>,
}

impl SqlMutableStateProvider {
    pub fn new(shard_id: i32, store: Arc<dyn GoExecutionStore>) -> Self {
        Self { shard_id, store }
    }
}

#[async_trait]
impl MutableStateProvider for SqlMutableStateProvider {
    async fn get_mutable_state(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> ReplResult<(Arc<dyn MutableStateView>, ReleaseFn)> {
        let snapshot = self
            .store
            .get_workflow_execution(
                self.shard_id,
                domain_id.to_string(),
                workflow_id.to_string(),
                run_id.to_string(),
                0,
            )
            .await
            .map_err(map_persistence_error)?;
        // persistence.WorkflowStateCreated(0) / Running(1)
        let running = matches!(snapshot.execution_info.state, 0 | 1);
        let mut activities = HashMap::new();
        for kb in &snapshot.activity_infos {
            let encoding = Encoding::parse(&kb.value.encoding).map_err(inconsistent)?;
            let st = encoding.decode(&kb.value.data).map_err(inconsistent)?;
            let p = activity_info_from_struct(&st).map_err(inconsistent)?;
            let hb = snapshot.activity_heartbeats.get(&kb.key);
            activities.insert(kb.key, to_activity_info(domain_id, kb.key, p, hb));
        }
        let version_histories = match &snapshot.version_histories {
            Some(blob) if !blob.data.is_empty() => {
                let encoding = Encoding::parse(&blob.encoding).map_err(inconsistent)?;
                let st = encoding.decode(&blob.data).map_err(inconsistent)?;
                Some(
                    crate::wiring::server::thrift_types_g2::version_histories_from_thrift(&st)
                        .map_err(inconsistent)?,
                )
            }
            _ => None,
        };
        let view: Arc<dyn MutableStateView> = Arc::new(SnapshotView { running, activities, version_histories });
        let release: ReleaseFn = Box::new(|_| {});
        Ok((view, release))
    }
}

// ---------------------------------------------------------------- engine adapters

fn to_history_task(task: &Task) -> HistoryTask {
    let i = task.info();
    let info = TaskInfo {
        task_id: i.task_id,
        domain_id: i.domain_id.clone(),
        workflow_id: i.workflow_id.clone(),
        run_id: i.run_id.clone(),
        version: i.version,
        visibility_timestamp_nanos: 0,
    };
    match task {
        Task::History { branch_token, new_run_branch_token, .. } => HistoryTask::HistoryReplication {
            first_event_id: i.first_event_id,
            next_event_id: i.next_event_id,
            branch_token: branch_token.clone(),
            new_run_branch_token: new_run_branch_token.clone(),
            info,
        },
        Task::SyncActivity(_) => HistoryTask::SyncActivity { scheduled_id: i.scheduled_id, info },
        Task::FailoverMarker(_) => HistoryTask::FailoverMarker { info },
    }
}

/// Engine-facing `ReplicationHydrator` over the replication `TaskHydrator`.
pub struct EngineHydrator {
    inner: Arc<TaskHydrator>,
}

#[async_trait]
impl ReplicationHydrator for EngineHydrator {
    async fn hydrate(&self, task: Task) -> HResult<Option<ReplicationTask>> {
        self.inner
            .hydrate(&to_history_task(&task))
            .await
            .map_err(replication_error_to_history_error)
    }
}

/// Placeholder for the NDC `Replicator` (see module docs).
pub struct UnwiredReplicator;

#[async_trait]
impl Replicator for UnwiredReplicator {
    async fn apply_events(&self, _request: &et::ReplicateEventsV2Request) -> HResult<()> {
        Err(HistoryError::internal("ndc replicator is not wired (execution cache is unwired)"))
    }
    async fn sync_activity(&self, _request: &et::SyncActivityRequest) -> HResult<()> {
        Err(HistoryError::internal("ndc replicator is not wired (execution cache is unwired)"))
    }
}

/// Approximation of `proto.ReplicationMessages.Size()` (JSON length).
fn messages_size_fn() -> ReplicationMessagesSizeFn {
    Arc::new(|m| serde_json::to_vec(m).map(|v| v.len()).unwrap_or(0))
}

// ---------------------------------------------------------------- history client

/// `replication::HistoryClient` routing to the local shard registry. Multi-host
/// forwarding is not implemented: non-owned shards surface `ShardOwnershipLost`.
#[derive(Default)]
pub struct RegistryHistoryClient {
    registry: OnceLock<Weak<ShardRegistry>>,
}

impl RegistryHistoryClient {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Must be called once after the `ShardRegistry` has been built.
    pub fn bind(&self, registry: &Arc<ShardRegistry>) {
        let _ = self.registry.set(Arc::downgrade(registry));
    }

    async fn engine_for(&self, workflow_id: &str) -> ReplResult<Arc<HistoryEngine>> {
        let registry = self
            .registry
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| ReplicationError::Other("shard registry is not bound".into()))?;
        registry.engine_for_workflow(workflow_id).await.map_err(history_error_to_replication)
    }
}

#[async_trait]
impl HistoryClient for RegistryHistoryClient {
    async fn sync_activity(&self, req: SyncActivityRequest, timeout: Duration) -> ReplResult<()> {
        let engine = self.engine_for(req.task.workflow_id.as_deref().unwrap_or_default()).await?;
        let r = to_engine_sync_activity(req);
        with_timeout(timeout, async move { engine.sync_activity(&r).await }).await
    }
    async fn replicate_events_v2(&self, req: ReplicateEventsV2Request, timeout: Duration) -> ReplResult<()> {
        let engine = self.engine_for(&req.workflow_execution.workflow_id).await?;
        let r = to_engine_replicate_events(req);
        with_timeout(timeout, async move { engine.replicate_events_v2(&r).await }).await
    }
}

// ---------------------------------------------------------------- factory

struct Shared {
    bean: Arc<dyn ClientBean>,
    fetchers: Arc<TaskFetchersImpl>,
}

/// `ReplicationFactory` producing the replication parts of one shard engine.
pub struct ReplicationFactoryImpl {
    history: Arc<dyn HistoryClient>,
    shared: Mutex<Option<Arc<Shared>>>,
}

impl ReplicationFactoryImpl {
    pub fn new(history: Arc<dyn HistoryClient>) -> Self {
        Self { history, shared: Mutex::new(None) }
    }

    /// Stops the process-level task fetchers (call on service shutdown).
    pub fn stop_fetchers(&self) {
        if let Some(s) = self.shared.lock().unwrap().as_ref() {
            s.fetchers.stop();
        }
    }

    fn shared(&self, ctx: &ShardBuildCtx) -> anyhow::Result<Arc<Shared>> {
        let mut guard = self.shared.lock().unwrap();
        if let Some(s) = guard.as_ref() {
            return Ok(s.clone());
        }
        let env = &ctx.env;
        let bean: Arc<dyn ClientBean> = Arc::new(ReplicationClientBean::new(
            self.history.clone(),
            env.cluster.metadata().clone(),
            "cadence-history",
        ));
        let cfg: Arc<dyn ReplicationConfig> = env.config_adapter.clone();
        let metrics: Arc<dyn ReplicationMetrics> = env.metrics.clone();
        let fetchers = TaskFetchersImpl::new(
            cfg,
            &env.cluster.metadata().current_cluster_name,
            env.cluster.remote_cluster_names(),
            bean.as_ref(),
            metrics,
        )
        .map_err(|e| anyhow::anyhow!("creating replication task fetchers: {e}"))?;
        let fetchers = Arc::new(fetchers);
        if tokio::runtime::Handle::try_current().is_ok() {
            fetchers.start();
        }
        let shared = Arc::new(Shared { bean, fetchers });
        *guard = Some(shared.clone());
        Ok(shared)
    }
}

impl ReplicationFactory for ReplicationFactoryImpl {
    fn build_replication(&self, ctx: &ShardBuildCtx) -> anyhow::Result<ReplicationParts> {
        let shared = self.shared(ctx)?;
        let env = &ctx.env;
        let shard_id = ctx.shard.shard_id();
        let cfg: Arc<dyn ReplicationConfig> = env.config_adapter.clone();
        let metrics: Arc<dyn ReplicationMetrics> = env.metrics.clone();
        let domains: Arc<dyn ReplDomainCache> = env.domain_cache.clone();
        let time: Arc<dyn ReplTimeSource> = Arc::new(RealTimeSource);
        let exec_mgr: Arc<dyn ExecutionManager> =
            Arc::new(SqlExecutionManager::new(env.persistence.executions.clone()));
        let hist_mgr: Arc<dyn HistoryManager> = Arc::new(SqlHistoryManager::new(env.persistence.history_nodes.clone()));
        let cluster = env.cluster.clone();
        let current_cluster = cluster.metadata().current_cluster_name.clone();

        let shard_adapter: Arc<dyn ReplShard> = ReplicationShardAdapter::new(ReplicationShardDeps {
            shard: ctx.shard.clone(),
            current_cluster: current_cluster.clone(),
            remote_clusters: cluster.remote_cluster_names(),
            time_source: time.clone(),
            repl_domain_cache: domains.clone(),
            domains: env.domain_cache.clone(),
            execution_manager: exec_mgr.clone(),
            client_bean: shared.bean.clone(),
            cluster_name_for_version: Arc::new(move |v| {
                cluster
                    .metadata()
                    .cluster_name_for_failover_version(v)
                    .map(str::to_string)
                    .map_err(ReplicationError::Other)
            }),
        });

        // TaskHydrator (deferred) -> TaskStore -> TaskAckManager.
        let ms_provider: Arc<dyn MutableStateProvider> =
            Arc::new(SqlMutableStateProvider::new(shard_id, env.persistence.executions.clone()));
        let hydrator = Arc::new(TaskHydrator::new_deferred(shard_id, hist_mgr, ms_provider, domains.clone()));
        let limiter: Arc<dyn Limiter> = Arc::new(SimpleRateLimiter::new(cfg.replication_task_generation_qps()));
        let store = Arc::new(TaskStore::new(
            cfg.as_ref(),
            shard_adapter.remote_cluster_names(),
            domains.clone(),
            metrics.clone(),
            hydrator.clone() as Arc<dyn TaskHydratorApi>,
            limiter,
            shard_id,
            time.clone(),
        ));
        let reader: Arc<dyn TaskReaderApi> =
            Arc::new(SqlTaskReader::new(shard_id, env.persistence.executions.clone()));
        let sizer = Arc::new(DynamicTaskBatchSizerImpl::new(shard_id, cfg.clone(), metrics.clone()));
        let ack_manager = Arc::new(TaskAckManager::new(
            shard_id,
            Arc::new(ShardAckLevelStore::new(shard_adapter.clone())),
            metrics.clone(),
            reader.clone(),
            store,
            time.clone(),
            cfg.as_ref(),
            messages_size_fn(),
            sizer,
        ));
        let emitter = MetricsEmitterImpl::new(shard_id, shard_adapter.clone(), reader, metrics.clone());

        // Per remote cluster: resender, executor, processor.
        let engine: Arc<dyn ReplEngine> = Arc::new(LocalReplicationEngine::new(ctx.shard.clone(), ctx.engine_slot.clone()));
        let resender: Arc<dyn HistoryResender> =
            Arc::new(HistoryResenderImpl::new(domains.clone(), shared.bean.clone(), self.history.clone()));
        let serializer: Arc<dyn PayloadSerializer> = Arc::new(ThriftPayloadSerializer);
        let mut executors: HashMap<String, Arc<dyn TaskExecutor>> = HashMap::new();
        let mut daemons: Vec<Arc<dyn Daemon>> = Vec::new();
        for fetcher in shared.fetchers.get_fetchers() {
            let source = fetcher.get_source_cluster().to_string();
            let executor: Arc<dyn TaskExecutor> = Arc::new(TaskExecutorImpl::new(
                source.clone(),
                shard_adapter.clone(),
                cfg.number_of_shards() as i32,
                domains.clone(),
                resender.clone(),
                engine.clone(),
                metrics.clone(),
            ));
            let processor = TaskProcessorImpl::new(
                shard_adapter.clone(),
                engine.clone(),
                cfg.clone(),
                metrics.clone(),
                fetcher.as_ref(),
                executor.clone(),
                serializer.clone(),
            );
            executors.insert(source, executor);
            daemons.push(processor);
        }
        let dlq = DlqHandlerImpl::new(shard_adapter.clone(), metrics.clone(), executors);
        daemons.push(emitter);
        daemons.push(dlq.clone());

        // No engine lifecycle exists yet: start now, stop when the shard closes.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _enter = handle.enter();
            for d in &daemons {
                d.start();
            }
            let shard = ctx.shard.clone();
            handle.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if shard.closed_error().is_some() {
                        for d in &daemons {
                            d.stop();
                        }
                        return;
                    }
                }
            });
        } else {
            tracing::warn!(shard_id, "no tokio runtime: replication processors not started");
        }

        Ok(ReplicationParts {
            dlq_handler: Arc::new(SqlReplicationDlqHandler::new(dlq)),
            ack_manager: Arc::new(SqlReplicationAckManager::new(ack_manager)),
            hydrator: Arc::new(EngineHydrator { inner: hydrator }),
            replicator: Arc::new(UnwiredReplicator),
        })
    }
}
