//! Port of `go/service/history/taskdlq/processor.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use super::host_limiter::HostLimiter;
use super::interface::TaskReinjector;
use crate::task::deps::*;
use crate::task::deps_exec::*;

const SWEEP_INTERVAL_JITTER_COEFFICIENT: f64 = 0.15;

// ---- persistence shims. TODO(link): replace by cadence-persistence HistoryTaskDLQManager types ----
#[derive(Clone, Debug, Default)]
pub struct HistoryDLQGetAckLevelsRequest {
    pub shard_id: i32,
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
}
#[derive(Clone, Debug)]
pub struct HistoryDLQAckLevel {
    pub shard_id: i32,
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    pub task_category: HistoryTaskCategory,
    pub ack_level_visibility_ts: Time,
    pub ack_level_task_id: i64,
}
#[derive(Clone, Debug)]
pub struct HistoryDLQGetTasksRequest {
    pub shard_id: i32,
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    pub task_category: HistoryTaskCategory,
    pub inclusive_min_task_key: HistoryTaskKey,
    pub exclusive_max_task_key: HistoryTaskKey,
    pub page_size: i32,
    pub next_page_token: Vec<u8>,
}
pub struct HistoryDLQGetTasksResponse {
    pub tasks: Vec<Arc<dyn PersistenceTask>>,
    pub page_size_bytes: i64,
    pub next_page_token: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct HistoryDLQUpdateAckLevelRequest {
    pub shard_id: i32,
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    pub task_category: HistoryTaskCategory,
    pub updated_inclusive_read_level: HistoryTaskKey,
}
#[derive(Clone, Debug)]
pub struct HistoryDLQDeleteTasksRequest {
    pub shard_id: i32,
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    pub task_category: HistoryTaskCategory,
    pub exclusive_max_task_key: HistoryTaskKey,
}

#[async_trait]
pub trait HistoryTaskDLQManager: Send + Sync {
    async fn get_history_dlq_ack_levels(&self, req: HistoryDLQGetAckLevelsRequest) -> Result<Vec<HistoryDLQAckLevel>>;
    async fn get_history_dlq_tasks(&self, req: HistoryDLQGetTasksRequest) -> Result<HistoryDLQGetTasksResponse>;
    async fn update_history_dlq_ack_level(&self, req: HistoryDLQUpdateAckLevelRequest) -> Result<()>;
    async fn delete_history_dlq_tasks(&self, req: HistoryDLQDeleteTasksRequest) -> Result<()>;
}

/// Go `Processor` (common.DaemonV2 + methods).
#[async_trait]
pub trait Processor: Send + Sync {
    async fn start(&self, ctx: CancellationToken) -> Result<()>;
    async fn stop(&self, ctx: CancellationToken) -> Result<()>;
    async fn process_shard(&self, ctx: &CancellationToken) -> Result<()>;
    async fn process_partition(
        &self,
        ctx: &CancellationToken,
        domain_id: &str,
        cluster_attribute_scope: &str,
        cluster_attribute_name: &str,
    ) -> Result<()>;
    fn failover_partitions(&self, partitions: Vec<Partition>);
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Partition {
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
}

impl Partition {
    fn key(&self) -> String {
        format!(
            "DomainID/{}/ClusterAttributeScope/{}/ClusterAttributeName/{}",
            self.domain_id, self.cluster_attribute_scope, self.cluster_attribute_name
        )
    }
}

pub type MaxReadLevelFn = Arc<dyn Fn(HistoryTaskCategory) -> HistoryTaskKey + Send + Sync>;
pub type DomainNameFn = Arc<dyn Fn(&str) -> Result<String> + Send + Sync>;

pub struct ProcessorParams {
    pub shard_id: i32,
    pub manager: Arc<dyn HistoryTaskDLQManager>,
    pub domain_name: DomainNameFn,
    pub reinjector: Arc<dyn TaskReinjector>,
    pub max_read_level: Option<MaxReadLevelFn>,
    pub page_size: i32,
    pub interval: DurationPropertyFnWithShardIDFilter,
    pub failover_jitter_max_delay: DurationPropertyFn,
    pub domain_mode: StringPropertyFnWithDomainFilter,
    pub enabled: BoolPropertyFn,
    pub host_limiter: HostLimiter,
    pub time_source: Arc<dyn TimeSource>,
    pub metrics_client: Arc<dyn MetricsClient>,
    pub logger: Logger,
}

pub struct ProcessorImpl {
    shard_id: i32,
    mgr: Arc<dyn HistoryTaskDLQManager>,
    get_domain_name: DomainNameFn,
    reinjector: Arc<dyn TaskReinjector>,
    max_read_level: MaxReadLevelFn,
    page_size: i32,
    interval: DurationPropertyFnWithShardIDFilter,
    failover_jitter_max_delay: DurationPropertyFn,
    domain_mode: StringPropertyFnWithDomainFilter,
    enabled: BoolPropertyFn,
    host_limiter: HostLimiter,
    time_source: Arc<dyn TimeSource>,
    metrics_client: Arc<dyn MetricsClient>,
    logger: Logger,

    status: AtomicI32,
    ctx: StdMutex<CancellationToken>,
    handle: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    /// serializes ProcessShard and ProcessPartition
    process_mu: Mutex<()>,
    /// guards pending_failover and sweep_cancel
    failover_mu: StdMutex<FailoverState>,
    failover_tx: mpsc::Sender<()>,
    failover_rx: StdMutex<Option<mpsc::Receiver<()>>>,
}

#[derive(Default)]
struct FailoverState {
    pending_failover: HashMap<String, Partition>,
    sweep_cancel: Option<CancellationToken>,
}

/// `NewProcessor`
pub fn new_processor(params: ProcessorParams) -> Arc<ProcessorImpl> {
    let max_read_level = params
        .max_read_level
        .unwrap_or_else(|| Arc::new(|_c: HistoryTaskCategory| HistoryTaskKey::maximum()));
    let (tx, rx) = mpsc::channel(1);
    Arc::new(ProcessorImpl {
        shard_id: params.shard_id,
        mgr: params.manager,
        get_domain_name: params.domain_name,
        reinjector: params.reinjector,
        max_read_level,
        page_size: params.page_size,
        interval: params.interval,
        failover_jitter_max_delay: params.failover_jitter_max_delay,
        domain_mode: params.domain_mode,
        enabled: params.enabled,
        host_limiter: params.host_limiter,
        time_source: params.time_source,
        metrics_client: params.metrics_client,
        logger: params.logger,
        status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
        ctx: StdMutex::new(CancellationToken::new()), // no-op until Start() sets the real token
        handle: StdMutex::new(None),
        process_mu: Mutex::new(()),
        failover_mu: StdMutex::new(FailoverState::default()),
        failover_tx: tx,
        failover_rx: StdMutex::new(Some(rx)),
    })
}

/// `NewShardMaxReadLevelFn`
pub fn new_shard_max_read_level_fn(shard: Arc<dyn ShardContext>) -> MaxReadLevelFn {
    let current_cluster_name = shard.get_cluster_metadata().get_current_cluster_name();
    Arc::new(move |category: HistoryTaskCategory| {
        let max_read_level = shard.update_if_needed_and_get_queue_max_read_level(category, &current_cluster_name);
        if category.is_immediate() {
            return HistoryTaskKey::new_immediate_task_key(max_read_level.get_task_id() + 1);
        }
        max_read_level
    })
}

impl ProcessorImpl {
    fn ctx(&self) -> CancellationToken {
        self.ctx.lock().unwrap().clone()
    }

    fn sweep_interval(&self) -> Duration {
        let mut rng = rand::thread_rng();
        jit_duration(&mut rng, (self.interval)(self.shard_id), SWEEP_INTERVAL_JITTER_COEFFICIENT)
    }

    async fn process_loop(self: Arc<Self>, mut failover_rx: mpsc::Receiver<()>) {
        let ctx = self.ctx();
        // Go: log.CapturePanic — a panic here ends the spawned task only.
        let timer = tokio::time::sleep(self.sweep_interval());
        tokio::pin!(timer);
        loop {
            tokio::select! {
                _ = ctx.cancelled() => return,
                _ = failover_rx.recv() => {
                    self.process_pending_failovers().await;
                }
                _ = &mut timer => {
                    self.run_sweep().await;
                    self.process_pending_failovers().await;
                    timer.as_mut().reset(tokio::time::Instant::now() + self.sweep_interval());
                }
            }
        }
    }

    async fn run_sweep(&self) {
        if !(self.enabled)() {
            return;
        }
        let ctx = self.ctx().child_token();
        self.failover_mu.lock().unwrap().sweep_cancel = Some(ctx.clone());
        let res = self.process_shard(&ctx).await;
        self.failover_mu.lock().unwrap().sweep_cancel = None;
        if let Err(e) = res {
            if !ctx.is_cancelled() {
                self.logger.error("DLQ periodic shard sweep failed", &[tag::shard_id(self.shard_id), tag::error(&e)]);
            }
        }
        ctx.cancel();
    }

    async fn process_pending_failovers(&self) {
        if self.failover_mu.lock().unwrap().pending_failover.is_empty() {
            return;
        }
        let ctx = self.ctx();
        let window = (self.failover_jitter_max_delay)();
        if window > Duration::ZERO {
            // Go: rand.Int63n(window) + 1
            let delay = Duration::from_nanos(rand::thread_rng().gen_range(0..window.as_nanos() as u64) + 1);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = ctx.cancelled() => return,
            }
        }
        let parts: Vec<Partition> = {
            let mut st = self.failover_mu.lock().unwrap();
            std::mem::take(&mut st.pending_failover).into_values().collect()
        };
        if !(self.enabled)() {
            return;
        }
        for part in parts {
            if ctx.is_cancelled() {
                return;
            }
            if let Err(e) = self
                .process_partition(&ctx, &part.domain_id, &part.cluster_attribute_scope, &part.cluster_attribute_name)
                .await
            {
                self.logger.error(
                    "DLQ failover partition reprocessing failed",
                    &[
                        tag::shard_id(self.shard_id),
                        tag::workflow_domain_id(&part.domain_id),
                        tag::dynamic("cluster-attribute-scope", &part.cluster_attribute_scope),
                        tag::dynamic("cluster-attribute-name", &part.cluster_attribute_name),
                        tag::error(&e),
                    ],
                );
            }
        }
    }

    fn domain_name(&self, domain_id: &str) -> String {
        match (self.get_domain_name)(domain_id) {
            Err(e) => {
                self.logger.debug_tags(
                    "Failed to get domain name from domain cache. Defaulting to domain ID.",
                    &[tag::workflow_domain_id(domain_id), tag::error(&e)],
                );
                domain_id.to_string()
            }
            Ok(n) => n,
        }
    }

    async fn process_ack_levels(&self, ctx: &CancellationToken, ack_levels: Vec<HistoryDLQAckLevel>) -> Result<()> {
        let mut errs: Vec<TaskError> = vec![];
        let join = |mut errs: Vec<TaskError>| if errs.is_empty() { Ok(()) } else if errs.len() == 1 { Err(errs.remove(0)) } else { Err(TaskError::Joined(errs)) };
        for al in ack_levels {
            if ctx.is_cancelled() {
                errs.push(TaskError::ContextCanceled);
                return join(errs);
            }
            if let Err(e) = self.process_ack_level(ctx, &al).await {
                if ctx.is_cancelled() {
                    errs.push(TaskError::ContextCanceled);
                    return join(errs);
                }
                self.logger.error(
                    "failed to process DLQ partition",
                    &[
                        tag::workflow_domain_id(&al.domain_id),
                        tag::dynamic("cluster-attribute-scope", &al.cluster_attribute_scope),
                        tag::dynamic("cluster-attribute-name", &al.cluster_attribute_name),
                        tag::task_type(al.task_category.id()),
                        tag::error(&e),
                    ],
                );
                errs.push(e);
            }
        }
        join(errs)
    }

    async fn process_ack_level(&self, ctx: &CancellationToken, al: &HistoryDLQAckLevel) -> Result<()> {
        if (self.domain_mode)(&self.domain_name(&al.domain_id)) != HISTORY_TASK_DLQ_MODE_ENABLED {
            self.logger.debug_tags(
                "DLQ not enabled for domain, skipping ack level processing",
                &[tag::shard_id(self.shard_id), tag::workflow_domain_id(&al.domain_id)],
            );
            return Ok(());
        }
        let id = al.task_category.id();
        if id != HISTORY_TASK_CATEGORY_ID_TRANSFER && id != HISTORY_TASK_CATEGORY_ID_TIMER {
            self.logger.debug_tags(
                "Skipping DLQ ack level for unsupported task category",
                &[tag::shard_id(self.shard_id), tag::workflow_domain_id(&al.domain_id), tag::task_type(id)],
            );
            return Ok(());
        }

        let scope = self
            .metrics_client
            .scope(metrics::HISTORY_TASK_DLQ_PROCESSOR_SCOPE, vec![domain_tag(&al.domain_id)]);

        let mut page_token: Vec<u8> = vec![];
        let mut last_good_key: Option<HistoryTaskKey> = None;
        let mut first_err: Option<TaskError> = None;

        let min_key = HistoryTaskKey::new(al.ack_level_visibility_ts, al.ack_level_task_id).next();
        let max_key = (self.max_read_level)(al.task_category);
        if min_key.compare(&max_key) >= 0 {
            return Ok(());
        }

        loop {
            if ctx.is_cancelled() {
                first_err = Some(TaskError::ContextCanceled);
                break;
            }
            let resp = match self
                .mgr
                .get_history_dlq_tasks(HistoryDLQGetTasksRequest {
                    shard_id: al.shard_id,
                    domain_id: al.domain_id.clone(),
                    cluster_attribute_scope: al.cluster_attribute_scope.clone(),
                    cluster_attribute_name: al.cluster_attribute_name.clone(),
                    task_category: al.task_category,
                    inclusive_min_task_key: min_key,
                    exclusive_max_task_key: max_key,
                    page_size: self.page_size,
                    next_page_token: page_token.clone(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    first_err = Some(e);
                    break;
                }
            };
            if !resp.tasks.is_empty() {
                scope.record_histogram_value(metrics::HISTORY_TASK_DLQ_PAGE_SIZE_BYTES, resp.page_size_bytes as f64);
                let k = resp.tasks[resp.tasks.len() - 1].get_task_key();
                if let Err(e) = self.reinjector.reinject_history_tasks(ctx, resp.tasks).await {
                    scope.inc_counter(metrics::HISTORY_TASK_DLQ_REINJECT_FAILURES_COUNTER);
                    first_err = Some(e);
                    break;
                }
                last_good_key = Some(k);
            }
            if resp.next_page_token.is_empty() {
                break;
            }
            page_token = resp.next_page_token;
        }

        if let Some(k) = last_good_key {
            // Go uses p.ctx (not the sweep ctx) so the ack level is advanced even if the sweep was cancelled.
            if let Err(e) = self.advance_ack_level(&self.ctx(), al, k).await {
                return Err(match first_err {
                    Some(f) => TaskError::Joined(vec![e, f]),
                    None => e,
                });
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn advance_ack_level(&self, _ctx: &CancellationToken, al: &HistoryDLQAckLevel, new_key: HistoryTaskKey) -> Result<()> {
        if let Err(e) = self
            .mgr
            .update_history_dlq_ack_level(HistoryDLQUpdateAckLevelRequest {
                shard_id: al.shard_id,
                domain_id: al.domain_id.clone(),
                cluster_attribute_scope: al.cluster_attribute_scope.clone(),
                cluster_attribute_name: al.cluster_attribute_name.clone(),
                task_category: al.task_category,
                updated_inclusive_read_level: new_key,
            })
            .await
        {
            return Err(TaskError::Other(format!("update DLQ ack level: {:?}", e)));
        }
        if let Err(e) = self
            .mgr
            .delete_history_dlq_tasks(HistoryDLQDeleteTasksRequest {
                shard_id: al.shard_id,
                domain_id: al.domain_id.clone(),
                cluster_attribute_scope: al.cluster_attribute_scope.clone(),
                cluster_attribute_name: al.cluster_attribute_name.clone(),
                task_category: al.task_category,
                exclusive_max_task_key: new_key.next(),
            })
            .await
        {
            self.logger.error(
                "failed to delete acknowledged DLQ tasks",
                &[tag::workflow_domain_id(&al.domain_id), tag::error(&e)],
            );
        }
        Ok(())
    }
}

/// `ProcessorImpl` wrapper needing `Arc<Self>` for the background loop.
#[async_trait]
impl Processor for Arc<ProcessorImpl> {
    async fn start(&self, ctx: CancellationToken) -> Result<()> {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        *self.ctx.lock().unwrap() = ctx.child_token();
        self.logger.debug_tags("DLQ processor starting", &[tag::shard_id(self.shard_id)]);
        let rx = self.failover_rx.lock().unwrap().take().expect("failover receiver");
        let me = self.clone();
        *self.handle.lock().unwrap() = Some(tokio::spawn(async move { me.process_loop(rx).await }));
        self.logger.debug_tags("DLQ processor started", &[tag::shard_id(self.shard_id)]);
        Ok(())
    }

    async fn stop(&self, ctx: CancellationToken) -> Result<()> {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        self.logger.debug_tags("DLQ processor stopping", &[tag::shard_id(self.shard_id)]);
        self.ctx().cancel();
        let handle = self.handle.lock().unwrap().take();
        if let Some(h) = handle {
            tokio::select! {
                _ = h => {
                    self.logger.debug_tags("DLQ processor stopped", &[tag::shard_id(self.shard_id)]);
                    Ok(())
                }
                _ = ctx.cancelled() => {
                    self.logger.warn_tags(
                        "DLQ processor stop timed out waiting for the processing loop",
                        &[tag::shard_id(self.shard_id)],
                    );
                    Err(TaskError::ContextCanceled)
                }
            }
        } else {
            Ok(())
        }
    }

    async fn process_shard(&self, ctx: &CancellationToken) -> Result<()> {
        ProcessorImpl::process_shard(self, ctx).await
    }

    async fn process_partition(&self, ctx: &CancellationToken, d: &str, s: &str, n: &str) -> Result<()> {
        ProcessorImpl::process_partition(self, ctx, d, s, n).await
    }

    fn failover_partitions(&self, partitions: Vec<Partition>) {
        ProcessorImpl::failover_partitions(self, partitions)
    }
}

impl ProcessorImpl {
    /// `ProcessShard` — read and reinject DLQ tasks for every ack level of the shard.
    pub async fn process_shard(&self, ctx: &CancellationToken) -> Result<()> {
        let _g = self.process_mu.lock().await;
        let _permit = self.host_limiter.acquire(ctx).await?;
        if ctx.is_cancelled() {
            return Err(TaskError::ContextCanceled);
        }
        let ack_levels = self
            .mgr
            .get_history_dlq_ack_levels(HistoryDLQGetAckLevelsRequest { shard_id: self.shard_id, ..Default::default() })
            .await
            .map_err(|e| TaskError::Other(format!("get DLQ ack levels for shard {}: {:?}", self.shard_id, e)))?;
        self.process_ack_levels(ctx, ack_levels).await
    }

    /// `ProcessPartition`
    pub async fn process_partition(
        &self,
        ctx: &CancellationToken,
        domain_id: &str,
        cluster_attribute_scope: &str,
        cluster_attribute_name: &str,
    ) -> Result<()> {
        if (self.domain_mode)(&self.domain_name(domain_id)) != HISTORY_TASK_DLQ_MODE_ENABLED {
            self.logger.debug_tags(
                "DLQ not enabled for domain, skipping partition processing",
                &[tag::shard_id(self.shard_id), tag::workflow_domain_id(domain_id)],
            );
            return Ok(());
        }
        let _g = self.process_mu.lock().await;
        let _permit = self.host_limiter.acquire(ctx).await?;
        if ctx.is_cancelled() {
            return Err(TaskError::ContextCanceled);
        }
        let ack_levels = self
            .mgr
            .get_history_dlq_ack_levels(HistoryDLQGetAckLevelsRequest {
                shard_id: self.shard_id,
                domain_id: domain_id.to_string(),
                cluster_attribute_scope: cluster_attribute_scope.to_string(),
                cluster_attribute_name: cluster_attribute_name.to_string(),
            })
            .await
            .map_err(|e| {
                TaskError::Other(format!(
                    "get DLQ ack levels for partition (shard={} domain={} scope={} name={}): {:?}",
                    self.shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, e
                ))
            })?;
        self.process_ack_levels(ctx, ack_levels).await
    }

    /// `FailoverPartitions`: queue partitions for reprocessing and interrupt the running sweep.
    pub fn failover_partitions(&self, partitions: Vec<Partition>) {
        if partitions.is_empty() {
            return;
        }
        let cancel = {
            let mut st = self.failover_mu.lock().unwrap();
            for part in partitions {
                st.pending_failover.insert(part.key(), part);
            }
            st.sweep_cancel.clone()
        };
        if let Some(c) = cancel {
            c.cancel();
        }
        let _ = self.failover_tx.try_send(());
    }
}

/// `NewProcessorFromShard`
#[allow(clippy::too_many_arguments)]
pub fn new_processor_from_shard(
    shard: Arc<dyn ShardContext>,
    page_size: i32,
    interval: DurationPropertyFnWithShardIDFilter,
    failover_jitter_max_delay: DurationPropertyFn,
    domain_mode: StringPropertyFnWithDomainFilter,
    enabled: BoolPropertyFn,
    host_limiter: HostLimiter,
) -> Arc<ProcessorImpl> {
    let dc = shard.get_domain_cache();
    new_processor(ProcessorParams {
        shard_id: shard.get_shard_id(),
        manager: shard.get_service().get_history_task_dlq_manager(),
        domain_name: Arc::new(move |id: &str| dc.get_domain_name(id)),
        reinjector: shard.clone().as_task_reinjector(),
        max_read_level: Some(new_shard_max_read_level_fn(shard.clone())),
        page_size,
        interval,
        failover_jitter_max_delay,
        domain_mode,
        enabled,
        host_limiter,
        time_source: shard.get_time_source(),
        metrics_client: shard.get_metrics_client(),
        logger: shard.get_logger(),
    })
}
