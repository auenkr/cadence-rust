//! Port of `go/service/history/queue/transfer_queue_processor.go`.
//!
//! Rust-forced differences (no logic change): task executors / resetter / archival client /
//! history resender are built by the caller and arrive as `HistoryTaskFactory` closures
//! (task/ is ported elsewhere); `time.Timer` loops are tokio `select!` loops.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::action::{Action, ActionResult};
use super::constants::{DEFAULT_PROCESSING_QUEUE_LEVEL, GRACEFUL_SHUTDOWN_TIMEOUT};
use super::deps::{
    Daemon, HistoryTaskCategory, Limiter, MetricsScope, NotifyTaskInfo, PersistenceTask, QueueError, Redispatcher,
    TaskInitializer, TaskKey, TaskProcessor,
};
use super::domain_filter::DomainFilter;
use super::interface::Processor;
use super::processing_queue::min_task_key;
use super::processing_queue_state::ProcessingQueueState;
use super::processor_base::{
    QueueShutdownFn, UpdateClusterAckLevelFn, UpdateMaxReadLevelFn, UpdateProcessingQueueStatesFn,
    DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED,
};
use super::processor_options_factory::new_transfer_queue_processor_options;
use super::shard_deps::{
    has_more_rows_to_delete, FailoverLevel, HistoryKey, HistoryTaskFactory, QueueShardContext, QueueType,
    RangeCompleteHistoryTaskRequest, TaskFilter,
};
use super::task_allocator::{is_domain_not_registered, TaskAllocator};
use super::transfer_queue_processor_base::{transfer_task_id, TransferQueueProcessorBase};
use super::util::{
    convert_from_persistence_transfer_processing_queue_states, convert_to_persistence_transfer_processing_queue_states,
    new_transfer_task_key, validate_processing_queue_states, AckLevelValue, MAXIMUM_TRANSFER_TASK_KEY,
};

/// Go `errUnexpectedQueueTask`.
pub const ERR_UNEXPECTED_QUEUE_TASK: &str = "unexpected queue task";
/// Go `errProcessorShutdown`.
pub const ERR_PROCESSOR_SHUTDOWN: &str = "queue processor has been shutdown";

/// Wiring that Go builds inline in `NewTransferQueueProcessor` from executors etc.
pub struct TransferQueueDeps {
    pub shard: Arc<dyn QueueShardContext>,
    pub task_processor: Arc<dyn TaskProcessor>,
    pub task_allocator: Arc<dyn TaskAllocator>,
    pub metrics: Arc<dyn MetricsScope>,
    /// builds `quotas.NewDynamicRateLimiter(options.MaxPollRPS)`
    pub rate_limiter_factory: Arc<dyn Fn(super::deps::IntFn) -> Arc<dyn Limiter> + Send + Sync>,
    /// builds `redispatcher` (`task.NewRedispatcher`) per processor
    pub redispatcher_factory: Arc<dyn Fn(QueueType) -> Arc<dyn Redispatcher> + Send + Sync>,
    /// `task.NewHistoryTask` wrapper bound to the active executor
    pub active_task_factory: HistoryTaskFactory,
    /// per remote cluster: `task.NewHistoryTask` wrapper bound to that cluster's standby executor
    pub standby_task_factories: HashMap<String, HistoryTaskFactory>,
}

pub struct TransferQueueProcessor {
    shard: Arc<dyn QueueShardContext>,
    task_processor: Arc<dyn TaskProcessor>,
    metrics: Arc<dyn MetricsScope>,
    current_cluster_name: String,

    status: AtomicI32,
    shutdown_ch: CancellationToken,
    shutdown_wg: TaskTracker,

    ack_level: AtomicI64,
    task_allocator: Arc<dyn TaskAllocator>,
    deps: TransferQueueDeps,
    active_queue_processor: Arc<TransferQueueProcessorBase>,
    standby_queue_processors: HashMap<String, Arc<TransferQueueProcessorBase>>,
    failover_queue_processors: StdMutex<Vec<Arc<TransferQueueProcessorBase>>>,
}

impl TransferQueueProcessor {
    /// Go `NewTransferQueueProcessor`.
    pub fn new(deps: TransferQueueDeps) -> Arc<Self> {
        let shard = deps.shard.clone();
        let current_cluster_name = shard.current_cluster_name();

        let active_queue_processor = new_transfer_queue_active_processor(&deps);

        let mut standby_queue_processors = HashMap::new();
        for cluster_name in shard.remote_cluster_names() {
            let factory = deps
                .standby_task_factories
                .get(&cluster_name)
                .unwrap_or_else(|| panic!("missing standby task factory for {cluster_name}"))
                .clone();
            standby_queue_processors
                .insert(cluster_name.clone(), new_transfer_queue_standby_processor(&cluster_name, &deps, factory));
        }

        Arc::new(TransferQueueProcessor {
            shard: shard.clone(),
            task_processor: deps.task_processor.clone(),
            metrics: deps.metrics.clone(),
            current_cluster_name,
            status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
            shutdown_ch: CancellationToken::new(),
            shutdown_wg: TaskTracker::new(),
            ack_level: AtomicI64::new(shard.get_queue_ack_level(HistoryTaskCategory::Transfer).task_id),
            task_allocator: deps.task_allocator.clone(),
            deps,
            active_queue_processor,
            standby_queue_processors,
            failover_queue_processors: StdMutex::new(Vec::new()),
        })
    }

    fn graceful(&self) -> bool {
        (self.shard.processor_config().queue_processor_enable_graceful_sync_shutdown)()
    }

    async fn stop_impl(self: &Arc<Self>) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        if !self.graceful() {
            tracing::info!("Stopping transfer queue processor non-gracefully");
            self.active_queue_processor.stop().await;
            for p in self.standby_queue_processors.values() {
                p.stop().await;
            }

            self.shutdown_ch.cancel();
            self.shutdown_wg.close();
            if tokio::time::timeout(Duration::from_secs(60), self.shutdown_wg.wait()).await.is_err() {
                tracing::warn!("transferQueueProcessor timed out on shut down");
            }
            tracing::info!("Transfer queue processor stopped non-gracefully");
            return;
        }

        tracing::info!("Stopping transfer queue processor gracefully");

        // close the shutdown channel so processor pump goroutine drains tasks and then stop the processors
        self.shutdown_ch.cancel();
        self.shutdown_wg.close();
        if tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, self.shutdown_wg.wait()).await.is_err() {
            tracing::warn!("transferQueueProcessor timed out on shut down");
        }
        self.active_queue_processor.stop().await;
        for p in self.standby_queue_processors.values() {
            p.stop().await;
        }

        let failovers: Vec<_> = self.failover_queue_processors.lock().unwrap().clone();
        if !failovers.is_empty() {
            tracing::info!(count = failovers.len(), "Shutting down failover transfer queues");
            for p in failovers {
                p.stop().await;
            }
        }
        tracing::info!("Transfer queue processor stopped gracefully");
    }

    async fn drain(&self) {
        // before shutdown, make sure the ack level is up to date
        if let Err(e) = self.complete_transfer().await {
            tracing::error!(error = %e, "Failed to complete transfer task during shutdown");
        }
    }

    async fn complete_transfer_loop(self: Arc<Self>) {
        tracing::info!("Transfer queue processor completeTransferLoop");
        let cfg = self.shard.processor_config().clone();

        let complete_timer = tokio::time::sleep((cfg.transfer_processor_complete_transfer_interval)());
        tokio::pin!(complete_timer);

        loop {
            tokio::select! {
                _ = self.shutdown_ch.cancelled() => {
                    self.drain().await;
                    break;
                }
                _ = &mut complete_timer => {
                    let retries = (cfg.transfer_processor_complete_transfer_failure_retry_count)();
                    let mut attempt: i64 = 0;
                    while attempt < retries {
                        match self.complete_transfer().await {
                            Ok(()) => break,
                            Err(err) => {
                                tracing::error!(error = %err, attempt, "Failed to complete transfer task");
                                if err.is_shard_closed() {
                                    // shard closed, trigger shutdown and bail out
                                    if !self.graceful() {
                                        let t = self.clone();
                                        tokio::spawn(async move { t.stop_impl().await });
                                        tracing::info!("Transfer queue processor completeTransferLoop completed");
                                        return;
                                    }
                                    self.stop_impl().await;
                                    tracing::info!("Transfer queue processor completeTransferLoop completed");
                                    return;
                                }
                                // TODO: the first retry has 0 backoff, revisit it to see if it's expected
                                let retry = Duration::from_millis((attempt * 100) as u64);
                                tokio::select! {
                                    _ = self.shutdown_ch.cancelled() => {
                                        self.drain().await;
                                        tracing::info!("Transfer queue processor completeTransferLoop completed");
                                        return;
                                    }
                                    _ = tokio::time::sleep(retry) => {}
                                }
                            }
                        }
                        attempt += 1;
                    }
                    complete_timer
                        .as_mut()
                        .reset(tokio::time::Instant::now() + (cfg.transfer_processor_complete_transfer_interval)());
                }
            }
        }
        tracing::info!("Transfer queue processor completeTransferLoop completed");
    }

    async fn complete_transfer(&self) -> Result<(), QueueError> {
        let mut new_ack_level: Option<TaskKey> = Some(MAXIMUM_TRANSFER_TASK_KEY);
        let result = self
            .handle_action_impl(CancellationToken::new(), &self.current_cluster_name, Action::new_get_state_action())
            .await?;
        for s in &result.get_state_action_result.as_ref().expect("get state result").states {
            new_ack_level = Some(min_task_key(new_ack_level.unwrap(), s.ack_level()));
        }

        for standby in self.standby_queue_processors.keys() {
            let result =
                self.handle_action_impl(CancellationToken::new(), standby, Action::new_get_state_action()).await?;
            for s in &result.get_state_action_result.as_ref().expect("get state result").states {
                new_ack_level = Some(min_task_key(new_ack_level.unwrap(), s.ack_level()));
            }
        }

        for info in self.shard.get_all_failover_levels(HistoryTaskCategory::Transfer) {
            let failover_level = new_transfer_task_key(info.min_level.task_id);
            new_ack_level = Some(match new_ack_level {
                None => failover_level,
                Some(l) => min_task_key(l, failover_level),
            });
        }

        let Some(new_ack_level) = new_ack_level else {
            panic!("Unable to get transfer queue processor ack level");
        };

        let new_ack_level_task_id = transfer_task_id(new_ack_level);
        let ack_level = self.ack_level.load(Ordering::SeqCst);
        tracing::debug!(from = ack_level, to = new_ack_level_task_id, "Start completing transfer task");
        if ack_level >= new_ack_level_task_id {
            return Ok(());
        }

        self.metrics.inc_counter("TaskBatchCompleteCounter");

        loop {
            let page_size = (self.shard.processor_config().transfer_task_delete_batch_size)();
            // we're switching from exclusive begin/end to inclusive min/exclusive max,
            // so we need to adjust the taskID: add 1 to both the min and max taskID
            let completed = self
                .shard
                .range_complete_history_task(RangeCompleteHistoryTaskRequest {
                    task_category: HistoryTaskCategory::Transfer,
                    inclusive_min_task_key: HistoryKey::immediate(ack_level + 1),
                    exclusive_max_task_key: HistoryKey::immediate(new_ack_level_task_id + 1),
                    page_size,
                    shard_id: self.shard.get_shard_id(),
                })
                .await?;
            if !has_more_rows_to_delete(completed, page_size) {
                break;
            }
        }

        self.ack_level.store(new_ack_level_task_id, Ordering::SeqCst);

        self.shard
            .update_queue_ack_level(HistoryTaskCategory::Transfer, HistoryKey::immediate(new_ack_level_task_id))
            .await
    }

    async fn handle_action_impl(
        &self,
        ctx: CancellationToken,
        cluster_name: &str,
        action: Action,
    ) -> Result<ActionResult, QueueError> {
        let rx = if cluster_name == self.current_cluster_name {
            self.active_queue_processor.base.add_action(Some(ctx.clone()), action).await
        } else {
            match self.standby_queue_processors.iter().find(|(name, _)| name.as_str() == cluster_name) {
                Some((_, p)) => p.base.add_action(Some(ctx.clone()), action).await,
                None => return Err(QueueError::Other(format!("unknown cluster name: {cluster_name}"))),
            }
        };

        let Some(rx) = rx else {
            if ctx.is_cancelled() {
                return Err(QueueError::Other("context canceled".to_string()));
            }
            return Err(QueueError::Other(ERR_PROCESSOR_SHUTDOWN.to_string()));
        };

        tokio::select! {
            r = rx => match r {
                Ok(Ok(Some(res))) => Ok(res),
                Ok(Ok(None)) => Ok(ActionResult::default()),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(QueueError::Other(ERR_PROCESSOR_SHUTDOWN.to_string())),
            },
            _ = self.shutdown_ch.cancelled() => Err(QueueError::Other(ERR_PROCESSOR_SHUTDOWN.to_string())),
            _ = ctx.cancelled() => Err(QueueError::Other("context canceled".to_string())),
        }
    }
}

#[async_trait]
impl Daemon for TransferQueueProcessor {
    fn start(self: Arc<Self>) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        tracing::info!("Starting transfer queue processor");
        self.active_queue_processor.clone().start();
        for p in self.standby_queue_processors.values() {
            p.clone().start();
        }

        let t = self.clone();
        self.shutdown_wg.spawn(async move { t.complete_transfer_loop().await });
        tracing::info!("Transfer queue processor started");
    }

    async fn stop(&self) {
        // `stop_impl` needs `Arc<Self>` only to spawn a detached stop from the complete loop.
        // Callers holding `Arc<Self>` use `Processor::stop_arc`; the plain path re-wraps via a shim.
        self.stop_shim().await
    }
}

impl TransferQueueProcessor {
    async fn stop_shim(&self) {
        // Equivalent of stop_impl without the self-spawn; keep the two in sync.
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        if !self.graceful() {
            tracing::info!("Stopping transfer queue processor non-gracefully");
            self.active_queue_processor.stop().await;
            for p in self.standby_queue_processors.values() {
                p.stop().await;
            }
            self.shutdown_ch.cancel();
            self.shutdown_wg.close();
            if tokio::time::timeout(Duration::from_secs(60), self.shutdown_wg.wait()).await.is_err() {
                tracing::warn!("transferQueueProcessor timed out on shut down");
            }
            tracing::info!("Transfer queue processor stopped non-gracefully");
            return;
        }
        tracing::info!("Stopping transfer queue processor gracefully");
        self.shutdown_ch.cancel();
        self.shutdown_wg.close();
        if tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, self.shutdown_wg.wait()).await.is_err() {
            tracing::warn!("transferQueueProcessor timed out on shut down");
        }
        self.active_queue_processor.stop().await;
        for p in self.standby_queue_processors.values() {
            p.stop().await;
        }
        let failovers: Vec<_> = self.failover_queue_processors.lock().unwrap().clone();
        if !failovers.is_empty() {
            tracing::info!(count = failovers.len(), "Shutting down failover transfer queues");
            for p in failovers {
                p.stop().await;
            }
        }
        tracing::info!("Transfer queue processor stopped gracefully");
    }
}

#[async_trait]
impl Processor for TransferQueueProcessor {
    fn notify_new_task(&self, cluster_name: &str, info: &NotifyTaskInfo) {
        if info.tasks.is_empty() {
            return;
        }

        if cluster_name == self.current_cluster_name {
            self.active_queue_processor.notify_new_task(info);
            return;
        }

        match self.standby_queue_processors.get(cluster_name) {
            Some(p) => p.notify_new_task(info),
            None => panic!("Cannot find transfer processor for {cluster_name}."),
        }
    }

    async fn failover_domain(&self, domain_ids: HashSet<String>) {
        self.failover_domain_async(domain_ids).await
    }

    async fn handle_action(
        &self,
        ctx: CancellationToken,
        cluster_name: &str,
        action: Action,
    ) -> Result<ActionResult, QueueError> {
        self.handle_action_impl(ctx, cluster_name, action).await
    }

    async fn lock_task_processing(&self) {
        tracing::debug!("Transfer queue processor locking task processing");
        self.task_allocator.lock().await;
    }

    fn unlock_task_processing(&self) {
        tracing::debug!("Transfer queue processor unlocking task processing");
        self.task_allocator.unlock();
    }
}

impl TransferQueueProcessor {
    /// Go `FailoverDomain`.
    pub async fn failover_domain_async(&self, domain_ids: HashSet<String>) {
        if (self.shard.processor_config().disable_transfer_failover_queue)() {
            return;
        }

        // Failover queue is used to scan all inflight tasks. If queue processor is not
        // started, there's no inflight task, and we don't need to create a failover processor.
        // Also, the HandleAction will be blocked if queue processor processing loop is not running.
        if self.status.load(Ordering::SeqCst) != DAEMON_STATUS_STARTED {
            return;
        }

        let mut min_level =
            self.shard.get_queue_cluster_ack_level(HistoryTaskCategory::Transfer, &self.current_cluster_name).task_id;
        let mut standby_cluster_name = self.current_cluster_name.clone();
        for cluster_name in self.shard.enabled_cluster_names() {
            let ack_level =
                self.shard.get_queue_cluster_ack_level(HistoryTaskCategory::Transfer, &cluster_name).task_id;
            if ack_level < min_level {
                min_level = ack_level;
                standby_cluster_name = cluster_name;
            }
        }

        let mut max_read_level: i64 = 0;
        let action_result = match self
            .handle_action_impl(CancellationToken::new(), &self.current_cluster_name, Action::new_get_state_action())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(domain_ids = ?domain_ids, error = %e, "Transfer Failover Failed");
                if e.to_string() == ERR_PROCESSOR_SHUTDOWN {
                    // processor/shard already shutdown, we don't need to create failover queue processor
                    return;
                }
                // other errors should never be returned for GetStateAction
                panic!("unknown error for GetStateAction: {e}");
            }
        };
        for s in &action_result.get_state_action_result.as_ref().expect("get state result").states {
            let queue_read_level = transfer_task_id(s.read_level());
            if max_read_level < queue_read_level {
                max_read_level = queue_read_level;
            }
        }
        // maxReadLevel is exclusive, so add 1
        max_read_level += 1;

        tracing::info!(domain_ids = ?domain_ids, min_level, max_level = max_read_level, "Transfer Failover Triggered");

        let (update_shard_ack_level, failover_queue_processor) = new_transfer_queue_failover_processor(
            &self.deps,
            min_level,
            max_read_level,
            domain_ids,
            &standby_cluster_name,
        );

        // NOTE: READ REF BEFORE MODIFICATION
        // ref: historyEngine.go registerDomainFailoverCallback function
        if let Err(e) = update_shard_ack_level(new_transfer_task_key(min_level)).await {
            tracing::error!(error = %e, "Error update shard ack level");
        }

        // Failover queue processors are started on the fly when domains are failed over.
        // They are stopped when the transfer queue instance is stopped; there is no mechanism to
        // re-start ongoing failover queue processors in the new shard owner.
        self.failover_queue_processors.lock().unwrap().push(failover_queue_processor.clone());
        failover_queue_processor.clone().start();
    }
}

fn load_task_initializer(
    deps: &TransferQueueDeps,
    queue_type: QueueType,
    factory: HistoryTaskFactory,
    filter: TaskFilter,
) -> TaskInitializer {
    let _ = deps;
    Arc::new(move |info: Arc<dyn PersistenceTask>| factory(info, queue_type, filter.clone()))
}

fn unexpected_task_err() -> QueueError {
    QueueError::Other(ERR_UNEXPECTED_QUEUE_TASK.to_string())
}

/// Go `newTransferQueueActiveProcessor`.
fn new_transfer_queue_active_processor(deps: &TransferQueueDeps) -> Arc<TransferQueueProcessorBase> {
    let shard = deps.shard.clone();
    let cfg = shard.queue_config();
    let options = Arc::new(new_transfer_queue_processor_options(&cfg, true, false));

    let current_cluster_name = shard.current_cluster_name();

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        Arc::new(move |task: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            Box::pin(async move {
                if task.get_task_category() != HistoryTaskCategory::Transfer {
                    return Err(unexpected_task_err());
                }
                if let Ok(true) = is_domain_not_registered(shard.domain_cache().as_ref(), &task.get_domain_id()) {
                    tracing::info!(
                        domain_id = %task.get_domain_id(),
                        task = ?task,
                        "Domain is not in registered status, skip task in active transfer queue."
                    );
                    return Ok(false);
                }
                allocator.verify_active_task(&task.get_domain_id(), &task.get_workflow_id(), &task.get_run_id()).await
            })
        })
    };

    let update_max_read_level: UpdateMaxReadLevelFn = {
        let shard = shard.clone();
        let cluster = current_cluster_name.clone();
        Arc::new(move || {
            new_transfer_task_key(
                shard.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Transfer, &cluster).task_id,
            )
        })
    };

    let update_cluster_ack_level: UpdateClusterAckLevelFn = {
        let shard = shard.clone();
        let cluster = current_cluster_name.clone();
        Arc::new(move |ack_level: TaskKey| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                let task_id = transfer_task_id(ack_level);
                shard
                    .update_queue_cluster_ack_level(HistoryTaskCategory::Transfer, &cluster, HistoryKey::immediate(task_id))
                    .await
            })
        })
    };

    let update_processing_queue_states: UpdateProcessingQueueStatesFn = {
        let shard = shard.clone();
        let cluster = current_cluster_name.clone();
        Arc::new(move |states: Vec<ProcessingQueueState>| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                let p_states = convert_to_persistence_transfer_processing_queue_states(&states);
                shard.update_transfer_processing_queue_states(&cluster, p_states).await
            })
        })
    };

    let queue_shutdown: QueueShutdownFn = Arc::new(|| Box::pin(async { Ok(()) }));

    let states = load_transfer_processing_queue_states(&current_cluster_name, shard.as_ref(), &options);
    TransferQueueProcessorBase::new(
        shard.clone(),
        states,
        deps.task_processor.clone(),
        (deps.redispatcher_factory)(QueueType::ActiveTransfer),
        options.clone(),
        update_max_read_level,
        update_cluster_ack_level,
        Some(update_processing_queue_states),
        queue_shutdown,
        load_task_initializer(deps, QueueType::ActiveTransfer, deps.active_task_factory.clone(), task_filter),
        deps.metrics.clone(),
        (deps.rate_limiter_factory)(options.max_poll_rps.clone()),
    )
}

/// Go `newTransferQueueStandbyProcessor`.
fn new_transfer_queue_standby_processor(
    cluster_name: &str,
    deps: &TransferQueueDeps,
    factory: HistoryTaskFactory,
) -> Arc<TransferQueueProcessorBase> {
    let shard = deps.shard.clone();
    let cfg = shard.queue_config();
    let options = Arc::new(new_transfer_queue_processor_options(&cfg, false, false));
    let cluster_name = cluster_name.to_string();

    // TransferTaskTypeCloseExecution = 2, TransferTaskTypeRecordWorkflowClosed = 9
    const TRANSFER_TASK_TYPE_CLOSE_EXECUTION: i32 = 2;
    const TRANSFER_TASK_TYPE_RECORD_WORKFLOW_CLOSED: i32 = 9;

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        let cluster = cluster_name.clone();
        Arc::new(move |task: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                if task.get_task_category() != HistoryTaskCategory::Transfer {
                    return Err(unexpected_task_err());
                }
                if let Ok(true) = is_domain_not_registered(shard.domain_cache().as_ref(), &task.get_domain_id()) {
                    tracing::info!(
                        domain_id = %task.get_domain_id(),
                        task = ?task,
                        "Domain is not in registered status, skip task in standby transfer queue."
                    );
                    return Ok(false);
                }
                if task.get_task_type() == TRANSFER_TASK_TYPE_CLOSE_EXECUTION
                    || task.get_task_type() == TRANSFER_TASK_TYPE_RECORD_WORKFLOW_CLOSED
                {
                    match shard.domain_cache().get_domain_by_id(&task.get_domain_id()) {
                        Ok(entry) => {
                            if entry.has_replication_cluster(&cluster) {
                                // guarantee the processing of workflow execution close
                                return Ok(true);
                            }
                        }
                        Err(QueueError::EntityNotExists(_)) => {
                            tracing::warn!(
                                domain_id = %task.get_domain_id(),
                                task = ?task,
                                "Cannot find domain, default to not process task."
                            );
                            return Ok(false);
                        }
                        Err(e) => {
                            // retry the task if failed to find the domain
                            tracing::warn!(domain_id = %task.get_domain_id(), "Cannot find domain");
                            return Err(e);
                        }
                    }
                }
                allocator
                    .verify_standby_task(&cluster, &task.get_domain_id(), &task.get_workflow_id(), &task.get_run_id())
                    .await
            })
        })
    };

    let update_max_read_level: UpdateMaxReadLevelFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move || {
            new_transfer_task_key(
                shard.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Transfer, &cluster).task_id,
            )
        })
    };

    let update_cluster_ack_level: UpdateClusterAckLevelFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move |ack_level: TaskKey| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                let task_id = transfer_task_id(ack_level);
                shard
                    .update_queue_cluster_ack_level(HistoryTaskCategory::Transfer, &cluster, HistoryKey::immediate(task_id))
                    .await
            })
        })
    };

    let update_processing_queue_states: UpdateProcessingQueueStatesFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move |states: Vec<ProcessingQueueState>| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                let p_states = convert_to_persistence_transfer_processing_queue_states(&states);
                shard.update_transfer_processing_queue_states(&cluster, p_states).await
            })
        })
    };

    let queue_shutdown: QueueShutdownFn = Arc::new(|| Box::pin(async { Ok(()) }));

    let states = load_transfer_processing_queue_states(&cluster_name, shard.as_ref(), &options);
    TransferQueueProcessorBase::new(
        shard.clone(),
        states,
        deps.task_processor.clone(),
        (deps.redispatcher_factory)(QueueType::StandbyTransfer),
        options.clone(),
        update_max_read_level,
        update_cluster_ack_level,
        Some(update_processing_queue_states),
        queue_shutdown,
        load_task_initializer(deps, QueueType::StandbyTransfer, factory, task_filter),
        deps.metrics.clone(),
        (deps.rate_limiter_factory)(options.max_poll_rps.clone()),
    )
}

type UpdateClusterAckLevelAsync =
    Arc<dyn Fn(TaskKey) -> super::deps::BoxFut<Result<(), QueueError>> + Send + Sync>;

/// Go `newTransferQueueFailoverProcessor`: returns (updateClusterAckLevelFn, processor).
fn new_transfer_queue_failover_processor(
    deps: &TransferQueueDeps,
    min_level: i64,
    max_level: i64,
    domain_ids: HashSet<String>,
    standby_cluster_name: &str,
) -> (UpdateClusterAckLevelAsync, Arc<TransferQueueProcessorBase>) {
    let shard = deps.shard.clone();
    let cfg = shard.queue_config();
    let options = Arc::new(new_transfer_queue_processor_options(&cfg, true, true));

    let failover_uuid = format!("{:032x}", rand::random::<u128>());
    tracing::info!(
        cluster = %shard.current_cluster_name(),
        domain_ids = ?domain_ids,
        failover = %format!("from: {standby_cluster_name}"),
        "creating transfer failover processor"
    );

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        let domain_ids = domain_ids.clone();
        Arc::new(move |task: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            let domain_ids = domain_ids.clone();
            Box::pin(async move {
                if task.get_task_category() != HistoryTaskCategory::Transfer {
                    return Err(unexpected_task_err());
                }
                if let Ok(true) = is_domain_not_registered(shard.domain_cache().as_ref(), &task.get_domain_id()) {
                    tracing::info!(
                        domain_id = %task.get_domain_id(),
                        task = ?task,
                        "Domain is not in registered status, skip task in failover transfer queue."
                    );
                    return Ok(false);
                }
                allocator
                    .verify_failover_active_task(
                        &domain_ids,
                        &task.get_domain_id(),
                        &task.get_workflow_id(),
                        &task.get_run_id(),
                    )
                    .await
            })
        })
    };

    let max_read_level_task_key = new_transfer_task_key(max_level);
    let update_max_read_level: UpdateMaxReadLevelFn = Arc::new(move || max_read_level_task_key); // this is a const

    let update_cluster_ack_level: UpdateClusterAckLevelAsync = {
        let shard = shard.clone();
        let failover_uuid = failover_uuid.clone();
        let domain_ids = domain_ids.clone();
        Arc::new(move |ack_level: TaskKey| {
            let shard = shard.clone();
            let failover_uuid = failover_uuid.clone();
            let domain_ids = domain_ids.clone();
            Box::pin(async move {
                let task_id = transfer_task_id(ack_level);
                let level = FailoverLevel {
                    start_time: shard.now(),
                    min_level: HistoryKey::immediate(min_level),
                    current_level: HistoryKey::immediate(task_id),
                    max_level: HistoryKey::immediate(max_level),
                    domain_ids,
                };
                shard.update_failover_level(HistoryTaskCategory::Transfer, &failover_uuid, level).await
            })
        })
    };

    let queue_shutdown: QueueShutdownFn = {
        let shard = shard.clone();
        let failover_uuid = failover_uuid.clone();
        Arc::new(move || {
            let shard = shard.clone();
            let failover_uuid = failover_uuid.clone();
            Box::pin(async move { shard.delete_failover_level(HistoryTaskCategory::Transfer, &failover_uuid).await })
        })
    };

    let processing_queue_states = vec![ProcessingQueueState::new(
        DEFAULT_PROCESSING_QUEUE_LEVEL,
        new_transfer_task_key(min_level),
        max_read_level_task_key,
        DomainFilter::new(domain_ids.clone(), false),
    )];

    let processor = TransferQueueProcessorBase::new(
        shard.clone(),
        processing_queue_states,
        deps.task_processor.clone(),
        (deps.redispatcher_factory)(QueueType::ActiveTransfer),
        options.clone(),
        update_max_read_level,
        update_cluster_ack_level.clone(),
        None,
        queue_shutdown,
        load_task_initializer(deps, QueueType::ActiveTransfer, deps.active_task_factory.clone(), task_filter),
        deps.metrics.clone(),
        (deps.rate_limiter_factory)(options.max_poll_rps.clone()),
    );
    (update_cluster_ack_level, processor)
}

/// Go `loadTransferProcessingQueueStates`.
fn load_transfer_processing_queue_states(
    cluster_name: &str,
    shard: &dyn QueueShardContext,
    options: &super::processor_options::QueueProcessorOptions,
) -> Vec<ProcessingQueueState> {
    let ack_level = shard.get_queue_cluster_ack_level(HistoryTaskCategory::Transfer, cluster_name).task_id;
    if (options.enable_load_queue_states)() {
        let p_states = shard.get_transfer_processing_queue_states(cluster_name);
        if validate_processing_queue_states(&p_states, AckLevelValue::TaskId(ack_level)) {
            return convert_from_persistence_transfer_processing_queue_states(&p_states);
        }

        tracing::error!(states = ?p_states, shard_transfer_acks = ack_level, "Incompatible processing queue states and ackLevel");
    }

    // LoadQueueStates is disabled or sanity check failed: fallback to use ackLevel
    vec![ProcessingQueueState::new(
        DEFAULT_PROCESSING_QUEUE_LEVEL,
        new_transfer_task_key(ack_level),
        MAXIMUM_TRANSFER_TASK_KEY,
        DomainFilter::new(HashSet::new(), true),
    )]
}

#[allow(dead_code)]
fn _min(_: Option<TaskKey>) {
    let _ = min_task_key;
}
