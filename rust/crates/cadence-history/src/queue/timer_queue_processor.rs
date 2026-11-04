//! Port of `go/service/history/queue/timer_queue_processor.go`.
//!
//! Rust-forced differences (no logic change), same as `transfer_queue_processor.rs`: executors,
//! redispatchers and rate limiters are built by the caller and arrive through `TimerQueueDeps`;
//! `time.Timer` loops are tokio `select!` loops.
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
    TaskInitializer, TaskKey, TaskProcessor, Time,
};
use super::domain_filter::DomainFilter;
use super::interface::Processor;
use super::processing_queue::min_task_key;
use super::processing_queue_state::ProcessingQueueState;
use super::processor_base::{DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED};
use super::processor_options::QueueProcessorOptions;
use super::shard_deps::{
    has_more_rows_to_delete, HistoryKey, HistoryTaskFactory, QueueShardContext, QueueType,
    RangeCompleteHistoryTaskRequest, TaskFilter,
};
use super::task_allocator::TaskAllocator;
use super::timer_gate::TimerGate;
use super::timer_queue_active_processor::new_timer_queue_active_processor;
use super::timer_queue_failover_processor::new_timer_queue_failover_processor;
use super::timer_queue_processor_base::TimerQueueProcessorBase;
use super::timer_queue_standby_processor::new_timer_queue_standby_processor;
use super::transfer_queue_processor::{ERR_PROCESSOR_SHUTDOWN, ERR_UNEXPECTED_QUEUE_TASK};
use super::util::{
    convert_from_persistence_timer_processing_queue_states, new_timer_task_key, validate_processing_queue_states,
    AckLevelValue, MAXIMUM_TIMER_TASK_KEY,
};

/// `persistence.TaskTypeWorkflowTimeout`
pub const TASK_TYPE_WORKFLOW_TIMEOUT: i32 = 3;
/// `persistence.TaskTypeDeleteHistoryEvent`
pub const TASK_TYPE_DELETE_HISTORY_EVENT: i32 = 4;

/// Wiring that Go builds inline in `NewTimerQueueProcessor` from executors etc.
pub struct TimerQueueDeps {
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

/// Go `timerTaskKey.visibilityTimestamp`.
pub fn timer_visibility(k: TaskKey) -> Time {
    super::timer_queue_processor_base::timer_visibility_timestamp(k)
}

pub(crate) fn unexpected_task_err() -> QueueError {
    QueueError::Other(ERR_UNEXPECTED_QUEUE_TASK.to_string())
}

pub(crate) fn load_timer_task_initializer(
    queue_type: QueueType,
    factory: HistoryTaskFactory,
    filter: TaskFilter,
) -> TaskInitializer {
    Arc::new(move |info: Arc<dyn PersistenceTask>| factory(info, queue_type, filter.clone()))
}

pub struct TimerQueueProcessor {
    shard: Arc<dyn QueueShardContext>,
    current_cluster_name: String,

    status: AtomicI32,
    shutdown_ch: CancellationToken,
    shutdown_wg: TaskTracker,

    ack_level: AtomicI64,
    task_allocator: Arc<dyn TaskAllocator>,
    metrics: Arc<dyn MetricsScope>,
    deps: TimerQueueDeps,
    active_queue_processor: Arc<TimerQueueProcessorBase>,
    standby_queue_processors: HashMap<String, Arc<TimerQueueProcessorBase>>,
    standby_queue_timer_gates: HashMap<String, Arc<TimerGate>>,
    failover_queue_processors: StdMutex<Vec<Arc<TimerQueueProcessorBase>>>,
}

impl TimerQueueProcessor {
    /// Go `NewTimerQueueProcessor`.
    pub fn new(deps: TimerQueueDeps) -> Arc<Self> {
        let shard = deps.shard.clone();
        let current_cluster_name = shard.current_cluster_name();

        let active_queue_processor = new_timer_queue_active_processor(&deps);

        let mut standby_queue_processors = HashMap::new();
        let mut standby_queue_timer_gates = HashMap::new();
        for cluster_name in shard.remote_cluster_names() {
            let factory = deps
                .standby_task_factories
                .get(&cluster_name)
                .unwrap_or_else(|| panic!("missing standby task factory for {cluster_name}"))
                .clone();
            let (p, gate) = new_timer_queue_standby_processor(&cluster_name, &deps, factory);
            standby_queue_processors.insert(cluster_name.clone(), p);
            standby_queue_timer_gates.insert(cluster_name, gate);
        }

        Arc::new(TimerQueueProcessor {
            shard: shard.clone(),
            current_cluster_name,
            status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
            shutdown_ch: CancellationToken::new(),
            shutdown_wg: TaskTracker::new(),
            ack_level: AtomicI64::new(shard.get_queue_ack_level(HistoryTaskCategory::Timer).scheduled_time.unix_nano()),
            task_allocator: deps.task_allocator.clone(),
            metrics: deps.metrics.clone(),
            deps,
            active_queue_processor,
            standby_queue_processors,
            standby_queue_timer_gates,
            failover_queue_processors: StdMutex::new(Vec::new()),
        })
    }

    fn graceful(&self) -> bool {
        (self.shard.processor_config().queue_processor_enable_graceful_sync_shutdown)()
    }

    /// Go `Stop`. `detached` is false here; the complete loop spawns its own task for the
    /// non-graceful shard-closed path, as Go does with `go t.Stop()`.
    async fn stop_impl(&self) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        if !self.graceful() {
            tracing::info!("Stopping timer queue processor non-gracefully");
            self.active_queue_processor.stop().await;
            for p in self.standby_queue_processors.values() {
                p.stop().await;
            }

            self.shutdown_ch.cancel();
            self.shutdown_wg.close();
            if tokio::time::timeout(Duration::from_secs(60), self.shutdown_wg.wait()).await.is_err() {
                tracing::warn!("timerQueueProcessor timed out on shut down");
            }
            tracing::info!("Timer queue processor stopped non-gracefully");
            return;
        }

        tracing::info!("Stopping timer queue processor gracefully");

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
            tracing::info!(count = failovers.len(), "Shutting down failover timer queues");
            for p in failovers {
                p.stop().await;
            }
        }
        tracing::info!("Timer queue processor stopped gracefully");
    }

    async fn drain(&self) {
        if !self.graceful() {
            if let Err(e) = self.complete_timer(CancellationToken::new()).await {
                tracing::error!(error = %e, "Failed to complete timer task during drain");
            }
            return;
        }

        let ctx = CancellationToken::new();
        let r = tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, self.complete_timer(ctx.clone())).await;
        ctx.cancel();
        match r {
            Ok(Err(e)) => tracing::error!(error = %e, "Failed to complete timer task during drain"),
            Err(_) => tracing::error!(error = "context deadline exceeded", "Failed to complete timer task during drain"),
            Ok(Ok(())) => {}
        }
    }

    async fn complete_timer_loop(self: Arc<Self>) {
        tracing::info!("Timer queue processor completeTimerLoop");
        let cfg = self.shard.processor_config().clone();

        let complete_timer = tokio::time::sleep((cfg.timer_processor_complete_timer_interval)());
        tokio::pin!(complete_timer);

        loop {
            tokio::select! {
                _ = self.shutdown_ch.cancelled() => {
                    self.drain().await;
                    break;
                }
                _ = &mut complete_timer => {
                    let retries = (cfg.timer_processor_complete_timer_failure_retry_count)();
                    let mut attempt: i64 = 0;
                    while attempt < retries {
                        match self.complete_timer(CancellationToken::new()).await {
                            Ok(()) => break,
                            Err(err) => {
                                tracing::error!(error = %err, attempt, "Failed to complete timer task");
                                if err.is_shard_closed() {
                                    if !self.graceful() {
                                        let t = self.clone();
                                        tokio::spawn(async move { t.stop_impl().await });
                                        tracing::info!("Timer queue processor completeTimerLoop completed");
                                        return;
                                    }
                                    self.stop_impl().await;
                                    tracing::info!("Timer queue processor completeTimerLoop completed");
                                    return;
                                }

                                let retry = Duration::from_millis((attempt * 100) as u64);
                                tokio::select! {
                                    _ = self.shutdown_ch.cancelled() => {
                                        self.drain().await;
                                        tracing::info!("Timer queue processor completeTimerLoop completed");
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
                        .reset(tokio::time::Instant::now() + (cfg.timer_processor_complete_timer_interval)());
                }
            }
        }
        tracing::info!("Timer queue processor completeTimerLoop completed");
    }

    async fn complete_timer(&self, ctx: CancellationToken) -> Result<(), QueueError> {
        let mut new_ack_level = MAXIMUM_TIMER_TASK_KEY;
        let result =
            self.handle_action_impl(ctx.clone(), &self.current_cluster_name, Action::new_get_state_action()).await?;
        for s in &result.get_state_action_result.as_ref().expect("get state result").states {
            new_ack_level = min_task_key(new_ack_level, s.ack_level());
        }

        for standby in self.standby_queue_processors.keys() {
            let result = self.handle_action_impl(ctx.clone(), standby, Action::new_get_state_action()).await?;
            for s in &result.get_state_action_result.as_ref().expect("get state result").states {
                new_ack_level = min_task_key(new_ack_level, s.ack_level());
            }
        }

        for info in self.shard.get_all_failover_levels(HistoryTaskCategory::Timer) {
            let failover_level = new_timer_task_key(info.min_level.scheduled_time, 0);
            new_ack_level = min_task_key(new_ack_level, failover_level);
        }

        if new_ack_level == MAXIMUM_TIMER_TASK_KEY {
            panic!("Unable to get timer queue processor ack level");
        }

        let new_ack_level_timestamp = timer_visibility(new_ack_level);
        let ack_level = Time(self.ack_level.load(Ordering::SeqCst));
        if !ack_level.before(new_ack_level_timestamp) {
            tracing::debug!(
                new = new_ack_level_timestamp.unix_nano(),
                current = ack_level.unix_nano(),
                "Skipping timer task completion because new ack level is not before ack level"
            );
            return Ok(());
        }

        tracing::debug!(from = ack_level.unix_nano(), to = new_ack_level_timestamp.unix_nano(), "Start completing timer task");
        self.metrics.inc_counter("TaskBatchCompleteCounter");

        let mut total_deleted: i64 = 0;
        loop {
            let page_size = (self.shard.processor_config().timer_task_delete_batch_size)();
            let completed = self
                .shard
                .range_complete_history_task(RangeCompleteHistoryTaskRequest {
                    task_category: HistoryTaskCategory::Timer,
                    inclusive_min_task_key: HistoryKey::new(ack_level, 0),
                    exclusive_max_task_key: HistoryKey::new(new_ack_level_timestamp, 0),
                    page_size,
                    shard_id: self.shard.get_shard_id(),
                })
                .await?;
            total_deleted += completed;
            tracing::debug!(page_size, total_deleted, "Timer task batch deletion");
            if !has_more_rows_to_delete(completed, page_size) {
                break;
            }
        }

        self.ack_level.store(new_ack_level_timestamp.unix_nano(), Ordering::SeqCst);

        self.shard
            .update_queue_ack_level(HistoryTaskCategory::Timer, HistoryKey::new(new_ack_level_timestamp, 0))
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

    /// Go `FailoverDomain`.
    pub async fn failover_domain_async(&self, domain_ids: HashSet<String>) {
        if (self.shard.processor_config().disable_timer_failover_queue)() {
            return;
        }

        if self.status.load(Ordering::SeqCst) != DAEMON_STATUS_STARTED {
            return;
        }

        let mut min_level =
            self.shard.get_queue_cluster_ack_level(HistoryTaskCategory::Timer, &self.current_cluster_name).scheduled_time;
        let mut standby_cluster_name = self.current_cluster_name.clone();
        for cluster_name in self.shard.enabled_cluster_names() {
            let ack_level =
                self.shard.get_queue_cluster_ack_level(HistoryTaskCategory::Timer, &cluster_name).scheduled_time;
            if ack_level.before(min_level) {
                min_level = ack_level;
                standby_cluster_name = cluster_name;
            }
        }

        if standby_cluster_name != self.current_cluster_name {
            tracing::debug!(
                min_level = min_level.unix_nano(),
                standby_cluster_name = %standby_cluster_name,
                "Timer queue failover will use minLevel from standbyClusterName"
            );
        } else {
            tracing::debug!(
                min_level = min_level.unix_nano(),
                cluster = %self.current_cluster_name,
                "Timer queue failover will use minLevel from current cluster"
            );
        }

        let mut max_read_level = Time::ZERO;
        let action_result = match self
            .handle_action_impl(CancellationToken::new(), &self.current_cluster_name, Action::new_get_state_action())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(domain_ids = ?domain_ids, error = %e, "Timer failover failed while getting queue states");
                if e.to_string() == ERR_PROCESSOR_SHUTDOWN {
                    return;
                }
                panic!("unknown error for GetStateAction: {e}");
            }
        };

        let mut max_read_level_queue_level = 0;
        for s in &action_result.get_state_action_result.as_ref().expect("get state result").states {
            let queue_read_level = timer_visibility(s.read_level());
            if max_read_level.before(queue_read_level) {
                max_read_level = queue_read_level;
                max_read_level_queue_level = s.level();
            }
        }

        if max_read_level != Time::ZERO {
            tracing::debug!(
                max_read_level = max_read_level.unix_nano(),
                level = max_read_level_queue_level,
                "Timer queue failover will use maxReadLevel"
            );
        }

        // Go: `maxReadLevel.Add(1 * time.Millisecond)` discards its result (time.Time is immutable),
        // so maxReadLevel is intentionally left unchanged.
        let _ = max_read_level.add(Duration::from_millis(1));

        tracing::info!(
            domain_ids = ?domain_ids,
            min_level = min_level.unix_nano(),
            max_level = max_read_level.unix_nano(),
            "Timer Failover Triggered"
        );

        let (update_cluster_ack_level_fn, failover_queue_processor) =
            new_timer_queue_failover_processor(&self.deps, &standby_cluster_name, min_level, max_read_level, domain_ids);

        if let Err(e) = update_cluster_ack_level_fn(new_timer_task_key(min_level, 0)).await {
            tracing::error!(error = %e, "Error update shard ack level");
        }

        self.failover_queue_processors.lock().unwrap().push(failover_queue_processor.clone());
        failover_queue_processor.start();
    }
}

#[async_trait]
impl Daemon for TimerQueueProcessor {
    fn start(self: Arc<Self>) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        tracing::info!("Starting timer queue processor");
        self.active_queue_processor.start();
        for p in self.standby_queue_processors.values() {
            p.start();
        }

        let t = self.clone();
        self.shutdown_wg.spawn(async move { t.complete_timer_loop().await });
        tracing::info!("Timer queue processor started");
    }

    async fn stop(&self) {
        self.stop_impl().await
    }
}

#[async_trait]
impl Processor for TimerQueueProcessor {
    fn notify_new_task(&self, cluster_name: &str, info: &NotifyTaskInfo) {
        if cluster_name == self.current_cluster_name {
            self.active_queue_processor.notify_new_timers(&info.tasks);
            return;
        }

        let Some(standby_queue_processor) = self.standby_queue_processors.get(cluster_name) else {
            panic!("Cannot find standby timer processor for {cluster_name}.");
        };
        let Some(standby_queue_timer_gate) = self.standby_queue_timer_gates.get(cluster_name) else {
            panic!("Cannot find standby timer gate for {cluster_name}.");
        };

        let cur_time = info.cluster_current_times.get(cluster_name).copied().unwrap_or(Time::ZERO);
        standby_queue_timer_gate.set_current_time(cur_time);
        tracing::debug!(
            cluster = %cluster_name,
            timestamp = cur_time.unix_nano(),
            "Current time for standby queue timergate is updated"
        );
        standby_queue_processor.notify_new_timers(&info.tasks);
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
        tracing::debug!("Timer queue processor locking task processing");
        self.task_allocator.lock().await;
    }

    fn unlock_task_processing(&self) {
        tracing::debug!("Timer queue processor unlocking task processing");
        self.task_allocator.unlock();
    }
}

/// Go `loadTimerProcessingQueueStates`.
pub(crate) fn load_timer_processing_queue_states(
    cluster_name: &str,
    shard: &dyn QueueShardContext,
    options: &QueueProcessorOptions,
) -> Vec<ProcessingQueueState> {
    let ack_level = shard.get_queue_cluster_ack_level(HistoryTaskCategory::Timer, cluster_name).scheduled_time;
    if (options.enable_load_queue_states)() {
        let p_states = shard.get_timer_processing_queue_states(cluster_name);
        if validate_processing_queue_states(&p_states, AckLevelValue::Time(ack_level)) {
            return convert_from_persistence_timer_processing_queue_states(&p_states);
        }

        tracing::error!(
            states = ?p_states,
            shard_timer_acks = ack_level.unix_nano(),
            "Incompatible processing queue states and ackLevel"
        );
    }

    vec![ProcessingQueueState::new(
        DEFAULT_PROCESSING_QUEUE_LEVEL,
        new_timer_task_key(ack_level, 0),
        MAXIMUM_TIMER_TASK_KEY,
        DomainFilter::new(HashSet::new(), true),
    )]
}
