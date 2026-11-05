//! Port of `go/service/history/queue/timer_queue_processor.go`, `timer_queue_active_processor.go`,
//! `timer_queue_standby_processor.go` and `timer_queue_failover_processor.go`.
//!
//! Rust-forced differences (no logic change): task executors / archival client / history resender
//! are built by the caller and arrive as `HistoryTaskFactory` closures (task/ is ported elsewhere,
//! so executor `Stop()` is owned by the caller too); `time.Timer` loops are tokio `select!` loops.
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
    Daemon, HistoryTaskCategory, MetricsScope, NotifyTaskInfo, PersistenceTask, QueueError, TaskInitializer, TaskKey,
    Time,
};
use super::domain_filter::DomainFilter;
use super::interface::Processor;
use super::processing_queue::min_task_key;
use super::processing_queue_state::ProcessingQueueState;
use super::processor_base::{
    QueueShutdownFn, UpdateClusterAckLevelFn, UpdateMaxReadLevelFn, UpdateProcessingQueueStatesFn,
    DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED,
};
use super::processor_options_factory::new_timer_queue_processor_options;
use super::shard_deps::{
    has_more_rows_to_delete, FailoverLevel, HistoryKey, HistoryTaskFactory, QueueShardContext, QueueType,
    RangeCompleteHistoryTaskRequest, TaskFilter,
};
use super::task_allocator::{is_domain_not_registered, TaskAllocator};
use super::timer_gate::{EventTimerGate, RealTimerGate, TimerGate};
use super::timer_queue_processor_base::{timer_visibility_timestamp, TimerQueueProcessorBase};
use super::transfer_queue_processor::{ERR_PROCESSOR_SHUTDOWN, ERR_UNEXPECTED_QUEUE_TASK};
use super::util::{
    convert_from_persistence_timer_processing_queue_states, convert_to_persistence_timer_processing_queue_states,
    new_timer_task_key, validate_processing_queue_states, AckLevelValue, MAXIMUM_TIMER_TASK_KEY,
};

/// `persistence.TaskTypeWorkflowTimeout`
const TASK_TYPE_WORKFLOW_TIMEOUT: i32 = 3;
/// `persistence.TaskTypeDeleteHistoryEvent`
const TASK_TYPE_DELETE_HISTORY_EVENT: i32 = 4;

/// Wiring that Go builds inline in `NewTimerQueueProcessor` from executors etc.
pub struct TimerQueueDeps {
    pub shard: Arc<dyn QueueShardContext>,
    pub task_processor: Arc<dyn super::deps::TaskProcessor>,
    pub task_allocator: Arc<dyn TaskAllocator>,
    pub metrics: Arc<dyn MetricsScope>,
    /// builds `quotas.NewDynamicRateLimiter(options.MaxPollRPS)`
    pub rate_limiter_factory:
        Arc<dyn Fn(super::deps::IntFn) -> Arc<dyn super::deps::Limiter> + Send + Sync>,
    /// builds `redispatcher` (`task.NewRedispatcher`) per processor
    pub redispatcher_factory: Arc<dyn Fn(QueueType) -> Arc<dyn super::deps::Redispatcher> + Send + Sync>,
    /// `task.NewHistoryTask` wrapper bound to the active executor
    pub active_task_factory: HistoryTaskFactory,
    /// per remote cluster: `task.NewHistoryTask` wrapper bound to that cluster's standby executor
    pub standby_task_factories: HashMap<String, HistoryTaskFactory>,
}

pub struct TimerQueueProcessor {
    shard: Arc<dyn QueueShardContext>,
    metrics: Arc<dyn MetricsScope>,
    current_cluster_name: String,

    status: AtomicI32,
    shutdown_ch: CancellationToken,
    shutdown_wg: TaskTracker,

    /// Go `ackLevel time.Time` as unix nanos.
    ack_level: AtomicI64,
    task_allocator: Arc<dyn TaskAllocator>,
    deps: TimerQueueDeps,
    active_queue_processor: Arc<TimerQueueProcessorBase>,
    standby_queue_processors: HashMap<String, Arc<TimerQueueProcessorBase>>,
    standby_queue_timer_gates: HashMap<String, Arc<EventTimerGate>>,
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
            metrics: deps.metrics.clone(),
            current_cluster_name,
            status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
            shutdown_ch: CancellationToken::new(),
            shutdown_wg: TaskTracker::new(),
            ack_level: AtomicI64::new(shard.get_queue_ack_level(HistoryTaskCategory::Timer).scheduled_time.unix_nano()),
            task_allocator: deps.task_allocator.clone(),
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

    /// Go `Stop`.
    async fn stop_inner(&self) {
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

        // close the shutdown channel first so processor pumps drains tasks
        // and then stop the processors
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

    /// Go `drain`.
    async fn drain(&self) {
        if !self.graceful() {
            if let Err(e) = self.complete_timer(CancellationToken::new()).await {
                tracing::error!(error = %e, "Failed to complete timer task during drain");
            }
            return;
        }

        // when graceful shutdown is enabled for queue processor, use a context with timeout
        match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, self.complete_timer(CancellationToken::new())).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "Failed to complete timer task during drain"),
            Err(_) => tracing::error!("Failed to complete timer task during drain: deadline exceeded"),
        }
    }

    /// Go `completeTimerLoop`.
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
                                        tokio::spawn(async move { t.stop_inner().await });
                                        tracing::info!("Timer queue processor completeTimerLoop completed");
                                        return;
                                    }
                                    self.stop_inner().await;
                                    tracing::info!("Timer queue processor completeTimerLoop completed");
                                    return;
                                }

                                // TODO: the first retry has 0 backoff, revisit it to see if it's expected
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

    /// Go `completeTimer`.
    async fn complete_timer(&self, ctx: CancellationToken) -> Result<(), QueueError> {
        let mut new_ack_level = MAXIMUM_TIMER_TASK_KEY;
        let result = self
            .handle_action_impl(ctx.clone(), &self.current_cluster_name, Action::new_get_state_action())
            .await?;
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

        let new_ack_level_timestamp = timer_visibility_timestamp(new_ack_level);
        let ack_level = Time(self.ack_level.load(Ordering::SeqCst));
        if !ack_level.before(new_ack_level_timestamp) {
            tracing::debug!(
                "Skipping timer task completion because new ack level {:?} is not before ack level {:?}",
                new_ack_level_timestamp,
                ack_level
            );
            return Ok(());
        }

        tracing::debug!("Start completing timer task from: {:?}, to {:?}", ack_level, new_ack_level_timestamp);
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

    /// Go `HandleAction`.
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

        // Failover queue is used to scan all inflight tasks, if queue processor is not
        // started, there's no inflight task and we don't need to create a failover processor.
        // Also the HandleAction will be blocked if queue processor processing loop is not running.
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

        let mut max_read_level = Time::ZERO;
        let action_result = match self
            .handle_action_impl(CancellationToken::new(), &self.current_cluster_name, Action::new_get_state_action())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(domain_ids = ?domain_ids, error = %e, "Timer failover failed while getting queue states");
                if e.to_string() == ERR_PROCESSOR_SHUTDOWN {
                    // processor/shard already shutdown, we don't need to create failover queue processor
                    return;
                }
                // other errors should never be returned for GetStateAction
                panic!("unknown error for GetStateAction: {e}");
            }
        };
        for s in &action_result.get_state_action_result.as_ref().expect("get state result").states {
            let queue_read_level = timer_visibility_timestamp(s.read_level());
            if max_read_level.before(queue_read_level) {
                max_read_level = queue_read_level;
            }
        }

        // TODO (Go): the `maxReadLevel.Add(1 * time.Millisecond)` call has no effect, understand the
        // underlying intent and fix it.

        tracing::info!(
            domain_ids = ?domain_ids,
            min_level = min_level.unix_nano(),
            max_level = max_read_level.unix_nano(),
            "Timer Failover Triggered"
        );

        let (update_cluster_ack_level, failover_queue_processor) = new_timer_queue_failover_processor(
            &self.deps,
            &standby_cluster_name,
            min_level,
            max_read_level,
            domain_ids,
        );

        // NOTE: READ REF BEFORE MODIFICATION
        // ref: historyEngine.go registerDomainFailoverCallback function
        if let Err(e) = update_cluster_ack_level(new_timer_task_key(min_level, 0)).await {
            tracing::error!(error = %e, "Error update shard ack level");
        }

        // Failover queue processors are started on the fly when domains are failed over.
        // Failover queue processors will be stopped when the timer queue instance is stopped (due to restart or shard movement).
        // This means the failover queue processor might not finish its job.
        // There is no mechanism to re-start ongoing failover queue processors in the new shard owner.
        self.failover_queue_processors.lock().unwrap().push(failover_queue_processor.clone());
        TimerQueueProcessorBase::start(&failover_queue_processor);
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
        TimerQueueProcessorBase::start(&self.active_queue_processor);
        for p in self.standby_queue_processors.values() {
            TimerQueueProcessorBase::start(p);
        }

        let t = self.clone();
        self.shutdown_wg.spawn(async move { t.complete_timer_loop().await });
        tracing::info!("Timer queue processor started");
    }

    async fn stop(&self) {
        self.stop_inner().await
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
        tracing::debug!(cluster = cluster_name, "Current time for standby queue timergate is updated");
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

fn unexpected_task_err() -> QueueError {
    QueueError::Other(ERR_UNEXPECTED_QUEUE_TASK.to_string())
}

fn load_task_initializer(queue_type: QueueType, factory: HistoryTaskFactory, filter: TaskFilter) -> TaskInitializer {
    Arc::new(move |info: Arc<dyn PersistenceTask>| factory(info, queue_type, filter.clone()))
}

/// Shared prefix of the three timer task filters: category check and the "domain not registered" skip.
/// Returns `Some(result)` when the filter is already decided.
fn pre_filter(
    shard: &Arc<dyn QueueShardContext>,
    task: &Arc<dyn PersistenceTask>,
    queue_name: &str,
) -> Option<Result<bool, QueueError>> {
    if task.get_task_category() != HistoryTaskCategory::Timer {
        return Some(Err(unexpected_task_err()));
    }
    if let Ok(true) = is_domain_not_registered(shard.domain_cache().as_ref(), &task.get_domain_id()) {
        // Allow deletion tasks for deprecated domains
        if task.get_task_type() == TASK_TYPE_DELETE_HISTORY_EVENT {
            return Some(Ok(true));
        }

        tracing::info!(
            domain_id = %task.get_domain_id(),
            task = ?task,
            "Domain is not in registered status, skip task in {} timer queue.",
            queue_name
        );
        return Some(Ok(false));
    }
    None
}

/// Go `newTimerQueueActiveProcessor`.
fn new_timer_queue_active_processor(deps: &TimerQueueDeps) -> Arc<TimerQueueProcessorBase> {
    let shard = deps.shard.clone();
    let cluster_name = shard.current_cluster_name();
    let cfg = shard.queue_config();
    let options = Arc::new(new_timer_queue_processor_options(&cfg, true, false));

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        Arc::new(move |timer: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            Box::pin(async move {
                if let Some(r) = pre_filter(&shard, &timer, "active") {
                    return r;
                }
                allocator.verify_active_task(&timer.get_domain_id(), &timer.get_workflow_id(), &timer.get_run_id()).await
            })
        })
    };

    let update_max_read_level: UpdateMaxReadLevelFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move || {
            new_timer_task_key(
                shard.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Timer, &cluster).scheduled_time,
                0,
            )
        })
    };

    let states = load_timer_processing_queue_states(&cluster_name, shard.as_ref(), &options);
    let now_shard = shard.clone();
    let timer_gate: Arc<dyn TimerGate> = RealTimerGate::new(Arc::new(move || now_shard.now()));
    new_timer_base(
        deps,
        &cluster_name,
        &cluster_name,
        states,
        timer_gate,
        options,
        update_max_read_level,
        QueueType::ActiveTimer,
        deps.active_task_factory.clone(),
        task_filter,
    )
}

/// Go `newTimerQueueStandbyProcessor`.
fn new_timer_queue_standby_processor(
    cluster_name: &str,
    deps: &TimerQueueDeps,
    factory: HistoryTaskFactory,
) -> (Arc<TimerQueueProcessorBase>, Arc<EventTimerGate>) {
    let shard = deps.shard.clone();
    let cfg = shard.queue_config();
    let options = Arc::new(new_timer_queue_processor_options(&cfg, false, false));
    let cluster_name = cluster_name.to_string();

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        let cluster = cluster_name.clone();
        Arc::new(move |timer: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                if let Some(r) = pre_filter(&shard, &timer, "standby") {
                    return r;
                }
                if timer.get_task_type() == TASK_TYPE_WORKFLOW_TIMEOUT
                    || timer.get_task_type() == TASK_TYPE_DELETE_HISTORY_EVENT
                {
                    match shard.domain_cache().get_domain_by_id(&timer.get_domain_id()) {
                        Ok(entry) => {
                            if entry.has_replication_cluster(&cluster) {
                                // guarantee the processing of workflow execution history deletion
                                return Ok(true);
                            }
                        }
                        Err(QueueError::EntityNotExists(_)) => {
                            tracing::warn!(
                                domain_id = %timer.get_domain_id(),
                                task = ?timer,
                                "Cannot find domain, default to not process task."
                            );
                            return Ok(false);
                        }
                        Err(e) => {
                            // retry the task if failed to find the domain
                            tracing::warn!(domain_id = %timer.get_domain_id(), "Cannot find domain");
                            return Err(e);
                        }
                    }
                }
                allocator
                    .verify_standby_task(&cluster, &timer.get_domain_id(), &timer.get_workflow_id(), &timer.get_run_id())
                    .await
            })
        })
    };

    let update_max_read_level: UpdateMaxReadLevelFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move || {
            new_timer_task_key(
                shard.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Timer, &cluster).scheduled_time,
                0,
            )
        })
    };

    let states = load_timer_processing_queue_states(&cluster_name, shard.as_ref(), &options);
    let remote_timer_gate = EventTimerGate::new(shard.get_current_time(&cluster_name));
    let p = new_timer_base(
        deps,
        &cluster_name,
        &cluster_name,
        states,
        remote_timer_gate.clone(),
        options,
        update_max_read_level,
        QueueType::StandbyTimer,
        factory,
        task_filter,
    );
    (p, remote_timer_gate)
}

type UpdateClusterAckLevelAsync = Arc<dyn Fn(TaskKey) -> super::deps::BoxFut<Result<(), QueueError>> + Send + Sync>;

/// Go `newTimerQueueFailoverProcessor`: returns (updateClusterAckLevelFn, processor).
fn new_timer_queue_failover_processor(
    deps: &TimerQueueDeps,
    standby_cluster_name: &str,
    min_level: Time,
    max_level: Time,
    domain_ids: HashSet<String>,
) -> (UpdateClusterAckLevelAsync, Arc<TimerQueueProcessorBase>) {
    let shard = deps.shard.clone();
    let cfg = shard.queue_config();
    let options = Arc::new(new_timer_queue_processor_options(&cfg, true, true));

    let current_cluster_name = shard.current_cluster_name();
    let failover_start_time = shard.now();
    let failover_uuid = format!("{:032x}", rand::random::<u128>());
    tracing::info!(
        cluster = %current_cluster_name,
        domain_ids = ?domain_ids,
        failover = %format!("from: {standby_cluster_name}"),
        "creating timer failover processor"
    );

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        let domain_ids = domain_ids.clone();
        Arc::new(move |timer: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            let domain_ids = domain_ids.clone();
            Box::pin(async move {
                if let Some(r) = pre_filter(&shard, &timer, "failover") {
                    return r;
                }
                allocator
                    .verify_failover_active_task(
                        &domain_ids,
                        &timer.get_domain_id(),
                        &timer.get_workflow_id(),
                        &timer.get_run_id(),
                    )
                    .await
            })
        })
    };

    let max_read_level_task_key = new_timer_task_key(max_level, 0);
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
                let level = FailoverLevel {
                    start_time: failover_start_time,
                    min_level: HistoryKey::new(min_level, 0),
                    current_level: HistoryKey::new(timer_visibility_timestamp(ack_level), 0),
                    max_level: HistoryKey::new(max_level, 0),
                    domain_ids,
                };
                shard.update_failover_level(HistoryTaskCategory::Timer, &failover_uuid, level).await
            })
        })
    };

    let queue_shutdown: QueueShutdownFn = {
        let shard = shard.clone();
        let failover_uuid = failover_uuid.clone();
        Arc::new(move || {
            let shard = shard.clone();
            let failover_uuid = failover_uuid.clone();
            Box::pin(async move { shard.delete_failover_level(HistoryTaskCategory::Timer, &failover_uuid).await })
        })
    };

    let processing_queue_states = vec![ProcessingQueueState::new(
        DEFAULT_PROCESSING_QUEUE_LEVEL,
        new_timer_task_key(min_level, 0),
        max_read_level_task_key,
        DomainFilter::new(domain_ids.clone(), false),
    )];

    let now_shard = shard.clone();
    let timer_gate: Arc<dyn TimerGate> = RealTimerGate::new(Arc::new(move || now_shard.now()));
    let processor = TimerQueueProcessorBase::new(
        current_cluster_name.clone(), // should use current cluster's time when doing domain failover
        shard.clone(),
        processing_queue_states,
        deps.task_processor.clone(),
        (deps.redispatcher_factory)(QueueType::ActiveTimer),
        timer_gate,
        options.clone(),
        update_max_read_level,
        update_cluster_ack_level.clone(),
        None,
        queue_shutdown,
        load_task_initializer(QueueType::ActiveTimer, deps.active_task_factory.clone(), task_filter),
        deps.metrics.clone(),
        (deps.rate_limiter_factory)(options.max_poll_rps.clone()),
    );
    (update_cluster_ack_level, processor)
}

/// Builds an active/standby timer processor base; the `update*` closures are identical for both
/// (Go duplicates them in `newTimerQueueActiveProcessor` / `newTimerQueueStandbyProcessor`).
#[allow(clippy::too_many_arguments)]
fn new_timer_base(
    deps: &TimerQueueDeps,
    base_cluster_name: &str,
    ack_cluster_name: &str,
    states: Vec<ProcessingQueueState>,
    timer_gate: Arc<dyn TimerGate>,
    options: Arc<super::processor_options::QueueProcessorOptions>,
    update_max_read_level: UpdateMaxReadLevelFn,
    queue_type: QueueType,
    factory: HistoryTaskFactory,
    task_filter: TaskFilter,
) -> Arc<TimerQueueProcessorBase> {
    let shard = deps.shard.clone();

    let update_cluster_ack_level: UpdateClusterAckLevelFn = {
        let shard = shard.clone();
        let cluster = ack_cluster_name.to_string();
        Arc::new(move |ack_level: TaskKey| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                shard
                    .update_queue_cluster_ack_level(
                        HistoryTaskCategory::Timer,
                        &cluster,
                        HistoryKey::new(timer_visibility_timestamp(ack_level), 0),
                    )
                    .await
            })
        })
    };

    let update_processing_queue_states: UpdateProcessingQueueStatesFn = {
        let shard = shard.clone();
        let cluster = ack_cluster_name.to_string();
        Arc::new(move |states: Vec<ProcessingQueueState>| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                let p_states = convert_to_persistence_timer_processing_queue_states(&states);
                shard.update_timer_processing_queue_states(&cluster, p_states).await
            })
        })
    };

    let queue_shutdown: QueueShutdownFn = Arc::new(|| Box::pin(async { Ok(()) }));

    TimerQueueProcessorBase::new(
        base_cluster_name.to_string(),
        shard.clone(),
        states,
        deps.task_processor.clone(),
        (deps.redispatcher_factory)(queue_type),
        timer_gate,
        options.clone(),
        update_max_read_level,
        update_cluster_ack_level,
        Some(update_processing_queue_states),
        queue_shutdown,
        load_task_initializer(queue_type, factory, task_filter),
        deps.metrics.clone(),
        (deps.rate_limiter_factory)(options.max_poll_rps.clone()),
    )
}

/// Go `loadTimerProcessingQueueStates`.
fn load_timer_processing_queue_states(
    cluster_name: &str,
    shard: &dyn QueueShardContext,
    options: &super::processor_options::QueueProcessorOptions,
) -> Vec<ProcessingQueueState> {
    let ack_level = shard.get_queue_cluster_ack_level(HistoryTaskCategory::Timer, cluster_name).scheduled_time;
    if (options.enable_load_queue_states)() {
        let p_states = shard.get_timer_processing_queue_states(cluster_name);
        if validate_processing_queue_states(&p_states, AckLevelValue::Time(ack_level)) {
            return convert_from_persistence_timer_processing_queue_states(&p_states);
        }

        tracing::error!(states = ?p_states, shard_timer_acks = ack_level.unix_nano(), "Incompatible processing queue states and ackLevel");
    }

    vec![ProcessingQueueState::new(
        DEFAULT_PROCESSING_QUEUE_LEVEL,
        new_timer_task_key(ack_level, 0),
        MAXIMUM_TIMER_TASK_KEY,
        DomainFilter::new(HashSet::new(), true),
    )]
}
