//! Port of `go/service/history/queue/processor_base.go`.
//!
//! Differences forced by Rust (no logic change):
//! * the redispatcher is passed in instead of built via `task.NewRedispatcher` (task/ is ported elsewhere);
//! * `processingQueueCollections` lives behind an async mutex and methods that touch it take the
//!   locked `&mut Vec<ProcessingQueueCollection>`;
//! * `ctx` of actions is a `CancellationToken`, result channel is a oneshot.
use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::action::{Action, ActionResult, ActionType, GetStateActionResult, ResetActionResult};
use super::deps::{
    BoxFut, Limiter, MetricScopeIdx, MetricsScope, QueueError, Redispatcher, ShardContext, TaskKey, TaskProcessor,
    TaskRef,
};
use super::domain_filter::DomainFilter;
use super::processing_queue::{min_task_key, ProcessingQueue};
use super::processing_queue_collection::ProcessingQueueCollection;
use super::processing_queue_state::ProcessingQueueState;
use super::processor_options::QueueProcessorOptions;
use super::split_policy::{
    AggregatedSplitPolicy, LookAheadFunc, PendingTaskSplitPolicy, ProcessingQueueSplitPolicy, RandomSplitPolicy,
    StuckTaskSplitPolicy,
};
use super::constants::DEFAULT_PROCESSING_QUEUE_LEVEL;
use super::util::{MAXIMUM_TIMER_TASK_KEY, MAXIMUM_TRANSFER_TASK_KEY};

pub const WARN_PENDING_TASKS: i64 = 2000;

pub const DAEMON_STATUS_INITIALIZED: i32 = 0;
pub const DAEMON_STATUS_STARTED: i32 = 1;
pub const DAEMON_STATUS_STOPPED: i32 = 2;

pub type UpdateMaxReadLevelFn = Arc<dyn Fn() -> TaskKey + Send + Sync>;
pub type UpdateClusterAckLevelFn = Arc<dyn Fn(TaskKey) -> BoxFut<Result<(), QueueError>> + Send + Sync>;
pub type UpdateProcessingQueueStatesFn =
    Arc<dyn Fn(Vec<ProcessingQueueState>) -> BoxFut<Result<(), QueueError>> + Send + Sync>;
pub type QueueShutdownFn = Arc<dyn Fn() -> BoxFut<Result<(), QueueError>> + Send + Sync>;

pub struct ActionNotification {
    pub ctx: CancellationToken,
    pub action: Action,
    pub result_tx: oneshot::Sender<Result<Option<ActionResult>, QueueError>>,
}

pub struct ProcessorBase {
    pub shard: Arc<dyn ShardContext>,
    pub task_processor: Arc<dyn TaskProcessor>,
    pub redispatcher: Arc<dyn Redispatcher>,

    pub options: Arc<QueueProcessorOptions>,
    pub update_max_read_level: UpdateMaxReadLevelFn,
    pub update_cluster_ack_level: UpdateClusterAckLevelFn,
    pub update_processing_queue_states: Option<UpdateProcessingQueueStatesFn>,
    pub queue_shutdown: QueueShutdownFn,

    pub metrics_scope: Arc<dyn MetricsScope>,
    pub rate_limiter: Arc<dyn Limiter>,

    pub status: AtomicI32,
    pub shutdown_wg: TaskTracker,
    pub shutdown_ch: CancellationToken,
    pub action_notify_tx: mpsc::Sender<ActionNotification>,
    pub action_notify_rx: Mutex<mpsc::Receiver<ActionNotification>>,

    pub processing_queue_collections: Mutex<Vec<ProcessingQueueCollection>>,
}

impl ProcessorBase {
    /// Go `newProcessorBase`. `rate_limiter` is `quotas.NewDynamicRateLimiter(options.MaxPollRPS)`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard: Arc<dyn ShardContext>,
        processing_queue_states: Vec<ProcessingQueueState>,
        task_processor: Arc<dyn TaskProcessor>,
        redispatcher: Arc<dyn Redispatcher>,
        options: Arc<QueueProcessorOptions>,
        update_max_read_level: UpdateMaxReadLevelFn,
        update_cluster_ack_level: UpdateClusterAckLevelFn,
        update_processing_queue_states: Option<UpdateProcessingQueueStatesFn>,
        queue_shutdown: QueueShutdownFn,
        metrics_scope: Arc<dyn MetricsScope>,
        rate_limiter: Arc<dyn Limiter>,
    ) -> ProcessorBase {
        let (tx, rx) = mpsc::channel(1);
        ProcessorBase {
            shard,
            task_processor,
            redispatcher,
            options,
            update_max_read_level,
            update_cluster_ack_level,
            update_processing_queue_states,
            queue_shutdown,
            metrics_scope,
            rate_limiter,
            status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
            shutdown_wg: TaskTracker::new(),
            shutdown_ch: CancellationToken::new(),
            action_notify_tx: tx,
            action_notify_rx: Mutex::new(rx),
            processing_queue_collections: Mutex::new(ProcessingQueueCollection::new_collections(
                processing_queue_states,
            )),
        }
    }

    /// Go `updateAckLevel`: (processFinished, minAckLevel, err).
    pub async fn update_ack_level(
        &self,
        colls: &mut [ProcessingQueueCollection],
    ) -> (bool, Option<TaskKey>, Option<QueueError>) {
        self.metrics_scope.inc_counter("AckLevelUpdateCounter");
        let mut min_ack_level: Option<TaskKey> = None;
        let mut total_pending: i64 = 0;
        for c in colls.iter_mut() {
            let (ack_level, num_pending) = c.update_ack_levels();
            let Some(ack_level) = ack_level else { continue };
            total_pending += num_pending as i64;
            min_ack_level = Some(match min_ack_level {
                None => ack_level,
                Some(m) => min_task_key(m, ack_level),
            });
        }

        let Some(min_ack_level) = min_ack_level else {
            // note that the shutdown fn may be invoked more than once
            return match (self.queue_shutdown)().await {
                Err(e) => {
                    tracing::error!(error = %e, "Error shutdown queue");
                    (false, None, Some(e))
                }
                Ok(()) => (true, None, None),
            };
        };

        if total_pending > WARN_PENDING_TASKS {
            tracing::warn!(number = total_pending, "Too many pending tasks.");
        }
        let (timer_name, hist_name) = pending_tasks_metric_names(self.options.metric_scope);
        self.metrics_scope.record_timer(timer_name, Duration::from_nanos(total_pending as u64));
        self.metrics_scope.int_exponential_histogram(hist_name, total_pending);

        if let (true, Some(update_states)) =
            ((self.options.enable_persist_queue_states)(), self.update_processing_queue_states.as_ref())
        {
            let states = Self::get_processing_queue_states(colls)
                .get_state_action_result
                .expect("get state result")
                .states;
            if let Err(e) = update_states(states).await {
                tracing::error!(error = %e, "Error persisting processing queue states");
                self.metrics_scope.inc_counter("AckLevelUpdateFailedCounter");
                return (false, Some(min_ack_level), Some(e));
            }
        } else if let Err(e) = (self.update_cluster_ack_level)(min_ack_level).await {
            tracing::error!(error = %e, "Error updating ack level for shard");
            self.metrics_scope.inc_counter("AckLevelUpdateFailedCounter");
            return (false, Some(min_ack_level), Some(e));
        }

        (false, Some(min_ack_level), None)
    }

    /// Go `initializeSplitPolicy`.
    pub fn initialize_split_policy(&self, look_ahead: Option<LookAheadFunc>) -> Option<Box<dyn ProcessingQueueSplitPolicy>> {
        if !(self.options.enable_split)() {
            return None;
        }

        let mut policies: Vec<Box<dyn ProcessingQueueSplitPolicy>> = Vec::new();
        let max_new_queue_level = (self.options.split_max_level)() as i32;

        match convert_dynamic_config_map_property_to_int_map(&(self.options.pending_task_split_threshold)()) {
            Err(e) => tracing::error!(error = %e, "Failed to convert pending task threshold"),
            Ok(th) => policies.push(Box::new(PendingTaskSplitPolicy::new(
                th,
                self.options.enable_pending_task_split_by_domain_id.clone(),
                look_ahead.clone(),
                max_new_queue_level,
                self.metrics_scope.clone(),
            ))),
        }

        match convert_dynamic_config_map_property_to_int_map(&(self.options.stuck_task_split_threshold)()) {
            Err(e) => tracing::error!(error = %e, "Failed to convert stuck task threshold"),
            Ok(th) => policies.push(Box::new(StuckTaskSplitPolicy::new(
                th,
                self.options.enable_stuck_task_split_by_domain_id.clone(),
                max_new_queue_level,
                self.metrics_scope.clone(),
            ))),
        }

        let p = (self.options.random_split_probability)();
        if p != 0.0 {
            policies.push(Box::new(RandomSplitPolicy::new(
                p,
                self.options.enable_random_split_by_domain_id.clone(),
                max_new_queue_level,
                look_ahead,
                self.metrics_scope.clone(),
            )));
        }

        if policies.is_empty() {
            return None;
        }
        Some(Box::new(AggregatedSplitPolicy::new(policies)))
    }

    /// Go `splitProcessingQueueCollection`.
    pub fn split_processing_queue_collection(
        &self,
        colls: &mut Vec<ProcessingQueueCollection>,
        split_policy: Option<&dyn ProcessingQueueSplitPolicy>,
        mut upsert_poll_time_fn: impl FnMut(i32, super::deps::Time),
    ) {
        let Some(split_policy) = split_policy else {
            self.emit_processing_queue_metrics(colls);
            return;
        };

        let mut new_queues_map: HashMap<i32, Vec<Vec<ProcessingQueue>>> = HashMap::new();
        for c in colls.iter_mut() {
            let mut current: HashMap<i32, Vec<ProcessingQueue>> = HashMap::new();
            for q in c.split(split_policy) {
                current.entry(q.state().level()).or_default().push(q);
            }
            for (level, queues) in current {
                new_queues_map.entry(level).or_default().push(queues);
            }
        }

        for c in colls.iter_mut() {
            if let Some(list) = new_queues_map.remove(&c.level()) {
                for queues in list {
                    c.merge(queues);
                }
            }
        }

        for (level, list) in new_queues_map {
            let mut nc = ProcessingQueueCollection::new(level, Vec::new());
            for queues in list {
                nc.merge(queues);
            }
            colls.push(nc);
        }

        for c in colls.iter() {
            upsert_poll_time_fn(c.level(), super::deps::Time::ZERO);
        }
        self.emit_processing_queue_metrics(colls);
    }

    /// Go `emitProcessingQueueMetrics`.
    pub fn emit_processing_queue_metrics(&self, colls: &[ProcessingQueueCollection]) {
        let mut num: i64 = 0;
        let mut max_level: i32 = 0;
        for c in colls {
            let size = c.queues().len();
            num += size as i64;
            if size != 0 && c.level() > max_level {
                max_level = c.level();
            }
        }
        self.metrics_scope.record_timer("ProcessingQueueNumTimer", Duration::from_nanos(num as u64));
        self.metrics_scope.int_exponential_histogram("ProcessingQueueNumHistogram", num);
        self.metrics_scope.record_timer("ProcessingQueueMaxLevelTimer", Duration::from_nanos(max_level as u64));
        self.metrics_scope.int_exponential_histogram("ProcessingQueueMaxLevelHistogram", max_level as i64);
    }

    /// Go `addAction`: None when the processor is shut down or `ctx` is cancelled.
    pub async fn add_action(
        &self,
        ctx: Option<CancellationToken>,
        action: Action,
    ) -> Option<oneshot::Receiver<Result<Option<ActionResult>, QueueError>>> {
        let (result_tx, result_rx) = oneshot::channel();
        let ctx = ctx.unwrap_or_default();
        let n = ActionNotification { ctx: ctx.clone(), action, result_tx };
        tokio::select! {
            r = self.action_notify_tx.send(n) => r.ok().map(|_| result_rx),
            _ = self.shutdown_ch.cancelled() => None,
            _ = ctx.cancelled() => None,
        }
    }

    /// Go `handleActionNotification`.
    pub fn handle_action_notification(
        &self,
        colls: &mut Vec<ProcessingQueueCollection>,
        notification: ActionNotification,
        post_action_fn: impl FnOnce(),
    ) {
        let (result, err) = match notification.action.action_type {
            ActionType::Reset => match self.reset_processing_queue_states(colls) {
                Ok(r) => (Some(r), None),
                Err(e) => (None, Some(e)),
            },
            ActionType::GetState => (Some(Self::get_processing_queue_states(colls)), None),
            other => (None, Some(QueueError::Other(format!("unknown queue action type: {:?}", other)))),
        };

        let failed = err.is_some();
        let _ = notification.result_tx.send(match err {
            Some(e) => Err(e),
            None => Ok(result),
        });
        if !failed {
            post_action_fn();
        }
    }

    /// Go `resetProcessingQueueStates`.
    pub fn reset_processing_queue_states(
        &self,
        colls: &mut Vec<ProcessingQueueCollection>,
    ) -> Result<ActionResult, QueueError> {
        let mut min_ack_level: Option<TaskKey> = None;
        for c in colls.iter_mut() {
            let (ack, _) = c.update_ack_levels();
            let Some(ack) = ack else { continue };
            min_ack_level = Some(match min_ack_level {
                None => ack,
                Some(m) => min_task_key(m, ack),
            });
        }
        let Some(min_ack_level) = min_ack_level else {
            tracing::error!("unable to find minAckLevel during reset");
            panic!("unable to find minAckLevel during reset");
        };

        let max_read_level = match self.options.metric_scope {
            MetricScopeIdx::TransferActiveQueueProcessorScope | MetricScopeIdx::TransferStandbyQueueProcessorScope => {
                MAXIMUM_TRANSFER_TASK_KEY
            }
            MetricScopeIdx::TimerActiveQueueProcessorScope | MetricScopeIdx::TimerStandbyQueueProcessorScope => {
                MAXIMUM_TIMER_TASK_KEY
            }
            // Go leaves the key nil here, which would panic on first use.
            other => panic!("unsupported metric scope for reset: {:?}", other),
        };

        *colls = ProcessingQueueCollection::new_collections(vec![ProcessingQueueState::new(
            DEFAULT_PROCESSING_QUEUE_LEVEL,
            min_ack_level,
            max_read_level,
            DomainFilter::new(Default::default(), true),
        )]);

        Ok(ActionResult {
            action_type: Some(ActionType::Reset),
            reset_action_result: Some(ResetActionResult),
            ..Default::default()
        })
    }

    /// Go `getProcessingQueueStates`.
    pub fn get_processing_queue_states(colls: &[ProcessingQueueCollection]) -> ActionResult {
        let mut states = Vec::new();
        for c in colls {
            for q in c.queues() {
                states.push(q.state().clone());
            }
        }
        ActionResult {
            action_type: Some(ActionType::GetState),
            get_state_action_result: Some(GetStateActionResult { states }),
            ..Default::default()
        }
    }

    /// Go `submitTask`: (submitted, err). err is only set when the processor is shutting down.
    pub async fn submit_task(&self, task: TaskRef) -> Result<bool, QueueError> {
        let res = self.task_processor.try_submit(task.clone()).await;
        let (submitted, err) = match res {
            Ok(s) => (s, None),
            Err(e) => (false, Some(e)),
        };
        if let Some(e) = &err {
            if self.shutdown_ch.is_cancelled() {
                return Err(e.clone());
            }
            tracing::error!(error = %e, "Failed to submit task");
        }
        if err.is_some() || !submitted {
            self.redispatcher.add_task(task);
            return Ok(false);
        }
        Ok(true)
    }
}

/// Go `dynamicproperties.ConvertDynamicConfigMapPropertyToIntMap`: keys are level numbers,
/// values must be integers.
pub fn convert_dynamic_config_map_property_to_int_map(
    m: &HashMap<String, serde_json::Value>,
) -> Result<HashMap<i32, i64>, String> {
    let mut out = HashMap::new();
    for (k, v) in m {
        let level: i32 = k.parse().map_err(|_| format!("failed to convert key {} to int", k))?;
        let val = match v {
            serde_json::Value::Number(n) if n.is_i64() => n.as_i64().unwrap(),
            serde_json::Value::Number(n) if n.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false) => {
                n.as_f64().unwrap() as i64
            }
            _ => return Err(format!("failed to convert value {} to int", v)),
        };
        out.insert(level, val);
    }
    Ok(out)
}

/// Go `getPendingTasksMetricIdx` / `getPendingTasksHistogramIdx` (metric names).
pub fn pending_tasks_metric_names(scope: MetricScopeIdx) -> (&'static str, &'static str) {
    match scope {
        MetricScopeIdx::TimerActiveQueueProcessorScope => {
            ("ShardInfoTimerActivePendingTasksTimer", "ShardInfoTimerActivePendingTasksHistogram")
        }
        MetricScopeIdx::TimerStandbyQueueProcessorScope => {
            ("ShardInfoTimerStandbyPendingTasksTimer", "ShardInfoTimerStandbyPendingTasksHistogram")
        }
        MetricScopeIdx::TransferActiveQueueProcessorScope => {
            ("ShardInfoTransferActivePendingTasksTimer", "ShardInfoTransferActivePendingTasksHistogram")
        }
        MetricScopeIdx::TransferStandbyQueueProcessorScope => {
            ("ShardInfoTransferStandbyPendingTasksTimer", "ShardInfoTransferStandbyPendingTasksHistogram")
        }
        MetricScopeIdx::CrossClusterQueueProcessorScope => {
            ("ShardInfoCrossClusterPendingTasksTimer", "ShardInfoCrossClusterPendingTasksHistogram")
        }
        MetricScopeIdx::ReplicatorQueueProcessorScope => {
            ("ShardInfoReplicationPendingTasksTimer", "ShardInfoReplicationPendingTasksHistogram")
        }
    }
}
