//! Interfaces of not-yet-merged Rust modules that queuev2 depends on.
//!
//! Go sources replaced by these stubs:
//! - `common/persistence` (HistoryTaskKey, HistoryTaskCategory, Task, helpers)
//! - `common/clock` (TimeSource, Timer, TimerGate), `common/quotas` (Limiter)
//! - `common/log` (Logger), `common/metrics` (Scope, Client)
//! - `common/dynamicconfig/dynamicproperties` (property fns)
//! - `service/history/task` (Task, Processor, Rescheduler, Initializer, Executor)
//! - `service/history/shard` (Context), `service/history/queue` (Action, ActionResult)
//! - `service/history/common` (NotifyTaskInfo), `common/types` queue-state types.
//!
//! When the owning modules (task/, queue/, shard/, ...) land, these traits are
//! to be implemented by / replaced with those modules' types.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use tokio::sync::Notify;

pub type Time = DateTime<Utc>;

/// Go zero `time.Time{}` (0001-01-01 UTC).
pub fn zero_time() -> Time {
    NaiveDate::from_ymd_opt(1, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

pub fn unix_nano_time(nanos: i64) -> Time {
    DateTime::<Utc>::from_timestamp_nanos(nanos)
}

pub fn time_unix_nano(t: &Time) -> i64 {
    t.timestamp_nanos_opt().unwrap_or(i64::MAX)
}

// ---------------------------------------------------------------- persistence

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HistoryTaskKey {
    scheduled_time: Time,
    task_id: i64,
}

impl HistoryTaskKey {
    pub fn new(scheduled_time: Time, task_id: i64) -> Self {
        Self {
            scheduled_time,
            task_id,
        }
    }
    pub fn new_immediate(task_id: i64) -> Self {
        Self::new(unix_nano_time(0), task_id)
    }
    pub fn minimum() -> Self {
        Self::new(zero_time(), 0)
    }
    pub fn maximum() -> Self {
        Self::new(unix_nano_time(i64::MAX), i64::MAX)
    }
    pub fn get_task_id(&self) -> i64 {
        self.task_id
    }
    pub fn get_scheduled_time(&self) -> Time {
        self.scheduled_time
    }
    /// Go `Compare`: -1/0/1 by (scheduledTime, taskID).
    pub fn compare(&self, other: &Self) -> i32 {
        match self.cmp(other) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }
    pub fn next(&self) -> Self {
        if self.task_id == i64::MAX {
            Self::new(self.scheduled_time + chrono::Duration::nanoseconds(1), 0)
        } else {
            Self::new(self.scheduled_time, self.task_id + 1)
        }
    }
    pub fn is_zero(&self) -> bool {
        self.scheduled_time == zero_time() && self.task_id == 0
    }
    pub fn less(&self, o: &Self) -> bool {
        self.compare(o) < 0
    }
    pub fn greater(&self, o: &Self) -> bool {
        self.compare(o) > 0
    }
    pub fn greater_or_equal(&self, o: &Self) -> bool {
        self.compare(o) >= 0
    }
    pub fn equal(&self, o: &Self) -> bool {
        self.compare(o) == 0
    }
}

pub fn min_history_task_key(a: HistoryTaskKey, b: HistoryTaskKey) -> HistoryTaskKey {
    if a.compare(&b) < 0 {
        a
    } else {
        b
    }
}

pub fn max_history_task_key(a: HistoryTaskKey, b: HistoryTaskKey) -> HistoryTaskKey {
    if a.compare(&b) > 0 {
        a
    } else {
        b
    }
}

pub const HISTORY_TASK_CATEGORY_TYPE_IMMEDIATE: i32 = 1;
pub const HISTORY_TASK_CATEGORY_TYPE_SCHEDULED: i32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HistoryTaskCategory {
    pub category_type: i32,
    pub category_id: i32,
}

impl HistoryTaskCategory {
    pub const TRANSFER: Self = Self {
        category_type: HISTORY_TASK_CATEGORY_TYPE_IMMEDIATE,
        category_id: 1,
    };
    pub const TIMER: Self = Self {
        category_type: HISTORY_TASK_CATEGORY_TYPE_SCHEDULED,
        category_id: 2,
    };
    pub fn type_(&self) -> i32 {
        self.category_type
    }
}

/// `persistence.Task` (read-only view used by queuev2).
pub trait PersistenceTask: Send + Sync {
    fn get_task_key(&self) -> HistoryTaskKey;
    fn get_task_id(&self) -> i64;
    fn get_domain_id(&self) -> String;
    fn get_workflow_id(&self) -> String;
    fn get_run_id(&self) -> String;
    fn get_visibility_timestamp(&self) -> Time;
    /// Go `IsTaskCorrupted` needs the concrete type (FailoverMarkerTask only
    /// requires a domain ID); implementors return true for that variant.
    fn is_failover_marker(&self) -> bool {
        false
    }
}

pub type PTask = Arc<dyn PersistenceTask>;

/// `persistence.IsTaskCorrupted`.
pub fn is_task_corrupted(t: &dyn PersistenceTask) -> bool {
    if t.is_failover_marker() {
        t.get_domain_id().is_empty()
    } else {
        t.get_domain_id().is_empty() || t.get_workflow_id().is_empty() || t.get_run_id().is_empty()
    }
}

pub const UNKNOWN_NUM_ROWS_AFFECTED: i64 = -1;

/// `persistence.HasMoreRowsToDelete`.
pub fn has_more_rows_to_delete(rows_deleted: i64, batch_size: i64) -> bool {
    !(rows_deleted < batch_size
        || rows_deleted == UNKNOWN_NUM_ROWS_AFFECTED
        || rows_deleted > batch_size
        || batch_size <= 0)
}

#[derive(Clone, Debug)]
pub struct GetHistoryTasksRequest {
    pub task_category: HistoryTaskCategory,
    pub inclusive_min_task_key: HistoryTaskKey,
    pub exclusive_max_task_key: HistoryTaskKey,
    pub page_size: usize,
    pub next_page_token: Option<Vec<u8>>,
    pub shard_id: Option<i32>,
}

pub struct GetHistoryTasksResponse {
    pub tasks: Vec<PTask>,
    pub next_page_token: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct RangeCompleteHistoryTaskRequest {
    pub task_category: HistoryTaskCategory,
    pub inclusive_min_task_key: HistoryTaskKey,
    pub exclusive_max_task_key: HistoryTaskKey,
    pub page_size: i64,
    pub shard_id: Option<i32>,
}

pub struct RangeCompleteHistoryTaskResponse {
    pub tasks_completed: i64,
}

pub type DepError = String;

/// `persistence.ExecutionManager` subset.
#[async_trait]
pub trait ExecutionManager: Send + Sync {
    async fn get_history_tasks(
        &self,
        req: GetHistoryTasksRequest,
    ) -> Result<GetHistoryTasksResponse, DepError>;
    async fn range_complete_history_task(
        &self,
        req: RangeCompleteHistoryTaskRequest,
    ) -> Result<RangeCompleteHistoryTaskResponse, DepError>;
}

// ---------------------------------------------------------- types.QueueState

#[derive(Clone, Debug, PartialEq)]
pub struct TypesTaskKey {
    pub task_id: i64,
    pub scheduled_time_nano: i64,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TypesTaskRange {
    pub inclusive_min: TypesTaskKey,
    pub exclusive_max: TypesTaskKey,
}
#[derive(Clone, Debug, PartialEq)]
pub enum TypesPredicate {
    Universal,
    Empty,
    DomainId {
        domain_ids: Vec<String>,
        is_exclusive: Option<bool>,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub struct TypesVirtualSliceState {
    pub task_range: TypesTaskRange,
    pub predicate: Option<TypesPredicate>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TypesVirtualQueueState {
    pub virtual_slice_states: Vec<TypesVirtualSliceState>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TypesQueueState {
    pub virtual_queue_states: BTreeMap<i64, TypesVirtualQueueState>,
    pub exclusive_max_read_level: TypesTaskKey,
}

// ---------------------------------------------------------------------- clock

pub trait TimeSource: Send + Sync {
    fn now(&self) -> Time;
    fn new_timer(&self, d: Duration) -> Arc<dyn Timer>;
    fn after_func(&self, d: Duration, f: Box<dyn FnOnce() + Send>) -> Arc<dyn Timer>;
}

/// `clock.Timer`; `fired` is Go's `<-timer.Chan()`.
#[async_trait]
pub trait Timer: Send + Sync {
    fn stop(&self) -> bool;
    fn reset(&self, d: Duration) -> bool;
    async fn fired(&self);
}

/// `clock.TimerGate`.
#[async_trait]
pub trait TimerGate: Send + Sync {
    fn update(&self, t: Time) -> bool;
    fn stop(&self);
    async fn fired(&self);
}

/// Cancellation signal (Go `context.Context` Done/Err subset).
#[derive(Clone, Default)]
pub struct CancelToken {
    inner: Arc<(AtomicBool, Notify)>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.inner.0.store(true, Ordering::SeqCst);
        self.inner.1.notify_waiters();
    }
    pub fn is_cancelled(&self) -> bool {
        self.inner.0.load(Ordering::SeqCst)
    }
    pub async fn cancelled(&self) {
        loop {
            let n = self.inner.1.notified();
            tokio::pin!(n);
            n.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            n.await;
        }
    }
}

/// `quotas.Limiter`.
#[async_trait]
pub trait Limiter: Send + Sync {
    async fn wait(&self, ctx: &CancelToken) -> Result<(), DepError>;
}

/// `backoff.JitDuration`: random in [(1-c)*d, (1+c)*d).
pub fn jit_duration(d: Duration, coefficient: f64) -> Duration {
    let nanos = d.as_nanos() as f64;
    if coefficient == 0.0 || nanos == 0.0 {
        return d;
    }
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|x| x.subsec_nanos())
        .unwrap_or(0);
    let r = (seed % 1_000_000) as f64 / 1_000_000.0;
    let v = nanos * (1.0 - coefficient) + r * nanos * 2.0 * coefficient;
    Duration::from_nanos(v.max(0.0) as u64)
}

// ------------------------------------------------------------ log and metrics

pub trait Logger: Send + Sync {
    fn debug_on(&self) -> bool;
    fn debug(&self, msg: &str);
    fn info(&self, msg: &str);
    fn warn(&self, msg: &str);
    fn error(&self, msg: &str);
    /// Go `logger.Fatal`.
    fn fatal(&self, msg: &str);
    fn with_tags(&self, tags: &[(&str, String)]) -> Arc<dyn Logger>;
}

pub trait Stopwatch: Send {
    fn stop(&self);
}

pub trait MetricsScope: Send + Sync {
    fn inc_counter(&self, name: &str);
    fn add_counter(&self, name: &str, v: i64);
    fn update_gauge(&self, name: &str, v: f64);
    fn record_histogram_duration(&self, name: &str, d: Duration);
    fn record_histogram_value(&self, name: &str, v: f64);
    fn exponential_histogram(&self, name: &str, d: chrono::Duration);
    fn int_exponential_histogram(&self, name: &str, v: i64);
    fn start_timer_with_exponential_histogram(&self, timer: &str, hist: &str) -> Box<dyn Stopwatch>;
    fn tagged(&self, tags: &[(&str, String)]) -> Arc<dyn MetricsScope>;
}

pub trait MetricsClient: Send + Sync {
    fn scope(&self, scope: &str) -> Arc<dyn MetricsScope>;
    fn record_timer(&self, scope: &str, name: &str, d: Duration);
}

pub mod metric {
    pub const PENDING_TASK_GAUGE: &str = "PendingTaskGauge";
    pub const VIRTUAL_QUEUE_PAUSED_GAUGE: &str = "VirtualQueuePausedGauge";
    pub const VIRTUAL_QUEUE_RUNNING_GAUGE: &str = "VirtualQueueRunningGauge";
    pub const VIRTUAL_QUEUE_COUNT_GAUGE: &str = "VirtualQueueCountGauge";
    pub const CORRUPTED_HISTORY_TASK_COUNTER: &str = "CorruptedHistoryTaskCounter";
    pub const TASK_ENQUEUE_TO_FETCH_LATENCY: &str = "TaskEnqueueToFetchLatency";
    pub const PROCESSING_QUEUE_THROTTLED_COUNTER: &str = "ProcessingQueueThrottledCounter";
    pub const TASK_SCHEDULE_THROTTLED_PER_TASK_LIST: &str = "TaskScheduleThrottledPerTaskList";
    pub const TASK_SCHEDULE_SUBMITTED_PER_TASK_LIST: &str = "TaskScheduleSubmittedPerTaskList";
    pub const ACK_LEVEL_UPDATE_COUNTER: &str = "AckLevelUpdateCounter";
    pub const ACK_LEVEL_UPDATE_FAILED_COUNTER: &str = "AckLevelUpdateFailedCounter";
    pub const TASK_BATCH_COMPLETE_COUNTER: &str = "TaskBatchCompleteCounter";
    pub const NEW_HISTORY_TASK_COUNTER: &str = "NewHistoryTaskCounter";
    pub const SHARD_INFO_SCOPE: &str = "ShardInfo";
    pub const SHARD_INFO_TRANSFER_ACTIVE_PENDING_TASKS_TIMER: &str =
        "ShardInfoTransferActivePendingTasksTimer";
    pub const SHARD_INFO_TRANSFER_ACTIVE_PENDING_TASKS_HISTOGRAM: &str =
        "ShardInfoTransferActivePendingTasksHistogram";
    pub const SHARD_INFO_TIMER_ACTIVE_PENDING_TASKS_TIMER: &str =
        "ShardInfoTimerActivePendingTasksTimer";
    pub const SHARD_INFO_TIMER_ACTIVE_PENDING_TASKS_HISTOGRAM: &str =
        "ShardInfoTimerActivePendingTasksHistogram";
    pub const CACHED_QUEUE_SIZE_HISTOGRAM: &str = "CachedQueueSizeHistogram";
    pub const CACHED_QUEUE_INJECT_ATTEMPT_COUNTER: &str = "CachedQueueInjectAttemptCounter";
    pub const CACHED_QUEUE_MISSES_COUNTER: &str = "CachedQueueMissesCounter";
    pub const CACHED_QUEUE_HITS_COUNTER: &str = "CachedQueueHitsCounter";
    pub const CACHED_QUEUE_SHADOW_MISMATCH_COUNTER: &str = "CachedQueueShadowMismatchCounter";
    pub const CACHED_QUEUE_PREFETCH_LATENCY: &str = "CachedQueuePrefetchLatency";
    pub const CACHED_QUEUE_PREFETCH_LATENCY_HISTOGRAM: &str = "CachedQueuePrefetchLatencyHistogram";
    pub const CACHED_QUEUE_PREFETCH_FAILURE_COUNTER: &str = "CachedQueuePrefetchFailureCounter";
    pub const CACHED_QUEUE_PREFETCH_GAP_DETECTED_COUNTER: &str =
        "CachedQueuePrefetchGapDetectedCounter";
    pub const CACHED_QUEUE_PREFETCH_SUCCESS_COUNTER: &str = "CachedQueuePrefetchSuccessCounter";
    pub const CACHED_QUEUE_DROPPED_FUTURE_TIMER_TASKS_DURATION_HISTOGRAM: &str =
        "CachedQueueDroppedFutureTimerTasksDurationHistogram";
    pub const TRANSFER_QUEUE_PROCESSOR_V2_SCOPE: &str = "TransferQueueProcessorV2";
    pub const TIMER_QUEUE_PROCESSOR_V2_SCOPE: &str = "TimerQueueProcessorV2";
}

// ---------------------------------------------------------- dynamic properties

pub type IntFn = Arc<dyn Fn() -> i64 + Send + Sync>;
pub type DurationFn = Arc<dyn Fn() -> Duration + Send + Sync>;
pub type FloatFn = Arc<dyn Fn() -> f64 + Send + Sync>;
pub type BoolFn = Arc<dyn Fn() -> bool + Send + Sync>;
/// `dynamicproperties.*PropertyFnWithShardIDFilter`
pub type StringShardFn = Arc<dyn Fn(i32) -> String + Send + Sync>;
pub type BoolShardFn = Arc<dyn Fn(i32) -> bool + Send + Sync>;

// ------------------------------------------------------------------- task pkg

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Acked,
    Nacked,
}

/// `service/history/task.Task` (extends persistence.Task).
pub trait Task: PersistenceTask {
    fn state(&self) -> TaskState;
    fn ack(&self);
    fn cancel(&self);
    fn set_initial_submit_time(&self, t: Time);
    fn get_original_task_list(&self) -> String;
    fn get_original_task_list_kind(&self) -> i32;
}

pub type HTask = Arc<dyn Task>;
/// `task.Initializer`
pub type TaskInitializer = Arc<dyn Fn(PTask) -> HTask + Send + Sync>;

pub trait Processor: Send + Sync {
    fn try_submit(&self, t: HTask) -> Result<bool, DepError>;
}

pub trait Rescheduler: Send + Sync {
    fn start(&self);
    fn stop(&self);
    fn reschedule_task(&self, t: HTask, at: Time);
    fn reschedule_domains(&self, domain_ids: &std::collections::HashSet<String>);
}

/// `task.Executor`
pub trait TaskExecutor: Send + Sync {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueType {
    Transfer,
    Timer,
}

/// `queue.Action` / `queue.ActionResult` placeholders.
pub struct Action;
pub struct ActionResult;

/// `hcommon.NotifyTaskInfo`
pub struct NotifyTaskInfo {
    pub tasks: Vec<PTask>,
    pub persistence_error: bool,
}

/// Dynamic config used by queuev2 (`config.Config` subset).
#[derive(Clone)]
pub struct QueueConfig {
    pub range_size_bits: u32,
    pub task_critical_retry_count: IntFn,
    pub transfer_task_batch_size: IntFn,
    pub transfer_task_delete_batch_size: IntFn,
    pub transfer_processor_max_poll_rps: IntFn,
    pub transfer_processor_max_poll_interval: DurationFn,
    pub transfer_processor_max_poll_interval_jitter_coefficient: FloatFn,
    pub transfer_processor_update_ack_interval: DurationFn,
    pub transfer_processor_update_ack_interval_jitter_coefficient: FloatFn,
    pub transfer_processor_enable_validator: BoolFn,
    pub transfer_processor_validation_interval: DurationFn,
    pub transfer_processor_cached_queue_reader_mode: StringShardFn,
    pub transfer_processor_cache_max_size: IntFn,
    pub transfer_processor_cached_queue_reader_shadow_sample_interval: DurationFn,
    pub timer_task_batch_size: IntFn,
    pub timer_task_delete_batch_size: IntFn,
    pub timer_processor_max_poll_rps: IntFn,
    pub timer_processor_max_poll_interval: DurationFn,
    pub timer_processor_max_poll_interval_jitter_coefficient: FloatFn,
    pub timer_processor_update_ack_interval: DurationFn,
    pub timer_processor_update_ack_interval_jitter_coefficient: FloatFn,
    pub timer_processor_cached_queue_reader_mode: StringShardFn,
    pub timer_processor_cache_max_size: IntFn,
    pub timer_processor_cached_queue_reader_shadow_sample_interval: DurationFn,
    pub timer_processor_cache_prefetch_trigger_window: DurationFn,
    pub timer_processor_cache_time_eviction_window: DurationFn,
    pub timer_processor_cache_min_prefetch_interval: DurationFn,
    pub queue_max_pending_task_count: IntFn,
    pub queue_processor_poll_backoff_interval: DurationFn,
    pub queue_processor_poll_backoff_interval_jitter_coefficient: FloatFn,
    pub virtual_slice_force_append_interval: DurationFn,
    pub active_task_redispatch_interval: DurationFn,
    pub queue_critical_pending_task_count: IntFn,
    pub queue_max_virtual_queue_count: IntFn,
    pub enable_transfer_queue_v2: BoolShardFn,
    pub enable_timer_queue_v2: BoolShardFn,
    pub enable_transfer_queue_v2_pending_task_count_alert: BoolShardFn,
    pub enable_timer_queue_v2_pending_task_count_alert: BoolShardFn,
}

/// `shard.Context` subset used by queuev2.
pub trait ShardContext: Send + Sync {
    fn get_shard_id(&self) -> i32;
    fn get_range_id(&self) -> i64;
    fn get_time_source(&self) -> Arc<dyn TimeSource>;
    fn get_config(&self) -> Arc<QueueConfig>;
    fn get_logger(&self) -> Arc<dyn Logger>;
    fn get_metrics_client(&self) -> Arc<dyn MetricsClient>;
    fn get_execution_manager(&self) -> Arc<dyn ExecutionManager>;
    fn get_queue_state(&self, c: HistoryTaskCategory) -> Result<TypesQueueState, DepError>;
    fn update_queue_state(&self, c: HistoryTaskCategory, s: TypesQueueState) -> Result<(), DepError>;
    fn update_if_needed_and_get_queue_max_read_level(
        &self,
        c: HistoryTaskCategory,
        cluster: &str,
    ) -> HistoryTaskKey;
    fn get_current_cluster_name(&self) -> String;
}

/// Constructors that live in other (not yet ported here) packages: `task.NewRescheduler`,
/// `task.NewHistoryTaskV2`, `quotas.NewDynamicRateLimiter`, `clock.NewTimerGate`.
pub trait QueueEnv: Send + Sync {
    fn new_rescheduler(
        &self,
        shard: &Arc<dyn ShardContext>,
        processor: Arc<dyn Processor>,
        logger: Arc<dyn Logger>,
        scope: Arc<dyn MetricsScope>,
    ) -> Arc<dyn Rescheduler>;
    #[allow(clippy::too_many_arguments)]
    fn new_history_task(
        &self,
        shard: &Arc<dyn ShardContext>,
        t: PTask,
        queue_type: QueueType,
        executor: Arc<dyn TaskExecutor>,
        processor: Arc<dyn Processor>,
        rescheduler: Arc<dyn Rescheduler>,
        logger: Arc<dyn Logger>,
    ) -> HTask;
    fn new_dynamic_rate_limiter(&self, rps: IntFn) -> Arc<dyn Limiter>;
    fn new_timer_gate(&self, ts: Arc<dyn TimeSource>) -> Arc<dyn TimerGate>;
}

/// `common.GetTaskListTag` (types.TaskListKind: Normal=0, Sticky=1, Ephemeral=2).
pub fn get_task_list_tag(name: &str, kind: i32) -> (&'static str, String) {
    let mut v = "unknown".to_string();
    if !name.is_empty() && kind == 0 {
        v = name.to_string();
    }
    if kind == 1 {
        v = "__sticky__".to_string();
    }
    if kind == 2 {
        v = "__ephemeral__".to_string();
    }
    ("tasklist", v)
}
