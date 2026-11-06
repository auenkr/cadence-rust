//! Minimal stand-ins for packages other workers are porting concurrently
//! (task/, shard, persistence tasks, metrics, dynamic config).
//! Each item keeps the Go name; `TODO(link)` marks the replacement point.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Go `time.Time` as unix nanoseconds; `Time(0)` is the zero value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub i64);

impl Time {
    pub const ZERO: Time = Time(0);
    pub fn unix_nano(&self) -> i64 {
        self.0
    }
    pub fn add(&self, d: Duration) -> Time {
        Time(self.0.saturating_add(d.as_nanos() as i64))
    }
    pub fn sub_nanos(&self, o: Time) -> i64 {
        self.0.saturating_sub(o.0)
    }
    pub fn before(&self, o: Time) -> bool {
        self.0 < o.0
    }
    pub fn after(&self, o: Time) -> bool {
        self.0 > o.0
    }
}

/// `task.Key` implementations: `transferTaskKey` and `timerTaskKey`
/// (go/service/history/queue/queue_processor_util.go). Go's type assertions
/// on `task.Key` become matches on this enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TaskKey {
    Transfer { task_id: i64 },
    Timer { visibility_timestamp: Time, task_id: i64 },
}

impl TaskKey {
    /// Go `Key.Less`. Mixing key kinds is a programming error (Go panics on the assertion).
    pub fn less(&self, other: &TaskKey) -> bool {
        match (self, other) {
            (TaskKey::Transfer { task_id: a }, TaskKey::Transfer { task_id: b }) => a < b,
            (
                TaskKey::Timer { visibility_timestamp: ta, task_id: ia },
                TaskKey::Timer { visibility_timestamp: tb, task_id: ib },
            ) => {
                if ta == tb {
                    ia < ib
                } else {
                    ta.before(*tb)
                }
            }
            _ => panic!("comparing task keys of different kinds: {:?} vs {:?}", self, other),
        }
    }
}

/// Go `ctask.State`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Acked,
    Nacked,
    Canceled,
}

/// Subset of go `task.Task` (+ `persistence.Task`) used by the queue package.
/// TODO(link): replace by task::Task
pub trait Task: Send + Sync {
    fn get_domain_id(&self) -> String;
    fn get_workflow_id(&self) -> String;
    fn get_run_id(&self) -> String;
    fn get_task_type(&self) -> i32;
    fn get_task_id(&self) -> i64;
    fn get_visibility_timestamp(&self) -> Time;
    fn get_version(&self) -> i64;
    fn get_task_key(&self) -> TaskKey;
    fn state(&self) -> TaskState;
    fn get_attempt(&self) -> i32;
    fn ack(&self);
    fn nack(&self);
    fn set_initial_submit_time(&self, t: Time);
    /// Lets adapters recover the concrete task (Go type assertion); `None` for plain test tasks.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }
}

pub type TaskRef = Arc<dyn Task>;

/// Go `dynamicproperties.*PropertyFn` families: closures evaluated on every call.
pub type IntFn = Arc<dyn Fn() -> i64 + Send + Sync>;
pub type FloatFn = Arc<dyn Fn() -> f64 + Send + Sync>;
pub type BoolFn = Arc<dyn Fn() -> bool + Send + Sync>;
pub type DurationFn = Arc<dyn Fn() -> Duration + Send + Sync>;
pub type MapFn = Arc<dyn Fn() -> HashMap<String, serde_json::Value> + Send + Sync>;
pub type BoolDomainIdFn = Arc<dyn Fn(&str) -> bool + Send + Sync>;
pub type DurationDomainIdFn = Arc<dyn Fn(&str) -> Duration + Send + Sync>;

/// Metric counter sink. TODO(link): replace by metrics::Scope
pub trait MetricsScope: Send + Sync {
    fn inc_counter(&self, name: &'static str);
    fn add_counter(&self, name: &'static str, delta: i64);
    fn record_timer(&self, name: &'static str, d: Duration);
    fn int_exponential_histogram(&self, _name: &'static str, _v: i64) {}
}

/// Scope that drops everything (default until the metrics port lands).
pub struct NoopScope;
impl MetricsScope for NoopScope {
    fn inc_counter(&self, _: &'static str) {}
    fn add_counter(&self, _: &'static str, _: i64) {}
    fn record_timer(&self, _: &'static str, _: Duration) {}
}

/// Go `common.Daemon`.
#[async_trait::async_trait]
pub trait Daemon: Send + Sync {
    fn start(self: Arc<Self>);
    async fn stop(&self);
}

/// Errors surfaced by the queue package.
/// `ShardClosed` is go `shard.ErrShardClosed`; `Transient` mirrors
/// `persistence.IsBackgroundTransientError` (InternalServiceError / TimeoutError).
#[derive(Debug, Clone)]
pub enum QueueError {
    ShardClosed(String),
    Transient(String),
    EntityNotExists(String),
    Other(String),
}
impl QueueError {
    pub fn is_shard_closed(&self) -> bool {
        matches!(self, QueueError::ShardClosed(_))
    }
    pub fn is_background_transient(&self) -> bool {
        matches!(self, QueueError::Transient(_))
    }
}
impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::ShardClosed(m)
            | QueueError::Transient(m)
            | QueueError::EntityNotExists(m)
            | QueueError::Other(m) => f.write_str(m),
        }
    }
}
impl std::error::Error for QueueError {}

/// TODO(link): replace by types::CrossClusterTaskRequest
#[derive(Clone, Debug, Default)]
pub struct CrossClusterTaskRequest;
/// TODO(link): replace by types::CrossClusterTaskResponse
#[derive(Clone, Debug, Default)]
pub struct CrossClusterTaskResponse;

/// Go `persistence.Task` (task info read from the history task tables).
/// TODO(link): replace by persistence::Task
pub trait PersistenceTask: Send + Sync + fmt::Debug {
    fn get_domain_id(&self) -> String;
    fn get_workflow_id(&self) -> String;
    fn get_run_id(&self) -> String;
    fn get_task_id(&self) -> i64;
    fn get_task_type(&self) -> i32;
    fn get_task_category(&self) -> HistoryTaskCategory;
    fn get_version(&self) -> i64;
    fn get_visibility_timestamp(&self) -> Time;
    /// Go `persistence.IsTaskCorrupted`.
    fn is_corrupted(&self) -> bool;
}

/// Go `GetHistoryTasksRequest` (only the fields the queue package sets).
#[derive(Clone, Debug)]
pub struct GetHistoryTasksRequest {
    pub task_category: HistoryTaskCategory,
    pub inclusive_min_task_key: HistoryTaskKey,
    pub exclusive_max_task_key: HistoryTaskKey,
    pub page_size: i64,
    pub next_page_token: Vec<u8>,
    pub shard_id: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryTaskCategory {
    Transfer,
    Timer,
}

/// Go `persistence.HistoryTaskKey{ScheduledTime, TaskID}`.
#[derive(Clone, Copy, Debug)]
pub struct HistoryTaskKey {
    pub scheduled_time: Time,
    pub task_id: i64,
}
impl HistoryTaskKey {
    /// Go `persistence.NewImmediateTaskKey`.
    pub fn immediate(task_id: i64) -> Self {
        HistoryTaskKey { scheduled_time: Time::ZERO, task_id }
    }
    /// Go `persistence.NewHistoryTaskKey`.
    pub fn new(scheduled_time: Time, task_id: i64) -> Self {
        HistoryTaskKey { scheduled_time, task_id }
    }
}

pub struct GetHistoryTasksResponse {
    pub tasks: Vec<Arc<dyn PersistenceTask>>,
    pub next_page_token: Vec<u8>,
}

/// Subset of go `shard.Context` + `shard.Context.GetExecutionManager()` used by the queue package.
/// TODO(link): replace by shard::Context
#[async_trait::async_trait]
pub trait ShardContext: Send + Sync {
    fn get_shard_id(&self) -> i32;
    fn now(&self) -> Time;
    /// `GetCurrentTime(cluster)`
    fn get_current_time(&self, cluster: &str) -> Time;
    /// `GetConfig().RangeSizeBits`
    fn range_size_bits(&self) -> u32;
    /// `GetConfig().EnableDebugMode`
    fn enable_debug_mode(&self) -> bool;
    /// `GetCurrentTime(cluster)`: wall clock for the current cluster, replicated remote time otherwise.
    fn get_current_time(&self, _cluster: &str) -> Time {
        self.now()
    }
    async fn get_history_tasks(&self, req: GetHistoryTasksRequest) -> Result<GetHistoryTasksResponse, QueueError>;
}

/// Go `task.Processor`. TODO(link): replace by task::Processor
#[async_trait::async_trait]
pub trait TaskProcessor: Send + Sync {
    async fn try_submit(&self, task: TaskRef) -> Result<bool, QueueError>;
}

/// Go `task.Redispatcher`. TODO(link): replace by task::Redispatcher
#[async_trait::async_trait]
pub trait Redispatcher: Send + Sync {
    fn start(&self);
    fn stop(&self);
    fn add_task(&self, task: TaskRef);
    async fn redispatch(&self, target_size: usize);
    fn redispatch_task(&self, task: TaskRef, t: Time);
    fn size(&self) -> usize;
}

/// Go `task.Initializer`.
pub type TaskInitializer = Arc<dyn Fn(Arc<dyn PersistenceTask>) -> TaskRef + Send + Sync>;

/// Go `quotas.Limiter` (only `Wait` is used by the queue package).
#[async_trait::async_trait]
pub trait Limiter: Send + Sync {
    async fn wait(&self, timeout: Duration) -> Result<(), QueueError>;
}

/// Go `metrics.ScopeIdx` values used by queue options.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricScopeIdx {
    TransferActiveQueueProcessorScope,
    TransferStandbyQueueProcessorScope,
    TimerActiveQueueProcessorScope,
    TimerStandbyQueueProcessorScope,
    CrossClusterQueueProcessorScope,
    ReplicatorQueueProcessorScope,
}

pub type BoxFut<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// Go `persistence.WorkflowExecutionInfo` (identity fields only).
#[derive(Clone, Debug, Default)]
pub struct WorkflowExecutionInfo {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
}

/// Go `hcommon.NotifyTaskInfo` (fields the queue package reads).
#[derive(Clone, Default)]
pub struct NotifyTaskInfo {
    pub execution_info: Option<WorkflowExecutionInfo>,
    pub tasks: Vec<Arc<dyn PersistenceTask>>,
    pub persistence_error: bool,
    /// Go `ClusterCurrentTimes` (standby timer gates).
    pub cluster_current_times: HashMap<String, Time>,
}
