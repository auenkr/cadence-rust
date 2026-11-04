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
}

/// Scope that drops everything (default until the metrics port lands).
pub struct NoopScope;
impl MetricsScope for NoopScope {
    fn inc_counter(&self, _: &'static str) {}
    fn add_counter(&self, _: &'static str, _: i64) {}
    fn record_timer(&self, _: &'static str, _: Duration) {}
}

/// Go `common.Daemon`.
pub trait Daemon: Send + Sync {
    fn start(self: Arc<Self>);
    fn stop(&self);
}

#[derive(Debug, Clone)]
pub struct QueueError(pub String);
impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for QueueError {}

/// TODO(link): replace by types::CrossClusterTaskRequest
#[derive(Clone, Debug, Default)]
pub struct CrossClusterTaskRequest;
/// TODO(link): replace by types::CrossClusterTaskResponse
#[derive(Clone, Debug, Default)]
pub struct CrossClusterTaskResponse;
