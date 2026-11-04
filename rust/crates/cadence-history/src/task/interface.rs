//! Port of `go/service/history/task/interface.go`.

use std::sync::Arc;

use async_trait::async_trait;

use super::deps::{
    Daemon, MetricsScope, PersistenceTask, Result, ShardContext, TaskError, Time,
};

/// `ctask.State`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Acked,
    Nacked,
    Canceled,
}

/// Go `QueueType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QueueType {
    /// QueueTypeActiveTransfer is the queue type for active transfer queue processor
    ActiveTransfer = 1,
    StandbyTransfer,
    ActiveTimer,
    StandbyTimer,
    Replication,
    Transfer,
    Timer,
}

pub const QUEUE_TYPE_ACTIVE_TRANSFER: QueueType = QueueType::ActiveTransfer;
pub const QUEUE_TYPE_STANDBY_TRANSFER: QueueType = QueueType::StandbyTransfer;
pub const QUEUE_TYPE_ACTIVE_TIMER: QueueType = QueueType::ActiveTimer;
pub const QUEUE_TYPE_STANDBY_TIMER: QueueType = QueueType::StandbyTimer;
pub const QUEUE_TYPE_REPLICATION: QueueType = QueueType::Replication;
pub const QUEUE_TYPE_TRANSFER: QueueType = QueueType::Transfer;
pub const QUEUE_TYPE_TIMER: QueueType = QueueType::Timer;

/// Go `Task` interface: `task.PriorityTask` + `persistence.Task` + history specific accessors.
#[async_trait]
pub trait Task: PersistenceTask {
    // task.PriorityTask
    async fn execute(&self) -> Result<()>;
    fn handle_err(&self, err: Option<TaskError>) -> Option<TaskError>;
    fn retry_err(&self, err: &TaskError) -> bool;
    fn ack(&self);
    async fn nack(&self, err: Option<TaskError>);
    fn cancel(&self);
    fn state(&self) -> TaskState;
    fn priority(&self) -> i32;
    fn set_priority(&self, priority: i32);

    fn get_queue_type(&self) -> QueueType;
    fn get_shard(&self) -> Arc<dyn ShardContext>;
    fn get_attempt(&self) -> i32;
    fn get_info(&self) -> Arc<dyn PersistenceTask>;
    fn set_initial_submit_time(&self, t: Time);
}

/// `CrossClusterTask` is the interface for processing cross cluster task in the source cluster.
pub trait CrossClusterTask: Task {
    fn is_valid(&self) -> bool;
    fn is_ready_for_poll(&self) -> bool;
    // TODO(link): replace by types::CrossClusterTaskRequest/Response
    fn get_cross_cluster_request(&self) -> Result<Vec<u8>>;
    fn record_response(&self, resp: Vec<u8>) -> Result<()>;
}

/// `Key` identifies a Task and defines a total order among tasks.
pub trait Key: Send + Sync {
    fn less(&self, other: &dyn Key) -> bool;
}

/// `ExecuteResponse`
#[derive(Clone)]
pub struct ExecuteResponse {
    pub scope: Arc<dyn MetricsScope>,
    pub is_active_task: bool,
}

/// `Executor` contains the execution logic for Task.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, task: Arc<dyn Task>) -> (ExecuteResponse, Result<()>);
    fn stop(&self);
}

/// `Filter` filters Task.
pub type Filter = Arc<dyn Fn(&dyn PersistenceTask) -> Result<bool> + Send + Sync>;

/// `Initializer` initializes a Task based on the Info.
pub type Initializer = Arc<dyn Fn(Arc<dyn PersistenceTask>) -> Arc<dyn Task> + Send + Sync>;

/// `PriorityAssigner` assigns priority to Tasks.
pub trait PriorityAssigner: Send + Sync {
    fn assign(&self, task: &dyn Task) -> Result<()>;
}

/// `Processor` is the worker pool for processing Tasks.
#[async_trait]
pub trait Processor: Daemon {
    async fn submit(&self, task: Arc<dyn Task>) -> Result<()>;
    async fn try_submit(&self, task: Arc<dyn Task>) -> Result<bool>;
}

/// `Redispatcher` buffers tasks and periodically redispatch them to Processor.
#[async_trait]
pub trait Redispatcher: Daemon {
    fn add_task(&self, task: Arc<dyn Task>);
    async fn redispatch(&self, target_size: usize);
    fn redispatch_task(&self, task: Arc<dyn Task>, t: Time);
    fn size(&self) -> usize;
}

pub trait Rescheduler: Daemon {
    fn reschedule_task(&self, task: Arc<dyn Task>, t: Time);
    fn reschedule_domains(&self, domain_ids: &std::collections::HashSet<String>);
    fn size(&self) -> usize;
}

/// `Fetcher` is a host level component for aggregating task fetch requests
/// from all shards on the host and perform one fetching operation for aggregated requests.
#[async_trait]
pub trait Fetcher: Daemon {
    fn get_source_cluster(&self) -> String;
    // TODO(link): replace by future::Future
    async fn fetch(
        &self,
        shard_id: i32,
        fetch_params: Vec<Box<dyn std::any::Any + Send>>,
    ) -> Result<Box<dyn std::any::Any + Send>>;
}

/// `Fetchers` is a group of Fetchers, one for each source cluster.
pub type Fetchers = Vec<Arc<dyn Fetcher>>;
