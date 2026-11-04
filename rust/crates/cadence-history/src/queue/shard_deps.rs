//! Stand-ins for the parts of go `shard.Context`, `task.*`, `config.Config` used by the
//! transfer/timer queue processors (transfer_queue_processor.go, timer_queue_processor.go,
//! factory.go) beyond the minimal `ShardContext` in deps.rs.
//! TODO(link): replace by shard::Context / task / config once those ports merge.
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use super::deps::{
    HistoryTaskCategory, PersistenceTask, QueueError, ShardContext, TaskRef, Time,
};
use super::util::PersistenceProcessingQueueState;

/// Go `persistence.FailoverLevel` (task keys flattened to the key kinds' fields).
#[derive(Clone, Debug)]
pub struct FailoverLevel {
    pub start_time: Time,
    pub min_level: HistoryKey,
    pub current_level: HistoryKey,
    pub max_level: HistoryKey,
    pub domain_ids: HashSet<String>,
}

/// Go `persistence.HistoryTaskKey` (`GetTaskID` / `GetScheduledTime`).
#[derive(Clone, Copy, Debug)]
pub struct HistoryKey {
    pub scheduled_time: Time,
    pub task_id: i64,
}
impl HistoryKey {
    pub fn immediate(task_id: i64) -> Self {
        HistoryKey { scheduled_time: Time::ZERO, task_id }
    }
    pub fn new(scheduled_time: Time, task_id: i64) -> Self {
        HistoryKey { scheduled_time, task_id }
    }
}

/// Go `RangeCompleteHistoryTaskRequest` (+ response `TasksCompleted`).
#[derive(Clone, Debug)]
pub struct RangeCompleteHistoryTaskRequest {
    pub task_category: HistoryTaskCategory,
    pub inclusive_min_task_key: HistoryKey,
    pub exclusive_max_task_key: HistoryKey,
    pub page_size: i64,
    pub shard_id: i32,
}

/// Go `task.QueueType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueType {
    ActiveTransfer,
    StandbyTransfer,
    ActiveTimer,
    StandbyTimer,
}

/// Go `task.Filter`.
pub type TaskFilter =
    Arc<dyn Fn(Arc<dyn PersistenceTask>) -> super::deps::BoxFut<Result<bool, QueueError>> + Send + Sync>;

/// Wraps `task.NewHistoryTask(...)` (+ `InitializeLoggerForTask`): builds an executable task.
/// The shard, executor, processor, redispatcher and critical retry count are captured by the closure.
pub type HistoryTaskFactory =
    Arc<dyn Fn(Arc<dyn PersistenceTask>, QueueType, TaskFilter) -> TaskRef + Send + Sync>;

/// Go `config.Config` entries read by the processors (beyond `QueueConfig`).
#[derive(Clone)]
pub struct ProcessorConfig {
    pub disable_transfer_failover_queue: super::deps::BoolFn,
    pub disable_timer_failover_queue: super::deps::BoolFn,
    pub queue_processor_enable_graceful_sync_shutdown: super::deps::BoolFn,
    pub transfer_processor_complete_transfer_interval: super::deps::DurationFn,
    pub transfer_processor_complete_transfer_failure_retry_count: super::deps::IntFn,
    pub timer_processor_complete_timer_interval: super::deps::DurationFn,
    pub timer_processor_complete_timer_failure_retry_count: super::deps::IntFn,
    pub transfer_task_delete_batch_size: super::deps::IntFn,
    pub timer_task_delete_batch_size: super::deps::IntFn,
    pub task_critical_retry_count: super::deps::IntFn,
}

/// Go `shard.Context` members used by the queue processors.
#[async_trait]
pub trait QueueShardContext: ShardContext {
    fn current_cluster_name(&self) -> String;
    /// `GetClusterMetadata().GetRemoteClusterInfo()` keys
    fn remote_cluster_names(&self) -> Vec<String>;
    /// `GetClusterMetadata().GetEnabledClusterInfo()` keys
    fn enabled_cluster_names(&self) -> Vec<String>;
    fn processor_config(&self) -> &ProcessorConfig;
    fn queue_config(&self) -> Arc<super::config::QueueConfig>;
    fn domain_cache(&self) -> Arc<dyn super::task_allocator::DomainCache>;
    fn active_cluster_manager(&self) -> Arc<dyn super::task_allocator::ActiveClusterManager>;

    /// `GetQueueAckLevel(category)`
    fn get_queue_ack_level(&self, category: HistoryTaskCategory) -> HistoryKey;
    /// `GetQueueClusterAckLevel(category, cluster)`
    fn get_queue_cluster_ack_level(&self, category: HistoryTaskCategory, cluster: &str) -> HistoryKey;
    /// `UpdateIfNeededAndGetQueueMaxReadLevel(category, cluster)`
    fn update_if_needed_and_get_queue_max_read_level(&self, category: HistoryTaskCategory, cluster: &str) -> HistoryKey;
    async fn update_queue_ack_level(&self, category: HistoryTaskCategory, key: HistoryKey) -> Result<(), QueueError>;
    async fn update_queue_cluster_ack_level(
        &self,
        category: HistoryTaskCategory,
        cluster: &str,
        key: HistoryKey,
    ) -> Result<(), QueueError>;

    fn get_transfer_processing_queue_states(&self, cluster: &str) -> Vec<PersistenceProcessingQueueState>;
    async fn update_transfer_processing_queue_states(
        &self,
        cluster: &str,
        states: Vec<PersistenceProcessingQueueState>,
    ) -> Result<(), QueueError>;
    fn get_timer_processing_queue_states(&self, cluster: &str) -> Vec<PersistenceProcessingQueueState>;
    async fn update_timer_processing_queue_states(
        &self,
        cluster: &str,
        states: Vec<PersistenceProcessingQueueState>,
    ) -> Result<(), QueueError>;

    fn get_all_failover_levels(&self, category: HistoryTaskCategory) -> Vec<FailoverLevel>;
    async fn update_failover_level(
        &self,
        category: HistoryTaskCategory,
        failover_id: &str,
        level: FailoverLevel,
    ) -> Result<(), QueueError>;
    async fn delete_failover_level(&self, category: HistoryTaskCategory, failover_id: &str) -> Result<(), QueueError>;

    /// `GetExecutionManager().RangeCompleteHistoryTask`: returns `TasksCompleted`.
    async fn range_complete_history_task(&self, req: RangeCompleteHistoryTaskRequest) -> Result<i64, QueueError>;
}

/// Go `persistence.HasMoreRowsToDelete`.
pub fn has_more_rows_to_delete(rows_deleted: i64, batch_size: i64) -> bool {
    const UNKNOWN_NUM_ROWS_AFFECTED: i64 = -1;
    !(rows_deleted < batch_size
        || rows_deleted == UNKNOWN_NUM_ROWS_AFFECTED
        || rows_deleted > batch_size
        || batch_size <= 0)
}
