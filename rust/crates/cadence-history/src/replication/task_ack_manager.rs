//! Port of `service/history/replication/task_ack_manager.go`.
use cadence_api::admin::ReplicationMessages;
use std::sync::Arc;
use std::time::Duration;

use super::deps::{
    ReplicationConfig, ReplicationError, ReplicationMessagesSizeFn, ReplicationMetrics, Result,
    TimeSource, EMPTY_MESSAGE_ID,
};
use super::dynamic_task_batch_sizer::DynamicTaskBatchSizer;
use super::task_reader::TaskReaderApi;
use super::task_store::TaskStore;
use super::deps::HistoryTask;

const SCOPE: &str = "ReplicatorQueueProcessor";

/// `ackLevelStore` (replication category only).
pub trait AckLevelStore: Send + Sync {
    fn update_if_needed_and_get_queue_max_read_level(&self, cluster: &str) -> i64;
    fn get_queue_cluster_ack_level(&self, cluster: &str) -> i64;
    fn update_queue_cluster_ack_level(&self, cluster: &str, ack_level: i64) -> Result<()>;
}

/// `getTasksResult`.
#[derive(Debug, Clone)]
pub struct GetTasksResult {
    pub previous_read_task_id: i64,
    pub last_read_task_id: i64,
    pub msgs: ReplicationMessages,
    pub task_infos: Vec<HistoryTask>,
    pub is_shrunk: bool,
}

/// `TaskAckManager`.
pub struct TaskAckManager {
    ack_levels: Arc<dyn AckLevelStore>,
    metrics: Arc<dyn ReplicationMetrics>,
    reader: Arc<dyn TaskReaderApi>,
    store: Arc<TaskStore>,
    replication_messages_size_fn: ReplicationMessagesSizeFn,
    max_replication_messages_size: usize,
    dynamic_task_batch_sizer: Arc<dyn DynamicTaskBatchSizer>,
    time_source: Arc<dyn TimeSource>,
    shard_id: i32,
}

impl TaskAckManager {
    /// `NewTaskAckManager`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_id: i32,
        ack_levels: Arc<dyn AckLevelStore>,
        metrics: Arc<dyn ReplicationMetrics>,
        reader: Arc<dyn TaskReaderApi>,
        store: Arc<TaskStore>,
        time_source: Arc<dyn TimeSource>,
        config: &dyn ReplicationConfig,
        replication_messages_size_fn: ReplicationMessagesSizeFn,
        dynamic_task_batch_sizer: Arc<dyn DynamicTaskBatchSizer>,
    ) -> Self {
        Self {
            ack_levels,
            metrics,
            reader,
            store,
            replication_messages_size_fn,
            max_replication_messages_size: config.max_response_size(),
            dynamic_task_batch_sizer,
            time_source,
            shard_id,
        }
    }

    /// `GetTasks`.
    pub async fn get_tasks(
        &self,
        polling_cluster: &str,
        last_read_task_id: i64,
    ) -> Result<ReplicationMessages> {
        let result = self.get_tasks_inner(polling_cluster, last_read_task_id).await;
        match &result {
            Ok(r) => self.dynamic_task_batch_sizer.analyse(None, Some(r)),
            Err(e) => self.dynamic_task_batch_sizer.analyse(Some(e), None),
        }
        result.map(|r| r.msgs)
    }

    async fn get_tasks_inner(
        &self,
        polling_cluster: &str,
        mut last_read_task_id: i64,
    ) -> Result<GetTasksResult> {
        let previous_read_task_id = self.ack_levels.get_queue_cluster_ack_level(polling_cluster);
        if last_read_task_id == EMPTY_MESSAGE_ID {
            last_read_task_id = previous_read_task_id;
        }
        let instance = self.shard_id.to_string();
        let tags = [("instance", instance.as_str())];

        let start = self.time_source.now_nanos();
        let result = self
            .get_tasks_body(polling_cluster, previous_read_task_id, last_read_task_id, &tags)
            .await;
        let elapsed = (self.time_source.now_nanos() - start).max(0) as u64;
        self.metrics.record_timer(SCOPE, "task_latency", &tags, Duration::from_nanos(elapsed));
        self.metrics
            .exponential_histogram(SCOPE, "task_latency_histogram", &tags, Duration::from_nanos(elapsed));
        result
    }

    async fn get_tasks_body(
        &self,
        polling_cluster: &str,
        previous_read_task_id: i64,
        last_read_task_id: i64,
        tags: &[(&str, &str)],
    ) -> Result<GetTasksResult> {
        let batch_size = self.dynamic_task_batch_sizer.value();
        self.metrics.update_gauge(SCOPE, "replication_tasks_batch_size", tags, batch_size as f64);

        let max_read_level = self
            .ack_levels
            .update_if_needed_and_get_queue_max_read_level(polling_cluster);
        let (task_infos, has_more) = self
            .reader
            .read(last_read_task_id, max_read_level, batch_size)
            .await?;
        let tasks_fetched = task_infos.len();
        self.metrics.record_timer(SCOPE, "replication_tasks_fetched", tags, Duration::from_nanos(tasks_fetched as u64));
        self.metrics.record_histogram_value(SCOPE, "replication_tasks_fetched_histogram", tags, tasks_fetched as f64);
        self.metrics.add_counter(SCOPE, "replication_tasks_fetched_counter", tags, tasks_fetched as i64);

        let mut msgs = ReplicationMessages {
            replication_tasks: Vec::with_capacity(task_infos.len()),
            last_retrieved_message_id: last_read_task_id,
            has_more,
            ..Default::default()
        };

        let (oldest_unprocessed_task_id, oldest_unprocessed_task_timestamp) =
            if let Some(first) = task_infos.first() {
                (first.task_id(), first.info().visibility_timestamp_nanos)
            } else {
                msgs.last_retrieved_message_id = max_read_level;
                (max_read_level, self.time_source.now_nanos())
            };

        let lag_raw = max_read_level - oldest_unprocessed_task_id;
        self.metrics.record_timer(SCOPE, "replication_tasks_lag_raw", tags, Duration::from_nanos(lag_raw.max(0) as u64));
        self.metrics.record_histogram_value(SCOPE, "replication_tasks_lag_raw_histogram", tags, lag_raw as f64);
        self.metrics.update_gauge(SCOPE, "replication_tasks_lag_raw_gauge", tags, lag_raw as f64);
        self.metrics.record_histogram_duration(
            SCOPE,
            "replication_tasks_delay",
            tags,
            Duration::from_nanos((self.time_source.now_nanos() - oldest_unprocessed_task_timestamp).max(0) as u64),
        );

        for info in &task_infos {
            let task = match self.store.get(polling_cluster, info).await {
                Ok(t) => t,
                Err(err) => {
                    if matches!(
                        err,
                        ReplicationError::BadRequest(_)
                            | ReplicationError::InternalDataInconsistency(_)
                            | ReplicationError::EntityNotExists(_)
                    ) {
                        tracing::warn!(error = %err, "Failed to get replication task.");
                        None
                    } else {
                        tracing::error!(error = %err, "Failed to get replication task. Return what we have so far.");
                        msgs.has_more = true;
                        break;
                    }
                }
            };
            msgs.last_retrieved_message_id = info.task_id();
            if let Some(task) = task {
                msgs.replication_tasks.push((*task).clone());
            }
        }

        let is_shrunk = self.shrink_messages_by_size(&mut msgs)?;

        let replication_lag = max_read_level - msgs.last_retrieved_message_id;
        self.metrics.record_timer(SCOPE, "replication_tasks_lag", tags, Duration::from_nanos(replication_lag.max(0) as u64));
        self.metrics.record_histogram_value(SCOPE, "replication_tasks_lag_histogram", tags, replication_lag as f64);
        self.metrics.update_gauge(SCOPE, "replication_tasks_lag_gauge", tags, replication_lag as f64);

        let tasks_returned = msgs.replication_tasks.len();
        self.metrics.record_timer(SCOPE, "replication_tasks_returned", tags, Duration::from_nanos(tasks_returned as u64));
        self.metrics.update_gauge(SCOPE, "replication_tasks_returned_gauge", tags, tasks_returned as f64);
        self.metrics.add_counter(SCOPE, "replication_tasks_returned_counter", tags, tasks_returned as i64);

        let tasks_returned_diff = task_infos.len() as i64 - tasks_returned as i64;
        self.metrics.record_timer(SCOPE, "replication_tasks_returned_diff", tags, Duration::from_nanos(tasks_returned_diff.max(0) as u64));
        self.metrics.update_gauge(SCOPE, "replication_tasks_returned_diff_gauge", tags, tasks_returned_diff as f64);
        self.metrics.add_counter(SCOPE, "replication_tasks_returned_diff_counter", tags, tasks_returned_diff);

        self.ack_level(polling_cluster, last_read_task_id);

        tracing::debug!(
            source_cluster = polling_cluster,
            read_level = msgs.last_retrieved_message_id,
            "Get replication tasks"
        );

        Ok(GetTasksResult {
            previous_read_task_id,
            last_read_task_id,
            msgs,
            task_infos,
            is_shrunk,
        })
    }

    /// `ackLevel`.
    fn ack_level(&self, polling_cluster: &str, last_read_task_id: i64) {
        if let Err(e) = self
            .ack_levels
            .update_queue_cluster_ack_level(polling_cluster, last_read_task_id)
        {
            tracing::error!(error = %e, "error updating replication level for shard");
        }
        if let Err(e) = self.store.ack(polling_cluster, last_read_task_id) {
            tracing::error!(error = %e, "error updating replication level for hydrated task store");
        }
    }

    /// `shrinkMessagesBySize`.
    fn shrink_messages_by_size(&self, msgs: &mut ReplicationMessages) -> Result<bool> {
        if msgs.replication_tasks.is_empty() {
            return Ok(false);
        }
        let max_size = self.max_replication_messages_size;
        let mut is_shrunk = false;
        loop {
            let total_size = (self.replication_messages_size_fn)(msgs);
            if total_size < max_size {
                return Ok(is_shrunk);
            }
            if let Some(last) = msgs.replication_tasks.last() {
                tracing::warn!(
                    total_size,
                    max_size,
                    task_id = last.source_task_id,
                    creation_time = last.creation_time,
                    "Replication messages size is too large. Shrinking the messages by removing the last replication task"
                );
            }
            msgs.has_more = true;
            msgs.replication_tasks.pop();
            is_shrunk = true;
            match msgs.replication_tasks.last() {
                None => {
                    return Err(ReplicationError::Other(
                        "replication messages size is too large and cannot be shrunk anymore, shard will be stuck until the message size is reduced or max size is increased".into(),
                    ))
                }
                Some(last) => {
                    msgs.last_retrieved_message_id = last.source_task_id.unwrap_or_default();
                }
            }
        }
    }
}
