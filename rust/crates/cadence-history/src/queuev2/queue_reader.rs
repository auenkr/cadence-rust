//! Port of queuev2/queue_reader.go

use std::sync::Arc;

use async_trait::async_trait;

use super::deps::*;
use super::predicate::Predicate;
use super::range::Range;

#[derive(Clone)]
pub struct LookAHeadRequest {
    pub inclusive_min_task_key: HistoryTaskKey,
}

pub struct LookAHeadResponse {
    pub task: Option<PTask>,
    pub look_ahead_max_time: Time,
}

#[derive(Clone)]
pub struct GetTaskRequest {
    pub progress: GetTaskProgress,
    pub predicate: Predicate,
    pub page_size: usize,
}

/// Range of the slice to read, the next page token, and the next task key.
#[derive(Clone)]
pub struct GetTaskProgress {
    pub range: Range,
    pub next_page_token: Option<Vec<u8>>,
    pub next_task_key: HistoryTaskKey,
}

pub struct GetTaskResponse {
    pub tasks: Vec<PTask>,
    pub progress: GetTaskProgress,
}

#[async_trait]
pub trait QueueReader: Send + Sync {
    async fn get_task(&self, req: &GetTaskRequest) -> Result<GetTaskResponse, DepError>;
    async fn look_ahead(&self, req: &LookAHeadRequest) -> Result<LookAHeadResponse, DepError>;
}

pub struct SimpleQueueReader {
    shard: Arc<dyn ShardContext>,
    category: HistoryTaskCategory,
    max_poll_interval: DurationFn,
    max_poll_interval_jitter_coeff: FloatFn,
}

pub fn new_queue_reader(
    shard: Arc<dyn ShardContext>,
    category: HistoryTaskCategory,
    max_poll_interval: DurationFn,
    max_poll_interval_jitter_coeff: FloatFn,
) -> Arc<dyn QueueReader> {
    Arc::new(SimpleQueueReader {
        shard,
        category,
        max_poll_interval,
        max_poll_interval_jitter_coeff,
    })
}

#[async_trait]
impl QueueReader for SimpleQueueReader {
    async fn get_task(&self, req: &GetTaskRequest) -> Result<GetTaskResponse, DepError> {
        let resp = self
            .shard
            .get_execution_manager()
            .get_history_tasks(GetHistoryTasksRequest {
                task_category: self.category,
                inclusive_min_task_key: req.progress.range.inclusive_min_task_key,
                exclusive_max_task_key: req.progress.range.exclusive_max_task_key,
                page_size: req.page_size,
                next_page_token: req.progress.next_page_token.clone(),
                shard_id: Some(self.shard.get_shard_id()),
            })
            .await?;

        let mut next_task_key = req.progress.range.exclusive_max_task_key;
        let mut tasks = Vec::with_capacity(resp.tasks.len());
        for t in &resp.tasks {
            // filter out tasks that don't match the predicate
            if req.predicate.check(t.as_ref()) {
                tasks.push(t.clone());
            }
        }
        let has_more = resp
            .next_page_token
            .as_ref()
            .map(|t| !t.is_empty())
            .unwrap_or(false);
        // If there are more tasks to read, set the next task key to the next of the last task
        if has_more && !resp.tasks.is_empty() {
            next_task_key = resp.tasks[resp.tasks.len() - 1].get_task_key().next();
        }
        Ok(GetTaskResponse {
            tasks,
            progress: GetTaskProgress {
                range: req.progress.range,
                next_page_token: resp.next_page_token,
                next_task_key,
            },
        })
    }

    async fn look_ahead(&self, req: &LookAHeadRequest) -> Result<LookAHeadResponse, DepError> {
        let jit = jit_duration(
            (self.max_poll_interval)(),
            (self.max_poll_interval_jitter_coeff)(),
        );
        let max_time = req.inclusive_min_task_key.get_scheduled_time()
            + chrono::Duration::from_std(jit).unwrap_or(chrono::Duration::zero());
        let resp = self
            .shard
            .get_execution_manager()
            .get_history_tasks(GetHistoryTasksRequest {
                task_category: self.category,
                inclusive_min_task_key: req.inclusive_min_task_key,
                exclusive_max_task_key: HistoryTaskKey::new(max_time, 0),
                page_size: 1,
                next_page_token: None,
                shard_id: Some(self.shard.get_shard_id()),
            })
            .await?;
        Ok(LookAHeadResponse {
            task: resp.tasks.first().cloned(),
            look_ahead_max_time: max_time,
        })
    }
}
