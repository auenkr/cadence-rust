//! Port of `task_reader.go`.
use super::deps::{ExecutionManager, HistoryTask, Result};
use async_trait::async_trait;
use std::sync::Arc;

/// `taskReader` interface.
#[async_trait]
pub trait TaskReaderApi: Send + Sync {
    async fn read(&self, read_level: i64, max_read_level: i64, batch_size: usize) -> Result<(Vec<HistoryTask>, bool)>;
}

pub struct TaskReader {
    shard_id: i32,
    execution_manager: Arc<dyn ExecutionManager>,
}

impl TaskReader {
    pub fn new(shard_id: i32, execution_manager: Arc<dyn ExecutionManager>) -> Self {
        Self { shard_id, execution_manager }
    }
}

#[async_trait]
impl TaskReaderApi for TaskReader {
    async fn read(&self, read_level: i64, max_read_level: i64, batch_size: usize) -> Result<(Vec<HistoryTask>, bool)> {
        if read_level >= max_read_level {
            return Ok((Vec::new(), false));
        }
        self.execution_manager
            .get_replication_tasks(self.shard_id, read_level + 1, max_read_level + 1, batch_size)
            .await
    }
}
