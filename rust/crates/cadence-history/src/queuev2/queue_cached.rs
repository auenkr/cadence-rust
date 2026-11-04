//! Port of queuev2/queue_cached.go

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use super::deps::*;
use super::interface::{Daemon, Queue};
use super::queue_base::QueueBase;
use super::queue_reader_cached_base::CachedQueueReader;

pub struct CachedQueue {
    queue: Arc<dyn Queue>,
    reader: Arc<dyn CachedQueueReader>,
}

/// Go `newCachedQueue`: the queue-state update hook (read level propagation) is
/// installed on the base via `set_read_level_listener`.
pub fn new_cached_queue(
    queue: Arc<dyn Queue>,
    queue_base: &Arc<QueueBase>,
    reader: Arc<dyn CachedQueueReader>,
) -> Arc<dyn Queue> {
    queue_base.set_read_level_listener(reader.clone());
    Arc::new(CachedQueue { queue, reader })
}

#[async_trait]
impl Daemon for CachedQueue {
    async fn start(&self) {
        self.reader.start();
        self.queue.start().await;
    }

    async fn stop(&self) {
        self.queue.stop().await;
        self.reader.stop();
    }
}

#[async_trait]
impl Queue for CachedQueue {
    fn category(&self) -> HistoryTaskCategory {
        self.queue.category()
    }

    fn notify_new_task(&self, cluster_name: &str, info: &NotifyTaskInfo) {
        if info.persistence_error {
            self.reader.clear();
        } else {
            self.reader.inject(&info.tasks);
        }
        self.queue.notify_new_task(cluster_name, info);
    }

    fn failover_domain(&self, domain_ids: &HashSet<String>) {
        self.queue.failover_domain(domain_ids);
    }

    async fn handle_action(
        &self,
        cluster_name: &str,
        action: &Action,
    ) -> Result<Option<ActionResult>, DepError> {
        self.queue.handle_action(cluster_name, action).await
    }

    fn lock_task_processing(&self) {
        self.queue.lock_task_processing();
    }

    fn unlock_task_processing(&self) {
        self.queue.unlock_task_processing();
    }
}
