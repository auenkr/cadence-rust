//! Port of the queue/history notification entry points in engineimpl/notify_tasks.go.
use super::{deps::HistoryTaskCategory, runtime::{HistoryEngine, HistoryNotification, Task}};

impl HistoryEngine {
    pub fn notify_new_history_event(&self, event: HistoryNotification) {
        self.history_event_notifier.notify_new_history_event(event);
    }

    pub fn notify_new_transfer_tasks(&self, tasks: &[Task]) {
        self.notify_new_queue_tasks(HistoryTaskCategory::Transfer, tasks);
    }

    pub fn notify_new_timer_tasks(&self, tasks: &[Task]) {
        self.notify_new_queue_tasks(HistoryTaskCategory::Timer, tasks);
    }

    fn notify_new_queue_tasks(&self, category: HistoryTaskCategory, tasks: &[Task]) {
        let Some(task) = tasks.first() else { return; };
        // Resolve the first task's version, as Go does; do not regroup a batch.
        let cluster = match self.cluster_metadata.cluster_name_for_failover_version(task.info().version) {
            Ok(cluster) => cluster,
            Err(error) => {
                tracing::warn!(error = %error, value = task.info().version, task_id = task.info().task_id,
                    "cluster name for failover version not found");
                return;
            }
        };
        let Some(processor) = self.queue_processors.get(&category) else {
            match category {
                HistoryTaskCategory::Transfer => tracing::error!("transfer processor not found"),
                HistoryTaskCategory::Timer => tracing::error!("timer processor not found"),
                _ => unreachable!("only transfer and timer notification callers"),
            }
            return;
        };
        processor.notify_new_task(&cluster, tasks);
    }
}
