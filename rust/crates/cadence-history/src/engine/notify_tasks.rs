//! Port of the queue/history notification entry points in engineimpl/notify_tasks.go.
use super::{
    deps::HistoryTaskCategory,
    runtime::{HistoryEngine, HistoryNotification, Task},
};

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
        let Some(task) = tasks.first() else {
            return;
        };
        // Resolve the first task's version, as Go does; do not regroup a batch.
        let cluster = match self
            .cluster_metadata
            .cluster_name_for_failover_version(task.info().version)
        {
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

use super::deps::{ActivityInfo, HResult, HistoryError, WorkflowExecutionInfo, FIRST_EVENT_ID};
use async_trait::async_trait;
use cadence_api::{
    admin::ReplicationTaskInfo,
    types::{DataBlob, ReplicationTask, VersionHistories},
};
use std::{collections::HashMap, sync::Arc};

/// TODO(link): persistence.Task with its visibility timestamp and concrete type.
pub struct NotificationReplicationTask {
    pub task: Task,
    pub visibility_timestamp: i64,
}
pub struct PersistedNotificationBlob {
    pub branch_token: Vec<u8>,
    pub first_event_id: i64,
    pub blob: DataBlob,
}
pub struct ReplicationNotification {
    pub tasks: Vec<NotificationReplicationTask>,
    pub execution_info: WorkflowExecutionInfo,
    pub version_histories: Option<VersionHistories>,
    pub activities: HashMap<i64, ActivityInfo>,
    pub history: Vec<PersistedNotificationBlob>,
}
pub struct ImmediateHydration<'a> {
    pub running: bool,
    pub version_histories: Option<&'a VersionHistories>,
    pub activities: &'a HashMap<i64, ActivityInfo>,
    pub history: Option<&'a DataBlob>,
    pub new_run_history: Option<&'a DataBlob>,
    /// Mirrors the temporary persistence.ReplicationTaskInfo built by Go.
    pub info: ReplicationTaskInfo,
    pub creation_time: i64,
}
#[async_trait]
pub trait ImmediateReplicationHydrator: Send + Sync {
    async fn hydrate(
        &self,
        task: &Task,
        inputs: ImmediateHydration<'_>,
    ) -> HResult<ReplicationTask>;
}
pub trait ReplicationTaskStore: Send + Sync {
    fn put(&self, task: ReplicationTask);
}
pub struct ReplicationNotificationComponents {
    pub hydrator: Arc<dyn ImmediateReplicationHydrator>,
    pub store: Arc<dyn ReplicationTaskStore>,
}
impl HistoryEngine {
    pub async fn notify_new_replication_tasks(&self, notification: &ReplicationNotification) {
        for task in &notification.tasks {
            match self.hydrate_replication_task(task, notification).await {
                Ok(task) => self.replication_notifications.store.put(task),
                Err(error) => {
                    tracing::error!(error = %error, "failed to preemptively hydrate replication task")
                }
            }
        }
    }
    pub async fn hydrate_replication_task(
        &self,
        task: &NotificationReplicationTask,
        notification: &ReplicationNotification,
    ) -> HResult<ReplicationTask> {
        let mut info = task.task.info().clone();
        info.domain_id = notification.execution_info.domain_id.clone();
        info.workflow_id = notification.execution_info.workflow_id.clone();
        info.run_id = notification.execution_info.run_id.clone();
        let (branch, new_branch) = match &task.task {
            Task::History {
                branch_token,
                new_run_branch_token,
                ..
            } => (branch_token.as_slice(), new_run_branch_token.as_slice()),
            Task::SyncActivity(_) | Task::FailoverMarker(_) => (&[][..], &[][..]),
            Task::Other(_) => return Err(HistoryError::Other("unknown replication task".into())),
        };
        let find = |branch: &[u8], first| {
            notification
                .history
                .iter()
                .find(|blob| blob.branch_token == branch && blob.first_event_id == first)
                .map(|blob| &blob.blob)
        };
        let history = find(branch, info.first_event_id);
        let new_run_history = find(new_branch, FIRST_EVENT_ID);
        self.replication_notifications
            .hydrator
            .hydrate(
                &task.task,
                ImmediateHydration {
                    running: notification.execution_info.is_running(),
                    version_histories: notification.version_histories.as_ref(),
                    activities: &notification.activities,
                    history,
                    new_run_history,
                    info,
                    creation_time: task.visibility_timestamp,
                },
            )
            .await
    }
}
