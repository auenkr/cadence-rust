//! Port of engineimpl/notify_tasks.go.
use std::collections::HashMap;
use super::{deps::*, runtime::*};

/// Persistence task kind carried by `hcommon.NotifyTaskInfo.Tasks`.
#[derive(Clone, Debug)]
pub enum NotifiedTaskKind {
    Generic,
    HistoryReplication { branch_token: Vec<u8>, new_run_branch_token: Vec<u8>, first_event_id: i64, next_event_id: i64 },
    SyncActivity { scheduled_id: i64 },
    FailoverMarker,
    Unknown,
}
/// `persistence.Task` accessors used by notify.
#[derive(Clone, Debug)]
pub struct NotifiedTask {
    pub version: i64,
    pub task_id: i64,
    pub task_type: i16,
    pub visibility_timestamp: i64,
    pub kind: NotifiedTaskKind,
}
/// `hcommon.NotifyTaskInfo`
#[derive(Clone, Default)]
pub struct NotifyTaskInfo {
    pub execution_info: WorkflowExecutionInfo,
    pub tasks: Vec<NotifiedTask>,
    pub version_histories: Option<cadence_api::types::VersionHistories>,
    pub activities: HashMap<i64, ActivityInfo>,
    pub history: Vec<PersistedBlobRef>,
}
/// `events.PersistedBlob` (lookup key + payload).
#[derive(Clone)]
pub struct PersistedBlobRef { pub branch_token: Vec<u8>, pub first_event_id: i64, pub events: Vec<HistoryEvent> }
/// `events.PersistedBlobs.Find`
pub fn find_blob(blobs: &[PersistedBlobRef], branch_token: &[u8], first_event_id: i64) -> Option<PersistedBlobRef> {
    blobs.iter().find(|b| b.branch_token == branch_token && b.first_event_id == first_event_id).cloned()
}

impl HistoryEngine {
    /// `NotifyNewHistoryEvent`
    pub fn notify_new_history_event_engine(&self, event: HistoryNotification) {
        self.history_event_notifier.notify_new_history_event(event);
    }
    /// `NotifyNewTransferTasks`
    pub fn notify_new_transfer_tasks(&self, info: &NotifyTaskInfo) {
        self.notify_queue(HistoryTaskCategory::Transfer, "transfer processor not found", info);
    }
    /// `NotifyNewTimerTasks`
    pub fn notify_new_timer_tasks(&self, info: &NotifyTaskInfo) {
        self.notify_queue(HistoryTaskCategory::Timer, "timer processor not found", info);
    }
    fn notify_queue(&self, category: HistoryTaskCategory, missing: &str, info: &NotifyTaskInfo) {
        let Some(task) = info.tasks.first() else { return };
        let cluster_name = match self.cluster_metadata.cluster_name_for_failover_version(task.version) {
            Ok(name) => name,
            Err(err) => {
                tracing::warn!(error = %err, value = task.version, task_id = task.task_id, "cluster name for failover version not found");
                return;
            }
        };
        let Some(processor) = self.queue_processors.get(&category) else {
            tracing::error!("{}", missing);
            return;
        };
        processor.notify_new_task(&cluster_name, info);
    }
    /// `NotifyNewReplicationTasks`
    pub async fn notify_new_replication_tasks(&self, info: &NotifyTaskInfo) {
        for task in &info.tasks {
            match self.hydrate_replication_task(task, info).await {
                Ok(Some(h)) => self.replication_task_store.put(h),
                Ok(None) => {}
                Err(err) => tracing::error!(error = %err, "failed to preemptively hydrate replication task"),
            }
        }
    }
    /// `hydrateReplicationTask`
    async fn hydrate_replication_task(&self, task: &NotifiedTask, info: &NotifyTaskInfo) -> HResult<Option<cadence_api::types::ReplicationTask>> {
        let exec = &info.execution_info;
        let mut ti = cadence_api::admin::ReplicationTaskInfo {
            domain_id: exec.domain_id.clone(),
            workflow_id: exec.workflow_id.clone(),
            run_id: exec.run_id.clone(),
            task_type: task.task_type,
            task_id: task.task_id,
            version: task.version,
            ..Default::default()
        };
        let (branch, new_branch);
        match &task.kind {
            NotifiedTaskKind::HistoryReplication { branch_token, new_run_branch_token, first_event_id, next_event_id } => {
                branch = branch_token.clone();
                new_branch = new_run_branch_token.clone();
                ti.first_event_id = *first_event_id;
                ti.next_event_id = *next_event_id;
            }
            NotifiedTaskKind::SyncActivity { scheduled_id } => {
                branch = Vec::new(); new_branch = Vec::new();
                ti.scheduled_id = *scheduled_id;
            }
            NotifiedTaskKind::FailoverMarker => { branch = Vec::new(); new_branch = Vec::new(); }
            _ => return Err(HistoryError::Other("unknown replication task".into())),
        }
        let history = find_blob(&info.history, &branch, ti.first_event_id);
        let new_run_history = find_blob(&info.history, &new_branch, FIRST_EVENT_ID);
        self.replication_hydrator.hydrate_immediate(ImmediateHydration {
            is_running: exec.is_running(),
            version_histories: info.version_histories.clone(),
            activities: info.activities.clone(),
            history,
            new_run_history,
            info: ti,
            branch_token: branch,
            new_run_branch_token: new_branch,
        }).await
    }
}
