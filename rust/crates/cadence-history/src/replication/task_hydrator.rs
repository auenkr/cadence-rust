//! Port of `service/history/replication/task_hydrator.go`.
use async_trait::async_trait;
use cadence_api::enums::ReplicationTaskType;
use cadence_api::types::{
    DataBlob, FailoverMarkerAttributes, HistoryTaskV2Attributes, ReplicationTask,
    SyncActivityTaskAttributes, VersionHistories,
};
use cadence_persistence::version_history::{new_item, VersionHistoriesExt};
use std::collections::HashMap;
use std::sync::Arc;

use super::deps::{
    DomainCache, HistoryManager, HistoryTask, MutableStateProvider, MutableStateView,
    ReleaseFn, ReplicationError, Result, TaskInfo, EMPTY_EVENT_ID, FIRST_EVENT_ID,
};
use super::task_store::TaskHydratorApi;
use crate::mutable_state_activity::ActivityInfo;

/// `errUnknownReplicationTask`.
fn err_unknown_replication_task() -> ReplicationError {
    ReplicationError::unknown_replication_task()
}

fn bad_request<E: std::fmt::Display>(e: E) -> ReplicationError {
    ReplicationError::BadRequest(e.to_string())
}

/// Task parameters of a `persistence.HistoryReplicationTask`.
#[derive(Debug, Clone)]
pub struct HistoryReplicationTaskRef {
    pub info: TaskInfo,
    pub first_event_id: i64,
    pub next_event_id: i64,
    pub branch_token: Vec<u8>,
    pub new_run_branch_token: Vec<u8>,
}

/// `historyProvider`.
#[async_trait]
pub trait HistoryProvider: Send + Sync {
    async fn get_event_blob(&self, task: &HistoryReplicationTaskRef) -> Result<DataBlob>;
    async fn get_next_run_event_blob(
        &self,
        task: &HistoryReplicationTaskRef,
    ) -> Result<Option<DataBlob>>;
}

/// `TaskHydrator`.
pub struct TaskHydrator {
    ms_provider: Arc<dyn MutableStateProvider>,
    history: Arc<dyn HistoryProvider>,
}

impl TaskHydrator {
    /// `NewImmediateTaskHydrator`.
    pub fn new_immediate(
        is_running: bool,
        version_histories: Option<VersionHistories>,
        activities: HashMap<i64, ActivityInfo>,
        blob: Option<DataBlob>,
        next_blob: Option<DataBlob>,
    ) -> Self {
        Self {
            history: Arc::new(ImmediateHistoryProvider { blob, next_blob }),
            ms_provider: Arc::new(ImmediateMutableStateProvider {
                ms: Arc::new(ImmediateMutableState {
                    is_running,
                    activities,
                    version_histories,
                }),
            }),
        }
    }

    /// `NewDeferredTaskHydrator`.
    pub fn new_deferred(
        shard_id: i32,
        history_manager: Arc<dyn HistoryManager>,
        ms_provider: Arc<dyn MutableStateProvider>,
        domains: Arc<dyn DomainCache>,
    ) -> Self {
        Self {
            history: Arc::new(HistoryLoader {
                shard_id,
                history: history_manager,
                domains,
            }),
            ms_provider,
        }
    }

    /// `Hydrate`.
    pub async fn hydrate(&self, task: &HistoryTask) -> Result<Option<ReplicationTask>> {
        if let HistoryTask::FailoverMarker { info } = task {
            return Ok(Some(hydrate_failover_marker_task(info)));
        }
        let info = task.info();
        let (ms, release) = match self
            .ms_provider
            .get_mutable_state(&info.domain_id, &info.workflow_id, &info.run_id)
            .await
        {
            Ok(v) => v,
            Err(e) if e.is_entity_not_exists() => return Ok(None),
            Err(e) => return Err(e),
        };
        // Go releases through a deferred func with the return error and, for
        // history replication tasks, an extra `release(nil)`; release exactly once.
        match task {
            HistoryTask::SyncActivity { info, scheduled_id } => {
                let res = hydrate_sync_activity_task(info, *scheduled_id, ms.as_ref());
                release(res.as_ref().err());
                res
            }
            HistoryTask::HistoryReplication {
                info,
                first_event_id,
                next_event_id,
                branch_token,
                new_run_branch_token,
            } => {
                let version_histories = ms.get_version_histories();
                release(None);
                let mut t = HistoryReplicationTaskRef {
                    info: info.clone(),
                    first_event_id: *first_event_id,
                    next_event_id: *next_event_id,
                    branch_token: branch_token.clone(),
                    new_run_branch_token: new_run_branch_token.clone(),
                };
                hydrate_history_replication_task(&mut t, version_histories, self.history.as_ref())
                    .await
            }
            _ => {
                let err = err_unknown_replication_task();
                release(Some(&err));
                Err(err)
            }
        }
    }
}

#[async_trait]
impl TaskHydratorApi for TaskHydrator {
    async fn hydrate(&self, task: &HistoryTask) -> Result<Option<ReplicationTask>> {
        TaskHydrator::hydrate(self, task).await
    }
}

/// `hydrateFailoverMarkerTask`.
pub fn hydrate_failover_marker_task(t: &TaskInfo) -> ReplicationTask {
    ReplicationTask {
        task_type: Some(ReplicationTaskType::FailoverMarker),
        source_task_id: Some(t.task_id),
        failover_marker_attributes: Some(FailoverMarkerAttributes {
            domain_id: Some(t.domain_id.clone()),
            failover_version: Some(t.version),
            ..Default::default()
        }),
        creation_time: Some(t.visibility_timestamp_nanos),
        ..Default::default()
    }
}

/// `hydrateSyncActivityTask`.
pub fn hydrate_sync_activity_task(
    task: &TaskInfo,
    scheduled_id: i64,
    ms: &dyn MutableStateView,
) -> Result<Option<ReplicationTask>> {
    if !ms.is_workflow_execution_running() {
        return Ok(None);
    }
    let activity_info = match ms.get_activity_info(scheduled_id) {
        Some(a) => a,
        None => return Ok(None),
    };
    let started_time = if activity_info.started_id != EMPTY_EVENT_ID {
        Some(activity_info.started_time)
    } else {
        None
    };
    let version_history = match ms.get_version_histories() {
        Some(vhs) => Some(vhs.current_history().map_err(bad_request)?.clone()),
        None => None,
    };
    Ok(Some(ReplicationTask {
        task_type: Some(ReplicationTaskType::SyncActivity),
        source_task_id: Some(task.task_id),
        sync_activity_task_attributes: Some(SyncActivityTaskAttributes {
            domain_id: Some(task.domain_id.clone()),
            workflow_id: Some(task.workflow_id.clone()),
            run_id: Some(task.run_id.clone()),
            version: Some(activity_info.version),
            scheduled_id: Some(activity_info.schedule_id),
            scheduled_time: Some(activity_info.scheduled_time),
            started_id: Some(activity_info.started_id),
            started_time,
            last_heartbeat_time: Some(activity_info.last_heartbeat_updated_time),
            details: Some(activity_info.details.clone()),
            attempt: Some(activity_info.attempt),
            last_failure_reason: Some(activity_info.last_failure_reason.clone()),
            last_worker_identity: Some(activity_info.last_worker_identity.clone()),
            last_failure_details: Some(activity_info.last_failure_details.clone()),
            version_history,
            ..Default::default()
        }),
        creation_time: Some(task.visibility_timestamp_nanos),
        ..Default::default()
    }))
}

/// `hydrateHistoryReplicationTask`.
pub async fn hydrate_history_replication_task(
    task: &mut HistoryReplicationTaskRef,
    version_histories: Option<VersionHistories>,
    history: &dyn HistoryProvider,
) -> Result<Option<ReplicationTask>> {
    let version_histories = match version_histories {
        Some(v) => v,
        None => return Ok(None),
    };
    let (_, version_history) = version_histories
        .find_first_by_item(&new_item(task.first_event_id, task.info.version))
        .map_err(bad_request)?;
    if task.branch_token.is_empty() {
        task.branch_token = version_history.branch_token.clone();
    }
    let events_blob = history.get_event_blob(task).await?;
    let new_run_events_blob = history.get_next_run_event_blob(task).await?;
    Ok(Some(ReplicationTask {
        task_type: Some(ReplicationTaskType::HistoryV2),
        source_task_id: Some(task.info.task_id),
        history_task_v2_attributes: Some(HistoryTaskV2Attributes {
            domain_id: Some(task.info.domain_id.clone()),
            workflow_id: Some(task.info.workflow_id.clone()),
            run_id: Some(task.info.run_id.clone()),
            version_history_items: Some(version_history.items.clone()),
            events: Some(events_blob),
            new_run_events: new_run_events_blob,
            ..Default::default()
        }),
        creation_time: Some(task.info.visibility_timestamp_nanos),
        ..Default::default()
    }))
}

/// `historyLoader`.
struct HistoryLoader {
    shard_id: i32,
    history: Arc<dyn HistoryManager>,
    domains: Arc<dyn DomainCache>,
}

impl HistoryLoader {
    async fn get_events_blob(
        &self,
        domain_id: &str,
        branch_token: &[u8],
        min_event_id: i64,
        max_event_id: i64,
    ) -> Result<DataBlob> {
        let domain = self.domains.get_domain_by_id(domain_id)?;
        let mut blobs = self
            .history
            .read_raw_history_branch(
                self.shard_id,
                &domain.name,
                branch_token,
                min_event_id,
                max_event_id,
                2, // Load more than one to check for data inconsistency errors
            )
            .await?;
        if blobs.len() != 1 {
            return Err(ReplicationError::InternalDataInconsistency(
                "replication hydrator encountered more than 1 NDC raw event batch".into(),
            ));
        }
        Ok(blobs.remove(0))
    }
}

#[async_trait]
impl HistoryProvider for HistoryLoader {
    async fn get_event_blob(&self, task: &HistoryReplicationTaskRef) -> Result<DataBlob> {
        self.get_events_blob(
            &task.info.domain_id,
            &task.branch_token,
            task.first_event_id,
            task.next_event_id,
        )
        .await
    }

    async fn get_next_run_event_blob(
        &self,
        task: &HistoryReplicationTaskRef,
    ) -> Result<Option<DataBlob>> {
        if task.new_run_branch_token.is_empty() {
            return Ok(None);
        }
        self.get_events_blob(
            &task.info.domain_id,
            &task.new_run_branch_token,
            FIRST_EVENT_ID,
            FIRST_EVENT_ID + 1,
        )
        .await
        .map(Some)
    }
}

/// `immediateHistoryProvider`.
struct ImmediateHistoryProvider {
    blob: Option<DataBlob>,
    next_blob: Option<DataBlob>,
}

#[async_trait]
impl HistoryProvider for ImmediateHistoryProvider {
    async fn get_event_blob(&self, _task: &HistoryReplicationTaskRef) -> Result<DataBlob> {
        self.blob
            .clone()
            .ok_or_else(|| ReplicationError::Other("history blob not set".into()))
    }

    async fn get_next_run_event_blob(
        &self,
        _task: &HistoryReplicationTaskRef,
    ) -> Result<Option<DataBlob>> {
        Ok(self.next_blob.clone())
    }
}

/// `immediateMutableStateProvider` / `immediateMutableState`.
struct ImmediateMutableStateProvider {
    ms: Arc<ImmediateMutableState>,
}

#[async_trait]
impl MutableStateProvider for ImmediateMutableStateProvider {
    async fn get_mutable_state(
        &self,
        _domain_id: &str,
        _workflow_id: &str,
        _run_id: &str,
    ) -> Result<(Arc<dyn MutableStateView>, ReleaseFn)> {
        let ms: Arc<dyn MutableStateView> = self.ms.clone();
        // `execution.NoopReleaseFn`
        Ok((ms, Box::new(|_| {})))
    }
}

struct ImmediateMutableState {
    is_running: bool,
    activities: HashMap<i64, ActivityInfo>,
    version_histories: Option<VersionHistories>,
}

impl MutableStateView for ImmediateMutableState {
    fn is_workflow_execution_running(&self) -> bool {
        self.is_running
    }
    fn get_activity_info(&self, id: i64) -> Option<ActivityInfo> {
        self.activities.get(&id).cloned()
    }
    fn get_version_histories(&self) -> Option<VersionHistories> {
        self.version_histories.clone()
    }
}
