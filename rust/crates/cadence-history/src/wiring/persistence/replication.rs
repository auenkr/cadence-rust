//! group: replication
//!
//! SQL-backed implementations of the persistence-facing traits of the
//! replication package (and the replication adapters of the engine).
//! Sync-over-async bridging uses `tokio::task::block_in_place`, which needs a multi-thread runtime.
use async_trait::async_trait;
use cadence_api::admin::ReplicationMessages;
use cadence_api::enums::EncodingType;
use cadence_api::errors::{ServiceBusyError, RetryTaskV2Error};
use cadence_api::types::DataBlob;
use cadence_api::CadenceError;
use cadence_persistence::history_contract::{
    GoExecutionStore, HistoryTaskCategory, PersistenceError, ReplicationTask as PReplicationTask,
};
use cadence_persistence::history_reader::{
    read_history_branch, HistoryNodeStore, HistoryReadError, ReadHistoryBranchRequest,
};
use cadence_persistence::history_task_sql::{HistoryTaskQuery, ReplicationDlqQuery};
use cadence_persistence::sqlblobs::{replication_task_from_struct, Encoding};
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::{DLQRead, ReplicationAckManager, ReplicationDLQHandler};
use crate::replication::deps::{
    DlqTaskInfo, DomainCache, ExecutionManager, GetReplicationTasksFromDlqResponse, HistoryManager,
    HistoryTask, PutReplicationTaskToDlqRequest, ReplicationDlqTask, ReplicationError, Result,
    ShardContext, TaskInfo, FIRST_EVENT_ID,
};
use crate::replication::dlq_handler::DlqHandler;
use crate::replication::task_ack_manager::{AckLevelStore, TaskAckManager};
use crate::replication::task_hydrator::{HistoryProvider, HistoryReplicationTaskRef};
use crate::replication::task_reader::TaskReaderApi;

/// Maps persistence errors the way the Go replication package classifies them.
pub fn map_persistence_error(e: PersistenceError) -> ReplicationError {
    match e {
        PersistenceError::ShardOwnershipLost { .. } => {
            ReplicationError::ShardOwnershipLost(format!("{e:?}"))
        }
        PersistenceError::EntityNotExists(m) => ReplicationError::EntityNotExists(m),
        PersistenceError::InternalDataInconsistency(m) => {
            ReplicationError::InternalDataInconsistency(m)
        }
        PersistenceError::InvalidArgument(m) | PersistenceError::InvalidPersistenceRequest(m) => {
            ReplicationError::BadRequest(m)
        }
        PersistenceError::Internal(m) => ReplicationError::Transient(m),
        other => ReplicationError::Other(format!("{other:?}")),
    }
}

fn map_read_error(e: HistoryReadError) -> ReplicationError {
    match e {
        HistoryReadError::EntityNotExists(m) => ReplicationError::EntityNotExists(m),
        HistoryReadError::DataInconsistency(m) => ReplicationError::InternalDataInconsistency(m),
        HistoryReadError::InvalidRequest(m) => ReplicationError::BadRequest(m),
        HistoryReadError::Internal(m) => ReplicationError::Transient(m),
    }
}

/// Converts replication errors into the engine's error type.
pub fn replication_error_to_history_error(e: ReplicationError) -> HistoryError {
    match e {
        ReplicationError::BadRequest(m) => HistoryError::bad_request(m),
        ReplicationError::MissingVersionHistories => HistoryError::MissingVersionHistories,
        ReplicationError::ServiceBusy(m) => {
            ServiceBusyError { message: m, reason: None }.into()
        }
        ReplicationError::EntityNotExists(m) => HistoryError::entity_not_exists(m),
        ReplicationError::InternalDataInconsistency(m) | ReplicationError::Transient(m) => {
            HistoryError::internal(m)
        }
        ReplicationError::ShardOwnershipLost(m) => HistoryError::PersistenceShardOwnershipLost(m),
        ReplicationError::DeadlineExceeded(_) => HistoryError::DeadlineExceeded,
        ReplicationError::RetryTaskV2(e) => retry_task_v2(e),
        other => HistoryError::Other(other.to_string()),
    }
}

fn retry_task_v2(e: RetryTaskV2Error) -> HistoryError {
    HistoryError::Cadence(Box::new(CadenceError::RetryTaskV2(e)))
}

// ---------------------------------------------------------------- ExecutionManager

/// `persistence.ExecutionManager` (replication calls) on the SQL execution store.
pub struct SqlExecutionManager {
    store: Arc<dyn GoExecutionStore>,
}

impl SqlExecutionManager {
    pub fn new(store: Arc<dyn GoExecutionStore>) -> Self {
        Self { store }
    }
}

const REPLICATION_CATEGORY: HistoryTaskCategory =
    HistoryTaskCategory { category_type: 1, category_id: 3 };

fn dlq_info_of(task: &PReplicationTask) -> DlqTaskInfo {
    DlqTaskInfo {
        domain_id: task.domain_id.clone(),
        workflow_id: task.workflow_id.clone(),
        run_id: task.run_id.clone(),
        task_id: task.task_id,
        task_type: task.task_type as i16,
        version: task.version,
        first_event_id: task.first_event_id,
        next_event_id: task.next_event_id,
        scheduled_id: task.scheduled_id,
    }
}

fn to_history_task(task_id: i64, t: PReplicationTask) -> HistoryTask {
    let info = TaskInfo {
        task_id,
        domain_id: t.domain_id,
        workflow_id: t.workflow_id,
        run_id: t.run_id,
        version: t.version,
        visibility_timestamp_nanos: t.creation_time_unix_nanos,
    };
    // persistence.ReplicationTaskTypeHistory=0, SyncActivity=1, FailoverMarker=2
    match t.task_type {
        0 => HistoryTask::HistoryReplication {
            info,
            first_event_id: t.first_event_id,
            next_event_id: t.next_event_id,
            branch_token: t.branch_token,
            new_run_branch_token: t.new_run_branch_token,
        },
        1 => HistoryTask::SyncActivity { info, scheduled_id: t.scheduled_id },
        2 => HistoryTask::FailoverMarker { info },
        _ => HistoryTask::Unknown { info },
    }
}

#[async_trait]
impl ExecutionManager for SqlExecutionManager {
    async fn get_replication_tasks(
        &self,
        shard_id: i32,
        inclusive_min_task_id: i64,
        exclusive_max_task_id: i64,
        page_size: usize,
    ) -> Result<(Vec<HistoryTask>, bool)> {
        let page = self
            .store
            .get_history_task_page(HistoryTaskQuery {
                shard_id,
                category: REPLICATION_CATEGORY,
                min_task_id: inclusive_min_task_id,
                max_task_id: exclusive_max_task_id,
                page_size: page_size as i32,
                ..Default::default()
            })
            .await
            .map_err(map_persistence_error)?;
        let mut tasks = Vec::with_capacity(page.tasks.len());
        for t in page.tasks {
            let value = Encoding::parse(&t.data.encoding)
                .and_then(|e| e.decode(&t.data.data))
                .and_then(|v| replication_task_from_struct(&v))
                .map_err(|e| ReplicationError::InternalDataInconsistency(e.to_string()))?;
            tasks.push(to_history_task(t.task_id, value));
        }
        Ok((tasks, !page.next_page_token.is_empty()))
    }

    async fn range_complete_replication_task(
        &self,
        shard_id: i32,
        exclusive_max_task_id: i64,
        page_size: usize,
    ) -> Result<usize> {
        self.store
            .range_complete_history_task_page(HistoryTaskQuery {
                shard_id,
                category: REPLICATION_CATEGORY,
                max_task_id: exclusive_max_task_id,
                page_size: page_size as i32,
                ..Default::default()
            })
            .await
            .map(|n| n as usize)
            .map_err(map_persistence_error)
    }

    async fn put_replication_task_to_dlq(&self, req: PutReplicationTaskToDlqRequest) -> Result<()> {
        let i = &req.task_info;
        let task = PReplicationTask {
            domain_id: i.domain_id.clone(),
            workflow_id: i.workflow_id.clone(),
            run_id: i.run_id.clone(),
            task_id: i.task_id,
            task_type: i32::from(i.task_type),
            first_event_id: i.first_event_id,
            next_event_id: i.next_event_id,
            version: i.version,
            scheduled_id: i.scheduled_id,
            ..Default::default()
        };
        self.store
            .put_replication_task_to_dlq(req.source_cluster_name, task)
            .await
            .map_err(map_persistence_error)
    }

    async fn get_replication_dlq_size(&self, _shard_id: i32, source_cluster: &str) -> Result<i64> {
        self.store
            .get_replication_dlq_size(source_cluster.to_string())
            .await
            .map_err(map_persistence_error)
    }

    async fn get_replication_tasks_from_dlq(
        &self,
        shard_id: i32,
        source_cluster: &str,
        read_level: i64,
        max_read_level: i64,
        batch_size: usize,
        next_page_token: Vec<u8>,
    ) -> Result<GetReplicationTasksFromDlqResponse> {
        let page = self
            .store
            .get_replication_dlq_page(ReplicationDlqQuery {
                shard_id,
                source_cluster: source_cluster.to_string(),
                read_level,
                max_read_level,
                page_size: batch_size as i32,
                next_page_token,
            })
            .await
            .map_err(map_persistence_error)?;
        Ok(GetReplicationTasksFromDlqResponse {
            // Go returns only ReplicationTaskInfo; the task body is hydrated remotely.
            tasks: page
                .tasks
                .iter()
                .map(|t| ReplicationDlqTask { info: Some(dlq_info_of(t)), task: None })
                .collect(),
            next_page_token: page.next_page_token,
        })
    }

    async fn range_delete_replication_task_from_dlq(
        &self,
        shard_id: i32,
        source_cluster: &str,
        inclusive_begin_task_id: i64,
        exclusive_end_task_id: i64,
    ) -> Result<()> {
        self.store
            .range_delete_replication_dlq_page(
                shard_id,
                source_cluster.to_string(),
                inclusive_begin_task_id,
                exclusive_end_task_id,
                0,
            )
            .await
            .map(|_| ())
            .map_err(map_persistence_error)
    }
}

// ---------------------------------------------------------------- HistoryManager

/// `persistence.HistoryManager.ReadRawHistoryBranch` over a `history_node` store.
pub struct SqlHistoryManager {
    nodes: Arc<dyn HistoryNodeStore>,
}

impl SqlHistoryManager {
    pub fn new(nodes: Arc<dyn HistoryNodeStore>) -> Self {
        Self { nodes }
    }
}

#[async_trait]
impl HistoryManager for SqlHistoryManager {
    async fn read_raw_history_branch(
        &self,
        shard_id: i32,
        _domain_name: &str,
        branch_token: &[u8],
        min_event_id: i64,
        max_event_id: i64,
        page_size: usize,
    ) -> Result<Vec<DataBlob>> {
        let request = ReadHistoryBranchRequest {
            branch_token: branch_token.to_vec(),
            min_event_id,
            max_event_id,
            page_size: page_size as i64,
            next_page_token: Vec::new(),
            shard_id,
        };
        let nodes = self.nodes.clone();
        let resp = tokio::task::block_in_place(|| read_history_branch(nodes.as_ref(), &request))
            .map_err(map_read_error)?;
        Ok(resp
            .raw_blobs
            .into_iter()
            .map(|data| DataBlob::new(EncodingType::ThriftRW, data))
            .collect())
    }
}

// ---------------------------------------------------------------- AckLevelStore

/// `ackLevelStore`: ack levels live in the shard context (persisted with shard info).
pub struct ShardAckLevelStore {
    shard: Arc<dyn ShardContext>,
}

impl ShardAckLevelStore {
    pub fn new(shard: Arc<dyn ShardContext>) -> Self {
        Self { shard }
    }
}

impl AckLevelStore for ShardAckLevelStore {
    fn update_if_needed_and_get_queue_max_read_level(&self, cluster: &str) -> i64 {
        self.shard.update_if_needed_and_get_queue_max_read_level(cluster)
    }
    fn get_queue_cluster_ack_level(&self, cluster: &str) -> i64 {
        self.shard.get_queue_cluster_ack_level(cluster)
    }
    fn update_queue_cluster_ack_level(&self, cluster: &str, ack_level: i64) -> Result<()> {
        self.shard.update_queue_cluster_ack_level(cluster, ack_level)
    }
}

// ---------------------------------------------------------------- TaskReaderApi

/// `taskReader` reading straight from the SQL execution store.
pub struct SqlTaskReader {
    shard_id: i32,
    store: Arc<dyn GoExecutionStore>,
}

impl SqlTaskReader {
    pub fn new(shard_id: i32, store: Arc<dyn GoExecutionStore>) -> Self {
        Self { shard_id, store }
    }
}

#[async_trait]
impl TaskReaderApi for SqlTaskReader {
    async fn read(
        &self,
        read_level: i64,
        max_read_level: i64,
        batch_size: usize,
    ) -> Result<(Vec<HistoryTask>, bool)> {
        if read_level >= max_read_level {
            return Ok((Vec::new(), false));
        }
        SqlExecutionManager::new(self.store.clone())
            .get_replication_tasks(self.shard_id, read_level + 1, max_read_level + 1, batch_size)
            .await
    }
}

// ---------------------------------------------------------------- HistoryProvider

/// `historyProvider` reading event batches from the history store.
pub struct SqlHistoryProvider {
    shard_id: i32,
    history: Arc<dyn HistoryManager>,
    domains: Arc<dyn DomainCache>,
}

impl SqlHistoryProvider {
    pub fn new(
        shard_id: i32,
        history: Arc<dyn HistoryManager>,
        domains: Arc<dyn DomainCache>,
    ) -> Self {
        Self { shard_id, history, domains }
    }

    async fn events_blob(
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
                2, // load more than one to detect data inconsistency
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
impl HistoryProvider for SqlHistoryProvider {
    async fn get_event_blob(&self, task: &HistoryReplicationTaskRef) -> Result<DataBlob> {
        self.events_blob(
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
        self.events_blob(
            &task.info.domain_id,
            &task.new_run_branch_token,
            FIRST_EVENT_ID,
            FIRST_EVENT_ID + 1,
        )
        .await
        .map(Some)
    }
}

// ---------------------------------------------------------------- engine adapters

/// Engine-facing `ReplicationAckManager` over the replication `TaskAckManager`.
pub struct SqlReplicationAckManager {
    inner: Arc<TaskAckManager>,
}

impl SqlReplicationAckManager {
    pub fn new(inner: Arc<TaskAckManager>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ReplicationAckManager for SqlReplicationAckManager {
    async fn get_tasks(&self, cluster: &str, last_read_id: i64) -> HResult<ReplicationMessages> {
        self.inner
            .get_tasks(cluster, last_read_id)
            .await
            .map_err(replication_error_to_history_error)
    }
}

/// Engine-facing `ReplicationDLQHandler` over the replication `DlqHandler`.
pub struct SqlReplicationDlqHandler {
    inner: Arc<dyn DlqHandler>,
}

impl SqlReplicationDlqHandler {
    pub fn new(inner: Arc<dyn DlqHandler>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ReplicationDLQHandler for SqlReplicationDlqHandler {
    async fn get_message_count(&self, force_fetch: bool) -> HResult<HashMap<String, i64>> {
        self.inner
            .get_message_count(force_fetch)
            .await
            .map_err(replication_error_to_history_error)
    }

    async fn read_messages(
        &self,
        cluster: &str,
        end: i64,
        page_size: i32,
        token: &[u8],
    ) -> HResult<DLQRead> {
        self.inner
            .read_messages(cluster, end, page_size.max(0) as usize, token.to_vec())
            .await
            .map_err(replication_error_to_history_error)
    }

    async fn purge_messages(&self, cluster: &str, end: i64) -> HResult<()> {
        self.inner
            .purge_messages(cluster, end)
            .await
            .map_err(replication_error_to_history_error)
    }

    async fn merge_messages(
        &self,
        cluster: &str,
        end: i64,
        page_size: i32,
        token: &[u8],
    ) -> HResult<Vec<u8>> {
        self.inner
            .merge_messages(cluster, end, page_size.max(0) as usize, token.to_vec())
            .await
            .map_err(replication_error_to_history_error)
    }
}
