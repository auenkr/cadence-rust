//! group: shard_events
//!
//! SQL-backed implementations of History's local persistence traits for shards,
//! current-execution reads, history branch reads and the task DLQ manager.
//! Mirrors `go/common/persistence/sql` (shard store, history store) and
//! `go/common/persistence/history_manager.go`.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::service_types::HistoryEvent;
use cadence_persistence::history_contract::{GoExecutionStore, PersistenceError};
use cadence_persistence::history_reader::{
    read_history_branch, HistoryNodeStore, HistoryReadError, ReadHistoryBranchRequest,
};
use cadence_persistence::history_sql::{HistoryPersistenceError, ShardRow, ShardStore as RowShardStore};
use cadence_persistence::sqlblobs::{
    Encoding, ShardInfoRecord, ShardPredicateRecord, ShardProcessingQueueRecord, ShardQueueStateRecord, ShardVirtualSliceRecord,
};

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::{CurrentExecutionReader, HistoryReader};
use crate::events::{EventCacheError, HistoryBranchStore, HistoryEventReader, StoreEventReader};
use crate::execution::integrity::HistoryPageSource;
use crate::shard::{ShardError, ShardInfo, ShardStore};
use crate::shard_context::{
    FailoverMarker, ProcessingQueueState, QueuePredicate, QueueState, ShardContextError, ShardInfoData,
    ShardPersistence, TaskKey, VirtualSliceState,
};
use crate::task::deps::{Result as TaskResult, TaskError};
use crate::taskdlq::processor::{
    HistoryDLQAckLevel, HistoryDLQDeleteTasksRequest, HistoryDLQGetAckLevelsRequest,
    HistoryDLQGetTasksRequest, HistoryDLQGetTasksResponse, HistoryDLQUpdateAckLevelRequest,
    HistoryTaskDLQManager,
};

/// Private sync-to-async bridge (History traits are sync, the stores are async).
fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        // Current-thread runtime (or none): block on a dedicated thread.
        _ => std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("build bridge runtime")
                        .block_on(fut)
                })
                .join()
                .expect("bridge thread panicked")
        }),
    }
}

/// Run a blocking (sync) store call without starving a multi-thread runtime.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Shard row <-> ShardInfoData
// ---------------------------------------------------------------------------

fn encoding_of(row: &ShardRow) -> Result<Encoding, String> {
    let enc = if row.data_encoding.is_empty() { "thriftrw" } else { row.data_encoding.as_str() };
    Encoding::parse(enc).map_err(|e| e.to_string())
}

fn to_pq_records(
    src: &HashMap<String, Vec<ProcessingQueueState>>,
) -> BTreeMap<String, Vec<ShardProcessingQueueRecord>> {
    src.iter()
        .map(|(cluster, list)| {
            let list = list
                .iter()
                .map(|s| ShardProcessingQueueRecord {
                    level: s.level,
                    ack_level: s.ack_level,
                    max_level: s.max_level,
                    domain_ids: Vec::new(),
                    reverse_match: s.domain_filter_reverse_match,
                })
                .collect();
            (cluster.clone(), list)
        })
        .collect()
}

fn from_pq_records(
    src: &BTreeMap<String, Vec<ShardProcessingQueueRecord>>,
) -> HashMap<String, Vec<ProcessingQueueState>> {
    src.iter()
        .map(|(cluster, list)| {
            let list = list
                .iter()
                .map(|s| ProcessingQueueState {
                    level: s.level,
                    ack_level: s.ack_level,
                    max_level: s.max_level,
                    domain_filter_reverse_match: s.reverse_match,
                })
                .collect();
            (cluster.clone(), list)
        })
        .collect()
}

fn to_btree(src: &HashMap<String, i64>) -> BTreeMap<String, i64> {
    src.iter().map(|(k, v)| (k.clone(), *v)).collect()
}

/// Port of `shardManager.toInternalShardInfo` (data blob contents).
fn shard_data_to_record(info: &ShardInfoData) -> ShardInfoRecord {
    ShardInfoRecord {
        stolen_since_renew: info.stolen_since_renew,
        updated_at_nanos: now_nanos(),
        replication_ack_level: info.replication_ack_level,
        transfer_ack_level: info.transfer_ack_level,
        timer_ack_level_nanos: info.timer_ack_level,
        domain_notification_version: info.domain_notification_version,
        cluster_transfer_ack_level: to_btree(&info.cluster_transfer_ack_level),
        cluster_timer_ack_level: to_btree(&info.cluster_timer_ack_level),
        owner: info.owner.clone(),
        cluster_replication_level: to_btree(&info.cluster_replication_level),
        pending_failover_markers: info
            .pending_failover_markers
            .iter()
            .map(|m| (m.domain_id.clone(), m.failover_version, m.creation_time))
            .collect(),
        transfer_processing_queue_states: to_pq_records(&info.transfer_processing_queue_states),
        timer_processing_queue_states: to_pq_records(&info.timer_processing_queue_states),
        queue_states: info
            .queue_states
            .iter()
            .map(|(cat, q)| {
                (
                    *cat,
                    ShardQueueStateRecord {
                        exclusive_max_read_level: (
                            q.exclusive_max_read_level.scheduled_time_nano,
                            q.exclusive_max_read_level.task_id,
                        ),
                        virtual_queue_states: q
                            .virtual_queue_states
                            .iter()
                            .map(|(id, slices)| (*id, slices.iter().map(slice_to_record).collect()))
                            .collect(),
                    },
                )
            })
            .collect(),
    }
}

fn key_to_pair(k: &TaskKey) -> (i64, i64) {
    (k.scheduled_time_nano, k.task_id)
}
fn key_from_pair(k: (i64, i64)) -> TaskKey {
    TaskKey { scheduled_time_nano: k.0, task_id: k.1 }
}
fn slice_to_record(s: &VirtualSliceState) -> ShardVirtualSliceRecord {
    ShardVirtualSliceRecord {
        inclusive_min: key_to_pair(&s.inclusive_min),
        exclusive_max: key_to_pair(&s.exclusive_max),
        predicate: s.predicate.as_ref().map(|p| match p {
            QueuePredicate::Universal => ShardPredicateRecord::Universal,
            QueuePredicate::Empty => ShardPredicateRecord::Empty,
            QueuePredicate::DomainId { domain_ids, is_exclusive } => {
                ShardPredicateRecord::DomainId { domain_ids: domain_ids.clone(), is_exclusive: *is_exclusive }
            }
        }),
    }
}
fn slice_from_record(s: &ShardVirtualSliceRecord) -> VirtualSliceState {
    VirtualSliceState {
        inclusive_min: key_from_pair(s.inclusive_min),
        exclusive_max: key_from_pair(s.exclusive_max),
        predicate: s.predicate.as_ref().map(|p| match p {
            ShardPredicateRecord::Universal => QueuePredicate::Universal,
            ShardPredicateRecord::Empty => QueuePredicate::Empty,
            ShardPredicateRecord::DomainId { domain_ids, is_exclusive } => {
                QueuePredicate::DomainId { domain_ids: domain_ids.clone(), is_exclusive: *is_exclusive }
            }
        }),
    }
}

/// Port of `shardManager.fromInternalShardInfo`.
fn shard_row_to_data(row: &ShardRow) -> Result<ShardInfoData, String> {
    let rec = ShardInfoRecord::decode(&row.data, encoding_of(row)?).map_err(|e| e.to_string())?;
    Ok(ShardInfoData {
        shard_id: row.shard_id,
        owner: rec.owner,
        range_id: row.range_id,
        stolen_since_renew: rec.stolen_since_renew,
        transfer_ack_level: rec.transfer_ack_level,
        timer_ack_level: rec.timer_ack_level_nanos,
        replication_ack_level: rec.replication_ack_level,
        domain_notification_version: rec.domain_notification_version,
        cluster_transfer_ack_level: rec.cluster_transfer_ack_level.into_iter().collect(),
        cluster_timer_ack_level: rec.cluster_timer_ack_level.into_iter().collect(),
        cluster_replication_level: rec.cluster_replication_level.into_iter().collect(),
        queue_states: rec
            .queue_states
            .into_iter()
            .map(|(cat, q)| {
                (
                    cat,
                    QueueState {
                        exclusive_max_read_level: TaskKey {
                            scheduled_time_nano: q.exclusive_max_read_level.0,
                            task_id: q.exclusive_max_read_level.1,
                        },
                        virtual_queue_states: q
                            .virtual_queue_states
                            .into_iter()
                            .map(|(id, slices)| (id, slices.iter().map(slice_from_record).collect()))
                            .collect(),
                    },
                )
            })
            .collect(),
        transfer_processing_queue_states: from_pq_records(&rec.transfer_processing_queue_states),
        timer_processing_queue_states: from_pq_records(&rec.timer_processing_queue_states),
        pending_failover_markers: rec
            .pending_failover_markers
            .into_iter()
            .map(|(domain_id, failover_version, creation_time)| FailoverMarker { domain_id, failover_version, creation_time })
            .collect(),
    })
}

fn data_to_row(info: &ShardInfoData) -> Result<ShardRow, String> {
    let data = shard_data_to_record(info)
        .encode(Encoding::ThriftRW)
        .map_err(|e| e.to_string())?;
    Ok(ShardRow {
        shard_id: info.shard_id,
        range_id: info.range_id,
        data,
        data_encoding: Encoding::ThriftRW.as_str().to_owned(),
    })
}

// ---------------------------------------------------------------------------
// shard.rs ShardStore
// ---------------------------------------------------------------------------

/// SQL `ShardStore` for the shard controller: shard rows with range_id
/// compare-and-swap (`lockShard` + `updateShard` in one transaction).
pub struct SqlShardStore {
    rows: Arc<dyn RowShardStore>,
}

impl SqlShardStore {
    pub fn new(rows: Arc<dyn RowShardStore>) -> Self {
        Self { rows }
    }

    fn info(row: &ShardRow) -> Result<ShardInfo, ShardError> {
        let rec = ShardInfoRecord::decode(&row.data, encoding_of(row).map_err(ShardError::Store)?)
            .map_err(|e| ShardError::Store(e.to_string()))?;
        Ok(ShardInfo { shard_id: row.shard_id as u32, range_id: row.range_id, owner: rec.owner })
    }

    fn map_err(shard_id: u32, e: HistoryPersistenceError, owner: &str) -> ShardError {
        match e {
            HistoryPersistenceError::ShardOwnershipLost { .. } => {
                ShardError::ShardOwnershipLost { shard_id, owner: owner.to_owned() }
            }
            other => ShardError::Store(other.to_string()),
        }
    }
}

impl ShardStore for SqlShardStore {
    fn get_shard(&self, shard_id: u32) -> Result<Option<ShardInfo>, ShardError> {
        match block_on(self.rows.get_shard(shard_id as i32)) {
            Ok(row) => Self::info(&row).map(Some),
            Err(HistoryPersistenceError::EntityNotExists(_)) => Ok(None),
            Err(e) => Err(ShardError::Store(e.to_string())),
        }
    }

    fn create_shard(&self, shard_id: u32, owner: &str) -> Result<ShardInfo, ShardError> {
        let record = ShardInfoRecord { owner: owner.to_owned(), updated_at_nanos: now_nanos(), ..Default::default() };
        let data = record.encode(Encoding::ThriftRW).map_err(|e| ShardError::Store(e.to_string()))?;
        let row = ShardRow {
            shard_id: shard_id as i32,
            range_id: 1,
            data,
            data_encoding: Encoding::ThriftRW.as_str().to_owned(),
        };
        match block_on(self.rows.create_shard(row.clone())) {
            Ok(()) => Self::info(&row),
            // Lost the creation race: Go re-reads the existing shard.
            Err(HistoryPersistenceError::AlreadyExists(_)) => match block_on(self.rows.get_shard(shard_id as i32)) {
                Ok(existing) => Self::info(&existing),
                Err(e) => Err(ShardError::Store(e.to_string())),
            },
            Err(e) => Err(ShardError::Store(e.to_string())),
        }
    }

    fn update_shard(&self, shard_id: u32, prev_range_id: i64, owner: &str) -> Result<ShardInfo, ShardError> {
        let current = block_on(self.rows.get_shard(shard_id as i32))
            .map_err(|e| ShardError::Store(e.to_string()))?;
        let mut rec = ShardInfoRecord::decode(&current.data, encoding_of(&current).map_err(ShardError::Store)?)
            .map_err(|e| ShardError::Store(e.to_string()))?;
        if current.range_id != prev_range_id {
            return Err(ShardError::ShardOwnershipLost { shard_id, owner: rec.owner });
        }
        let previous_owner = rec.owner.clone();
        rec.owner = owner.to_owned();
        rec.updated_at_nanos = now_nanos();
        let data = rec.encode(Encoding::ThriftRW).map_err(|e| ShardError::Store(e.to_string()))?;
        let row = ShardRow {
            shard_id: shard_id as i32,
            range_id: prev_range_id + 1,
            data,
            data_encoding: Encoding::ThriftRW.as_str().to_owned(),
        };
        block_on(self.rows.update_shard(row.clone(), prev_range_id))
            .map_err(|e| Self::map_err(shard_id, e, &previous_owner))?;
        Self::info(&row)
    }
}

// ---------------------------------------------------------------------------
// shard_context.rs ShardPersistence
// ---------------------------------------------------------------------------

/// `ShardManager.UpdateShard` over the SQL `shards` table.
pub struct SqlShardPersistence {
    rows: Arc<dyn RowShardStore>,
}

impl SqlShardPersistence {
    pub fn new(rows: Arc<dyn RowShardStore>) -> Self {
        Self { rows }
    }

    /// `ShardManager.GetShard` (used by shard acquisition).
    pub fn get_shard(&self, shard_id: i32) -> Result<ShardInfoData, ShardContextError> {
        let row = block_on(self.rows.get_shard(shard_id)).map_err(|e| ShardContextError::Persistence(e.to_string()))?;
        shard_row_to_data(&row).map_err(ShardContextError::Persistence)
    }
}

impl ShardPersistence for SqlShardPersistence {
    fn update_shard(&self, info: &ShardInfoData, previous_range_id: i64) -> Result<(), ShardContextError> {
        let row = data_to_row(info).map_err(ShardContextError::Persistence)?;
        block_on(self.rows.update_shard(row, previous_range_id)).map_err(|e| match e {
            HistoryPersistenceError::ShardOwnershipLost { .. } => ShardContextError::ShardOwnershipLost(e.to_string()),
            other => ShardContextError::Persistence(other.to_string()),
        })
    }
}

// ---------------------------------------------------------------------------
// engine/runtime.rs CurrentExecutionReader and HistoryReader
// ---------------------------------------------------------------------------

fn map_persistence_error(e: PersistenceError) -> HistoryError {
    match e {
        PersistenceError::EntityNotExists(m) => HistoryError::entity_not_exists(m),
        PersistenceError::ShardOwnershipLost { shard_id, expected_range_id, actual_range_id } => {
            HistoryError::PersistenceShardOwnershipLost {
                shard_id: Some(shard_id),
                message: format!("Failed to update shard {shard_id}. Previous range ID: {expected_range_id}; new range ID: {actual_range_id}"),
            }
        }
        PersistenceError::InvalidArgument(m) | PersistenceError::InvalidPersistenceRequest(m) => HistoryError::bad_request(m),
        other => HistoryError::internal(format!("{other:?}")),
    }
}

/// `ExecutionManager.GetCurrentExecution` reader (returns the current run ID).
pub struct SqlCurrentExecutionReader {
    store: Arc<dyn GoExecutionStore>,
    num_history_shards: u32,
}

impl SqlCurrentExecutionReader {
    pub fn new(store: Arc<dyn GoExecutionStore>, num_history_shards: u32) -> Self {
        Self { store, num_history_shards }
    }
}

#[async_trait]
impl CurrentExecutionReader for SqlCurrentExecutionReader {
    async fn get_current_execution(&self, domain_id: &str, workflow_id: &str, _domain_name: &str) -> HResult<String> {
        let shard_id = crate::shard::ShardController::workflow_id_to_shard(
            workflow_id,
            self.num_history_shards,
        ) as i32;
        let current = self
            .store
            .get_current_execution(shard_id, domain_id.to_owned(), workflow_id.to_owned())
            .await
            .map_err(map_persistence_error)?;
        Ok(current.run_id)
    }
}

fn map_read_error(e: HistoryReadError) -> HistoryError {
    match e {
        HistoryReadError::EntityNotExists(m) => HistoryError::entity_not_exists(m),
        HistoryReadError::InvalidRequest(m) => HistoryError::bad_request(m),
        HistoryReadError::DataInconsistency(m) | HistoryReadError::Internal(m) => HistoryError::internal(m),
    }
}

/// `HistoryManager.ReadHistoryBranchByBatch` for one shard.
pub struct SqlHistoryReader {
    shard_id: i32,
    store: Arc<dyn HistoryNodeStore>,
}

impl SqlHistoryReader {
    pub fn new(shard_id: i32, store: Arc<dyn HistoryNodeStore>) -> Self {
        Self { shard_id, store }
    }
}

#[async_trait]
impl HistoryReader for SqlHistoryReader {
    async fn paginate_history(
        &self,
        first: i64,
        next: i64,
        branch: &[u8],
        token: &[u8],
        page_size: i32,
        _domain: &str,
    ) -> HResult<(Vec<Vec<HistoryEvent>>, Vec<u8>)> {
        let request = ReadHistoryBranchRequest {
            branch_token: branch.to_vec(),
            min_event_id: first,
            max_event_id: next,
            page_size: i64::from(page_size),
            next_page_token: token.to_vec(),
            shard_id: self.shard_id,
        };
        let store = self.store.clone();
        let response = tokio::task::spawn_blocking(move || read_history_branch(store.as_ref(), &request))
            .await
            .map_err(|e| HistoryError::internal(e.to_string()))?
            .map_err(map_read_error)?;
        Ok((response.batches, response.next_page_token))
    }
}

// ---------------------------------------------------------------------------
// events/mod.rs HistoryBranchStore and HistoryEventReader
// ---------------------------------------------------------------------------

/// Domain ID to name lookup (domain cache boundary, supplied by the resources unit).
pub type DomainNameLookup = Arc<dyn Fn(&str) -> Result<String, String> + Send + Sync>;

/// `HistoryManager.ReadHistoryBranch` for the event cache (one page, flattened).
pub struct SqlHistoryBranchStore {
    store: Arc<dyn HistoryNodeStore>,
    domain_name: DomainNameLookup,
}

impl SqlHistoryBranchStore {
    pub fn new(store: Arc<dyn HistoryNodeStore>, domain_name: DomainNameLookup) -> Self {
        Self { store, domain_name }
    }
}

impl HistoryBranchStore for SqlHistoryBranchStore {
    fn domain_name(&self, domain_id: &str) -> Result<String, EventCacheError> {
        (self.domain_name)(domain_id).map_err(EventCacheError)
    }

    fn read_history_branch(
        &self,
        shard_id: i32,
        _domain_name: &str,
        branch_token: &[u8],
        min_event_id: i64,
        max_event_id: i64,
        page_size: usize,
    ) -> Result<Vec<HistoryEvent>, EventCacheError> {
        let request = ReadHistoryBranchRequest {
            branch_token: branch_token.to_vec(),
            min_event_id,
            max_event_id,
            page_size: page_size as i64,
            next_page_token: Vec::new(),
            shard_id,
        };
        blocking(|| read_history_branch(self.store.as_ref(), &request))
            .map(|r| r.events())
            .map_err(|e| EventCacheError(e.to_string()))
    }
}

/// Event cache reader over SQL history (`StoreEventReader<SqlHistoryBranchStore>`).
pub struct SqlHistoryEventReader(StoreEventReader<SqlHistoryBranchStore>);

impl SqlHistoryEventReader {
    pub fn new(store: Arc<dyn HistoryNodeStore>, domain_name: DomainNameLookup) -> Self {
        Self(StoreEventReader(SqlHistoryBranchStore::new(store, domain_name)))
    }
}

impl HistoryEventReader for SqlHistoryEventReader {
    fn read_event(
        &self,
        shard_id: i32,
        domain_id: &str,
        first_event_id: i64,
        event_id: i64,
        branch_token: &[u8],
    ) -> Result<HistoryEvent, EventCacheError> {
        self.0.read_event(shard_id, domain_id, first_event_id, event_id, branch_token)
    }
}

// ---------------------------------------------------------------------------
// execution/integrity.rs HistoryPageSource
// ---------------------------------------------------------------------------

/// Pages one history branch from event 1 (preserves transaction-chain validation).
pub struct SqlHistoryPageSource {
    store: Arc<dyn HistoryNodeStore>,
    shard_id: i32,
    branch_token: Vec<u8>,
    page_size: i64,
}

impl SqlHistoryPageSource {
    pub fn new(store: Arc<dyn HistoryNodeStore>, shard_id: i32, branch_token: Vec<u8>, page_size: i64) -> Self {
        Self { store, shard_id, branch_token, page_size }
    }
}

impl HistoryPageSource for SqlHistoryPageSource {
    type Error = HistoryReadError;

    fn page(&mut self, first_event_id: i64, next_event_id: i64, token: Vec<u8>) -> Result<(Vec<HistoryEvent>, Vec<u8>), Self::Error> {
        let request = ReadHistoryBranchRequest {
            branch_token: self.branch_token.clone(),
            min_event_id: first_event_id,
            max_event_id: next_event_id,
            page_size: self.page_size,
            next_page_token: token,
            shard_id: self.shard_id,
        };
        let response = blocking(|| read_history_branch(self.store.as_ref(), &request))?;
        Ok((response.events(), response.next_page_token))
    }
}

// ---------------------------------------------------------------------------
// taskdlq HistoryTaskDLQManager
// ---------------------------------------------------------------------------

/// Go only ships a NoSQL `HistoryTaskDLQ` store; the SQL factory returns
/// `ErrNotSupported`-style errors for the history task DLQ, so every method
/// fails explicitly (identical observable behavior to Go on SQL).
#[derive(Default)]
pub struct SqlHistoryTaskDLQManager;

fn dlq_unsupported<T>() -> TaskResult<T> {
    Err(TaskError::Other("history task DLQ is not supported by the SQL persistence plugin".into()))
}

#[async_trait]
impl HistoryTaskDLQManager for SqlHistoryTaskDLQManager {
    async fn get_history_dlq_ack_levels(&self, _req: HistoryDLQGetAckLevelsRequest) -> TaskResult<Vec<HistoryDLQAckLevel>> {
        dlq_unsupported()
    }
    async fn get_history_dlq_tasks(&self, _req: HistoryDLQGetTasksRequest) -> TaskResult<HistoryDLQGetTasksResponse> {
        dlq_unsupported()
    }
    async fn update_history_dlq_ack_level(&self, _req: HistoryDLQUpdateAckLevelRequest) -> TaskResult<()> {
        dlq_unsupported()
    }
    async fn delete_history_dlq_tasks(&self, _req: HistoryDLQDeleteTasksRequest) -> TaskResult<()> {
        dlq_unsupported()
    }
}
