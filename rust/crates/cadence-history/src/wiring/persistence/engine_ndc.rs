//! group: engine_ndc
//!
//! SQL-backed implementations of the persistence-facing traits of the
//! `execution` and `ndc` modules, built on `cadence-persistence`
//! (`GoExecutionStore`, `GoHistoryV2Store`, `HistoryNodeStore`).
//!
//! Sync traits bridge to the async stores with [`block_on`]
//! (`block_in_place` + `Handle::block_on`, so a multi-thread runtime is required).

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use cadence_api::errors::{BadRequestError, ServiceBusyError};
use cadence_api::service_types::HistoryEvent;
use cadence_persistence::history_contract::{
    ConflictResolveWorkflowExecutionRequest, ConflictResolveWorkflowMode, CreateWorkflowExecutionRequest,
    CreateWorkflowMode, DataBlob, GoExecutionStore, GoHistoryV2Store, HistoryBranchRange, HistoryBranchRequest,
    HistoryTaskCategory, PersistenceError, Task, UpdateWorkflowExecutionRequest, UpdateWorkflowMode,
    WorkflowMutation, WorkflowRequest, WorkflowRequestMode, WorkflowSnapshot,
};
use cadence_persistence::history_reader::{read_history_branch, HistoryNodeStore, ReadHistoryBranchRequest};

use crate::engine::deps::{DomainCache, HResult, HistoryError, PersistenceAlreadyStarted};
use crate::execution::cache::ExecutionCacheBackend;
use crate::execution::context::{
    ConflictContextStore, PersistedBlob, TransactionState, WorkflowContextStore, WorkflowEvents,
    WorkflowExecutionKey,
};
use crate::execution::state_rebuilder::HistoryBatchPage;
use crate::ndc::branch_manager::BranchManagerEnv;
use crate::ndc::transaction_manager::TransactionManagerEnv;
use crate::ndc::workflow_resetter::WorkflowResetterEnv;
use crate::ndc::{NdcError, NdcMutableState, NdcResult, NdcWorkflow};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Bridge an async store call into a sync trait method.
pub fn block_on<F: Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// `persistence` error -> History error (Go keeps the typed persistence errors).
pub fn to_history_error(err: PersistenceError) -> HistoryError {
    match err {
        PersistenceError::ShardOwnershipLost { .. } => HistoryError::PersistenceShardOwnershipLost(err.to_string()),
        PersistenceError::ConditionFailed(m) => HistoryError::ConditionFailed(m),
        PersistenceError::WorkflowAlreadyStarted { message, start_request_id, run_id, state, close_status, last_write_version } => {
            HistoryError::PersistenceAlreadyStarted(Box::new(PersistenceAlreadyStarted {
                msg: message,
                start_request_id,
                run_id,
                state,
                close_status,
                last_write_version,
            }))
        }
        PersistenceError::CurrentWorkflowConditionFailed(m) => HistoryError::CurrentWorkflowConditionFailed(m),
        PersistenceError::EntityNotExists(m) => HistoryError::entity_not_exists(m),
        PersistenceError::AlreadyExists(m) => HistoryError::Other(m),
        PersistenceError::InvalidPersistenceRequest(m) | PersistenceError::InvalidArgument(m) => HistoryError::bad_request(m),
        PersistenceError::InternalDataInconsistency(m) | PersistenceError::Internal(m) => HistoryError::internal(m),
        PersistenceError::Timeout(m) => HistoryError::internal(m),
        PersistenceError::ServiceBusy(m) => HistoryError::internal(m),
    }
}

/// `persistence` error -> ndc error.
pub fn to_ndc_error(err: PersistenceError) -> NdcError {
    match err {
        PersistenceError::EntityNotExists(m) => NdcError::EntityNotExists(m),
        PersistenceError::WorkflowAlreadyStarted { message, .. } => NdcError::WorkflowExecutionAlreadyStarted(message),
        PersistenceError::InvalidPersistenceRequest(m) | PersistenceError::InvalidArgument(m) => NdcError::bad_request(m),
        PersistenceError::InternalDataInconsistency(m) | PersistenceError::Internal(m) => NdcError::internal(m),
        other => NdcError::Other(other.to_string()),
    }
}

fn history_to_ndc(err: HistoryError) -> NdcError {
    if err.is_entity_not_exists() {
        return NdcError::EntityNotExists(err.to_string());
    }
    NdcError::Other(err.to_string())
}

fn is_transient(err: &PersistenceError) -> bool {
    matches!(err, PersistenceError::Internal(_))
}

/// Go `persistenceOperationRetryPolicy`: exponential backoff, transient errors only.
async fn retry_transient<T, F, Fut>(mut op: F) -> Result<T, PersistenceError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, PersistenceError>>,
{
    let mut delay = Duration::from_millis(50);
    let mut attempt = 0;
    loop {
        match op().await {
            Err(err) if is_transient(&err) && attempt < 4 => {
                attempt += 1;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(1));
            }
            other => return other,
        }
    }
}

/// Current shard range id (Go `shard.GetRangeID`).
pub type RangeIdSource = Arc<dyn Fn() -> i64 + Send + Sync>;
/// Go `shard.GenerateTransferTaskID`.
pub type TransactionIdSource = Arc<dyn Fn() -> HResult<i64> + Send + Sync>;

// ---------------------------------------------------------------------------
// history manager (Go historyV2ManagerImpl: ForkHistoryBranch / AppendHistoryNodes)
// ---------------------------------------------------------------------------

/// Go `persistence.HistoryManager` over a [`GoHistoryV2Store`] for one shard.
#[derive(Clone)]
pub struct SqlHistoryManager {
    pub store: Arc<dyn GoHistoryV2Store>,
    pub shard_id: i32,
}

pub(crate) fn branch_request(shard_id: i32, token: &[u8]) -> Result<HistoryBranchRequest, PersistenceError> {
    let branch = cadence_persistence::history_branch::decode(token)
        .map_err(|e| PersistenceError::InvalidPersistenceRequest(e.to_string()))?;
    Ok(HistoryBranchRequest {
        shard_id,
        tree_id: branch.tree_id,
        branch_id: branch.branch_id,
        ancestors: branch
            .ancestors
            .into_iter()
            .map(|r| HistoryBranchRange { branch_id: r.branch_id, begin_node_id: r.begin_node_id, end_node_id: r.end_node_id })
            .collect(),
        ..Default::default()
    })
}

impl SqlHistoryManager {
    pub fn new(store: Arc<dyn GoHistoryV2Store>, shard_id: i32) -> Self {
        Self { store, shard_id }
    }

    /// Go `ForkHistoryBranch`: returns the new branch token.
    pub async fn fork_history_branch(&self, fork_token: &[u8], fork_node_id: i64, info: &str) -> Result<Vec<u8>, PersistenceError> {
        if fork_node_id <= 1 {
            return Err(PersistenceError::InvalidPersistenceRequest("ForkNodeID must be > 1".into()));
        }
        let mut request = branch_request(self.shard_id, fork_token)?;
        request.info = info.to_string();
        request.current_timestamp_unix_nanos = now_unix_nanos();
        let new_branch_id = uuid::Uuid::new_v4().to_string();
        let forked = self.store.fork_history_branch(request, fork_node_id, new_branch_id).await?;
        let token = cadence_persistence::history_branch::encode(&cadence_api::types::HistoryBranch {
            tree_id: forked.tree_id,
            branch_id: forked.branch_id,
            ancestors: forked
                .ancestors
                .into_iter()
                .map(|r| cadence_api::types::HistoryBranchRange { branch_id: r.branch_id, begin_node_id: r.begin_node_id, end_node_id: r.end_node_id })
                .collect(),
        });
        Ok(token)
    }

    /// Go `AppendHistoryNodes`: serializes the batch (ThriftRW) and appends it.
    pub async fn append_history_nodes(
        &self,
        token: &[u8],
        events: &[HistoryEvent],
        transaction_id: i64,
        new_branch: bool,
        info: &str,
    ) -> Result<DataBlob, PersistenceError> {
        let first = events
            .first()
            .ok_or_else(|| PersistenceError::InvalidPersistenceRequest("events to be appended cannot be empty".into()))?;
        if first.event_id <= 0 {
            return Err(PersistenceError::InvalidPersistenceRequest("eventID cannot be less than 1".into()));
        }
        let encoded = cadence_thrift::history_blob::encode(events.to_vec());
        let blob = DataBlob { encoding: "thriftrw".to_string(), data: encoded.data };
        let mut request = branch_request(self.shard_id, token)?;
        request.node_id = first.event_id;
        request.transaction_id = transaction_id;
        request.data = blob.clone();
        request.is_new_branch = new_branch;
        if new_branch {
            request.info = info.to_string();
        }
        request.current_timestamp_unix_nanos = now_unix_nanos();
        retry_transient(|| self.store.append_history_nodes(request.clone())).await?;
        Ok(blob)
    }
}

// ---------------------------------------------------------------------------
// BranchManagerEnv / WorkflowResetterEnv
// ---------------------------------------------------------------------------

/// Implements [`BranchManagerEnv`].
pub struct SqlBranchManagerEnv {
    pub history: SqlHistoryManager,
    pub domains: Arc<dyn DomainCache>,
}

impl BranchManagerEnv for SqlBranchManagerEnv {
    fn get_domain_name(&self, domain_id: &str) -> NdcResult<String> {
        self.domains.get_domain_name(domain_id).map_err(history_to_ndc)
    }
    fn fork_history_branch(&self, token: &[u8], node_id: i64, info: &str, _domain_name: &str) -> NdcResult<Vec<u8>> {
        block_on(self.history.fork_history_branch(token, node_id, info)).map_err(to_ndc_error)
    }
    fn now_nanos(&self) -> i64 {
        now_unix_nanos()
    }
}

/// Implements [`WorkflowResetterEnv`].
pub struct SqlWorkflowResetterEnv {
    pub history: SqlHistoryManager,
    pub domains: Arc<dyn DomainCache>,
}

impl WorkflowResetterEnv for SqlWorkflowResetterEnv {
    fn get_domain_name(&self, domain_id: &str) -> NdcResult<String> {
        self.domains.get_domain_name(domain_id).map_err(history_to_ndc)
    }
    fn fork_history_branch(&self, token: &[u8], node_id: i64, info: &str, _domain_name: &str) -> NdcResult<Vec<u8>> {
        block_on(self.history.fork_history_branch(token, node_id, info)).map_err(to_ndc_error)
    }
}

// ---------------------------------------------------------------------------
// TransactionManagerEnv
// ---------------------------------------------------------------------------

/// Non-persistence collaborators of Go `transactionManagerImpl`
/// (cluster metadata, active-cluster manager, workflow loading, reset package).
pub trait TransactionManagerCollaborators: Send + Sync {
    fn current_cluster_name(&self) -> String;
    fn active_cluster_name(&self, ms: &dyn NdcMutableState) -> NdcResult<String>;
    fn load_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<Box<dyn NdcWorkflow>>;
    #[allow(clippy::too_many_arguments)]
    fn reset_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        base_run_id: &str,
        base_branch_token: &[u8],
        base_rebuild_last_event_id: i64,
        base_rebuild_last_event_version: i64,
        base_next_event_id: i64,
        reset_run_id: &str,
        reset_request_id: &str,
        current: &mut dyn NdcWorkflow,
        reset_reason: &str,
        additional_reapply_events: &[HistoryEvent],
        skip_signal_reapply: bool,
    ) -> NdcResult<()>;
    fn inc_reapply_events_skipped(&self) {}
}

/// Implements [`TransactionManagerEnv`]; persistence reads go to SQL.
pub struct SqlTransactionManagerEnv {
    pub executions: Arc<dyn GoExecutionStore>,
    pub domains: Arc<dyn DomainCache>,
    pub shard_id: i32,
    pub range_id: RangeIdSource,
    pub collaborators: Arc<dyn TransactionManagerCollaborators>,
}

impl TransactionManagerEnv for SqlTransactionManagerEnv {
    fn get_domain_name(&self, domain_id: &str) -> NdcResult<String> {
        self.domains.get_domain_name(domain_id).map_err(history_to_ndc)
    }
    fn get_workflow_execution(&self, domain_id: &str, _domain_name: &str, workflow_id: &str, run_id: &str) -> NdcResult<()> {
        block_on(retry_transient(|| {
            self.executions.get_workflow_execution(
                self.shard_id,
                domain_id.to_string(),
                workflow_id.to_string(),
                run_id.to_string(),
                (self.range_id)(),
            )
        }))
        .map(|_| ())
        .map_err(to_ndc_error)
    }
    fn get_current_execution(&self, domain_id: &str, _domain_name: &str, workflow_id: &str) -> NdcResult<String> {
        block_on(retry_transient(|| {
            self.executions.get_current_execution(self.shard_id, domain_id.to_string(), workflow_id.to_string())
        }))
        .map(|c| c.run_id)
        .map_err(to_ndc_error)
    }
    fn get_current_cluster_name(&self) -> String {
        self.collaborators.current_cluster_name()
    }
    fn get_active_cluster_name(&self, ms: &dyn NdcMutableState) -> NdcResult<String> {
        self.collaborators.active_cluster_name(ms)
    }
    fn load_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<Box<dyn NdcWorkflow>> {
        self.collaborators.load_workflow(domain_id, workflow_id, run_id)
    }
    fn reset_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        base_run_id: &str,
        base_branch_token: &[u8],
        base_rebuild_last_event_id: i64,
        base_rebuild_last_event_version: i64,
        base_next_event_id: i64,
        reset_run_id: &str,
        reset_request_id: &str,
        current: &mut dyn NdcWorkflow,
        reset_reason: &str,
        additional_reapply_events: &[HistoryEvent],
        skip_signal_reapply: bool,
    ) -> NdcResult<()> {
        self.collaborators.reset_workflow(
            domain_id,
            workflow_id,
            base_run_id,
            base_branch_token,
            base_rebuild_last_event_id,
            base_rebuild_last_event_version,
            base_next_event_id,
            reset_run_id,
            reset_request_id,
            current,
            reset_reason,
            additional_reapply_events,
            skip_signal_reapply,
        )
    }
    fn inc_reapply_events_skipped(&self) {
        self.collaborators.inc_reapply_events_skipped()
    }
}

// ---------------------------------------------------------------------------
// state rebuilder history paging (RebuildContext::page)
// ---------------------------------------------------------------------------

/// Go `persistence.HistoryManager.ReadHistoryBranchByBatch` used by `execution.StateRebuilder`.
pub struct SqlHistoryPager {
    pub nodes: Arc<dyn HistoryNodeStore>,
    pub shard_id: i32,
}

impl SqlHistoryPager {
    /// Same contract as `RebuildContext::page`; an `Err` is a message for the caller to wrap.
    pub fn page(&self, branch: &[u8], first: i64, next: i64, token: Vec<u8>, page_size: usize) -> Result<HistoryBatchPage, HistoryError> {
        let nodes = self.nodes.clone();
        let request = ReadHistoryBranchRequest {
            branch_token: branch.to_vec(),
            min_event_id: first,
            max_event_id: next,
            page_size: page_size as i64,
            next_page_token: token,
            shard_id: self.shard_id,
        };
        let response = tokio::task::block_in_place(|| read_history_branch(nodes.as_ref(), &request))
            .map_err(|e| HistoryError::Other(e.to_string()))?;
        Ok(HistoryBatchPage { batches: response.batches, next_token: response.next_page_token, size: response.size })
    }
}

// ---------------------------------------------------------------------------
// WorkflowContextStore / ConflictContextStore
// ---------------------------------------------------------------------------

/// Errors the SQL context store can produce and inspect.
pub trait StoreError: From<HistoryError> {
    fn history_error(&self) -> Option<&HistoryError>;
}
impl StoreError for HistoryError {
    fn history_error(&self) -> Option<&HistoryError> {
        Some(self)
    }
}

/// Non-persistence half of `WorkflowContextStore` (clock, metrics, repair,
/// notifications, replication-task merging, event reapplication).
#[allow(async_fn_in_trait)]
pub trait WorkflowContextHooks<M: TransactionState> {
    fn now_nanos(&self) -> i64;
    fn strong_idempotency_check(&self, domain: &str) -> bool;
    fn request_type(&self, request: &WorkflowRequest) -> i32;
    fn log_request_validation_error(&mut self, message: &'static str);
    fn cleared(&mut self);
    /// Build the mutable state from the persisted snapshot (Go `NewMutableStateBuilder...` + Load).
    fn build_state(&mut self, snapshot: WorkflowSnapshot, domain: &str) -> Result<M, M::Error>;
    async fn verify_and_repair(&mut self, state: &mut M) -> Result<bool, M::Error>;
    async fn flush_as_active(&mut self, state: &mut M, now: i64) -> Result<(), M::Error>;
    fn notify_snapshot(&mut self, snapshot: Option<&WorkflowSnapshot>, history: &[PersistedBlob], persistence_error: bool);
    fn notify_mutation(&mut self, mutation: &WorkflowMutation, history: &[PersistedBlob], persistence_error: bool);
    fn notify_history(&mut self, state: &M);
    fn emit_loaded_stats(&mut self, domain: &str, state: &M);
    fn emit_session_stats(&mut self, domain: &str);
    fn emit_history_stats(&mut self, domain: &str, size: i64, count: i64);
    fn emit_large_workflow_stats(&mut self, domain: &str, blob_size: i64, old_count: i64, old_size: i64, new_count: i64);
    fn emit_completion_stats(&mut self, domain: &str, state: &M);
    fn merge_continue_as_new_replication_tasks(
        &mut self,
        mode: UpdateWorkflowMode,
        mutation: &mut WorkflowMutation,
        new_workflow: Option<&mut WorkflowSnapshot>,
    ) -> Result<(), M::Error>;
    async fn reapply_events(&mut self, batches: &[WorkflowEvents]) -> Result<(), M::Error>;
    /// Go shard `allocateTransferIDs` / `allocateTimerIDs`: assigns task IDs
    /// (and visibility timestamps) to the encoded tasks of one write image
    /// before the write. The hook remembers what to publish after commit.
    fn allocate_task_ids(&mut self, tasks: &mut BTreeMap<HistoryTaskCategory, Vec<Task>>) -> Result<(), M::Error>;
    /// Go holds the shard lock from task-id allocation until the write and the
    /// read-level update finish: acquire it before `allocate_task_ids`; it is
    /// released by `tasks_committed` / `write_failed`.
    async fn begin_task_write(&mut self) {}
    /// The write is durable: advance max read level and notify queues.
    fn tasks_committed(&mut self);
    /// The write failed: drop the pending allocation (handle ownership loss).
    fn write_failed(&mut self, error: &M::Error);
}

/// Implements [`WorkflowContextStore`] and [`ConflictContextStore`] on SQL stores.
pub struct SqlWorkflowContextStore<M, H> {
    pub executions: Arc<dyn GoExecutionStore>,
    pub history: SqlHistoryManager,
    pub domains: Arc<dyn DomainCache>,
    pub shard_id: i32,
    pub range_id: RangeIdSource,
    pub transaction_ids: TransactionIdSource,
    pub hooks: H,
    _state: PhantomData<fn() -> M>,
}

impl<M, H> SqlWorkflowContextStore<M, H> {
    pub fn new(
        executions: Arc<dyn GoExecutionStore>,
        history: SqlHistoryManager,
        domains: Arc<dyn DomainCache>,
        range_id: RangeIdSource,
        transaction_ids: TransactionIdSource,
        hooks: H,
    ) -> Self {
        let shard_id = history.shard_id;
        Self { executions, history, domains, shard_id, range_id, transaction_ids, hooks, _state: PhantomData }
    }
}

impl<M, H> WorkflowContextStore<M> for SqlWorkflowContextStore<M, H>
where
    M: TransactionState,
    M::Error: StoreError,
    H: WorkflowContextHooks<M>,
{
    fn now_nanos(&self) -> i64 {
        self.hooks.now_nanos()
    }
    fn domain_name(&self, domain_id: &str) -> Result<String, M::Error> {
        self.domains.get_domain_name(domain_id).map_err(M::Error::from)
    }
    fn internal_error(&self, message: &'static str) -> M::Error {
        HistoryError::internal(message).into()
    }
    /// Go: timeouts may have been applied by the database.
    fn possibly_successful(&self, error: &M::Error) -> bool {
        error.history_error().is_some_and(|e| e.is_timeout())
    }
    fn strong_idempotency_check(&self, domain: &str) -> bool {
        self.hooks.strong_idempotency_check(domain)
    }
    fn request_type(&self, request: &WorkflowRequest) -> i32 {
        self.hooks.request_type(request)
    }
    fn log_request_validation_error(&mut self, message: &'static str) {
        self.hooks.log_request_validation_error(message)
    }
    fn cleared(&mut self) {
        self.hooks.cleared()
    }
    async fn get_workflow_with_retry(&mut self, key: &WorkflowExecutionKey, domain: &str) -> Result<M, M::Error> {
        let range_id = (self.range_id)();
        let snapshot = retry_transient(|| {
            self.executions.get_workflow_execution(
                self.shard_id,
                key.domain_id.clone(),
                key.workflow_id.clone(),
                key.run_id.clone(),
                range_id,
            )
        })
        .await
        .map_err(|e| M::Error::from(to_history_error(e)))?;
        self.hooks.build_state(snapshot, domain)
    }
    async fn verify_and_repair(&mut self, state: &mut M) -> Result<bool, M::Error> {
        self.hooks.verify_and_repair(state).await
    }
    async fn flush_as_active(&mut self, state: &mut M, now: i64) -> Result<(), M::Error> {
        self.hooks.flush_as_active(state, now).await
    }
    async fn append_history_with_retry(
        &mut self,
        events: &WorkflowEvents,
        _domain: &str,
        new_branch: bool,
        cleanup_info: Option<String>,
    ) -> Result<DataBlob, M::Error> {
        let transaction_id = (self.transaction_ids)().map_err(M::Error::from)?;
        self.history
            .append_history_nodes(
                &events.branch_token,
                &events.events,
                transaction_id,
                new_branch,
                cleanup_info.as_deref().unwrap_or_default(),
            )
            .await
            .map_err(|e| M::Error::from(to_history_error(e)))
    }
    async fn create_with_retry(
        &mut self,
        _domain: &str,
        snapshot: &WorkflowSnapshot,
        mode: CreateWorkflowMode,
        previous_run: &str,
        previous_version: i64,
        requests: WorkflowRequestMode,
    ) -> Result<(), M::Error> {
        // Go does not retry create (a timeout may have succeeded).
        let mut snapshot = snapshot.clone();
        self.hooks.begin_task_write().await;
        if let Err(e) = self.hooks.allocate_task_ids(&mut snapshot.tasks_by_category) {
            self.hooks.write_failed(&e);
            return Err(e);
        }
        let request = CreateWorkflowExecutionRequest {
            shard_id: self.shard_id,
            range_id: (self.range_id)(),
            mode,
            previous_run_id: previous_run.to_string(),
            previous_last_write_version: previous_version,
            new_workflow_snapshot: snapshot,
            workflow_request_mode: requests,
            current_timestamp_unix_nanos: now_unix_nanos(),
        };
        match self.executions.create_workflow_execution(request).await {
            Ok(_) => {
                self.hooks.tasks_committed();
                Ok(())
            }
            Err(e) => {
                let e = M::Error::from(to_history_error(e));
                self.hooks.write_failed(&e);
                Err(e)
            }
        }
    }
    async fn update_with_retry(
        &mut self,
        _domain: &str,
        mutation: &WorkflowMutation,
        new_workflow: Option<&WorkflowSnapshot>,
        mode: UpdateWorkflowMode,
        requests: WorkflowRequestMode,
    ) -> Result<(), M::Error> {
        let mut mutation = mutation.clone();
        let mut new_snapshot = new_workflow.cloned();
        self.hooks.begin_task_write().await;
        let allocated = self.hooks.allocate_task_ids(&mut mutation.tasks_by_category).and_then(|_| match new_snapshot.as_mut() {
            Some(s) => self.hooks.allocate_task_ids(&mut s.tasks_by_category),
            None => Ok(()),
        });
        if let Err(e) = allocated {
            self.hooks.write_failed(&e);
            return Err(e);
        }
        let request = UpdateWorkflowExecutionRequest {
            shard_id: self.shard_id,
            range_id: (self.range_id)(),
            mode,
            update_workflow_mutation: mutation,
            new_workflow_snapshot: new_snapshot,
            workflow_request_mode: requests,
            current_timestamp_unix_nanos: now_unix_nanos(),
        };
        match self.executions.update_workflow_execution(request).await {
            Ok(()) => {
                self.hooks.tasks_committed();
                Ok(())
            }
            Err(e) => {
                let e = M::Error::from(to_history_error(e));
                self.hooks.write_failed(&e);
                Err(e)
            }
        }
    }
    fn notify_snapshot(&mut self, snapshot: Option<&WorkflowSnapshot>, history: &[PersistedBlob], persistence_error: bool) {
        self.hooks.notify_snapshot(snapshot, history, persistence_error)
    }
    fn notify_mutation(&mut self, mutation: &WorkflowMutation, history: &[PersistedBlob], persistence_error: bool) {
        self.hooks.notify_mutation(mutation, history, persistence_error)
    }
    fn notify_history(&mut self, state: &M) {
        self.hooks.notify_history(state)
    }
    fn emit_loaded_stats(&mut self, domain: &str, state: &M) {
        self.hooks.emit_loaded_stats(domain, state)
    }
    fn emit_session_stats(&mut self, domain: &str) {
        self.hooks.emit_session_stats(domain)
    }
    fn emit_history_stats(&mut self, domain: &str, size: i64, count: i64) {
        self.hooks.emit_history_stats(domain, size, count)
    }
    fn emit_large_workflow_stats(&mut self, domain: &str, blob_size: i64, old_count: i64, old_size: i64, new_count: i64) {
        self.hooks.emit_large_workflow_stats(domain, blob_size, old_count, old_size, new_count)
    }
    fn emit_completion_stats(&mut self, domain: &str, state: &M) {
        self.hooks.emit_completion_stats(domain, state)
    }
    fn merge_continue_as_new_replication_tasks(
        &mut self,
        mode: UpdateWorkflowMode,
        mutation: &mut WorkflowMutation,
        new_workflow: Option<&mut WorkflowSnapshot>,
    ) -> Result<(), M::Error> {
        self.hooks.merge_continue_as_new_replication_tasks(mode, mutation, new_workflow)
    }
    async fn reapply_events(&mut self, batches: &[WorkflowEvents]) -> Result<(), M::Error> {
        self.hooks.reapply_events(batches).await
    }
}

impl<M, H> ConflictContextStore<M> for SqlWorkflowContextStore<M, H>
where
    M: TransactionState,
    M::Error: StoreError,
    H: WorkflowContextHooks<M>,
{
    async fn conflict_resolve(
        &mut self,
        _domain: &str,
        mode: ConflictResolveWorkflowMode,
        reset: &WorkflowSnapshot,
        new: Option<&WorkflowSnapshot>,
        current: Option<&WorkflowMutation>,
    ) -> Result<(), M::Error> {
        let mut reset = reset.clone();
        let mut new = new.cloned();
        let mut current = current.cloned();
        self.hooks.begin_task_write().await;
        let allocated = self.hooks.allocate_task_ids(&mut reset.tasks_by_category).and_then(|_| {
            match new.as_mut() {
                Some(s) => self.hooks.allocate_task_ids(&mut s.tasks_by_category),
                None => Ok(()),
            }
        }).and_then(|_| match current.as_mut() {
            Some(m) => self.hooks.allocate_task_ids(&mut m.tasks_by_category),
            None => Ok(()),
        });
        if let Err(e) = allocated {
            self.hooks.write_failed(&e);
            return Err(e);
        }
        let request = ConflictResolveWorkflowExecutionRequest {
            shard_id: self.shard_id,
            range_id: (self.range_id)(),
            mode,
            reset_workflow_snapshot: reset,
            new_workflow_snapshot: new,
            current_workflow_mutation: current,
            workflow_request_mode: WorkflowRequestMode::New,
            current_timestamp_unix_nanos: now_unix_nanos(),
        };
        match self.executions.conflict_resolve_workflow_execution(request).await {
            Ok(_) => {
                self.hooks.tasks_committed();
                Ok(())
            }
            Err(e) => {
                let e = M::Error::from(to_history_error(e));
                self.hooks.write_failed(&e);
                Err(e)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ExecutionCacheBackend (Go execution/cache.go pinned TTL/size cache)
// ---------------------------------------------------------------------------

/// A cached workflow context: a lock plus an opaque payload (the loaded
/// mutable-state context) that `clear` drops.
pub struct CacheSlot {
    pub key: WorkflowExecutionKey,
    locked: Mutex<bool>,
    wake: Condvar,
    pub payload: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    cleared: AtomicBool,
}

impl CacheSlot {
    fn new(key: WorkflowExecutionKey) -> Arc<Self> {
        Arc::new(Self { key, locked: Mutex::new(false), wake: Condvar::new(), payload: Mutex::new(None), cleared: AtomicBool::new(false) })
    }
    pub fn was_cleared(&self) -> bool {
        self.cleared.load(Ordering::Acquire)
    }
}

struct CacheEntry {
    slot: Arc<CacheSlot>,
    pins: usize,
    last_access: Instant,
}

pub type CacheMetricFn = Arc<dyn Fn(&'static str, &'static str) + Send + Sync>;

/// Implements [`ExecutionCacheBackend`]. Domain names come from the domain
/// cache and current-run lookups hit the SQL `current_executions` table.
pub struct SqlExecutionCacheBackend {
    entries: Mutex<HashMap<WorkflowExecutionKey, CacheEntry>>,
    pub max_size: usize,
    pub ttl: Duration,
    pub domains: Arc<dyn DomainCache>,
    pub executions: Arc<dyn GoExecutionStore>,
    pub shard_id: i32,
    pub metrics: Option<CacheMetricFn>,
}

impl SqlExecutionCacheBackend {
    pub fn new(
        domains: Arc<dyn DomainCache>,
        executions: Arc<dyn GoExecutionStore>,
        shard_id: i32,
        max_size: usize,
        ttl: Duration,
    ) -> Self {
        Self { entries: Mutex::new(HashMap::new()), max_size, ttl, domains, executions, shard_id, metrics: None }
    }

    fn evict(&self, entries: &mut HashMap<WorkflowExecutionKey, CacheEntry>) {
        let now = Instant::now();
        let ttl = self.ttl;
        entries.retain(|_, e| e.pins > 0 || now.duration_since(e.last_access) < ttl);
        while entries.len() >= self.max_size {
            let oldest = entries.iter().filter(|(_, e)| e.pins == 0).min_by_key(|(_, e)| e.last_access).map(|(k, _)| k.clone());
            match oldest {
                Some(key) => {
                    entries.remove(&key);
                }
                None => break,
            }
        }
    }
}

impl ExecutionCacheBackend for SqlExecutionCacheBackend {
    type Context = Arc<CacheSlot>;
    type Error = HistoryError;

    fn get(&mut self, key: &WorkflowExecutionKey) -> Option<Self::Context> {
        let mut entries = self.entries.lock().expect("cache poisoned");
        let ttl = self.ttl;
        if let Some(entry) = entries.get(key) {
            if entry.pins == 0 && entry.last_access.elapsed() >= ttl {
                entries.remove(key);
                return None;
            }
        }
        entries.get_mut(key).map(|entry| {
            entry.pins += 1;
            entry.last_access = Instant::now();
            entry.slot.clone()
        })
    }
    fn put_if_not_exist(&mut self, key: &WorkflowExecutionKey, context: Self::Context) -> Result<Self::Context, Self::Error> {
        let mut entries = self.entries.lock().expect("cache poisoned");
        if let Some(entry) = entries.get_mut(key) {
            entry.pins += 1;
            entry.last_access = Instant::now();
            return Ok(entry.slot.clone());
        }
        self.evict(&mut entries);
        if entries.len() >= self.max_size {
            return Err(ServiceBusyError::new("cache capacity is fully occupied by pinned elements").into());
        }
        entries.insert(key.clone(), CacheEntry { slot: context.clone(), pins: 1, last_access: Instant::now() });
        Ok(context)
    }
    fn new_context(&mut self, key: &WorkflowExecutionKey) -> Self::Context {
        CacheSlot::new(key.clone())
    }
    fn lock(&mut self, context: &Self::Context, timeout: Option<Duration>) -> Result<(), Self::Error> {
        let mut locked = context.locked.lock().expect("slot poisoned");
        match timeout {
            None => {
                while *locked {
                    locked = context.wake.wait(locked).expect("slot poisoned");
                }
            }
            Some(limit) => {
                let (guard, result) = context
                    .wake
                    .wait_timeout_while(locked, limit, |held| *held)
                    .expect("slot poisoned");
                locked = guard;
                if result.timed_out() && *locked {
                    return Err(HistoryError::DeadlineExceeded);
                }
            }
        }
        *locked = true;
        Ok(())
    }
    fn unlock(&mut self, context: &Self::Context) {
        *context.locked.lock().expect("slot poisoned") = false;
        context.wake.notify_one();
    }
    fn clear(&mut self, context: &Self::Context) {
        *context.payload.lock().expect("slot poisoned") = None;
        context.cleared.store(true, Ordering::Release);
    }
    fn release(&mut self, key: &WorkflowExecutionKey) {
        if let Some(entry) = self.entries.lock().expect("cache poisoned").get_mut(key) {
            entry.pins = entry.pins.saturating_sub(1);
            entry.last_access = Instant::now();
        }
    }
    fn domain_name(&self, id: &str) -> Result<String, Self::Error> {
        self.domains.get_domain_name(id)
    }
    fn current_execution_with_retry(&mut self, domain_id: &str, _domain_name: &str, workflow_id: &str) -> Result<String, Self::Error> {
        let executions = self.executions.clone();
        let shard = self.shard_id;
        block_on(retry_transient(|| executions.get_current_execution(shard, domain_id.to_string(), workflow_id.to_string())))
            .map(|c| c.run_id)
            .map_err(to_history_error)
    }
    fn valid_run_id(&self, id: &str) -> bool {
        uuid::Uuid::parse_str(id).is_ok()
    }
    fn bad_request(&self, message: &'static str) -> Self::Error {
        BadRequestError::new(message).into()
    }
    fn metric(&mut self, scope: &'static str, name: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics(scope, name);
        }
    }
}
