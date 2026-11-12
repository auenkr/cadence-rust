//! Workflow execution identity and transaction coordination primitives.
//!
//! Persistence operations are implemented by the history persistence service;
//! this module owns the stable workflow identity used by load/update calls.

use cadence_api::types::WorkflowExecution;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkflowExecutionKey {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
}

impl WorkflowExecutionKey {
    pub fn new(domain_id: impl Into<String>, execution: &WorkflowExecution) -> Self {
        Self { domain_id: domain_id.into(), workflow_id: execution.workflow_id.clone(), run_id: execution.run_id.clone() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowContextError {
    #[error("workflow identity requires non-empty domain, workflow, and run IDs")]
    EmptyIdentity,
    #[error("workflow update conflict for {0:?}")]
    Conflict(WorkflowExecutionKey),
}

impl WorkflowExecutionKey {
    pub fn validate(&self) -> Result<(), WorkflowContextError> {
        if self.domain_id.is_empty() || self.workflow_id.is_empty() || self.run_id.is_empty() { return Err(WorkflowContextError::EmptyIdentity); }
        Ok(())
    }
}

use cadence_api::{enums::EventType, service_types::HistoryEvent};
use cadence_persistence::history_contract::{DataBlob, WorkflowSnapshot, WorkflowMutation, WorkflowRequest, WorkflowRequestMode, CreateWorkflowMode, UpdateWorkflowMode};
use super::workflow::TransactionPolicy;

#[derive(Debug, Clone, Default)]
pub struct PersistedBlob { pub data_blob: DataBlob, pub branch_token: Vec<u8>, pub first_event_id: i64 }
#[derive(Debug, Clone)]
pub struct WorkflowEvents { pub key: WorkflowExecutionKey, pub branch_token: Vec<u8>, pub events: Vec<HistoryEvent> }

/// Transaction operations supplied by a mutable-state implementation. The
/// persistence-facing images are typed; event batches remain separate because
/// history must be appended before updating the execution row.
#[allow(async_fn_in_trait)]
pub trait TransactionState {
    type Error;
    fn history_size(&self) -> i64;
    fn set_history_size(&mut self, size: i64);
    fn next_event_id(&self) -> i64;
    /// Go `StartTransaction` first step: `UpdateCurrentVersion(activeClusterInfo.FailoverVersion, false)`.
    fn update_current_version_from_active(&mut self, _failover_version: i64) -> Result<(), Self::Error> { Ok(()) }
    async fn start_transaction(&mut self, incoming_version: i64) -> Result<bool, Self::Error>;
    fn close_mutation(&mut self, now: i64, policy: TransactionPolicy) -> Result<(WorkflowMutation, Vec<WorkflowEvents>), Self::Error>;
    fn close_snapshot(&mut self, now: i64, policy: TransactionPolicy) -> Result<(WorkflowSnapshot, Vec<WorkflowEvents>), Self::Error>;
    /// Drop transaction deltas only after the persistence commit succeeds.
    fn transaction_committed(&mut self) {}
}

/// Shard-facing calls preserve the Go retry boundaries. Implementations assign
/// range/transaction IDs and encoding in the shard, and retry only transient
/// failures (create/update persistence timeouts must not be retried).
#[allow(async_fn_in_trait)]
pub trait WorkflowContextStore<M: TransactionState> {
    fn now_nanos(&self) -> i64;
    fn domain_name(&self, domain_id: &str) -> Result<String, M::Error>;
    fn internal_error(&self, message: &'static str) -> M::Error;
    fn possibly_successful(&self, error: &M::Error) -> bool;
    fn strong_idempotency_check(&self, domain: &str) -> bool;
    fn request_type(&self, request: &WorkflowRequest) -> i32;
    fn log_request_validation_error(&mut self, message: &'static str);
    fn cleared(&mut self);
    async fn get_workflow_with_retry(&mut self, key: &WorkflowExecutionKey, domain: &str) -> Result<M, M::Error>;
    /// Go `shard.GetActiveClusterManager().GetActiveClusterInfoByWorkflow(...).FailoverVersion`;
    /// `None` when the store has no active cluster manager.
    async fn active_failover_version(&mut self, _key: &WorkflowExecutionKey) -> Result<Option<i64>, M::Error> { Ok(None) }
    async fn verify_and_repair(&mut self, state: &mut M) -> Result<bool, M::Error>;
    async fn flush_as_active(&mut self, state: &mut M, now: i64) -> Result<(), M::Error>;
    async fn append_history_with_retry(&mut self, events: &WorkflowEvents, domain: &str, new_branch: bool, cleanup_info: Option<String>) -> Result<DataBlob, M::Error>;
    async fn create_with_retry(&mut self, domain: &str, snapshot: &WorkflowSnapshot, mode: CreateWorkflowMode, previous_run: &str, previous_version: i64, requests: WorkflowRequestMode) -> Result<(), M::Error>;
    async fn update_with_retry(&mut self, domain: &str, mutation: &WorkflowMutation, new_workflow: Option<&WorkflowSnapshot>, mode: UpdateWorkflowMode, requests: WorkflowRequestMode) -> Result<(), M::Error>;
    fn notify_snapshot(&mut self, snapshot: Option<&WorkflowSnapshot>, history: &[PersistedBlob], persistence_error: bool);
    fn notify_mutation(&mut self, mutation: &WorkflowMutation, history: &[PersistedBlob], persistence_error: bool);
    fn notify_history(&mut self, state: &M);
    fn emit_loaded_stats(&mut self, domain: &str, state: &M);
    fn emit_session_stats(&mut self, domain: &str);
    fn emit_history_stats(&mut self, domain: &str, size: i64, count: i64);
    fn emit_large_workflow_stats(&mut self, domain: &str, blob_size: i64, old_count: i64, old_size: i64, new_count: i64);
    fn emit_completion_stats(&mut self, domain: &str, state: &M);
    fn merge_continue_as_new_replication_tasks(&mut self, mode: UpdateWorkflowMode, mutation: &mut WorkflowMutation, new_workflow: Option<&mut WorkflowSnapshot>) -> Result<(), M::Error>;
    async fn reapply_events(&mut self, batches: &[WorkflowEvents]) -> Result<(), M::Error>;
}

pub struct WorkflowContext<M, S> { pub key: WorkflowExecutionKey, pub mutable_state: Option<M>, pub history_size: i64, pub store: S }
impl<M: TransactionState, S: WorkflowContextStore<M>> WorkflowContext<M, S> {
    pub fn new(key: WorkflowExecutionKey, store: S) -> Self { Self { key, mutable_state: None, history_size: 0, store } }
    pub fn clear(&mut self) { self.store.cleared(); self.mutable_state = None; self.history_size = 0; }
    pub fn set_history_size(&mut self, size: i64) { self.history_size = size; if let Some(state) = &mut self.mutable_state { state.set_history_size(size); } }
    pub fn byte_size(&self) -> u64 { (self.key.domain_id.len() + self.key.workflow_id.len() + self.key.run_id.len() + 3 * 16 + 3 * 8 + 512 + 256 + 8 + 1024 + 8 + 18 * 8) as u64 }
    pub async fn load_workflow_execution(&mut self) -> Result<&mut M, M::Error> { self.load_workflow_execution_with_task_version(-24).await }
    pub async fn load_workflow_execution_with_task_version(&mut self, incoming_version: i64) -> Result<&mut M, M::Error> {
        let domain = self.store.domain_name(&self.key.domain_id)?;
        if self.mutable_state.is_none() {
            let mut loaded = false;
            for _ in 0..3 {
                let mut state = self.store.get_workflow_with_retry(&self.key, &domain).await?;
                let repaired = self.store.verify_and_repair(&mut state).await?;
                self.mutable_state = Some(state);
                if !repaired { loaded = true; break; }
            }
            if !loaded { return Err(self.store.internal_error("mutable state could not be loaded after multiple attempts")); }
            let state = self.mutable_state.as_ref().expect("loaded state");
            self.history_size = state.history_size(); self.store.emit_loaded_stats(&domain, state);
        }
        let state = self.mutable_state.as_mut().expect("loaded state");
        if let Some(version) = self.store.active_failover_version(&self.key).await? { state.update_current_version_from_active(version)?; }
        if state.start_transaction(incoming_version).await? {
            let now = self.store.now_nanos(); self.store.flush_as_active(state, now).await?;
            if state.start_transaction(incoming_version).await? { return Err(self.store.internal_error("workflowExecutionContext counter flushBeforeReady status after loading mutable state from DB")); }
        }
        Ok(state)
    }
    pub async fn persist_start_workflow_batch_events(&mut self, events: &WorkflowEvents) -> Result<PersistedBlob, M::Error> { self.persist_batch(events, true).await }
    pub async fn persist_non_start_workflow_batch_events(&mut self, events: &WorkflowEvents) -> Result<PersistedBlob, M::Error> { self.persist_batch(events, false).await }
    async fn persist_batch(&mut self, events: &WorkflowEvents, new_branch: bool) -> Result<PersistedBlob, M::Error> {
        if events.events.is_empty() { return if new_branch { Err(self.store.internal_error("cannot persist first workflow events with empty events")) } else { Ok(PersistedBlob::default()) }; }
        let domain = self.store.domain_name(&events.key.domain_id)?;
        let cleanup = new_branch.then(|| format!("{}:{}:{}", events.key.domain_id, events.key.workflow_id, events.key.run_id));
        let data_blob = self.store.append_history_with_retry(events, &domain, new_branch, cleanup).await?;
        Ok(PersistedBlob { data_blob, branch_token: events.branch_token.clone(), first_event_id: events.events[0].event_id })
    }
    fn validate_requests(&mut self, requests: &[WorkflowRequest], mode: WorkflowRequestMode, domain: &str) -> Result<(), M::Error> {
        let types = requests.iter().map(|r| self.store.request_type(r)).collect::<Vec<_>>();
        if validate_workflow_requests_and_mode(&types, mode).is_err() {
            const MESSAGE: &str = "too many workflow request entities generated from a single API request";
            if self.store.strong_idempotency_check(domain) { return Err(self.store.internal_error(MESSAGE)); }
            self.store.log_request_validation_error(MESSAGE);
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn create_workflow_execution(&mut self, snapshot: &mut WorkflowSnapshot, history: PersistedBlob, mode: CreateWorkflowMode, previous_run: &str, previous_version: i64, requests: WorkflowRequestMode) -> Result<(), M::Error> {
        let result = self.create_inner(snapshot, history, mode, previous_run, previous_version, requests).await;
        if result.is_err() { self.clear(); } result
    }
    #[allow(clippy::too_many_arguments)]
    async fn create_inner(&mut self, snapshot: &mut WorkflowSnapshot, history: PersistedBlob, mode: CreateWorkflowMode, previous_run: &str, previous_version: i64, requests: WorkflowRequestMode) -> Result<(), M::Error> {
        let domain = self.store.domain_name(&self.key.domain_id)?;
        self.validate_requests(&snapshot.workflow_requests, requests, &domain)?;
        self.set_history_size(self.history_size + history.data_blob.data.len() as i64);
        snapshot.execution_info.history_size = self.history_size;
        let histories = [history];
        if let Err(err) = self.store.create_with_retry(&domain, snapshot, mode, previous_run, previous_version, requests).await {
            if self.store.possibly_successful(&err) { self.store.notify_snapshot(Some(snapshot), &histories, true); } return Err(err);
        }
        if let Some(state) = &mut self.mutable_state { state.transaction_committed(); }
        self.store.notify_snapshot(Some(snapshot), &histories, false); self.store.emit_session_stats(&domain); Ok(())
    }
    pub async fn update_workflow_execution_tasks(&mut self, now: i64) -> Result<(), M::Error> {
        let result = self.update_tasks_inner(now).await; if result.is_err() { self.clear(); } result
    }
    async fn update_tasks_inner(&mut self, now: i64) -> Result<(), M::Error> {
        let state = self.mutable_state.as_mut().ok_or_else(|| self.store.internal_error("mutable state not loaded"))?;
        let (mut mutation, events) = state.close_mutation(now, TransactionPolicy::Passive)?;
        if !events.is_empty() { return Err(self.store.internal_error("UpdateWorkflowExecutionTask can only be used for persisting new workflow tasks, but found new history events")); }
        let domain = self.store.domain_name(&self.key.domain_id)?;
        if !mutation.workflow_requests.is_empty() {
            const MESSAGE: &str = "UpdateWorkflowExecutionTask can only be used for persisting new workflow tasks, but found new workflow requests";
            if self.store.strong_idempotency_check(&domain) { return Err(self.store.internal_error(MESSAGE)); } self.store.log_request_validation_error(MESSAGE);
        }
        mutation.execution_info.history_size = self.history_size;
        if let Err(err) = self.store.update_with_retry(&domain, &mutation, None, UpdateWorkflowMode::IgnoreCurrent, WorkflowRequestMode::New).await {
            if self.store.possibly_successful(&err) { self.store.notify_mutation(&mutation, &[], true); } return Err(err);
        }
        if let Some(state) = &mut self.mutable_state { state.transaction_committed(); }
        self.store.notify_mutation(&mutation, &[], false); self.store.emit_session_stats(&domain); Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("too many workflow request entities generated from a single API request")]
pub struct InvalidWorkflowRequests;
pub fn validate_workflow_requests_and_mode(types: &[i32], mode: WorkflowRequestMode) -> Result<(), InvalidWorkflowRequests> {
    if mode != WorkflowRequestMode::New || types.len() < 2 { return Ok(()); }
    // WorkflowRequestTypeStart=0 and Signal=1. SignalWithStart may produce both.
    if types.len() == 2 && ((types[0] == 0 && types[1] == 1) || (types[0] == 1 && types[1] == 0)) { Ok(()) } else { Err(InvalidWorkflowRequests) }
}

/// Validate and select reapplicable signals before routing to the workflow's
/// active cluster. Run IDs may differ across batches; domain/workflow may not.
pub fn select_reapply_events(batches: &[WorkflowEvents]) -> Result<Vec<HistoryEvent>, &'static str> {
    let Some(first) = batches.first() else { return Ok(Vec::new()); };
    let mut events = Vec::new();
    for batch in batches {
        if batch.key.domain_id != first.key.domain_id || batch.key.workflow_id != first.key.workflow_id { return Err("workflowExecutionContext encounter mismatch domainID / workflowID in events reapplication."); }
        events.extend(batch.events.iter().filter(|event| event.event_type == Some(EventType::WorkflowExecutionSignaled)).cloned());
    }
    Ok(events)
}

impl<M: TransactionState, S: WorkflowContextStore<M>> WorkflowContext<M, S> {
    pub async fn update_workflow_execution_as_active(&mut self, now: i64) -> Result<(), M::Error> {
        self.update_workflow_execution_with_new(now, UpdateWorkflowMode::UpdateCurrent, None, TransactionPolicy::Active, None, WorkflowRequestMode::New).await
    }
    pub async fn update_workflow_execution_as_passive(&mut self, now: i64) -> Result<(), M::Error> {
        self.update_workflow_execution_with_new(now, UpdateWorkflowMode::UpdateCurrent, None, TransactionPolicy::Passive, None, WorkflowRequestMode::Replicated).await
    }
    pub async fn update_workflow_execution_with_new_as_active(&mut self, now: i64, new: &mut Self) -> Result<(), M::Error> {
        self.update_workflow_execution_with_new(now, UpdateWorkflowMode::UpdateCurrent, Some(new), TransactionPolicy::Active, Some(TransactionPolicy::Active), WorkflowRequestMode::New).await
    }
    pub async fn update_workflow_execution_with_new_as_passive(&mut self, now: i64, new: &mut Self) -> Result<(), M::Error> {
        self.update_workflow_execution_with_new(now, UpdateWorkflowMode::UpdateCurrent, Some(new), TransactionPolicy::Passive, Some(TransactionPolicy::Passive), WorkflowRequestMode::Replicated).await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn update_workflow_execution_with_new(&mut self, now: i64, mode: UpdateWorkflowMode, mut new: Option<&mut Self>, policy: TransactionPolicy, new_policy: Option<TransactionPolicy>, requests: WorkflowRequestMode) -> Result<(), M::Error> {
        let result = self.update_inner(now, mode, new.as_deref_mut(), policy, new_policy, requests).await;
        if result.is_err() { self.clear(); if let Some(new) = new { if new_policy.is_some() { new.clear(); } } }
        result
    }
    #[allow(clippy::too_many_arguments)]
    async fn update_inner(&mut self, now: i64, mode: UpdateWorkflowMode, mut new: Option<&mut Self>, policy: TransactionPolicy, new_policy: Option<TransactionPolicy>, requests: WorkflowRequestMode) -> Result<(), M::Error> {
        let state = self.mutable_state.as_mut().ok_or_else(|| self.store.internal_error("mutable state not loaded"))?;
        let (mut mutation, current_events) = state.close_mutation(now, policy)?;
        let domain = self.store.domain_name(&self.key.domain_id)?;
        self.validate_requests(&mutation.workflow_requests, requests, &domain)?;
        let old_size = self.history_size;
        let old_count = self.mutable_state.as_ref().expect("loaded state").next_event_id() - 1;
        let mut count = old_count; let mut size = old_size; let mut blobs = Vec::new();
        for events in &current_events {
            let blob = self.persist_non_start_workflow_batch_events(events).await?;
            count += events.events.len() as i64; size += blob.data_blob.data.len() as i64; blobs.push(blob);
        }
        self.set_history_size(size); mutation.execution_info.history_size = size;
        let mut snapshot = None; let mut new_events = Vec::new();
        if let (Some(context), Some(new_policy)) = (new.as_deref_mut(), new_policy) {
            if let Some(state) = &mut context.mutable_state {
                let (mut image, events) = state.close_snapshot(now, new_policy)?;
                if !image.workflow_requests.is_empty() && !mutation.workflow_requests.is_empty() {
                    const MESSAGE: &str = "workflow requests are only expected to be generated from one workflow for UpdateWorkflowExecution";
                    if self.store.strong_idempotency_check(&domain) { return Err(self.store.internal_error(MESSAGE)); } self.store.log_request_validation_error(MESSAGE);
                }
                self.validate_requests(&image.workflow_requests, requests, &domain)?;
                let first = events.first().ok_or_else(|| self.store.internal_error("new workflow snapshot has no event batches"))?;
                let first_id = first.events.first().ok_or_else(|| self.store.internal_error("new workflow snapshot has empty event batch"))?.event_id;
                let blob = if first_id == 1 { self.persist_start_workflow_batch_events(first).await? } else { self.persist_non_start_workflow_batch_events(first).await? };
                let new_size = context.history_size + blob.data_blob.data.len() as i64;
                context.set_history_size(new_size); image.execution_info.history_size = new_size; blobs.push(blob);
                snapshot = Some(image); new_events = events;
            }
        }
        self.store.merge_continue_as_new_replication_tasks(mode, &mut mutation, snapshot.as_mut())?;
        if mode == UpdateWorkflowMode::BypassCurrent {
            let mut events = current_events; events.extend(new_events); self.store.reapply_events(&events).await?;
        }
        if let Err(err) = self.store.update_with_retry(&domain, &mutation, snapshot.as_ref(), mode, requests).await {
            if self.store.possibly_successful(&err) { self.store.notify_mutation(&mutation, &blobs, true); self.store.notify_snapshot(snapshot.as_ref(), &blobs, true); } return Err(err);
        }
        if let Some(state) = &mut self.mutable_state { state.transaction_committed(); }
        if let Some(context) = new { if let Some(state) = &mut context.mutable_state { state.transaction_committed(); } }
        let state = self.mutable_state.as_ref().expect("loaded state");
        self.store.notify_history(state);
        self.store.notify_mutation(&mutation, &blobs, false); self.store.notify_snapshot(snapshot.as_ref(), &blobs, false);
        self.store.emit_history_stats(&domain, self.history_size, state.next_event_id() - 1);
        self.store.emit_session_stats(&domain); self.store.emit_large_workflow_stats(&domain, size - old_size, old_count, old_size, count);
        if mutation.execution_info.state == 2 { self.store.emit_completion_stats(&domain, state); }
        Ok(())
    }
}

#[allow(async_fn_in_trait)]
pub trait ConflictContextStore<M: TransactionState>: WorkflowContextStore<M> {
    async fn conflict_resolve(&mut self, domain: &str, mode: cadence_persistence::history_contract::ConflictResolveWorkflowMode, reset: &WorkflowSnapshot, new: Option<&WorkflowSnapshot>, current: Option<&WorkflowMutation>) -> Result<(), M::Error>;
}
impl<M: TransactionState, S: ConflictContextStore<M>> WorkflowContext<M, S> {
    #[allow(clippy::too_many_arguments)]
    pub async fn conflict_resolve_workflow_execution(&mut self, now: i64, mode: cadence_persistence::history_contract::ConflictResolveWorkflowMode, reset: &mut M, mut new: Option<&mut Self>, mut current: Option<&mut Self>, current_policy: Option<TransactionPolicy>) -> Result<(), M::Error> {
        let result = self.conflict_resolve_inner(now, mode, reset, new.as_deref_mut(), current.as_deref_mut(), current_policy).await;
        if result.is_err() { self.clear(); if let Some(new) = new { new.clear(); }
 if current_policy.is_some() { if let Some(current) = current { current.clear(); } } }
        result
    }
    #[allow(clippy::too_many_arguments)]
    async fn conflict_resolve_inner(&mut self, now: i64, mode: cadence_persistence::history_contract::ConflictResolveWorkflowMode, reset: &mut M, new: Option<&mut Self>, current: Option<&mut Self>, current_policy: Option<TransactionPolicy>) -> Result<(), M::Error> {
        let (mut reset_snapshot, reset_events) = reset.close_snapshot(now, TransactionPolicy::Passive)?;
        let domain = self.store.domain_name(&self.key.domain_id)?;
        let mut blobs = Vec::new(); let mut size = self.history_size;
        for events in &reset_events { let blob = self.persist_non_start_workflow_batch_events(events).await?; size += blob.data_blob.data.len() as i64; blobs.push(blob); }
        self.set_history_size(size); reset_snapshot.execution_info.history_size = size;
        let mut new_snapshot = None; let mut new_events = Vec::new();
        if let Some(context) = new {
            if let Some(state) = &mut context.mutable_state {
                let (mut image, events) = state.close_snapshot(now, TransactionPolicy::Passive)?;
                if !reset_snapshot.workflow_requests.is_empty() && !image.workflow_requests.is_empty() {
                    const MESSAGE: &str = "workflow requests are only expected to be generated from either reset workflow or continue-as-new workflow for ConflictResolveWorkflowExecution";
                    if self.store.strong_idempotency_check(&domain) { return Err(self.store.internal_error(MESSAGE)); } self.store.log_request_validation_error(MESSAGE);
                }
                let first = events.first().ok_or_else(|| self.store.internal_error("new workflow snapshot has no event batches"))?;
                let blob = self.persist_start_workflow_batch_events(first).await?;
                let size = context.history_size + blob.data_blob.data.len() as i64; context.set_history_size(size); image.execution_info.history_size = size;
                blobs.push(blob); new_snapshot = Some(image); new_events = events;
            }
        }
        let mut current_mutation = None;
        if let (Some(context), Some(policy)) = (current, current_policy) {
            if let Some(state) = &mut context.mutable_state {
                let (mut image, events) = state.close_mutation(now, policy)?;
                if !image.workflow_requests.is_empty() {
                    const MESSAGE: &str = "workflow requests are not expected from current workflow for ConflictResolveWorkflowExecution";
                    if self.store.strong_idempotency_check(&domain) { return Err(self.store.internal_error(MESSAGE)); } self.store.log_request_validation_error(MESSAGE);
                }
                let mut size = context.history_size;
                for event in &events { let blob = self.persist_non_start_workflow_batch_events(event).await?; size += blob.data_blob.data.len() as i64; blobs.push(blob); }
                context.set_history_size(size); image.execution_info.history_size = size; current_mutation = Some(image);
            }
        }
        if mode == cadence_persistence::history_contract::ConflictResolveWorkflowMode::BypassCurrent {
            // Current-workflow events deliberately do not participate.
            let mut events = reset_events; events.extend(new_events); self.store.reapply_events(&events).await?;
        }
        if let Err(err) = self.store.conflict_resolve(&domain, mode, &reset_snapshot, new_snapshot.as_ref(), current_mutation.as_ref()).await {
            if self.store.possibly_successful(&err) {
                self.store.notify_snapshot(Some(&reset_snapshot), &blobs, true); self.store.notify_snapshot(new_snapshot.as_ref(), &blobs, true);
                if let Some(current) = &current_mutation { self.store.notify_mutation(current, &blobs, true); }
            }
            return Err(err);
        }
        self.store.notify_history(reset); self.store.notify_snapshot(Some(&reset_snapshot), &blobs, false); self.store.notify_snapshot(new_snapshot.as_ref(), &blobs, false);
        if let Some(current) = &current_mutation { self.store.notify_mutation(current, &blobs, false); }
        self.store.emit_history_stats(&domain, self.history_size, reset.next_event_id() - 1); self.store.emit_session_stats(&domain);
        if reset_snapshot.execution_info.state == 2 { self.store.emit_completion_stats(&domain, reset); }
        Ok(())
    }
}

#[allow(async_fn_in_trait)]
pub trait EventReapplyContext {
    type Error;
    fn domain_pending_active(&mut self, domain: &str) -> Result<bool, Self::Error>;
    fn domain_name(&self, domain: &str) -> Result<String, Self::Error>;
    fn internal_error(&self, message: &'static str) -> Self::Error;
    async fn active_cluster(&mut self, key: &WorkflowExecutionKey) -> Result<String, Self::Error>;
    fn current_cluster(&self) -> &str;
    async fn reapply_local(&mut self, key: &WorkflowExecutionKey, events: Vec<HistoryEvent>) -> Result<(), Self::Error>;
    /// Serialize with ThriftRW and route through the remote admin client. Both
    /// local and remote calls use independent background remote-call deadlines.
    async fn reapply_remote(&mut self, cluster: &str, domain_name: &str, key: &WorkflowExecutionKey, events: Vec<HistoryEvent>) -> Result<(), Self::Error>;
}
pub async fn reapply_events<C: EventReapplyContext>(context: &mut C, batches: &[WorkflowEvents]) -> Result<(), C::Error> {
    let Some(first) = batches.first() else { return Ok(()); };
    if context.domain_pending_active(&first.key.domain_id)? { return Ok(()); }
    let events = select_reapply_events(batches).map_err(|message| context.internal_error(message))?;
    if events.is_empty() { return Ok(()); }
    let active = context.active_cluster(&first.key).await?;
    if active == context.current_cluster() { context.reapply_local(&first.key, events).await }
    else { let domain = context.domain_name(&first.key.domain_id)?; context.reapply_remote(&active, &domain, &first.key, events).await }
}
