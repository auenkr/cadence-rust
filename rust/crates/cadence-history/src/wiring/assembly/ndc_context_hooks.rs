//! group: assembly/ndc_context_hooks
//!
//! Concrete [`WorkflowContextHooks`] for the NDC path (Go
//! `execution/context.go`: create / update persistence for a single run).
//!
//! The generic `WorkflowContext` + `SqlWorkflowContextStore` machinery does the
//! Go ordering (prepare -> write -> cleanup only after commit). These hooks add
//! the shard-side pieces: task id allocation before the write, max read level
//! and queue notification only after a durable write, and shard closing on
//! ownership loss. Anything that depends on unported code is an explicit error.
//!
//! Gaps (recorded in memory/history-ndcpersist2-20261111.md): `verify_and_repair`
//! and `flush_as_active` (error), history notifier
//! (no-op), strong idempotency (Go default false), request type decoding (always Start),
//! continued-as-new two-run writes (error).

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::Arc;

use cadence_persistence::history_contract::{self as pc, HistoryTaskCategory, Task};
use cadence_persistence::sqlblobs::{
    replication_task_from_struct, replication_task_to_struct, timer_task_from_struct, timer_task_to_struct,
    transfer_task_from_struct, transfer_task_to_struct, Encoding,
};

use crate::engine::deps::{HResult, HistoryError};
use crate::execution::context::{
    select_reapply_events, PersistedBlob, TransactionState, WorkflowContext, WorkflowEvents, WorkflowExecutionKey,
};
use crate::execution::workflow::TransactionPolicy;
use crate::ndc::{NdcError, NdcMutableState};
use crate::wiring::assembly::engine_cache::{shard_err, task_blob, EngineCore};
use crate::wiring::assembly::ndc_state::OwnedNdcMutableState;
use crate::wiring::assembly::ndc_workflow::state_from_snapshot;
use crate::wiring::persistence::engine_ndc::{
    block_on, SqlHistoryManager, SqlWorkflowContextStore, WorkflowContextHooks,
};

const TRANSFER: HistoryTaskCategory = HistoryTaskCategory { category_type: 1, category_id: 1 };
const TIMER: HistoryTaskCategory = HistoryTaskCategory { category_type: 2, category_id: 2 };
const REPLICATION: HistoryTaskCategory = HistoryTaskCategory { category_type: 1, category_id: 3 };
const CLOSE_STATUS_CONTINUED_AS_NEW: i32 = crate::execution::mutable_state_methods_common::CLOSE_STATUS_CONTINUED_AS_NEW;

pub(super) fn ndc_err(err: NdcError) -> HistoryError {
    match err {
        NdcError::EntityNotExists(m) => HistoryError::entity_not_exists(m),
        NdcError::BadRequest(e) => HistoryError::bad_request(e.to_string()),
        other => HistoryError::internal(other.to_string()),
    }
}

/// `TransactionState` over the dyn NDC mutable state. Errors are `HistoryError`
/// so the persistence variants (condition failed, timeout, ownership lost)
/// reach the caller unchanged.
pub struct NdcStateBox {
    pub ms: Box<dyn NdcMutableState>,
    pub history_size: i64,
}

impl TransactionState for NdcStateBox {
    type Error = HistoryError;

    fn history_size(&self) -> i64 {
        self.history_size
    }
    fn set_history_size(&mut self, size: i64) {
        self.history_size = size;
    }
    fn next_event_id(&self) -> i64 {
        self.ms.get_next_event_id()
    }
    async fn start_transaction(&mut self, _incoming_version: i64) -> Result<bool, HistoryError> {
        Ok(false)
    }
    fn close_mutation(
        &mut self,
        now: i64,
        policy: TransactionPolicy,
    ) -> Result<(pc::WorkflowMutation, Vec<WorkflowEvents>), HistoryError> {
        self.ms.close_transaction_as_mutation(now, policy).map_err(ndc_err)
    }
    fn close_snapshot(
        &mut self,
        now: i64,
        policy: TransactionPolicy,
    ) -> Result<(pc::WorkflowSnapshot, Vec<WorkflowEvents>), HistoryError> {
        self.ms.close_transaction_as_snapshot(now, policy).map_err(ndc_err)
    }
    fn transaction_committed(&mut self) {
        self.ms.transaction_committed();
    }
}

#[derive(Default)]
struct PendingTasks {
    max_read_level: i64,
    transfer: Vec<pc::TransferTask>,
    timer: Vec<pc::TimerTask>,
}

/// Hooks wired where the NDC env builds the context.
pub struct EngineNdcContextHooks {
    core: Arc<EngineCore>,
    pending: PendingTasks,
}

impl EngineNdcContextHooks {
    pub fn new(core: Arc<EngineCore>) -> Self {
        Self { core, pending: PendingTasks::default() }
    }
}

fn internal(msg: impl Into<String>) -> HistoryError {
    HistoryError::internal(msg)
}

impl WorkflowContextHooks<NdcStateBox> for EngineNdcContextHooks {
    fn now_nanos(&self) -> i64 {
        self.core.clock.now()
    }
    /// Go `EnableStrongIdempotencySanityCheck(domain)` (dynamic config).
    fn strong_idempotency_check(&self, domain: &str) -> bool {
        (self.core.strong_idempotency_sanity_check)(domain)
    }
    /// GAP: the Rust `WorkflowRequest` carries no type/version (the mutable
    /// state does not encode them yet), so there is nothing to decode; the Go
    /// zero value (Start = 0) is returned, which is what an empty blob decodes to.
    fn request_type(&self, _request: &pc::WorkflowRequest) -> i32 {
        // Go `WorkflowRequestType` zero value (Start); see GAP above.
        0
    }
    fn log_request_validation_error(&mut self, message: &'static str) {
        tracing::warn!(message, "ndc workflow request validation");
    }
    fn cleared(&mut self) {}
    fn build_state(&mut self, snapshot: pc::WorkflowSnapshot, domain: &str) -> Result<NdcStateBox, HistoryError> {
        let info = &snapshot.execution_info;
        let execution = cadence_api::types::WorkflowExecution {
            workflow_id: info.workflow_id.clone(),
            run_id: info.run_id.clone(),
        };
        let history_size = info.history_size;
        let ms = state_from_snapshot(domain, &execution, self.core.clock.now(), snapshot).map_err(ndc_err)?;
        Ok(NdcStateBox { ms, history_size })
    }
    /// Go `checksum.Verify`: the typed state carries no checksum and the Go
    /// default verify probability is 0, so nothing is verified or repaired.
    async fn verify_and_repair(&mut self, _state: &mut NdcStateBox) -> Result<bool, HistoryError> {
        Ok(false)
    }
    /// Go `UpdateWorkflowExecutionAsActive` after a decision failover on load.
    async fn flush_as_active(&mut self, state: &mut NdcStateBox, now: i64) -> Result<(), HistoryError> {
        let info = state.ms.get_execution_info();
        let key = WorkflowExecutionKey::new(
            info.domain_id.clone(),
            &cadence_api::types::WorkflowExecution { workflow_id: info.workflow_id, run_id: info.run_id },
        );
        let placeholder = placeholder_state_for(&key);
        let lent = NdcStateBox {
            ms: std::mem::replace(&mut state.ms, placeholder),
            history_size: state.history_size,
        };
        let writer = EngineNdcWriter::new(self.core.clone());
        // Go `UpdateWorkflowExecutionAsActive`.
        let updated = writer.write_update(
            &key,
            lent,
            now,
            pc::UpdateWorkflowMode::UpdateCurrent,
            TransactionPolicy::Active,
            pc::WorkflowRequestMode::New,
        )?;
        *state = updated;
        Ok(())
    }
    fn notify_snapshot(&mut self, _s: Option<&pc::WorkflowSnapshot>, _h: &[PersistedBlob], _err: bool) {}
    fn notify_mutation(&mut self, _m: &pc::WorkflowMutation, _h: &[PersistedBlob], _err: bool) {}
    /// Go `shard.GetEngine().NotifyNewHistoryEvent(events.NewNotification(...))`.
    fn notify_history(&mut self, state: &NdcStateBox) {
        let ms = &state.ms;
        let (last_first_event_id, workflow_state, workflow_close_state) = match ms.history_notification_inputs() {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(%err, "unable to build history notification");
                return;
            }
        };
        let info = ms.get_execution_info();
        self.core.notify_history_event(crate::engine::runtime::HistoryNotification {
            domain_id: info.domain_id,
            workflow_id: info.workflow_id,
            run_id: info.run_id,
            last_first_event_id,
            next_event_id: ms.get_next_event_id(),
            previous_started_event_id: ms.get_previous_started_event_id(),
            workflow_state,
            workflow_close_state,
            version_histories: ms.get_version_histories().cloned().unwrap_or_default(),
        });
    }
    fn emit_loaded_stats(&mut self, _domain: &str, _state: &NdcStateBox) {}
    fn emit_session_stats(&mut self, _domain: &str) {}
    fn emit_history_stats(&mut self, _domain: &str, _size: i64, _count: i64) {}
    fn emit_large_workflow_stats(&mut self, _d: &str, _blob: i64, _oc: i64, _os: i64, _nc: i64) {}
    fn emit_completion_stats(&mut self, _domain: &str, _state: &NdcStateBox) {}
    fn merge_continue_as_new_replication_tasks(
        &mut self,
        mode: pc::UpdateWorkflowMode,
        mutation: &mut pc::WorkflowMutation,
        new_workflow: Option<&mut pc::WorkflowSnapshot>,
    ) -> Result<(), HistoryError> {
        // Go `mergeContinueAsNewReplicationTasks`.
        if mutation.execution_info.close_status != CLOSE_STATUS_CONTINUED_AS_NEW {
            return Ok(());
        }
        if mode == pc::UpdateWorkflowMode::BypassCurrent && new_workflow.is_none() {
            return Ok(());
        }
        let has = |tasks: &BTreeMap<HistoryTaskCategory, Vec<Task>>| tasks.get(&REPLICATION).is_some_and(|l| !l.is_empty());
        if !has(&mutation.tasks_by_category) {
            return Ok(());
        }
        let new_run_task = match new_workflow {
            Some(new) if new.tasks_by_category.get(&REPLICATION).map_or(0, |l| l.len()) == 1 => {
                // Go: `TasksByCategory[Replication] = nil` after taking the task.
                let task = new.tasks_by_category.remove(&REPLICATION).unwrap_or_default().remove(0);
                let s = Encoding::ThriftRW.decode(&task.data.data).map_err(|e| internal(e.to_string()))?;
                replication_task_from_struct(&s).map_err(|e| internal(e.to_string()))?
            }
            _ => {
                return Err(internal("unable to find replication task from new workflow for continue as new replication"))
            }
        };
        let mut updated = false;
        if let Some(list) = mutation.tasks_by_category.get_mut(&REPLICATION) {
            for task in list.iter_mut() {
                let s = Encoding::ThriftRW.decode(&task.data.data).map_err(|e| internal(e.to_string()))?;
                let mut decoded = replication_task_from_struct(&s).map_err(|e| internal(e.to_string()))?;
                decoded.new_run_branch_token = new_run_task.branch_token.clone();
                let s = replication_task_to_struct(&decoded).map_err(|e| internal(e.to_string()))?;
                task.data = task_blob(Encoding::ThriftRW.encode(&s))?;
                updated = true;
            }
        }
        if !updated {
            return Err(internal("unable to find replication task from current workflow for continue as new replication"));
        }
        Ok(())
    }
    async fn reapply_events(&mut self, batches: &[WorkflowEvents]) -> Result<(), HistoryError> {
        reapply_via_engine(&self.core, batches).await
    }

    fn allocate_task_ids(&mut self, tasks: &mut BTreeMap<HistoryTaskCategory, Vec<Task>>) -> Result<(), HistoryError> {
        let shard = &self.core.shard;
        for (category, list) in tasks.iter() {
            if !list.is_empty() && *category != TRANSFER && *category != TIMER && *category != REPLICATION {
                return Err(internal(format!(
                    "TODO(ndc-persistence): task category {category:?} is not supported by NDC persistence"
                )));
            }
        }
        if let Some(list) = tasks.get_mut(&TRANSFER) {
            let mut decoded = Vec::with_capacity(list.len());
            for task in list.iter() {
                let s = Encoding::ThriftRW.decode(&task.data.data).map_err(|e| internal(e.to_string()))?;
                decoded.push(transfer_task_from_struct(&s).map_err(|e| internal(e.to_string()))?);
            }
            let mut slots: Vec<(i64, i64)> = decoded.iter().map(|_| (0, 0)).collect();
            let mut max_read_level = 0i64;
            shard.allocate_transfer_ids(&mut slots, &mut max_read_level).map_err(shard_err)?;
            for ((task, t), slot) in list.iter_mut().zip(decoded.iter_mut()).zip(slots.iter()) {
                t.task_id = slot.0;
                t.visibility_time_unix_nanos = slot.1;
                let s = transfer_task_to_struct(t).map_err(|e| internal(e.to_string()))?;
                task.task_id = slot.0;
                task.visibility_timestamp_unix_nanos = slot.1;
                task.data = task_blob(Encoding::ThriftRW.encode(&s))?;
            }
            self.pending.max_read_level = self.pending.max_read_level.max(max_read_level);
            self.pending.transfer.extend(decoded);
        }
        if let Some(list) = tasks.get_mut(&TIMER) {
            let mut decoded = Vec::with_capacity(list.len());
            for task in list.iter() {
                let s = Encoding::ThriftRW.decode(&task.data.data).map_err(|e| internal(e.to_string()))?;
                decoded.push(timer_task_from_struct(&s).map_err(|e| internal(e.to_string()))?);
            }
            let mut slots: Vec<(i64, i64)> = decoded.iter().map(|t| (0, t.visibility_timestamp_unix_nanos)).collect();
            shard.allocate_timer_ids(&self.core.current_cluster, &mut slots).map_err(shard_err)?;
            for ((task, t), slot) in list.iter_mut().zip(decoded.iter_mut()).zip(slots.iter()) {
                t.task_id = slot.0;
                t.visibility_timestamp_unix_nanos = slot.1;
                let s = timer_task_to_struct(t).map_err(|e| internal(e.to_string()))?;
                task.task_id = slot.0;
                task.visibility_timestamp_unix_nanos = slot.1;
                task.data = task_blob(Encoding::ThriftRW.encode(&s))?;
            }
            self.pending.timer.extend(decoded);
        }
        // Go allocates replication task ids last (after transfer and timer).
        if let Some(list) = tasks.get_mut(&REPLICATION) {
            let mut decoded = Vec::with_capacity(list.len());
            for task in list.iter() {
                let s = Encoding::ThriftRW.decode(&task.data.data).map_err(|e| internal(e.to_string()))?;
                decoded.push(replication_task_from_struct(&s).map_err(|e| internal(e.to_string()))?);
            }
            let mut slots: Vec<(i64, i64)> = list.iter().map(|t| (0, t.visibility_timestamp_unix_nanos)).collect();
            let mut max_read_level = 0i64;
            shard.allocate_transfer_ids(&mut slots, &mut max_read_level).map_err(shard_err)?;
            for (task, slot) in list.iter_mut().zip(slots.iter()) {
                task.task_id = slot.0;
                task.visibility_timestamp_unix_nanos = slot.1;
            }
            self.pending.max_read_level = self.pending.max_read_level.max(max_read_level);
        }
        Ok(())
    }

    fn tasks_committed(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        self.core.shard.update_max_read_level(pending.max_read_level);
        self.core.notify_tasks(&pending.transfer, &pending.timer);
    }

    fn write_failed(&mut self, error: &HistoryError) {
        self.pending = PendingTasks::default();
        if matches!(error, HistoryError::PersistenceShardOwnershipLost(_)) {
            self.core.shard.close_shard(None);
        }
    }
}

/// Go `contextImpl.ReapplyEvents`: skip when the domain is pending active; when
/// the workflow is active here call the engine, otherwise (remote active
/// cluster) send the events through the remote admin client.
pub(super) async fn reapply_via_engine(core: &EngineCore, batches: &[WorkflowEvents]) -> Result<(), HistoryError> {
    let events = select_reapply_events(batches).map_err(internal)?;
    if events.is_empty() {
        return Ok(());
    }
    let key = &batches[0].key;
    let domain = core.domains.get_domain_by_id(&key.domain_id)?;
    if domain.is_domain_pending_active() {
        return Ok(());
    }
    let active = core
        .active_cluster_manager
        .active_cluster_by_workflow(&key.domain_id, &key.workflow_id, &key.run_id)
        .await?;
    if active != core.current_cluster {
        // Go: serialize the events (ThriftRW) and send them through the remote admin client.
        let blob = cadence_thrift::history_blob::encode(events);
        let admin = core
            .remote_admin_clients
            .remote_admin_client(&active)
            .map_err(|e| internal(e.to_string()))?;
        return admin
            .reapply_events(cadence_api::admin::ReapplyEventsRequest {
                domain: domain.info.name.clone(),
                workflow_execution: Some(cadence_api::types::WorkflowExecution {
                    workflow_id: key.workflow_id.clone(),
                    run_id: key.run_id.clone(),
                }),
                events: Some(blob),
            })
            .await
            .map_err(|e| internal(e.to_string()));
    }
    let engine = core
        .engine_slot
        .get()
        .and_then(std::sync::Weak::upgrade)
        .ok_or_else(|| internal("history engine is not available for reapply events"))?;
    engine.reapply_events(&key.domain_id, &key.workflow_id, &key.run_id, &events).await
}

// ------------------------------------------------------------------ writer seam

/// Sync single-run create/update persistence (the NDC state is not `Send`, so
/// the write is driven with `block_on`). Production: [`EngineNdcWriter`].
pub trait NdcWriter: Send + Sync {
    /// Go `contextImpl.ReapplyEvents`.
    fn reapply_events(&self, _key: &WorkflowExecutionKey, _batches: &[WorkflowEvents]) -> HResult<()> {
        Err(internal("reapply_events is not supported by this writer"))
    }

    #[allow(clippy::too_many_arguments)]
    fn write_create(
        &self,
        key: &WorkflowExecutionKey,
        state: NdcStateBox,
        snapshot: &mut pc::WorkflowSnapshot,
        history: PersistedBlob,
        mode: pc::CreateWorkflowMode,
        previous_run_id: &str,
        previous_last_write_version: i64,
        request_mode: pc::WorkflowRequestMode,
    ) -> HResult<NdcStateBox>;

    #[allow(clippy::too_many_arguments)]
    fn write_update(
        &self,
        key: &WorkflowExecutionKey,
        state: NdcStateBox,
        now_nanos: i64,
        mode: pc::UpdateWorkflowMode,
        policy: TransactionPolicy,
        request_mode: pc::WorkflowRequestMode,
    ) -> HResult<NdcStateBox>;

    /// Go `UpdateWorkflowExecutionWithNew` with a new run: one atomic write of
    /// the mutation, the new-run snapshot and the current-run update. Both
    /// states are consumed; on error the caller clears both workflows.
    #[allow(clippy::too_many_arguments)]
    fn write_update_with_new(
        &self,
        key: &WorkflowExecutionKey,
        state: NdcStateBox,
        new_key: &WorkflowExecutionKey,
        new_state: NdcStateBox,
        now_nanos: i64,
        mode: pc::UpdateWorkflowMode,
        policy: TransactionPolicy,
        new_policy: TransactionPolicy,
        request_mode: pc::WorkflowRequestMode,
    ) -> HResult<(NdcStateBox, NdcStateBox)>;

    /// Go `ConflictResolveWorkflowExecution`: `reset` is the rebuilt state of
    /// the workflow being resolved; `new` / `current` are optional other runs.
    #[allow(clippy::too_many_arguments)]
    fn write_conflict_resolve(
        &self,
        key: &WorkflowExecutionKey,
        reset: NdcStateBox,
        new: Option<(WorkflowExecutionKey, NdcStateBox)>,
        current: Option<(WorkflowExecutionKey, NdcStateBox)>,
        now_nanos: i64,
        mode: pc::ConflictResolveWorkflowMode,
        current_policy: Option<TransactionPolicy>,
    ) -> HResult<(NdcStateBox, Option<NdcStateBox>, Option<NdcStateBox>)>;
}

pub struct EngineNdcWriter {
    core: Arc<EngineCore>,
}

impl EngineNdcWriter {
    pub fn new(core: Arc<EngineCore>) -> Self {
        Self { core }
    }

    fn context(
        &self,
        key: &WorkflowExecutionKey,
        state: NdcStateBox,
    ) -> WorkflowContext<NdcStateBox, SqlWorkflowContextStore<NdcStateBox, EngineNdcContextHooks>> {
        let shard = self.core.shard.clone();
        let range_shard = shard.clone();
        let id_shard = shard.clone();
        let store = SqlWorkflowContextStore::new(
            self.core.executions.clone(),
            SqlHistoryManager::new(self.core.history_v2.clone(), shard.shard_id()),
            self.core.domains.clone(),
            Arc::new(move || range_shard.range_id()),
            Arc::new(move || id_shard.generate_task_id().map_err(shard_err)),
            EngineNdcContextHooks::new(self.core.clone()),
        );
        let mut ctx = WorkflowContext::new(key.clone(), store);
        ctx.history_size = state.history_size;
        ctx.mutable_state = Some(state);
        ctx
    }

    fn finish(
        ctx: &mut WorkflowContext<NdcStateBox, SqlWorkflowContextStore<NdcStateBox, EngineNdcContextHooks>>,
    ) -> HResult<NdcStateBox> {
        let mut state = ctx
            .mutable_state
            .take()
            .ok_or_else(|| internal("mutable state missing after a successful write"))?;
        state.history_size = ctx.history_size;
        Ok(state)
    }
}

impl NdcWriter for EngineNdcWriter {
    fn reapply_events(&self, _key: &WorkflowExecutionKey, batches: &[WorkflowEvents]) -> HResult<()> {
        block_on(reapply_via_engine(&self.core, batches))
    }

    fn write_create(
        &self,
        key: &WorkflowExecutionKey,
        state: NdcStateBox,
        snapshot: &mut pc::WorkflowSnapshot,
        history: PersistedBlob,
        mode: pc::CreateWorkflowMode,
        previous_run_id: &str,
        previous_last_write_version: i64,
        request_mode: pc::WorkflowRequestMode,
    ) -> HResult<NdcStateBox> {
        let mut ctx = self.context(key, state);
        block_on(ctx.create_workflow_execution(
            snapshot,
            history,
            mode,
            previous_run_id,
            previous_last_write_version,
            request_mode,
        ))?;
        Self::finish(&mut ctx)
    }

    fn write_update(
        &self,
        key: &WorkflowExecutionKey,
        state: NdcStateBox,
        now_nanos: i64,
        mode: pc::UpdateWorkflowMode,
        policy: TransactionPolicy,
        request_mode: pc::WorkflowRequestMode,
    ) -> HResult<NdcStateBox> {
        let mut ctx = self.context(key, state);
        block_on(ctx.update_workflow_execution_with_new(now_nanos, mode, None, policy, None, request_mode))?;
        Self::finish(&mut ctx)
    }

    fn write_update_with_new(
        &self,
        key: &WorkflowExecutionKey,
        state: NdcStateBox,
        new_key: &WorkflowExecutionKey,
        new_state: NdcStateBox,
        now_nanos: i64,
        mode: pc::UpdateWorkflowMode,
        policy: TransactionPolicy,
        new_policy: TransactionPolicy,
        request_mode: pc::WorkflowRequestMode,
    ) -> HResult<(NdcStateBox, NdcStateBox)> {
        let mut ctx = self.context(key, state);
        let mut new_ctx = self.context(new_key, new_state);
        block_on(ctx.update_workflow_execution_with_new(
            now_nanos,
            mode,
            Some(&mut new_ctx),
            policy,
            Some(new_policy),
            request_mode,
        ))?;
        Ok((Self::finish(&mut ctx)?, Self::finish(&mut new_ctx)?))
    }

    fn write_conflict_resolve(
        &self,
        key: &WorkflowExecutionKey,
        reset: NdcStateBox,
        new: Option<(WorkflowExecutionKey, NdcStateBox)>,
        current: Option<(WorkflowExecutionKey, NdcStateBox)>,
        now_nanos: i64,
        mode: pc::ConflictResolveWorkflowMode,
        current_policy: Option<TransactionPolicy>,
    ) -> HResult<(NdcStateBox, Option<NdcStateBox>, Option<NdcStateBox>)> {
        let mut reset = reset;
        // The reset workflow's state is passed separately (Go passes
        // `resetMutableState`); the context only supplies the history size.
        let mut ctx = self.context(key, NdcStateBox { ms: placeholder_state_for(key), history_size: reset.history_size });
        ctx.mutable_state = None;
        let mut new_ctx = new.map(|(k, s)| self.context(&k, s));
        let mut current_ctx = current.map(|(k, s)| self.context(&k, s));
        block_on(ctx.conflict_resolve_workflow_execution(
            now_nanos,
            mode,
            &mut reset,
            new_ctx.as_mut(),
            current_ctx.as_mut(),
            current_policy,
        ))?;
        reset.history_size = ctx.history_size;
        let new_state = new_ctx.as_mut().map(Self::finish).transpose()?;
        let current_state = current_ctx.as_mut().map(Self::finish).transpose()?;
        Ok((reset, new_state, current_state))
    }
}

fn placeholder_state_for(key: &WorkflowExecutionKey) -> Box<dyn NdcMutableState> {
    placeholder_state(
        &key.domain_id,
        &cadence_api::types::WorkflowExecution { workflow_id: key.workflow_id.clone(), run_id: key.run_id.clone() },
        0,
    )
}

/// Placeholder state used while the real state is lent to a write.
pub(super) fn placeholder_state(
    domain_id: &str,
    execution: &cadence_api::types::WorkflowExecution,
    now_nanos: i64,
) -> Box<dyn NdcMutableState> {
    Box::new(OwnedNdcMutableState::new(
        domain_id,
        execution,
        now_nanos,
        crate::execution::mutable_state_methods_common::TaskGenEnv::default(),
    ))
}
