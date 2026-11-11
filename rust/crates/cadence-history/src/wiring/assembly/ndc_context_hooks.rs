//! group: assembly/ndc_context_hooks
//!
//! Concrete [`WorkflowContextHooks`] for the NDC path (Go
//! `execution/context.go`: create / update persistence for a single run).
//!
//! The generic `WorkflowContext` + `SqlWorkflowContextStore` machinery does the
//! Go ordering (prepare -> write -> cleanup only after commit). These hooks add
//! the shard-side pieces: task id allocation before the write, max read level
//! + queue notification only after a durable write, and shard closing on
//! ownership loss. Anything that depends on unported code is an explicit error.
//!
//! Gaps (recorded in memory/history-ndcpersist2-20261111.md):
//! `verify_and_repair`/`flush_as_active` (error), signal reapplication (error),
//! history notifier (no-op), strong idempotency (Go default false), request type
//! decoding (-1), continued-as-new two-run writes (error).

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::Arc;

use cadence_persistence::history_contract::{self as pc, HistoryTaskCategory, Task};
use cadence_persistence::sqlblobs::{
    timer_task_from_struct, timer_task_to_struct, transfer_task_from_struct, transfer_task_to_struct, Encoding,
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
    /// Go default (`EnableStrongIdempotency` is off); config is not reachable here.
    fn strong_idempotency_check(&self, _domain: &str) -> bool {
        false
    }
    /// GAP: no decoder for the `WorkflowRequest` blob yet.
    fn request_type(&self, _request: &pc::WorkflowRequest) -> i32 {
        -1
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
    async fn verify_and_repair(&mut self, _state: &mut NdcStateBox) -> Result<bool, HistoryError> {
        Err(internal("TODO(ndc-persistence): verify_and_repair (checksum verification) is not ported"))
    }
    async fn flush_as_active(&mut self, _state: &mut NdcStateBox, _now: i64) -> Result<(), HistoryError> {
        Err(internal("TODO(ndc-persistence): flush_as_active (decision failover on load) is not ported"))
    }
    fn notify_snapshot(&mut self, _s: Option<&pc::WorkflowSnapshot>, _h: &[PersistedBlob], _err: bool) {}
    fn notify_mutation(&mut self, _m: &pc::WorkflowMutation, _h: &[PersistedBlob], _err: bool) {}
    /// GAP: no history notifier wiring (long-poll wakeups).
    fn notify_history(&mut self, _state: &NdcStateBox) {}
    fn emit_loaded_stats(&mut self, _domain: &str, _state: &NdcStateBox) {}
    fn emit_session_stats(&mut self, _domain: &str) {}
    fn emit_history_stats(&mut self, _domain: &str, _size: i64, _count: i64) {}
    fn emit_large_workflow_stats(&mut self, _d: &str, _blob: i64, _oc: i64, _os: i64, _nc: i64) {}
    fn emit_completion_stats(&mut self, _domain: &str, _state: &NdcStateBox) {}
    fn merge_continue_as_new_replication_tasks(
        &mut self,
        _mode: pc::UpdateWorkflowMode,
        _mutation: &mut pc::WorkflowMutation,
        new_workflow: Option<&mut pc::WorkflowSnapshot>,
    ) -> Result<(), HistoryError> {
        if new_workflow.is_some() {
            return Err(internal("TODO(ndc-persistence): continued-as-new two-run writes are not implemented"));
        }
        Ok(())
    }
    async fn reapply_events(&mut self, batches: &[WorkflowEvents]) -> Result<(), HistoryError> {
        let events = select_reapply_events(batches).map_err(internal)?;
        if events.is_empty() {
            return Ok(());
        }
        Err(internal("TODO(ndc-persistence): signal event reapplication is not implemented"))
    }

    fn allocate_task_ids(&mut self, tasks: &mut BTreeMap<HistoryTaskCategory, Vec<Task>>) -> Result<(), HistoryError> {
        let shard = &self.core.shard;
        for (category, list) in tasks.iter() {
            if !list.is_empty() && *category != TRANSFER && *category != TIMER {
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
            self.pending.max_read_level = max_read_level;
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

// ------------------------------------------------------------------ writer seam

/// Sync single-run create/update persistence (the NDC state is not `Send`, so
/// the write is driven with `block_on`). Production: [`EngineNdcWriter`].
pub trait NdcWriter: Send + Sync {
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
