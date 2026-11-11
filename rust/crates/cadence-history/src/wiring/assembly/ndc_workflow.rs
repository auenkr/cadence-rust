//! group: assembly/ndc_workflow
//!
//! NDC `Workflow` shell (Go `ndc/workflow.go` + `execution/workflow.go`) and the
//! `NdcExecutionCache` adapter over the shared engine cache.
//!
//! The adapter acquires the *same* per-workflow lock as the engine
//! (`EngineExecutionCache::get_or_create`: current-run resolution identical,
//! never a second lock table) and owns the `ReleaseFunc` until `release` (or
//! `Drop`, e.g. on panic) runs it exactly once. The NDC working state is an
//! exclusively owned `OwnedNdcMutableState` built from the clean persisted
//! snapshot while the lock is held; a dirty engine transaction is rejected.
//!
//! Create, active/passive update, two-run (continued-as-new) update and
//! conflict-resolve persistence go through the shared `WorkflowContext`
//! machinery (`ndc_context_hooks`). `reapply_events` goes through the engine (Go
//! `contextImpl.ReapplyEvents`).
//! Not ported here: `StartTransaction` (active-cluster `UpdateCurrentVersion`
//! and decision failover on load), pending activity/timer/child/cancel/signal
//! maps in the loaded snapshot (explicit error), and event-generating policy
//! operations (decision fail/schedule, terminate) in `OwnedNdcMutableState`.

#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::types::WorkflowExecution;
use cadence_persistence::history_contract as pc;

use crate::engine::deps::{HResult, HistoryError, ReleaseFunc, TimeSource};
use crate::execution::context::{PersistedBlob, WorkflowEvents, WorkflowExecutionKey};
use crate::execution::mutable_state_bridge::snapshot_to_envelope;
use crate::execution::mutable_state_methods_common::TaskGenEnv;
use crate::execution::workflow::{
    workflow_happens_after, TransactionPolicy, Workflow, WorkflowState, WorkflowStateAccess, WorkflowVectorClock,
};
use crate::ndc::{
    NdcClusterMetadata, NdcError, NdcExecutionCache, NdcLentState, NdcMutableState, NdcResult, NdcWorkflow,
};
use crate::wiring::assembly::engine_cache::{EngineExecutionCache, EngineExecutionContext};
use crate::wiring::assembly::ndc_adapters::EngineNdcClusterMetadata;
use crate::wiring::assembly::ndc_context_hooks::{placeholder_state, EngineNdcWriter, NdcStateBox, NdcWriter};
use crate::wiring::assembly::ndc_state::OwnedNdcMutableState;
use crate::wiring::persistence::engine_ndc::block_on;

// ------------------------------------------------------------------ checkout seam

/// A cached context checked out under the workflow lock.
#[async_trait]
pub trait NdcCheckoutContext: Send + Sync {
    /// Clean persisted snapshot (caller owns the lock). Dirty state is an error.
    async fn clean_snapshot(&self) -> HResult<pc::WorkflowSnapshot>;
    async fn persist_history_batch(&self, events: &WorkflowEvents, new_branch: bool) -> HResult<PersistedBlob>;
    /// Go `Context.Clear`.
    fn clear(&self);
}

/// Source of locked contexts (the shared engine cache in production).
#[async_trait]
pub trait NdcCheckoutSource: Send + Sync {
    async fn checkout(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(Arc<dyn NdcCheckoutContext>, WorkflowExecution, ReleaseFunc)>;
    /// Single-run create/update persistence (shard-side writes).
    fn writer(&self) -> Arc<dyn NdcWriter>;
}

#[async_trait]
impl NdcCheckoutContext for EngineExecutionContext {
    async fn clean_snapshot(&self) -> HResult<pc::WorkflowSnapshot> {
        self.clean_snapshot_for_ndc().await
    }
    async fn persist_history_batch(&self, events: &WorkflowEvents, new_branch: bool) -> HResult<PersistedBlob> {
        self.persist_ndc_history_batch(events, new_branch).await
    }
    fn clear(&self) {
        EngineExecutionContext::clear(self)
    }
}

#[async_trait]
impl NdcCheckoutSource for EngineExecutionCache {
    async fn checkout(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(Arc<dyn NdcCheckoutContext>, WorkflowExecution, ReleaseFunc)> {
        // `get_or_create` validates (resolves an empty run id to the current
        // run without taking any lock) and then acquires the shared lock.
        let resolved = self.validate(domain_id, execution).await?;
        let (context, release) = self.acquire(domain_id, &resolved, false).await?;
        Ok((context, resolved, release))
    }
    fn writer(&self) -> Arc<dyn NdcWriter> {
        Arc::new(EngineNdcWriter::new(self.core()))
    }
}

pub(super) fn history_to_ndc(err: HistoryError) -> NdcError {
    if err.is_entity_not_exists() {
        return NdcError::EntityNotExists(err.to_string());
    }
    if matches!(err, HistoryError::PersistenceAlreadyStarted(_)) {
        return NdcError::WorkflowExecutionAlreadyStarted(err.to_string());
    }
    NdcError::Other(err.to_string())
}


fn lend_other(w: &mut Option<&mut dyn NdcWorkflow>) -> NdcResult<Option<(WorkflowExecutionKey, NdcStateBox)>> {
    match w.as_mut() {
        Some(w) => {
            let l = w.lend_state()?;
            let k = WorkflowExecutionKey::new(l.domain_id.clone(), &l.execution);
            Ok(Some((k, NdcStateBox { ms: l.ms, history_size: l.history_size })))
        }
        None => Ok(None),
    }
}

// ------------------------------------------------------------------ state loading

/// Go `mutableStateBuilder.Load` over the clean persisted snapshot. Pending
/// maps are refused (explicitly) until the lossless codec lands: dropping them
/// would lose data.
pub(super) fn state_from_snapshot(
    domain_id: &str,
    execution: &WorkflowExecution,
    now_nanos: i64,
    snapshot: pc::WorkflowSnapshot,
) -> NdcResult<Box<dyn NdcMutableState>> {
    if !snapshot.activity_infos.is_empty()
        || !snapshot.timer_infos.is_empty()
        || !snapshot.child_execution_infos.is_empty()
        || !snapshot.request_cancel_infos.is_empty()
        || !snapshot.signal_infos.is_empty()
        || !snapshot.semaphore_infos.is_empty()
    {
        return Err(NdcError::internal(
            "TODO(ndc-persistence): loading pending activity/timer/child/cancel/signal/semaphore maps into NDC state is not implemented",
        ));
    }
    let envelope = snapshot_to_envelope(snapshot.clone()).map_err(history_to_ndc)?;
    let info = snapshot.execution_info.clone();
    let next_event_id = info.next_event_id;
    let history_size = info.history_size;
    let mut state = OwnedNdcMutableState::new(domain_id, execution, now_nanos, TaskGenEnv::default());
    state.typed_mut().load(
        info,
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        snapshot.signal_requested_ids.clone(),
        envelope.buffered_events,
        envelope.version_histories,
        history_size,
    );
    state.set_update_condition(next_event_id);
    Ok(Box::new(state))
}

// ------------------------------------------------------------------ policy adapter

/// `WorkflowStateAccess` over the dyn NDC state (policy lives in execution/workflow.rs).
struct PolicyAccess<'a> {
    ms: &'a mut dyn NdcMutableState,
    cluster: &'a dyn NdcClusterMetadata,
    current_cluster: String,
    /// `has_*` accessors are infallible in the policy trait; keep the first error.
    deferred: RefCell<Option<NdcError>>,
}

impl<'a> PolicyAccess<'a> {
    fn new(ms: &'a mut dyn NdcMutableState, cluster: &'a dyn NdcClusterMetadata) -> Self {
        let current_cluster = cluster.get_current_cluster_name();
        Self { ms, cluster, current_cluster, deferred: RefCell::new(None) }
    }

    fn bool_or_defer(&self, r: NdcResult<bool>) -> bool {
        match r {
            Ok(v) => v,
            Err(e) => {
                self.deferred.borrow_mut().get_or_insert(e);
                false
            }
        }
    }

    fn check<T>(&self, r: NdcResult<T>) -> NdcResult<T> {
        let r = r?;
        match self.deferred.borrow_mut().take() {
            Some(e) => Err(e),
            None => Ok(r),
        }
    }
}

impl WorkflowStateAccess for PolicyAccess<'_> {
    type Error = NdcError;

    fn vector_clock(&self) -> NdcResult<WorkflowVectorClock> {
        let (policy, last_event_task_id) = self.ms.vector_clock_inputs()?;
        let info = self.ms.get_execution_info();
        Ok(WorkflowVectorClock {
            active_cluster_selection_policy: policy,
            last_write_version: self.ms.get_last_write_version()?,
            last_event_task_id,
            start_timestamp_unix_nanos: info.start_timestamp_nanos,
            run_id: info.run_id,
        })
    }
    fn state(&self) -> WorkflowState {
        self.ms.get_workflow_state()
    }
    fn is_running(&self) -> bool {
        self.ms.is_workflow_execution_running()
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.bool_or_defer(self.ms.has_processed_or_pending_decision())
    }
    fn has_buffered_events(&self) -> bool {
        self.ms.has_buffered_events()
    }
    fn has_in_flight_decision(&self) -> bool {
        self.bool_or_defer(self.ms.has_in_flight_decision())
    }
    fn next_event_id(&self) -> i64 {
        self.ms.get_next_event_id()
    }
    fn cluster_for_version(&self, version: i64) -> NdcResult<String> {
        self.cluster.cluster_name_for_failover_version(version)
    }
    fn current_cluster(&self) -> &str {
        &self.current_cluster
    }
    fn internal_error(&self, message: &'static str) -> NdcError {
        NdcError::internal(message)
    }
    fn update_state(&mut self, state: WorkflowState) -> NdcResult<()> {
        self.ms.update_workflow_state(state)
    }
    fn update_current_version(&mut self, version: i64, force: bool) -> NdcResult<()> {
        self.ms.update_current_version(version, force)
    }
    fn fail_in_flight_decision_for_failover(&mut self) -> NdcResult<()> {
        self.ms.fail_in_flight_decision_for_failover()
    }
    fn schedule_decision(&mut self) -> NdcResult<()> {
        self.ms.schedule_decision()
    }
    fn terminate(&mut self, batch_first_event_id: i64, reason: &str, details: Vec<u8>, identity: &str) -> NdcResult<()> {
        self.ms.terminate_workflow_execution(batch_first_event_id, reason, details, identity)
    }
}

// ------------------------------------------------------------------ workflow

/// Go `ndc.Workflow` (`execution.Workflow` + context + release function).
pub struct EngineNdcWorkflow {
    domain_id: String,
    execution: WorkflowExecution,
    ms: Box<dyn NdcMutableState>,
    /// False after `clear`/before the first `load_workflow_execution`.
    loaded: bool,
    history_size: i64,
    context: Option<Arc<dyn NdcCheckoutContext>>,
    writer: Arc<dyn NdcWriter>,
    /// Taken exactly once by `release` / `Drop`.
    release_fn: Option<ReleaseFunc>,
    cluster: Arc<EngineNdcClusterMetadata>,
    clock: Arc<dyn TimeSource>,
}

impl EngineNdcWorkflow {
    pub fn history_size(&self) -> i64 {
        self.history_size
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded
    }

    fn write_key(&self) -> WorkflowExecutionKey {
        WorkflowExecutionKey::new(self.domain_id.clone(), &self.execution)
    }

    /// Lend the state to a write; a placeholder stays behind until `finish_write`.
    fn take_state(&mut self) -> NdcStateBox {
        let placeholder = placeholder_state(&self.domain_id, &self.execution, self.clock.now());
        NdcStateBox { ms: std::mem::replace(&mut self.ms, placeholder), history_size: self.history_size }
    }

    /// Success: keep the committed state and drop the engine's cached snapshot
    /// (it is stale after our write). Failure: Go `Context.Clear` so the next
    /// load rebuilds from the database.
    fn finish_write(&mut self, result: HResult<NdcStateBox>) -> NdcResult<()> {
        match result {
            Ok(state) => {
                self.ms = state.ms;
                self.history_size = state.history_size;
                self.loaded = true;
                if let Some(ctx) = &self.context {
                    ctx.clear();
                }
                Ok(())
            }
            Err(err) => {
                self.clear();
                Err(history_to_ndc(err))
            }
        }
    }

    fn run_release(&mut self, err: Option<&NdcError>) {
        if let Some(release) = self.release_fn.take() {
            let he = err.map(|e| HistoryError::internal(e.to_string()));
            release(he.as_ref());
        }
    }
}

impl Drop for EngineNdcWorkflow {
    fn drop(&mut self) {
        // A workflow dropped without `release` (early return, panic) must not
        // leave the workflow lock held; treat it as a failed operation.
        if self.release_fn.is_some() {
            self.run_release(Some(&NdcError::internal("ndc workflow dropped without release")));
        }
    }
}

impl NdcWorkflow for EngineNdcWorkflow {
    fn mutable_state(&self) -> &dyn NdcMutableState {
        self.ms.as_ref()
    }
    fn mutable_state_mut(&mut self) -> &mut dyn NdcMutableState {
        self.ms.as_mut()
    }
    fn replace_mutable_state(&mut self, ms: Box<dyn NdcMutableState>) {
        self.ms = ms;
        self.loaded = true;
    }
    /// Go `Workflow.Clear` -> `Context.Clear`: the next `load_workflow_execution`
    /// reads persistence again.
    fn clear(&mut self) {
        if let Some(ctx) = &self.context {
            ctx.clear();
        }
        self.loaded = false;
        self.history_size = 0;
    }
    fn set_history_size(&mut self, size: i64) {
        self.history_size = size;
    }
    fn release(&mut self, err: Option<&NdcError>) {
        self.run_release(err);
    }

    fn get_vector_clock(&self) -> NdcResult<WorkflowVectorClock> {
        let (policy, last_event_task_id) = self.ms.vector_clock_inputs()?;
        let info = self.ms.get_execution_info();
        Ok(WorkflowVectorClock {
            active_cluster_selection_policy: policy,
            last_write_version: self.ms.get_last_write_version()?,
            last_event_task_id,
            start_timestamp_unix_nanos: info.start_timestamp_nanos,
            run_id: info.run_id,
        })
    }
    fn happens_after(&self, other: &dyn NdcWorkflow) -> NdcResult<bool> {
        Ok(workflow_happens_after(&self.get_vector_clock()?, &other.get_vector_clock()?))
    }
    fn suppress_by(&mut self, other: &dyn NdcWorkflow) -> NdcResult<TransactionPolicy> {
        let incoming = other.get_vector_clock()?;
        let access = PolicyAccess::new(self.ms.as_mut(), self.cluster.as_ref());
        let mut wf = Workflow { state: access };
        let r = wf.suppress_by(&incoming);
        wf.state.check(r)
    }
    fn revive(&mut self) -> NdcResult<()> {
        let access = PolicyAccess::new(self.ms.as_mut(), self.cluster.as_ref());
        let mut wf = Workflow { state: access };
        let r = wf.revive();
        wf.state.check(r)
    }
    fn flush_buffered_events(&mut self) -> NdcResult<()> {
        let access = PolicyAccess::new(self.ms.as_mut(), self.cluster.as_ref());
        let mut wf = Workflow { state: access };
        let r = wf.flush_buffered_events();
        wf.state.check(r)
    }

    /// Go `UpdateWorkflowExecutionAsActive`.
    fn update_workflow_execution_as_active(&mut self, now_nanos: i64) -> NdcResult<()> {
        self.update_workflow_execution_with_new(
            now_nanos,
            pc::UpdateWorkflowMode::UpdateCurrent,
            None,
            TransactionPolicy::Active,
            None,
            pc::WorkflowRequestMode::New,
        )
    }
    fn lend_state(&mut self) -> NdcResult<NdcLentState> {
        let key = self.write_key();
        let state = self.take_state();
        Ok(NdcLentState {
            domain_id: key.domain_id,
            execution: self.execution.clone(),
            ms: state.ms,
            history_size: state.history_size,
        })
    }
    fn restore_state(&mut self, ms: Box<dyn NdcMutableState>, history_size: i64) {
        self.finish_write(Ok(NdcStateBox { ms, history_size })).ok();
    }
    fn update_workflow_execution_as_passive(&mut self, now_nanos: i64) -> NdcResult<()> {
        self.update_workflow_execution_with_new(
            now_nanos,
            pc::UpdateWorkflowMode::UpdateCurrent,
            None,
            TransactionPolicy::Passive,
            None,
            pc::WorkflowRequestMode::Replicated,
        )
    }
    fn update_workflow_execution_with_new_as_passive(
        &mut self,
        now_nanos: i64,
        new_workflow: &mut dyn NdcWorkflow,
    ) -> NdcResult<()> {
        self.update_workflow_execution_with_new(
            now_nanos,
            pc::UpdateWorkflowMode::UpdateCurrent,
            Some(new_workflow),
            TransactionPolicy::Passive,
            Some(TransactionPolicy::Passive),
            pc::WorkflowRequestMode::Replicated,
        )
    }
    fn update_workflow_execution_with_new(
        &mut self,
        now_nanos: i64,
        mode: pc::UpdateWorkflowMode,
        new_workflow: Option<&mut dyn NdcWorkflow>,
        current_policy: TransactionPolicy,
        new_policy: Option<TransactionPolicy>,
        request_mode: pc::WorkflowRequestMode,
    ) -> NdcResult<()> {
        let (new_workflow, new_policy) = match (new_workflow, new_policy) {
            (Some(n), Some(p)) => (n, p),
            (None, None) => {
                let key = self.write_key();
                let state = self.take_state();
                let writer = self.writer.clone();
                let result = writer.write_update(&key, state, now_nanos, mode, current_policy, request_mode);
                return self.finish_write(result);
            }
            _ => {
                return Err(NdcError::internal(
                    "update_workflow_execution_with_new: new workflow and new policy must be given together",
                ))
            }
        };
        let new_lent = new_workflow.lend_state()?;
        let new_key = WorkflowExecutionKey::new(new_lent.domain_id.clone(), &new_lent.execution);
        let new_state = NdcStateBox { ms: new_lent.ms, history_size: new_lent.history_size };
        let key = self.write_key();
        let state = self.take_state();
        let writer = self.writer.clone();
        let result = writer.write_update_with_new(
            &key,
            state,
            &new_key,
            new_state,
            now_nanos,
            mode,
            current_policy,
            new_policy,
            request_mode,
        );
        match result {
            Ok((current, new)) => {
                new_workflow.restore_state(new.ms, new.history_size);
                self.finish_write(Ok(current))
            }
            Err(err) => {
                // Go clears both contexts on failure.
                new_workflow.clear();
                self.finish_write(Err(err))
            }
        }
    }
    fn conflict_resolve_workflow_execution(
        &mut self,
        now_nanos: i64,
        mode: pc::ConflictResolveWorkflowMode,
        new_workflow: Option<&mut dyn NdcWorkflow>,
        current_workflow: Option<&mut dyn NdcWorkflow>,
        current_policy: Option<TransactionPolicy>,
    ) -> NdcResult<()> {
        if current_workflow.is_some() != current_policy.is_some() {
            return Err(NdcError::internal(
                "conflict_resolve_workflow_execution: current workflow and current policy must be given together",
            ));
        }
        let mut new_workflow = new_workflow;
        let mut current_workflow = current_workflow;
        let new_lent = lend_other(&mut new_workflow)?;
        let current_lent = lend_other(&mut current_workflow)?;
        let key = self.write_key();
        let reset = self.take_state();
        let writer = self.writer.clone();
        let result = writer.write_conflict_resolve(&key, reset, new_lent, current_lent, now_nanos, mode, current_policy);
        match result {
            Ok((reset, new, current)) => {
                if let (Some(w), Some(s)) = (new_workflow, new) {
                    w.restore_state(s.ms, s.history_size);
                }
                if let (Some(w), Some(s)) = (current_workflow, current) {
                    w.restore_state(s.ms, s.history_size);
                }
                self.finish_write(Ok(reset))
            }
            Err(err) => {
                if let Some(w) = new_workflow {
                    w.clear();
                }
                if let Some(w) = current_workflow {
                    w.clear();
                }
                self.finish_write(Err(err))
            }
        }
    }
    fn persist_start_workflow_batch_events(&mut self, events: &WorkflowEvents) -> NdcResult<PersistedBlob> {
        let context = self.context.as_ref().ok_or_else(|| NdcError::internal("ndc workflow has no persistence context"))?;
        block_on(context.persist_history_batch(events, true)).map_err(history_to_ndc)
    }
    fn persist_non_start_workflow_batch_events(&mut self, events: &WorkflowEvents) -> NdcResult<PersistedBlob> {
        let context = self.context.as_ref().ok_or_else(|| NdcError::internal("ndc workflow has no persistence context"))?;
        block_on(context.persist_history_batch(events, false)).map_err(history_to_ndc)
    }
    fn create_workflow_execution(
        &mut self,
        mut snapshot: pc::WorkflowSnapshot,
        history_blob: PersistedBlob,
        mode: pc::CreateWorkflowMode,
        prev_run_id: &str,
        prev_last_write_version: i64,
        request_mode: pc::WorkflowRequestMode,
    ) -> NdcResult<()> {
        let key = self.write_key();
        let state = self.take_state();
        let writer = self.writer.clone();
        let result = writer.write_create(
            &key,
            state,
            &mut snapshot,
            history_blob,
            mode,
            prev_run_id,
            prev_last_write_version,
            request_mode,
        );
        self.finish_write(result)
    }
    fn reapply_events(&mut self, events: &[WorkflowEvents]) -> NdcResult<()> {
        let key = WorkflowExecutionKey::new(self.domain_id.clone(), &self.execution);
        self.writer.reapply_events(&key, events).map_err(history_to_ndc)
    }

    /// Go `LoadWorkflowExecution`. Requires the lock-owning context; a loaded
    /// state is kept (Go: `if c.mutableState == nil`).
    fn load_workflow_execution(&mut self, task_version: Option<i64>) -> NdcResult<()> {
        if task_version.is_some() {
            return Err(NdcError::internal(
                "TODO(ndc-persistence): load with incoming task version needs StartTransaction decision failover",
            ));
        }
        if self.loaded {
            return Ok(());
        }
        let Some(context) = self.context.clone() else {
            return Err(NdcError::internal("ndc workflow has no context to load from"));
        };
        let snapshot = block_on(context.clean_snapshot()).map_err(history_to_ndc)?;
        self.history_size = snapshot.execution_info.history_size;
        self.ms = state_from_snapshot(&self.domain_id, &self.execution, self.clock.now(), snapshot)?;
        self.loaded = true;
        Ok(())
    }
}

// ------------------------------------------------------------------ cache adapter

/// `NdcExecutionCache` over the shared engine cache lock.
pub struct EngineNdcExecutionCache {
    source: Arc<dyn NdcCheckoutSource>,
    cluster: Arc<EngineNdcClusterMetadata>,
    clock: Arc<dyn TimeSource>,
}

impl EngineNdcExecutionCache {
    pub fn new(
        source: Arc<dyn NdcCheckoutSource>,
        cluster: Arc<EngineNdcClusterMetadata>,
        clock: Arc<dyn TimeSource>,
    ) -> Self {
        Self { source, cluster, clock }
    }

    fn empty_state(&self, domain_id: &str, execution: &WorkflowExecution) -> Box<dyn NdcMutableState> {
        Box::new(OwnedNdcMutableState::new(domain_id, execution, self.clock.now(), TaskGenEnv::default()))
    }
}

impl NdcExecutionCache for EngineNdcExecutionCache {
    /// Returns an *unloaded* workflow holding the lock (Go returns an unloaded
    /// context); callers must `load_workflow_execution`.
    fn get_or_create_workflow_execution(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> NdcResult<Box<dyn NdcWorkflow>> {
        let (context, resolved, release) =
            block_on(self.source.checkout(domain_id, execution)).map_err(history_to_ndc)?;
        Ok(Box::new(EngineNdcWorkflow {
            domain_id: domain_id.to_string(),
            ms: self.empty_state(domain_id, &resolved),
            execution: resolved,
            loaded: false,
            history_size: 0,
            context: Some(context),
            writer: self.source.writer(),
            release_fn: Some(release),
            cluster: self.cluster.clone(),
            clock: self.clock.clone(),
        }))
    }

    /// Go `NewContext` + `NewWorkflow(..., NoopReleaseFn)`: not cached, no lock.
    fn new_workflow(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
        mutable_state: Box<dyn NdcMutableState>,
        _release_noop: bool,
    ) -> NdcResult<Box<dyn NdcWorkflow>> {
        Ok(Box::new(EngineNdcWorkflow {
            domain_id: domain_id.to_string(),
            execution: execution.clone(),
            ms: mutable_state,
            loaded: true,
            history_size: 0,
            context: None,
            writer: self.source.writer(),
            release_fn: None,
            cluster: self.cluster.clone(),
            clock: self.clock.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::sync::OwnedMutexGuard;

    use super::*;
    use crate::engine::deps::ClusterMetadata;
    use crate::execution::mutable_state_methods_common::WORKFLOW_STATE_ZOMBIE;

    struct FakeContext {
        persisted: Mutex<pc::WorkflowSnapshot>,
        clears: AtomicUsize,
    }

    #[async_trait]
    impl NdcCheckoutContext for FakeContext {
        async fn clean_snapshot(&self) -> HResult<pc::WorkflowSnapshot> {
            Ok(self.persisted.lock().unwrap().clone())
        }
        async fn persist_history_batch(&self, _events: &WorkflowEvents, _new_branch: bool) -> HResult<PersistedBlob> {
            Err(HistoryError::internal("fake history append is not configured"))
        }
        fn clear(&self) {
            self.clears.fetch_add(1, Ordering::SeqCst);
        }
    }

    type Contexts = Arc<Mutex<HashMap<String, Arc<FakeContext>>>>;

    /// In-memory "database" write path with Go's CAS on `next_event_id`.
    struct FakeWriter {
        contexts: Contexts,
    }

    impl NdcWriter for FakeWriter {
        fn write_create(
            &self,
            key: &WorkflowExecutionKey,
            state: NdcStateBox,
            snapshot: &mut pc::WorkflowSnapshot,
            _history: PersistedBlob,
            _mode: pc::CreateWorkflowMode,
            _previous_run_id: &str,
            _previous_last_write_version: i64,
            _request_mode: pc::WorkflowRequestMode,
        ) -> HResult<NdcStateBox> {
            let mut contexts = self.contexts.lock().unwrap();
            if contexts.contains_key(&key.run_id) {
                return Err(HistoryError::ConditionFailed("run already exists".into()));
            }
            contexts.insert(
                key.run_id.clone(),
                Arc::new(FakeContext { persisted: Mutex::new(snapshot.clone()), clears: AtomicUsize::new(0) }),
            );
            Ok(state)
        }

        fn write_update(
            &self,
            key: &WorkflowExecutionKey,
            state: NdcStateBox,
            _now_nanos: i64,
            _mode: pc::UpdateWorkflowMode,
            _policy: TransactionPolicy,
            _request_mode: pc::WorkflowRequestMode,
        ) -> HResult<NdcStateBox> {
            let ctx = self.contexts.lock().unwrap().get(&key.run_id).cloned();
            let Some(ctx) = ctx else { return Err(HistoryError::entity_not_exists("no such run")) };
            let mut db = ctx.persisted.lock().unwrap();
            if db.execution_info.next_event_id != state.ms.get_update_condition() {
                return Err(HistoryError::ConditionFailed("next event id mismatch".into()));
            }
            db.execution_info.next_event_id = state.ms.get_next_event_id();
            if state.ms.get_workflow_state() == WorkflowState::Zombie {
                db.execution_info.state = WORKFLOW_STATE_ZOMBIE;
            }
            Ok(state)
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
            _new_policy: TransactionPolicy,
            request_mode: pc::WorkflowRequestMode,
        ) -> HResult<(NdcStateBox, NdcStateBox)> {
            // Atomic: validate the new run first so a failure writes nothing.
            if self.contexts.lock().unwrap().contains_key(&new_key.run_id) {
                return Err(HistoryError::ConditionFailed("new run already exists".into()));
            }
            let state = self.write_update(key, state, now_nanos, mode, policy, request_mode)?;
            self.contexts.lock().unwrap().insert(
                new_key.run_id.clone(),
                Arc::new(FakeContext {
                    persisted: Mutex::new(snapshot(new_state.ms.get_next_event_id())),
                    clears: AtomicUsize::new(0),
                }),
            );
            Ok((state, new_state))
        }

        fn write_conflict_resolve(
            &self,
            key: &WorkflowExecutionKey,
            reset: NdcStateBox,
            _new: Option<(WorkflowExecutionKey, NdcStateBox)>,
            _current: Option<(WorkflowExecutionKey, NdcStateBox)>,
            _now_nanos: i64,
            _mode: pc::ConflictResolveWorkflowMode,
            _current_policy: Option<TransactionPolicy>,
        ) -> HResult<(NdcStateBox, Option<NdcStateBox>, Option<NdcStateBox>)> {
            let ctx = self.contexts.lock().unwrap().get(&key.run_id).cloned();
            let Some(ctx) = ctx else { return Err(HistoryError::entity_not_exists("no such run")) };
            let mut db = ctx.persisted.lock().unwrap();
            if db.execution_info.next_event_id != reset.ms.get_update_condition() {
                return Err(HistoryError::ConditionFailed("next event id mismatch".into()));
            }
            db.execution_info.next_event_id = reset.ms.get_next_event_id();
            Ok((reset, None, None))
        }
    }

    struct FakeSource {
        locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
        contexts: Contexts,
        releases: Arc<AtomicUsize>,
        writer: Arc<FakeWriter>,
    }

    impl Default for FakeSource {
        fn default() -> Self {
            let contexts: Contexts = Arc::default();
            Self {
                locks: Mutex::default(),
                writer: Arc::new(FakeWriter { contexts: contexts.clone() }),
                contexts,
                releases: Arc::default(),
            }
        }
    }

    impl FakeSource {
        fn context(&self, run_id: &str) -> Arc<FakeContext> {
            self.contexts
                .lock()
                .unwrap()
                .entry(run_id.to_string())
                .or_insert_with(|| {
                    Arc::new(FakeContext { persisted: Mutex::new(snapshot(5)), clears: AtomicUsize::new(0) })
                })
                .clone()
        }
    }

    #[async_trait]
    impl NdcCheckoutSource for FakeSource {
        async fn checkout(
            &self,
            _domain_id: &str,
            execution: &WorkflowExecution,
        ) -> HResult<(Arc<dyn NdcCheckoutContext>, WorkflowExecution, ReleaseFunc)> {
            let lock = self
                .locks
                .lock()
                .unwrap()
                .entry(execution.run_id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone();
            let guard: OwnedMutexGuard<()> = lock.lock_owned().await;
            let slot = Mutex::new(Some(guard));
            let ctx = self.context(&execution.run_id);
            let release_ctx = ctx.clone();
            let releases = self.releases.clone();
            let release: ReleaseFunc = Arc::new(move |err: Option<&HistoryError>| {
                if err.is_some() {
                    release_ctx.clear();
                }
                if slot.lock().unwrap().take().is_some() {
                    releases.fetch_add(1, Ordering::SeqCst);
                }
            });
            Ok((ctx, execution.clone(), release))
        }
        fn writer(&self) -> Arc<dyn NdcWriter> {
            self.writer.clone()
        }
    }

    struct Meta;
    impl ClusterMetadata for Meta {
        fn get_current_cluster_name(&self) -> String {
            "active".into()
        }
        fn cluster_name_for_failover_version(&self, _version: i64) -> HResult<String> {
            Ok("active".into())
        }
    }
    struct Clock;
    impl TimeSource for Clock {
        fn now(&self) -> i64 {
            1_000
        }
    }

    fn snapshot(next_event_id: i64) -> pc::WorkflowSnapshot {
        let mut s = pc::WorkflowSnapshot::default();
        s.execution_info.next_event_id = next_event_id;
        s
    }

    fn cache(source: Arc<FakeSource>) -> Arc<EngineNdcExecutionCache> {
        Arc::new(EngineNdcExecutionCache::new(
            source,
            Arc::new(EngineNdcClusterMetadata::new(Arc::new(Meta))),
            Arc::new(Clock),
        ))
    }

    fn exec(run: &str) -> WorkflowExecution {
        WorkflowExecution { workflow_id: "wf".into(), run_id: run.into() }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_acquire_serializes_on_one_lock() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        let c1 = cache.clone();
        let (first_tx, first_rx) = std::sync::mpsc::channel::<()>();
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let first = tokio::task::spawn_blocking(move || {
            let mut w = c1.get_or_create_workflow_execution("d", &exec("r1")).unwrap();
            held_tx.send(()).unwrap();
            first_rx.recv().unwrap();
            w.release(None);
        });
        tokio::task::spawn_blocking(move || held_rx.recv().unwrap()).await.unwrap();
        let got = Arc::new(AtomicBool::new(false));
        let (c2, g2) = (cache.clone(), got.clone());
        let second = tokio::task::spawn_blocking(move || {
            let mut w = c2.get_or_create_workflow_execution("d", &exec("r1")).unwrap();
            g2.store(true, Ordering::SeqCst);
            w.release(None);
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!got.load(Ordering::SeqCst), "second acquire must wait for the first release");
        first_tx.send(()).unwrap();
        first.await.unwrap();
        second.await.unwrap();
        assert!(got.load(Ordering::SeqCst));
        assert_eq!(source.releases.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn release_runs_exactly_once_on_error_and_drop() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        let c = cache.clone();
        let (ctx_clears, releases) = tokio::task::spawn_blocking(move || {
            let mut w = c.get_or_create_workflow_execution("d", &exec("r2")).unwrap();
            w.release(Some(&NdcError::internal("boom")));
            w.release(None);
            drop(w);
            // Dropped without release (panic/early return path) also releases once.
            let w2 = c.get_or_create_workflow_execution("d", &exec("r3")).unwrap();
            drop(w2);
            // The lock is free again.
            let mut w3 = c.get_or_create_workflow_execution("d", &exec("r3")).unwrap();
            w3.release(None);
            0
        })
        .await
        .map(|_| (source.context("r2").clears.load(Ordering::SeqCst), source.releases.load(Ordering::SeqCst)))
        .unwrap();
        assert_eq!(ctx_clears, 1, "error release clears the cached context once");
        assert_eq!(releases, 3, "r2 once, r3 dropped once, r3 reacquired once");
        assert_eq!(source.context("r3").clears.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clear_then_reload_sees_persisted_state() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        let ctx = source.context("r4");
        let c = cache.clone();
        let src = source.clone();
        tokio::task::spawn_blocking(move || {
            let mut w = c.get_or_create_workflow_execution("d", &exec("r4")).unwrap();
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 5);
            assert_eq!(w.mutable_state().get_update_condition(), 5);
            // Persistence moves on; the loaded copy is stale until cleared.
            src.context("r4").persisted.lock().unwrap().execution_info.next_event_id = 9;
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 5);
            w.clear();
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 9);
            w.release(None);
        })
        .await
        .unwrap();
        assert_eq!(ctx.clears.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persistence_methods_and_task_version_load_are_explicit_errors() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source);
        tokio::task::spawn_blocking(move || {
            let mut w = cache.get_or_create_workflow_execution("d", &exec("r5")).unwrap();
            assert!(w
                .update_workflow_execution_with_new(
                    1,
                    pc::UpdateWorkflowMode::UpdateCurrent,
                    None,
                    TransactionPolicy::Active,
                    Some(TransactionPolicy::Active),
                    pc::WorkflowRequestMode::New,
                )
                .is_err());
            assert!(w.reapply_events(&[]).is_err());
            assert!(w.load_workflow_execution(Some(3)).is_err());
            w.release(None);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_then_reload_sees_created_run() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        tokio::task::spawn_blocking(move || {
            let state = OwnedNdcMutableState::new("d", &exec("rc"), 1, TaskGenEnv::default());
            let mut w = cache.new_workflow("d", &exec("rc"), Box::new(state), true).unwrap();
            w.create_workflow_execution(
                snapshot(3),
                PersistedBlob::default(),
                pc::CreateWorkflowMode::BrandNew,
                "",
                0,
                pc::WorkflowRequestMode::Replicated,
            )
            .unwrap();
            w.release(None);
            let mut r = cache.get_or_create_workflow_execution("d", &exec("rc")).unwrap();
            r.load_workflow_execution(None).unwrap();
            assert_eq!(r.mutable_state().get_next_event_id(), 3);
            r.release(None);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn passive_update_persists_and_reloads() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        let ctx = source.context("ru");
        tokio::task::spawn_blocking(move || {
            let mut w = cache.get_or_create_workflow_execution("d", &exec("ru")).unwrap();
            w.load_workflow_execution(None).unwrap();
            w.mutable_state_mut().update_workflow_state(WorkflowState::Zombie).unwrap();
            w.update_workflow_execution_as_passive(1).unwrap();
            // Engine's cached snapshot is dropped after the write.
            w.clear();
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_workflow_state(), WorkflowState::Zombie);
            w.release(None);
        })
        .await
        .unwrap();
        assert!(ctx.clears.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cas_mismatch_returns_error_and_state_is_recoverable() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        let ctx = source.context("rm");
        tokio::task::spawn_blocking(move || {
            let mut w = cache.get_or_create_workflow_execution("d", &exec("rm")).unwrap();
            w.load_workflow_execution(None).unwrap();
            // Another writer moved the row.
            ctx.persisted.lock().unwrap().execution_info.next_event_id = 9;
            let err = w.update_workflow_execution_as_passive(1).unwrap_err();
            assert!(err.to_string().contains("next event id mismatch"), "{err}");
            assert_eq!(ctx.persisted.lock().unwrap().execution_info.next_event_id, 9);
            // Cleared: reload recovers the current persisted state.
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 9);
            assert_eq!(w.mutable_state().get_update_condition(), 9);
            w.release(None);
        })
        .await
        .unwrap();
    }

    fn owned(run: &str, next_event_id: i64) -> OwnedNdcMutableState {
        let mut state = OwnedNdcMutableState::new("d", &exec(run), 1, TaskGenEnv::default());
        state.set_update_condition(5);
        state.typed_mut().execution_info.next_event_id = next_event_id;
        state
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn continued_as_new_writes_both_runs_and_reloads_both() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        tokio::task::spawn_blocking(move || {
            let mut w = cache.get_or_create_workflow_execution("d", &exec("rcan")).unwrap();
            w.load_workflow_execution(None).unwrap();
            let mut new = cache.new_workflow("d", &exec("rcan2"), Box::new(owned("rcan2", 3)), true).unwrap();
            w.update_workflow_execution_with_new_as_passive(1, new.as_mut()).unwrap();
            assert_eq!(new.mutable_state().get_next_event_id(), 3);
            w.clear();
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 5);
            w.release(None);
            let mut r = cache.get_or_create_workflow_execution("d", &exec("rcan2")).unwrap();
            r.load_workflow_execution(None).unwrap();
            assert_eq!(r.mutable_state().get_next_event_id(), 3);
            r.release(None);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn continued_as_new_failure_writes_nothing_and_clears_both() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        let ctx = source.context("rcf");
        source.context("rcf2");
        tokio::task::spawn_blocking(move || {
            let mut w = cache.get_or_create_workflow_execution("d", &exec("rcf")).unwrap();
            w.load_workflow_execution(None).unwrap();
            // The new run already exists: the whole write fails.
            let mut new = cache.new_workflow("d", &exec("rcf2"), Box::new(owned("rcf2", 3)), true).unwrap();
            assert!(w.update_workflow_execution_with_new_as_passive(1, new.as_mut()).is_err());
            assert_eq!(ctx.persisted.lock().unwrap().execution_info.next_event_id, 5);
            // Both are cleared; the current run reloads from the database.
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 5);
            w.release(None);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conflict_resolve_replaces_state_and_cas_mismatch_is_recoverable() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source.clone());
        let ctx = source.context("rcr");
        tokio::task::spawn_blocking(move || {
            let mut w = cache.get_or_create_workflow_execution("d", &exec("rcr")).unwrap();
            w.load_workflow_execution(None).unwrap();
            w.replace_mutable_state(Box::new(owned("rcr", 8)));
            w.conflict_resolve_workflow_execution(1, pc::ConflictResolveWorkflowMode::UpdateCurrent, None, None, None)
                .unwrap();
            assert_eq!(ctx.persisted.lock().unwrap().execution_info.next_event_id, 8);
            w.clear();
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 8);
            // Another writer moved the row: CAS mismatch, then recover by reload.
            ctx.persisted.lock().unwrap().execution_info.next_event_id = 9;
            w.replace_mutable_state(Box::new(owned("rcr", 12)));
            let err = w
                .conflict_resolve_workflow_execution(1, pc::ConflictResolveWorkflowMode::UpdateCurrent, None, None, None)
                .unwrap_err();
            assert!(err.to_string().contains("next event id mismatch"), "{err}");
            assert_eq!(ctx.persisted.lock().unwrap().execution_info.next_event_id, 9);
            w.load_workflow_execution(None).unwrap();
            assert_eq!(w.mutable_state().get_next_event_id(), 9);
            w.release(None);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revive_moves_zombie_without_decision_to_created() {
        let source = Arc::new(FakeSource::default());
        let cache = cache(source);
        tokio::task::spawn_blocking(move || {
            let mut state = OwnedNdcMutableState::new("d", &exec("r6"), 1, TaskGenEnv::default());
            state.typed_mut().execution_info.state = WORKFLOW_STATE_ZOMBIE;
            let mut w = cache.new_workflow("d", &exec("r6"), Box::new(state), true).unwrap();
            assert_eq!(w.mutable_state().get_workflow_state(), WorkflowState::Zombie);
            w.revive().unwrap();
            assert_eq!(w.mutable_state().get_workflow_state(), WorkflowState::Created);
            w.release(None);
        })
        .await
        .unwrap();
    }
}
