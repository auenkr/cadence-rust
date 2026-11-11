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
//! Persistence methods (update/create/conflict-resolve/persist events/reapply)
//! are explicit errors until plan item 5a; they are never success no-ops.
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
use crate::execution::context::{PersistedBlob, WorkflowEvents};
use crate::execution::mutable_state_bridge::snapshot_to_envelope;
use crate::execution::mutable_state_methods_common::TaskGenEnv;
use crate::execution::workflow::{
    workflow_happens_after, TransactionPolicy, Workflow, WorkflowState, WorkflowStateAccess, WorkflowVectorClock,
};
use crate::ndc::{
    NdcClusterMetadata, NdcError, NdcExecutionCache, NdcMutableState, NdcResult, NdcWorkflow,
};
use crate::wiring::assembly::engine_cache::{EngineExecutionCache, EngineExecutionContext};
use crate::wiring::assembly::ndc_adapters::EngineNdcClusterMetadata;
use crate::wiring::assembly::ndc_state::OwnedNdcMutableState;
use crate::wiring::persistence::engine_ndc::block_on;

// ------------------------------------------------------------------ checkout seam

/// A cached context checked out under the workflow lock.
#[async_trait]
pub trait NdcCheckoutContext: Send + Sync {
    /// Clean persisted snapshot (caller owns the lock). Dirty state is an error.
    async fn clean_snapshot(&self) -> HResult<pc::WorkflowSnapshot>;
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
}

#[async_trait]
impl NdcCheckoutContext for EngineExecutionContext {
    async fn clean_snapshot(&self) -> HResult<pc::WorkflowSnapshot> {
        self.clean_snapshot_for_ndc().await
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
}

fn history_to_ndc(err: HistoryError) -> NdcError {
    if err.is_entity_not_exists() {
        return NdcError::EntityNotExists(err.to_string());
    }
    NdcError::Other(err.to_string())
}

fn unsupported<T>(what: &str) -> NdcResult<T> {
    Err(NdcError::internal(format!(
        "TODO(ndc-persistence): NdcWorkflow::{what} is not implemented until plan item 5a"
    )))
}

// ------------------------------------------------------------------ state loading

/// Go `mutableStateBuilder.Load` over the clean persisted snapshot. Pending
/// maps are refused (explicitly) until the lossless codec lands: dropping them
/// would lose data.
fn state_from_snapshot(
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

    fn update_workflow_execution_as_active(&mut self, _now_nanos: i64) -> NdcResult<()> {
        unsupported("update_workflow_execution_as_active")
    }
    fn update_workflow_execution_as_passive(&mut self, _now_nanos: i64) -> NdcResult<()> {
        unsupported("update_workflow_execution_as_passive")
    }
    fn update_workflow_execution_with_new_as_passive(
        &mut self,
        _now_nanos: i64,
        _new_workflow: &mut dyn NdcWorkflow,
    ) -> NdcResult<()> {
        unsupported("update_workflow_execution_with_new_as_passive")
    }
    fn update_workflow_execution_with_new(
        &mut self,
        _now_nanos: i64,
        _mode: pc::UpdateWorkflowMode,
        _new_workflow: Option<&mut dyn NdcWorkflow>,
        _current_policy: TransactionPolicy,
        _new_policy: Option<TransactionPolicy>,
        _request_mode: pc::WorkflowRequestMode,
    ) -> NdcResult<()> {
        unsupported("update_workflow_execution_with_new")
    }
    fn conflict_resolve_workflow_execution(
        &mut self,
        _now_nanos: i64,
        _mode: pc::ConflictResolveWorkflowMode,
        _new_workflow: Option<&mut dyn NdcWorkflow>,
        _current_workflow: Option<&mut dyn NdcWorkflow>,
        _current_policy: Option<TransactionPolicy>,
    ) -> NdcResult<()> {
        unsupported("conflict_resolve_workflow_execution")
    }
    fn persist_start_workflow_batch_events(&mut self, _events: &WorkflowEvents) -> NdcResult<PersistedBlob> {
        unsupported("persist_start_workflow_batch_events")
    }
    fn persist_non_start_workflow_batch_events(&mut self, _events: &WorkflowEvents) -> NdcResult<PersistedBlob> {
        unsupported("persist_non_start_workflow_batch_events")
    }
    fn create_workflow_execution(
        &mut self,
        _snapshot: pc::WorkflowSnapshot,
        _history_blob: PersistedBlob,
        _mode: pc::CreateWorkflowMode,
        _prev_run_id: &str,
        _prev_last_write_version: i64,
        _request_mode: pc::WorkflowRequestMode,
    ) -> NdcResult<()> {
        unsupported("create_workflow_execution")
    }
    fn reapply_events(&mut self, _events: &[WorkflowEvents]) -> NdcResult<()> {
        unsupported("reapply_events")
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
        fn clear(&self) {
            self.clears.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct FakeSource {
        locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
        contexts: Mutex<HashMap<String, Arc<FakeContext>>>,
        releases: Arc<AtomicUsize>,
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
        let mut first = tokio::task::spawn_blocking(move || c1.get_or_create_workflow_execution("d", &exec("r1")))
            .await
            .unwrap()
            .unwrap();
        let got = Arc::new(AtomicBool::new(false));
        let (c2, g2) = (cache.clone(), got.clone());
        let second = tokio::task::spawn_blocking(move || {
            let mut w = c2.get_or_create_workflow_execution("d", &exec("r1")).unwrap();
            g2.store(true, Ordering::SeqCst);
            w.release(None);
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!got.load(Ordering::SeqCst), "second acquire must wait for the first release");
        first.release(None);
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
            assert!(w.update_workflow_execution_as_active(1).is_err());
            assert!(w.update_workflow_execution_as_passive(1).is_err());
            assert!(w.reapply_events(&[]).is_err());
            assert!(w.load_workflow_execution(Some(3)).is_err());
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
