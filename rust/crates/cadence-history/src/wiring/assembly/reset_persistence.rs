//! `reset::history::ResetPersistence` over the execution context + SQL persistence
//! adapters (Go `workflowResetterImpl.persistToDB`), plus the engine-facing
//! `WorkflowResetter` that drives `reset::WorkflowResetter`.
use std::sync::Arc;

use async_trait::async_trait;
use cadence_persistence::history_contract::{CreateWorkflowMode, WorkflowRequestMode, WorkflowSnapshot};
use tokio::sync::Mutex;

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::{ResetWorkflow, WorkflowResetter as EngineWorkflowResetterTrait};
use crate::execution::context::{PersistedBlob, TransactionState, WorkflowContext, WorkflowContextStore, WorkflowEvents};
use crate::execution::workflow::TransactionPolicy;
use crate::reset::history::{ResetEngineBackend, ResetPersistence, ResetPreparation, ResetRebuilder, ResetTermination};
use crate::reset::ResetError;

/// Read access to the facts `persistToDB` needs from a mutable state
/// (`GetExecutionInfo().RunID`, `GetLastWriteVersion()`).
pub trait ResetStateView: TransactionState {
    fn run_id(&self) -> String;
    fn last_write_version(&self) -> Result<i64, Self::Error>;
}

/// A workflow as seen by the resetter: its shared execution context.
pub type ResetWorkflowHandle<M, S> = Arc<Mutex<WorkflowContext<M, S>>>;

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    crate::wiring::persistence::engine_ndc::block_on(f)
}

fn rerr<E: std::fmt::Display>(e: E) -> ResetError {
    ResetError(e.to_string())
}

/// Go `persistToDB` persistence primitives. `now` is the shard time source (unix nanos).
pub struct ContextResetPersistence<M, S> {
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    _marker: std::marker::PhantomData<fn() -> (M, S)>,
}

impl<M, S> ContextResetPersistence<M, S> {
    pub fn new(now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self { now, _marker: std::marker::PhantomData }
    }
}

impl<M, S> ResetPersistence for ContextResetPersistence<M, S>
where
    M: ResetStateView,
    M::Error: std::fmt::Display,
    S: WorkflowContextStore<M>,
{
    type Workflow = ResetWorkflowHandle<M, S>;
    type Snapshot = WorkflowSnapshot;
    type Events = WorkflowEvents;
    type PersistedHistory = PersistedBlob;

    fn update_with_new_as_active(&self, current: &Self::Workflow, reset: &Self::Workflow) -> Result<(), ResetError> {
        let now = (self.now)();
        block_on(async {
            let mut current = current.lock().await;
            let mut reset = reset.lock().await;
            current.update_workflow_execution_with_new_as_active(now, &mut reset).await.map_err(rerr)
        })
    }

    fn current_run_and_last_write_version(&self, current: &Self::Workflow) -> Result<(String, i64), ResetError> {
        block_on(async {
            let current = current.lock().await;
            let state = current.mutable_state.as_ref().ok_or_else(|| ResetError("current mutable state not loaded".into()))?;
            Ok((state.run_id(), state.last_write_version().map_err(rerr)?))
        })
    }

    fn close_transaction_as_active_snapshot(&self, reset: &Self::Workflow) -> Result<(Self::Snapshot, Vec<Self::Events>), ResetError> {
        let now = (self.now)();
        block_on(async {
            let mut reset = reset.lock().await;
            let state = reset.mutable_state.as_mut().ok_or_else(|| ResetError("reset mutable state not loaded".into()))?;
            state.close_snapshot(now, TransactionPolicy::Active).map_err(rerr)
        })
    }

    fn persist_non_start_workflow_batch_events(&self, reset: &Self::Workflow, events: Self::Events) -> Result<Self::PersistedHistory, ResetError> {
        block_on(async {
            let mut reset = reset.lock().await;
            reset.persist_non_start_workflow_batch_events(&events).await.map_err(rerr)
        })
    }

    fn create_workflow_execution(&self, reset: &Self::Workflow, mut snapshot: Self::Snapshot, history: Self::PersistedHistory, previous_run_id: &str, previous_last_write_version: i64) -> Result<(), ResetError> {
        block_on(async {
            let mut reset = reset.lock().await;
            reset
                .create_workflow_execution(&mut snapshot, history, CreateWorkflowMode::ContinueAsNew, previous_run_id, previous_last_write_version, WorkflowRequestMode::New)
                .await
                .map_err(rerr)
        })
    }
}

/// Builds the resetter's view of the current workflow from the engine request
/// (the engine holds `ExecutionContextRef`/`MutableStateRef`; the reset backend
/// works on its own `Workflow`).
pub trait CurrentWorkflowResolver<W>: Send + Sync {
    fn current_workflow(&self, request: &ResetWorkflow) -> HResult<W>;
}

fn to_history_error(e: ResetError) -> HistoryError {
    if e.0 == "Can not reset workflow with pending child workflows" {
        HistoryError::bad_request(e.0)
    } else {
        HistoryError::Other(e.0)
    }
}

/// `engine::runtime::WorkflowResetter` driving `reset::WorkflowResetter`.
pub struct EngineWorkflowResetter<B: ResetEngineBackend>
where
    B::MutableState: ResetPreparation + ResetTermination,
{
    resetter: crate::reset::WorkflowResetter<B>,
    resolver: Arc<dyn CurrentWorkflowResolver<<B as ResetRebuilder>::Workflow>>,
}

impl<B: ResetEngineBackend> EngineWorkflowResetter<B>
where
    B::MutableState: ResetPreparation + ResetTermination,
{
    pub fn new(backend: Arc<B>, resolver: Arc<dyn CurrentWorkflowResolver<<B as ResetRebuilder>::Workflow>>) -> Self {
        Self { resetter: crate::reset::WorkflowResetter::new(backend), resolver }
    }
}

#[async_trait]
impl<B> EngineWorkflowResetterTrait for EngineWorkflowResetter<B>
where
    B: ResetEngineBackend + Send + Sync + 'static,
    B::MutableState: ResetPreparation + ResetTermination,
    <B as ResetRebuilder>::Workflow: Send + 'static,
{
    async fn reset_workflow(&self, request: ResetWorkflow) -> HResult<()> {
        let mut current = self.resolver.current_workflow(&request)?;
        tokio::task::block_in_place(|| {
            self.resetter.reset_workflow(
                &request.domain_id,
                &request.workflow_id,
                &request.base_run_id,
                &request.base_branch_token,
                request.rebuild_last_event_id,
                request.rebuild_last_event_version,
                request.base_next_event_id,
                &request.reset_run_id,
                &request.request_id,
                &mut current,
                &request.reason,
                &request.events,
                request.skip_signal_reapply,
            )
        })
        .map_err(to_history_error)
    }
}
