//! Reset history replay and persistence algorithms from resetter.go.
//! TODO(link): implement these primitive operations using execution contexts and HistoryManager.
use super::{reapply_events, ResetError, SignalEventReapplier};
use cadence_api::{enums::EventType, service_types::HistoryEvent};

pub struct HistoryPage {
    pub batches: Vec<Vec<HistoryEvent>>,
    pub next_page_token: Vec<u8>,
}
pub trait ResetHistory {
    fn paginate_history(&self, first_event_id: i64, next_event_id: i64, branch_token: &[u8], domain_id: &str, page_token: &[u8]) -> Result<HistoryPage, ResetError>;
    /// Acquires, loads and releases the workflow context, passing load errors to release.
    fn load_run_history_info(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> Result<(i64, Vec<u8>), ResetError>;
    fn domain_name(&self, domain_id: &str) -> Result<String, ResetError>;
    fn fork_history_branch(&self, token: &[u8], node_id: i64, cleanup_info: &str, domain_name: &str) -> Result<Vec<u8>, ResetError>;
}
pub fn fork_and_generate_branch_token(history: &impl ResetHistory, domain_id: &str, workflow_id: &str, token: &[u8], node_id: i64, reset_run_id: &str) -> Result<Vec<u8>, ResetError> {
    let domain_name = history.domain_name(domain_id)?;
    let cleanup_info = format!("{domain_id}:{workflow_id}:{reset_run_id}");
    history.fork_history_branch(token, node_id, &cleanup_info, &domain_name)
}
pub fn reapply_workflow_events(history: &impl ResetHistory, state: &mut impl SignalEventReapplier, domain_id: &str, first_event_id: i64, next_event_id: i64, branch_token: &[u8]) -> Result<String, ResetError> {
    if first_event_id == next_event_id { return Ok(String::new()); }
    let mut token = Vec::new();
    let mut last_events = Vec::new();
    loop {
        let page = history.paginate_history(first_event_id, next_event_id, branch_token, domain_id, &token)?;
        for batch in page.batches {
            reapply_events(state, &batch)?;
            last_events = batch;
        }
        token = page.next_page_token;
        if token.is_empty() { break; }
    }
    Ok(last_events.last().filter(|e| e.event_type == Some(EventType::WorkflowExecutionContinuedAsNew)).and_then(|e| e.workflow_execution_continued_as_new_event_attributes.as_ref()).map_or_else(String::new, |a| a.new_execution_run_id.clone()))
}
#[allow(clippy::too_many_arguments)]
pub fn reapply_reset_and_continue_as_new_workflow_events(history: &impl ResetHistory, state: &mut impl SignalEventReapplier, domain_id: &str, workflow_id: &str, base_branch_token: &[u8], base_rebuild_next_event_id: i64, base_next_event_id: i64, current_run_id: &str, current_next_event_id: i64, current_branch_token: &[u8]) -> Result<(), ResetError> {
    let mut next_run_id = reapply_workflow_events(history, state, domain_id, base_rebuild_next_event_id, base_next_event_id, base_branch_token)?;
    while !next_run_id.is_empty() {
        // The caller holds the current workflow lock. Reacquiring it deadlocks.
        let (next_event_id, branch_token) = if next_run_id == current_run_id {
            (current_next_event_id, current_branch_token.to_vec())
        } else {
            history.load_run_history_info(domain_id, workflow_id, &next_run_id)?
        };
        next_run_id = reapply_workflow_events(history, state, domain_id, 1, next_event_id, &branch_token)?;
    }
    Ok(())
}

pub trait ResetPersistence {
    type Workflow;
    type Snapshot;
    type Events;
    type PersistedHistory;
    fn update_with_new_as_active(&self, current: &Self::Workflow, reset: &Self::Workflow) -> Result<(), ResetError>;
    fn current_run_and_last_write_version(&self, current: &Self::Workflow) -> Result<(String, i64), ResetError>;
    fn close_transaction_as_active_snapshot(&self, reset: &Self::Workflow) -> Result<(Self::Snapshot, Vec<Self::Events>), ResetError>;
    fn persist_non_start_workflow_batch_events(&self, reset: &Self::Workflow, events: Self::Events) -> Result<Self::PersistedHistory, ResetError>;
    /// CreateWorkflowModeContinueAsNew, CreateWorkflowRequestModeNew.
    fn create_workflow_execution(&self, reset: &Self::Workflow, snapshot: Self::Snapshot, history: Self::PersistedHistory, previous_run_id: &str, previous_last_write_version: i64) -> Result<(), ResetError>;
}
pub fn persist_to_db<P: ResetPersistence>(p: &P, current_terminated: bool, current: &P::Workflow, reset: &P::Workflow) -> Result<(), ResetError> {
    if current_terminated { return p.update_with_new_as_active(current, reset); }
    let (run_id, version) = p.current_run_and_last_write_version(current)?;
    let (snapshot, mut events) = p.close_transaction_as_active_snapshot(reset)?;
    if events.len() != 1 { return Err(ResetError("there should be EXACTLY one batch of events for reset".into())); }
    let history = p.persist_non_start_workflow_batch_events(reset, events.remove(0))?;
    p.create_workflow_execution(reset, snapshot, history, &run_id, version)
}

pub trait ResetPreparation: super::ResetMutableState + super::PendingActivityMutableState + SignalEventReapplier {
    fn current_version(&self) -> i64;
    fn update_current_version(&mut self, version: i64, force: bool) -> Result<(), ResetError>;
    fn start_timestamp_unix_nanos(&self) -> i64;
    fn schedule_decision(&mut self) -> Result<(), ResetError>;
}
#[allow(clippy::too_many_arguments)]
pub fn prepare_reset_workflow<S: ResetPreparation>(history: &impl ResetHistory, state: &mut S, domain_id: &str, workflow_id: &str, base_run_id: &str, base_branch_token: &[u8], base_rebuild_last_event_id: i64, base_next_event_id: i64, reset_run_id: &str, reset_request_id: &str, reset_version: i64, reason: &str, additional_events: &[HistoryEvent], skip_signal_reapply: bool, current: &super::CurrentWorkflowInfo) -> Result<(), ResetError> {
    let base_last_event_version = state.current_version();
    if base_last_event_version > reset_version { return Err(ResetError(super::ERR_VERSION_MISMATCH.into())); }
    state.update_current_version(reset_version, false)?;
    super::close_pending_decision_task(state, base_run_id, reset_run_id, base_last_event_version, reason, reset_request_id)?;
    super::fail_inflight_activity(state, state.start_timestamp_unix_nanos(), reason)?;
    if !skip_signal_reapply {
        reapply_reset_and_continue_as_new_workflow_events(history, state, domain_id, workflow_id, base_branch_token, base_rebuild_last_event_id + 1, base_next_event_id, &current.run_id, current.next_event_id, &current.branch_token)?;
    }
    reapply_events(state, additional_events)?;
    state.schedule_decision()
}
pub trait ResetTermination {
    fn next_event_id(&self) -> i64;
    fn terminate_workflow(&mut self, first_event_id: i64, reason: &str, details: &[u8], identity: &str) -> Result<(), ResetError>;
}
pub fn terminate_workflow(state: &mut impl ResetTermination, reason: &str) -> Result<(), ResetError> {
    state.terminate_workflow(state.next_event_id(), reason, &[], "history-service")
}

pub struct RebuildRequest<'a> {
    pub domain_id: &'a str, pub workflow_id: &'a str, pub base_run_id: &'a str,
    pub base_branch_token: &'a [u8], pub last_event_id: i64, pub last_event_version: i64,
    pub reset_run_id: &'a str, pub reset_request_id: &'a str,
}
pub trait ResetRebuilder: ResetHistory {
    type Context;
    type MutableState;
    type Workflow;
    fn new_context(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> Self::Context;
    /// Rebuild at the shard clock's current time; invoke branch callback lazily.
    fn rebuild(&self, request: &RebuildRequest<'_>, branch: &mut dyn FnMut() -> Result<Vec<u8>, ResetError>) -> Result<(Self::MutableState, i64), ResetError>;
    fn set_history_size(&self, context: &mut Self::Context, size: i64);
    /// NewWorkflow with NoopReleaseFn, shard cluster metadata and logger.
    fn new_workflow(&self, context: Self::Context, state: Self::MutableState) -> Self::Workflow;
}
pub fn replay_reset_workflow<R: ResetRebuilder>(r: &R, request: &RebuildRequest<'_>) -> Result<R::Workflow, ResetError> {
    let mut context = r.new_context(request.domain_id, request.workflow_id, request.reset_run_id);
    let mut branch = || fork_and_generate_branch_token(r, request.domain_id, request.workflow_id, request.base_branch_token, request.last_event_id + 1, request.reset_run_id);
    let (state, size) = r.rebuild(request, &mut branch)?;
    r.set_history_size(&mut context, size);
    Ok(r.new_workflow(context, state))
}
