//! group: assembly/ndc_rebuild
//!
//! Concrete `NdcStateRebuilder` (Go `execution.stateRebuilderImpl`): replays a
//! history branch page by page into a fresh [`OwnedNdcMutableState`] via the
//! generic [`StateRebuilder`] sequencing, then closes the rebuilt state as a
//! passive snapshot, regenerates tasks once (Go `RefreshTasks`) and stamps the
//! start timestamp.
//!
//! The rebuilt state is the owned typed state itself; nothing is converted
//! through JSON and no CAS value is copied from any old run (the caller, Go
//! `conflictResolver.rebuild`, sets the update condition explicitly).
//!
//! Unported (explicit): `ChildWorkflowOnly` of pending signal-external events
//! (the Rust event attributes do not carry it, so refreshed signal tasks use
//! `false`).

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::Arc;

use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::{VersionHistories, WorkflowExecution};

use crate::engine::deps::{DomainCache, HistoryError};
use crate::execution::context::{WorkflowExecutionKey};
use crate::execution::mutable_state_methods_common::{TaskGenEnv, CLOSE_STATUS_NONE};
use crate::execution::mutable_state_task_generator::{
    MutableStateTaskGenerator, TaskGeneratorDomains, TaskGeneratorState, TypedTaskGeneratorState,
};
use crate::execution::mutable_state_builder::{TypedMutableStateError, EMPTY_VERSION};
use crate::execution::mutable_state_task_refresher::{
    refresh_tasks, ExternalTaskKind, InitiatedInfo, TaskRefreshState,
};
use crate::execution::state_builder::{StateBuilder, StateBuilderError};
use crate::execution::state_rebuilder::{HistoryBatchPage, RebuildContext, RebuildRequest, StateRebuilder};
use crate::execution::timer::TimerInfo;
use crate::execution::timer_sequence::{ActivityTimerInfo, TimerSequenceId, TimerSequenceState};
use crate::execution::workflow::TransactionPolicy;
use crate::mutable_state::DecisionInfo;
use crate::ndc::conflict_resolver::NdcStateRebuilder;
use crate::ndc::{NdcError, NdcMutableState, NdcResult};
use crate::wiring::assembly::engine_state::StateParams;
use crate::wiring::assembly::ndc_state::OwnedNdcMutableState;
use crate::wiring::persistence::engine_ndc::SqlHistoryPager;

fn history_err(err: HistoryError) -> NdcError {
    if err.is_entity_not_exists() {
        return NdcError::EntityNotExists(err.to_string());
    }
    NdcError::Other(err.to_string())
}

fn state_builder_err(err: StateBuilderError) -> NdcError {
    match err {
        StateBuilderError::Typed(TypedMutableStateError::BadRequest(m)) => NdcError::bad_request(m),
        other => NdcError::internal(other.to_string()),
    }
}

/// Go `PaginateHistory` over a branch (`ReadHistoryBranchByBatch`).
pub trait RebuildPager: Send + Sync {
    fn page(
        &self,
        branch: &[u8],
        first: i64,
        next: i64,
        token: Vec<u8>,
        page_size: usize,
    ) -> Result<HistoryBatchPage, HistoryError>;
}

impl RebuildPager for SqlHistoryPager {
    fn page(
        &self,
        branch: &[u8],
        first: i64,
        next: i64,
        token: Vec<u8>,
        page_size: usize,
    ) -> Result<HistoryBatchPage, HistoryError> {
        SqlHistoryPager::page(self, branch, first, next, token, page_size)
    }
}

/// Pager used when the shard has no history node store: every read fails loudly.
pub struct UnavailableRebuildPager;

impl RebuildPager for UnavailableRebuildPager {
    fn page(&self, _: &[u8], _: i64, _: i64, _: Vec<u8>, _: usize) -> Result<HistoryBatchPage, HistoryError> {
        Err(HistoryError::internal("state rebuild: history node store is not configured"))
    }
}

/// Domain lookups for task generation (Go `domainCache.GetDomainID/GetDomainByID`).
pub struct EngineTaskDomains {
    pub domains: Arc<dyn DomainCache>,
    pub state_params: Arc<dyn Fn(&str) -> StateParams + Send + Sync>,
}

fn internal(e: HistoryError) -> TypedMutableStateError {
    TypedMutableStateError::Internal(e.to_string())
}

impl TaskGeneratorDomains for EngineTaskDomains {
    fn domain_id_by_name(&self, name: &str) -> Result<String, TypedMutableStateError> {
        self.domains.get_domain(name).map(|e| e.info.id.clone()).map_err(internal)
    }
    fn retention_days(&self, domain_id: &str, _workflow_id: &str) -> Result<Option<i32>, TypedMutableStateError> {
        match self.domains.get_domain_by_id(domain_id) {
            Ok(_) => Ok(Some((self.state_params)(domain_id).retention_days)),
            Err(e) if e.is_entity_not_exists() => Ok(None),
            Err(e) => Err(internal(e)),
        }
    }
    fn is_global_domain(&self, domain_id: &str) -> Result<bool, TypedMutableStateError> {
        self.domains.get_domain_by_id(domain_id).map(|e| e.is_global_domain).map_err(internal)
    }
}

/// Everything one shard needs to rebuild mutable state.
#[derive(Clone)]
pub struct NdcRebuildEnv {
    pub pager: Arc<dyn RebuildPager>,
    /// Go `domainCache.GetDomainByID` (existence check done first by `Rebuild`).
    pub domains: Arc<dyn DomainCache>,
    pub task_domains: Arc<dyn TaskGeneratorDomains + Send + Sync>,
    pub state_params: Arc<dyn Fn(&str) -> StateParams + Send + Sync>,
    /// `IsAdvancedVisibilityWritingEnabled(...)`, evaluated per rebuild.
    pub advanced_visibility_writing: Arc<dyn Fn() -> bool + Send + Sync>,
}

/// Event types the task refresher reads back from history (Go events cache).
fn is_refresh_event(event_type: Option<EventType>) -> bool {
    matches!(
        event_type,
        Some(
            EventType::WorkflowExecutionStarted
                | EventType::ActivityTaskScheduled
                | EventType::StartChildWorkflowExecutionInitiated
                | EventType::RequestCancelExternalWorkflowExecutionInitiated
                | EventType::SignalExternalWorkflowExecutionInitiated
                | EventType::WorkflowExecutionCompleted
                | EventType::WorkflowExecutionFailed
                | EventType::WorkflowExecutionTimedOut
                | EventType::WorkflowExecutionCanceled
                | EventType::WorkflowExecutionTerminated
                | EventType::WorkflowExecutionContinuedAsNew
        )
    )
}

/// Concrete [`RebuildContext`] over the owned typed state.
pub struct NdcRebuildContext {
    env: NdcRebuildEnv,
    now: i64,
    target: WorkflowExecutionKey,
    /// Events the refresher needs, keyed by event ID (replaces Go's events cache reads).
    events: BTreeMap<i64, HistoryEvent>,
}

impl NdcRebuildContext {
    pub fn new(env: NdcRebuildEnv, now: i64, target: WorkflowExecutionKey) -> Self {
        Self { env, now, target, events: BTreeMap::new() }
    }
}

impl RebuildContext for NdcRebuildContext {
    type State = OwnedNdcMutableState;
    type Error = NdcError;

    fn load_domain(&mut self, domain_id: &str) -> NdcResult<()> {
        self.env.domains.get_domain_by_id(domain_id).map(|_| ()).map_err(history_err)
    }

    /// Go `initializeBuilders`: version histories with one empty branch, `EmptyVersion`.
    fn initialize_builders(&mut self, domain_id: &str) -> OwnedNdcMutableState {
        let params = (self.env.state_params)(domain_id);
        let mut task_gen = TaskGenEnv::default();
        task_gen.domains = Some(self.env.task_domains.clone());
        task_gen.deletion_jitter_range = params.deletion_jitter_range as i32;
        task_gen.normal_schedule_to_start_timeout_nanos = params.normal_decision_s2s_timeout_nanos;
        task_gen.normal_schedule_to_start_max_attempts = params.normal_decision_s2s_max_attempts;
        let execution = WorkflowExecution { workflow_id: self.target.workflow_id.clone(), run_id: self.target.run_id.clone() };
        let mut state = OwnedNdcMutableState::new(domain_id, &execution, self.now, task_gen);
        let typed = state.typed_mut();
        typed.current_version = EMPTY_VERSION;
        typed.version_histories = Some(VersionHistories {
            current_version_history_index: 0,
            histories: vec![cadence_persistence::version_history::new_history(Vec::new(), &[])],
        });
        state
    }

    fn page(
        &mut self,
        branch: &[u8],
        first: i64,
        next: i64,
        token: Vec<u8>,
        _domain_id: &str,
        page_size: usize,
    ) -> NdcResult<HistoryBatchPage> {
        self.env.pager.page(branch, first, next, token, page_size).map_err(history_err)
    }

    fn apply_events(
        &mut self,
        state: &mut OwnedNdcMutableState,
        target: &WorkflowExecutionKey,
        events: &[HistoryEvent],
        request_id: &str,
    ) -> NdcResult<()> {
        let execution = WorkflowExecution { workflow_id: target.workflow_id.clone(), run_id: target.run_id.clone() };
        // Go passes `nil` new-run history when rebuilding.
        StateBuilder::apply_events_typed(state.typed_mut(), &target.domain_id, request_id, &execution, events)
            .map_err(state_builder_err)?;
        for event in events.iter().filter(|e| is_refresh_event(e.event_type)) {
            self.events.insert(event.event_id, event.clone());
        }
        Ok(())
    }

    fn last_version_history_item(&self, state: &OwnedNdcMutableState) -> NdcResult<Option<(i64, i64)>> {
        use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};
        let Some(histories) = state.get_version_histories() else { return Ok(None) };
        let item = histories.current_history()?.last_item()?;
        Ok(Some((item.event_id, item.version)))
    }

    /// Go `SetCurrentBranchToken`.
    fn set_current_branch_token(&mut self, state: &mut OwnedNdcMutableState, branch: Vec<u8>) -> NdcResult<()> {
        let typed = state.typed_mut();
        match typed.version_histories.as_mut() {
            None => typed.execution_info.branch_token = branch,
            Some(histories) => {
                let index = histories.current_version_history_index as usize;
                let history = histories
                    .histories
                    .get_mut(index)
                    .ok_or_else(NdcError::missing_version_histories)?;
                history.branch_token = branch;
            }
        }
        Ok(())
    }

    /// Go `CloseTransactionAsSnapshot(now, Passive)`: the snapshot is discarded,
    /// then the transaction is cleaned up (generated tasks/requests cleared).
    fn close_passive_snapshot(&mut self, state: &mut OwnedNdcMutableState, now: i64) -> NdcResult<()> {
        state.close_transaction_as_snapshot(now, TransactionPolicy::Passive)?;
        NdcMutableState::transaction_committed(state);
        Ok(())
    }

    /// Go `taskRefresher.RefreshTasks`: all tasks are generated in one pass and
    /// drained into the state exactly once.
    fn refresh_tasks(&mut self, state: &mut OwnedNdcMutableState, now: i64) -> NdcResult<()> {
        let jitter = state.task_gen_env().deletion_jitter_range;
        let advanced = (self.env.advanced_visibility_writing)();
        let schedule_to_start = state.typed().env_decision_schedule_to_start_timeout_nanos();
        let domains = self.env.task_domains.clone();
        let events = &self.events;
        state
            .typed_mut()
            .run_task_generator_with(&DomainsHandle(domains), schedule_to_start, |adapter| {
                let mut refresh = RefreshAdapter { adapter, events };
                refresh_tasks(&mut refresh, now, jitter, advanced)
            })
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    /// Go `GetExecutionInfo().StartTimestamp = now`.
    fn set_start_timestamp(&mut self, state: &mut OwnedNdcMutableState, now: i64) {
        state.typed_mut().execution_info.start_time_unix_nanos = now;
    }

    fn empty_history_error(&self) -> NdcError {
        NdcError::internal("Attempting to build history state but the iterator has found no history")
    }

    fn bad_request(&self, message: String) -> NdcError {
        NdcError::bad_request(message)
    }
}

/// Sized handle so the generic adapter can borrow the shared domain resolver.
struct DomainsHandle(Arc<dyn TaskGeneratorDomains + Send + Sync>);

impl TaskGeneratorDomains for DomainsHandle {
    fn domain_id_by_name(&self, name: &str) -> Result<String, TypedMutableStateError> {
        self.0.domain_id_by_name(name)
    }
    fn retention_days(&self, domain_id: &str, workflow_id: &str) -> Result<Option<i32>, TypedMutableStateError> {
        self.0.retention_days(domain_id, workflow_id)
    }
    fn is_global_domain(&self, domain_id: &str) -> Result<bool, TypedMutableStateError> {
        self.0.is_global_domain(domain_id)
    }
}

/// `TaskRefreshState` over the typed task-generator adapter; history reads come
/// from the events captured during replay.
struct RefreshAdapter<'a, 'b, D: TaskGeneratorDomains> {
    adapter: &'a mut TypedTaskGeneratorState<'b, D>,
    events: &'a BTreeMap<i64, HistoryEvent>,
}

impl<D: TaskGeneratorDomains> RefreshAdapter<'_, '_, D> {
    fn event(&self, event_id: i64, what: &str) -> Result<HistoryEvent, TypedMutableStateError> {
        self.events
            .get(&event_id)
            .cloned()
            .ok_or_else(|| TypedMutableStateError::Internal(format!("unable to find {what} (event {event_id}) while refreshing tasks")))
    }
}

impl<D: TaskGeneratorDomains> TimerSequenceState for RefreshAdapter<'_, '_, D> {
    type Error = TypedMutableStateError;
    fn user_timers(&self) -> Vec<TimerInfo> { self.adapter.user_timers() }
    fn activities(&self) -> Vec<ActivityTimerInfo> { self.adapter.activities() }
    fn user_timer(&self, event_id: i64) -> Option<TimerInfo> { self.adapter.user_timer(event_id) }
    fn activity(&self, event_id: i64) -> Option<ActivityTimerInfo> { self.adapter.activity(event_id) }
    fn missing_activity_info(&self, event_id: i64) -> Self::Error { self.adapter.missing_activity_info(event_id) }
    fn update_user_timer(&mut self, timer: TimerInfo) -> Result<(), Self::Error> { self.adapter.update_user_timer(timer) }
    fn update_activity_timer(&mut self, activity: ActivityTimerInfo) -> Result<(), Self::Error> {
        self.adapter.update_activity_timer(activity)
    }
    fn add_user_timer_task(&mut self, timer: TimerSequenceId) { self.adapter.add_user_timer_task(timer) }
    fn add_activity_timeout_task(&mut self, timer: TimerSequenceId, task_list: String) {
        self.adapter.add_activity_timeout_task(timer, task_list)
    }
}

impl<D: TaskGeneratorDomains> TaskRefreshState for RefreshAdapter<'_, '_, D> {
    fn start_event(&mut self) -> Result<HistoryEvent, Self::Error> {
        // Go `ErrMissingWorkflowStartEvent`.
        self.event(1, "workflow start event")
    }
    fn completion_event(&mut self) -> Result<HistoryEvent, Self::Error> {
        let id = self.adapter.state.execution_info.next_event_id - 1;
        self.event(id, "workflow completion event")
    }
    fn current_branch_token(&mut self) -> Result<Vec<u8>, Self::Error> {
        let state = &*self.adapter.state;
        if let Some(histories) = state.version_histories.as_ref() {
            let index = histories.current_version_history_index as usize;
            return histories
                .histories
                .get(index)
                .map(|h| h.branch_token.clone())
                .ok_or_else(|| TypedMutableStateError::Internal("current version history is missing".into()));
        }
        Ok(state.execution_info.branch_token.clone())
    }
    fn cached_event(&mut self, _batch_id: i64, event_id: i64, _branch: &[u8]) -> Result<HistoryEvent, Self::Error> {
        self.event(event_id, "initiated event")
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        let state = &*self.adapter.state;
        state.has_pending_decision() || state.execution_info.last_processed_event != crate::ndc::EMPTY_EVENT_ID
    }
    fn is_closed(&self) -> bool {
        // Go: `CloseStatus != WorkflowCloseStatusNone`.
        self.adapter.state.execution_info.close_status != CLOSE_STATUS_NONE
    }
    fn has_pending_decision(&self) -> bool { self.adapter.state.has_pending_decision() }
    fn pending_decision(&self) -> Option<DecisionInfo> {
        let schedule_id = self.adapter.state.execution_info.decision_schedule_id;
        TaskGeneratorState::decision(&*self.adapter, schedule_id)
    }
    fn missing_pending_decision(&self) -> Self::Error {
        TypedMutableStateError::Internal("it could be a bug, cannot get pending decision".into())
    }
    fn pending_external(&self, kind: ExternalTaskKind) -> Vec<InitiatedInfo> {
        let state = &*self.adapter.state;
        match kind {
            ExternalTaskKind::ChildWorkflow => state
                .pending_child_execution_info_ids
                .values()
                .map(|i| InitiatedInfo { event_batch_id: i.initiated_event_batch_id, initiated_id: i.initiated_id, started_id: i.started_id })
                .collect(),
            ExternalTaskKind::RequestCancel => state
                .pending_request_cancel_info_ids
                .values()
                .map(|i| InitiatedInfo { event_batch_id: i.initiated_event_batch_id, initiated_id: i.initiated_id, started_id: 0 })
                .collect(),
            ExternalTaskKind::Signal => state
                .pending_signal_info_ids
                .values()
                .map(|i| InitiatedInfo { event_batch_id: i.initiated_event_batch_id, initiated_id: i.initiated_id, started_id: 0 })
                .collect(),
        }
    }
    fn generate_workflow_start_tasks(&mut self, start_time: i64, event: &HistoryEvent) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_workflow_start_tasks(start_time, event)
    }
    fn generate_delayed_decision_tasks(&mut self, event: &HistoryEvent) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_delayed_decision_tasks(event)
    }
    fn generate_workflow_close_tasks(&mut self, event: &HistoryEvent, jitter_range: i32) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_workflow_close_tasks(event, jitter_range)
    }
    fn generate_record_workflow_started_tasks(&mut self, event: &HistoryEvent) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_record_workflow_started_tasks(event)
    }
    fn generate_decision_start_tasks(&mut self, schedule_id: i64) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_decision_start_tasks(schedule_id)
    }
    fn generate_decision_schedule_tasks(&mut self, schedule_id: i64) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_decision_schedule_tasks(schedule_id)
    }
    fn generate_activity_transfer_tasks(&mut self, event: &HistoryEvent) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_activity_transfer_tasks(event)
    }
    fn generate_external_tasks(&mut self, kind: ExternalTaskKind, event: &HistoryEvent) -> Result<(), Self::Error> {
        let mut generator = MutableStateTaskGenerator { state: &mut *self.adapter };
        match kind {
            ExternalTaskKind::ChildWorkflow => generator.generate_child_workflow_tasks(event),
            ExternalTaskKind::RequestCancel => generator.generate_request_cancel_external_tasks(event),
            ExternalTaskKind::Signal => generator.generate_signal_external_tasks(event),
        }
    }
    fn generate_workflow_search_attr_tasks(&mut self) -> Result<(), Self::Error> {
        MutableStateTaskGenerator { state: &mut *self.adapter }.generate_workflow_search_attr_tasks()
    }
}

/// Go `execution.NewStateRebuilder(shard, logger)` as the NDC adapter.
pub struct SqlNdcStateRebuilder {
    env: NdcRebuildEnv,
}

impl SqlNdcStateRebuilder {
    pub fn new(env: NdcRebuildEnv) -> Self {
        Self { env }
    }
}

impl NdcStateRebuilder for SqlNdcStateRebuilder {
    fn rebuild(
        &self,
        now_nanos: i64,
        base_domain_id: &str,
        base_workflow_id: &str,
        base_run_id: &str,
        base_branch_token: &[u8],
        base_last_event_id: i64,
        base_last_event_version: i64,
        target_run_id: &str,
        target_branch_token: &dyn Fn() -> NdcResult<Vec<u8>>,
        request_id: &str,
    ) -> NdcResult<(Box<dyn NdcMutableState>, i64)> {
        let (state, size) = self.rebuild_owned(
            now_nanos,
            base_domain_id,
            base_workflow_id,
            base_run_id,
            base_branch_token,
            base_last_event_id,
            base_last_event_version,
            target_run_id,
            target_branch_token,
            request_id,
        )?;
        Ok((Box::new(state), size))
    }
}

impl SqlNdcStateRebuilder {
    /// Same as the trait `rebuild` but returns the concrete state (Go
    /// `workflowRepairerImpl.repairViaRebuild` needs the typed builder).
    #[allow(clippy::too_many_arguments)]
    pub fn rebuild_owned(
        &self,
        now_nanos: i64,
        base_domain_id: &str,
        base_workflow_id: &str,
        _base_run_id: &str,
        base_branch_token: &[u8],
        base_last_event_id: i64,
        base_last_event_version: i64,
        target_run_id: &str,
        target_branch_token: &dyn Fn() -> NdcResult<Vec<u8>>,
        request_id: &str,
    ) -> NdcResult<(OwnedNdcMutableState, i64)> {
        let target = WorkflowExecutionKey {
            domain_id: base_domain_id.to_string(),
            workflow_id: base_workflow_id.to_string(),
            run_id: target_run_id.to_string(),
        };
        let context = NdcRebuildContext::new(self.env.clone(), now_nanos, target.clone());
        let mut rebuilder = StateRebuilder::new(context);
        let (state, size) = rebuilder.rebuild(
            RebuildRequest {
                now: now_nanos,
                base_branch_token,
                base_last_event_id,
                base_last_event_version,
                target: &target,
                request_id,
            },
            target_branch_token,
        )?;
        Ok((state, size))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use cadence_api::service_types::WorkflowExecutionStartedEventAttributes;
    use cadence_api::tasks::{DecisionTaskScheduledEventAttributes, DecisionTaskStartedEventAttributes};
    use cadence_api::types::TaskList;

    use super::*;
    use crate::execution::mutable_state_task_generator::TaskKind;

    struct Domains;
    impl TaskGeneratorDomains for Domains {
        fn domain_id_by_name(&self, name: &str) -> Result<String, TypedMutableStateError> { Ok(format!("id-{name}")) }
        fn retention_days(&self, _: &str, _: &str) -> Result<Option<i32>, TypedMutableStateError> { Ok(Some(3)) }
        fn is_global_domain(&self, _: &str) -> Result<bool, TypedMutableStateError> { Ok(false) }
    }

    type PagerCall = (Vec<u8>, i64, i64, Vec<u8>);

    /// Scripted pager: page `i` is returned for token `[i]`; records every call.
    struct ScriptedPager {
        pages: Vec<(Vec<Vec<HistoryEvent>>, i64)>,
        calls: Mutex<Vec<PagerCall>>,
    }
    impl RebuildPager for ScriptedPager {
        fn page(&self, branch: &[u8], first: i64, next: i64, token: Vec<u8>, _: usize) -> Result<HistoryBatchPage, HistoryError> {
            self.calls.lock().unwrap().push((branch.to_vec(), first, next, token.clone()));
            let index = token.first().map(|t| *t as usize).unwrap_or(0);
            let (batches, size) = self.pages[index].clone();
            let next_token = if index + 1 < self.pages.len() { vec![(index + 1) as u8] } else { Vec::new() };
            Ok(HistoryBatchPage { batches, next_token, size })
        }
    }

    struct FakeDomainCache;
    impl DomainCache for FakeDomainCache {
        fn get_domain_by_id(&self, _: &str) -> crate::engine::deps::HResult<Arc<crate::engine::deps::DomainCacheEntry>> {
            Ok(Arc::new(Default::default()))
        }
        fn get_domain(&self, _: &str) -> crate::engine::deps::HResult<Arc<crate::engine::deps::DomainCacheEntry>> {
            Ok(Arc::new(Default::default()))
        }
        fn get_domain_name(&self, _: &str) -> crate::engine::deps::HResult<String> { Ok("domain".into()) }
        fn unregister_domain_change_callback(&self, _: &str) {}
    }

    fn event(id: i64, kind: EventType) -> HistoryEvent {
        HistoryEvent { event_id: id, version: 7, timestamp: Some(100 + id), task_id: 90 + id, event_type: Some(kind), ..Default::default() }
    }
    fn started() -> HistoryEvent {
        HistoryEvent {
            workflow_execution_started_event_attributes: Some(WorkflowExecutionStartedEventAttributes { request_id: "start".into(), ..Default::default() }),
            ..event(1, EventType::WorkflowExecutionStarted)
        }
    }
    fn scheduled() -> HistoryEvent {
        HistoryEvent {
            decision_task_scheduled_event_attributes: Some(DecisionTaskScheduledEventAttributes {
                task_list: Some(TaskList { name: "decisions".into(), ..Default::default() }),
                start_to_close_timeout_seconds: Some(30),
                attempt: 0,
            }),
            ..event(2, EventType::DecisionTaskScheduled)
        }
    }
    fn decision_started() -> HistoryEvent {
        HistoryEvent {
            decision_task_started_event_attributes: Some(DecisionTaskStartedEventAttributes { scheduled_event_id: 2, request_id: "d".into(), ..Default::default() }),
            ..event(3, EventType::DecisionTaskStarted)
        }
    }

    fn rebuilder(pager: Arc<ScriptedPager>) -> SqlNdcStateRebuilder {
        SqlNdcStateRebuilder::new(NdcRebuildEnv {
            pager,
            domains: Arc::new(FakeDomainCache),
            task_domains: Arc::new(Domains),
            state_params: Arc::new(|_| StateParams::default()),
            advanced_visibility_writing: Arc::new(|| false),
        })
    }

    fn run(
        pager: &Arc<ScriptedPager>,
        last_id: i64,
        last_version: i64,
    ) -> NdcResult<(Box<dyn NdcMutableState>, i64)> {
        rebuilder(pager.clone()).rebuild(
            5_000,
            "domain-id",
            "wf",
            "run",
            &[1, 1],
            last_id,
            last_version,
            "run",
            &|| Ok(vec![9, 9]),
            "request",
        )
    }

    fn pager(pages: Vec<(Vec<Vec<HistoryEvent>>, i64)>) -> Arc<ScriptedPager> {
        Arc::new(ScriptedPager { pages, calls: Mutex::new(Vec::new()) })
    }

    #[test]
    fn rebuilds_over_multiple_pages_and_sums_history_size() {
        let pager = pager(vec![(vec![vec![started(), scheduled()]], 10), (vec![vec![decision_started()]], 5)]);
        let (state, size) = run(&pager, 3, 7).unwrap();
        assert_eq!(size, 15);
        assert_eq!(state.get_next_event_id(), 4);
        assert_eq!(state.get_execution_info().start_timestamp_nanos, 5_000);
        let calls = pager.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        // Go: first event 1 .. baseLastEventID + 1, base branch token, token threaded through.
        assert_eq!((calls[0].1, calls[0].2, calls[0].3.clone()), (1, 4, Vec::new()));
        assert_eq!(calls[1].3, vec![1]);
        assert_eq!(calls[1].0, vec![1, 1]);
    }

    #[test]
    fn rejects_mismatch_at_end_of_batch_and_in_middle_of_batch() {
        // Rebuilt last item is (3, 7). Asking for 2 is the middle of the last batch.
        let pager_a = pager(vec![(vec![vec![started(), scheduled()], vec![decision_started()]], 1)]);
        match run(&pager_a, 2, 7) {
            Err(NdcError::BadRequest(e)) => assert!(e.to_string().contains("middle of a batch"), "{e}"),
            other => panic!("expected bad request, got {:?}", other.map(|_| ())),
        }
        // Right event ID, wrong version.
        let pager_b = pager(vec![(vec![vec![started(), scheduled()], vec![decision_started()]], 1)]);
        assert!(matches!(run(&pager_b, 3, 8), Err(NdcError::BadRequest(_))));
        // Exact end of the last batch is accepted.
        let pager_c = pager(vec![(vec![vec![started(), scheduled()], vec![decision_started()]], 1)]);
        assert!(run(&pager_c, 3, 7).is_ok());
    }

    #[test]
    fn empty_history_is_an_internal_error() {
        let pager = pager(vec![(Vec::new(), 0)]);
        assert!(matches!(run(&pager, 3, 7), Err(NdcError::Internal(_))));
    }

    #[test]
    fn branch_switch_sets_target_branch_token_on_rebuilt_state() {
        let pager = pager(vec![(vec![vec![started(), scheduled(), decision_started()]], 1)]);
        let (state, _) = run(&pager, 3, 7).unwrap();
        // Replay read the base branch [1,1]; the rebuilt state points at the fork [9,9].
        assert_eq!(pager.calls.lock().unwrap()[0].0, vec![1, 1]);
        assert_eq!(state.get_current_branch_token().unwrap(), vec![9, 9]);
    }

    #[test]
    fn target_branch_error_is_propagated() {
        let pager = pager(vec![(vec![vec![started(), scheduled(), decision_started()]], 1)]);
        let result = rebuilder(pager).rebuild(
            5_000, "domain-id", "wf", "run", &[1, 1], 3, 7, "run",
            &|| Err(NdcError::internal("fork failed")), "request",
        );
        assert!(matches!(result, Err(NdcError::Internal(_))));
    }

    #[test]
    fn refresh_tasks_are_generated_once_and_cas_is_not_copied() {
        let pager = pager(vec![(vec![vec![started(), scheduled()], vec![decision_started()]], 1)]);
        let context_env = NdcRebuildEnv {
            pager: pager.clone(),
            domains: Arc::new(FakeDomainCache),
            task_domains: Arc::new(Domains),
            state_params: Arc::new(|_| StateParams::default()),
            advanced_visibility_writing: Arc::new(|| false),
        };
        let target = WorkflowExecutionKey { domain_id: "domain-id".into(), workflow_id: "wf".into(), run_id: "run".into() };
        let mut rebuilder = StateRebuilder::new(NdcRebuildContext::new(context_env, 5_000, target.clone()));
        let (state, _) = rebuilder
            .rebuild(
                RebuildRequest { now: 5_000, base_branch_token: &[1, 1], base_last_event_id: 3, base_last_event_version: 7, target: &target, request_id: "request" },
                || Ok(vec![9, 9]),
            )
            .unwrap();
        let typed = state.typed();
        let record_started = typed.transfer_tasks.iter().filter(|t| t.kind == TaskKind::RecordWorkflowStarted).count();
        let decision_timeouts = typed.timer_tasks.iter().filter(|t| matches!(t.kind, TaskKind::DecisionTimeout { .. })).count();
        assert_eq!(record_started, 1, "{:?}", typed.transfer_tasks);
        assert_eq!(decision_timeouts, 1, "{:?}", typed.timer_tasks);
        // Replay itself generated nothing: everything present came from the one refresh pass.
        assert!(typed.history_events.is_empty());
        // No CAS from any old run: the condition is the rebuilt state's own next event ID
        // as of the passive snapshot cleanup (before refresh), never an input.
        assert_eq!(state.get_update_condition(), 4);
        assert_eq!(typed.execution_info.start_time_unix_nanos, 5_000);
    }
}
