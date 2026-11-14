//! Engine `WorkflowResetter`: Go `reset.workflowResetterImpl.ResetWorkflow`
//! (`go/service/history/reset/resetter.go`).
//!
//! The current run stays in the engine state world (`EngineMutableState`,
//! terminated through `MutableState::terminate_workflow`); the reset run is
//! rebuilt in the typed world (`OwnedNdcMutableState`) because the rebuild,
//! decision-failure and activity-failure helpers live there. All typed-state
//! work happens in the synchronous [`EngineWorkflowResetter::prepare`] so the
//! non-`Send` state never crosses an `.await`.
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::enums::DecisionTaskFailedCause;
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::RespondActivityTaskFailedRequest;
use cadence_persistence::history_contract as pc;

use super::engine_cache::{shard_err, EngineCore, EngineExecutionContext};
use super::ndc_adapters::new_state_rebuilder;
use super::ndc_context_hooks::{ndc_err, EngineNdcContextHooks};
use super::ndc_rebuild::{RebuildPager, UnavailableRebuildPager};
use super::ndc_state::OwnedNdcMutableState;
use crate::engine::deps::{ExecutionCache, HResult, HistoryError};
use crate::engine::runtime::{ResetWorkflow, WorkflowResetter};
use crate::execution::mutable_state_builder::TypedMutableState;
use crate::execution::mutable_state_methods_decision::DecisionEnv;
use crate::execution::workflow::TransactionPolicy;
use crate::ndc::{NdcError, NdcMutableState};
use crate::reset::history::{
    prepare_reset_workflow, HistoryPage, ResetHistory, ResetPreparation,
};
use crate::reset::{
    CurrentWorkflowInfo, PendingActivityInfo, PendingActivityMutableState, ResetDecisionInfo, ResetError,
    ResetMutableState, SignalEventReapplier,
};
use crate::wiring::persistence::engine_ndc::{block_on, to_history_error, SqlHistoryManager, SqlHistoryPager};
use cadence_api::types::WorkflowExecution;
use crate::wiring::persistence::engine_ndc::WorkflowContextHooks;

/// Go `common.IdentityHistoryService`.
const IDENTITY_HISTORY_SERVICE: &str = "history-service";
const PAGE_SIZE: usize = 100;

/// Go `ErrPendingChildren` / reset errors → API errors.
fn reset_err(err: ResetError) -> HistoryError {
    if err.0.starts_with("Can not reset workflow with pending child") {
        HistoryError::bad_request(err.0)
    } else {
        HistoryError::internal(err.0)
    }
}

// ------------------------------------------------------------ typed adapter

/// Typed mutable state seen through the reset traits.
struct TypedReset<'a> {
    state: &'a mut TypedMutableState,
    env: DecisionEnv,
}

fn typed_err<E: std::fmt::Display>(e: E) -> ResetError {
    ResetError(e.to_string())
}

impl ResetMutableState for TypedReset<'_> {
    fn pending_child_execution_count(&self) -> usize {
        self.state.get_pending_child_execution_infos().len()
    }
    fn in_flight_decision(&self) -> Option<ResetDecisionInfo> {
        self.state
            .get_in_flight_decision()
            .map(|d| ResetDecisionInfo { schedule_id: d.schedule_id, started_id: d.started_id })
    }
    fn pending_decision(&self) -> Option<ResetDecisionInfo> {
        self.state
            .get_pending_decision()
            .map(|d| ResetDecisionInfo { schedule_id: d.schedule_id, started_id: d.started_id })
    }
    fn add_decision_task_failed_event(
        &mut self,
        decision: ResetDecisionInfo,
        base_run_id: &str,
        reset_run_id: &str,
        base_last_event_version: i64,
        reset_reason: &str,
        reset_request_id: &str,
    ) -> Result<(), ResetError> {
        self.state
            .add_decision_task_failed_event(
                &self.env,
                decision.schedule_id,
                decision.started_id,
                DecisionTaskFailedCause::ResetWorkflow,
                Vec::new(),
                IDENTITY_HISTORY_SERVICE,
                reset_reason,
                "",
                base_run_id,
                reset_run_id,
                base_last_event_version,
                reset_request_id,
            )
            .map(|_| ())
            .map_err(typed_err)
    }
    fn add_decision_task_reset_timeout_event(
        &mut self,
        decision: ResetDecisionInfo,
        base_run_id: &str,
        reset_run_id: &str,
        base_last_event_version: i64,
        reset_reason: &str,
        reset_request_id: &str,
    ) -> Result<(), ResetError> {
        self.state
            .add_decision_task_reset_timeout_event(
                &self.env,
                decision.schedule_id,
                base_run_id,
                reset_run_id,
                base_last_event_version,
                reset_reason,
                reset_request_id,
            )
            .map(|_| ())
            .map_err(typed_err)
    }
}

impl PendingActivityMutableState for TypedReset<'_> {
    fn pending_activity_infos(&self) -> Vec<PendingActivityInfo> {
        self.state
            .get_pending_activity_infos()
            .values()
            .map(|ai| PendingActivityInfo {
                schedule_id: ai.schedule_id,
                started_id: ai.started_id,
                scheduled_time_unix_nanos: ai.scheduled_time,
                timer_task_status: ai.timer_task_status,
                // Heartbeat details are persisted separately in the Rust model.
                details: Vec::new(),
                started_identity: ai.started_identity.clone(),
            })
            .collect()
    }
    fn update_pending_activity(&mut self, activity: PendingActivityInfo) -> Result<(), ResetError> {
        let mut ai = self
            .state
            .get_activity_info(activity.schedule_id)
            .cloned()
            .ok_or_else(|| ResetError("unable to get activity info".into()))?;
        ai.scheduled_time = activity.scheduled_time_unix_nanos;
        ai.timer_task_status = activity.timer_task_status;
        self.state.update_activity_checked(ai).map_err(typed_err)
    }
    fn add_activity_task_failed_event(
        &mut self,
        schedule_id: i64,
        started_id: i64,
        reason: &str,
        details: &[u8],
        identity: &str,
    ) -> Result<(), ResetError> {
        let request = RespondActivityTaskFailedRequest {
            reason: Some(reason.to_string()),
            details: details.to_vec(),
            identity: identity.to_string(),
            ..Default::default()
        };
        self.state
            .add_activity_task_failed_event(self.env.now_nanos, schedule_id, started_id, &request)
            .map(|_| ())
            .map_err(typed_err)
    }
}

impl SignalEventReapplier for TypedReset<'_> {
    fn add_workflow_execution_signaled(
        &mut self,
        signal_name: &str,
        input: &[u8],
        identity: &str,
        request_id: &str,
    ) -> Result<(), ResetError> {
        self.state
            .add_workflow_execution_signaled(self.env.now_nanos, signal_name, input, identity, request_id)
            .map(|_| ())
            .map_err(typed_err)
    }
}

impl ResetPreparation for TypedReset<'_> {
    fn current_version(&self) -> i64 {
        self.state.get_current_version()
    }
    fn update_current_version(&mut self, version: i64, force: bool) -> Result<(), ResetError> {
        self.state.update_current_version(version, force).map_err(typed_err)
    }
    fn start_timestamp_unix_nanos(&self) -> i64 {
        self.state.execution_info.start_time_unix_nanos
    }
    /// Go `ScheduleDecision`: no-op when a decision is already pending.
    fn schedule_decision(&mut self) -> Result<(), ResetError> {
        if self.state.get_pending_decision().is_some() {
            return Ok(());
        }
        self.state.add_decision_task_scheduled_event(&self.env, false).map(|_| ()).map_err(typed_err)
    }
}

// ------------------------------------------------------------ history access

struct EngineResetHistory {
    core: Arc<EngineCore>,
    cache: Arc<dyn ExecutionCache>,
}

impl EngineResetHistory {
    fn pager(&self) -> Arc<dyn RebuildPager> {
        match (self.core.state_params)("").history_nodes {
            Some(nodes) => Arc::new(SqlHistoryPager { nodes, shard_id: self.core.shard.shard_id() }),
            None => Arc::new(UnavailableRebuildPager),
        }
    }
}

impl ResetHistory for EngineResetHistory {
    fn paginate_history(
        &self,
        first_event_id: i64,
        next_event_id: i64,
        branch_token: &[u8],
        _domain_id: &str,
        page_token: &[u8],
    ) -> Result<HistoryPage, ResetError> {
        let page = self
            .pager()
            .page(branch_token, first_event_id, next_event_id, page_token.to_vec(), PAGE_SIZE)
            .map_err(typed_err)?;
        Ok(HistoryPage { batches: page.batches, next_page_token: page.next_token })
    }

    /// Go `getRunHistoryInfo`: acquire, load and release (error passed to release).
    fn load_run_history_info(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<(i64, Vec<u8>), ResetError> {
        let execution = WorkflowExecution { workflow_id: workflow_id.to_string(), run_id: run_id.to_string() };
        block_on(async {
            let (context, release) = self
                .cache
                .get_or_create_workflow_execution(domain_id, &execution)
                .await
                .map_err(typed_err)?;
            let loaded = async {
                let state = context.load_workflow_execution().await?;
                Ok::<_, HistoryError>((state.get_next_event_id(), state.get_current_branch_token()?))
            }
            .await;
            release(loaded.as_ref().err());
            loaded.map_err(typed_err)
        })
    }

    fn domain_name(&self, domain_id: &str) -> Result<String, ResetError> {
        self.core.domains.get_domain_name(domain_id).map_err(typed_err)
    }

    fn fork_history_branch(
        &self,
        token: &[u8],
        node_id: i64,
        cleanup_info: &str,
        _domain_name: &str,
    ) -> Result<Vec<u8>, ResetError> {
        let manager = SqlHistoryManager::new(self.core.history_v2.clone(), self.core.shard.shard_id())
            .with_transaction_size_limit(self.core.transaction_size_limit.clone());
        block_on(manager.fork_history_branch(token, node_id, cleanup_info)).map_err(typed_err)
    }
}

// ------------------------------------------------------------ resetter

/// Values read from the current run before it is mutated (Go captures them for
/// the same reason: the caller already holds the workflow lock).
struct CurrentRun {
    run_id: String,
    next_event_id: i64,
    branch_token: Vec<u8>,
    last_write_version: i64,
}

/// Result of the synchronous prepare step; only `Send` data.
struct Prepared {
    snapshot: pc::WorkflowSnapshot,
    events: Vec<HistoryEvent>,
    branch_token: Vec<u8>,
    history_size: i64,
    now: i64,
}

pub struct EngineWorkflowResetter {
    core: Arc<EngineCore>,
    cache: Arc<dyn ExecutionCache>,
}

impl EngineWorkflowResetter {
    pub fn new(core: Arc<EngineCore>, cache: Arc<dyn ExecutionCache>) -> Self {
        Self { core, cache }
    }

    /// Go `prepareResetWorkflow` + `closeTransactionAsSnapshot` of `persistToDB`.
    #[allow(clippy::result_large_err)]
    fn prepare(&self, request: &ResetWorkflow, current: &CurrentRun, reset_version: i64) -> HResult<Prepared> {
        let core = &self.core;
        let now = core.clock.now();
        let history = EngineResetHistory { core: core.clone(), cache: self.cache.clone() };

        // replayResetWorkflow: lazily fork the reset branch, then rebuild.
        let manager = SqlHistoryManager::new(core.history_v2.clone(), core.shard.shard_id())
            .with_transaction_size_limit(core.transaction_size_limit.clone());
        let cleanup_info = format!("{}:{}:{}", request.domain_id, request.workflow_id, request.reset_run_id);
        let fork = || -> Result<Vec<u8>, NdcError> {
            block_on(manager.fork_history_branch(
                &request.base_branch_token,
                request.rebuild_last_event_id + 1,
                &cleanup_info,
            ))
            .map_err(|e| NdcError::internal(e.to_string()))
        };
        let rebuilder = new_state_rebuilder(core);
        let (mut boxed, size) = crate::ndc::conflict_resolver::NdcStateRebuilder::rebuild(
            &rebuilder,
            now,
            &request.domain_id,
            &request.workflow_id,
            &request.base_run_id,
            &request.base_branch_token,
            request.rebuild_last_event_id,
            request.rebuild_last_event_version,
            &request.reset_run_id,
            &fork,
            &request.request_id,
        )
        .map_err(ndc_err)?;
        let owned = boxed
            .as_any_mut()
            .and_then(|a| a.downcast_mut::<OwnedNdcMutableState>())
            .ok_or_else(|| HistoryError::internal("workflow resetter: rebuilt state is not the typed state"))?;
        owned.typed_mut().history_size = size;

        let mut env = DecisionEnv { now_nanos: now, ..Default::default() };
        env.normal_schedule_to_start_timeout_nanos = owned.task_gen_env().normal_schedule_to_start_timeout_nanos;
        env.normal_schedule_to_start_max_attempts = owned.task_gen_env().normal_schedule_to_start_max_attempts;
        let current_info = CurrentWorkflowInfo {
            run_id: current.run_id.clone(),
            next_event_id: current.next_event_id,
            branch_token: current.branch_token.clone(),
            ..Default::default()
        };
        {
            let mut adapter = TypedReset { state: owned.typed_mut(), env };
            prepare_reset_workflow(
                &history,
                &mut adapter,
                &request.domain_id,
                &request.workflow_id,
                &request.base_run_id,
                &request.base_branch_token,
                request.rebuild_last_event_id,
                request.base_next_event_id,
                &request.reset_run_id,
                &request.request_id,
                reset_version,
                &request.reason,
                &request.events,
                request.skip_signal_reapply,
                &current_info,
            )
            .map_err(reset_err)?;
        }
        // Go insertWorkflowRequest only records requests when
        // EnableStrongIdempotency is on (default off); drop un-encoded ones.
        owned.typed_mut().insert_workflow_requests.clear();

        let (snapshot, mut batches) = owned
            .close_transaction_as_snapshot(now, TransactionPolicy::Active)
            .map_err(ndc_err)?;
        if batches.len() != 1 {
            return Err(HistoryError::internal("there should be EXACTLY one batch of events for reset"));
        }
        let batch = batches.remove(0);
        let history_size = owned.typed().history_size;
        owned.transaction_committed();
        Ok(Prepared { snapshot, events: batch.events, branch_token: batch.branch_token, history_size, now })
    }

    /// Go `persistToDB`.
    async fn persist(
        &self,
        request: &ResetWorkflow,
        current: &CurrentRun,
        current_terminated: bool,
        mut prepared: Prepared,
    ) -> HResult<()> {
        let core = &self.core;
        let shard = &core.shard;

        // PersistNonStartWorkflowBatchEvents on the forked branch.
        let ids = shard.generate_task_ids(prepared.events.len()).map_err(shard_err(shard.shard_id()))?;
        for (event, id) in prepared.events.iter_mut().zip(ids.iter()) {
            event.task_id = *id;
        }
        let transaction_id = shard.generate_task_id().map_err(shard_err(shard.shard_id()))?;
        let manager = SqlHistoryManager::new(core.history_v2.clone(), shard.shard_id())
            .with_transaction_size_limit(core.transaction_size_limit.clone());
        let blob = manager
            .append_history_nodes(&prepared.branch_token, &prepared.events, transaction_id, false, "")
            .await
            .map_err(to_history_error)?;
        let history_size = prepared.history_size + blob.data.len() as i64;
        prepared.snapshot.execution_info.history_size = history_size;
        if let Some(id) = ids.last() {
            prepared.snapshot.execution_info.last_event_task_id = *id;
        }

        // Go holds the shard lock from task-id allocation until the write and read-level update finish.
        let gate = shard.lock_task_writes().await;
        let mut hooks = EngineNdcContextHooks::new(core.clone());
        WorkflowContextHooks::<super::ndc_context_hooks::NdcStateBox>::allocate_task_ids(
            &mut hooks,
            &mut prepared.snapshot.tasks_by_category,
        )?;

        let result = if current_terminated {
            // Go UpdateWorkflowExecutionWithNewAsActive.
            let context = request
                .current_context
                .as_any()
                .and_then(|a| a.downcast_ref::<EngineExecutionContext>())
                .ok_or_else(|| HistoryError::internal("workflow resetter: current context is not an engine context"))?;
            context.update_with_new_as_active(prepared.now, prepared.snapshot, &gate).await
        } else {
            let create = pc::CreateWorkflowExecutionRequest {
                shard_id: shard.shard_id(),
                range_id: shard.range_id(),
                mode: pc::CreateWorkflowMode::ContinueAsNew,
                previous_run_id: current.run_id.clone(),
                previous_last_write_version: current.last_write_version,
                new_workflow_snapshot: prepared.snapshot,
                workflow_request_mode: pc::WorkflowRequestMode::New,
                current_timestamp_unix_nanos: prepared.now,
            };
            core.executions.create_workflow_execution(create).await.map(|_| ()).map_err(to_history_error)
        };
        let result = match result {
            Ok(()) => {
                // Publishes the reset run's tasks (the current run's own write,
                // if any, published its tasks itself).
                WorkflowContextHooks::<super::ndc_context_hooks::NdcStateBox>::tasks_committed(&mut hooks);
                Ok(())
            }
            Err(err) => {
                WorkflowContextHooks::<super::ndc_context_hooks::NdcStateBox>::write_failed(&mut hooks, &err);
                Err(err)
            }
        };
        drop(gate);
        result
    }
}

#[async_trait]
impl WorkflowResetter for EngineWorkflowResetter {
    async fn reset_workflow(&self, request: ResetWorkflow) -> HResult<()> {
        let state = request.current_state.clone();
        let mut reset_version = (self.core.state_params)(&request.domain_id).failover_version;

        // Capture the current run before any in-memory mutation (termination adds
        // unpersisted events); the caller already holds the lock.
        let current = CurrentRun {
            run_id: state.get_execution_info().run_id,
            next_event_id: state.get_next_event_id(),
            branch_token: state.get_current_branch_token()?,
            last_write_version: state.get_last_write_version().unwrap_or_default(),
        };

        let mut current_terminated = false;
        if state.is_workflow_execution_running() {
            state.terminate_workflow(current.next_event_id, &request.reason, &[], IDENTITY_HISTORY_SERVICE)?;
            reset_version = state.get_current_version();
            current_terminated = true;
        }

        let prepared = match self.prepare(&request, &current, reset_version) {
            Ok(prepared) => prepared,
            Err(err) => {
                if current_terminated {
                    request.current_context.clear();
                }
                return Err(err);
            }
        };
        let result = self.persist(&request, &current, current_terminated, prepared).await;
        if result.is_err() && current_terminated {
            request.current_context.clear();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::mutable_state_methods_common::TaskGenEnv;
    use crate::reset::{close_pending_decision_task, fail_inflight_activity};

    fn owned() -> OwnedNdcMutableState {
        OwnedNdcMutableState::new(
            "domain",
            &WorkflowExecution { workflow_id: "wf".into(), run_id: "run".into() },
            1,
            TaskGenEnv::default(),
        )
    }

    #[test]
    fn pending_children_map_to_bad_request_and_others_to_internal() {
        let err = reset_err(ResetError("Can not reset workflow with pending child workflows".into()));
        assert_eq!(err.to_string(), HistoryError::bad_request("Can not reset workflow with pending child workflows").to_string());
        assert!(err.to_string().contains("pending child"));
        let err = reset_err(ResetError("workflowResetter encounter version mismatch.".into()));
        assert!(err.to_string().contains("version mismatch"));
    }

    #[test]
    fn fresh_state_has_no_decision_activity_or_children() {
        let mut state = owned();
        let mut adapter = TypedReset { state: state.typed_mut(), env: DecisionEnv::default() };
        assert_eq!(adapter.pending_child_execution_count(), 0);
        assert!(adapter.in_flight_decision().is_none());
        assert!(adapter.pending_decision().is_none());
        assert!(adapter.pending_activity_infos().is_empty());
        close_pending_decision_task(&mut adapter, "base", "reset", 1, "reason", "req").unwrap();
        fail_inflight_activity(&mut adapter, 0, "reason").unwrap();
    }
}
