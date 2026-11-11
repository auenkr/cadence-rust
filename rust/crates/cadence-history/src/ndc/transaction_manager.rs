//! Port of Go `ndc/transaction_manager.go`.
use cadence_api::service_types::HistoryEvent;
use cadence_persistence::history_contract::{UpdateWorkflowMode, WorkflowRequestMode};
use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};

use super::existing_workflow_transaction_manager::TransactionManagerForExistingWorkflow;
use super::events_reapplier::EventsReapplier;
use super::new_workflow_transaction_manager::TransactionManagerForNewWorkflow;
use super::{
    finish_guarded, get_reapplicable_events, guarded, NdcError, NdcResult, NdcWorkflow, EMPTY_EVENT_ID,
};
use crate::execution::context::WorkflowEvents;
use crate::execution::workflow::TransactionPolicy;

/// Go `transactionPolicy` (ndc-local transaction kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NdcTransactionPolicy {
    CreateAsCurrent,
    CreateAsZombie,
    SuppressCurrentAndCreateAsCurrent,
    UpdateAsCurrent,
    UpdateAsZombie,
    ConflictResolveAsCurrent,
    ConflictResolveAsZombie,
    SuppressCurrentAndUpdateAsCurrent,
}

pub const EVENTS_REAPPLICATION_RESET_WORKFLOW_REASON: &str = "events-reapplication";

/// Go `transactionManager`.
pub trait NdcTransactionManager {
    fn create_workflow(&self, now_nanos: i64, target_workflow: Box<dyn NdcWorkflow>) -> NdcResult<()>;
    fn update_workflow(
        &self,
        now_nanos: i64,
        is_workflow_rebuilt: bool,
        target_workflow: Box<dyn NdcWorkflow>,
        new_workflow: Option<Box<dyn NdcWorkflow>>,
    ) -> NdcResult<()>;
    fn backfill_workflow(
        &self,
        now_nanos: i64,
        target_workflow: Box<dyn NdcWorkflow>,
        target_workflow_events: &WorkflowEvents,
    ) -> NdcResult<()>;
    fn check_workflow_exists(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<bool>;
    /// Returns "" when there is no current run.
    fn get_current_workflow_run_id(&self, domain_id: &str, workflow_id: &str) -> NdcResult<String>;
    fn load_ndc_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> NdcResult<Box<dyn NdcWorkflow>>;
}

/// Shard/persistence dependencies of Go `transactionManagerImpl`.
pub trait TransactionManagerEnv {
    fn get_domain_name(&self, domain_id: &str) -> NdcResult<String>;
    /// Go `shard.GetWorkflowExecution`; `EntityNotExists` error when absent.
    fn get_workflow_execution(&self, domain_id: &str, domain_name: &str, workflow_id: &str, run_id: &str) -> NdcResult<()>;
    /// Go `GetExecutionManager().GetCurrentExecution`; `EntityNotExists` when absent.
    fn get_current_execution(&self, domain_id: &str, domain_name: &str, workflow_id: &str) -> NdcResult<String>;
    fn get_current_cluster_name(&self) -> String;
    /// `activeClusterManager.GetActiveClusterInfoByClusterAttribute(...).ActiveClusterName`
    /// for the workflow's active-cluster selection policy.
    fn get_active_cluster_name(&self, ms: &dyn super::NdcMutableState) -> NdcResult<String>;
    fn load_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<Box<dyn NdcWorkflow>>;
    /// Go `workflowResetter.ResetWorkflow` (reset package).
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
    /// Go `metricsClient.IncCounter(HistoryReapplyEventsScope, EventReapplySkippedCount)`.
    fn inc_reapply_events_skipped(&self) {}
}

/// Go `transactionManagerImpl`.
pub struct TransactionManagerImpl<'a> {
    pub env: &'a dyn TransactionManagerEnv,
    pub events_reapplier: &'a dyn EventsReapplier,
}

impl<'a> TransactionManagerImpl<'a> {
    pub fn new(env: &'a dyn TransactionManagerEnv, events_reapplier: &'a dyn EventsReapplier) -> Self {
        Self { env, events_reapplier }
    }

    fn backfill_workflow_events_reapply(
        &self,
        target: &mut dyn NdcWorkflow,
        events: &WorkflowEvents,
    ) -> NdcResult<(UpdateWorkflowMode, TransactionPolicy)> {
        let is_current = self.is_workflow_current(target)?;
        let is_running = target.mutable_state().is_workflow_execution_running();
        let target_active_cluster = self.env.get_active_cluster_name(target.mutable_state())?;
        let current_cluster = self.env.get_current_cluster_name();
        let is_active_cluster = target_active_cluster == current_cluster;

        if is_current && is_active_cluster {
            if is_running {
                let run_id = target.mutable_state().get_execution_info().run_id;
                self.events_reapplier
                    .reapply_events(target.mutable_state_mut(), &events.events, &run_id)?;
                return Ok((UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Active));
            }

            let base_ms = target.mutable_state();
            let info = base_ms.get_execution_info();
            let (domain_id, workflow_id, base_run_id) = (info.domain_id, info.workflow_id, info.run_id);

            let events_to_reapply = get_reapplicable_events(base_ms, &events.events, &base_run_id)?;
            if events_to_reapply.is_empty() {
                return Ok((UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Passive));
            }

            let reset_run_id = uuid::Uuid::new_v4().to_string();
            let base_rebuild_last_event_id = base_ms.get_previous_started_event_id();
            if base_rebuild_last_event_id == EMPTY_EVENT_ID {
                // cannot reapply event to a finished workflow
                self.env.inc_reapply_events_skipped();
                return Ok((UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Passive));
            }

            let base_vhs = base_ms
                .get_version_histories()
                .ok_or_else(NdcError::missing_version_histories)?;
            let base_current_vh = base_vhs.current_history()?;
            let base_rebuild_last_event_version = base_current_vh.event_version(base_rebuild_last_event_id)?;
            let base_current_branch_token = base_current_vh.branch_token.clone();
            let base_next_event_id = base_ms.get_next_event_id();

            self.env.reset_workflow(
                &domain_id,
                &workflow_id,
                &base_run_id,
                &base_current_branch_token,
                base_rebuild_last_event_id,
                base_rebuild_last_event_version,
                base_next_event_id,
                &reset_run_id,
                &uuid::Uuid::new_v4().to_string(),
                target,
                EVENTS_REAPPLICATION_RESET_WORKFLOW_REASON,
                &events_to_reapply,
                false,
            )?;
            return Ok((UpdateWorkflowMode::BypassCurrent, TransactionPolicy::Passive));
        }

        target.reapply_events(std::slice::from_ref(events))?;

        if is_current {
            return Ok((UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Passive));
        }
        Ok((UpdateWorkflowMode::BypassCurrent, TransactionPolicy::Passive))
    }

    fn is_workflow_current(&self, target: &dyn NdcWorkflow) -> NdcResult<bool> {
        if target.mutable_state().is_current_workflow_guaranteed() {
            return Ok(true);
        }
        let info = target.mutable_state().get_execution_info();
        let current_run_id = self.get_current_workflow_run_id(&info.domain_id, &info.workflow_id)?;
        Ok(current_run_id == info.run_id)
    }
}

impl<'a> NdcTransactionManager for TransactionManagerImpl<'a> {
    fn create_workflow(&self, now_nanos: i64, target_workflow: Box<dyn NdcWorkflow>) -> NdcResult<()> {
        TransactionManagerForNewWorkflow::new(self).dispatch_for_new_workflow(now_nanos, target_workflow)
    }

    fn update_workflow(
        &self,
        now_nanos: i64,
        is_workflow_rebuilt: bool,
        target_workflow: Box<dyn NdcWorkflow>,
        new_workflow: Option<Box<dyn NdcWorkflow>>,
    ) -> NdcResult<()> {
        TransactionManagerForExistingWorkflow::new(self).dispatch_for_existing_workflow(
            now_nanos,
            is_workflow_rebuilt,
            target_workflow,
            new_workflow,
        )
    }

    fn backfill_workflow(
        &self,
        now_nanos: i64,
        mut target_workflow: Box<dyn NdcWorkflow>,
        target_workflow_events: &WorkflowEvents,
    ) -> NdcResult<()> {
        let outcome = {
            let tw: &mut dyn NdcWorkflow = target_workflow.as_mut();
            guarded(|| -> NdcResult<()> {
                tw.persist_non_start_workflow_batch_events(target_workflow_events)?;
                let (update_mode, policy) =
                    self.backfill_workflow_events_reapply(tw, target_workflow_events)?;
                tw.update_workflow_execution_with_new(
                    now_nanos,
                    update_mode,
                    None,
                    policy,
                    None,
                    WorkflowRequestMode::Replicated,
                )
            })
        };
        finish_guarded(outcome, |e| target_workflow.release(e))
    }

    fn check_workflow_exists(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<bool> {
        let domain_name = self.env.get_domain_name(domain_id)?;
        match self.env.get_workflow_execution(domain_id, &domain_name, workflow_id, run_id) {
            Ok(()) => Ok(true),
            Err(NdcError::EntityNotExists(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn get_current_workflow_run_id(&self, domain_id: &str, workflow_id: &str) -> NdcResult<String> {
        let domain_name = self.env.get_domain_name(domain_id)?;
        match self.env.get_current_execution(domain_id, &domain_name, workflow_id) {
            Ok(run_id) => Ok(run_id),
            Err(NdcError::EntityNotExists(_)) => Ok(String::new()),
            Err(e) => Err(e),
        }
    }

    fn load_ndc_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> NdcResult<Box<dyn NdcWorkflow>> {
        // Go: GetOrCreateWorkflowExecution, LoadWorkflowExecution (release(err) on failure),
        // then NewWorkflow(...). Provided by the environment adapter.
        self.env.load_workflow(domain_id, workflow_id, run_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::WorkflowExecutionKey;
    use crate::ndc::test_support::{Calls, MockEnv, MockMs, MockWorkflow, ResetCall, SharedCalls};
    use crate::ndc::NdcExecutionInfo;
    use cadence_api::enums::EventType;
    use cadence_persistence::version_history::{new_history, new_item};
    use cadence_api::types::VersionHistories;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    const DOMAIN: &str = "some random domain ID";
    const WF: &str = "some random workflow ID";
    const RUN: &str = "some random run ID";
    const FAILOVER_VERSION: i64 = 1;

    struct CountingReapplier {
        calls: Cell<usize>,
        run_ids: RefCell<Vec<String>>,
    }
    impl CountingReapplier {
        fn new() -> Self {
            Self { calls: Cell::new(0), run_ids: RefCell::new(Vec::new()) }
        }
    }
    impl EventsReapplier for CountingReapplier {
        fn reapply_events(
            &self,
            _ms: &mut dyn super::super::NdcMutableState,
            events: &[HistoryEvent],
            run_id: &str,
        ) -> NdcResult<Vec<HistoryEvent>> {
            self.calls.set(self.calls.get() + 1);
            self.run_ids.borrow_mut().push(run_id.to_string());
            Ok(events.to_vec())
        }
    }

    fn event(id: i64, t: EventType) -> HistoryEvent {
        HistoryEvent { event_id: id, version: FAILOVER_VERSION, event_type: Some(t), ..Default::default() }
    }

    fn events(evs: Vec<HistoryEvent>) -> WorkflowEvents {
        WorkflowEvents {
            key: WorkflowExecutionKey { domain_id: DOMAIN.into(), workflow_id: WF.into(), run_id: RUN.into() },
            branch_token: Vec::new(),
            events: evs,
        }
    }

    fn env(calls: &SharedCalls, active_is_current: bool, current_run: NdcResult<String>) -> MockEnv {
        MockEnv {
            domain_name: Ok("some random domainName".into()),
            workflow_execution: Ok(()),
            current_execution: current_run,
            current_cluster: "active".into(),
            active_cluster: if active_is_current { "active".into() } else { "standby".into() },
            calls: calls.clone(),
        }
    }

    fn wf(calls: &SharedCalls, running: bool, guaranteed: bool) -> Box<MockWorkflow> {
        Box::new(MockWorkflow {
            ms: MockMs {
                running,
                guaranteed,
                info: NdcExecutionInfo {
                    domain_id: DOMAIN.into(),
                    workflow_id: WF.into(),
                    run_id: RUN.into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            calls: calls.clone(),
        })
    }

    fn shared() -> SharedCalls {
        Rc::new(RefCell::new(Calls::default()))
    }

    fn assert_update(calls: &SharedCalls, mode: UpdateWorkflowMode, policy: TransactionPolicy) {
        let c = calls.borrow();
        assert_eq!(c.persisted_non_start, 1);
        assert_eq!(c.update_with_new, vec![(mode, policy, false, WorkflowRequestMode::Replicated)]);
        assert_eq!(c.released, Some(None), "release called without error");
    }

    /// Go `TestBackfillWorkflow_CurrentWorkflow_Active_Open`.
    #[test]
    fn backfill_current_workflow_active_open() {
        let calls = shared();
        let env = env(&calls, true, Ok(RUN.into()));
        let reapplier = CountingReapplier::new();
        let tm = TransactionManagerImpl::new(&env, &reapplier);
        let evs = events(vec![event(1, EventType::WorkflowExecutionStarted)]);
        tm.backfill_workflow(0, wf(&calls, true, true), &evs).unwrap();
        assert_eq!(reapplier.calls.get(), 1);
        assert_eq!(*reapplier.run_ids.borrow(), vec![RUN.to_string()]);
        assert_update(&calls, UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Active);
    }

    /// Go `TestBackfillWorkflow_CurrentWorkflow_Active_Closed`.
    #[test]
    fn backfill_current_workflow_active_closed() {
        let calls = shared();
        let env = env(&calls, true, Ok(RUN.into()));
        let reapplier = CountingReapplier::new();
        let tm = TransactionManagerImpl::new(&env, &reapplier);
        let last_started = 9999;
        let mut w = wf(&calls, false, false);
        w.ms.previous_started = last_started;
        w.ms.next_event_id = last_started * 2;
        w.ms.vh = Some(VersionHistories {
            current_version_history_index: 0,
            histories: vec![new_history(b"branch token".to_vec(), &[new_item(last_started, FAILOVER_VERSION)])],
        });
        w.ms.duplicated.insert((RUN.into(), 2, FAILOVER_VERSION), true);
        let dup = event(2, EventType::WorkflowExecutionSignaled);
        let new_signal = event(3, EventType::WorkflowExecutionSignaled);
        let evs = events(vec![event(1, EventType::DecisionTaskCompleted), dup, new_signal.clone()]);
        tm.backfill_workflow(0, w, &evs).unwrap();
        assert_eq!(reapplier.calls.get(), 0);
        assert_eq!(
            calls.borrow().resets,
            vec![ResetCall {
                domain_id: DOMAIN.into(),
                workflow_id: WF.into(),
                base_run_id: RUN.into(),
                branch_token: b"branch token".to_vec(),
                last_event_id: last_started,
                last_event_version: FAILOVER_VERSION,
                next_event_id: last_started * 2,
                reason: EVENTS_REAPPLICATION_RESET_WORKFLOW_REASON.into(),
                events: vec![new_signal],
                skip_signal_reapply: false,
            }]
        );
        assert_update(&calls, UpdateWorkflowMode::BypassCurrent, TransactionPolicy::Passive);
    }

    /// Go `TestBackfillWorkflow_CurrentWorkflow_Active_Closed_SkipReset`.
    #[test]
    fn backfill_current_workflow_active_closed_skip_reset() {
        for (name, signal_duplicated, previous_started, want_skip_metric) in [
            ("no reapplicable events", true, 0, 0),
            ("no decision completed", false, EMPTY_EVENT_ID, 1),
        ] {
            let calls = shared();
            let env = env(&calls, true, Ok(RUN.into()));
            let reapplier = CountingReapplier::new();
            let tm = TransactionManagerImpl::new(&env, &reapplier);
            let mut w = wf(&calls, false, false);
            w.ms.previous_started = previous_started;
            if signal_duplicated {
                w.ms.duplicated.insert((RUN.into(), 2, FAILOVER_VERSION), true);
            }
            let evs = events(vec![
                event(1, EventType::DecisionTaskCompleted),
                event(2, EventType::WorkflowExecutionSignaled),
            ]);
            tm.backfill_workflow(0, w, &evs).unwrap();
            assert!(calls.borrow().resets.is_empty(), "{name}");
            assert_eq!(calls.borrow().skipped_metric, want_skip_metric, "{name}");
            // no reset happened, so the target workflow is still current
            assert_update(&calls, UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Passive);
        }
    }

    /// Go `TestGetReapplicableEvents`.
    #[test]
    fn get_reapplicable_events_cases() {
        let s1 = event(1, EventType::WorkflowExecutionSignaled);
        let s2 = event(2, EventType::WorkflowExecutionSignaled);
        let non = event(3, EventType::DecisionTaskCompleted);
        let cases: Vec<(&str, Vec<HistoryEvent>, Vec<i64>, Vec<HistoryEvent>)> = vec![
            ("nil events", vec![], vec![], vec![]),
            ("no signal events", vec![non.clone()], vec![], vec![]),
            ("all signals duplicated", vec![s1.clone(), non.clone(), s2.clone()], vec![1, 2], vec![]),
            ("only signals not yet applied", vec![s1.clone(), non, s2.clone()], vec![1], vec![s2]),
        ];
        for (name, evs, dups, want) in cases {
            let mut ms = MockMs::default();
            for d in dups {
                ms.duplicated.insert((RUN.into(), d, FAILOVER_VERSION), true);
            }
            assert_eq!(get_reapplicable_events(&ms, &evs, RUN).unwrap(), want, "{name}");
        }
    }

    /// Go `TestBackfillWorkflow_CurrentWorkflow_Passive_Open`.
    #[test]
    fn backfill_current_workflow_passive_open() {
        let calls = shared();
        let env = env(&calls, false, Ok(RUN.into()));
        let reapplier = CountingReapplier::new();
        let tm = TransactionManagerImpl::new(&env, &reapplier);
        let evs = events(vec![event(1, EventType::WorkflowExecutionStarted)]);
        tm.backfill_workflow(0, wf(&calls, true, true), &evs).unwrap();
        assert_eq!(calls.borrow().reapplied, 1);
        assert_eq!(reapplier.calls.get(), 0);
        assert_update(&calls, UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Passive);
    }

    /// Go `TestBackfillWorkflow_CurrentWorkflow_Passive_Closed`.
    #[test]
    fn backfill_current_workflow_passive_closed() {
        let calls = shared();
        let env = env(&calls, false, Ok(RUN.into()));
        let reapplier = CountingReapplier::new();
        let tm = TransactionManagerImpl::new(&env, &reapplier);
        tm.backfill_workflow(0, wf(&calls, false, false), &events(vec![])).unwrap();
        assert_eq!(calls.borrow().reapplied, 1);
        assert_update(&calls, UpdateWorkflowMode::UpdateCurrent, TransactionPolicy::Passive);
    }

    /// Go `TestBackfillWorkflow_NotCurrentWorkflow_Active` / `_Passive`.
    #[test]
    fn backfill_not_current_workflow() {
        for active in [true, false] {
            let calls = shared();
            let env = env(&calls, active, Ok("other random run ID".into()));
            let reapplier = CountingReapplier::new();
            let tm = TransactionManagerImpl::new(&env, &reapplier);
            let evs = events(vec![event(0, EventType::WorkflowExecutionSignaled)]);
            tm.backfill_workflow(0, wf(&calls, false, false), &evs).unwrap();
            assert_eq!(calls.borrow().reapplied, 1);
            assert_update(&calls, UpdateWorkflowMode::BypassCurrent, TransactionPolicy::Passive);
        }
    }

    /// Go `TestCheckWorkflowExists_DoesNotExists` / `_DoesExists`.
    #[test]
    fn check_workflow_exists() {
        let calls = shared();
        let reapplier = CountingReapplier::new();
        let mut e = env(&calls, true, Ok(RUN.into()));
        e.workflow_execution = Err(NdcError::EntityNotExists("x".into()));
        assert!(!TransactionManagerImpl::new(&e, &reapplier).check_workflow_exists(DOMAIN, WF, RUN).unwrap());
        e.workflow_execution = Ok(());
        assert!(TransactionManagerImpl::new(&e, &reapplier).check_workflow_exists(DOMAIN, WF, RUN).unwrap());
        e.workflow_execution = Err(NdcError::Other("boom".into()));
        assert!(TransactionManagerImpl::new(&e, &reapplier).check_workflow_exists(DOMAIN, WF, RUN).is_err());
    }

    /// Go `TestGetWorkflowCurrentRunID_Missing` / `_Exists`.
    #[test]
    fn get_workflow_current_run_id() {
        let calls = shared();
        let reapplier = CountingReapplier::new();
        let e = env(&calls, true, Err(NdcError::EntityNotExists("x".into())));
        assert_eq!(TransactionManagerImpl::new(&e, &reapplier).get_current_workflow_run_id(DOMAIN, WF).unwrap(), "");
        let e = env(&calls, true, Ok(RUN.into()));
        assert_eq!(TransactionManagerImpl::new(&e, &reapplier).get_current_workflow_run_id(DOMAIN, WF).unwrap(), RUN);
    }
}
