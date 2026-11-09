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
