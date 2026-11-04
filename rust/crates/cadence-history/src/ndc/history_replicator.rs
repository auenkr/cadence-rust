//! Port of Go `ndc/history_replicator.go`.
//!
//! Go's function-valued fields (kept for test injection) are collapsed into
//! direct method calls; the shard/domain/state-builder dependencies are provided
//! through [`HistoryReplicatorEnv`].
use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::WorkflowExecution;
use cadence_persistence::version_history::{new_item, VersionHistoriesExt, VersionHistoryExt};

use super::branch_manager::{BranchManager, BranchManagerEnv};
use super::conflict_resolver::{ConflictResolver, NdcStateRebuilder};
use super::replication_task::{HistoryBatchDeserializer, ReplicationTask};
use super::transaction_manager::NdcTransactionManager;
use super::workflow_resetter::{NdcWorkflowResetter, WorkflowResetterEnv, WorkflowResetterImpl};
use super::{
    new_ndc_retry_task_error_with_hint, NdcClusterMetadata, NdcError, NdcExecutionCache,
    NdcMutableState, NdcResult, NdcWorkflow, ReplicateEventsV2Request,
};
use crate::execution::context::{WorkflowEvents, WorkflowExecutionKey};

/// Go `mutableStateMissingMessage`.
pub const MUTABLE_STATE_MISSING_MESSAGE: &str = "Resend events due to missing mutable state";

/// Go `HistoryReplicator`.
pub trait HistoryReplicator {
    fn apply_events(&self, request: &ReplicateEventsV2Request) -> NdcResult<()>;
}

/// Go `execution.StateBuilder.ApplyEvents`.
pub trait NdcStateBuilder {
    /// Returns the new run's mutable state when `new_events` was supplied.
    fn apply_events(
        &self,
        mutable_state: &mut dyn NdcMutableState,
        domain_id: &str,
        request_id: &str,
        execution: &WorkflowExecution,
        events: &[HistoryEvent],
        new_events: &[HistoryEvent],
    ) -> NdcResult<Option<Box<dyn NdcMutableState>>>;
}

/// Shard / domain-cache / metrics dependencies of Go `historyReplicatorImpl`.
pub trait HistoryReplicatorEnv {
    fn cluster_metadata(&self) -> &dyn NdcClusterMetadata;
    fn serializer(&self) -> &dyn HistoryBatchDeserializer;
    fn execution_cache(&self) -> &dyn NdcExecutionCache;
    fn transaction_manager(&self) -> &dyn NdcTransactionManager;
    fn state_builder(&self) -> &dyn NdcStateBuilder;
    fn state_rebuilder(&self) -> &dyn NdcStateRebuilder;
    fn branch_manager_env(&self) -> &dyn BranchManagerEnv;
    fn resetter_env(&self) -> &dyn WorkflowResetterEnv;
    fn now_nanos(&self) -> i64;
    /// Go `domainCache.GetDomainByID` + `NewMutableStateBuilderWithVersionHistories(.., EmptyVersion)`.
    fn new_mutable_state(&self, domain_id: &str) -> NdcResult<Box<dyn NdcMutableState>>;
    /// Go `uuid.New()`.
    fn new_request_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
    /// Go `shard.GetConfig().StandbyClusterDelay()` in nanos.
    fn standby_cluster_delay_nanos(&self) -> i64;
    /// Go `shard.SetCurrentTime`.
    fn set_current_time(&self, cluster: &str, nanos: i64);
    /// Go `metricsClient.IncCounter(ReplicateHistoryEventsScope, DuplicateReplicationEventsCounter)`.
    fn inc_duplicate_replication_events(&self) {}
    fn log_error(&self, _message: &str, _err: &NdcError) {}
    fn log_warn(&self, _message: &str) {}
}

/// Go `historyReplicatorImpl`.
pub struct HistoryReplicatorImpl<'a> {
    pub env: &'a dyn HistoryReplicatorEnv,
}

impl<'a> HistoryReplicatorImpl<'a> {
    pub fn new(env: &'a dyn HistoryReplicatorEnv) -> Self {
        Self { env }
    }

    fn apply_events_task(&self, task: ReplicationTask) -> NdcResult<()> {
        let workflow = self
            .env
            .execution_cache()
            .get_or_create_workflow_execution(task.get_domain_id(), task.get_execution())?;
        // `slot` holds the workflow until ownership moves into a transaction manager
        // (which then releases it); otherwise the deferred release below runs.
        let mut slot = Some(workflow);
        let outcome = super::guarded(|| self.apply_events_inner(&task, &mut slot));
        super::finish_guarded(outcome, |err| {
            if let Some(w) = slot.as_mut() {
                w.release(err);
            }
        })
    }

    fn apply_events_inner(
        &self,
        task: &ReplicationTask,
        slot: &mut Option<Box<dyn NdcWorkflow>>,
    ) -> NdcResult<()> {
        if task.get_first_event().event_type == Some(EventType::WorkflowExecutionStarted) {
            let wf = slot.take().expect("workflow");
            return self.apply_start_events(wf, task);
        }
        // apply events, other than simple start workflow execution
        // the continue as new + start workflow execution combination will also be processed here
        let load = slot
            .as_mut()
            .expect("workflow")
            .load_workflow_execution(Some(task.get_version()));
        match load {
            Ok(()) => {
                // Sanity check to make only 3DC mutable state here
                if slot.as_ref().unwrap().mutable_state().get_version_histories().is_none() {
                    return Err(NdcError::missing_version_histories());
                }
                let (do_continue, branch_index) =
                    self.apply_non_start_events_prepare_branch(slot.as_mut().unwrap().as_mut(), task)?;
                if !do_continue {
                    self.env.inc_duplicate_replication_events();
                    return Ok(());
                }
                let is_rebuilt = self.apply_non_start_events_prepare_mutable_state(
                    slot.as_mut().unwrap().as_mut(),
                    branch_index,
                    task,
                )?;
                let current_index = slot
                    .as_ref()
                    .unwrap()
                    .mutable_state()
                    .get_version_histories()
                    .ok_or_else(NdcError::missing_version_histories)?
                    .current_version_history_index;
                if current_index == branch_index {
                    let wf = slot.take().unwrap();
                    return self.apply_non_start_events_to_current_branch(wf, is_rebuilt, task);
                }
                self.apply_non_start_events_to_none_current_branch(slot, branch_index, task)
            }
            Err(NdcError::EntityNotExists(_)) => {
                // mutable state not created, check if is workflow reset
                let mutable_state =
                    self.apply_non_start_events_missing_mutable_state(slot.as_mut().unwrap().as_mut(), task)?;
                let wf = slot.take().unwrap();
                self.apply_non_start_events_reset_workflow(wf, mutable_state, task)
            }
            // unable to get mutable state, return err so we can retry the task later
            Err(e) => Err(e),
        }
    }

    fn apply_start_events(&self, mut workflow: Box<dyn NdcWorkflow>, task: &ReplicationTask) -> NdcResult<()> {
        let mut mutable_state = match self.env.new_mutable_state(task.get_domain_id()) {
            Ok(m) => m,
            Err(e) => {
                workflow.release(Some(&e));
                return Err(e);
            }
        };
        // requestID used for start workflow execution request. This is not on the history event.
        let request_id = self.env.new_request_id();
        // use state builder for workflow mutable state mutation
        if let Err(e) = self.env.state_builder().apply_events(
            mutable_state.as_mut(),
            task.get_domain_id(),
            &request_id,
            task.get_execution(),
            task.get_events(),
            task.get_new_events(),
        ) {
            self.env
                .log_error("nDCHistoryReplicator unable to apply events when applyStartEvents", &e);
            workflow.release(Some(&e));
            return Err(e);
        }
        workflow.replace_mutable_state(mutable_state);
        let result = self.env.transaction_manager().create_workflow(task.get_event_time(), workflow);
        match &result {
            Err(e) => self
                .env
                .log_error("nDCHistoryReplicator unable to create workflow when applyStartEvents", e),
            Ok(()) => self.notify(task.get_source_cluster(), task.get_event_time()),
        }
        result
    }

    fn apply_non_start_events_prepare_branch(
        &self,
        workflow: &mut dyn NdcWorkflow,
        task: &ReplicationTask,
    ) -> NdcResult<(bool, i32)> {
        let branch_manager = BranchManager::new(self.env.branch_manager_env());
        match branch_manager.prepare_version_history(
            workflow,
            task.get_version_history(),
            task.get_first_event().event_id,
            task.get_first_event().version,
        ) {
            Ok(v) => Ok(v),
            // replication message can arrive out of order; do not log
            Err(e @ NdcError::RetryTaskV2(_)) => Err(e),
            Err(e) => {
                self.env.log_error(
                    "nDCHistoryReplicator unable to prepare version history when applyNonStartEventsPrepareBranch",
                    &e,
                );
                Err(e)
            }
        }
    }

    fn apply_non_start_events_prepare_mutable_state(
        &self,
        workflow: &mut dyn NdcWorkflow,
        branch_index: i32,
        task: &ReplicationTask,
    ) -> NdcResult<bool> {
        let resolver = ConflictResolver::new(self.env.state_rebuilder());
        match resolver.prepare_mutable_state(workflow, branch_index, task.get_version()) {
            Ok(prepared) => {
                if let Some(rebuilt) = prepared.rebuilt {
                    workflow.replace_mutable_state(rebuilt);
                }
                Ok(prepared.is_rebuilt)
            }
            Err(e) => {
                self.env.log_error(
                    "nDCHistoryReplicator unable to prepare mutable state when applyNonStartEventsPrepareMutableState",
                    &e,
                );
                Err(e)
            }
        }
    }

    fn apply_non_start_events_to_current_branch(
        &self,
        mut workflow: Box<dyn NdcWorkflow>,
        is_rebuilt: bool,
        task: &ReplicationTask,
    ) -> NdcResult<()> {
        let request_id = self.env.new_request_id();
        let applied = self.env.state_builder().apply_events(
            workflow.mutable_state_mut(),
            task.get_domain_id(),
            &request_id,
            task.get_execution(),
            task.get_events(),
            task.get_new_events(),
        );
        let new_mutable_state = match applied {
            Ok(m) => m,
            Err(e) => {
                self.env.log_error(
                    "nDCHistoryReplicator unable to apply events when applyNonStartEventsToCurrentBranch",
                    &e,
                );
                workflow.release(Some(&e));
                return Err(e);
            }
        };

        let new_workflow = new_mutable_state.map(|ms| {
            let info = ms.get_execution_info();
            let execution = WorkflowExecution {
                workflow_id: info.workflow_id.clone(),
                run_id: info.run_id.clone(),
            };
            self.env
                .execution_cache()
                .new_workflow(&info.domain_id, &execution, ms, true)
        });

        let result = self.env.transaction_manager().update_workflow(
            task.get_event_time(),
            is_rebuilt,
            workflow,
            new_workflow,
        );
        match &result {
            Err(e) => self.env.log_error(
                "nDCHistoryReplicator unable to update workflow when applyNonStartEventsToCurrentBranch",
                e,
            ),
            Ok(()) => self.notify(task.get_source_cluster(), task.get_event_time()),
        }
        result
    }

    fn apply_non_start_events_to_none_current_branch(
        &self,
        slot: &mut Option<Box<dyn NdcWorkflow>>,
        branch_index: i32,
        task: &ReplicationTask,
    ) -> NdcResult<()> {
        if !task.get_new_events().is_empty() {
            return self.apply_non_start_events_to_none_current_branch_with_continue_as_new(slot, task);
        }
        let wf = slot.take().unwrap();
        self.apply_non_start_events_to_none_current_branch_without_continue_as_new(wf, branch_index, task)
    }

    fn apply_non_start_events_to_none_current_branch_without_continue_as_new(
        &self,
        mut workflow: Box<dyn NdcWorkflow>,
        branch_index: i32,
        task: &ReplicationTask,
    ) -> NdcResult<()> {
        let prepared = (|| -> NdcResult<Vec<u8>> {
            let item = new_item(task.get_last_event().event_id, task.get_last_event().version);
            let histories = workflow
                .mutable_state_mut()
                .get_version_histories_mut()
                .ok_or_else(NdcError::missing_version_histories)?;
            // validate the index first (Go GetVersionHistory)
            histories.history(branch_index)?;
            let version_history = &mut histories.histories[branch_index as usize];
            version_history.add_or_update(&item)?;
            Ok(version_history.branch_token.clone())
        })();
        let branch_token = match prepared {
            Ok(t) => t,
            Err(e) => {
                workflow.release(Some(&e));
                return Err(e);
            }
        };
        let events = WorkflowEvents {
            key: WorkflowExecutionKey {
                domain_id: task.get_domain_id().to_string(),
                workflow_id: task.get_execution().workflow_id.clone(),
                run_id: task.get_execution().run_id.clone(),
            },
            branch_token,
            events: task.get_events().to_vec(),
        };
        let result = self
            .env
            .transaction_manager()
            .backfill_workflow(task.get_event_time(), workflow, &events);
        if let Err(e) = &result {
            self.env.log_error(
                "nDCHistoryReplicator unable to backfill workflow when applyNonStartEventsToNoneCurrentBranch",
                e,
            );
        }
        result
    }

    fn apply_non_start_events_to_none_current_branch_with_continue_as_new(
        &self,
        slot: &mut Option<Box<dyn NdcWorkflow>>,
        task: &ReplicationTask,
    ) -> NdcResult<()> {
        // workflow backfill to non current branch with continue as new
        // first, release target workflow lock & create the new workflow as zombie
        // NOTE: need to release target workflow due to target workflow
        //  can potentially be the current workflow causing deadlock

        // 1. clear all in memory changes & release target workflow lock
        // 2. apply new workflow first
        // 3. apply target workflow

        // step 1
        {
            let mut wf = slot.take().unwrap();
            wf.clear();
            wf.release(None);
        }

        // step 2
        let mut task = task.clone();
        let start_time = self.env.now_nanos();
        let new_task = task.split_task(start_time)?;
        if let Err(e) = self.apply_events_task(new_task) {
            self.env.log_error(
                "nDCHistoryReplicator unable to create new workflow when applyNonStartEventsToNoneCurrentBranchWithContinueAsNew",
                &e,
            );
            return Err(e);
        }

        // step 3
        if let Err(e) = self.apply_events_task(task) {
            self.env.log_error(
                "nDCHistoryReplicator unable to create target workflow when applyNonStartEventsToNoneCurrentBranchWithContinueAsNew",
                &e,
            );
            return Err(e);
        }
        Ok(())
    }

    fn apply_non_start_events_missing_mutable_state(
        &self,
        new_workflow: &mut dyn NdcWorkflow,
        task: &ReplicationTask,
    ) -> NdcResult<Box<dyn NdcMutableState>> {
        // for non reset workflow execution replication task, just do re-replication
        if !task.is_workflow_reset() {
            let first_event = task.get_first_event();
            return Err(new_ndc_retry_task_error_with_hint(
                MUTABLE_STATE_MISSING_MESSAGE,
                task.get_domain_id(),
                task.get_workflow_id(),
                task.get_run_id(),
                None,
                None,
                Some(first_event.event_id),
                Some(first_event.version),
            ));
        }

        let decision_task_event = task.get_first_event();
        let base_event_id = decision_task_event.event_id - 1;
        let (base_run_id, new_run_id, base_event_version, _) = task.get_workflow_reset_metadata();

        let mut resetter = WorkflowResetterImpl::new(
            self.env.resetter_env(),
            self.env.transaction_manager(),
            self.env.state_rebuilder(),
            task.get_domain_id(),
            task.get_workflow_id(),
            &base_run_id,
            new_workflow,
            &new_run_id,
        );
        match resetter.reset_workflow(
            task.get_event_time(),
            base_event_id,
            base_event_version,
            task.get_first_event().event_id,
            task.get_version(),
        ) {
            Ok(ms) => Ok(ms),
            Err(e) => {
                self.env.log_error(
                    "nDCHistoryReplicator unable to reset workflow when applyNonStartEventsMissingMutableState",
                    &e,
                );
                Err(e)
            }
        }
    }

    fn apply_non_start_events_reset_workflow(
        &self,
        mut workflow: Box<dyn NdcWorkflow>,
        mut mutable_state: Box<dyn NdcMutableState>,
        task: &ReplicationTask,
    ) -> NdcResult<()> {
        let request_id = self.env.new_request_id();
        if let Err(e) = self.env.state_builder().apply_events(
            mutable_state.as_mut(),
            task.get_domain_id(),
            &request_id,
            task.get_execution(),
            task.get_events(),
            task.get_new_events(),
        ) {
            self.env.log_error(
                "nDCHistoryReplicator unable to apply events when applyNonStartEventsResetWorkflow",
                &e,
            );
            workflow.release(Some(&e));
            return Err(e);
        }
        workflow.replace_mutable_state(mutable_state);
        // Go wraps with NoopReleaseFn and relies on the outer deferred release; the
        // transaction manager releases the owned workflow here instead.
        let result = self.env.transaction_manager().create_workflow(task.get_event_time(), workflow);
        match &result {
            Err(e) => self.env.log_error(
                "nDCHistoryReplicator unable to create workflow when applyNonStartEventsResetWorkflow",
                e,
            ),
            Ok(()) => self.notify(task.get_source_cluster(), task.get_event_time()),
        }
        result
    }

    /// Go `notify`.
    fn notify(&self, cluster_name: &str, now_nanos: i64) {
        if cluster_name == self.env.cluster_metadata().get_current_cluster_name() {
            // this is a valid use case for testing, but not for production
            self.env
                .log_warn("nDCHistoryReplicator applying events generated by current cluster");
            return;
        }
        let now = now_nanos - self.env.standby_cluster_delay_nanos();
        self.env.set_current_time(cluster_name, now);
    }
}

impl HistoryReplicator for HistoryReplicatorImpl<'_> {
    fn apply_events(&self, request: &ReplicateEventsV2Request) -> NdcResult<()> {
        let start_time = self.env.now_nanos();
        let task = ReplicationTask::new(
            self.env.cluster_metadata(),
            self.env.serializer(),
            start_time,
            request,
        )?;
        self.apply_events_task(task)
    }
}
