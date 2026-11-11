//! Port of Go `ndc/new_workflow_transaction_manager.go`.
use cadence_api::enums::EventType;
use cadence_persistence::history_contract::{
    CreateWorkflowMode, UpdateWorkflowMode, WorkflowRequestMode,
};

use super::transaction_manager::{NdcTransactionManager, NdcTransactionPolicy};
use super::{finish_guarded, guarded, NdcError, NdcResult, NdcWorkflow};
use crate::execution::context::PersistedBlob;
use crate::execution::workflow::TransactionPolicy;

/// Go `transactionManagerForNewWorkflowImpl`.
pub struct TransactionManagerForNewWorkflow<'a> {
    pub transaction_manager: &'a dyn NdcTransactionManager,
}

impl<'a> TransactionManagerForNewWorkflow<'a> {
    pub fn new(transaction_manager: &'a dyn NdcTransactionManager) -> Self {
        Self { transaction_manager }
    }

    /// Go `dispatchForNewWorkflow`. Takes ownership of `target_workflow` and releases it.
    pub fn dispatch_for_new_workflow(
        &self,
        now: i64,
        mut target_workflow: Box<dyn NdcWorkflow>,
    ) -> NdcResult<()> {
        let info = target_workflow.mutable_state().get_execution_info();
        let (domain_id, workflow_id, target_run_id) = (info.domain_id, info.workflow_id, info.run_id);

        // Go returns without releasing target on these early paths and relies on the
        // caller's deferred release; Rust moves the workflow in, so release it here.
        let current_run_id = match self
            .transaction_manager
            .get_current_workflow_run_id(&domain_id, &workflow_id)
        {
            Ok(r) => r,
            Err(e) => {
                target_workflow.release(Some(&e));
                return Err(e);
            }
        };
        if current_run_id == target_run_id {
            target_workflow.release(None);
            return Ok(());
        }

        if current_run_id.is_empty() {
            return self.execute_transaction(
                now,
                NdcTransactionPolicy::CreateAsCurrent,
                None,
                target_workflow,
            );
        }

        let current_workflow = match self
            .transaction_manager
            .load_ndc_workflow(&domain_id, &workflow_id, &current_run_id)
        {
            Ok(w) => w,
            Err(e) => {
                target_workflow.release(Some(&e));
                return Err(e);
            }
        };

        let target_is_newer = match target_workflow.happens_after(current_workflow.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                target_workflow.release(Some(&e));
                let mut current_workflow = current_workflow;
                current_workflow.release(Some(&e));
                return Err(e);
            }
        };
        if !target_is_newer {
            return self.execute_transaction(
                now,
                NdcTransactionPolicy::CreateAsZombie,
                Some(current_workflow),
                target_workflow,
            );
        }
        if !current_workflow.mutable_state().is_workflow_execution_running() {
            return self.execute_transaction(
                now,
                NdcTransactionPolicy::CreateAsCurrent,
                Some(current_workflow),
                target_workflow,
            );
        }
        self.execute_transaction(
            now,
            NdcTransactionPolicy::SuppressCurrentAndCreateAsCurrent,
            Some(current_workflow),
            target_workflow,
        )
    }

    fn persist_first_batch(
        target: &mut dyn NdcWorkflow,
        events_seq: &[crate::execution::context::WorkflowEvents],
    ) -> NdcResult<PersistedBlob> {
        let mut blob = PersistedBlob::default();
        if let Some(first) = events_seq.first() {
            if !first.events.is_empty() {
                if first.events[0].event_type == Some(EventType::WorkflowExecutionStarted) {
                    blob = target.persist_start_workflow_batch_events(first)?;
                } else {
                    // reset workflows fall into else branch
                    blob = target.persist_non_start_workflow_batch_events(first)?;
                }
            }
        }
        Ok(blob)
    }

    fn create_as_current(
        &self,
        now: i64,
        current_workflow: Option<&dyn NdcWorkflow>,
        target: &mut dyn NdcWorkflow,
    ) -> NdcResult<()> {
        let (snapshot, events_seq) = target
            .mutable_state_mut()
            .close_transaction_as_snapshot(now, TransactionPolicy::Passive)?;
        let blob = Self::persist_first_batch(target, &events_seq)?;

        if let Some(current) = current_workflow {
            let prev_run_id = current.mutable_state().get_execution_info().run_id;
            let prev_vector_clock = current.get_vector_clock()?;
            target.create_workflow_execution(
                snapshot,
                blob,
                CreateWorkflowMode::WorkflowIdReuse,
                &prev_run_id,
                prev_vector_clock.last_write_version,
                WorkflowRequestMode::Replicated,
            )?;
            target.mutable_state_mut().transaction_committed();
            return Ok(());
        }
        target.create_workflow_execution(
            snapshot,
            blob,
            CreateWorkflowMode::BrandNew,
            "",
            0,
            WorkflowRequestMode::Replicated,
        )?;
        target.mutable_state_mut().transaction_committed();
        Ok(())
    }

    fn create_as_zombie(
        &self,
        now: i64,
        current_workflow: &mut Option<Box<dyn NdcWorkflow>>,
        target: &mut dyn NdcWorkflow,
    ) -> NdcResult<()> {
        let policy = {
            let current = current_workflow.as_ref().expect("current workflow required");
            target.suppress_by(current.as_ref())?
        };
        if policy != TransactionPolicy::Passive {
            return Err(NdcError::internal(
                "nDCTransactionManagerForNewWorkflow createAsZombie encounter target workflow policy not being passive",
            ));
        }
        let (snapshot, events_seq) = target
            .mutable_state_mut()
            .close_transaction_as_snapshot(now, policy)?;
        let blob = Self::persist_first_batch(target, &events_seq)?;

        if let Some(mut current) = current_workflow.take() {
            current.release(None);
        }

        target.reapply_events(&events_seq)?;

        match target.create_workflow_execution(
            snapshot,
            blob,
            CreateWorkflowMode::Zombie,
            "",
            0,
            WorkflowRequestMode::Replicated,
        ) {
            Ok(()) => { target.mutable_state_mut().transaction_committed(); Ok(()) },
            Err(NdcError::WorkflowExecutionAlreadyStarted(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn suppress_current_and_create_as_current(
        &self,
        now: i64,
        current: &mut dyn NdcWorkflow,
        target: &mut dyn NdcWorkflow,
    ) -> NdcResult<()> {
        let current_policy = current.suppress_by(&*target)?;
        target.revive()?;
        current.update_workflow_execution_with_new(
            now,
            UpdateWorkflowMode::UpdateCurrent,
            Some(target),
            current_policy,
            Some(TransactionPolicy::Passive),
            WorkflowRequestMode::Replicated,
        )
    }

    fn execute_transaction(
        &self,
        now: i64,
        policy: NdcTransactionPolicy,
        mut current_workflow: Option<Box<dyn NdcWorkflow>>,
        mut target_workflow: Box<dyn NdcWorkflow>,
    ) -> NdcResult<()> {
        let outcome = guarded(|| match policy {
            NdcTransactionPolicy::CreateAsCurrent => self.create_as_current(
                now,
                current_workflow.as_deref(),
                target_workflow.as_mut(),
            ),
            NdcTransactionPolicy::CreateAsZombie => {
                self.create_as_zombie(now, &mut current_workflow, target_workflow.as_mut())
            }
            NdcTransactionPolicy::SuppressCurrentAndCreateAsCurrent => self
                .suppress_current_and_create_as_current(
                    now,
                    current_workflow.as_deref_mut().expect("current workflow required"),
                    target_workflow.as_mut(),
                ),
            other => Err(NdcError::internal(format!(
                "nDCTransactionManager: encounter unknown transaction type: {other:?}"
            ))),
        });
        finish_guarded(outcome, |err| {
            // Go cleanupTransaction
            if let Some(c) = current_workflow.as_mut() {
                c.release(err);
            }
            target_workflow.release(err);
        })
    }
}
