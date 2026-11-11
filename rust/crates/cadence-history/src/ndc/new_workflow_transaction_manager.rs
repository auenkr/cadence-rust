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

/// Port of Go `new_workflow_transaction_manager_test.go`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ndc::test_support::{events_of, new_log, Log, LogWorkflow, MockTm};

    const CREATE_BLOB: &str = "blob=12345";

    fn log_of(log: &Log) -> Vec<String> {
        log.borrow().clone()
    }

    fn setup(
        current_run: &str,
        first_event: EventType,
    ) -> (Log, MockTm, Box<LogWorkflow>, Box<LogWorkflow>) {
        let log = new_log();
        let tm = MockTm::new(&log, current_run);
        let mut target = Box::new(LogWorkflow::new("target", &log, "target-run"));
        target.ms.events_seq = vec![events_of(first_event)];
        let current = Box::new(LogWorkflow::new("current", &log, "current-run"));
        (log, tm, target, current)
    }

    #[test]
    fn dispatch_for_new_workflow_dup() {
        let log = new_log();
        let tm = MockTm::new(&log, "target-run");
        let target = Box::new(LogWorkflow::new("target", &log, "target-run"));
        let mgr = TransactionManagerForNewWorkflow::new(&tm);
        mgr.dispatch_for_new_workflow(1, target).unwrap();
        assert_eq!(log_of(&log), vec!["tm:get_current_workflow_run_id", "target:release(ok)"]);
    }

    #[test]
    fn dispatch_for_new_workflow_brand_new() {
        let (log, tm, target, _) = setup("", EventType::WorkflowExecutionStarted);
        let mgr = TransactionManagerForNewWorkflow::new(&tm);
        mgr.dispatch_for_new_workflow(1, target).unwrap();
        assert_eq!(
            log_of(&log),
            vec![
                "tm:get_current_workflow_run_id".to_string(),
                "target:persist_start".to_string(),
                format!("target:create(BrandNew,{CREATE_BLOB},\"\",0,Replicated)"),
                "target:release(ok)".to_string(),
            ]
        );
    }

    #[test]
    fn dispatch_for_new_workflow_create_as_current() {
        let (log, tm, target, mut current) = setup("current-run", EventType::DecisionTaskScheduled);
        current.last_write_version = 4321;
        let mut target = target;
        target.happens_after_result = true;
        *tm.loaded.borrow_mut() = Some(current);
        let mgr = TransactionManagerForNewWorkflow::new(&tm);
        mgr.dispatch_for_new_workflow(1, target).unwrap();
        assert_eq!(
            log_of(&log),
            vec![
                "tm:get_current_workflow_run_id".to_string(),
                "tm:load_ndc_workflow(current-run)".to_string(),
                "target:happens_after(current-run)".to_string(),
                "target:persist_non_start".to_string(),
                format!("target:create(WorkflowIdReuse,{CREATE_BLOB},\"current-run\",4321,Replicated)"),
                "current:release(ok)".to_string(),
                "target:release(ok)".to_string(),
            ]
        );
    }

    #[test]
    fn dispatch_for_new_workflow_create_as_zombie() {
        let (log, tm, target, current) = setup("current-run", EventType::WorkflowExecutionStarted);
        *tm.loaded.borrow_mut() = Some(current);
        let mgr = TransactionManagerForNewWorkflow::new(&tm);
        mgr.dispatch_for_new_workflow(1, target).unwrap();
        assert_eq!(
            log_of(&log),
            vec![
                "tm:get_current_workflow_run_id".to_string(),
                "tm:load_ndc_workflow(current-run)".to_string(),
                "target:happens_after(current-run)".to_string(),
                "target:suppress_by(current-run)".to_string(),
                "target:persist_start".to_string(),
                "current:release(ok)".to_string(),
                "target:reapply".to_string(),
                format!("target:create(Zombie,{CREATE_BLOB},\"\",0,Replicated)"),
                "target:release(ok)".to_string(),
            ]
        );
    }

    #[test]
    fn dispatch_for_new_workflow_create_as_zombie_dedup() {
        let (log, tm, mut target, current) = setup("current-run", EventType::DecisionTaskScheduled);
        target.ms.fail_create_already_started = true;
        *tm.loaded.borrow_mut() = Some(current);
        let mgr = TransactionManagerForNewWorkflow::new(&tm);
        mgr.dispatch_for_new_workflow(1, target).unwrap();
        let l = log_of(&log);
        assert!(l.contains(&"target:persist_non_start".to_string()));
        assert!(l.contains(&"target:reapply".to_string()));
        assert!(l.contains(&format!("target:create(Zombie,{CREATE_BLOB},\"\",0,Replicated)")));
        assert!(l.contains(&"current:release(ok)".to_string()));
        assert_eq!(l.last().unwrap(), "target:release(ok)");
    }

    #[test]
    fn dispatch_for_new_workflow_suppress_current_and_create_as_current() {
        let (log, tm, mut target, mut current) = setup("current-run", EventType::WorkflowExecutionStarted);
        target.happens_after_result = true;
        current.ms.running = true;
        current.suppress_policy = TransactionPolicy::Active;
        *tm.loaded.borrow_mut() = Some(current);
        let mgr = TransactionManagerForNewWorkflow::new(&tm);
        mgr.dispatch_for_new_workflow(1, target).unwrap();
        assert_eq!(
            log_of(&log),
            vec![
                "tm:get_current_workflow_run_id".to_string(),
                "tm:load_ndc_workflow(current-run)".to_string(),
                "target:happens_after(current-run)".to_string(),
                "current:suppress_by(target-run)".to_string(),
                "target:revive".to_string(),
                "current:update_with_new(UpdateCurrent,new=target-run,Active,Some(Passive),Replicated)".to_string(),
                "current:release(ok)".to_string(),
                "target:release(ok)".to_string(),
            ]
        );
    }
}
