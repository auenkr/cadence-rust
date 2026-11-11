//! Port of Go `ndc/existing_workflow_transaction_manager.go`.
use cadence_persistence::history_contract::{
    ConflictResolveWorkflowMode, UpdateWorkflowMode, WorkflowRequestMode,
};

use super::transaction_manager::{NdcTransactionManager, NdcTransactionPolicy};
use super::{finish_guarded, guarded, NdcError, NdcResult, NdcWorkflow};
use crate::execution::workflow::TransactionPolicy;

type OptWorkflow = Option<Box<dyn NdcWorkflow>>;

/// Reborrow with a shortened trait-object lifetime (plain `as_deref_mut` keeps `'static`,
/// which is invariant behind `&mut`).
fn opt_mut(w: &mut OptWorkflow) -> Option<&mut dyn NdcWorkflow> {
    match w {
        Some(b) => Some(b.as_mut()),
        None => None,
    }
}

/// Go `transactionManagerForExistingWorkflowImpl`.
pub struct TransactionManagerForExistingWorkflow<'a> {
    pub transaction_manager: &'a dyn NdcTransactionManager,
}

impl<'a> TransactionManagerForExistingWorkflow<'a> {
    pub fn new(transaction_manager: &'a dyn NdcTransactionManager) -> Self {
        Self { transaction_manager }
    }

    /// Go `dispatchForExistingWorkflow`. Takes ownership of the workflows and releases them.
    pub fn dispatch_for_existing_workflow(
        &self,
        now: i64,
        is_workflow_rebuilt: bool,
        mut target_workflow: Box<dyn NdcWorkflow>,
        mut new_workflow: OptWorkflow,
    ) -> NdcResult<()> {
        if !is_workflow_rebuilt && target_workflow.mutable_state().is_current_workflow_guaranteed() {
            return self.dispatch_workflow_update_as_current(now, is_workflow_rebuilt, target_workflow, new_workflow);
        }

        let info = target_workflow.mutable_state().get_execution_info();
        let (domain_id, workflow_id, target_run_id) = (info.domain_id, info.workflow_id, info.run_id);

        // On early failure Go leaves releasing to the caller's deferred release; here
        // ownership moved in, so release all held workflows.
        let fail = |tw: &mut Box<dyn NdcWorkflow>, nw: &mut OptWorkflow, e: NdcError| -> NdcResult<()> {
            tw.release(Some(&e));
            if let Some(n) = nw.as_mut() {
                n.release(Some(&e));
            }
            Err(e)
        };

        let current_run_id = match self.transaction_manager.get_current_workflow_run_id(&domain_id, &workflow_id) {
            Ok(r) => r,
            Err(e) => return fail(&mut target_workflow, &mut new_workflow, e),
        };
        if current_run_id.is_empty() {
            return fail(
                &mut target_workflow,
                &mut new_workflow,
                NdcError::internal("nDCTransactionManager: unable to locate current workflow during update"),
            );
        }
        if current_run_id == target_run_id {
            if !is_workflow_rebuilt {
                return fail(
                    &mut target_workflow,
                    &mut new_workflow,
                    NdcError::internal(
                        "nDCTransactionManager: encounter workflow not rebuilt & current workflow not guaranteed",
                    ),
                );
            }
            return self.dispatch_workflow_update_as_current(now, is_workflow_rebuilt, target_workflow, new_workflow);
        }

        let current_workflow = match self
            .transaction_manager
            .load_ndc_workflow(&domain_id, &workflow_id, &current_run_id)
        {
            Ok(w) => w,
            Err(e) => return fail(&mut target_workflow, &mut new_workflow, e),
        };
        let target_is_newer = match target_workflow.happens_after(current_workflow.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                let mut c = current_workflow;
                c.release(Some(&e));
                return fail(&mut target_workflow, &mut new_workflow, e);
            }
        };
        if !target_is_newer {
            return self.dispatch_workflow_update_as_zombie(
                now,
                is_workflow_rebuilt,
                current_workflow,
                target_workflow,
                new_workflow,
            );
        }
        self.execute_transaction(
            now,
            NdcTransactionPolicy::SuppressCurrentAndUpdateAsCurrent,
            Some(current_workflow),
            target_workflow,
            new_workflow,
        )
    }

    fn dispatch_workflow_update_as_current(
        &self,
        now: i64,
        is_workflow_rebuilt: bool,
        target_workflow: Box<dyn NdcWorkflow>,
        new_workflow: OptWorkflow,
    ) -> NdcResult<()> {
        let policy = if !is_workflow_rebuilt {
            NdcTransactionPolicy::UpdateAsCurrent
        } else {
            NdcTransactionPolicy::ConflictResolveAsCurrent
        };
        self.execute_transaction(now, policy, None, target_workflow, new_workflow)
    }

    fn dispatch_workflow_update_as_zombie(
        &self,
        now: i64,
        is_workflow_rebuilt: bool,
        current_workflow: Box<dyn NdcWorkflow>,
        target_workflow: Box<dyn NdcWorkflow>,
        new_workflow: OptWorkflow,
    ) -> NdcResult<()> {
        let policy = if !is_workflow_rebuilt {
            NdcTransactionPolicy::UpdateAsZombie
        } else {
            NdcTransactionPolicy::ConflictResolveAsZombie
        };
        self.execute_transaction(now, policy, Some(current_workflow), target_workflow, new_workflow)
    }

    fn update_as_current(
        &self,
        now: i64,
        target: &mut dyn NdcWorkflow,
        new_workflow: Option<&mut dyn NdcWorkflow>,
    ) -> NdcResult<()> {
        match new_workflow {
            None => target.update_workflow_execution_as_passive(now),
            Some(n) => target.update_workflow_execution_with_new_as_passive(now, n),
        }
    }

    fn update_as_zombie(
        &self,
        now: i64,
        current_workflow: &mut OptWorkflow,
        target: &mut dyn NdcWorkflow,
        new_workflow: Option<&mut dyn NdcWorkflow>,
    ) -> NdcResult<()> {
        let target_policy = target.suppress_by(current_workflow.as_ref().expect("current required").as_ref())?;
        if target_policy != TransactionPolicy::Passive {
            return Err(NdcError::internal(
                "nDCTransactionManagerForExistingWorkflow updateAsZombie encounter target workflow policy not being passive",
            ));
        }

        let mut new_ref: Option<&mut dyn NdcWorkflow> = None;
        let mut new_policy: Option<TransactionPolicy> = None;
        if let Some(n) = new_workflow {
            let new_policy_result = n.suppress_by(current_workflow.as_ref().expect("current required").as_ref())?;
            if new_policy_result != TransactionPolicy::Passive {
                return Err(NdcError::internal(
                    "nDCTransactionManagerForExistingWorkflow updateAsZombie encounter new workflow policy not being passive",
                ));
            }
            let info = n.mutable_state().get_execution_info();
            let exists = self
                .transaction_manager
                .check_workflow_exists(&info.domain_id, &info.workflow_id, &info.run_id)?;
            if !exists {
                new_ref = Some(n);
                new_policy = Some(TransactionPolicy::Passive);
            }
        }

        if let Some(mut c) = current_workflow.take() {
            c.release(None);
        }

        target.update_workflow_execution_with_new(
            now,
            UpdateWorkflowMode::BypassCurrent,
            new_ref,
            TransactionPolicy::Passive,
            new_policy,
            WorkflowRequestMode::Replicated,
        )
    }

    fn suppress_current_and_update_as_current(
        &self,
        now: i64,
        current: &mut dyn NdcWorkflow,
        target: &mut dyn NdcWorkflow,
        mut new_workflow: Option<&mut dyn NdcWorkflow>,
    ) -> NdcResult<()> {
        let mut current_policy = TransactionPolicy::Passive;
        if current.mutable_state().is_workflow_execution_running() {
            current_policy = current.suppress_by(&*target)?;
        }
        target.revive()?;
        if let Some(n) = new_workflow.as_mut() {
            n.revive()?;
        }
        target.conflict_resolve_workflow_execution(
            now,
            ConflictResolveWorkflowMode::UpdateCurrent,
            new_workflow,
            Some(current),
            Some(current_policy),
        )
    }

    fn conflict_resolve_as_current(
        &self,
        now: i64,
        target: &mut dyn NdcWorkflow,
        new_workflow: Option<&mut dyn NdcWorkflow>,
    ) -> NdcResult<()> {
        target.conflict_resolve_workflow_execution(
            now,
            ConflictResolveWorkflowMode::UpdateCurrent,
            new_workflow,
            None,
            None,
        )
    }

    fn conflict_resolve_as_zombie(
        &self,
        now: i64,
        current_workflow: &mut OptWorkflow,
        target: &mut dyn NdcWorkflow,
        new_workflow: Option<&mut dyn NdcWorkflow>,
    ) -> NdcResult<()> {
        let target_policy = target.suppress_by(current_workflow.as_ref().expect("current required").as_ref())?;
        if target_policy != TransactionPolicy::Passive {
            return Err(NdcError::internal(
                "nDCTransactionManagerForExistingWorkflow conflictResolveAsZombie encounter target workflow policy not being passive",
            ));
        }

        let mut new_ref: Option<&mut dyn NdcWorkflow> = None;
        if let Some(n) = new_workflow {
            let new_policy = n.suppress_by(current_workflow.as_ref().expect("current required").as_ref())?;
            if new_policy != TransactionPolicy::Passive {
                return Err(NdcError::internal(
                    "nDCTransactionManagerForExistingWorkflow conflictResolveAsZombie encounter new workflow policy not being passive",
                ));
            }
            let info = n.mutable_state().get_execution_info();
            let exists = self
                .transaction_manager
                .check_workflow_exists(&info.domain_id, &info.workflow_id, &info.run_id)?;
            if !exists {
                new_ref = Some(n);
            }
        }

        if let Some(mut c) = current_workflow.take() {
            c.release(None);
        }

        target.conflict_resolve_workflow_execution(
            now,
            ConflictResolveWorkflowMode::BypassCurrent,
            new_ref,
            None,
            None,
        )
    }

    fn execute_transaction(
        &self,
        now: i64,
        policy: NdcTransactionPolicy,
        mut current_workflow: OptWorkflow,
        mut target_workflow: Box<dyn NdcWorkflow>,
        mut new_workflow: OptWorkflow,
    ) -> NdcResult<()> {
        let outcome = guarded(|| match policy {
            NdcTransactionPolicy::UpdateAsCurrent => {
                self.update_as_current(now, target_workflow.as_mut(), opt_mut(&mut new_workflow))
            }
            NdcTransactionPolicy::UpdateAsZombie => self.update_as_zombie(
                now,
                &mut current_workflow,
                target_workflow.as_mut(),
                opt_mut(&mut new_workflow),
            ),
            NdcTransactionPolicy::SuppressCurrentAndUpdateAsCurrent => self
                .suppress_current_and_update_as_current(
                    now,
                    opt_mut(&mut current_workflow).expect("current required"),
                    target_workflow.as_mut(),
                    opt_mut(&mut new_workflow),
                ),
            NdcTransactionPolicy::ConflictResolveAsCurrent => {
                self.conflict_resolve_as_current(now, target_workflow.as_mut(), opt_mut(&mut new_workflow))
            }
            NdcTransactionPolicy::ConflictResolveAsZombie => self.conflict_resolve_as_zombie(
                now,
                &mut current_workflow,
                target_workflow.as_mut(),
                opt_mut(&mut new_workflow),
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
            if let Some(n) = new_workflow.as_mut() {
                n.release(err);
            }
        })
    }
}

/// Port of Go `existing_workflow_transaction_manager_test.go`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ndc::test_support::{new_log, Log, LogWorkflow, MockTm};

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    struct Fx {
        log: Log,
        tm: MockTm,
        target: Box<LogWorkflow>,
        new: Box<LogWorkflow>,
        current: Box<LogWorkflow>,
    }

    fn fx(current_run: &str) -> Fx {
        let log = new_log();
        let tm = MockTm::new(&log, current_run);
        Fx {
            tm,
            target: Box::new(LogWorkflow::new("target", &log, "target-run")),
            new: Box::new(LogWorkflow::new("new", &log, "new-run")),
            current: Box::new(LogWorkflow::new("current", &log, "current-run")),
            log,
        }
    }

    fn run(f: Fx, rebuilt: bool, with_new: bool, load_current: bool) -> (NdcResult<()>, Vec<String>) {
        if load_current {
            *f.tm.loaded.borrow_mut() = Some(f.current);
        }
        let mgr = TransactionManagerForExistingWorkflow::new(&f.tm);
        let new: OptWorkflow = if with_new { Some(f.new) } else { None };
        let r = mgr.dispatch_for_existing_workflow(1, rebuilt, f.target, new);
        let l = f.log.borrow().clone();
        (r, l)
    }

    #[test]
    fn no_rebuild_current_workflow_guaranteed() {
        let mut f = fx("");
        f.target.ms.guaranteed = true;
        let (r, l) = run(f, false, true, false);
        r.unwrap();
        assert_eq!(
            l,
            s(&[
                "target:update_with_new_as_passive(new=new-run)",
                "target:release(ok)",
                "new:release(ok)",
            ])
        );
    }

    #[test]
    fn no_rebuild_not_guaranteed_is_current() {
        let f = fx("target-run");
        let (r, l) = run(f, false, true, false);
        assert!(r.is_err());
        assert_eq!(l[0], "tm:get_current_workflow_run_id");
    }

    #[test]
    fn no_rebuild_not_current_current_running_update_as_current() {
        let mut f = fx("current-run");
        f.target.happens_after_result = true;
        f.current.ms.running = true;
        let (r, l) = run(f, false, true, true);
        r.unwrap();
        assert_eq!(
            l,
            s(&[
                "tm:get_current_workflow_run_id",
                "tm:load_ndc_workflow(current-run)",
                "target:happens_after(current-run)",
                "current:suppress_by(target-run)",
                "target:revive",
                "new:revive",
                "target:conflict_resolve(UpdateCurrent,new=new-run,current=current-run,Some(Passive))",
                "current:release(ok)",
                "target:release(ok)",
                "new:release(ok)",
            ])
        );
    }

    #[test]
    fn no_rebuild_not_current_current_complete_update_as_current() {
        let mut f = fx("current-run");
        f.target.happens_after_result = true;
        f.current.ms.running = false;
        let (r, l) = run(f, false, true, true);
        r.unwrap();
        // current is not running: SuppressBy is never called (Go Times(0))
        assert!(!l.iter().any(|e| e.starts_with("current:suppress_by")));
        assert!(l.contains(&"target:conflict_resolve(UpdateCurrent,new=new-run,current=current-run,Some(Passive))".to_string()));
        assert!(l.contains(&"current:release(ok)".to_string()));
        assert!(l.contains(&"target:release(ok)".to_string()));
        assert!(l.contains(&"new:release(ok)".to_string()));
    }

    #[test]
    fn no_rebuild_not_current_update_as_zombie_new_run_does_not_exist() {
        let f = fx("current-run");
        let (r, l) = run(f, false, true, true);
        r.unwrap();
        assert_eq!(
            l,
            s(&[
                "tm:get_current_workflow_run_id",
                "tm:load_ndc_workflow(current-run)",
                "target:happens_after(current-run)",
                "target:suppress_by(current-run)",
                "new:suppress_by(current-run)",
                "tm:check_workflow_exists(new-run)",
                "current:release(ok)",
                "target:update_with_new(BypassCurrent,new=new-run,Passive,Some(Passive),Replicated)",
                "target:release(ok)",
                "new:release(ok)",
            ])
        );
    }

    #[test]
    fn no_rebuild_not_current_update_as_zombie_new_run_exists() {
        let mut f = fx("current-run");
        f.tm.exists = true;
        let (r, l) = run(f, false, true, true);
        r.unwrap();
        assert!(l.contains(&"target:update_with_new(BypassCurrent,new=none,Passive,None,Replicated)".to_string()));
        assert!(l.contains(&"current:release(ok)".to_string()));
        assert!(l.contains(&"target:release(ok)".to_string()));
        assert!(l.contains(&"new:release(ok)".to_string()));
    }

    #[test]
    fn rebuild_is_current() {
        let f = fx("target-run");
        let (r, l) = run(f, true, true, false);
        r.unwrap();
        assert_eq!(
            l,
            s(&[
                "tm:get_current_workflow_run_id",
                "target:conflict_resolve(UpdateCurrent,new=new-run,current=none,None)",
                "target:release(ok)",
                "new:release(ok)",
            ])
        );
    }

    #[test]
    fn rebuild_not_current_update_as_current() {
        let mut f = fx("current-run");
        f.target.happens_after_result = true;
        f.current.ms.running = true;
        f.current.suppress_policy = TransactionPolicy::Active;
        let (r, l) = run(f, true, true, true);
        r.unwrap();
        assert!(l.contains(&"current:suppress_by(target-run)".to_string()));
        assert!(l.contains(&"target:revive".to_string()));
        assert!(l.contains(&"new:revive".to_string()));
        assert!(l.contains(
            &"target:conflict_resolve(UpdateCurrent,new=new-run,current=current-run,Some(Active))".to_string()
        ));
        assert!(l.contains(&"current:release(ok)".to_string()));
        assert!(l.contains(&"target:release(ok)".to_string()));
        assert!(l.contains(&"new:release(ok)".to_string()));
    }

    #[test]
    fn rebuild_not_current_update_as_zombie_new_run_does_not_exist() {
        let f = fx("current-run");
        let (r, l) = run(f, true, true, true);
        r.unwrap();
        assert!(l.contains(&"target:conflict_resolve(BypassCurrent,new=new-run,current=none,None)".to_string()));
        assert!(l.contains(&"current:release(ok)".to_string()));
        assert!(l.contains(&"target:release(ok)".to_string()));
        assert!(l.contains(&"new:release(ok)".to_string()));
    }

    #[test]
    fn rebuild_not_current_update_as_zombie_new_run_exists() {
        let mut f = fx("current-run");
        f.tm.exists = true;
        let (r, l) = run(f, true, true, true);
        r.unwrap();
        assert!(l.contains(&"target:conflict_resolve(BypassCurrent,new=none,current=none,None)".to_string()));
        assert!(l.contains(&"current:release(ok)".to_string()));
        assert!(l.contains(&"target:release(ok)".to_string()));
        assert!(l.contains(&"new:release(ok)".to_string()));
    }
}
