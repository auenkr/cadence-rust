//! Port of Go `ndc/workflow_resetter.go`.
use cadence_api::types::VersionHistoryItem;
use cadence_persistence::version_history::{new_item, VersionHistoriesExt};

use super::conflict_resolver::NdcStateRebuilder;
use super::transaction_manager::NdcTransactionManager;
use super::{new_ndc_retry_task_error_with_hint, NdcMutableState, NdcResult, NdcWorkflow};

pub const RESEND_ON_RESET_WORKFLOW_MESSAGE: &str = "Resend events due to reset workflow";

/// Go `WorkflowResetter`.
pub trait NdcWorkflowResetter {
    fn reset_workflow(
        &mut self,
        now_nanos: i64,
        base_last_event_id: i64,
        base_last_event_version: i64,
        incoming_first_event_id: i64,
        incoming_first_event_version: i64,
    ) -> NdcResult<Box<dyn NdcMutableState>>;
}

/// Go `persistence.HistoryManager.ForkHistoryBranch` + domain cache for the resetter.
pub trait WorkflowResetterEnv {
    fn get_domain_name(&self, domain_id: &str) -> NdcResult<String>;
    fn fork_history_branch(
        &self,
        fork_branch_token: &[u8],
        fork_node_id: i64,
        info: &str,
        domain_name: &str,
    ) -> NdcResult<Vec<u8>>;
}

/// Go `workflowResetterImpl`.
pub struct WorkflowResetterImpl<'a> {
    pub env: &'a dyn WorkflowResetterEnv,
    pub transaction_manager: &'a dyn NdcTransactionManager,
    pub state_rebuilder: &'a dyn NdcStateRebuilder,
    pub domain_id: String,
    pub workflow_id: String,
    pub base_run_id: String,
    /// The new run's workflow/context (Go `newContext`).
    pub new_workflow: &'a mut dyn NdcWorkflow,
    pub new_run_id: String,
}

impl<'a> WorkflowResetterImpl<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        env: &'a dyn WorkflowResetterEnv,
        transaction_manager: &'a dyn NdcTransactionManager,
        state_rebuilder: &'a dyn NdcStateRebuilder,
        domain_id: &str,
        workflow_id: &str,
        base_run_id: &str,
        new_workflow: &'a mut dyn NdcWorkflow,
        new_run_id: &str,
    ) -> Self {
        Self {
            env,
            transaction_manager,
            state_rebuilder,
            domain_id: domain_id.to_string(),
            workflow_id: workflow_id.to_string(),
            base_run_id: base_run_id.to_string(),
            new_workflow,
            new_run_id: new_run_id.to_string(),
        }
    }

    /// Go `ResetWorkflow`. (Takes `&mut self` because it clears the new context.)
    fn reset_workflow_impl(
        &mut self,
        now_nanos: i64,
        base_last_event_id: i64,
        base_last_event_version: i64,
        incoming_first_event_id: i64,
        incoming_first_event_version: i64,
    ) -> NdcResult<Box<dyn NdcMutableState>> {
        let base_branch_token = self.get_base_branch_token(
            base_last_event_id,
            base_last_event_version,
            incoming_first_event_id,
            incoming_first_event_version,
        )?;
        let env = self.env;
        let domain_id = self.domain_id.clone();
        let workflow_id = self.workflow_id.clone();
        let new_run_id = self.new_run_id.clone();
        let base_token_for_fn = base_branch_token.clone();
        let reset_branch_token_fn = move || -> NdcResult<Vec<u8>> {
            get_reset_branch_token(
                env,
                &domain_id,
                &workflow_id,
                &new_run_id,
                &base_token_for_fn,
                base_last_event_id,
            )
        };
        let request_id = uuid::Uuid::new_v4().to_string();
        let (rebuild_ms, rebuilt_history_size) = self.state_rebuilder.rebuild(
            now_nanos,
            &self.domain_id,
            &self.workflow_id,
            &self.base_run_id,
            &base_branch_token,
            base_last_event_id,
            base_last_event_version,
            &self.new_run_id,
            &reset_branch_token_fn,
            &request_id,
        )?;
        self.new_workflow.clear();
        self.new_workflow.set_history_size(rebuilt_history_size);
        Ok(rebuild_ms)
    }

    fn get_base_branch_token(
        &self,
        base_last_event_id: i64,
        base_last_event_version: i64,
        incoming_first_event_id: i64,
        incoming_first_event_version: i64,
    ) -> NdcResult<Vec<u8>> {
        let mut base_workflow = self.transaction_manager.load_ndc_workflow(
            &self.domain_id,
            &self.workflow_id,
            &self.base_run_id,
        )?;
        let result = (|| -> NdcResult<Vec<u8>> {
            let ms = base_workflow.mutable_state();
            let mut branch_token = ms.get_current_branch_token()?;
            if let Some(histories) = ms.get_version_histories() {
                let item: VersionHistoryItem = new_item(base_last_event_id, base_last_event_version);
                match histories.find_first_by_item(&item) {
                    Ok((_, vh)) => branch_token = vh.branch_token.clone(),
                    Err(_) => {
                        return Err(new_ndc_retry_task_error_with_hint(
                            RESEND_ON_RESET_WORKFLOW_MESSAGE,
                            &self.domain_id,
                            &self.workflow_id,
                            &self.new_run_id,
                            None,
                            None,
                            Some(incoming_first_event_id),
                            Some(incoming_first_event_version),
                        ))
                    }
                }
            }
            Ok(branch_token)
        })();
        base_workflow.release(result.as_ref().err());
        result
    }
}

fn get_reset_branch_token(
    env: &dyn WorkflowResetterEnv,
    domain_id: &str,
    workflow_id: &str,
    new_run_id: &str,
    base_branch_token: &[u8],
    base_last_event_id: i64,
) -> NdcResult<Vec<u8>> {
    let domain_name = env.get_domain_name(domain_id)?;
    env.fork_history_branch(
        base_branch_token,
        base_last_event_id + 1,
        &format!("{domain_id}:{workflow_id}:{new_run_id}"),
        &domain_name,
    )
}

impl<'a> NdcWorkflowResetter for WorkflowResetterImpl<'a> {
    fn reset_workflow(
        &mut self,
        now_nanos: i64,
        base_last_event_id: i64,
        base_last_event_version: i64,
        incoming_first_event_id: i64,
        incoming_first_event_version: i64,
    ) -> NdcResult<Box<dyn NdcMutableState>> {
        self.reset_workflow_impl(
            now_nanos,
            base_last_event_id,
            base_last_event_version,
            incoming_first_event_id,
            incoming_first_event_version,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::WorkflowEvents;
    use crate::ndc::test_support::{Calls, MockMs, MockWorkflow, SharedCalls};
    use cadence_api::types::VersionHistories;
    use cadence_persistence::version_history::new_history;
    use std::cell::RefCell;
    use std::rc::Rc;

    const DOMAIN: &str = "domain-id";
    const WF: &str = "some random workflow ID";
    const BASE_RUN: &str = "base-run";
    const NEW_RUN: &str = "new-run";

    struct MockTm {
        base: RefCell<Option<Box<MockWorkflow>>>,
        loads: RefCell<Vec<(String, String, String)>>,
    }
    impl NdcTransactionManager for MockTm {
        fn create_workflow(&self, _n: i64, _t: Box<dyn NdcWorkflow>) -> NdcResult<()> {
            unimplemented!()
        }
        fn update_workflow(
            &self,
            _n: i64,
            _r: bool,
            _t: Box<dyn NdcWorkflow>,
            _nw: Option<Box<dyn NdcWorkflow>>,
        ) -> NdcResult<()> {
            unimplemented!()
        }
        fn backfill_workflow(&self, _n: i64, _t: Box<dyn NdcWorkflow>, _e: &WorkflowEvents) -> NdcResult<()> {
            unimplemented!()
        }
        fn check_workflow_exists(&self, _d: &str, _w: &str, _r: &str) -> NdcResult<bool> {
            unimplemented!()
        }
        fn get_current_workflow_run_id(&self, _d: &str, _w: &str) -> NdcResult<String> {
            unimplemented!()
        }
        fn load_ndc_workflow(&self, d: &str, w: &str, r: &str) -> NdcResult<Box<dyn NdcWorkflow>> {
            self.loads.borrow_mut().push((d.into(), w.into(), r.into()));
            Ok(self.base.borrow_mut().take().expect("loaded once"))
        }
    }

    #[derive(Default)]
    struct MockRebuilder {
        calls: RefCell<Vec<(i64, Vec<u8>, i64, i64, String, Vec<u8>)>>,
    }
    impl NdcStateRebuilder for MockRebuilder {
        fn rebuild(
            &self,
            now: i64,
            _d: &str,
            _w: &str,
            _base_run: &str,
            base_branch_token: &[u8],
            last_event_id: i64,
            last_event_version: i64,
            target_run_id: &str,
            target_branch_token: &dyn Fn() -> NdcResult<Vec<u8>>,
            _request_id: &str,
        ) -> NdcResult<(Box<dyn NdcMutableState>, i64)> {
            let target = target_branch_token()?;
            self.calls.borrow_mut().push((
                now,
                base_branch_token.to_vec(),
                last_event_id,
                last_event_version,
                target_run_id.to_string(),
                target,
            ));
            Ok((Box::new(MockMs::default()), 9999))
        }
    }

    struct MockEnv {
        forks: RefCell<Vec<(Vec<u8>, i64, String, String)>>,
    }
    impl WorkflowResetterEnv for MockEnv {
        fn get_domain_name(&self, _d: &str) -> NdcResult<String> {
            Ok("test-domainName".into())
        }
        fn fork_history_branch(&self, token: &[u8], node: i64, info: &str, name: &str) -> NdcResult<Vec<u8>> {
            self.forks.borrow_mut().push((token.to_vec(), node, info.into(), name.into()));
            Ok(b"other random branch token".to_vec())
        }
    }

    fn base_workflow(calls: &SharedCalls, last_event_id: i64, version: i64) -> Box<MockWorkflow> {
        Box::new(MockWorkflow {
            ms: MockMs {
                branch_token: b"some random branch token".to_vec(),
                vh: Some(VersionHistories {
                    current_version_history_index: 0,
                    histories: vec![new_history(
                        b"some random branch token".to_vec(),
                        &[new_item(last_event_id, version)],
                    )],
                }),
                ..Default::default()
            },
            calls: calls.clone(),
        })
    }

    /// Go `TestResetWorkflow_NoError`.
    #[test]
    fn reset_workflow_no_error() {
        let base_calls: SharedCalls = Rc::new(RefCell::new(Calls::default()));
        let new_calls: SharedCalls = Rc::new(RefCell::new(Calls::default()));
        let (last_event_id, version) = (500, 123);
        let base_event_id = last_event_id - 100;
        let tm = MockTm {
            base: RefCell::new(Some(base_workflow(&base_calls, last_event_id, version))),
            loads: RefCell::new(Vec::new()),
        };
        let rebuilder = MockRebuilder::default();
        let env = MockEnv { forks: RefCell::new(Vec::new()) };
        let mut new_wf = MockWorkflow { ms: MockMs::default(), calls: new_calls.clone() };
        let mut resetter =
            WorkflowResetterImpl::new(&env, &tm, &rebuilder, DOMAIN, WF, BASE_RUN, &mut new_wf, NEW_RUN);
        let rebuilt = resetter
            .reset_workflow(42, base_event_id, version, base_event_id + 12, version + 3)
            .unwrap();
        drop(rebuilt);
        assert_eq!(*tm.loads.borrow(), vec![(DOMAIN.to_string(), WF.to_string(), BASE_RUN.to_string())]);
        assert_eq!(
            *rebuilder.calls.borrow(),
            vec![(
                42,
                b"some random branch token".to_vec(),
                base_event_id,
                version,
                NEW_RUN.to_string(),
                b"other random branch token".to_vec()
            )]
        );
        assert_eq!(
            *env.forks.borrow(),
            vec![(
                b"some random branch token".to_vec(),
                base_event_id + 1,
                format!("{DOMAIN}:{WF}:{NEW_RUN}"),
                "test-domainName".to_string()
            )]
        );
        assert_eq!(new_calls.borrow().history_size, Some(9999));
        assert_eq!(new_calls.borrow().cleared, 1);
        assert_eq!(base_calls.borrow().released, Some(None), "base release called");
    }

    /// Go `TestResetWorkflow_Error`: base event id is past the version history.
    #[test]
    fn reset_workflow_error() {
        let base_calls: SharedCalls = Rc::new(RefCell::new(Calls::default()));
        let new_calls: SharedCalls = Rc::new(RefCell::new(Calls::default()));
        let (last_event_id, version) = (500, 123);
        let base_event_id = last_event_id + 100;
        let tm = MockTm {
            base: RefCell::new(Some(base_workflow(&base_calls, last_event_id, version))),
            loads: RefCell::new(Vec::new()),
        };
        let rebuilder = MockRebuilder::default();
        let env = MockEnv { forks: RefCell::new(Vec::new()) };
        let mut new_wf = MockWorkflow { ms: MockMs::default(), calls: new_calls };
        let mut resetter =
            WorkflowResetterImpl::new(&env, &tm, &rebuilder, DOMAIN, WF, BASE_RUN, &mut new_wf, NEW_RUN);
        let incoming_first = base_event_id + 12;
        let incoming_version = version + 3;
        let res = resetter.reset_workflow(0, base_event_id, version, incoming_first, incoming_version);
        let Err(err) = res else { panic!("expected error") };
        assert_eq!(
            err,
            new_ndc_retry_task_error_with_hint(
                RESEND_ON_RESET_WORKFLOW_MESSAGE,
                DOMAIN,
                WF,
                NEW_RUN,
                None,
                None,
                Some(incoming_first),
                Some(incoming_version),
            )
        );
        assert!(rebuilder.calls.borrow().is_empty());
        assert!(base_calls.borrow().released.is_some());
    }
}
