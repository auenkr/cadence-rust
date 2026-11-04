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
