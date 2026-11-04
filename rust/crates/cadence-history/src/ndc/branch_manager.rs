//! Port of Go `ndc/branch_manager.go`.
use cadence_api::types::{VersionHistory, VersionHistoryItem};
use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};

use super::{new_ndc_retry_task_error_with_hint, NdcError, NdcResult, NdcWorkflow};

pub const OUT_OF_ORDER_DELIVERY_MESSAGE: &str = "Resend events due to out of order delivery";

/// Go `persistence.HistoryManager.ForkHistoryBranch` + domain cache name lookup + clock.
pub trait BranchManagerEnv {
    fn get_domain_name(&self, domain_id: &str) -> NdcResult<String>;
    /// Returns the new branch token. `info` is `BuildHistoryGarbageCleanupInfo(domainID, workflowID, uuid)`.
    fn fork_history_branch(
        &self,
        fork_branch_token: &[u8],
        fork_node_id: i64,
        info: &str,
        domain_name: &str,
    ) -> NdcResult<Vec<u8>>;
    fn now_nanos(&self) -> i64;
}

/// Go `branchManagerImpl`.
pub struct BranchManager<'a> {
    pub env: &'a dyn BranchManagerEnv,
}

impl<'a> BranchManager<'a> {
    pub fn new(env: &'a dyn BranchManagerEnv) -> Self {
        Self { env }
    }

    /// Returns `(do_continue, version_history_index)`.
    pub fn prepare_version_history(
        &self,
        workflow: &mut dyn NdcWorkflow,
        incoming_version_history: &VersionHistory,
        incoming_first_event_id: i64,
        incoming_first_event_version: i64,
    ) -> NdcResult<(bool, i32)> {
        let (version_history_index, lca_item) =
            self.flush_buffered_events(workflow, incoming_version_history)?;

        let local = workflow
            .mutable_state()
            .get_version_histories()
            .ok_or_else(NdcError::missing_version_histories)?;
        let version_history = local.history(version_history_index)?.clone();

        if version_history.is_lca_appendable(&lca_item) {
            let do_continue = self.verify_events_order(
                workflow,
                &version_history,
                incoming_first_event_id,
                incoming_first_event_version,
            )?;
            return Ok((do_continue, version_history_index));
        }

        let mut new_version_history = version_history.duplicate_until_lca(&lca_item)?;
        let do_continue = self.verify_events_order(
            workflow,
            &new_version_history,
            incoming_first_event_id,
            incoming_first_event_version,
        )?;
        if !do_continue {
            return Ok((false, 0));
        }
        let new_index = self.create_new_branch(
            workflow,
            &version_history.branch_token,
            lca_item.event_id,
            &mut new_version_history,
        )?;
        Ok((true, new_index))
    }

    fn flush_buffered_events(
        &self,
        workflow: &mut dyn NdcWorkflow,
        incoming: &VersionHistory,
    ) -> NdcResult<(i32, VersionHistoryItem)> {
        let find = |w: &dyn NdcWorkflow| -> NdcResult<(i32, VersionHistoryItem)> {
            let local = w
                .mutable_state()
                .get_version_histories()
                .ok_or_else(NdcError::missing_version_histories)?;
            let (idx, item) = local.find_lca_history(incoming)?;
            let item = item.ok_or_else(|| NdcError::bad_request("version history LCA not found"))?;
            Ok((idx as i32, item))
        };
        let result = find(workflow)?;
        if !workflow.mutable_state().has_buffered_events() {
            return Ok(result);
        }
        workflow.flush_buffered_events()?;
        let now = self.env.now_nanos();
        workflow.update_workflow_execution_as_active(now)?;
        find(workflow)
    }

    fn verify_events_order(
        &self,
        workflow: &dyn NdcWorkflow,
        local: &VersionHistory,
        incoming_first_event_id: i64,
        incoming_first_event_version: i64,
    ) -> NdcResult<bool> {
        let last = local.last_item()?;
        let next_event_id = last.event_id + 1;
        if incoming_first_event_id < next_event_id {
            return Ok(false);
        }
        if incoming_first_event_id > next_event_id {
            let info = workflow.mutable_state().get_execution_info();
            return Err(new_ndc_retry_task_error_with_hint(
                OUT_OF_ORDER_DELIVERY_MESSAGE,
                &info.domain_id,
                &info.workflow_id,
                &info.run_id,
                Some(last.event_id),
                Some(last.version),
                Some(incoming_first_event_id),
                Some(incoming_first_event_version),
            ));
        }
        Ok(true)
    }

    fn create_new_branch(
        &self,
        workflow: &mut dyn NdcWorkflow,
        base_branch_token: &[u8],
        base_branch_last_event_id: i64,
        new_version_history: &mut VersionHistory,
    ) -> NdcResult<i32> {
        let info = workflow.mutable_state().get_execution_info();
        let domain_name = self.env.get_domain_name(&info.domain_id)?;
        let cleanup_info = format!(
            "{}:{}:{}",
            info.domain_id,
            info.workflow_id,
            uuid::Uuid::new_v4()
        );
        let new_token = self.env.fork_history_branch(
            base_branch_token,
            base_branch_last_event_id + 1,
            &cleanup_info,
            &domain_name,
        )?;
        new_version_history.branch_token = new_token;
        let histories = workflow
            .mutable_state_mut()
            .get_version_histories_mut()
            .ok_or_else(NdcError::missing_version_histories)?;
        let (branch_changed, new_index) = histories.add_history(Some(new_version_history))?;
        if branch_changed {
            return Err(NdcError::bad_request(
                "nDCBranchMgr encounter branch change during conflict resolution",
            ));
        }
        Ok(new_index as i32)
    }
}
