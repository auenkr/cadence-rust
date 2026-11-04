//! Port of Go `ndc/conflict_resolver.go`.
use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};

use super::{NdcError, NdcMutableState, NdcResult, NdcWorkflow};

/// Go `execution.StateRebuilder.Rebuild` (state rebuilder is in `execution::state_rebuilder`).
pub trait NdcStateRebuilder {
    /// Returns the rebuilt mutable state and rebuilt history size.
    #[allow(clippy::too_many_arguments)]
    fn rebuild(
        &self,
        now_nanos: i64,
        base_domain_id: &str,
        base_workflow_id: &str,
        base_run_id: &str,
        base_branch_token: &[u8],
        base_last_event_id: i64,
        base_last_event_version: i64,
        target_run_id: &str,
        target_branch_token: &dyn Fn() -> NdcResult<Vec<u8>>,
        request_id: &str,
    ) -> NdcResult<(Box<dyn NdcMutableState>, i64)>;
}

/// Go `conflictResolver.prepareMutableState` result: `None` means keep the
/// workflow's existing mutable state (Go returned `r.mutableState, false`).
pub struct PreparedMutableState {
    pub rebuilt: Option<Box<dyn NdcMutableState>>,
    pub is_rebuilt: bool,
}

/// Go `conflictResolver`.
pub struct ConflictResolver<'a> {
    pub state_rebuilder: &'a dyn NdcStateRebuilder,
}

impl<'a> ConflictResolver<'a> {
    pub fn new(state_rebuilder: &'a dyn NdcStateRebuilder) -> Self {
        Self { state_rebuilder }
    }

    pub fn prepare_mutable_state(
        &self,
        workflow: &mut dyn NdcWorkflow,
        branch_index: i32,
        incoming_version: i64,
    ) -> NdcResult<PreparedMutableState> {
        let keep = PreparedMutableState { rebuilt: None, is_rebuilt: false };
        let version_histories = workflow
            .mutable_state()
            .get_version_histories()
            .ok_or_else(NdcError::missing_version_histories)?;
        let current_index = version_histories.current_version_history_index;
        if branch_index == current_index {
            return Ok(keep);
        }
        let current_vh = version_histories.history(current_index)?;
        let current_last_item = current_vh.last_item()?;
        if incoming_version < current_last_item.version {
            return Ok(keep);
        }
        if incoming_version == current_last_item.version {
            return Err(NdcError::bad_request(
                "nDCConflictResolver encounter replication task version == current branch last write version",
            ));
        }
        let rebuilt = self.rebuild(workflow, branch_index, &uuid::Uuid::new_v4().to_string())?;
        Ok(PreparedMutableState { rebuilt: Some(rebuilt), is_rebuilt: true })
    }

    fn rebuild(
        &self,
        workflow: &mut dyn NdcWorkflow,
        branch_index: i32,
        request_id: &str,
    ) -> NdcResult<Box<dyn NdcMutableState>> {
        let mut version_histories = workflow
            .mutable_state()
            .get_version_histories()
            .ok_or_else(NdcError::missing_version_histories)?
            .clone();
        let replay_vh = version_histories.history(branch_index)?.clone();
        let last_item = replay_vh.last_item()?;
        let info = workflow.mutable_state().get_execution_info();
        let branch_token = replay_vh.branch_token.clone();
        let token_for_fn = branch_token.clone();
        let (mut rebuilt, rebuilt_history_size) = self.state_rebuilder.rebuild(
            info.start_timestamp_nanos,
            &info.domain_id,
            &info.workflow_id,
            &info.run_id,
            &branch_token,
            last_item.event_id,
            last_item.version,
            &info.run_id,
            &move || Ok(token_for_fn.clone()),
            request_id,
        )?;
        let rebuild_histories = rebuilt
            .get_version_histories()
            .ok_or_else(NdcError::missing_version_histories)?;
        let rebuild_vh = rebuild_histories.current_history()?;
        if *rebuild_vh != replay_vh {
            return Err(NdcError::internal(
                "nDCConflictResolver encounter mismatch version history after rebuild",
            ));
        }
        version_histories.set_current_index(branch_index)?;
        rebuilt.set_version_histories(version_histories)?;
        rebuilt.set_update_condition(workflow.mutable_state().get_update_condition());
        workflow.clear();
        workflow.set_history_size(rebuilt_history_size);
        Ok(rebuilt)
    }
}
