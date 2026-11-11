//! Test-only scriptable `NdcMutableState` (stands in for Go `execution.MockMutableState`).
use std::collections::HashMap;

use cadence_api::types::VersionHistories;
use cadence_persistence::history_contract::WorkflowSnapshot;

use super::*;

#[derive(Default)]
pub struct MockMs {
    pub running: bool,
    /// `(run_id, event_id, version)` -> duplicated.
    pub duplicated: HashMap<(String, i64, i64), bool>,
    pub fail_add_signaled: bool,
    pub signaled: Vec<(String, Vec<u8>, String, String)>,
    pub updated_dedup: Vec<(String, i64, i64)>,
    pub is_running_calls: std::cell::Cell<usize>,
}

impl NdcMutableState for MockMs {
    fn get_execution_info(&self) -> NdcExecutionInfo {
        NdcExecutionInfo::default()
    }
    fn get_version_histories(&self) -> Option<&VersionHistories> {
        None
    }
    fn get_version_histories_mut(&mut self) -> Option<&mut VersionHistories> {
        None
    }
    fn set_version_histories(&mut self, _v: VersionHistories) -> NdcResult<()> {
        unimplemented!()
    }
    fn get_current_version(&self) -> i64 {
        1
    }
    fn get_start_version(&self) -> NdcResult<i64> {
        Ok(1)
    }
    fn get_last_write_version(&self) -> NdcResult<i64> {
        Ok(1)
    }
    fn get_update_condition(&self) -> i64 {
        0
    }
    fn set_update_condition(&mut self, _c: i64) {}
    fn has_buffered_events(&self) -> bool {
        false
    }
    fn is_workflow_execution_running(&self) -> bool {
        self.is_running_calls.set(self.is_running_calls.get() + 1);
        self.running
    }
    fn is_current_workflow_guaranteed(&self) -> bool {
        false
    }
    fn get_workflow_state(&self) -> WorkflowState {
        WorkflowState::Running
    }
    fn get_previous_started_event_id(&self) -> i64 {
        0
    }
    fn get_next_event_id(&self) -> i64 {
        0
    }
    fn get_current_branch_token(&self) -> NdcResult<Vec<u8>> {
        Ok(Vec::new())
    }
    fn is_event_reapplied(&self, run_id: &str, event_id: i64, version: i64) -> NdcResult<bool> {
        Ok(*self.duplicated.get(&(run_id.to_string(), event_id, version)).unwrap_or(&false))
    }
    fn update_event_reapplied(&mut self, run_id: &str, event_id: i64, version: i64) -> NdcResult<()> {
        self.updated_dedup.push((run_id.to_string(), event_id, version));
        Ok(())
    }
    fn add_workflow_execution_signaled(
        &mut self,
        signal_name: &str,
        input: &[u8],
        identity: &str,
        request_id: &str,
    ) -> NdcResult<()> {
        if self.fail_add_signaled {
            return Err(NdcError::Other("test".into()));
        }
        self.signaled.push((signal_name.into(), input.to_vec(), identity.into(), request_id.into()));
        Ok(())
    }
    fn get_activity_info(&self, _schedule_id: i64) -> NdcResult<Option<NdcActivityInfo>> {
        unimplemented!()
    }
    fn replicate_activity_info(&mut self, _r: &SyncActivityRequest, _reset: bool) -> NdcResult<()> {
        unimplemented!()
    }
    fn create_next_activity_timer(&mut self) -> NdcResult<bool> {
        unimplemented!()
    }
    fn generate_activity_retry_tasks(&mut self, _schedule_id: i64) -> NdcResult<()> {
        unimplemented!()
    }
    fn close_transaction_as_snapshot(
        &mut self,
        _now_nanos: i64,
        _policy: TransactionPolicy,
    ) -> NdcResult<(WorkflowSnapshot, Vec<WorkflowEvents>)> {
        unimplemented!()
    }
}
