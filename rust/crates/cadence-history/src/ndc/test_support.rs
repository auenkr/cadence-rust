//! Test-only scriptable `NdcMutableState` (stands in for Go `execution.MockMutableState`).
use std::collections::HashMap;

use cadence_api::types::VersionHistories;
use cadence_persistence::history_contract::{
    ConflictResolveWorkflowMode, CreateWorkflowMode, UpdateWorkflowMode, WorkflowRequestMode, WorkflowSnapshot,
};
use crate::execution::context::PersistedBlob;
use crate::execution::workflow::WorkflowVectorClock;

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
    pub info: NdcExecutionInfo,
    pub guaranteed: bool,
    pub previous_started: i64,
    pub next_event_id: i64,
    pub vh: Option<VersionHistories>,
}

impl NdcMutableState for MockMs {
    fn get_execution_info(&self) -> NdcExecutionInfo {
        self.info.clone()
    }
    fn get_version_histories(&self) -> Option<&VersionHistories> {
        self.vh.as_ref()
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
        self.guaranteed
    }
    fn get_workflow_state(&self) -> WorkflowState {
        WorkflowState::Running
    }
    fn get_previous_started_event_id(&self) -> i64 {
        self.previous_started
    }
    fn get_next_event_id(&self) -> i64 {
        self.next_event_id
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

/// Calls recorded by [`MockWorkflow`] / [`MockEnv`].
#[derive(Default, Debug)]
pub struct Calls {
    pub released: Option<Option<NdcError>>,
    pub persisted_non_start: usize,
    pub update_with_new: Vec<(UpdateWorkflowMode, TransactionPolicy, bool, WorkflowRequestMode)>,
    pub reapplied: usize,
    /// `(base_run_id, branch_token, last_event_id, last_event_version, next_event_id, reason, events, skip)`
    pub resets: Vec<ResetCall>,
    pub skipped_metric: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResetCall {
    pub domain_id: String,
    pub workflow_id: String,
    pub base_run_id: String,
    pub branch_token: Vec<u8>,
    pub last_event_id: i64,
    pub last_event_version: i64,
    pub next_event_id: i64,
    pub reason: String,
    pub events: Vec<cadence_api::service_types::HistoryEvent>,
    pub skip_signal_reapply: bool,
}

pub type SharedCalls = std::rc::Rc<std::cell::RefCell<Calls>>;

pub struct MockWorkflow {
    pub ms: MockMs,
    pub calls: SharedCalls,
}

impl NdcWorkflow for MockWorkflow {
    fn mutable_state(&self) -> &dyn NdcMutableState {
        &self.ms
    }
    fn mutable_state_mut(&mut self) -> &mut dyn NdcMutableState {
        &mut self.ms
    }
    fn replace_mutable_state(&mut self, _ms: Box<dyn NdcMutableState>) {
        unimplemented!()
    }
    fn clear(&mut self) {}
    fn set_history_size(&mut self, _size: i64) {}
    fn release(&mut self, err: Option<&NdcError>) {
        self.calls.borrow_mut().released = Some(err.cloned());
    }
    fn get_vector_clock(&self) -> NdcResult<WorkflowVectorClock> {
        unimplemented!()
    }
    fn happens_after(&self, _other: &dyn NdcWorkflow) -> NdcResult<bool> {
        unimplemented!()
    }
    fn suppress_by(&mut self, _other: &dyn NdcWorkflow) -> NdcResult<TransactionPolicy> {
        unimplemented!()
    }
    fn revive(&mut self) -> NdcResult<()> {
        unimplemented!()
    }
    fn flush_buffered_events(&mut self) -> NdcResult<()> {
        unimplemented!()
    }
    fn update_workflow_execution_as_active(&mut self, _now: i64) -> NdcResult<()> {
        unimplemented!()
    }
    fn update_workflow_execution_as_passive(&mut self, _now: i64) -> NdcResult<()> {
        unimplemented!()
    }
    fn update_workflow_execution_with_new_as_passive(
        &mut self,
        _now: i64,
        _new_workflow: &mut dyn NdcWorkflow,
    ) -> NdcResult<()> {
        unimplemented!()
    }
    fn update_workflow_execution_with_new(
        &mut self,
        _now: i64,
        mode: UpdateWorkflowMode,
        new_workflow: Option<&mut dyn NdcWorkflow>,
        current_policy: TransactionPolicy,
        new_policy: Option<TransactionPolicy>,
        request_mode: WorkflowRequestMode,
    ) -> NdcResult<()> {
        assert!(new_policy.is_none());
        self.calls.borrow_mut().update_with_new.push((
            mode,
            current_policy,
            new_workflow.is_some(),
            request_mode,
        ));
        Ok(())
    }
    fn conflict_resolve_workflow_execution(
        &mut self,
        _now: i64,
        _mode: ConflictResolveWorkflowMode,
        _new_workflow: Option<&mut dyn NdcWorkflow>,
        _current_workflow: Option<&mut dyn NdcWorkflow>,
        _current_policy: Option<TransactionPolicy>,
    ) -> NdcResult<()> {
        unimplemented!()
    }
    fn persist_start_workflow_batch_events(&mut self, _e: &WorkflowEvents) -> NdcResult<PersistedBlob> {
        unimplemented!()
    }
    fn persist_non_start_workflow_batch_events(&mut self, _e: &WorkflowEvents) -> NdcResult<PersistedBlob> {
        self.calls.borrow_mut().persisted_non_start += 1;
        Ok(PersistedBlob::default())
    }
    fn create_workflow_execution(
        &mut self,
        _snapshot: WorkflowSnapshot,
        _blob: PersistedBlob,
        _mode: CreateWorkflowMode,
        _prev_run_id: &str,
        _prev_last_write_version: i64,
        _request_mode: WorkflowRequestMode,
    ) -> NdcResult<()> {
        unimplemented!()
    }
    fn reapply_events(&mut self, _events: &[WorkflowEvents]) -> NdcResult<()> {
        self.calls.borrow_mut().reapplied += 1;
        Ok(())
    }
    fn load_workflow_execution(&mut self, _task_version: Option<i64>) -> NdcResult<()> {
        unimplemented!()
    }
}

/// Scriptable `TransactionManagerEnv`.
pub struct MockEnv {
    pub domain_name: NdcResult<String>,
    pub workflow_execution: NdcResult<()>,
    pub current_execution: NdcResult<String>,
    pub current_cluster: String,
    pub active_cluster: String,
    pub calls: SharedCalls,
}

impl super::transaction_manager::TransactionManagerEnv for MockEnv {
    fn get_domain_name(&self, _domain_id: &str) -> NdcResult<String> {
        self.domain_name.clone()
    }
    fn get_workflow_execution(&self, _d: &str, _n: &str, _w: &str, _r: &str) -> NdcResult<()> {
        self.workflow_execution.clone()
    }
    fn get_current_execution(&self, _d: &str, _n: &str, _w: &str) -> NdcResult<String> {
        self.current_execution.clone()
    }
    fn get_current_cluster_name(&self) -> String {
        self.current_cluster.clone()
    }
    fn get_active_cluster_name(&self, _ms: &dyn NdcMutableState) -> NdcResult<String> {
        Ok(self.active_cluster.clone())
    }
    fn load_workflow(&self, _d: &str, _w: &str, _r: &str) -> NdcResult<Box<dyn NdcWorkflow>> {
        unimplemented!()
    }
    fn reset_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        base_run_id: &str,
        base_branch_token: &[u8],
        base_rebuild_last_event_id: i64,
        base_rebuild_last_event_version: i64,
        base_next_event_id: i64,
        _reset_run_id: &str,
        _reset_request_id: &str,
        _current: &mut dyn NdcWorkflow,
        reset_reason: &str,
        additional_reapply_events: &[cadence_api::service_types::HistoryEvent],
        skip_signal_reapply: bool,
    ) -> NdcResult<()> {
        self.calls.borrow_mut().resets.push(ResetCall {
            domain_id: domain_id.into(),
            workflow_id: workflow_id.into(),
            base_run_id: base_run_id.into(),
            branch_token: base_branch_token.to_vec(),
            last_event_id: base_rebuild_last_event_id,
            last_event_version: base_rebuild_last_event_version,
            next_event_id: base_next_event_id,
            reason: reset_reason.into(),
            events: additional_reapply_events.to_vec(),
            skip_signal_reapply,
        });
        Ok(())
    }
    fn inc_reapply_events_skipped(&self) {
        self.calls.borrow_mut().skipped_metric += 1;
    }
}
