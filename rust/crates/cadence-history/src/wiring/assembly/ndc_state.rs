//! Exclusively owned typed working state for the first NDC migration slice.
//!
//! This is a structural state owner, not a persistence envelope: it deliberately
//! rejects replay and persistence operations until the lossless codec and shared
//! engine-cache checkout are available.

use cadence_api::types::{VersionHistories, WorkflowExecution};
use cadence_persistence::version_history::VersionHistoriesExt;

use crate::execution::mutable_state_builder::{TypedMutableState, EMPTY_VERSION};
use crate::execution::mutable_state_methods_common::{
    TaskGenEnv, CLOSE_STATUS_NONE, WORKFLOW_STATE_COMPLETED, WORKFLOW_STATE_CREATED, WORKFLOW_STATE_RUNNING,
    WORKFLOW_STATE_ZOMBIE,
};
use crate::execution::workflow::{TransactionPolicy, WorkflowState};
use crate::ndc::{NdcActivityInfo, NdcError, NdcExecutionInfo, NdcMutableState, NdcResult, SyncActivityRequest};
use crate::execution::context::{TransactionState, WorkflowEvents, WorkflowExecutionKey};
use cadence_persistence::history_contract::WorkflowSnapshot;

/// Owns the typed mutable state so its version histories can be borrowed
/// directly without mutex guards, leaked lifetimes, or unsafe references.
pub struct OwnedNdcMutableState {
    typed: TypedMutableState,
    domain_id: String,
    workflow_id: String,
    run_id: String,
    update_condition: i64,
    now_nanos: i64,
    task_gen_env: TaskGenEnv,
}

impl OwnedNdcMutableState {
    /// Go `newMutableStateBuilder`: created/running state, first event ID 1,
    /// empty version sentinels, and a zero `nextEventIDInDB` CAS condition.
    pub fn new(
        domain_id: impl Into<String>,
        execution: &WorkflowExecution,
        now_nanos: i64,
        task_gen_env: TaskGenEnv,
    ) -> Self {
        let mut typed = TypedMutableState::new();
        typed.execution_info.domain_id = domain_id.into();
        typed.execution_info.workflow_id = execution.workflow_id.clone();
        typed.execution_info.run_id = execution.run_id.clone();
        typed.current_version = EMPTY_VERSION;
        typed.next_event_id_in_db = 0;
        typed.condition = 0;
        typed.execution_info.last_first_event_id = crate::ndc::EMPTY_EVENT_ID;
        Self {
            domain_id: typed.execution_info.domain_id.clone(),
            workflow_id: typed.execution_info.workflow_id.clone(),
            run_id: typed.execution_info.run_id.clone(),
            typed,
            update_condition: 0,
            now_nanos,
            task_gen_env,
        }
    }

    pub fn typed(&self) -> &TypedMutableState { &self.typed }
    pub fn typed_mut(&mut self) -> &mut TypedMutableState { &mut self.typed }
    pub fn now_nanos(&self) -> i64 { self.now_nanos }
    pub fn task_gen_env(&self) -> &TaskGenEnv { &self.task_gen_env }

    #[allow(clippy::result_large_err)]
    fn unsupported<T>(&self, operation: &str) -> NdcResult<T> {
        Err(NdcError::internal(format!(
            "NDC owned state operation `{operation}` is unsupported until replay and the lossless persistence/cache bridge are implemented"
        )))
    }
}

impl NdcMutableState for OwnedNdcMutableState {
    fn get_execution_info(&self) -> NdcExecutionInfo {
        NdcExecutionInfo {
            domain_id: self.domain_id.clone(),
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            start_timestamp_nanos: self.typed.execution_info.start_time_unix_nanos,
        }
    }

    fn get_version_histories(&self) -> Option<&VersionHistories> {
        self.typed.version_histories.as_ref()
    }

    fn get_version_histories_mut(&mut self) -> Option<&mut VersionHistories> {
        self.typed.version_histories.as_mut()
    }

    fn set_version_histories(&mut self, histories: VersionHistories) -> NdcResult<()> {
        if histories.histories.is_empty() {
            return Err(NdcError::bad_request("version histories has no branches"));
        }
        histories.current_history().map_err(NdcError::BadRequest)?;
        self.typed.version_histories = Some(histories);
        Ok(())
    }

    fn get_current_version(&self) -> i64 { self.typed.get_current_version() }

    fn get_start_version(&self) -> NdcResult<i64> {
        self.typed.get_start_version().map_err(|e| NdcError::internal(e.to_string()))
    }

    fn get_last_write_version(&self) -> NdcResult<i64> {
        self.typed.get_last_write_version().map_err(|e| NdcError::internal(e.to_string()))
    }

    fn get_update_condition(&self) -> i64 { self.update_condition }

    fn set_update_condition(&mut self, condition: i64) {
        self.update_condition = condition;
        self.typed.condition = condition;
        self.typed.next_event_id_in_db = condition;
    }

    fn has_buffered_events(&self) -> bool { self.typed.has_buffered_events() }
    fn is_workflow_execution_running(&self) -> bool { self.typed.is_workflow_execution_running() }
    fn is_current_workflow_guaranteed(&self) -> bool { self.typed.is_current_workflow_guaranteed() }

    fn get_workflow_state(&self) -> WorkflowState {
        match self.typed.execution_info.state {
            WORKFLOW_STATE_RUNNING => WorkflowState::Running,
            WORKFLOW_STATE_COMPLETED => WorkflowState::Completed,
            WORKFLOW_STATE_ZOMBIE => WorkflowState::Zombie,
            _ => WorkflowState::Created,
        }
    }

    fn get_previous_started_event_id(&self) -> i64 { self.typed.execution_info.decision_started_id }
    fn get_next_event_id(&self) -> i64 { self.typed.execution_info.next_event_id }

    fn get_current_branch_token(&self) -> NdcResult<Vec<u8>> {
        if let Some(histories) = self.typed.version_histories.as_ref() {
            let history = histories.current_history().map_err(NdcError::BadRequest)?;
            if !history.branch_token.is_empty() {
                return Ok(history.branch_token.clone());
            }
        }
        if self.typed.execution_info.branch_token.is_empty() {
            return self.unsupported("get_current_branch_token");
        }
        Ok(self.typed.execution_info.branch_token.clone())
    }

    fn is_event_reapplied(&self, _run_id: &str, _event_id: i64, _version: i64) -> NdcResult<bool> {
        self.unsupported("is_event_reapplied")
    }

    fn update_event_reapplied(&mut self, _run_id: &str, _event_id: i64, _version: i64) -> NdcResult<()> {
        self.unsupported("update_event_reapplied")
    }

    fn add_workflow_execution_signaled(&mut self, _signal_name: &str, _input: &[u8], _identity: &str, _request_id: &str) -> NdcResult<()> {
        self.unsupported("add_workflow_execution_signaled")
    }

    fn get_activity_info(&self, schedule_id: i64) -> NdcResult<Option<NdcActivityInfo>> {
        if self.typed.pending_activity_info_ids.contains_key(&schedule_id) {
            return self.unsupported("get_activity_info (heartbeat timestamp is persistence-only)");
        }
        Ok(None)
    }

    fn replicate_activity_info(&mut self, _request: &SyncActivityRequest, _reset: bool) -> NdcResult<()> {
        self.unsupported("replicate_activity_info")
    }

    fn create_next_activity_timer(&mut self) -> NdcResult<bool> {
        self.unsupported("create_next_activity_timer")
    }

    fn generate_activity_retry_tasks(&mut self, _schedule_id: i64) -> NdcResult<()> {
        self.unsupported("generate_activity_retry_tasks")
    }

    fn vector_clock_inputs(
        &self,
    ) -> NdcResult<(Option<cadence_api::types::ActiveClusterSelectionPolicy>, i64)> {
        // Decoding the persisted policy blob is not ported: refuse rather than
        // compare clocks as if the policy were absent.
        if self
            .typed
            .execution_info
            .active_cluster_selection_policy
            .as_ref()
            .is_some_and(|b| !b.data.is_empty())
        {
            return self.unsupported("vector_clock_inputs (active cluster selection policy decode)");
        }
        Ok((None, self.typed.execution_info.last_event_task_id))
    }

    fn has_processed_or_pending_decision(&self) -> NdcResult<bool> {
        Ok(self.typed.has_pending_decision()
            || self.typed.execution_info.last_processed_event != crate::ndc::EMPTY_EVENT_ID)
    }

    fn has_in_flight_decision(&self) -> NdcResult<bool> {
        Ok(self.typed.has_in_flight_decision())
    }

    fn update_workflow_state(&mut self, state: WorkflowState) -> NdcResult<()> {
        let raw = match state {
            WorkflowState::Created => WORKFLOW_STATE_CREATED,
            WorkflowState::Running => WORKFLOW_STATE_RUNNING,
            WorkflowState::Zombie => WORKFLOW_STATE_ZOMBIE,
            WorkflowState::Completed | WorkflowState::Corrupted => {
                return self.unsupported("update_workflow_state to completed/corrupted (needs close status)")
            }
        };
        self.typed
            .update_workflow_state_close_status(raw, CLOSE_STATUS_NONE)
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    fn update_current_version(&mut self, version: i64, force: bool) -> NdcResult<()> {
        self.typed.update_current_version(version, force).map_err(|e| NdcError::internal(e.to_string()))
    }

    fn close_transaction_as_snapshot(&mut self, now_nanos: i64, policy: TransactionPolicy) -> NdcResult<(WorkflowSnapshot, Vec<WorkflowEvents>)> {
        let (typed, events) = self.typed.prepare_close_snapshot(now_nanos, policy)
            .map_err(|e| NdcError::internal(e.to_string()))?;
        let snapshot = crate::execution::mutable_state_bridge::typed_snapshot_to_persistence(&typed)
            .map_err(|e| NdcError::internal(e.to_string()))?;
        let batches = if events.is_empty() { Vec::new() } else { vec![WorkflowEvents {
            key: WorkflowExecutionKey { domain_id: self.domain_id.clone(), workflow_id: self.workflow_id.clone(), run_id: self.run_id.clone() },
            branch_token: self.get_current_branch_token()?, events,
        }] };
        Ok((snapshot, batches))
    }
    fn close_transaction_as_mutation(&mut self, now_nanos: i64, policy: TransactionPolicy) -> NdcResult<(cadence_persistence::history_contract::WorkflowMutation, Vec<WorkflowEvents>)> {
        TransactionState::close_mutation(self, now_nanos, policy)
    }
    fn transaction_committed(&mut self) { self.typed.cleanup_transaction(); self.update_condition = self.typed.next_event_id_in_db; self.typed.condition = self.update_condition; }
    fn history_notification_inputs(&self) -> NdcResult<(i64, i32, i32)> {
        let info = &self.typed.execution_info;
        Ok((info.last_first_event_id, info.state, info.close_status))
    }
}

impl TransactionState for OwnedNdcMutableState {
    type Error = NdcError;
    fn history_size(&self) -> i64 { self.typed.history_size }
    fn set_history_size(&mut self, size: i64) { self.typed.history_size = size; }
    fn next_event_id(&self) -> i64 { self.typed.execution_info.next_event_id }
    async fn start_transaction(&mut self, _incoming_version: i64) -> NdcResult<bool> { Ok(false) }
    fn close_mutation(&mut self, now: i64, policy: TransactionPolicy) -> NdcResult<(cadence_persistence::history_contract::WorkflowMutation, Vec<WorkflowEvents>)> {
        let (typed, events) = self.typed.prepare_close_mutation(now, policy).map_err(|e| NdcError::internal(e.to_string()))?;
        let envelope = crate::execution::mutable_state_bridge::snapshot_to_envelope(WorkflowSnapshot::default()).map_err(|e| NdcError::internal(e.to_string()))?;
        let mutation = crate::execution::mutable_state_bridge::mutation_to_persistence(&typed, &envelope).map_err(|e| NdcError::internal(e.to_string()))?;
        let batches = if events.is_empty() { Vec::new() } else { vec![WorkflowEvents { key: WorkflowExecutionKey { domain_id: self.domain_id.clone(), workflow_id: self.workflow_id.clone(), run_id: self.run_id.clone() }, branch_token: self.get_current_branch_token()?, events }] };
        Ok((mutation, batches))
    }
    fn close_snapshot(&mut self, now: i64, policy: TransactionPolicy) -> NdcResult<(WorkflowSnapshot, Vec<WorkflowEvents>)> { self.close_transaction_as_snapshot(now, policy) }
    fn transaction_committed(&mut self) { self.typed.cleanup_transaction(); self.update_condition = self.typed.next_event_id_in_db; self.typed.condition = self.update_condition; }
}

#[cfg(test)]
mod tests {
    use cadence_api::types::{VersionHistories, VersionHistory, VersionHistoryItem, WorkflowExecution};
    use cadence_persistence::version_history::VersionHistoryExt;

    use super::*;

    fn state() -> OwnedNdcMutableState {
        OwnedNdcMutableState::new(
            "domain-a",
            &WorkflowExecution { workflow_id: "workflow-a".into(), run_id: "run-a".into() },
            123,
            TaskGenEnv::default(),
        )
    }

    #[test]
    fn construction_uses_go_identity_and_empty_cas_sentinels() {
        let state = state();
        let info = state.get_execution_info();
        assert_eq!((info.domain_id.as_str(), info.workflow_id.as_str(), info.run_id.as_str()), ("domain-a", "workflow-a", "run-a"));
        assert_eq!(state.get_update_condition(), 0);
        assert_eq!(state.get_next_event_id(), 1);
        assert_eq!(state.get_current_version(), EMPTY_VERSION);
        assert_eq!(state.get_start_version().unwrap(), EMPTY_VERSION);
        assert_eq!(state.get_last_write_version().unwrap(), EMPTY_VERSION);
        assert!(state.is_workflow_execution_running());
        assert_eq!(state.get_workflow_state(), WorkflowState::Created);
    }

    #[test]
    fn owned_version_history_edits_are_visible_through_borrowed_accessors() {
        let mut state = state();
        state.set_version_histories(VersionHistories {
            current_version_history_index: 0,
            histories: vec![VersionHistory { branch_token: vec![1, 2, 3], items: vec![VersionHistoryItem { event_id: 1, version: 7 }] }],
        }).unwrap();
        state.get_version_histories_mut().unwrap().histories[0]
            .add_or_update(&VersionHistoryItem { event_id: 4, version: 7 }).unwrap();
        assert_eq!(state.get_version_histories().unwrap().current_history().unwrap().last_item().unwrap(), VersionHistoryItem { event_id: 4, version: 7 });
        assert_eq!(state.get_current_branch_token().unwrap(), vec![1, 2, 3]);
        assert_eq!(state.get_current_version(), EMPTY_VERSION);
        assert_eq!(state.get_start_version().unwrap(), 7);
        assert_eq!(state.get_last_write_version().unwrap(), 7);
    }

    #[test]
    fn update_condition_tracks_go_cas_value_independently_of_next_event_id() {
        let mut state = state();
        state.set_update_condition(18);
        assert_eq!(state.get_update_condition(), 18);
        assert_eq!(state.get_next_event_id(), 1);
        assert_eq!(state.typed().condition, 18);
    }

    #[test]
    fn unsupported_dedup_and_activity_reads_fail_explicitly() {
        let state = state();
        assert!(state.is_event_reapplied("run-a", 2, 7).is_err());
        assert!(state.get_activity_info(2).is_ok_and(|activity| activity.is_none()));
        let mut state = state;
        state.typed_mut().pending_activity_info_ids.insert(2, Default::default());
        assert!(state.get_activity_info(2).is_err());
    }
}
