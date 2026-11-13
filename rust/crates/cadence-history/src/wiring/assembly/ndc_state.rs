//! Exclusively owned typed working state for the first NDC migration slice.
//!
//! This is a structural state owner, not a persistence envelope: it deliberately
//! rejects replay and persistence operations until the lossless codec and shared
//! engine-cache checkout are available.

use cadence_api::types::{VersionHistories, WorkflowExecution};
use cadence_thrift::value::FromThrift as _;
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
    /// Go `appliedEvents` (in-memory duplicate-resource set).
    applied_events: std::collections::BTreeSet<String>,
}

/// Go `prepareEventsAndReplicationTasks` (`eventsToReplicationTask` + `syncActivityToReplicationTask`)
/// for the active policy; empty unless `canReplicateEvents()`.
fn active_replication_rows(
    typed: &TypedMutableState,
    domain_id: &str,
    workflow_id: &str,
    run_id: &str,
    events: &[cadence_api::types::HistoryEvent],
    branch_token: &[u8],
) -> NdcResult<Vec<cadence_persistence::history_contract::Task>> {
    use cadence_persistence::history_contract as pc;
    use cadence_persistence::sqlblobs::replication_task_to_struct;
    let Some(cluster) = typed.failover.as_ref().filter(|c| c.can_replicate_events) else {
        return Ok(Vec::new());
    };
    let mut tasks = Vec::new();
    let mut push = |t: pc::ReplicationTask| -> NdcResult<()> {
        let s = replication_task_to_struct(&t).map_err(|e| NdcError::internal(e.to_string()))?;
        let data = super::engine_cache::task_blob(cadence_persistence::sqlblobs::Encoding::ThriftRW.encode(&s))
            .map_err(|e| NdcError::internal(e.to_string()))?;
        tasks.push(pc::Task { visibility_timestamp_unix_nanos: 0, task_type: t.task_type, task_id: 0, data });
        Ok(())
    };
    if let (Some(first), Some(last)) = (events.first(), events.last()) {
        let source = (cluster.cluster_name_for_failover_version)(first.version).map_err(NdcError::internal)?;
        if cluster.current_cluster != source {
            return Err(NdcError::internal("mutableStateBuilder encounter contradicting version & transaction policy"));
        }
        push(pc::ReplicationTask {
            domain_id: domain_id.to_string(),
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            task_type: super::engine_close::REPLICATION_TASK_TYPE_HISTORY,
            version: first.version,
            first_event_id: first.event_id,
            next_event_id: last.event_id + 1,
            branch_token: branch_token.to_vec(),
            ..Default::default()
        })?;
    }
    for sid in &typed.sync_activity_tasks {
        if let Some(a) = typed.pending_activity_info_ids.get(sid) {
            push(pc::ReplicationTask {
                domain_id: domain_id.to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                task_type: super::engine_close::REPLICATION_TASK_TYPE_SYNC_ACTIVITY,
                version: a.version,
                scheduled_id: *sid,
                ..Default::default()
            })?;
        }
    }
    Ok(tasks)
}

fn add_replication_rows(
    tasks_by_category: &mut std::collections::BTreeMap<cadence_persistence::history_contract::HistoryTaskCategory, Vec<cadence_persistence::history_contract::Task>>,
    rows: Vec<cadence_persistence::history_contract::Task>,
) {
    if !rows.is_empty() {
        let category = cadence_persistence::history_contract::HistoryTaskCategory { category_type: 1, category_id: 3 };
        tasks_by_category.entry(category).or_default().extend(rows);
    }
}

/// Go `definition.NewEventReappliedID(runID, eventID, version).GetID()`.
fn event_reapplied_id(run_id: &str, event_id: i64, version: i64) -> String {
    format!("{run_id}::{event_id}::{version}")
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
            applied_events: Default::default(),
        }
    }

    /// Wraps an already replayed typed state (e.g. a continued-as-new run).
    pub fn from_typed(
        domain_id: impl Into<String>,
        execution: &WorkflowExecution,
        typed: TypedMutableState,
        now_nanos: i64,
        task_gen_env: TaskGenEnv,
    ) -> Self {
        Self {
            domain_id: domain_id.into(),
            workflow_id: execution.workflow_id.clone(),
            run_id: execution.run_id.clone(),
            update_condition: typed.condition,
            typed,
            now_nanos,
            task_gen_env,
            applied_events: Default::default(),
        }
    }

    /// Installs Go's `clusterMetadata` / `canReplicateEvents` and the decision config.
    pub fn set_failover_context(
        &mut self,
        failover: crate::execution::mutable_state_failover::FailoverCluster,
        normal_schedule_to_start_timeout_nanos: i64,
        normal_schedule_to_start_max_attempts: i64,
        now_nanos: i64,
    ) {
        self.typed.failover = Some(failover);
        self.task_gen_env.normal_schedule_to_start_timeout_nanos = normal_schedule_to_start_timeout_nanos;
        self.task_gen_env.normal_schedule_to_start_max_attempts = normal_schedule_to_start_max_attempts;
        self.now_nanos = now_nanos;
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
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> { Some(self) }

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
        // Go `GetCurrentBranchToken`: without version histories the execution
        // info token is returned as is (possibly empty).
        Ok(self.typed.execution_info.branch_token.clone())
    }

    /// Go `IsResourceDuplicated(definition.NewEventReappliedID(...))`: the Go
    /// `appliedEvents` set is in-memory only (never persisted).
    fn is_event_reapplied(&self, run_id: &str, event_id: i64, version: i64) -> NdcResult<bool> {
        Ok(self.applied_events.contains(&event_reapplied_id(run_id, event_id, version)))
    }

    fn update_event_reapplied(&mut self, run_id: &str, event_id: i64, version: i64) -> NdcResult<()> {
        self.applied_events.insert(event_reapplied_id(run_id, event_id, version));
        Ok(())
    }

    fn add_workflow_execution_signaled(&mut self, signal_name: &str, input: &[u8], identity: &str, request_id: &str) -> NdcResult<()> {
        self.typed
            .add_workflow_execution_signaled(self.now_nanos, signal_name, input, identity, request_id)
            .map(|_| ())
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    fn get_activity_info(&self, schedule_id: i64) -> NdcResult<Option<NdcActivityInfo>> {
        Ok(self.typed.get_activity_info(schedule_id).map(|a| NdcActivityInfo {
            version: a.version,
            attempt: a.attempt,
            last_heartbeat_updated_time_nanos: a.last_heartbeat_updated_time,
            cancel_requested: a.cancel_requested,
        }))
    }

    fn replicate_activity_info(&mut self, request: &SyncActivityRequest, reset: bool) -> NdcResult<()> {
        let engine = crate::engine::types::SyncActivityRequest {
            domain_id: request.domain_id.clone(),
            workflow_id: request.workflow_id.clone(),
            run_id: request.run_id.clone(),
            version: request.version,
            scheduled_id: request.scheduled_id,
            scheduled_time: Some(request.scheduled_time),
            started_id: request.started_id,
            started_time: Some(request.started_time),
            last_heartbeat_time: Some(request.last_heartbeat_time),
            details: request.details.clone(),
            attempt: request.attempt,
            last_failure_reason: request.last_failure_reason.clone(),
            last_worker_identity: request.last_worker_identity.clone(),
            last_failure_details: request.last_failure_details.clone(),
            last_failure_options: request.last_failure_options.clone(),
            version_history: request.version_history.clone(),
        };
        self.typed
            .replicate_activity_info(&engine, reset)
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    fn create_next_activity_timer(&mut self) -> NdcResult<bool> {
        self.typed
            .run_task_generator(0, |g| {
                crate::execution::timer_sequence::TimerSequence { state: g.state }.create_next_activity_timer()
            })
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    fn generate_activity_retry_tasks(&mut self, schedule_id: i64) -> NdcResult<()> {
        self.typed
            .run_task_generator(0, |g| g.generate_activity_retry_tasks(schedule_id))
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    fn vector_clock_inputs(
        &self,
    ) -> NdcResult<(Option<cadence_api::types::ActiveClusterSelectionPolicy>, i64)> {
        // Go `GetActiveClusterSelectionPolicy`: decode the persisted thriftrw blob.
        let policy = match self.typed.execution_info.active_cluster_selection_policy.as_ref() {
            Some(b) if !b.data.is_empty() => Some(
                cadence_api::types::ActiveClusterSelectionPolicy::from_thrift(
                    &super::engine_state::decode(b).map_err(|e| NdcError::internal(e.to_string()))?,
                )
                .map_err(|e| NdcError::internal(format!("decode active cluster selection policy: {e}")))?,
            ),
            _ => None,
        };
        Ok((policy, self.typed.execution_info.last_event_task_id))
    }

    fn has_processed_or_pending_decision(&self) -> NdcResult<bool> {
        Ok(self.typed.has_pending_decision()
            || self.typed.execution_info.last_processed_event != crate::ndc::EMPTY_EVENT_ID)
    }

    fn has_in_flight_decision(&self) -> NdcResult<bool> {
        Ok(self.typed.has_in_flight_decision())
    }

    fn update_workflow_state(&mut self, state: WorkflowState) -> NdcResult<()> {
        // Go callers pass the existing close status (Completed keeps it, others reset to none).
        let close_status = match state {
            WorkflowState::Completed => self.typed.execution_info.close_status,
            _ => CLOSE_STATUS_NONE,
        };
        let raw = match state {
            WorkflowState::Created => WORKFLOW_STATE_CREATED,
            WorkflowState::Running => WORKFLOW_STATE_RUNNING,
            WorkflowState::Zombie => WORKFLOW_STATE_ZOMBIE,
            WorkflowState::Completed => WORKFLOW_STATE_COMPLETED,
            WorkflowState::Corrupted => {
                return self.unsupported("update_workflow_state to corrupted (typed transition table has no corrupted state)")
            }
        };
        self.typed
            .update_workflow_state_close_status(raw, close_status)
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    fn start_transaction_handle_decision_failover(&mut self, incoming_task_version: i64) -> NdcResult<bool> {
        let env = crate::execution::mutable_state_methods_decision::DecisionEnv {
            now_nanos: self.now_nanos,
            normal_schedule_to_start_timeout_nanos: self.task_gen_env.normal_schedule_to_start_timeout_nanos,
            normal_schedule_to_start_max_attempts: self.task_gen_env.normal_schedule_to_start_max_attempts,
            ..Default::default()
        };
        self.typed
            .start_transaction_handle_decision_failover(&env, incoming_task_version)
            .map_err(|e| NdcError::internal(e.to_string()))
    }

    fn update_current_version(&mut self, version: i64, force: bool) -> NdcResult<()> {
        self.typed.update_current_version(version, force).map_err(|e| NdcError::internal(e.to_string()))
    }

    fn close_transaction_as_snapshot(&mut self, now_nanos: i64, policy: TransactionPolicy) -> NdcResult<(WorkflowSnapshot, Vec<WorkflowEvents>)> {
        let (typed, events) = self.typed.prepare_close_snapshot(now_nanos, policy)
            .map_err(|e| NdcError::internal(e.to_string()))?;
        let mut snapshot = crate::execution::mutable_state_bridge::typed_snapshot_to_persistence(&typed)
            .map_err(|e| NdcError::internal(e.to_string()))?;
        if policy == TransactionPolicy::Active {
            let rows = active_replication_rows(&self.typed, &self.domain_id, &self.workflow_id, &self.run_id, &events, &self.get_current_branch_token()?)?;
            add_replication_rows(&mut snapshot.tasks_by_category, rows);
        }
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
    fn update_current_version_from_active(&mut self, failover_version: i64) -> NdcResult<()> { self.update_current_version(failover_version, false) }
    async fn start_transaction(&mut self, incoming_version: i64) -> NdcResult<bool> { self.start_transaction_handle_decision_failover(incoming_version) }
    fn close_mutation(&mut self, now: i64, policy: TransactionPolicy) -> NdcResult<(cadence_persistence::history_contract::WorkflowMutation, Vec<WorkflowEvents>)> {
        let (typed, events) = self.typed.prepare_close_mutation(now, policy).map_err(|e| NdcError::internal(e.to_string()))?;
        let envelope = crate::execution::mutable_state_bridge::snapshot_to_envelope(WorkflowSnapshot::default()).map_err(|e| NdcError::internal(e.to_string()))?;
        let mut mutation = crate::execution::mutable_state_bridge::mutation_to_persistence(&typed, &envelope).map_err(|e| NdcError::internal(e.to_string()))?;
        if policy == TransactionPolicy::Active {
            let rows = active_replication_rows(&self.typed, &self.domain_id, &self.workflow_id, &self.run_id, &events, &self.get_current_branch_token()?)?;
            add_replication_rows(&mut mutation.tasks_by_category, rows);
        }
        let batches = if events.is_empty() { Vec::new() } else { vec![WorkflowEvents { key: WorkflowExecutionKey { domain_id: self.domain_id.clone(), workflow_id: self.workflow_id.clone(), run_id: self.run_id.clone() }, branch_token: self.get_current_branch_token()?, events }] };
        Ok((mutation, batches))
    }
    fn close_snapshot(&mut self, now: i64, policy: TransactionPolicy) -> NdcResult<(WorkflowSnapshot, Vec<WorkflowEvents>)> { self.close_transaction_as_snapshot(now, policy) }
    fn transaction_committed(&mut self) { self.typed.cleanup_transaction(); self.update_condition = self.typed.next_event_id_in_db; self.typed.condition = self.update_condition; }
}

#[cfg(test)]
mod tests {
    use cadence_api::enums::EventType;
    use cadence_api::types::{VersionHistories, VersionHistory, VersionHistoryItem, WorkflowExecution};
    use cadence_persistence::version_history::VersionHistoryExt;

    use super::*;
    use cadence_thrift::value::ToThrift as _;

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
    fn completed_state_keeps_existing_close_status_and_none_is_rejected() {
        let mut state = state();
        state.typed_mut().execution_info.state = WORKFLOW_STATE_RUNNING;
        // Running -> Completed with close status none is invalid (Go UpdateWorkflowStateCloseStatus).
        assert!(state.update_workflow_state(WorkflowState::Completed).is_err());
        state.typed_mut().execution_info.close_status = 1;
        state.update_workflow_state(WorkflowState::Completed).unwrap();
        assert_eq!(state.get_workflow_state(), WorkflowState::Completed);
        assert_eq!(state.typed().execution_info.close_status, 1);
    }

    #[test]
    fn empty_branch_token_without_version_histories_is_returned_as_is() {
        let state = state();
        assert_eq!(state.get_current_branch_token().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn vector_clock_inputs_decodes_active_cluster_selection_policy() {
        let mut state = state();
        assert_eq!(state.vector_clock_inputs().unwrap().0, None);
        let policy = cadence_api::types::ActiveClusterSelectionPolicy::default();
        let encoded = cadence_persistence::sqlblobs::Encoding::ThriftRW.encode(&policy.to_thrift()).unwrap();
        state.typed_mut().execution_info.active_cluster_selection_policy =
            Some(cadence_persistence::history_contract::DataBlob { encoding: "thriftrw".into(), data: encoded });
        state.typed_mut().execution_info.last_event_task_id = 9;
        let (decoded, task_id) = state.vector_clock_inputs().unwrap();
        assert_eq!(decoded, Some(policy));
        assert_eq!(task_id, 9);
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
    fn event_reapplied_dedup_is_tracked_in_memory_like_go_applied_events() {
        let mut state = state();
        assert!(!state.is_event_reapplied("run-a", 2, 7).unwrap());
        state.update_event_reapplied("run-a", 2, 7).unwrap();
        assert!(state.is_event_reapplied("run-a", 2, 7).unwrap());
        assert!(!state.is_event_reapplied("run-a", 2, 8).unwrap());
        assert!(!state.is_event_reapplied("run-b", 2, 7).unwrap());
        assert!(state.get_activity_info(2).is_ok_and(|activity| activity.is_none()));
    }

    fn with_activity(timer_status: i32) -> OwnedNdcMutableState {
        let mut state = state();
        let ai = crate::mutable_state_activity::ActivityInfo {
            version: 3,
            schedule_id: 5,
            scheduled_time: 1_000_000_000,
            started_id: crate::ndc::EMPTY_EVENT_ID,
            schedule_to_start_timeout: 10,
            schedule_to_close_timeout: 30,
            start_to_close_timeout: 20,
            attempt: 1,
            timer_task_status: timer_status,
            task_list: "tl".into(),
            ..Default::default()
        };
        state.typed_mut().pending_activity_info_ids.insert(5, ai);
        state
    }

    fn sync(version: i64, attempt: i32, heartbeat: i64) -> SyncActivityRequest {
        SyncActivityRequest {
            domain_id: "domain-a".into(),
            workflow_id: "workflow-a".into(),
            run_id: "run-a".into(),
            version,
            scheduled_id: 5,
            scheduled_time: 2_000_000_000,
            started_id: 6,
            started_time: 3_000_000_000,
            last_heartbeat_time: heartbeat,
            attempt,
            ..Default::default()
        }
    }

    #[test]
    fn replicate_activity_info_carries_version_attempt_heartbeat_and_orders() {
        let mut state = with_activity(0);
        let info = state.get_activity_info(5).unwrap().unwrap();
        assert_eq!((info.version, info.attempt), (3, 1));
        // newer version and higher attempt replace
        state.replicate_activity_info(&sync(4, 2, 77), false).unwrap();
        let info = state.get_activity_info(5).unwrap().unwrap();
        assert_eq!((info.version, info.attempt, info.last_heartbeat_updated_time_nanos), (4, 2, 77));
        assert_eq!(state.typed().pending_activity_info_ids[&5].started_id, 6);
        assert_eq!(state.typed().pending_activity_info_ids[&5].started_time, 3_000_000_000);
        assert!(state.typed().update_activity_infos.contains_key(&5));
        // equal version: the replicator policy owns ordering, the adapter simply carries the request
        state.replicate_activity_info(&sync(4, 3, 99), false).unwrap();
        assert_eq!(state.get_activity_info(5).unwrap().unwrap().attempt, 3);
        // an older version/attempt request is also carried verbatim (policy lives in activity_replicator)
        state.replicate_activity_info(&sync(2, 1, 5), false).unwrap();
        let info = state.get_activity_info(5).unwrap().unwrap();
        assert_eq!((info.version, info.attempt, info.last_heartbeat_updated_time_nanos), (2, 1, 5));
    }

    #[test]
    fn replicate_activity_info_resets_timer_status_only_when_requested() {
        let mut state = with_activity(3);
        state.replicate_activity_info(&sync(4, 1, 1), false).unwrap();
        assert_eq!(state.typed().pending_activity_info_ids[&5].timer_task_status, 3);
        state.replicate_activity_info(&sync(4, 1, 1), true).unwrap();
        assert_eq!(state.typed().pending_activity_info_ids[&5].timer_task_status, 0);
    }

    #[test]
    fn replicate_activity_info_missing_activity_is_error() {
        let mut state = state();
        assert!(state.replicate_activity_info(&sync(4, 1, 1), false).is_err());
        assert!(state.generate_activity_retry_tasks(5).is_err());
    }

    #[test]
    fn retry_tasks_and_activity_timer_are_generated() {
        let mut state = with_activity(0);
        state.generate_activity_retry_tasks(5).unwrap();
        assert_eq!(state.typed().timer_tasks.len(), 1);
        let before = state.typed().timer_tasks.len();
        assert!(state.create_next_activity_timer().unwrap());
        assert!(state.typed().timer_tasks.len() > before);
        assert_ne!(state.typed().pending_activity_info_ids[&5].timer_task_status, 0);
    }

    fn started(event_type: EventType, id: i64, version: i64) -> cadence_api::service_types::HistoryEvent {
        cadence_api::service_types::HistoryEvent { event_id: id, version, timestamp: Some(100 + id), task_id: 10 + id, event_type: Some(event_type), ..Default::default() }
    }

    fn with_histories(mut state: OwnedNdcMutableState) -> OwnedNdcMutableState {
        state.set_version_histories(VersionHistories {
            current_version_history_index: 0,
            histories: vec![VersionHistory { branch_token: vec![9], items: vec![] }],
        }).unwrap();
        state
    }

    #[test]
    fn typed_builder_replays_start_and_decision_batch() {
        use crate::ndc::history_replicator::NdcStateBuilder;
        use crate::wiring::assembly::ndc_adapters::TypedNdcStateBuilder;
        use cadence_api::service_types::WorkflowExecutionStartedEventAttributes;
        use cadence_api::tasks::{DecisionTaskScheduledEventAttributes, DecisionTaskStartedEventAttributes};
        let mut start = started(EventType::WorkflowExecutionStarted, 1, 7);
        start.workflow_execution_started_event_attributes = Some(WorkflowExecutionStartedEventAttributes { request_id: "r".into(), ..Default::default() });
        let mut sched = started(EventType::DecisionTaskScheduled, 2, 7);
        sched.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes { start_to_close_timeout_seconds: Some(10), ..Default::default() });
        let mut dstart = started(EventType::DecisionTaskStarted, 3, 7);
        dstart.decision_task_started_event_attributes = Some(DecisionTaskStartedEventAttributes { scheduled_event_id: 2, request_id: "d".into(), ..Default::default() });
        let mut st = with_histories(state());
        let exec = WorkflowExecution { workflow_id: "workflow-a".into(), run_id: "run-a".into() };
        let out = TypedNdcStateBuilder.apply_events(&mut st, "domain-a", "req", &exec, &[start, sched, dstart], &[]).unwrap();
        assert!(out.is_none());
        assert_eq!(st.get_next_event_id(), 4);
        assert_eq!(st.typed().execution_info.decision_schedule_id, 2);
        assert_eq!(st.typed().execution_info.decision_started_id, 3);
        assert_eq!(st.get_current_version(), 7);
    }

    #[test]
    fn typed_builder_continued_as_new_returns_new_run_state() {
        use crate::ndc::history_replicator::NdcStateBuilder;
        use crate::wiring::assembly::ndc_adapters::TypedNdcStateBuilder;
        use cadence_api::service_types::WorkflowExecutionStartedEventAttributes;
        use cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes;
        let mut can = started(EventType::WorkflowExecutionContinuedAsNew, 1, 7);
        can.workflow_execution_continued_as_new_event_attributes = Some(WorkflowExecutionContinuedAsNewEventAttributes { new_execution_run_id: "run-b".into(), ..Default::default() });
        let mut new_start = started(EventType::WorkflowExecutionStarted, 1, 7);
        new_start.workflow_execution_started_event_attributes = Some(WorkflowExecutionStartedEventAttributes { continued_execution_run_id: "run-a".into(), request_id: "r".into(), ..Default::default() });
        let mut st = with_histories(state());
        let exec = WorkflowExecution { workflow_id: "workflow-a".into(), run_id: "run-a".into() };
        let out = TypedNdcStateBuilder.apply_events(&mut st, "domain-a", "req", &exec, &[can], &[new_start]).unwrap().unwrap();
        let info = out.get_execution_info();
        assert_eq!((info.workflow_id.as_str(), info.run_id.as_str(), info.domain_id.as_str()), ("workflow-a", "run-b", "domain-a"));
        assert_eq!(out.get_next_event_id(), 2);
        assert_eq!(st.get_workflow_state(), WorkflowState::Completed);
        assert_eq!(st.get_execution_info().run_id, "run-a");
    }
}
