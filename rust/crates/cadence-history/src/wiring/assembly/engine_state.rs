//! group: assembly/engine_state
//!
//! `EngineMutableState`: the engine-local `MutableState` trait implemented over a
//! persisted `WorkflowSnapshot` (Go `execution.mutableStateBuilder` loaded via
//! `Load`). This file holds the state, the loader and the read accessors; the
//! history-event family lives in `engine_events`, task generation in
//! `engine_tasks`, the close transaction in `engine_close` and the decision
//! task manager in `engine_decision`.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::*;
use cadence_persistence::history_contract as pc;
use cadence_persistence::history_reader::HistoryNodeStore;
use cadence_persistence::sqlblobs::{activity_info_from_struct, Encoding};
use cadence_thrift::FromThrift;

use crate::engine::deps::{
    ActivityInfo, DomainCacheEntry, HResult, HistoryError, WorkflowExecutionInfo, EMPTY_EVENT_ID,
};
use crate::engine::deps_ms::{ChildExecutionInfo, MutableState};
use crate::engine::runtime::{QueryRegistry, QueryTermination};
use crate::engine::types::*;
use crate::query::{Registry, TerminationState, TerminationType};
use cadence_api::types::{VersionHistories, WorkflowExecution, WorkflowType};

/// Go `constants.EmptyVersion`.
pub(super) const EMPTY_VERSION: i64 = -24;
/// Go `constants.BufferedEventID`.
pub(super) const BUFFERED_EVENT_ID: i64 = -123;
/// Go `constants.EmptyEventTaskID`.
pub(super) const EMPTY_EVENT_TASK_ID: i64 = -1234;
/// Go `constants.TransientEventID`.
pub(super) const TRANSIENT_EVENT_ID: i64 = -124;

pub(super) fn decode(blob: &pc::DataBlob) -> HResult<cadence_thrift::value::Struct> {
    Encoding::parse(&blob.encoding)
        .and_then(|e| e.decode(&blob.data))
        .map_err(|e| HistoryError::internal(format!("decode mutable state blob: {e}")))
}

/// Config and domain derived values a state needs (Go reads these from the shard
/// and domain cache while building tasks).
#[derive(Clone)]
pub struct StateParams {
    /// `config.NormalDecisionScheduleToStartTimeout(domain)` in nanoseconds.
    pub normal_decision_s2s_timeout_nanos: i64,
    /// `config.NormalDecisionScheduleToStartMaxAttempts(domain)`.
    pub normal_decision_s2s_max_attempts: i64,
    /// Domain workflow retention in days (Go defaults to 1 when the domain is missing).
    pub retention_days: i32,
    /// `config.WorkflowDeletionJitterRange(domain)` in minutes.
    pub deletion_jitter_range: i64,
    /// Domain failover version (`-24` for local domains).
    pub failover_version: i64,
    /// History node store used to read events of the current branch.
    pub history_nodes: Option<Arc<dyn HistoryNodeStore>>,
    pub shard_id: i32,
}

impl Default for StateParams {
    fn default() -> Self {
        Self {
            normal_decision_s2s_timeout_nanos: 300_000_000_000,
            normal_decision_s2s_max_attempts: 0,
            retention_days: 1,
            deletion_jitter_range: 60,
            failover_version: EMPTY_VERSION,
            history_nodes: None,
            shard_id: 0,
        }
    }
}

/// Adapter from `query::Registry` to the engine `QueryRegistry` trait.
pub struct QueryRegistryAdapter(pub Arc<Registry>);

impl QueryRegistry for QueryRegistryAdapter {
    fn buffered_count(&self) -> usize {
        self.0.get_buffered_ids().len()
    }
    fn buffer_query(&self, query: Option<&WorkflowQuery>) -> (String, tokio::sync::oneshot::Receiver<()>) {
        let (id, mut term) = self.0.buffer_query(query.cloned().unwrap_or_default());
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            crate::query::wait_for_termination(term.clone()).await;
            let _ = term.borrow_and_update();
            let _ = tx.send(());
        });
        (id, rx)
    }
    fn remove_query(&self, id: &str) {
        self.0.remove_query(id)
    }
    fn get_termination_state(&self, id: &str) -> HResult<QueryTermination> {
        let state: TerminationState =
            self.0.get_termination_state(id).map_err(|e| HistoryError::Other(e.to_string()))?;
        Ok(match state.termination_type {
            TerminationType::Completed => {
                let r = state.query_result.unwrap_or(crate::query::WorkflowQueryResult {
                    result_type: None,
                    answer: None,
                    error_message: String::new(),
                });
                QueryTermination::Completed(WorkflowQueryResult {
                    result_type: r.result_type,
                    answer: r.answer.unwrap_or_default(),
                    error_message: r.error_message,
                })
            }
            TerminationType::Unblocked => QueryTermination::Unblocked,
            TerminationType::Failed => {
                QueryTermination::Failed(HistoryError::Other(state.failure.unwrap_or_default()))
            }
        })
    }
}

/// Builds the engine view of a persisted activity.
pub(super) fn engine_activity(schedule_id: i64, a: &pc::ActivityInfo, details: Vec<u8>) -> ActivityInfo {
    ActivityInfo {
        schedule_id,
        started_id: a.started_id,
        activity_id: a.activity_id.clone(),
        request_id: a.request_id.clone(),
        details,
        attempt: a.attempt,
        started_time: a.started_time_unix_nanos,
        scheduled_time: a.scheduled_time_unix_nanos,
        schedule_to_close_timeout: a.schedule_to_close_timeout_seconds,
        cancel_requested: a.cancel_requested,
        cancel_request_id: a.cancel_request_id,
        has_retry_policy: a.has_retry_policy,
        version: a.version,
        started_identity: a.started_identity.clone(),
        task_list: a.task_list.clone(),
        last_heart_beat_updated_time: 0,
        expiration_time: a.retry_expiration_time_unix_nanos,
        maximum_attempts: a.retry_maximum_attempts,
        last_failure_reason: a.retry_last_failure_reason.clone(),
        last_failure_details: a.retry_last_failure_details.clone(),
        last_failure_category: None,
        last_retry_interval_seconds: a.retry_last_retry_interval_seconds,
        last_worker_identity: a.retry_last_worker_identity.clone(),
    }
}

pub(super) struct Inner {
    /// Engine view of the execution info (state, close status, sticky, decision ids).
    pub(super) exec: WorkflowExecutionInfo,
    /// Persistence view; the source of truth for the decision_* fields.
    pub(super) pc_exec: pc::WorkflowExecutionInfo,
    pub(super) next_event_id: i64,
    /// Go `nextEventIDInDB`: the CAS condition of the next write.
    pub(super) db_next_event_id: i64,
    pub(super) last_updated_nanos: i64,
    pub(super) last_processed_event: i64,
    pub(super) last_first_event_id: i64,
    pub(super) branch_token: Vec<u8>,
    pub(super) history_size: i64,
    pub(super) activities: BTreeMap<i64, ActivityInfo>,
    pub(super) pc_acts: BTreeMap<i64, pc::ActivityInfo>,
    pub(super) upsert_acts: BTreeSet<i64>,
    pub(super) deleted_activities: HashSet<i64>,
    pub(super) heartbeats: BTreeMap<i64, pc::ActivityHeartbeat>,
    pub(super) children: BTreeMap<i64, ChildExecutionInfo>,
    pub(super) pc_children: BTreeMap<i64, pc::ChildExecutionInfo>,
    pub(super) upsert_children: BTreeSet<i64>,
    pub(super) deleted_children: BTreeSet<i64>,
    pub(super) signal_requested: HashSet<String>,
    pub(super) upsert_signal_requested: BTreeSet<String>,
    pub(super) delete_signal_requested: BTreeSet<String>,
    pub(super) version_histories: Option<VersionHistories>,
    pub(super) current_version: i64,
    /// New events of this transaction (Go `hBuilder.history`).
    pub(super) hist: Vec<HistoryEvent>,
    /// Persisted buffered events (Go `bufferedEvents`).
    pub(super) buffered: Vec<HistoryEvent>,
    /// Buffered events created in this transaction (Go `updateBufferedEvents`).
    pub(super) update_buffered: Vec<HistoryEvent>,
    pub(super) has_buffered_in_db: bool,
    pub(super) clear_buffered: bool,
    pub(super) transfer_tasks: Vec<pc::TransferTask>,
    pub(super) timer_tasks: Vec<pc::TimerTask>,
    pub(super) dirty: bool,
}

impl Inner {
    /// Copies the decision ids from the persistence view into the engine view.
    pub(super) fn mirror_decision(&mut self) {
        self.exec.decision_schedule_id = self.pc_exec.decision_schedule_id;
        self.exec.decision_started_id = self.pc_exec.decision_started_id;
        self.exec.decision_attempt = self.pc_exec.decision_attempt;
    }

    /// Go `HasInFlightDecision`.
    pub(super) fn in_flight(&self) -> bool {
        self.pc_exec.decision_started_id != EMPTY_EVENT_ID
    }

    pub(super) fn has_buffered(&self) -> bool {
        !self.buffered.is_empty() || !self.update_buffered.is_empty() || self.hist.iter().any(|e| e.event_id == BUFFERED_EVENT_ID)
    }

    /// Re-derives the engine view of one activity from the persistence view.
    pub(super) fn refresh_activity(&mut self, schedule_id: i64) {
        if let Some(a) = self.pc_acts.get(&schedule_id) {
            let details = self.heartbeats.get(&schedule_id).map(|h| h.details.clone()).unwrap_or_default();
            self.activities.insert(schedule_id, engine_activity(schedule_id, a, details));
            self.upsert_acts.insert(schedule_id);
            self.dirty = true;
        }
    }

    /// Go `DeleteActivity`.
    pub(super) fn remove_activity(&mut self, schedule_id: i64) -> HResult<()> {
        if self.activities.remove(&schedule_id).is_none() {
            return Err(HistoryError::internal(format!(
                "unable to find activity with schedule event id: {schedule_id} in mutable state"
            )));
        }
        self.pc_acts.remove(&schedule_id);
        self.heartbeats.remove(&schedule_id);
        self.upsert_acts.remove(&schedule_id);
        self.deleted_activities.insert(schedule_id);
        self.dirty = true;
        Ok(())
    }

    /// Go `DeletePendingChildExecution`.
    pub(super) fn remove_child(&mut self, initiated_id: i64) -> HResult<()> {
        if self.children.remove(&initiated_id).is_none() {
            return Err(HistoryError::internal(format!(
                "unable to find child workflow with initiated id: {initiated_id} in mutable state"
            )));
        }
        self.pc_children.remove(&initiated_id);
        self.upsert_children.remove(&initiated_id);
        self.deleted_children.insert(initiated_id);
        self.dirty = true;
        Ok(())
    }
}

pub struct EngineMutableState {
    pub(super) inner: Mutex<Inner>,
    pub(super) domain: DomainCacheEntry,
    pub(super) queries: Arc<QueryRegistryAdapter>,
    pub(super) snapshot: pc::WorkflowSnapshot,
    pub(super) sticky_ttl_nanos: i64,
    pub(super) now_nanos: Arc<dyn Fn() -> i64 + Send + Sync>,
    pub(super) params: StateParams,
}

fn decode_events(blob: &pc::DataBlob) -> HResult<Vec<HistoryEvent>> {
    cadence_thrift::history_blob::decode(&cadence_api::types::DataBlob {
        encoding_type: Some(cadence_api::enums::EncodingType::ThriftRW),
        data: blob.data.clone(),
    })
    .map_err(|e| HistoryError::internal(format!("decode buffered events: {e}")))
}

impl EngineMutableState {
    /// Go `mutableStateBuilder.Load`: builds the state from a persisted snapshot.
    pub fn from_snapshot(
        snapshot: pc::WorkflowSnapshot,
        domain: DomainCacheEntry,
        sticky_ttl_nanos: i64,
        now_nanos: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> HResult<Self> {
        let i = &snapshot.execution_info;
        let auto_reset_points = match &i.auto_reset_points {
            Some(b) if !b.data.is_empty() => Some(
                cadence_api::types::ResetPoints::from_thrift(&decode(b)?)
                    .map_err(|e| HistoryError::internal(format!("decode auto reset points: {e}")))?,
            ),
            _ => None,
        };
        let active_cluster_selection_policy = match &i.active_cluster_selection_policy {
            Some(b) if !b.data.is_empty() => Some(
                cadence_api::types::ActiveClusterSelectionPolicy::from_thrift(&decode(b)?)
                    .map_err(|e| HistoryError::internal(format!("decode active cluster selection policy: {e}")))?,
            ),
            _ => None,
        };
        let exec = WorkflowExecutionInfo {
            domain_id: i.domain_id.clone(),
            workflow_id: i.workflow_id.clone(),
            run_id: i.run_id.clone(),
            first_execution_run_id: i.first_execution_run_id.clone(),
            create_request_id: i.create_request_id.clone(),
            active_cluster_selection_policy,
            parent_workflow_id: i.parent_workflow_id.clone(),
            parent_run_id: i.parent_run_id.clone(),
            workflow_type_name: i.workflow_type_name.clone(),
            state: i.state,
            close_status: i.close_status,
            signal_count: i.signal_count,
            start_timestamp: i.start_time_unix_nanos,
            task_list: i.task_list.clone(),
            sticky_task_list: i.sticky_task_list.clone(),
            decision_schedule_id: i.decision_schedule_id,
            decision_started_id: i.decision_started_id,
            decision_attempt: i.decision_attempt,
            cron_schedule: i.cron_schedule.clone(),
            has_retry_policy: i.has_retry_policy,
            attempt: i.attempt,
            client_library_version: i.client_library_version.clone(),
            client_feature_version: i.client_feature_version.clone(),
            client_impl: i.client_impl.clone(),
            sticky_schedule_to_start_timeout: (i.sticky_schedule_to_start_timeout_nanos / 1_000_000_000) as i32,
            auto_reset_points,
            workflow_timeout: (i.workflow_timeout_nanos / 1_000_000_000) as i32,
            decision_start_to_close_timeout: (i.decision_start_to_close_timeout_nanos / 1_000_000_000) as i32,
            task_list_kind: None,
            memo: Some(i.memo.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            search_attributes: Some(i.search_attributes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            partition_config: Some(i.partition_config.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            cron_overlap_policy: None,
            last_updated_timestamp: i.last_updated_time_unix_nanos,
            parent_domain_id: i.parent_domain_id.clone(),
            initiated_id: i.initiated_id,
        };
        let mut activities = BTreeMap::new();
        let mut pc_acts = BTreeMap::new();
        for kb in &snapshot.activity_infos {
            let a = activity_info_from_struct(&decode(&kb.value)?)
                .map_err(|e| HistoryError::internal(format!("decode activity info: {e}")))?;
            let details = snapshot.activity_heartbeats.get(&kb.key).map(|h| h.details.clone()).unwrap_or_default();
            activities.insert(kb.key, engine_activity(kb.key, &a, details));
            pc_acts.insert(kb.key, a);
        }
        let mut children = BTreeMap::new();
        let mut pc_children = BTreeMap::new();
        for kb in &snapshot.child_execution_infos {
            let c = cadence_persistence::sqlblobs::child_execution_info_from_struct(&decode(&kb.value)?)
                .map_err(|e| HistoryError::internal(format!("decode child execution info: {e}")))?;
            children.insert(
                kb.key,
                ChildExecutionInfo { started_id: c.started_id, started_workflow_id: c.started_workflow_id.clone() },
            );
            pc_children.insert(kb.key, c);
        }
        let version_histories = match &snapshot.version_histories {
            Some(b) => Some(
                crate::wiring::server::thrift_types_g2::version_histories_from_thrift(&decode(b)?)
                    .map_err(|e| HistoryError::internal(format!("decode version histories: {e}")))?,
            ),
            None => None,
        };
        // Go Load: currentVersion is the last item version of the current history.
        let current_version = version_histories
            .as_ref()
            .and_then(|vh| vh.histories.get(vh.current_version_history_index as usize))
            .and_then(|h| h.items.last())
            .map(|item| item.version)
            .unwrap_or(EMPTY_VERSION);
        let mut buffered = Vec::new();
        for blob in &snapshot.buffered_events {
            buffered.extend(decode_events(blob)?);
        }
        let inner = Inner {
            exec,
            pc_exec: i.clone(),
            next_event_id: i.next_event_id,
            db_next_event_id: i.next_event_id,
            last_updated_nanos: i.last_updated_time_unix_nanos,
            last_processed_event: i.last_processed_event,
            last_first_event_id: i.last_first_event_id,
            branch_token: i.branch_token.clone(),
            history_size: i.history_size,
            activities,
            pc_acts,
            upsert_acts: BTreeSet::new(),
            deleted_activities: HashSet::new(),
            heartbeats: snapshot.activity_heartbeats.clone(),
            children,
            pc_children,
            upsert_children: BTreeSet::new(),
            deleted_children: BTreeSet::new(),
            signal_requested: snapshot.signal_requested_ids.iter().cloned().collect(),
            upsert_signal_requested: BTreeSet::new(),
            delete_signal_requested: BTreeSet::new(),
            version_histories,
            current_version,
            hist: Vec::new(),
            has_buffered_in_db: !buffered.is_empty(),
            buffered,
            update_buffered: Vec::new(),
            clear_buffered: false,
            transfer_tasks: Vec::new(),
            timer_tasks: Vec::new(),
            dirty: false,
        };
        Ok(Self {
            inner: Mutex::new(inner),
            domain,
            queries: Arc::new(QueryRegistryAdapter(Arc::new(Registry::new()))),
            snapshot,
            sticky_ttl_nanos,
            now_nanos,
            params: StateParams::default(),
        })
    }

    /// Sets the config / domain derived parameters.
    pub fn with_params(mut self, params: StateParams) -> Self {
        self.params = params;
        self
    }

    /// True when an in-memory mutation has not been persisted.
    pub fn is_dirty(&self) -> bool {
        self.inner.lock().unwrap().dirty
    }

    /// The persisted snapshot this state was loaded from.
    pub fn snapshot(&self) -> &pc::WorkflowSnapshot {
        &self.snapshot
    }

    pub(super) fn now(&self) -> i64 {
        (self.now_nanos)()
    }

    pub(super) fn now_millis(&self) -> i64 {
        self.now() / 1_000_000
    }
}

#[async_trait]
impl MutableState for EngineMutableState {
    fn get_last_write_version(&self) -> HResult<i64> {
        Ok(self.inner.lock().unwrap().pc_exec.last_write_version)
    }
    fn is_resource_duplicated(&self, _dedup_resource_id: &str) -> bool {
        false
    }
    fn get_pending_activity_infos(&self) -> Vec<ActivityInfo> {
        self.inner.lock().unwrap().activities.values().cloned().collect()
    }
    fn get_pending_child_execution_infos(&self) -> Vec<crate::engine::deps_ms::PendingChildInfo> {
        self.inner
            .lock()
            .unwrap()
            .pc_children
            .iter()
            .map(|(id, c)| crate::engine::deps_ms::PendingChildInfo {
                domain_id: c.domain_id.clone(),
                domain_name: c.domain_name.clone(),
                started_workflow_id: c.started_workflow_id.clone(),
                started_run_id: String::from_utf8_lossy(&c.started_run_id).to_string(),
                workflow_type_name: c.workflow_type_name.clone(),
                initiated_id: *id,
                parent_close_policy: None,
            })
            .collect()
    }
    fn get_pending_decision(&self) -> Option<crate::mutable_state::DecisionInfo> {
        let g = self.inner.lock().unwrap();
        let e = &g.pc_exec;
        if e.decision_schedule_id == EMPTY_EVENT_ID {
            return None;
        }
        Some(crate::mutable_state::DecisionInfo {
            schedule_id: e.decision_schedule_id,
            started_id: e.decision_started_id,
            version: e.decision_version,
            request_id: e.decision_request_id.clone(),
            task_list: e.task_list.clone(),
            start_to_close_timeout_seconds: (e.decision_timeout_nanos / 1_000_000_000) as i32,
            started_timestamp: e.decision_started_time_unix_nanos,
            attempt: e.decision_attempt,
            schedule_timestamp: e.decision_scheduled_time_unix_nanos,
            original_schedule_timestamp: e.decision_original_scheduled_time_unix_nanos,
        })
    }
    async fn get_completion_event(&self) -> HResult<cadence_api::service_types::HistoryEvent> {
        Err(HistoryError::internal("get_completion_event: completion event blob decoding not wired"))
    }
    fn is_workflow_execution_running(&self) -> bool {
        self.inner.lock().unwrap().exec.is_running()
    }
    fn get_next_event_id(&self) -> i64 {
        self.inner.lock().unwrap().next_event_id
    }
    fn get_execution_info(&self) -> WorkflowExecutionInfo {
        self.inner.lock().unwrap().exec.clone()
    }
    fn get_domain_entry(&self) -> DomainCacheEntry {
        self.domain.clone()
    }
    fn get_activity_info(&self, schedule_id: i64) -> Option<ActivityInfo> {
        self.inner.lock().unwrap().activities.get(&schedule_id).cloned()
    }
    fn get_activity_by_activity_id(&self, activity_id: &str) -> Option<ActivityInfo> {
        self.inner.lock().unwrap().activities.values().find(|a| a.activity_id == activity_id).cloned()
    }
    fn has_pending_decision(&self) -> bool {
        self.inner.lock().unwrap().exec.decision_schedule_id != EMPTY_EVENT_ID
    }
    fn has_in_flight_decision(&self) -> bool {
        self.inner.lock().unwrap().exec.decision_started_id > 0
    }
    fn get_query_registry(&self) -> Arc<dyn QueryRegistry> {
        self.queries.clone()
    }
    fn get_child_execution_info(&self, initiated_id: i64) -> Option<ChildExecutionInfo> {
        self.inner.lock().unwrap().children.get(&initiated_id).cloned()
    }
    fn add_child_workflow_execution_completed_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&WorkflowExecutionCompletedEventAttributes>) -> HResult<()> {
        self.ev_child_completed(initiated_id, execution, attrs)
    }
    fn add_child_workflow_execution_failed_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&WorkflowExecutionFailedEventAttributes>) -> HResult<()> {
        self.ev_child_failed(initiated_id, execution, attrs)
    }
    fn add_child_workflow_execution_canceled_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&WorkflowExecutionCanceledEventAttributes>) -> HResult<()> {
        self.ev_child_canceled(initiated_id, execution, attrs)
    }
    fn add_child_workflow_execution_terminated_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes>) -> HResult<()> {
        self.ev_child_terminated(initiated_id, execution, attrs)
    }
    fn add_child_workflow_execution_timed_out_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&WorkflowExecutionTimedOutEventAttributes>) -> HResult<()> {
        self.ev_child_timed_out(initiated_id, execution, attrs)
    }
    fn get_current_version(&self) -> i64 {
        self.inner.lock().unwrap().current_version
    }
    fn get_last_first_event_id(&self) -> i64 {
        self.inner.lock().unwrap().last_first_event_id
    }
    fn get_previous_started_event_id(&self) -> i64 {
        self.inner.lock().unwrap().last_processed_event
    }
    fn get_current_branch_token(&self) -> HResult<Vec<u8>> {
        let g = self.inner.lock().unwrap();
        match &g.version_histories {
            Some(vh) => vh
                .histories
                .get(vh.current_version_history_index as usize)
                .map(|h| h.branch_token.clone())
                .ok_or_else(|| HistoryError::Other("current version history index out of range".into())),
            None => Ok(g.branch_token.clone()),
        }
    }
    fn get_version_histories(&self) -> Option<VersionHistories> {
        self.inner.lock().unwrap().version_histories.clone()
    }
    fn is_sticky_task_list_enabled(&self) -> bool {
        let g = self.inner.lock().unwrap();
        if g.exec.sticky_task_list.is_empty() {
            return false;
        }
        // !now.After(lastUpdated + ttl)
        (self.now_nanos)() <= g.last_updated_nanos.saturating_add(self.sticky_ttl_nanos)
    }
    fn get_history_size(&self) -> i64 {
        self.inner.lock().unwrap().history_size
    }
    fn get_workflow_type(&self) -> Option<WorkflowType> {
        Some(WorkflowType { name: self.inner.lock().unwrap().exec.workflow_type_name.clone() })
    }
    async fn get_resurrected_activities(&self) -> HResult<HashSet<i64>> {
        self.ev_resurrected_activities().await
    }
    fn delete_activity(&self, schedule_id: i64) -> HResult<()> {
        self.inner.lock().unwrap().remove_activity(schedule_id)
    }
    async fn get_activity_scheduled_event(&self, schedule_id: i64) -> HResult<HistoryEvent> {
        self.ev_activity_scheduled_event(schedule_id).await
    }
    fn add_activity_task_started_event(&self, info: &ActivityInfo, schedule_id: i64, request_id: &str, identity: &str) -> HResult<ActivityInfo> {
        self.ev_activity_started(info, schedule_id, request_id, identity)
    }
    fn is_signal_requested(&self, request_id: &str) -> bool {
        self.inner.lock().unwrap().signal_requested.contains(request_id)
    }
    fn add_signal_requested(&self, request_id: &str) {
        let mut g = self.inner.lock().unwrap();
        g.signal_requested.insert(request_id.to_string());
        g.delete_signal_requested.remove(request_id);
        g.upsert_signal_requested.insert(request_id.to_string());
        g.dirty = true;
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        let g = self.inner.lock().unwrap();
        g.exec.decision_schedule_id != EMPTY_EVENT_ID || g.last_processed_event != EMPTY_EVENT_ID
    }
    fn set_workflow_state(&self, state: i32) {
        let mut g = self.inner.lock().unwrap();
        g.exec.state = state;
        g.dirty = true;
    }
    fn copy_to_persistence(&self) -> HResult<serde_json::Value> {
        self.ev_copy_to_persistence()
    }
    fn add_workflow_execution_signaled(&self, name: &str, input: &[u8], identity: &str, request_id: &str) -> HResult<()> {
        self.ev_signaled(name, input, identity, request_id)
    }
    fn delete_signal_requested(&self, request_id: &str) {
        let mut g = self.inner.lock().unwrap();
        g.signal_requested.remove(request_id);
        g.upsert_signal_requested.remove(request_id);
        g.delete_signal_requested.insert(request_id.to_string());
        g.dirty = true;
    }
    fn clear_stickyness(&self) {
        self.inner.lock().unwrap().clear_stickyness();
    }
    fn is_cancel_requested(&self) -> (bool, String) {
        let g = self.inner.lock().unwrap();
        (g.pc_exec.cancel_requested, g.pc_exec.cancel_request_id.clone())
    }
    async fn get_start_event(&self) -> HResult<HistoryEvent> {
        self.ev_start_event().await
    }
    fn add_activity_task_completed_event(&self, schedule_id: i64, started_id: i64, request: &RespondActivityTaskCompletedRequest) -> HResult<()> {
        self.ev_activity_completed(schedule_id, started_id, request)
    }
    fn add_activity_task_failed_event(&self, schedule_id: i64, started_id: i64, request: &RespondActivityTaskFailedRequest) -> HResult<()> {
        self.ev_activity_failed(schedule_id, started_id, request)
    }
    fn add_activity_task_canceled_event(&self, schedule_id: i64, started_id: i64, cancel_request_id: i64, details: &[u8], identity: &str) -> HResult<()> {
        self.ev_activity_canceled(schedule_id, started_id, cancel_request_id, details, identity)
    }
    fn retry_activity(&self, info: &ActivityInfo, reason: &str, details: &[u8], options: Option<&cadence_api::types::FailureOptions>) -> HResult<bool> {
        self.ev_retry_activity(info, reason, details, options)
    }
    fn update_activity_progress(&self, info: &ActivityInfo, request: &RecordActivityTaskHeartbeatRequest) {
        let now = self.now();
        let mut g = self.inner.lock().unwrap();
        if g.activities.contains_key(&info.schedule_id) {
            g.heartbeats.insert(
                info.schedule_id,
                pc::ActivityHeartbeat { details: request.details.clone(), updated_time_unix_nanos: now },
            );
            g.refresh_activity(info.schedule_id);
        }
    }
    fn add_workflow_execution_cancel_requested_event(&self, cause: &str, request: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()> {
        self.ev_cancel_requested(cause, request)
    }
    fn terminate_workflow(&self, first_event_id: i64, reason: &str, details: &[u8], identity: &str) -> HResult<()> {
        self.ev_terminate(first_event_id, reason, details, identity)
    }
}

impl Inner {
    /// Go `ClearStickyness`.
    pub(super) fn clear_stickyness(&mut self) {
        self.exec.sticky_task_list.clear();
        self.exec.sticky_schedule_to_start_timeout = 0;
        self.exec.client_library_version.clear();
        self.exec.client_feature_version.clear();
        self.exec.client_impl.clear();
        self.dirty = true;
    }
}
