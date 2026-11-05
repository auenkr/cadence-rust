//! group: assembly/engine_state
//!
//! `EngineMutableState`: the engine-local `MutableState` trait implemented over a
//! persisted `WorkflowSnapshot` (Go `execution.mutableStateBuilder` loaded via
//! `Load`). Read accessors and the in-memory bookkeeping mutators are ported.
//! Every method that appends a history event (the Go `AddXxxEvent` family and
//! `RetryActivity`/`TerminateWorkflow`) needs the history builder + task
//! generator over this state and is NOT ported yet: it returns an explicit
//! internal error and never silently succeeds. See memory "Unit D5".

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_api::tasks::*;
use cadence_persistence::history_contract as pc;
use cadence_persistence::sqlblobs::{activity_info_from_struct, Encoding};

use crate::engine::deps::{
    ActivityInfo, DomainCacheEntry, HResult, HistoryError, WorkflowExecutionInfo, EMPTY_EVENT_ID,
};
use crate::engine::deps_ms::{ChildExecutionInfo, MutableState};
use crate::engine::runtime::{QueryRegistry, QueryTermination};
use crate::engine::types::*;
use crate::query::{Registry, TerminationState, TerminationType};
use cadence_api::types::{VersionHistories, WorkflowExecution, WorkflowType};

/// Go `constants.EmptyVersion`.
const EMPTY_VERSION: i64 = -24;

fn not_ported<T>(what: &str) -> HResult<T> {
    Err(HistoryError::internal(format!(
        "mutable state operation {what} is not ported yet (history builder / task generator over EngineMutableState)"
    )))
}

fn decode(blob: &pc::DataBlob) -> HResult<cadence_thrift::value::Struct> {
    Encoding::parse(&blob.encoding)
        .and_then(|e| e.decode(&blob.data))
        .map_err(|e| HistoryError::internal(format!("decode mutable state blob: {e}")))
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
                    ..Default::default()
                })
            }
            TerminationType::Unblocked => QueryTermination::Unblocked,
            TerminationType::Failed => {
                QueryTermination::Failed(HistoryError::Other(state.failure.unwrap_or_default()))
            }
        })
    }
}

struct Inner {
    exec: WorkflowExecutionInfo,
    last_updated_nanos: i64,
    last_processed_event: i64,
    last_first_event_id: i64,
    branch_token: Vec<u8>,
    history_size: i64,
    activities: BTreeMap<i64, ActivityInfo>,
    deleted_activities: HashSet<i64>,
    children: BTreeMap<i64, ChildExecutionInfo>,
    signal_requested: HashSet<String>,
    version_histories: Option<VersionHistories>,
    current_version: i64,
    dirty: bool,
}

pub struct EngineMutableState {
    inner: Mutex<Inner>,
    domain: DomainCacheEntry,
    queries: Arc<QueryRegistryAdapter>,
    snapshot: pc::WorkflowSnapshot,
    sticky_ttl_nanos: i64,
    now_nanos: Arc<dyn Fn() -> i64 + Send + Sync>,
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
        let exec = WorkflowExecutionInfo {
            domain_id: i.domain_id.clone(),
            workflow_id: i.workflow_id.clone(),
            run_id: i.run_id.clone(),
            first_execution_run_id: i.first_execution_run_id.clone(),
            create_request_id: i.create_request_id.clone(),
            active_cluster_selection_policy: None,
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
            auto_reset_points: None,
        };
        let mut activities = BTreeMap::new();
        for kb in &snapshot.activity_infos {
            let a = activity_info_from_struct(&decode(&kb.value)?)
                .map_err(|e| HistoryError::internal(format!("decode activity info: {e}")))?;
            let details = snapshot.activity_heartbeats.get(&kb.key).map(|h| h.details.clone()).unwrap_or_default();
            activities.insert(
                kb.key,
                ActivityInfo {
                    schedule_id: kb.key,
                    started_id: a.started_id,
                    activity_id: a.activity_id,
                    request_id: a.request_id,
                    details,
                    attempt: a.attempt,
                    started_time: a.started_time_unix_nanos,
                    scheduled_time: a.scheduled_time_unix_nanos,
                    schedule_to_close_timeout: a.schedule_to_close_timeout_seconds,
                    cancel_requested: a.cancel_requested,
                    cancel_request_id: a.cancel_request_id,
                    has_retry_policy: a.has_retry_policy,
                    version: a.version,
                    started_identity: a.started_identity,
                    task_list: a.task_list,
                },
            );
        }
        let mut children = BTreeMap::new();
        for kb in &snapshot.child_execution_infos {
            let c = cadence_persistence::sqlblobs::child_execution_info_from_struct(&decode(&kb.value)?)
                .map_err(|e| HistoryError::internal(format!("decode child execution info: {e}")))?;
            children.insert(
                kb.key,
                ChildExecutionInfo { started_id: c.started_id, started_workflow_id: c.started_workflow_id },
            );
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
        let inner = Inner {
            exec,
            last_updated_nanos: i.last_updated_time_unix_nanos,
            last_processed_event: i.last_processed_event,
            last_first_event_id: i.last_first_event_id,
            branch_token: i.branch_token.clone(),
            history_size: i.history_size,
            activities,
            deleted_activities: HashSet::new(),
            children,
            signal_requested: snapshot.signal_requested_ids.iter().cloned().collect(),
            version_histories,
            current_version,
            dirty: false,
        };
        Ok(Self {
            inner: Mutex::new(inner),
            domain,
            queries: Arc::new(QueryRegistryAdapter(Arc::new(Registry::new()))),
            snapshot,
            sticky_ttl_nanos,
            now_nanos,
        })
    }

    /// True when an in-memory mutation has not been persisted.
    pub fn is_dirty(&self) -> bool {
        self.inner.lock().unwrap().dirty
    }

    /// The persisted snapshot this state was loaded from.
    pub fn snapshot(&self) -> &pc::WorkflowSnapshot {
        &self.snapshot
    }
}

#[async_trait]
impl MutableState for EngineMutableState {
    fn is_workflow_execution_running(&self) -> bool {
        self.inner.lock().unwrap().exec.is_running()
    }
    fn get_next_event_id(&self) -> i64 {
        self.snapshot.execution_info.next_event_id
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
    fn add_child_workflow_execution_completed_event(&self, _: i64, _: &WorkflowExecution, _: Option<&WorkflowExecutionCompletedEventAttributes>) -> HResult<()> {
        not_ported("AddChildWorkflowExecutionCompletedEvent")
    }
    fn add_child_workflow_execution_failed_event(&self, _: i64, _: &WorkflowExecution, _: Option<&WorkflowExecutionFailedEventAttributes>) -> HResult<()> {
        not_ported("AddChildWorkflowExecutionFailedEvent")
    }
    fn add_child_workflow_execution_canceled_event(&self, _: i64, _: &WorkflowExecution, _: Option<&WorkflowExecutionCanceledEventAttributes>) -> HResult<()> {
        not_ported("AddChildWorkflowExecutionCanceledEvent")
    }
    fn add_child_workflow_execution_terminated_event(&self, _: i64, _: &WorkflowExecution, _: Option<&cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes>) -> HResult<()> {
        not_ported("AddChildWorkflowExecutionTerminatedEvent")
    }
    fn add_child_workflow_execution_timed_out_event(&self, _: i64, _: &WorkflowExecution, _: Option<&WorkflowExecutionTimedOutEventAttributes>) -> HResult<()> {
        not_ported("AddChildWorkflowExecutionTimedOutEvent")
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
        not_ported("GetResurrectedActivities")
    }
    fn delete_activity(&self, schedule_id: i64) -> HResult<()> {
        let mut g = self.inner.lock().unwrap();
        if g.activities.remove(&schedule_id).is_none() {
            return Err(HistoryError::internal(format!(
                "unable to find activity with schedule event id: {schedule_id} in mutable state"
            )));
        }
        g.deleted_activities.insert(schedule_id);
        g.dirty = true;
        Ok(())
    }
    async fn get_activity_scheduled_event(&self, _schedule_id: i64) -> HResult<cadence_api::service_types::HistoryEvent> {
        not_ported("GetActivityScheduledEvent")
    }
    fn add_activity_task_started_event(&self, _: &ActivityInfo, _: i64, _: &str, _: &str) -> HResult<ActivityInfo> {
        not_ported("AddActivityTaskStartedEvent")
    }
    fn is_signal_requested(&self, request_id: &str) -> bool {
        self.inner.lock().unwrap().signal_requested.contains(request_id)
    }
    fn add_signal_requested(&self, request_id: &str) {
        let mut g = self.inner.lock().unwrap();
        g.signal_requested.insert(request_id.to_string());
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
        not_ported("CopyToPersistence")
    }
    fn add_workflow_execution_signaled(&self, _: &str, _: &[u8], _: &str, _: &str) -> HResult<()> {
        not_ported("AddWorkflowExecutionSignaled")
    }
    fn delete_signal_requested(&self, request_id: &str) {
        let mut g = self.inner.lock().unwrap();
        g.signal_requested.remove(request_id);
        g.dirty = true;
    }
    fn clear_stickyness(&self) {
        let mut g = self.inner.lock().unwrap();
        g.exec.sticky_task_list.clear();
        g.exec.sticky_schedule_to_start_timeout = 0;
        g.exec.client_library_version.clear();
        g.exec.client_feature_version.clear();
        g.exec.client_impl.clear();
        g.dirty = true;
    }
    fn is_cancel_requested(&self) -> (bool, String) {
        let i = &self.snapshot.execution_info;
        (i.cancel_requested, i.cancel_request_id.clone())
    }
    async fn get_start_event(&self) -> HResult<cadence_api::service_types::HistoryEvent> {
        not_ported("GetStartEvent")
    }
    fn add_activity_task_completed_event(&self, _: i64, _: i64, _: &RespondActivityTaskCompletedRequest) -> HResult<()> {
        not_ported("AddActivityTaskCompletedEvent")
    }
    fn add_activity_task_failed_event(&self, _: i64, _: i64, _: &RespondActivityTaskFailedRequest) -> HResult<()> {
        not_ported("AddActivityTaskFailedEvent")
    }
    fn add_activity_task_canceled_event(&self, _: i64, _: i64, _: i64, _: &[u8], _: &str) -> HResult<()> {
        not_ported("AddActivityTaskCanceledEvent")
    }
    fn retry_activity(&self, _: &ActivityInfo, _: &str, _: &[u8], _: Option<&cadence_api::types::FailureOptions>) -> HResult<bool> {
        not_ported("RetryActivity")
    }
    fn update_activity_progress(&self, info: &ActivityInfo, request: &RecordActivityTaskHeartbeatRequest) {
        let mut g = self.inner.lock().unwrap();
        if let Some(a) = g.activities.get_mut(&info.schedule_id) {
            a.details = request.details.clone();
            g.dirty = true;
        }
    }
    fn add_workflow_execution_cancel_requested_event(&self, _: &str, _: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()> {
        not_ported("AddWorkflowExecutionCancelRequestedEvent")
    }
    fn terminate_workflow(&self, _: i64, _: &str, _: &[u8], _: &str) -> HResult<()> {
        not_ported("TerminateWorkflow")
    }
}
