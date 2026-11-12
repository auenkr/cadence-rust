//! Test-only fakes for the engine traits (stand in for the gomock mocks used by
//! `go/service/history/engine/engineimpl/*_test.go`).
#![allow(dead_code, clippy::type_complexity)]
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_api::{admin::*, types::*};

use super::{deps::*, runtime::*, types::*};
use super::deps::{ActivityInfo, DomainCacheEntry, DomainInfo, WorkflowExecutionInfo};
use super::deps_ms::{ChildExecutionInfo, PendingChildInfo};
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::WorkflowExecution;

pub type EventFn = Arc<dyn Fn() -> HResult<HistoryEvent> + Send + Sync>;

pub fn internal(msg: &str) -> HistoryError {
    HistoryError::internal(msg)
}

pub fn ok_event(ev: HistoryEvent) -> EventFn {
    Arc::new(move || Ok(ev.clone()))
}

pub fn err_event(msg: &'static str) -> EventFn {
    Arc::new(move || Err(HistoryError::internal(msg)))
}

pub fn domain_entry(id: &str, name: &str) -> DomainCacheEntry {
    DomainCacheEntry {
        info: DomainInfo { id: id.to_string(), name: name.to_string(), status: DOMAIN_STATUS_REGISTERED },
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// FakeMs
// ---------------------------------------------------------------------------

/// Scriptable `execution.MutableState` (Go `MockMutableState`). Unscripted methods panic.
pub struct FakeMs {
    pub running: Mutex<bool>,
    pub info: Mutex<WorkflowExecutionInfo>,
    pub domain: DomainCacheEntry,
    pub next_event_id: i64,
    pub start_event: EventFn,
    pub completion_event: EventFn,
    pub activity_scheduled_event: Arc<dyn Fn(i64) -> HResult<HistoryEvent> + Send + Sync>,
    pub pending_activities: Vec<ActivityInfo>,
    pub pending_children: Vec<PendingChildInfo>,
    pub pending_decision: Option<crate::mutable_state::DecisionInfo>,
    pub signal_requested: Mutex<HashSet<String>>,
    pub processed_or_pending_decision: bool,
    pub cancel_requested: (bool, String),
    pub branch_token: Vec<u8>,
    pub current_version: i64,
    pub persistence_json: serde_json::Value,
    pub terminate_err: Mutex<Option<String>>,
    pub signal_err: Mutex<Option<String>>,
    pub last_write_version: i64,
    pub duplicated_resources: Mutex<HashSet<String>>,
    pub previous_started_event_id: i64,
    pub last_first_event_id: i64,
    pub version_histories: Option<VersionHistories>,
    pub in_flight_decision: bool,
    /// Recorded mutating calls, e.g. `terminate:reason`, `clear_stickyness`, `signal:name`.
    pub calls: Mutex<Vec<String>>,
}

impl Default for FakeMs {
    fn default() -> Self {
        Self {
            running: Mutex::new(true),
            info: Mutex::new(WorkflowExecutionInfo::default()),
            domain: DomainCacheEntry::default(),
            next_event_id: 10,
            start_event: ok_event(HistoryEvent::default()),
            completion_event: ok_event(HistoryEvent::default()),
            activity_scheduled_event: Arc::new(|_| Ok(HistoryEvent::default())),
            pending_activities: Vec::new(),
            pending_children: Vec::new(),
            pending_decision: None,
            signal_requested: Mutex::new(HashSet::new()),
            processed_or_pending_decision: true,
            cancel_requested: (false, String::new()),
            branch_token: Vec::new(),
            current_version: 0,
            persistence_json: serde_json::Value::Null,
            terminate_err: Mutex::new(None),
            signal_err: Mutex::new(None),
            last_write_version: 0,
            duplicated_resources: Mutex::new(HashSet::new()),
            previous_started_event_id: 0,
            last_first_event_id: 1,
            version_histories: None,
            in_flight_decision: false,
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl FakeMs {
    pub fn with_info(info: WorkflowExecutionInfo) -> Self {
        Self { info: Mutex::new(info), ..Default::default() }
    }
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn rec(&self, s: impl Into<String>) {
        self.calls.lock().unwrap().push(s.into());
    }
}

#[async_trait]
impl MutableState for FakeMs {
    fn is_workflow_execution_running(&self) -> bool {
        *self.running.lock().unwrap()
    }
    fn get_next_event_id(&self) -> i64 {
        self.next_event_id
    }
    fn get_execution_info(&self) -> WorkflowExecutionInfo {
        self.info.lock().unwrap().clone()
    }
    fn get_domain_entry(&self) -> DomainCacheEntry {
        self.domain.clone()
    }
    fn get_activity_info(&self, schedule_id: i64) -> Option<ActivityInfo> {
        self.pending_activities.iter().find(|a| a.schedule_id == schedule_id).cloned()
    }
    fn get_activity_by_activity_id(&self, activity_id: &str) -> Option<ActivityInfo> {
        self.pending_activities.iter().find(|a| a.activity_id == activity_id).cloned()
    }
    fn has_pending_decision(&self) -> bool {
        self.pending_decision.is_some()
    }
    fn has_in_flight_decision(&self) -> bool {
        self.in_flight_decision
    }
    fn get_query_registry(&self) -> Arc<dyn QueryRegistry> {
        unimplemented!()
    }
    fn get_child_execution_info(&self, _initiated_id: i64) -> Option<ChildExecutionInfo> {
        unimplemented!()
    }
    fn add_child_workflow_execution_completed_event(&self, _: i64, _: &WorkflowExecution, _: Option<&cadence_api::tasks::WorkflowExecutionCompletedEventAttributes>) -> HResult<()> {
        unimplemented!()
    }
    fn add_child_workflow_execution_failed_event(&self, _: i64, _: &WorkflowExecution, _: Option<&cadence_api::tasks::WorkflowExecutionFailedEventAttributes>) -> HResult<()> {
        unimplemented!()
    }
    fn add_child_workflow_execution_canceled_event(&self, _: i64, _: &WorkflowExecution, _: Option<&cadence_api::tasks::WorkflowExecutionCanceledEventAttributes>) -> HResult<()> {
        unimplemented!()
    }
    fn add_child_workflow_execution_terminated_event(&self, _: i64, _: &WorkflowExecution, _: Option<&cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes>) -> HResult<()> {
        unimplemented!()
    }
    fn add_child_workflow_execution_timed_out_event(&self, _: i64, _: &WorkflowExecution, _: Option<&cadence_api::tasks::WorkflowExecutionTimedOutEventAttributes>) -> HResult<()> {
        unimplemented!()
    }
    fn get_current_version(&self) -> i64 {
        self.current_version
    }
    fn get_last_first_event_id(&self) -> i64 {
        self.last_first_event_id
    }
    fn get_previous_started_event_id(&self) -> i64 {
        self.previous_started_event_id
    }
    fn get_current_branch_token(&self) -> HResult<Vec<u8>> {
        Ok(self.branch_token.clone())
    }
    fn get_version_histories(&self) -> Option<VersionHistories> {
        self.version_histories.clone()
    }
    fn is_sticky_task_list_enabled(&self) -> bool {
        false
    }
    fn get_history_size(&self) -> i64 {
        0
    }
    fn get_workflow_type(&self) -> Option<WorkflowType> {
        unimplemented!()
    }
    async fn get_resurrected_activities(&self) -> HResult<HashSet<i64>> {
        unimplemented!()
    }
    fn delete_activity(&self, _: i64) -> HResult<()> {
        unimplemented!()
    }
    async fn get_activity_scheduled_event(&self, schedule_id: i64) -> HResult<HistoryEvent> {
        (self.activity_scheduled_event)(schedule_id)
    }
    fn add_activity_task_started_event(&self, _: &ActivityInfo, _: i64, _: &str, _: &str) -> HResult<ActivityInfo> {
        unimplemented!()
    }
    fn is_signal_requested(&self, request_id: &str) -> bool {
        self.signal_requested.lock().unwrap().contains(request_id)
    }
    fn add_signal_requested(&self, request_id: &str) {
        self.signal_requested.lock().unwrap().insert(request_id.to_string());
        self.rec(format!("add_signal_requested:{request_id}"));
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.processed_or_pending_decision
    }
    fn set_workflow_state(&self, state: i32) {
        self.info.lock().unwrap().state = state;
        self.rec(format!("set_workflow_state:{state}"));
    }
    fn copy_to_persistence(&self) -> HResult<serde_json::Value> {
        Ok(self.persistence_json.clone())
    }
    fn add_workflow_execution_signaled(&self, name: &str, _: &[u8], _: &str, _: &str) -> HResult<()> {
        if let Some(e) = self.signal_err.lock().unwrap().clone() {
            return Err(HistoryError::Other(e));
        }
        self.rec(format!("signal:{name}"));
        Ok(())
    }
    fn delete_signal_requested(&self, request_id: &str) {
        self.signal_requested.lock().unwrap().remove(request_id);
        self.rec(format!("delete_signal_requested:{request_id}"));
    }
    fn clear_stickyness(&self) {
        self.rec("clear_stickyness");
    }
    fn is_cancel_requested(&self) -> (bool, String) {
        self.cancel_requested.clone()
    }
    async fn get_start_event(&self) -> HResult<HistoryEvent> {
        (self.start_event)()
    }
    fn add_activity_task_completed_event(&self, _: i64, _: i64, _: &cadence_api::tasks::RespondActivityTaskCompletedRequest) -> HResult<()> {
        unimplemented!()
    }
    fn add_activity_task_failed_event(&self, _: i64, _: i64, _: &cadence_api::tasks::RespondActivityTaskFailedRequest) -> HResult<()> {
        unimplemented!()
    }
    fn add_activity_task_canceled_event(&self, _: i64, _: i64, _: i64, _: &[u8], _: &str) -> HResult<()> {
        unimplemented!()
    }
    fn retry_activity(&self, _: &ActivityInfo, _: &str, _: &[u8], _: Option<&FailureOptions>) -> HResult<bool> {
        unimplemented!()
    }
    fn update_activity_progress(&self, _: &ActivityInfo, _: &cadence_api::tasks::RecordActivityTaskHeartbeatRequest) {
        unimplemented!()
    }
    fn add_workflow_execution_cancel_requested_event(&self, cause: &str, _: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()> {
        self.rec(format!("cancel_requested:{cause}"));
        Ok(())
    }
    fn get_last_write_version(&self) -> HResult<i64> {
        Ok(self.last_write_version)
    }
    fn is_resource_duplicated(&self, id: &str) -> bool {
        self.duplicated_resources.lock().unwrap().contains(id)
    }
    fn update_duplicated_resource(&self, _: &str) {
        unimplemented!()
    }
    fn get_pending_activity_infos(&self) -> Vec<ActivityInfo> {
        self.pending_activities.clone()
    }
    fn get_pending_child_execution_infos(&self) -> Vec<PendingChildInfo> {
        self.pending_children.clone()
    }
    fn get_pending_decision(&self) -> Option<crate::mutable_state::DecisionInfo> {
        self.pending_decision.clone()
    }
    async fn get_completion_event(&self) -> HResult<HistoryEvent> {
        (self.completion_event)()
    }
    fn terminate_workflow(&self, first_event_id: i64, reason: &str, _: &[u8], identity: &str) -> HResult<()> {
        if let Some(e) = self.terminate_err.lock().unwrap().clone() {
            return Err(HistoryError::Other(e));
        }
        self.rec(format!("terminate:{first_event_id}:{reason}:{identity}"));
        Ok(())
    }
    fn add_workflow_execution_started_event(&self, _: &WorkflowExecution, _: &HistoryStartWorkflowExecutionRequest) -> HResult<HistoryEvent> {
        unimplemented!()
    }
    fn add_first_decision_task_scheduled(&self, _: &HistoryEvent) -> HResult<()> {
        unimplemented!()
    }
    fn add_decision_task_scheduled_event(&self, _: bool) -> HResult<()> {
        unimplemented!()
    }
}

// ---------------------------------------------------------------------------
// Context / cache / updater
// ---------------------------------------------------------------------------

/// Scriptable `execution.Context`.
pub struct FakeCtx {
    pub execution: WorkflowExecution,
    pub ms: MutableStateRef,
    /// When set, `load_workflow_execution` fails with this message.
    pub load_err: Option<String>,
    pub cached: Option<MutableStateRef>,
    pub update_tasks_err: Option<String>,
    pub calls: Mutex<Vec<String>>,
}

impl FakeCtx {
    pub fn new(ms: MutableStateRef) -> Self {
        Self { execution: WorkflowExecution::default(), ms, load_err: None, cached: None, update_tasks_err: None, calls: Mutex::new(Vec::new()) }
    }
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ExecutionContext for FakeCtx {
    fn get_execution(&self) -> WorkflowExecution {
        self.execution.clone()
    }
    fn get_workflow_execution(&self) -> Option<MutableStateRef> {
        self.cached.clone()
    }
    async fn load_workflow_execution(&self) -> HResult<MutableStateRef> {
        match &self.load_err {
            Some(m) => Err(HistoryError::Other(m.clone())),
            None => Ok(self.ms.clone()),
        }
    }
    async fn update_workflow_execution_tasks(&self, now: i64) -> HResult<()> {
        self.calls.lock().unwrap().push(format!("update_workflow_execution_tasks:{now}"));
        match &self.update_tasks_err {
            Some(m) => Err(HistoryError::Other(m.clone())),
            None => Ok(()),
        }
    }
    async fn update_workflow_execution_as_active(&self, _: i64) -> HResult<()> {
        unimplemented!()
    }
    fn clear(&self) {
        self.calls.lock().unwrap().push("clear".into());
    }
}

/// `execution.Cache` that always hands out the same context and records releases.
pub struct FakeCache {
    pub ctx: Mutex<Option<ExecutionContextRef>>,
    pub db_ctx: Mutex<Option<ExecutionContextRef>>,
    pub get_err: Mutex<Option<String>>,
    pub cache_hit: bool,
    pub requested: Mutex<Vec<(String, WorkflowExecution)>>,
    /// One entry per release call: `true` if released with an error.
    pub releases: Arc<Mutex<Vec<bool>>>,
}

impl FakeCache {
    pub fn new(ctx: ExecutionContextRef) -> Self {
        Self {
            ctx: Mutex::new(Some(ctx)),
            db_ctx: Mutex::new(None),
            get_err: Mutex::new(None),
            cache_hit: false,
            requested: Mutex::new(Vec::new()),
            releases: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub fn failing(msg: &str) -> Self {
        Self {
            ctx: Mutex::new(None),
            db_ctx: Mutex::new(None),
            get_err: Mutex::new(Some(msg.to_string())),
            cache_hit: false,
            requested: Mutex::new(Vec::new()),
            releases: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn release_fn(&self) -> ReleaseFunc {
        let releases = self.releases.clone();
        Arc::new(move |err: Option<&HistoryError>| releases.lock().unwrap().push(err.is_some()))
    }
}

#[async_trait]
impl ExecutionCache for FakeCache {
    async fn get_or_create_workflow_execution(&self, domain_id: &str, execution: &WorkflowExecution) -> HResult<(ExecutionContextRef, ReleaseFunc)> {
        self.requested.lock().unwrap().push((domain_id.to_string(), execution.clone()));
        if let Some(m) = self.get_err.lock().unwrap().clone() {
            return Err(HistoryError::internal(m));
        }
        let ctx = self.ctx.lock().unwrap().clone().expect("ctx");
        Ok((ctx, self.release_fn()))
    }
    async fn get_and_create_workflow_execution(&self, domain_id: &str, execution: &WorkflowExecution) -> HResult<(ExecutionContextRef, ExecutionContextRef, ReleaseFunc, bool)> {
        self.requested.lock().unwrap().push((domain_id.to_string(), execution.clone()));
        if let Some(m) = self.get_err.lock().unwrap().clone() {
            return Err(HistoryError::internal(m));
        }
        let ctx = self.ctx.lock().unwrap().clone().expect("ctx");
        let db = self.db_ctx.lock().unwrap().clone().unwrap_or_else(|| ctx.clone());
        Ok((ctx, db, self.release_fn(), self.cache_hit))
    }
    async fn get_or_create_current_workflow_execution(&self, _: &str, _: &str) -> HResult<ReleaseFunc> {
        unimplemented!()
    }
}

/// `WorkflowUpdater` that runs the action against a fixed context/state and
/// records the arguments and the produced action.
pub struct FakeUpdater {
    pub ctx: ExecutionContextRef,
    pub ms: MutableStateRef,
    pub calls: Mutex<Vec<(String, WorkflowExecution, bool)>>,
    pub last_action: Mutex<Option<(bool, bool)>>,
}

impl FakeUpdater {
    pub fn new(ms: Arc<FakeMs>) -> Self {
        let ms: MutableStateRef = ms;
        Self { ctx: Arc::new(FakeCtx::new(ms.clone())), ms, calls: Mutex::new(Vec::new()), last_action: Mutex::new(None) }
    }
}

#[async_trait]
impl WorkflowUpdater for FakeUpdater {
    async fn update<'a>(&self, domain_id: &str, execution: &WorkflowExecution, current: bool, _now: i64, action: UpdateFn<'a>) -> HResult<()> {
        self.calls.lock().unwrap().push((domain_id.to_string(), execution.clone(), current));
        let a = action(self.ctx.clone(), self.ms.clone()).await?;
        *self.last_action.lock().unwrap() = Some((a.create_decision, a.noop));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Domain cache / misc small fakes
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct FakeDomainCache {
    pub by_id: HashMap<String, Arc<DomainCacheEntry>>,
    pub names: HashMap<String, String>,
    /// ids for which `get_domain_name` returns an EntityNotExists error.
    pub not_exists: HashSet<String>,
    /// ids for which `get_domain_name` returns an internal error with this message.
    pub name_err: HashMap<String, String>,
}

impl DomainCache for FakeDomainCache {
    fn get_domain_by_id(&self, id: &str) -> HResult<Arc<DomainCacheEntry>> {
        self.by_id.get(id).cloned().ok_or_else(|| HistoryError::entity_not_exists(format!("domain {id} not found")))
    }
    fn get_domain(&self, name: &str) -> HResult<Arc<DomainCacheEntry>> {
        self.by_id.values().find(|d| d.info.name == name).cloned().ok_or_else(|| HistoryError::entity_not_exists(format!("domain {name} not found")))
    }
    fn get_domain_name(&self, id: &str) -> HResult<String> {
        if let Some(m) = self.name_err.get(id) {
            return Err(HistoryError::internal(m.clone()));
        }
        if self.not_exists.contains(id) {
            return Err(HistoryError::entity_not_exists("domain not exists"));
        }
        self.names.get(id).cloned().ok_or_else(|| HistoryError::entity_not_exists("domain not exists"))
    }
    fn unregister_domain_change_callback(&self, _: &str) {}
}

pub struct FixedClusterMetadata(pub String);
impl ClusterMetadata for FixedClusterMetadata {
    fn get_current_cluster_name(&self) -> String {
        self.0.clone()
    }
    fn cluster_name_for_failover_version(&self, _: i64) -> HResult<String> {
        Ok(self.0.clone())
    }
}

pub struct FixedTime(pub i64);
impl TimeSource for FixedTime {
    fn now(&self) -> i64 {
        self.0
    }
}

pub struct FixedActiveCluster(pub String);
#[async_trait]
impl ActiveClusterManager for FixedActiveCluster {
    async fn active_cluster_by_workflow(&self, _: &str, _: &str, _: &str) -> HResult<String> {
        Ok(self.0.clone())
    }
}

#[derive(Default)]
pub struct TestConfig {
    pub corruption_check: bool,
    pub max_signals: i32,
    pub no_consistent_query: bool,
    pub no_consistent_query_by_domain: bool,
}
impl EngineConfig for TestConfig {
    fn enable_consistent_query(&self) -> bool {
        !self.no_consistent_query
    }
    fn enable_consistent_query_by_domain(&self, _: &str) -> bool {
        !self.no_consistent_query_by_domain
    }
    fn enable_sticky_query(&self, _: &str) -> bool {
        true
    }
    fn max_buffered_query_count(&self) -> usize {
        1
    }
    fn long_poll_expiration_interval(&self, _: &str) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
    fn maximum_signals_per_execution(&self, _: &str) -> i32 {
        self.max_signals
    }
    fn enable_history_corruption_check(&self, _: &str) -> bool {
        self.corruption_check
    }
}

#[derive(Default)]
pub struct CountingMetrics {
    pub replication_latency_calls: AtomicUsize,
}
impl EngineMetrics for CountingMetrics {
    fn stale_mutable_state(&self, _: &str) {}
    fn activity_e2e_latency(&self, _: &str, _: &str, _: &str, _: &str, _: &str, _: i64) {}
    fn replication_latency(&self, _: &str, _: std::time::Duration) {
        self.replication_latency_calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// Panics on any use. Implements every engine trait so that engines in tests only
/// have to override the collaborators they exercise.
pub struct Nop;

impl DomainCache for Nop {
    fn get_domain_by_id(&self, _: &str) -> HResult<Arc<DomainCacheEntry>> {
        unimplemented!()
    }
    fn get_domain(&self, _: &str) -> HResult<Arc<DomainCacheEntry>> {
        unimplemented!()
    }
    fn get_domain_name(&self, _: &str) -> HResult<String> {
        unimplemented!()
    }
    fn unregister_domain_change_callback(&self, _: &str) {}
}
impl ClusterMetadata for Nop {
    fn get_current_cluster_name(&self) -> String {
        unimplemented!()
    }
    fn cluster_name_for_failover_version(&self, _: i64) -> HResult<String> {
        unimplemented!()
    }
}
impl TimeSource for Nop {
    fn now(&self) -> i64 {
        unimplemented!()
    }
}
#[async_trait]
impl ActiveClusterManager for Nop {
    async fn active_cluster_by_workflow(&self, _: &str, _: &str, _: &str) -> HResult<String> {
        unimplemented!()
    }
}
#[async_trait]
impl WorkflowUpdater for Nop {
    async fn update<'a>(&self, _: &str, _: &WorkflowExecution, _: bool, _: i64, _: UpdateFn<'a>) -> HResult<()> {
        unimplemented!()
    }
}
#[async_trait]
impl ReplicationDLQHandler for Nop {
    async fn get_message_count(&self, _: bool) -> HResult<HashMap<String, i64>> {
        unimplemented!()
    }
    async fn read_messages(&self, _: &str, _: i64, _: i32, _: &[u8]) -> HResult<DLQRead> {
        unimplemented!()
    }
    async fn purge_messages(&self, _: &str, _: i64) -> HResult<()> {
        unimplemented!()
    }
    async fn merge_messages(&self, _: &str, _: i64, _: i32, _: &[u8]) -> HResult<Vec<u8>> {
        unimplemented!()
    }
}
#[async_trait]
impl ReplicationAckManager for Nop {
    async fn get_tasks(&self, _: &str, _: i64) -> HResult<ReplicationMessages> {
        unimplemented!()
    }
}
#[async_trait]
impl ReplicationHydrator for Nop {
    async fn hydrate(&self, _: Task) -> HResult<Option<cadence_api::types::ReplicationTask>> {
        unimplemented!()
    }
    async fn hydrate_immediate(&self, _: ImmediateHydration) -> HResult<Option<cadence_api::types::ReplicationTask>> {
        unimplemented!()
    }
}
#[async_trait]
impl Replicator for Nop {
    async fn apply_events(&self, _: &ReplicateEventsV2Request) -> HResult<()> {
        unimplemented!()
    }
    async fn sync_activity(&self, _: &SyncActivityRequest) -> HResult<()> {
        unimplemented!()
    }
}
#[async_trait]
impl DecisionHandler for Nop {
    async fn handle_decision_task_started(&self, _: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse> {
        unimplemented!()
    }
    async fn handle_decision_task_completed(&self, _: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        unimplemented!()
    }
    async fn handle_decision_task_failed(&self, _: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()> {
        unimplemented!()
    }
    async fn handle_decision_task_scheduled(&self, _: &ScheduleDecisionTaskRequest) -> HResult<()> {
        unimplemented!()
    }
}
impl ShardClock for Nop {
    fn set_current_time(&self, _: &str, _: i64) {}
}
#[async_trait]
impl ExecutionCache for Nop {
    async fn get_or_create_workflow_execution(&self, _: &str, _: &WorkflowExecution) -> HResult<(ExecutionContextRef, ReleaseFunc)> {
        unimplemented!()
    }
    async fn get_and_create_workflow_execution(&self, _: &str, _: &WorkflowExecution) -> HResult<(ExecutionContextRef, ExecutionContextRef, ReleaseFunc, bool)> {
        unimplemented!()
    }
    async fn get_or_create_current_workflow_execution(&self, _: &str, _: &str) -> HResult<ReleaseFunc> {
        unimplemented!()
    }
}
#[async_trait]
impl QueryMatchingClient for Nop {
    async fn query_workflow(&self, _: &str, _: &cadence_api::tasks::QueryWorkflowRequest, _: Option<&TaskList>) -> HResult<cadence_api::tasks::QueryWorkflowResponse> {
        unimplemented!()
    }
}
impl ClientChecker for Nop {
    fn supports_sticky_query(&self, _: &str, _: &str) -> bool {
        unimplemented!()
    }
}
#[async_trait]
impl WorkflowResetter for Nop {
    async fn reset_workflow(&self, _: ResetWorkflow) -> HResult<()> {
        unimplemented!()
    }
}
#[async_trait]
impl HistoryReader for Nop {
    async fn paginate_history(&self, _: i64, _: i64, _: &[u8], _: &[u8], _: i32, _: &str) -> HResult<(Vec<Vec<HistoryEvent>>, Vec<u8>)> {
        unimplemented!()
    }
}
#[async_trait]
impl CurrentExecutionReader for Nop {
    async fn get_current_execution(&self, _: &str, _: &str, _: &str) -> HResult<String> {
        unimplemented!()
    }
}
impl HistoryEventNotifier for Nop {
    fn watch_history_event(&self, _: &str, _: &WorkflowExecution) -> HResult<(String, tokio::sync::mpsc::UnboundedReceiver<HistoryNotification>)> {
        unimplemented!()
    }
    fn unwatch_history_event(&self, _: &str, _: &WorkflowExecution, _: &str) {}
    fn notify_new_history_event(&self, _: HistoryNotification) {}
}
#[async_trait]
impl MutableStateTaskRefresher for Nop {
    async fn refresh_tasks(&self, _: i64, _: MutableStateRef) -> HResult<()> {
        unimplemented!()
    }
}
#[async_trait]
impl EventsReapplier for Nop {
    async fn reapply_events(&self, _: MutableStateRef, _: &[HistoryEvent], _: &str) -> HResult<Vec<HistoryEvent>> {
        unimplemented!()
    }
}
impl ReplicationTaskStore for Nop {
    fn put(&self, _: cadence_api::types::ReplicationTask) {}
}
#[async_trait]
impl super::start_workflow_execution::WorkflowStartHost for Nop {
    fn shard_id(&self) -> i32 {
        unimplemented!()
    }
    fn start_limits(&self, _: &str) -> super::start_workflow_execution::StartLimits {
        unimplemented!()
    }
    fn enable_record_workflow_execution_uninitialized(&self, _: &str) -> bool {
        unimplemented!()
    }
    fn enable_cleanup_orphaned_history_branch(&self, _: &str) -> bool {
        unimplemented!()
    }
    fn has_visibility(&self) -> bool {
        unimplemented!()
    }
    async fn active_cluster_info_by_cluster_attribute(&self, _: &str, _: Option<&ClusterAttribute>) -> HResult<super::start_workflow_execution::ActiveClusterInfo> {
        unimplemented!()
    }
    async fn active_cluster_selection_policy_for_workflow(&self, _: &str, _: &str, _: &str) -> HResult<Option<ActiveClusterSelectionPolicy>> {
        unimplemented!()
    }
    fn new_mutable_state(&self, _: &DomainCacheEntry, _: i64, _: &str) -> HResult<MutableStateRef> {
        unimplemented!()
    }
    fn new_execution_context(&self, _: &str, _: &WorkflowExecution) -> ExecutionContextRef {
        unimplemented!()
    }
    fn close_transaction_as_snapshot(&self, _: &MutableStateRef, _: i64) -> HResult<super::start_workflow_execution::NewRunSnapshot> {
        unimplemented!()
    }
    async fn persist_start_workflow_batch_events(&self, _: &ExecutionContextRef, _: &super::start_workflow_execution::NewRunSnapshot) -> HResult<super::notify_tasks::PersistedBlobRef> {
        unimplemented!()
    }
    async fn create_workflow_execution(
        &self,
        _: &ExecutionContextRef,
        _: &super::start_workflow_execution::NewRunSnapshot,
        _: &super::notify_tasks::PersistedBlobRef,
        _: CreateWorkflowMode,
        _: &str,
        _: i64,
        _: CreateWorkflowRequestMode,
    ) -> HResult<()> {
        unimplemented!()
    }
    async fn update_workflow_execution_with_new_as_active(&self, _: &ExecutionContextRef, _: i64, _: &ExecutionContextRef, _: &MutableStateRef) -> HResult<()> {
        unimplemented!()
    }
    async fn delete_history_branch(&self, _: &[u8], _: &str) -> HResult<()> {
        unimplemented!()
    }
    async fn record_workflow_execution_uninitialized(&self, _: &str, _: &str, _: &WorkflowExecution, _: &str, _: i64) -> HResult<()> {
        unimplemented!()
    }
    async fn delete_visibility_workflow_execution(&self, _: &str, _: &str, _: &WorkflowExecution) -> HResult<()> {
        unimplemented!()
    }
}
/// `WorkflowStartHost` that only knows its shard id.
pub struct NopShard(pub i32);
#[async_trait]
impl super::start_workflow_execution::WorkflowStartHost for NopShard {
    fn shard_id(&self) -> i32 {
        self.0
    }
    fn start_limits(&self, _: &str) -> super::start_workflow_execution::StartLimits {
        unimplemented!()
    }
    fn enable_record_workflow_execution_uninitialized(&self, _: &str) -> bool {
        unimplemented!()
    }
    fn enable_cleanup_orphaned_history_branch(&self, _: &str) -> bool {
        unimplemented!()
    }
    fn has_visibility(&self) -> bool {
        unimplemented!()
    }
    async fn active_cluster_info_by_cluster_attribute(&self, _: &str, _: Option<&ClusterAttribute>) -> HResult<super::start_workflow_execution::ActiveClusterInfo> {
        unimplemented!()
    }
    async fn active_cluster_selection_policy_for_workflow(&self, _: &str, _: &str, _: &str) -> HResult<Option<ActiveClusterSelectionPolicy>> {
        unimplemented!()
    }
    fn new_mutable_state(&self, _: &DomainCacheEntry, _: i64, _: &str) -> HResult<MutableStateRef> {
        unimplemented!()
    }
    fn new_execution_context(&self, _: &str, _: &WorkflowExecution) -> ExecutionContextRef {
        unimplemented!()
    }
    fn close_transaction_as_snapshot(&self, _: &MutableStateRef, _: i64) -> HResult<super::start_workflow_execution::NewRunSnapshot> {
        unimplemented!()
    }
    async fn persist_start_workflow_batch_events(&self, _: &ExecutionContextRef, _: &super::start_workflow_execution::NewRunSnapshot) -> HResult<super::notify_tasks::PersistedBlobRef> {
        unimplemented!()
    }
    async fn create_workflow_execution(
        &self,
        _: &ExecutionContextRef,
        _: &super::start_workflow_execution::NewRunSnapshot,
        _: &super::notify_tasks::PersistedBlobRef,
        _: CreateWorkflowMode,
        _: &str,
        _: i64,
        _: CreateWorkflowRequestMode,
    ) -> HResult<()> {
        unimplemented!()
    }
    async fn update_workflow_execution_with_new_as_active(&self, _: &ExecutionContextRef, _: i64, _: &ExecutionContextRef, _: &MutableStateRef) -> HResult<()> {
        unimplemented!()
    }
    async fn delete_history_branch(&self, _: &[u8], _: &str) -> HResult<()> {
        unimplemented!()
    }
    async fn record_workflow_execution_uninitialized(&self, _: &str, _: &str, _: &WorkflowExecution, _: &str, _: i64) -> HResult<()> {
        unimplemented!()
    }
    async fn delete_visibility_workflow_execution(&self, _: &str, _: &str, _: &WorkflowExecution) -> HResult<()> {
        unimplemented!()
    }
}
#[async_trait]
impl super::register_domain_failover_callback::DomainFailoverHost for Nop {
    fn domain_notification_version(&self) -> i64 {
        unimplemented!()
    }
    fn update_domain_notification_version(&self, _: i64) -> HResult<()> {
        unimplemented!()
    }
    async fn replicate_failover_markers(&self, _: Vec<super::register_domain_failover_callback::FailoverMarkerTask>) -> HResult<()> {
        unimplemented!()
    }
    fn has_dlq_processor(&self) -> bool {
        unimplemented!()
    }
    fn failover_dlq_partitions(&self, _: Vec<super::register_domain_failover_callback::FailoverDlqPartition>) {
        unimplemented!()
    }
    fn register_domain_change_callback(&self, _: &str, _: i64, _: super::register_domain_failover_callback::DomainLockFn, _: super::register_domain_failover_callback::DomainChangedFn) {
        unimplemented!()
    }
}
#[async_trait]
impl super::lifecycle::EngineLifecycleHost for Nop {
    async fn start_dlq_processor(&self, _: tokio_util::sync::CancellationToken) -> HResult<()> {
        unimplemented!()
    }
    async fn stop_dlq_processor(&self, _: tokio_util::sync::CancellationToken) -> HResult<()> {
        unimplemented!()
    }
    fn start_replication_dlq_handler(&self) {}
    fn stop_replication_dlq_handler(&self) {}
    fn start_replication_metrics_emitter(&self) {}
    fn stop_replication_metrics_emitter(&self) {}
    fn start_replication_task_processors(&self) {}
    fn stop_replication_task_processors(&self) {}
    fn enable_graceful_failover(&self) -> bool {
        false
    }
    fn start_failover_marker_notifier(&self) {}
    fn stop_failover_marker_notifier(&self) {}
}

/// Engine whose collaborators panic on use; override the fields a test needs.
pub fn nop_engine() -> HistoryEngine {
    HistoryEngine {
        current_cluster_name: "cluster-a".to_string(),
        domain_cache: Arc::new(Nop),
        cluster_metadata: Arc::new(Nop),
        active_cluster_manager: Arc::new(Nop),
        time_source: Arc::new(Nop),
        updater: Arc::new(Nop),
        queue_processors: HashMap::new(),
        replication_dlq_handler: Arc::new(Nop),
        replication_ack_manager: Arc::new(Nop),
        replication_hydrator: Arc::new(Nop),
        replicator: Arc::new(Nop),
        decision_handler: Arc::new(Nop),
        shard_clock: Arc::new(Nop),
        metrics: Arc::new(CountingMetrics::default()),
        config: Arc::new(TestConfig::default()),
        execution_cache: Arc::new(Nop),
        matching_client: Arc::new(Nop),
        raw_matching_client: Arc::new(Nop),
        client_checker: Arc::new(Nop),
        workflow_resetter: Arc::new(Nop),
        history_reader: Arc::new(Nop),
        current_execution_reader: Arc::new(Nop),
        history_event_notifier: Arc::new(Nop),
        task_refresher: Arc::new(Nop),
        events_reapplier: Arc::new(Nop),
        replication_task_store: Arc::new(Nop),
        start_host: Arc::new(Nop),
        failover_host: Arc::new(Nop),
        lifecycle_host: Arc::new(Nop),
        lifecycle_cancel: std::sync::Mutex::new(None),
    }
}
