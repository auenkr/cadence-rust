//! Ports of go/service/history/task/transfer_active_task_executor_test.go against fake
//! `ActiveMutableState` / `ActiveTransferContext` / dependency traits (the Rust executor is written
//! against traits, not concrete mutable state + mocked persistence). Behaviour is asserted through the
//! exact calls the executor makes.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use super::deps::*;
use super::transfer_active_task_executor::*;
use super::transfer_task_executor_base::*;

const NOW: i64 = 1_000_000_000_000;
const GLOBAL: &str = "global-domain";

struct FakeTime(AtomicI64);
impl TimeSource for FakeTime {
    fn now(&self) -> Time {
        Time(self.0.load(Ordering::SeqCst))
    }
}

struct Domains;
impl DomainCache for Domains {
    fn get_domain_by_id(&self, id: &str) -> Result<Arc<DomainCacheEntry>> {
        if id == "missing-domain" {
            return Err(TaskError::EntityNotExists("domain".into()));
        }
        Ok(Arc::new(DomainCacheEntry {
            info: DomainInfo { id: id.into(), name: format!("name-of-{id}") },
            is_global_domain: id == GLOBAL,
            failover_version: 5,
            ..Default::default()
        }))
    }
    fn get_domain(&self, name: &str) -> Result<Arc<DomainCacheEntry>> {
        Ok(Arc::new(DomainCacheEntry { info: DomainInfo { id: format!("id-of-{name}"), name: name.into() }, ..Default::default() }))
    }
    fn get_domain_name(&self, id: &str) -> Result<String> {
        Ok(format!("name-of-{id}"))
    }
}

struct Shard {
    time: Arc<FakeTime>,
}
impl ShardContext for Shard {
    fn get_shard_id(&self) -> i32 {
        1
    }
    fn get_time_source(&self) -> Arc<dyn TimeSource> {
        self.time.clone()
    }
    fn get_config(&self) -> Arc<dyn HistoryConfig> {
        unimplemented!()
    }
    fn get_domain_cache(&self) -> Arc<dyn DomainCache> {
        Arc::new(Domains)
    }
    fn get_cluster_metadata(&self) -> Arc<dyn ClusterMetadata> {
        unimplemented!()
    }
    fn get_active_cluster_manager(&self) -> Arc<dyn ActiveClusterManager> {
        unimplemented!()
    }
    fn get_metrics_client(&self) -> Arc<dyn MetricsClient> {
        Arc::new(Metrics)
    }
    fn get_service(&self) -> Arc<dyn Service> {
        Arc::new(super::standby_test_support::Svc)
    }
    fn get_logger(&self) -> Logger {
        Logger::new()
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, _: HistoryTaskCategory, _: &str) -> HistoryTaskKey {
        unimplemented!()
    }
    fn as_task_reinjector(&self) -> Arc<dyn crate::taskdlq::interface::TaskReinjector> {
        unimplemented!()
    }
}

struct Metrics;
impl MetricsClient for Metrics {
    fn scope(&self, _: ScopeIdx, _: Vec<Tag>) -> Arc<dyn MetricsScope> {
        noop_scope()
    }
}

// ---------------------------------------------------------------------------
// fake mutable state / context
// ---------------------------------------------------------------------------
struct MsState {
    running: bool,
    next_event_id: i64,
    info: ActiveExecutionInfo,
    activities: HashMap<i64, ActiveActivityInfo>,
    decisions: HashMap<i64, ActiveDecisionInfo>,
    cancels: HashMap<i64, RequestCancelInfo>,
    signals: HashMap<i64, SignalInfo>,
    children: BTreeMap<i64, ChildExecutionInfo>,
    has_parent: bool,
    start_version: i64,
    completion: CloseEvent,
    start: StartEvent,
    attrs: StartChildAttributes,
    calls: Vec<String>,
    reset_point: Option<(String, ResetPointInfo)>,
    with_version_history: bool,
}
struct FakeVersionHistory;
impl ResetVersionHistory for FakeVersionHistory {
    fn get_event_version(&self, event_id: i64) -> Result<i64> {
        Ok(event_id * 10)
    }
    fn get_branch_token(&self) -> Vec<u8> {
        b"branch".to_vec()
    }
}
struct Ms(Mutex<MsState>);
impl Ms {
    fn new() -> Arc<Ms> {
        Arc::new(Ms(Mutex::new(MsState {
            running: true,
            next_event_id: 10,
            info: ActiveExecutionInfo {
                domain_id: "domain".into(),
                workflow_id: "wf".into(),
                run_id: "run".into(),
                task_list: "tl".into(),
                workflow_timeout: 100,
                sticky_schedule_to_start_timeout: 7,
                workflow_type_name: "wt".into(),
                ..Default::default()
            },
            activities: HashMap::new(),
            decisions: HashMap::new(),
            cancels: HashMap::new(),
            signals: HashMap::new(),
            children: BTreeMap::new(),
            has_parent: false,
            start_version: 0,
            completion: CloseEvent {
                event_id: 9,
                event_type: CloseEventType::Completed,
                timestamp: NOW - 5,
                continued_as_new_attributes_present: false,
                continued_as_new_failure_reason: String::new(),
                full_event: None,
            },
            start: StartEvent { timestamp: NOW - 100, ..Default::default() },
            attrs: StartChildAttributes { domain: "child-domain".into(), workflow_id: "child-wf".into(), ..Default::default() },
            calls: vec![],
            reset_point: None,
            with_version_history: false,
        })))
    }
    fn set(&self, f: impl FnOnce(&mut MsState)) {
        f(&mut self.0.lock().unwrap());
    }
    fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().calls.clone()
    }
    fn log(&self, c: String) -> Result<()> {
        self.0.lock().unwrap().calls.push(c);
        Ok(())
    }
}
impl DecisionState for Ms {
    fn has_pending_decision(&self) -> bool {
        false
    }
    fn has_in_flight_decision(&self) -> bool {
        false
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        false
    }
}
#[async_trait]
impl ActiveMutableState for Ms {
    fn is_workflow_execution_running(&self) -> bool {
        self.0.lock().unwrap().running
    }
    fn domain_name(&self) -> String {
        "domain-name".into()
    }
    fn execution_info(&self) -> ActiveExecutionInfo {
        self.0.lock().unwrap().info.clone()
    }
    fn next_event_id(&self) -> i64 {
        self.0.lock().unwrap().next_event_id
    }
    fn activity_info(&self, id: i64) -> Option<ActiveActivityInfo> {
        self.0.lock().unwrap().activities.get(&id).cloned()
    }
    fn decision_info(&self, id: i64) -> Option<ActiveDecisionInfo> {
        self.0.lock().unwrap().decisions.get(&id).cloned()
    }
    fn request_cancel_info(&self, id: i64) -> Option<RequestCancelInfo> {
        self.0.lock().unwrap().cancels.get(&id).cloned()
    }
    fn signal_info(&self, id: i64) -> Option<SignalInfo> {
        self.0.lock().unwrap().signals.get(&id).cloned()
    }
    fn child_execution_info(&self, id: i64) -> Option<ChildExecutionInfo> {
        self.0.lock().unwrap().children.get(&id).cloned()
    }
    fn pending_child_execution_infos(&self) -> BTreeMap<i64, ChildExecutionInfo> {
        self.0.lock().unwrap().children.clone()
    }
    fn has_parent_execution(&self) -> bool {
        self.0.lock().unwrap().has_parent
    }
    fn last_write_version(&self) -> Result<i64> {
        Ok(0)
    }
    fn start_version(&self) -> Result<i64> {
        Ok(self.0.lock().unwrap().start_version)
    }
    fn current_version_history(&self) -> Result<Option<Arc<dyn ResetVersionHistory>>> {
        if self.0.lock().unwrap().with_version_history {
            Ok(Some(Arc::new(FakeVersionHistory)))
        } else {
            Ok(None)
        }
    }
    fn find_auto_reset_point(&self, _: &dyn TimeSource, _: &DomainCacheEntry) -> Option<(String, ResetPointInfo)> {
        self.0.lock().unwrap().reset_point.clone()
    }
    fn add_child_workflow_execution_started_event(&self, domain: &str, ex: &WorkflowExecutionRef, _: &StartChildAttributes, init: i64) -> Result<()> {
        self.log(format!("child_started:{domain}:{}/{}@{init}", ex.workflow_id, ex.run_id))
    }
    fn add_start_child_workflow_execution_failed_event(&self, init: i64, _: &StartChildAttributes) -> Result<()> {
        self.log(format!("start_child_failed@{init}"))
    }
    fn add_external_workflow_execution_cancel_requested(&self, init: i64, domain: &str, wf: &str, run: &str) -> Result<()> {
        self.log(format!("cancel_requested@{init}:{domain}:{wf}/{run}"))
    }
    fn add_external_workflow_execution_signaled(&self, init: i64, domain: &str, wf: &str, run: &str, control: &[u8]) -> Result<()> {
        self.log(format!("signaled@{init}:{domain}:{wf}/{run}:{}", String::from_utf8_lossy(control)))
    }
    fn add_request_cancel_external_workflow_execution_failed_event(&self, init: i64, domain: &str, wf: &str, run: &str) -> Result<()> {
        self.log(format!("cancel_failed@{init}:{domain}:{wf}/{run}"))
    }
    fn add_signal_external_workflow_execution_failed_event(&self, init: i64, domain: &str, wf: &str, run: &str, control: &[u8]) -> Result<()> {
        self.log(format!("signal_failed@{init}:{domain}:{wf}/{run}:{}", String::from_utf8_lossy(control)))
    }
    fn schedule_decision(&self) -> Result<()> {
        self.log("schedule_decision".into())
    }
    async fn completion_event(&self) -> Result<CloseEvent> {
        Ok(self.0.lock().unwrap().completion.clone())
    }
    async fn start_event(&self) -> Result<StartEvent> {
        Ok(self.0.lock().unwrap().start.clone())
    }
    async fn child_execution_initiated_event(&self, _: i64) -> Result<StartChildAttributes> {
        Ok(self.0.lock().unwrap().attrs.clone())
    }
}

struct Ctx {
    ms: Arc<Ms>,
    released: Mutex<Vec<Option<TaskError>>>,
}
#[async_trait]
impl ActiveTransferContext for Ctx {
    fn release(&self, e: Option<TaskError>) {
        self.released.lock().unwrap().push(e);
    }
    async fn load_mutable_state(&self, _: &dyn PersistenceTask, _: &Arc<dyn MetricsScope>, _: &Logger, _: i64) -> Result<Option<Arc<dyn ActiveMutableState>>> {
        Ok(Some(self.ms.clone()))
    }
    async fn load_workflow_execution(&self) -> Result<Arc<dyn ActiveMutableState>> {
        Ok(self.ms.clone())
    }
    async fn update_workflow_execution_as_active(&self, _: Time) -> Result<()> {
        self.ms.log("update_as_active".into())
    }
}

// ---------------------------------------------------------------------------
// fake dependencies
// ---------------------------------------------------------------------------
#[derive(Default)]
struct TDeps {
    activity: Mutex<Vec<AddMatchingTaskRequest>>,
    decision: Mutex<Vec<AddMatchingTaskRequest>>,
    closed: Mutex<Vec<VisibilityRecord>>,
    started: Mutex<Vec<VisibilityRecord>>,
    upserted: Mutex<Vec<VisibilityRecord>>,
    context_header: std::sync::atomic::AtomicBool,
    decision_errors: Mutex<Vec<TaskError>>,
}
#[async_trait]
impl TransferDependencies for TDeps {
    fn standby_task_missing_events_discard_delay(&self) -> Duration {
        Duration::from_secs(60)
    }
    fn dlq_writer(&self) -> Arc<dyn super::standby_task_util::TaskDLQWriter> {
        unimplemented!()
    }
    fn history_task_dlq_mode(&self) -> StringPropertyFnWithDomainFilter {
        unimplemented!()
    }
    async fn add_activity_task(&self, r: AddMatchingTaskRequest) -> Result<()> {
        self.activity.lock().unwrap().push(r);
        Ok(())
    }
    async fn add_decision_task(&self, r: AddMatchingTaskRequest) -> Result<()> {
        self.decision.lock().unwrap().push(r);
        let mut errs = self.decision_errors.lock().unwrap();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.remove(0))
        }
    }
    fn visibility_domain(&self, _: &str, _: &str) -> Result<VisibilityDomain> {
        Ok(VisibilityDomain { name: "vis-domain".into(), retention_days: 3, ..Default::default() })
    }
    fn enable_context_header_in_visibility(&self, _: &str) -> bool {
        self.context_header.load(Ordering::SeqCst)
    }
    fn valid_search_attributes(&self) -> HashSet<String> {
        ["Header_foo".to_string()].into_iter().collect()
    }
    fn hidden_value_keys(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::new()
    }
    fn enable_record_workflow_execution_uninitialized(&self, _: &str) -> bool {
        false
    }
    async fn record_workflow_execution_uninitialized(&self, _: &VisibilityRecord) -> Result<()> {
        Ok(())
    }
    async fn record_workflow_execution_started(&self, r: VisibilityRecord) -> Result<()> {
        self.started.lock().unwrap().push(r);
        Ok(())
    }
    async fn upsert_workflow_execution(&self, r: VisibilityRecord) -> Result<()> {
        self.upserted.lock().unwrap().push(r);
        Ok(())
    }
    async fn record_workflow_execution_closed(&self, r: VisibilityRecord) -> Result<()> {
        self.closed.lock().unwrap().push(r);
        Ok(())
    }
    fn visibility_archival_time_limit(&self) -> Duration {
        Duration::from_secs(1)
    }
    async fn archive_visibility(&self, _: VisibilityRecord, _: VisibilityDomain) -> Result<()> {
        Ok(())
    }
}

struct ADeps {
    ctx: Arc<Ctx>,
    log: Mutex<Vec<String>>,
    errors: Mutex<HashMap<&'static str, TaskError>>,
    allow: bool,
    policy_worker: bool,
}
impl ADeps {
    fn record(&self, entry: String, key: &'static str) -> Result<()> {
        self.log.lock().unwrap().push(entry);
        match self.errors.lock().unwrap().get(key) {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}
fn ex(r: &WorkflowExecutionRef) -> String {
    format!("{}/{}", r.workflow_id, r.run_id)
}
#[async_trait]
impl TransferActiveDependencies for ADeps {
    async fn get_or_create_workflow_execution_with_timeout(&self, _: &str, _: &str, _: &str, _: Duration) -> Result<Arc<dyn ActiveTransferContext>> {
        Ok(self.ctx.clone())
    }
    fn allow_internal(&self, _: &str, _: &str) -> bool {
        self.allow
    }
    async fn record_child_execution_completed(&self, r: RecordChildExecutionCompletedRequest) -> Result<()> {
        self.record(format!("record_child_completed:{}:{}@{}:by:{}:ev{}", r.domain_uuid, ex(&r.workflow_execution), r.initiated_id, ex(&r.completed_execution), r.completion_event.event_id), "record_child_completed")
    }
    async fn request_cancel_workflow_execution(&self, r: HistoryRequestCancelRequest) -> Result<()> {
        self.record(format!("cancel:{}:{}:{}:{}:req={}:first={}:init={:?}:child_only={}", r.domain_uuid, r.domain, ex(&r.workflow_execution), r.identity, r.request_id, r.first_execution_run_id, r.external_initiated_event_id, r.child_workflow_only), "cancel")
    }
    async fn signal_workflow_execution(&self, r: HistorySignalRequest) -> Result<()> {
        self.record(format!("signal:{}:{}:{}:{}:req={}:{}", r.domain_uuid, r.domain, ex(&r.workflow_execution), r.signal_name, r.request_id, String::from_utf8_lossy(&r.control)), "signal")
    }
    async fn remove_signal_mutable_state(&self, r: RemoveSignalMutableStateRequest) -> Result<()> {
        self.record(format!("remove_signal:{}:{}:{}", r.domain_uuid, ex(&r.workflow_execution), r.request_id), "remove_signal")
    }
    async fn start_workflow_execution(&self, r: HistoryStartWorkflowRequest) -> Result<String> {
        let p = r.parent_execution_info.as_ref().unwrap();
        self.record(format!("start:{}:{}:reqid={}:parent={}:{}@{}", r.domain_uuid, r.request.attributes.workflow_id, r.request.request_id, p.domain, ex(&p.execution), p.initiated_id), "start")?;
        Ok("child-run".into())
    }
    async fn schedule_decision_task(&self, r: ScheduleDecisionTaskRequest) -> Result<()> {
        self.record(format!("schedule_first_decision:{}:{}:first={}", r.domain_uuid, ex(&r.workflow_execution), r.is_first_decision), "schedule_decision")
    }
    async fn terminate_workflow_execution(&self, r: HistoryTerminateRequest) -> Result<()> {
        self.record(format!("terminate:{}:{}:first={}:child_only={}:reason={}", r.domain, ex(&r.workflow_execution), r.first_execution_run_id, r.child_workflow_only, r.reason), "terminate")
    }
    fn create_history_start_workflow_request(&self, target: &str, request: FrontendStartRequest, now: Time, partition_config: BytesMap) -> Result<HistoryStartWorkflowRequest> {
        Ok(HistoryStartWorkflowRequest { domain_uuid: target.into(), request, now, partition_config, parent_execution_info: None })
    }
    async fn send_parent_close_policy_request(&self, r: ParentClosePolicyRequest) -> Result<()> {
        self.record(format!("parent_close_worker:{}:{}", r.domain_name, r.executions.len()), "worker")
    }
    async fn reset_workflow(&self, r: WorkflowResetRequest) -> Result<()> {
        self.record(
            format!(
                "reset:{}:{}/{}:base={}:last={}@{}:next={}:branch={}:reason={}:skip_signal={}:new_run_set={}:req_set={}",
                r.domain_id, r.workflow_id, r.reset_run_id.is_empty() as u8, r.base_run_id, r.base_rebuild_last_event_id,
                r.base_rebuild_last_event_version, r.base_next_event_id, String::from_utf8_lossy(&r.base_branch_token),
                r.reset_reason, r.skip_signal_reapply, !r.reset_run_id.is_empty(), !r.reset_request_id.is_empty()
            ),
            "reset",
        )
    }
    async fn get_current_execution(&self, _: &str, _: &str, _: &str, _: i32) -> Result<String> {
        Ok("run".into())
    }
    fn get_child_execution_domain_id(&self, child: &ChildExecutionInfo, _: &DomainCacheEntry) -> Result<String> {
        Ok(child.domain_id.clone())
    }
    fn advanced_visibility_writing_enabled(&self) -> bool {
        false
    }
    fn enable_parent_close_policy_worker(&self) -> bool {
        self.policy_worker
    }
    fn parent_close_policy_threshold(&self, _: &str) -> usize {
        2
    }
    fn parent_close_policy_batch_size(&self, _: &str) -> usize {
        10
    }
    fn log_execute_history_task(&self, _: i32, _: &dyn PersistenceTask) {}
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------
struct H {
    ms: Arc<Ms>,
    ctx: Arc<Ctx>,
    deps: Arc<ADeps>,
    tdeps: Arc<TDeps>,
    exec: TransferActiveTaskExecutor,
}
fn harness() -> H {
    harness_with(true, false)
}
fn harness_with(allow: bool, policy_worker: bool) -> H {
    let ms = Ms::new();
    let ctx = Arc::new(Ctx { ms: ms.clone(), released: Mutex::new(vec![]) });
    let deps = Arc::new(ADeps { ctx: ctx.clone(), log: Mutex::new(vec![]), errors: Mutex::new(HashMap::new()), allow, policy_worker });
    let tdeps = Arc::new(TDeps::default());
    let shard: Arc<dyn ShardContext> = Arc::new(Shard { time: Arc::new(FakeTime(AtomicI64::new(NOW))) });
    let base = new_transfer_task_executor_base(shard, tdeps.clone(), Logger::new());
    let exec = TransferActiveTaskExecutor { base, dependencies: deps.clone() };
    H { ms, ctx, deps, tdeps, exec }
}
fn wi() -> WorkflowIdentifier {
    WorkflowIdentifier { domain_id: "domain".into(), workflow_id: "wf".into(), run_id: "run".into() }
}
fn td(version: i64) -> TaskData {
    TaskData { version, task_id: 1, visibility_timestamp: Time(NOW - 1_000_000_000) }
}
fn child(started: bool, policy: ParentClosePolicy) -> ChildExecutionInfo {
    ChildExecutionInfo {
        version: 0,
        started_id: if started { 20 } else { EMPTY_EVENT_ID },
        started_workflow_id: "child-wf".into(),
        started_run_id: if started { "child-run0".into() } else { String::new() },
        create_request_id: "create-req".into(),
        parent_close_policy: policy,
        domain_id: "child-domain-id".into(),
        domain_name: "child-domain".into(),
    }
}
fn atask() -> ActivityTask {
    ActivityTask { workflow_identifier: wi(), task_data: td(0), target_domain_id: "domain".into(), task_list: "tl".into(), schedule_id: 5, ..Default::default() }
}
fn dtask(task_list: &str) -> DecisionTask {
    DecisionTask { workflow_identifier: wi(), task_data: td(0), target_domain_id: "domain".into(), task_list: task_list.into(), schedule_id: 5, ..Default::default() }
}
fn released_ok(h: &H) -> bool {
    h.ctx.released.lock().unwrap().iter().all(|e| e.is_none())
}

// ---------------------------------------------------------------------------
// TestProcessActivityTask_* / TestProcessDecisionTask_*
// ---------------------------------------------------------------------------
fn with_activity(h: &H) {
    h.ms.set(|s| {
        s.activities.insert(5, ActiveActivityInfo { version: 0, schedule_to_start_timeout: 20, task_list: "act-tl".into(), task_list_kind: TaskListKind::Normal });
    });
}

#[tokio::test]
async fn activity_success_pushes_to_matching() {
    let h = harness();
    with_activity(&h);
    h.exec.process_activity_task(&atask()).await.unwrap();
    let pushed = h.tdeps.activity.lock().unwrap().clone();
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].task_list.name, "act-tl");
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 20);
    assert_eq!(pushed[0].schedule_id, 5);
    assert_eq!(pushed[0].domain_uuid, "domain");
    assert_eq!(pushed[0].execution.workflow_id, "wf");
    assert!(released_ok(&h));
}

#[tokio::test]
async fn activity_empty_task_list_falls_back_to_task() {
    let h = harness();
    with_activity(&h);
    h.ms.set(|s| s.activities.get_mut(&5).unwrap().task_list = String::new());
    h.exec.process_activity_task(&atask()).await.unwrap();
    assert_eq!(h.tdeps.activity.lock().unwrap()[0].task_list.name, "tl");
}

#[tokio::test]
async fn activity_duplication_and_not_running_are_noop() {
    let h = harness(); // no activity info => duplicate
    h.exec.process_activity_task(&atask()).await.unwrap();
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
    let h = harness();
    with_activity(&h);
    h.ms.set(|s| s.running = false);
    h.exec.process_activity_task(&atask()).await.unwrap();
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
}

#[tokio::test]
async fn activity_rate_limited() {
    let h = harness_with(false, false);
    with_activity(&h);
    let err = h.exec.process_activity_task(&atask()).await.unwrap_err();
    assert_eq!(err, TaskError::WorkflowRateLimited);
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
}

#[tokio::test]
async fn decision_normal_pushes_with_workflow_timeout() {
    let h = harness();
    h.ms.set(|s| {
        s.decisions.insert(5, ActiveDecisionInfo { version: 0 });
    });
    h.exec.process_decision_task(&dtask("tl")).await.unwrap();
    let pushed = h.tdeps.decision.lock().unwrap().clone();
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].task_list.name, "tl");
    assert_eq!(pushed[0].task_list.kind, TaskListKind::Normal);
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 100);
}

#[tokio::test]
async fn decision_sticky_uses_sticky_timeout() {
    let h = harness();
    h.ms.set(|s| {
        s.decisions.insert(5, ActiveDecisionInfo { version: 0 });
    });
    h.exec.process_decision_task(&dtask("sticky-tl")).await.unwrap();
    let pushed = h.tdeps.decision.lock().unwrap().clone();
    assert_eq!(pushed[0].task_list.name, "sticky-tl");
    assert_eq!(pushed[0].task_list.kind, TaskListKind::Sticky);
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 7);
}

#[tokio::test]
async fn decision_sticky_worker_unavailable_falls_back_to_normal() {
    let h = harness();
    h.ms.set(|s| {
        s.decisions.insert(5, ActiveDecisionInfo { version: 0 });
    });
    h.tdeps.decision_errors.lock().unwrap().push(TaskError::StickyWorkerUnavailable("gone".into()));
    h.exec.process_decision_task(&dtask("sticky-tl")).await.unwrap();
    let pushed = h.tdeps.decision.lock().unwrap().clone();
    assert_eq!(pushed.len(), 2);
    assert_eq!(pushed[1].task_list.name, "tl");
    assert_eq!(pushed[1].task_list.kind, TaskListKind::Normal);
    // sticky schedule-to-start timeout is kept as the TTL
    assert_eq!(pushed[1].schedule_to_start_timeout_seconds, 7);
}

#[tokio::test]
async fn decision_duplication_noop_and_rate_limited() {
    let h = harness();
    h.exec.process_decision_task(&dtask("tl")).await.unwrap();
    assert!(h.tdeps.decision.lock().unwrap().is_empty());
    let h = harness_with(false, false);
    h.ms.set(|s| {
        s.decisions.insert(5, ActiveDecisionInfo { version: 0 });
    });
    assert_eq!(h.exec.process_decision_task(&dtask("tl")).await.unwrap_err(), TaskError::WorkflowRateLimited);
}

// ---------------------------------------------------------------------------
// TestProcessCloseExecution_*
// ---------------------------------------------------------------------------
fn closed_with_parent(h: &H) {
    h.ms.set(|s| {
        s.running = false;
        s.has_parent = true;
        s.info.parent_domain_id = "parent-domain".into();
        s.info.parent_workflow_id = "parent-wf".into();
        s.info.parent_run_id = "parent-run".into();
        s.info.initiated_id = 11;
        s.info.close_status = 1;
    });
}

#[tokio::test]
async fn close_with_parent_records_visibility_and_replies_to_parent() {
    let h = harness();
    closed_with_parent(&h);
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert_eq!(h.deps.log(), vec!["record_child_completed:parent-domain:parent-wf/parent-run@11:by:wf/run:ev9"]);
    let closed = h.tdeps.closed.lock().unwrap().clone();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].workflow_type_name, "wt");
    assert_eq!(closed[0].close_timestamp, NOW - 5);
    assert_eq!(closed[0].history_length, 9);
    assert_eq!(closed[0].domain, "vis-domain");
    assert_eq!(closed[0].retention_seconds, 3 * 86400);
    assert_eq!(closed[0].execution_status, WorkflowExecutionStatus::Completed);
}

#[tokio::test]
async fn close_without_parent_or_when_running_does_not_reply() {
    let h = harness();
    h.ms.set(|s| s.running = false);
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert!(h.deps.log().is_empty());
    assert_eq!(h.tdeps.closed.lock().unwrap().len(), 1);
    // still running => nothing at all
    let h = harness();
    closed_with_parent(&h);
    h.ms.set(|s| s.running = true);
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert!(h.deps.log().is_empty());
    assert!(h.tdeps.closed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn close_continued_as_new_does_not_reply_to_parent() {
    let h = harness();
    closed_with_parent(&h);
    h.ms.set(|s| s.info.is_continued_as_new = true);
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert!(h.deps.log().is_empty());
}

#[tokio::test]
async fn close_parent_reply_ignores_completed_but_returns_other_errors() {
    let h = harness();
    closed_with_parent(&h);
    h.deps.errors.lock().unwrap().insert("record_child_completed", TaskError::EntityNotExists("parent gone".into()));
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    let h = harness();
    closed_with_parent(&h);
    h.deps.errors.lock().unwrap().insert("record_child_completed", TaskError::BadRequest("bad".into()));
    let err = h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap_err();
    assert_eq!(err, TaskError::BadRequest("bad".into()));
}

#[tokio::test]
async fn record_child_execution_completed_task_only_replies() {
    let h = harness();
    closed_with_parent(&h);
    h.exec.process_record_child_execution_completed(&RecordChildExecutionCompletedTask { workflow_identifier: wi(), task_data: td(0), ..Default::default() }).await.unwrap();
    assert_eq!(h.deps.log().len(), 1);
    assert!(h.tdeps.closed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn record_workflow_closed_task_only_records_visibility() {
    let h = harness();
    closed_with_parent(&h);
    h.exec.process_record_workflow_closed(&RecordWorkflowClosedTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert!(h.deps.log().is_empty());
    assert_eq!(h.tdeps.closed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn close_applies_parent_close_policy_to_children() {
    let h = harness();
    h.ms.set(|s| {
        s.running = false;
        s.children.insert(1, child(true, ParentClosePolicy::Terminate));
        let mut c = child(true, ParentClosePolicy::RequestCancel);
        c.started_workflow_id = "cancel-wf".into();
        c.started_run_id = "cancel-run".into();
        s.children.insert(2, c);
        let mut c = child(true, ParentClosePolicy::Abandon);
        c.started_workflow_id = "abandon-wf".into();
        s.children.insert(3, c);
    });
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert_eq!(
        h.deps.log(),
        vec![
            "terminate:domain-name:child-wf/:first=child-run0:child_only=true:reason=by parent close policy",
            "cancel:domain:domain-name:cancel-wf/:history-service:req=:first=cancel-run:init=None:child_only=true",
        ]
    );
}

#[tokio::test]
async fn close_policy_goes_to_worker_above_threshold_skipping_abandon() {
    let h = harness_with(true, true);
    h.ms.set(|s| {
        s.running = false;
        s.children.insert(1, child(true, ParentClosePolicy::Terminate));
        s.children.insert(2, child(true, ParentClosePolicy::Abandon));
        s.children.insert(3, child(true, ParentClosePolicy::RequestCancel));
    });
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert_eq!(h.deps.log(), vec!["parent_close_worker:domain-name:2"]);
}

#[tokio::test]
async fn close_policy_noop_errors_ignored_other_errors_returned() {
    let h = harness();
    h.ms.set(|s| {
        s.running = false;
        s.children.insert(1, child(true, ParentClosePolicy::Terminate));
        s.children.insert(2, child(true, ParentClosePolicy::Terminate));
    });
    h.deps.errors.lock().unwrap().insert("terminate", TaskError::EntityNotExists("x".into()));
    h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.unwrap();
    assert_eq!(h.deps.log().len(), 2);
    h.deps.errors.lock().unwrap().insert("terminate", TaskError::BadRequest("x".into()));
    h.deps.log.lock().unwrap().clear();
    assert!(h.exec.process_close_execution(&CloseExecutionTask { workflow_identifier: wi(), task_data: td(0) }).await.is_err());
    assert_eq!(h.deps.log().len(), 1);
}

// ---------------------------------------------------------------------------
// TestProcessCancelExecution_*
// ---------------------------------------------------------------------------
fn ctask() -> CancelExecutionTask {
    CancelExecutionTask { workflow_identifier: wi(), task_data: td(0), target_domain_id: "t-domain".into(), target_workflow_id: "t-wf".into(), target_run_id: "t-run".into(), target_child_workflow_only: false, initiated_id: 12 }
}
fn with_cancel(h: &H) {
    h.ms.set(|s| {
        s.cancels.insert(12, RequestCancelInfo { version: 0, cancel_request_id: "cancel-req".into() });
    });
}

#[tokio::test]
async fn cancel_success_records_external_cancel_requested() {
    let h = harness();
    with_cancel(&h);
    h.exec.process_cancel_execution(&ctask()).await.unwrap();
    assert_eq!(h.deps.log(), vec!["cancel:t-domain:name-of-t-domain:t-wf/t-run:history-service:req=cancel-req:first=:init=Some(12):child_only=false"]);
    assert_eq!(h.ms.calls(), vec!["cancel_requested@12:name-of-t-domain:t-wf/t-run", "schedule_decision", "update_as_active"]);
}

#[tokio::test]
async fn cancel_failure_records_cancel_failed_for_non_transient() {
    let h = harness();
    with_cancel(&h);
    h.deps.errors.lock().unwrap().insert("cancel", TaskError::EntityNotExists("gone".into()));
    h.exec.process_cancel_execution(&ctask()).await.unwrap();
    assert_eq!(h.ms.calls(), vec!["cancel_failed@12:name-of-t-domain:t-wf/t-run", "schedule_decision", "update_as_active"]);
}

#[tokio::test]
async fn cancel_cancellation_already_requested_counts_as_success() {
    let h = harness();
    with_cancel(&h);
    h.deps.errors.lock().unwrap().insert("cancel", TaskError::CancellationAlreadyRequested("dup".into()));
    h.exec.process_cancel_execution(&ctask()).await.unwrap();
    assert_eq!(h.ms.calls()[0], "cancel_requested@12:name-of-t-domain:t-wf/t-run");
}

#[tokio::test]
async fn cancel_duplication_and_not_running_are_noop() {
    let h = harness(); // no cancel info
    h.exec.process_cancel_execution(&ctask()).await.unwrap();
    assert!(h.deps.log().is_empty() && h.ms.calls().is_empty());
    let h = harness();
    with_cancel(&h);
    h.ms.set(|s| s.running = false);
    h.exec.process_cancel_execution(&ctask()).await.unwrap();
    assert!(h.deps.log().is_empty() && h.ms.calls().is_empty());
}

#[tokio::test]
async fn cancel_workflow_cancelling_itself_records_failure_without_rpc() {
    let h = harness();
    with_cancel(&h);
    let mut t = ctask();
    t.target_domain_id = "domain".into();
    t.target_workflow_id = "wf".into();
    t.target_run_id = "other-run".into();
    h.exec.process_cancel_execution(&t).await.unwrap();
    assert!(h.deps.log().is_empty());
    assert_eq!(h.ms.calls()[0], "cancel_failed@12:name-of-domain:wf/other-run");
}

// ---------------------------------------------------------------------------
// TestProcessSignalExecution_*
// ---------------------------------------------------------------------------
fn stask() -> SignalExecutionTask {
    SignalExecutionTask { workflow_identifier: wi(), task_data: td(0), target_domain_id: "t-domain".into(), target_workflow_id: "t-wf".into(), target_run_id: "t-run".into(), target_child_workflow_only: false, initiated_id: 13 }
}
fn with_signal(h: &H) {
    h.ms.set(|s| {
        s.signals.insert(13, SignalInfo { version: 0, signal_name: "sig".into(), signal_request_id: "sig-req".into(), input: b"in".to_vec(), control: b"ctl".to_vec() });
    });
}

#[tokio::test]
async fn signal_success_records_signaled_then_removes_signal_from_target() {
    let h = harness();
    with_signal(&h);
    h.exec.process_signal_execution(&stask()).await.unwrap();
    assert_eq!(h.deps.log(), vec!["signal:t-domain:name-of-t-domain:t-wf/t-run:sig:req=sig-req:ctl", "remove_signal:t-domain:t-wf/t-run:sig-req"]);
    assert_eq!(h.ms.calls(), vec!["signaled@13:name-of-t-domain:t-wf/t-run:ctl", "schedule_decision", "update_as_active"]);
}

#[tokio::test]
async fn signal_failure_records_signal_failed_and_does_not_remove_signal() {
    let h = harness();
    with_signal(&h);
    h.deps.errors.lock().unwrap().insert("signal", TaskError::EntityNotExists("gone".into()));
    h.exec.process_signal_execution(&stask()).await.unwrap();
    assert_eq!(h.ms.calls()[0], "signal_failed@13:name-of-t-domain:t-wf/t-run:ctl");
    assert!(!h.deps.log().iter().any(|l| l.starts_with("remove_signal")));
}

#[tokio::test]
async fn signal_remove_signal_entity_not_exists_is_ignored() {
    let h = harness();
    with_signal(&h);
    h.deps.errors.lock().unwrap().insert("remove_signal", TaskError::EntityNotExists("gone".into()));
    h.exec.process_signal_execution(&stask()).await.unwrap();
}

#[tokio::test]
async fn signal_duplication_noop_and_signaling_itself() {
    let h = harness();
    h.exec.process_signal_execution(&stask()).await.unwrap();
    assert!(h.deps.log().is_empty() && h.ms.calls().is_empty());
    let h = harness();
    with_signal(&h);
    let mut t = stask();
    t.target_domain_id = "domain".into();
    t.target_workflow_id = "wf".into();
    h.exec.process_signal_execution(&t).await.unwrap();
    assert!(h.deps.log().is_empty());
    assert_eq!(h.ms.calls()[0], "signal_failed@13:name-of-domain:wf/t-run:ctl");
}

// ---------------------------------------------------------------------------
// TestProcessStartChildExecution_*
// ---------------------------------------------------------------------------
fn sctask() -> StartChildExecutionTask {
    StartChildExecutionTask { workflow_identifier: wi(), task_data: td(0), target_domain_id: "t-domain".into(), target_workflow_id: "child-wf".into(), initiated_id: 14, task_list: "tl".into() }
}
fn with_child(h: &H, started: bool, policy: ParentClosePolicy) {
    h.ms.set(|s| {
        s.children.insert(14, child(started, policy));
    });
}

#[tokio::test]
async fn start_child_success_starts_records_started_and_schedules_first_decision() {
    let h = harness();
    with_child(&h, false, ParentClosePolicy::Abandon);
    h.exec.process_start_child_execution(&sctask()).await.unwrap();
    assert_eq!(
        h.deps.log(),
        vec![
            "start:t-domain:child-wf:reqid=create-req:parent=name-of-domain:wf/run@14",
            "schedule_first_decision:t-domain:child-wf/child-run:first=true",
        ]
    );
    assert_eq!(h.ms.calls(), vec!["child_started:child-domain:child-wf/child-run@14", "schedule_decision", "update_as_active"]);
}

#[tokio::test]
async fn start_child_already_started_failure_records_start_child_failed() {
    let h = harness();
    with_child(&h, false, ParentClosePolicy::Abandon);
    h.deps.errors.lock().unwrap().insert("start", TaskError::WorkflowExecutionAlreadyStarted("dup".into()));
    h.exec.process_start_child_execution(&sctask()).await.unwrap();
    assert_eq!(h.ms.calls(), vec!["start_child_failed@14", "schedule_decision", "update_as_active"]);
    assert!(!h.deps.log().iter().any(|l| l.starts_with("schedule_first_decision")));
}

#[tokio::test]
async fn start_child_other_failure_is_returned() {
    let h = harness();
    with_child(&h, false, ParentClosePolicy::Abandon);
    h.deps.errors.lock().unwrap().insert("start", TaskError::BadRequest("bad".into()));
    assert_eq!(h.exec.process_start_child_execution(&sctask()).await.unwrap_err(), TaskError::BadRequest("bad".into()));
    assert!(h.ms.calls().is_empty());
}

#[tokio::test]
async fn start_child_already_started_only_schedules_first_decision() {
    let h = harness();
    with_child(&h, true, ParentClosePolicy::Abandon);
    h.exec.process_start_child_execution(&sctask()).await.unwrap();
    assert_eq!(h.deps.log(), vec!["schedule_first_decision:t-domain:child-wf/child-run0:first=true"]);
    assert!(h.ms.calls().is_empty());
}

#[tokio::test]
async fn start_child_started_abandon_child_with_closed_parent_still_schedules_decision() {
    let h = harness();
    with_child(&h, true, ParentClosePolicy::Abandon);
    h.ms.set(|s| s.running = false);
    h.exec.process_start_child_execution(&sctask()).await.unwrap();
    assert_eq!(h.deps.log().len(), 1);
}

#[tokio::test]
async fn start_child_parent_closed_without_abandon_or_unstarted_is_noop() {
    for (started, policy) in [(true, ParentClosePolicy::Terminate), (false, ParentClosePolicy::Abandon), (false, ParentClosePolicy::Terminate)] {
        let h = harness();
        with_child(&h, started, policy);
        h.ms.set(|s| s.running = false);
        h.exec.process_start_child_execution(&sctask()).await.unwrap();
        assert!(h.deps.log().is_empty() && h.ms.calls().is_empty(), "started={started} policy={policy:?}");
    }
}

#[tokio::test]
async fn start_child_duplication_noop_and_schedule_decision_completed_ignored() {
    let h = harness(); // no child info
    h.exec.process_start_child_execution(&sctask()).await.unwrap();
    assert!(h.deps.log().is_empty());
    let h = harness();
    with_child(&h, false, ParentClosePolicy::Abandon);
    h.deps.errors.lock().unwrap().insert("schedule_decision", TaskError::WorkflowExecutionAlreadyCompleted("done".into()));
    h.exec.process_start_child_execution(&sctask()).await.unwrap();
}

#[tokio::test]
async fn start_child_target_domain_deleted_falls_back_to_domain_id() {
    let h = harness();
    with_child(&h, false, ParentClosePolicy::Abandon);
    let mut t = sctask();
    t.target_domain_id = "missing-domain".into();
    h.exec.process_start_child_execution(&t).await.unwrap();
    assert!(h.deps.log()[0].starts_with("start:missing-domain:"));
    assert!(h.ms.calls()[0].starts_with("child_started:child-domain:"));
}

// ---------------------------------------------------------------------------
// record workflow started / upsert search attributes / reset / ephemeral / global version mismatch
// ---------------------------------------------------------------------------
fn gwi() -> WorkflowIdentifier {
    WorkflowIdentifier { domain_id: GLOBAL.into(), workflow_id: "wf".into(), run_id: "run".into() }
}
fn started_task(domain_global: bool, version: i64) -> RecordWorkflowStartedTask {
    RecordWorkflowStartedTask { workflow_identifier: if domain_global { gwi() } else { wi() }, task_data: td(version) }
}
fn upsert_task(domain_global: bool, version: i64) -> UpsertWorkflowSearchAttributesTask {
    UpsertWorkflowSearchAttributesTask { workflow_identifier: if domain_global { gwi() } else { wi() }, task_data: td(version), task_list: "tl".into() }
}

#[tokio::test]
async fn record_workflow_started_records_visibility() {
    let h = harness();
    h.exec.process_record_workflow_started(&started_task(false, 0)).await.unwrap();
    let started = h.tdeps.started.lock().unwrap().clone();
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].workflow_id, "wf");
    assert_eq!(started[0].workflow_type_name, "wt");
    assert_eq!(started[0].task_list, "tl");
    assert_eq!(started[0].workflow_timeout, 100);
    assert_eq!(started[0].update_timestamp, NOW);
    assert!(started[0].search_attributes.is_empty());
    assert!(h.tdeps.upserted.lock().unwrap().is_empty());
    assert!(released_ok(&h));
}

#[tokio::test]
async fn record_workflow_started_not_running_is_noop() {
    let h = harness();
    h.ms.set(|s| s.running = false);
    h.exec.process_record_workflow_started(&started_task(false, 0)).await.unwrap();
    assert!(h.tdeps.started.lock().unwrap().is_empty());
}

#[tokio::test]
async fn record_workflow_started_global_domain_version_mismatch_is_noop() {
    let h = harness();
    h.ms.set(|s| s.start_version = 4);
    h.exec.process_record_workflow_started(&started_task(true, 3)).await.unwrap();
    assert!(h.tdeps.started.lock().unwrap().is_empty());
    // matching version records
    h.exec.process_record_workflow_started(&started_task(true, 4)).await.unwrap();
    assert_eq!(h.tdeps.started.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn record_workflow_started_with_context_header() {
    let h = harness();
    h.tdeps.context_header.store(true, Ordering::SeqCst);
    h.ms.set(|s| {
        let mut headers = BytesMap::new();
        headers.insert("foo".into(), b"bar".to_vec());
        headers.insert("ignored".into(), b"x".to_vec());
        s.start.headers = Some(headers);
    });
    h.exec.process_record_workflow_started(&started_task(false, 0)).await.unwrap();
    let started = h.tdeps.started.lock().unwrap().clone();
    assert_eq!(started[0].search_attributes.len(), 1);
    assert_eq!(started[0].search_attributes.get("Header_foo").unwrap(), &b"\"bar\"".to_vec());
}

#[tokio::test]
async fn upsert_search_attributes_records_visibility_without_version_check() {
    let h = harness();
    h.ms.set(|s| s.start_version = 4);
    h.exec.process_upsert_workflow_search_attributes(&upsert_task(true, 3)).await.unwrap();
    assert_eq!(h.tdeps.upserted.lock().unwrap().len(), 1);
    assert!(h.tdeps.started.lock().unwrap().is_empty());
}

#[tokio::test]
async fn upsert_search_attributes_with_context_header() {
    let h = harness();
    h.tdeps.context_header.store(true, Ordering::SeqCst);
    h.ms.set(|s| {
        let mut headers = BytesMap::new();
        headers.insert("foo".into(), b"bar".to_vec());
        s.start.headers = Some(headers);
    });
    h.exec.process_upsert_workflow_search_attributes(&upsert_task(false, 0)).await.unwrap();
    let up = h.tdeps.upserted.lock().unwrap().clone();
    assert_eq!(up[0].search_attributes.get("Header_foo").unwrap(), &b"\"bar\"".to_vec());
}

#[tokio::test]
async fn activity_task_list_kind_ephemeral_is_forwarded() {
    let h = harness();
    h.ms.set(|s| {
        s.activities.insert(5, ActiveActivityInfo { version: 0, schedule_to_start_timeout: 20, task_list: "act-tl".into(), task_list_kind: TaskListKind::Ephemeral });
    });
    h.exec.process_activity_task(&atask()).await.unwrap();
    assert_eq!(h.tdeps.activity.lock().unwrap()[0].task_list.kind, TaskListKind::Ephemeral);
}

#[tokio::test]
async fn activity_global_domain_version_mismatch_is_noop() {
    let h = harness();
    h.ms.set(|s| {
        s.activities.insert(5, ActiveActivityInfo { version: 4, schedule_to_start_timeout: 20, task_list: "act-tl".into(), task_list_kind: TaskListKind::Normal });
    });
    let mut t = atask();
    t.workflow_identifier = gwi();
    t.target_domain_id = GLOBAL.into();
    t.task_data = td(3);
    h.exec.process_activity_task(&t).await.unwrap();
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
}

#[tokio::test]
async fn decision_global_domain_version_mismatch_is_noop() {
    let h = harness();
    h.ms.set(|s| {
        s.decisions.insert(5, ActiveDecisionInfo { version: 4 });
    });
    let mut t = dtask("tl");
    t.workflow_identifier = gwi();
    t.target_domain_id = GLOBAL.into();
    t.task_data = td(3);
    h.exec.process_decision_task(&t).await.unwrap();
    assert!(h.tdeps.decision.lock().unwrap().is_empty());
}

fn reset_task() -> ResetWorkflowTask {
    ResetWorkflowTask { workflow_identifier: wi(), task_data: td(0) }
}

#[tokio::test]
async fn reset_workflow_skipped_without_reset_point() {
    // fake mutable state has no auto-reset point
    let h = harness();
    h.exec.process_reset_workflow(&reset_task()).await.unwrap();
    assert!(h.deps.log().is_empty());
    assert!(released_ok(&h));
}

#[tokio::test]
async fn reset_workflow_skipped_with_pending_child_executions() {
    let h = harness();
    h.ms.set(|s| {
        s.children.insert(11, child(true, ParentClosePolicy::Abandon));
    });
    h.exec.process_reset_workflow(&reset_task()).await.unwrap();
    assert!(h.deps.log().is_empty());
}

fn with_reset_point(h: &H) {
    h.ms.set(|s| {
        s.with_version_history = true;
        s.reset_point = Some(("bad-binary".into(), ResetPointInfo { run_id: "run".into(), binary_checksum: "bin".into(), first_decision_completed_id: 4 }));
    });
}

#[tokio::test]
async fn reset_workflow_success_path_builds_request_from_reset_point() {
    // Go TestProcessResetWorkflow success: rebuild up to firstDecisionCompletedID-1 on the base branch.
    let h = harness();
    with_reset_point(&h);
    h.exec.process_reset_workflow(&reset_task()).await.unwrap();
    assert_eq!(
        h.deps.log(),
        vec!["reset:domain:wf/0:base=run:last=3@30:next=10:branch=branch:reason=bad-binary:skip_signal=false:new_run_set=true:req_set=true".to_string()]
    );
    assert!(released_ok(&h));
}

#[tokio::test]
async fn reset_workflow_bad_request_is_dropped() {
    let h = harness();
    with_reset_point(&h);
    h.deps.errors.lock().unwrap().insert("reset", TaskError::BadRequest("corrupt".into()));
    h.exec.process_reset_workflow(&reset_task()).await.unwrap();
    assert_eq!(h.deps.log().len(), 1);
}

#[tokio::test]
async fn reset_workflow_other_error_is_retried() {
    let h = harness();
    with_reset_point(&h);
    h.deps.errors.lock().unwrap().insert("reset", TaskError::InternalService("boom".into()));
    let err = h.exec.process_reset_workflow(&reset_task()).await.unwrap_err();
    assert_eq!(err, TaskError::InternalService("boom".into()));
}

#[tokio::test]
async fn reset_workflow_without_version_histories_errors() {
    let h = harness();
    h.ms.set(|s| {
        s.reset_point = Some(("r".into(), ResetPointInfo { run_id: "run".into(), binary_checksum: "bin".into(), first_decision_completed_id: 4 }));
    });
    let err = h.exec.process_reset_workflow(&reset_task()).await.unwrap_err();
    assert_eq!(err, TaskError::MissingVersionHistories);
}

#[tokio::test]
async fn decision_ephemeral_task_list_kind_is_forwarded() {
    // Same task list as mutable state: the kind comes from the execution info (Go: executionInfo.TaskList kind).
    let h = harness();
    h.ms.set(|s| {
        s.decisions.insert(5, ActiveDecisionInfo { version: 0 });
        s.info.task_list_kind = TaskListKind::Ephemeral;
    });
    h.exec.process_decision_task(&dtask("tl")).await.unwrap();
    let pushed = h.tdeps.decision.lock().unwrap().clone();
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].task_list.kind, TaskListKind::Ephemeral);
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 100);
}

#[test]
fn copy_search_attributes_nil_and_deep_copy() {
    assert!(copy_search_attributes(None).is_none());
    let mut input = BytesMap::new();
    input.insert("CustomKeywordField".to_string(), b"\"v\"".to_vec());
    let mut out = copy_search_attributes(Some(&input)).unwrap();
    assert_eq!(out, input);
    out.get_mut("CustomKeywordField").unwrap().push(b'x');
    assert_eq!(input.get("CustomKeywordField").unwrap(), &b"\"v\"".to_vec());
    // empty (non-nil) input stays empty non-nil, as Go make(map) does.
    assert_eq!(copy_search_attributes(Some(&BytesMap::new())), Some(BytesMap::new()));
}
