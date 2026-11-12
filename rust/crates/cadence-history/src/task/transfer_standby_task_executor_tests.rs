//! Ports of go/service/history/task/transfer_standby_task_executor_test.go against fake
//! `TransferWorkflow` / `TransferStandbyDependencies` (the Rust executor is trait based).
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use super::deps::*;
use super::standby_task_util::{HistoryResendInfo, TaskDLQWriter};
use super::standby_test_support::*;
use super::transfer_standby_task_executor::*;
use super::transfer_task_executor_base::*;

struct S {
    running: bool,
    activity: Option<TransferActivityInfo>,
    decision: Option<TransferDecisionInfo>,
    cancel: Option<TransferPendingInfo>,
    signal: Option<TransferPendingInfo>,
    child: Option<TransferPendingInfo>,
    task_list: TaskList,
    workflow_timeout: i32,
    sticky_timeout: i32,
    last_write_version: i64,
    start_version: i64,
    has_pending: bool,
    has_in_flight: bool,
    has_processed: bool,
    start_event: StartEvent,
    headers: BytesMap,
    released: Vec<Option<TaskError>>,
}

struct W(Mutex<S>);
impl W {
    fn new() -> Arc<W> {
        Arc::new(W(Mutex::new(S {
            running: true,
            activity: None,
            decision: None,
            cancel: None,
            signal: None,
            child: None,
            task_list: TaskList { name: "wf-tl".into(), kind: TaskListKind::Normal },
            workflow_timeout: 100,
            sticky_timeout: 7,
            last_write_version: VERSION,
            start_version: VERSION,
            has_pending: false,
            has_in_flight: false,
            has_processed: false,
            start_event: StartEvent { timestamp: NOW, first_decision_task_backoff_seconds: 0, headers: None },
            headers: BytesMap::new(),
            released: vec![],
        })))
    }
    fn set(&self, f: impl FnOnce(&mut S)) {
        f(&mut self.0.lock().unwrap());
    }
    fn released(&self) -> Vec<Option<TaskError>> {
        self.0.lock().unwrap().released.clone()
    }
}
impl DecisionState for W {
    fn has_pending_decision(&self) -> bool {
        self.0.lock().unwrap().has_pending
    }
    fn has_in_flight_decision(&self) -> bool {
        self.0.lock().unwrap().has_in_flight
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.0.lock().unwrap().has_processed
    }
}
#[async_trait]
impl TransferWorkflow for W {
    fn release(&self, e: Option<TaskError>) {
        self.0.lock().unwrap().released.push(e);
    }
    fn running(&self) -> bool {
        self.0.lock().unwrap().running
    }
    fn activity_info(&self, _: i64) -> Option<TransferActivityInfo> {
        self.0.lock().unwrap().activity.clone()
    }
    fn decision_info(&self, _: i64) -> Option<TransferDecisionInfo> {
        self.0.lock().unwrap().decision.clone()
    }
    fn request_cancel_info(&self, _: i64) -> Option<TransferPendingInfo> {
        self.0.lock().unwrap().cancel.clone()
    }
    fn signal_info(&self, _: i64) -> Option<TransferPendingInfo> {
        self.0.lock().unwrap().signal.clone()
    }
    fn child_execution_info(&self, _: i64) -> Option<TransferPendingInfo> {
        self.0.lock().unwrap().child.clone()
    }
    fn task_list(&self) -> TaskList {
        self.0.lock().unwrap().task_list.clone()
    }
    fn workflow_timeout(&self) -> i32 {
        self.0.lock().unwrap().workflow_timeout
    }
    fn sticky_schedule_to_start_timeout(&self) -> i32 {
        self.0.lock().unwrap().sticky_timeout
    }
    fn partition_config(&self) -> BytesMap {
        BytesMap::new()
    }
    fn history_resend_info(&self) -> Result<HistoryResendInfo> {
        Ok(HistoryResendInfo { last_event_id: Some(9), last_event_version: Some(VERSION) })
    }
    fn last_write_version(&self) -> Result<i64> {
        Ok(self.0.lock().unwrap().last_write_version)
    }
    fn start_version(&self) -> Result<i64> {
        Ok(self.0.lock().unwrap().start_version)
    }
    async fn start_event(&self) -> Result<StartEvent> {
        let s = self.0.lock().unwrap();
        let mut e = s.start_event.clone();
        if e.headers.is_none() && !s.headers.is_empty() {
            e.headers = Some(s.headers.clone());
        }
        Ok(e)
    }
    async fn visibility_record(&self, task: &dyn PersistenceTask, _: Time, _: bool) -> Result<VisibilityRecord> {
        Ok(VisibilityRecord {
            domain_id: task.get_domain_id().into(),
            domain: String::new(),
            workflow_id: task.get_workflow_id().into(),
            run_id: task.get_run_id().into(),
            workflow_type_name: "wf-type".into(),
            start_timestamp: NOW,
            execution_timestamp: NOW,
            close_timestamp: 0,
            workflow_timeout: 100,
            task_id: task.get_task_id(),
            memo: None,
            task_list: "wf-tl".into(),
            is_cron: false,
            cron_schedule: String::new(),
            num_clusters: 2,
            cluster_attribute: None,
            update_timestamp: NOW,
            search_attributes: BytesMap::new(),
            headers: self.0.lock().unwrap().headers.clone(),
            shard_id: 0,
            execution_status: WorkflowExecutionStatus::Started,
            scheduled_execution_timestamp: 0,
            close_status: 0,
            history_length: 3,
            retention_seconds: 0,
        })
    }
}

struct SDeps {
    w: Option<Arc<W>>,
    now: AtomicI64,
    v2: bool,
    advanced: bool,
    resent: Mutex<Vec<String>>,
    dlq: Arc<Dlq>,
}
#[async_trait]
impl TransferStandbyDependencies for SDeps {
    async fn load_workflow(&self, _: &dyn PersistenceTask, _: i64) -> Result<Option<Arc<dyn TransferWorkflow>>> {
        Ok(self.w.clone().map(|w| w as Arc<dyn TransferWorkflow>))
    }
    fn enable_transfer_queue_v2(&self, _: i32) -> bool {
        self.v2
    }
    fn current_time(&self, _: &str) -> Time {
        Time(self.now.load(Ordering::SeqCst))
    }
    fn resend_delay(&self) -> Duration {
        Duration::from_secs(RESEND_DELAY_SECS as u64)
    }
    fn discard_delay(&self) -> Duration {
        Duration::from_secs(DISCARD_DELAY_SECS as u64)
    }
    fn advanced_visibility_writing_enabled(&self) -> bool {
        self.advanced
    }
    async fn send_single_workflow_history(&self, cluster: &str, _: &dyn PersistenceTask, id: i64, version: i64) -> Result<()> {
        self.resent.lock().unwrap().push(format!("{cluster}:{id}:{version}"));
        Ok(())
    }
    fn dlq_writer(&self) -> Arc<dyn TaskDLQWriter> {
        self.dlq.clone()
    }
    fn history_task_dlq_mode(&self) -> StringPropertyFnWithDomainFilter {
        dlq_mode_enabled()
    }
}

#[derive(Default)]
struct TDeps {
    activity: Mutex<Vec<AddMatchingTaskRequest>>,
    decision: Mutex<Vec<AddMatchingTaskRequest>>,
    started: Mutex<Vec<VisibilityRecord>>,
    upserted: Mutex<Vec<VisibilityRecord>>,
    closed: Mutex<Vec<VisibilityRecord>>,
    context_header: bool,
}
#[async_trait]
impl TransferDependencies for TDeps {
    fn standby_task_missing_events_discard_delay(&self) -> Duration {
        Duration::from_secs(DISCARD_DELAY_SECS as u64)
    }
    fn dlq_writer(&self) -> Arc<dyn TaskDLQWriter> {
        Arc::new(Dlq::default())
    }
    fn history_task_dlq_mode(&self) -> StringPropertyFnWithDomainFilter {
        dlq_mode_enabled()
    }
    async fn add_activity_task(&self, r: AddMatchingTaskRequest) -> Result<()> {
        self.activity.lock().unwrap().push(r);
        Ok(())
    }
    async fn add_decision_task(&self, r: AddMatchingTaskRequest) -> Result<()> {
        self.decision.lock().unwrap().push(r);
        Ok(())
    }
    fn visibility_domain(&self, _: &str, _: &str) -> Result<VisibilityDomain> {
        Ok(VisibilityDomain { name: "vis-domain".into(), retention_days: 3, ..Default::default() })
    }
    fn enable_context_header_in_visibility(&self, _: &str) -> bool {
        self.context_header
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

struct H {
    w: Arc<W>,
    deps: Arc<SDeps>,
    tdeps: Arc<TDeps>,
    exec: TransferStandbyTaskExecutor,
}
fn harness() -> H {
    harness_with(false, false, false)
}
fn harness_with(v2: bool, advanced: bool, context_header: bool) -> H {
    let w = W::new();
    let deps = Arc::new(SDeps {
        w: Some(w.clone()),
        now: AtomicI64::new(NOW),
        v2,
        advanced,
        resent: Mutex::new(vec![]),
        dlq: Arc::new(Dlq::default()),
    });
    let tdeps = Arc::new(TDeps { context_header, ..Default::default() });
    let base = new_transfer_task_executor_base(shard(), tdeps.clone(), Logger::new());
    let exec = TransferStandbyTaskExecutor { base, cluster_name: STANDBY_CLUSTER.into(), dependencies: deps.clone() };
    H { w, deps, tdeps, exec }
}
fn set_now(h: &H, t: i64) {
    h.deps.now.store(t, Ordering::SeqCst);
}
fn wi(domain: &str) -> WorkflowIdentifier {
    WorkflowIdentifier { domain_id: domain.into(), workflow_id: "wf".into(), run_id: "run".into() }
}
fn td(version: i64) -> TaskData {
    TaskData { version, task_id: 59, visibility_timestamp: Time(NOW) }
}
fn atask(domain: &str, version: i64) -> ActivityTask {
    ActivityTask { workflow_identifier: wi(domain), task_data: td(version), target_domain_id: domain.into(), task_list: "unused".into(), schedule_id: 5, ..Default::default() }
}
fn dtask(domain: &str, task_list: &str) -> DecisionTask {
    DecisionTask { workflow_identifier: wi(domain), task_data: td(VERSION), target_domain_id: domain.into(), task_list: task_list.into(), schedule_id: 5, ..Default::default() }
}
fn act_info(version: i64, started: i64, kind: TaskListKind) -> TransferActivityInfo {
    TransferActivityInfo { version, started_id: started, task_list: TaskList { name: "act-tl".into(), kind }, schedule_to_start_timeout: 20 }
}
fn pending(version: i64, started: i64) -> TransferPendingInfo {
    TransferPendingInfo { version, started_id: started }
}
fn is_redispatch(r: &Result<()>) -> bool {
    matches!(r, Err(e) if e.is_redispatch())
}
fn no_error_released(h: &H) -> bool {
    let r = h.w.released();
    !r.is_empty() && r.iter().all(|e| e.is_none())
}

// ---------------------------------------------------------------------------
// activity
// ---------------------------------------------------------------------------
#[tokio::test]
async fn activity_pending_redispatches() {
    let h = harness();
    h.w.set(|s| s.activity = Some(act_info(VERSION, EMPTY_EVENT_ID, TaskListKind::Normal)));
    let r = h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await;
    assert!(is_redispatch(&r), "{r:?}");
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
    assert!(no_error_released(&h));
}

#[tokio::test]
async fn activity_pending_active_active_domain_redispatches() {
    let h = harness_with(true, false, false);
    h.w.set(|s| s.activity = Some(act_info(VERSION, EMPTY_EVENT_ID, TaskListKind::Normal)));
    set_now(&h, fetch_time());
    let r = h.exec.process_activity_task(&atask(AA_DOMAIN, VERSION)).await;
    // active in the other cluster: no push to matching, task keeps being redispatched
    assert!(is_redispatch(&r), "{r:?}");
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
}

#[tokio::test]
async fn activity_pending_push_to_matching() {
    let h = harness();
    h.w.set(|s| s.activity = Some(act_info(VERSION, EMPTY_EVENT_ID, TaskListKind::Normal)));
    set_now(&h, fetch_time());
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    let pushed = h.tdeps.activity.lock().unwrap().clone();
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].task_list.name, "act-tl");
    assert_eq!(pushed[0].task_list.kind, TaskListKind::Normal);
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 20);
    assert_eq!(pushed[0].schedule_id, 5);
    assert_eq!(pushed[0].domain_uuid, GLOBAL_DOMAIN);
    assert_eq!(pushed[0].source_domain_uuid.as_deref(), Some(GLOBAL_DOMAIN));
}

#[tokio::test]
async fn activity_pending_task_list_kind_ephemeral() {
    let h = harness();
    h.w.set(|s| s.activity = Some(act_info(VERSION, EMPTY_EVENT_ID, TaskListKind::Ephemeral)));
    set_now(&h, fetch_time());
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    let pushed = h.tdeps.activity.lock().unwrap().clone();
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].task_list.kind, TaskListKind::Ephemeral);
}

#[tokio::test]
async fn activity_empty_tasklist_falls_back_to_task() {
    let h = harness();
    let mut info = act_info(VERSION, EMPTY_EVENT_ID, TaskListKind::Normal);
    info.task_list.name = String::new();
    h.w.set(|s| s.activity = Some(info));
    set_now(&h, fetch_time());
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    assert_eq!(h.tdeps.activity.lock().unwrap()[0].task_list.name, "unused");
}

#[tokio::test]
async fn activity_success_when_already_started() {
    let h = harness();
    h.w.set(|s| s.activity = Some(act_info(VERSION, 7, TaskListKind::Normal)));
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
    assert!(no_error_released(&h));
}

#[tokio::test]
async fn activity_noop_when_activity_missing_or_workflow_not_loaded() {
    let h = harness();
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
    let tdeps = Arc::new(TDeps::default());
    let deps = Arc::new(SDeps {
        w: None,
        now: AtomicI64::new(fetch_time()),
        v2: false,
        advanced: false,
        resent: Mutex::new(vec![]),
        dlq: Arc::new(Dlq::default()),
    });
    let base = new_transfer_task_executor_base(shard(), tdeps.clone(), Logger::new());
    let exec = TransferStandbyTaskExecutor { base, cluster_name: STANDBY_CLUSTER.into(), dependencies: deps };
    exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    assert!(tdeps.activity.lock().unwrap().is_empty());
}

#[tokio::test]
async fn activity_version_mismatch_global_domain_is_noop() {
    let h = harness();
    h.w.set(|s| s.activity = Some(act_info(VERSION + 1, EMPTY_EVENT_ID, TaskListKind::Normal)));
    set_now(&h, fetch_time());
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
}

#[tokio::test]
async fn activity_version_mismatch_local_domain_still_pushes() {
    let h = harness();
    h.w.set(|s| s.activity = Some(act_info(VERSION + 1, EMPTY_EVENT_ID, TaskListKind::Normal)));
    set_now(&h, fetch_time());
    h.exec.process_activity_task(&atask(LOCAL_DOMAIN, VERSION)).await.unwrap();
    assert_eq!(h.tdeps.activity.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn activity_discarded_writes_to_dlq() {
    let h = harness();
    h.w.set(|s| s.activity = Some(act_info(VERSION, EMPTY_EVENT_ID, TaskListKind::Normal)));
    set_now(&h, discard_time());
    // Past the discard delay the activity is still pushed to matching (domain is active here).
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    assert_eq!(h.tdeps.activity.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// decision
// ---------------------------------------------------------------------------
fn dec_info(version: i64, started: i64) -> TransferDecisionInfo {
    TransferDecisionInfo { version, started_id: started }
}

#[tokio::test]
async fn decision_pending_redispatches() {
    let h = harness();
    h.w.set(|s| s.decision = Some(dec_info(VERSION, EMPTY_EVENT_ID)));
    let r = h.exec.process_decision_task(&dtask(GLOBAL_DOMAIN, "wf-tl")).await;
    assert!(is_redispatch(&r), "{r:?}");
    assert!(h.tdeps.decision.lock().unwrap().is_empty());
}

#[tokio::test]
async fn decision_pending_active_active_domain_redispatches() {
    let h = harness_with(true, false, false);
    h.w.set(|s| s.decision = Some(dec_info(VERSION, EMPTY_EVENT_ID)));
    set_now(&h, fetch_time());
    let r = h.exec.process_decision_task(&dtask(AA_DOMAIN, "wf-tl")).await;
    assert!(is_redispatch(&r), "{r:?}");
    assert!(h.tdeps.decision.lock().unwrap().is_empty());
}

#[tokio::test]
async fn decision_pending_push_to_matching_normal() {
    let h = harness();
    h.w.set(|s| s.decision = Some(dec_info(VERSION, EMPTY_EVENT_ID)));
    set_now(&h, fetch_time());
    h.exec.process_decision_task(&dtask(GLOBAL_DOMAIN, "wf-tl")).await.unwrap();
    let pushed = h.tdeps.decision.lock().unwrap().clone();
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].task_list.name, "wf-tl");
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 100);
    assert_eq!(pushed[0].domain_uuid, GLOBAL_DOMAIN);
    assert!(pushed[0].source_domain_uuid.is_none());
}

#[tokio::test]
async fn decision_pending_push_to_matching_sticky_uses_sticky_timeout() {
    let h = harness();
    h.w.set(|s| s.decision = Some(dec_info(VERSION, EMPTY_EVENT_ID)));
    set_now(&h, fetch_time());
    h.exec.process_decision_task(&dtask(GLOBAL_DOMAIN, "sticky-tl")).await.unwrap();
    let pushed = h.tdeps.decision.lock().unwrap().clone();
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 7);
}

#[tokio::test]
async fn decision_pending_task_list_kind_ephemeral() {
    let h = harness();
    h.w.set(|s| {
        s.decision = Some(dec_info(VERSION, EMPTY_EVENT_ID));
        s.task_list = TaskList { name: "wf-tl".into(), kind: TaskListKind::Ephemeral };
    });
    set_now(&h, fetch_time());
    h.exec.process_decision_task(&dtask(GLOBAL_DOMAIN, "wf-tl")).await.unwrap();
    assert_eq!(h.tdeps.decision.lock().unwrap()[0].task_list.kind, TaskListKind::Ephemeral);
}

#[tokio::test]
async fn decision_success_first_and_non_first_decision() {
    // decision already started (first or later decision): nothing to push.
    for started in [6, 12] {
        let h = harness();
        h.w.set(|s| s.decision = Some(dec_info(VERSION, started)));
        h.exec.process_decision_task(&dtask(GLOBAL_DOMAIN, "wf-tl")).await.unwrap();
        assert!(h.tdeps.decision.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn decision_version_mismatch_global_domain_is_noop() {
    let h = harness();
    h.w.set(|s| s.decision = Some(dec_info(VERSION + 1, EMPTY_EVENT_ID)));
    set_now(&h, fetch_time());
    h.exec.process_decision_task(&dtask(GLOBAL_DOMAIN, "wf-tl")).await.unwrap();
    assert!(h.tdeps.decision.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// close execution / record workflow closed
// ---------------------------------------------------------------------------
fn close_task(version: i64) -> CloseExecutionTask {
    CloseExecutionTask { workflow_identifier: wi(GLOBAL_DOMAIN), task_data: td(version) }
}

#[tokio::test]
async fn close_execution_records_closed_when_workflow_closed() {
    let h = harness();
    h.w.set(|s| s.running = false);
    h.exec.process_close_execution(&close_task(VERSION)).await.unwrap();
    let closed = h.tdeps.closed.lock().unwrap().clone();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].workflow_id, "wf");
    assert_eq!(closed[0].domain, "vis-domain");
    assert_eq!(closed[0].retention_seconds, 3 * 86400);
}

#[tokio::test]
async fn record_workflow_closed_task_records_closed() {
    let h = harness();
    h.w.set(|s| s.running = false);
    let t = RecordWorkflowClosedTask { workflow_identifier: wi(GLOBAL_DOMAIN), task_data: td(VERSION) };
    h.exec.process_close_execution(&t).await.unwrap();
    assert_eq!(h.tdeps.closed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn close_execution_noop_while_running_or_version_mismatch() {
    let h = harness();
    h.exec.process_close_execution(&close_task(VERSION)).await.unwrap();
    assert!(h.tdeps.closed.lock().unwrap().is_empty());
    h.w.set(|s| s.running = false);
    h.exec.process_close_execution(&close_task(VERSION + 1)).await.unwrap();
    assert!(h.tdeps.closed.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// cancel / signal / start child (same pending / fetch / discard shape)
// ---------------------------------------------------------------------------
#[derive(Clone, Copy, Debug)]
enum Kind {
    Cancel,
    Signal,
    Child,
}
async fn run_kind(h: &H, kind: Kind, domain: &str, version: i64) -> Result<()> {
    let target = "target-domain".to_string();
    match kind {
        Kind::Cancel => {
            let t = CancelExecutionTask { workflow_identifier: wi(domain), task_data: td(version), target_domain_id: target, target_workflow_id: "t".into(), target_run_id: "r".into(), target_child_workflow_only: false, initiated_id: 11 };
            h.exec.process_cancel_execution(&t).await
        }
        Kind::Signal => {
            let t = SignalExecutionTask { workflow_identifier: wi(domain), task_data: td(version), target_domain_id: target, target_workflow_id: "t".into(), target_run_id: "r".into(), target_child_workflow_only: false, initiated_id: 11 };
            h.exec.process_signal_execution(&t).await
        }
        Kind::Child => {
            let t = StartChildExecutionTask { workflow_identifier: wi(domain), task_data: td(version), target_domain_id: target, target_workflow_id: "t".into(), initiated_id: 11, task_list: "tl".into() };
            h.exec.process_start_child_execution(&t).await
        }
    }
}
fn set_kind(h: &H, kind: Kind, info: Option<TransferPendingInfo>) {
    h.w.set(|s| match kind {
        Kind::Cancel => s.cancel = info,
        Kind::Signal => s.signal = info,
        Kind::Child => s.child = info,
    });
}
const KINDS: [Kind; 3] = [Kind::Cancel, Kind::Signal, Kind::Child];

#[tokio::test]
async fn cancel_signal_child_pending_redispatch_before_resend_delay() {
    for kind in KINDS {
        let h = harness();
        set_kind(&h, kind, Some(pending(VERSION, EMPTY_EVENT_ID)));
        let r = run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await;
        assert!(is_redispatch(&r), "{kind:?}: {r:?}");
        assert!(h.deps.resent.lock().unwrap().is_empty(), "{kind:?}");
        assert!(no_error_released(&h), "{kind:?}");
    }
}

#[tokio::test]
async fn cancel_signal_child_pending_fetches_history_from_remote() {
    for kind in KINDS {
        let h = harness();
        set_kind(&h, kind, Some(pending(VERSION, EMPTY_EVENT_ID)));
        set_now(&h, fetch_time());
        let r = run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await;
        assert!(is_redispatch(&r), "{kind:?}: {r:?}");
        assert_eq!(*h.deps.resent.lock().unwrap(), vec![format!("{STANDBY_CLUSTER}:9:{VERSION}")], "{kind:?}");
        assert!(h.deps.dlq.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancel_signal_child_pending_active_active_domain_resends_to_remote_cluster() {
    for kind in KINDS {
        let h = harness_with(true, false, false);
        set_kind(&h, kind, Some(pending(VERSION, EMPTY_EVENT_ID)));
        set_now(&h, fetch_time());
        let r = run_kind(&h, kind, AA_DOMAIN, VERSION).await;
        assert!(is_redispatch(&r), "{kind:?}: {r:?}");
        assert_eq!(*h.deps.resent.lock().unwrap(), vec![format!("{ALT_CLUSTER}:9:{VERSION}")], "{kind:?}");
    }
}

#[tokio::test]
async fn cancel_signal_child_pending_past_discard_delay_writes_to_dlq() {
    for kind in KINDS {
        let h = harness();
        set_kind(&h, kind, Some(pending(VERSION, EMPTY_EVENT_ID)));
        set_now(&h, discard_time());
        run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await.unwrap();
        assert!(h.deps.resent.lock().unwrap().is_empty());
        let calls = h.deps.dlq.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2, "{kind:?}: {calls:?}");
        assert!(calls[0].starts_with(&format!("task:{GLOBAL_DOMAIN}:")), "{kind:?}: {calls:?}");
        assert!(calls[0].ends_with(":59"), "{kind:?}: {calls:?}");
        assert_eq!(calls[1], format!("ack:{GLOBAL_DOMAIN}"), "{kind:?}");
    }
}

#[tokio::test]
async fn cancel_signal_child_success_when_info_missing() {
    for kind in KINDS {
        let h = harness();
        run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await.unwrap();
        assert!(h.deps.resent.lock().unwrap().is_empty(), "{kind:?}");
        assert!(no_error_released(&h), "{kind:?}");
    }
}

#[tokio::test]
async fn child_already_started_is_noop() {
    let h = harness();
    set_kind(&h, Kind::Child, Some(pending(VERSION, 20)));
    set_now(&h, fetch_time());
    run_kind(&h, Kind::Child, GLOBAL_DOMAIN, VERSION).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancel_signal_child_version_mismatch_global_domain_is_noop() {
    for kind in KINDS {
        let h = harness();
        set_kind(&h, kind, Some(pending(VERSION + 1, EMPTY_EVENT_ID)));
        set_now(&h, fetch_time());
        run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await.unwrap();
        assert!(h.deps.resent.lock().unwrap().is_empty(), "{kind:?}");
    }
}

// ---------------------------------------------------------------------------
// record workflow started / upsert search attributes
// ---------------------------------------------------------------------------
fn started_task(version: i64) -> RecordWorkflowStartedTask {
    RecordWorkflowStartedTask { workflow_identifier: wi(GLOBAL_DOMAIN), task_data: td(version) }
}
fn upsert_task() -> UpsertWorkflowSearchAttributesTask {
    UpsertWorkflowSearchAttributesTask { workflow_identifier: wi(GLOBAL_DOMAIN), task_data: td(VERSION), task_list: "tl".into() }
}

#[tokio::test]
async fn record_workflow_started_records_visibility() {
    let h = harness();
    h.exec.process_record_workflow_started(&started_task(VERSION)).await.unwrap();
    let started = h.tdeps.started.lock().unwrap().clone();
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].workflow_id, "wf");
    assert_eq!(started[0].domain, "vis-domain");
    assert_eq!(started[0].execution_status, WorkflowExecutionStatus::Started);
    assert!(started[0].search_attributes.is_empty());
}

#[tokio::test]
async fn record_workflow_started_version_mismatch_is_noop() {
    let h = harness();
    h.w.set(|s| s.start_version = VERSION + 1);
    h.exec.process_record_workflow_started(&started_task(VERSION)).await.unwrap();
    assert!(h.tdeps.started.lock().unwrap().is_empty());
}

#[tokio::test]
async fn record_workflow_started_pending_status_with_advanced_visibility() {
    let h = harness_with(false, true, false);
    h.w.set(|s| s.start_event.first_decision_task_backoff_seconds = 30);
    h.exec.process_record_workflow_started(&started_task(VERSION)).await.unwrap();
    let started = h.tdeps.started.lock().unwrap().clone();
    assert_eq!(started[0].execution_status, WorkflowExecutionStatus::Pending);
    assert_eq!(started[0].scheduled_execution_timestamp, NOW + 30 * SEC);
}

#[tokio::test]
async fn record_workflow_started_pending_becomes_started_without_advanced_visibility() {
    let h = harness();
    h.w.set(|s| s.start_event.first_decision_task_backoff_seconds = 30);
    h.exec.process_record_workflow_started(&started_task(VERSION)).await.unwrap();
    assert_eq!(h.tdeps.started.lock().unwrap()[0].execution_status, WorkflowExecutionStatus::Started);
}

#[tokio::test]
async fn record_workflow_started_with_context_header() {
    let h = harness_with(false, false, true);
    h.w.set(|s| {
        s.headers.insert("foo".into(), b"bar".to_vec());
        s.headers.insert("ignored".into(), b"x".to_vec());
    });
    h.exec.process_record_workflow_started(&started_task(VERSION)).await.unwrap();
    let started = h.tdeps.started.lock().unwrap().clone();
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].search_attributes.len(), 1);
    assert_eq!(started[0].search_attributes.get("Header_foo").unwrap(), &b"\"bar\"".to_vec());
}

#[tokio::test]
async fn upsert_search_attributes_records_visibility() {
    let h = harness();
    h.exec.process_upsert_workflow_search_attributes(&upsert_task()).await.unwrap();
    assert_eq!(h.tdeps.upserted.lock().unwrap().len(), 1);
    assert!(h.tdeps.started.lock().unwrap().is_empty());
}

#[tokio::test]
async fn upsert_search_attributes_ignores_version_mismatch() {
    // Go: only the start task checks the start version; upsert always writes.
    let h = harness();
    h.w.set(|s| s.start_version = VERSION + 1);
    h.exec.process_upsert_workflow_search_attributes(&upsert_task()).await.unwrap();
    assert_eq!(h.tdeps.upserted.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn upsert_search_attributes_with_context_header() {
    let h = harness_with(false, false, true);
    h.w.set(|s| {
        s.headers.insert("foo".into(), b"bar".to_vec());
    });
    h.exec.process_upsert_workflow_search_attributes(&upsert_task()).await.unwrap();
    let up = h.tdeps.upserted.lock().unwrap().clone();
    assert_eq!(up[0].search_attributes.get("Header_foo").unwrap(), &b"\"bar\"".to_vec());
}

#[tokio::test]
async fn not_running_workflow_skips_non_close_actions() {
    let h = harness();
    h.w.set(|s| {
        s.running = false;
        s.activity = Some(act_info(VERSION, EMPTY_EVENT_ID, TaskListKind::Normal));
    });
    set_now(&h, fetch_time());
    h.exec.process_activity_task(&atask(GLOBAL_DOMAIN, VERSION)).await.unwrap();
    assert!(h.tdeps.activity.lock().unwrap().is_empty());
    assert!(no_error_released(&h));
}
