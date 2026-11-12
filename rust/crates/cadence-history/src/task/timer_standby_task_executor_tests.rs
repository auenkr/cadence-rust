//! Ports of go/service/history/task/timer_standby_task_executor_test.go against fake
//! `StandbyTimerWorkflow` / `TimerStandbyDependencies`.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use super::deps::*;
use super::standby_task_util::{HistoryResendInfo, TaskDLQWriter};
use super::standby_test_support::*;
use super::timer_active_task_executor::*;
use super::timer_standby_task_executor::*;
use super::timer_task_executor_base::*;

#[derive(Default)]
struct S {
    running: bool,
    user_timers: Vec<TimerSequenceId>,
    activity_timers: Vec<TimerSequenceId>,
    user_infos: HashMap<i64, UserTimerInfo>,
    activity_infos: HashMap<i64, TimerActivityInfo>,
    decision: Option<TimerDecisionInfo>,
    has_processed: bool,
    start_version: i64,
    last_write_version: i64,
    next_timer_modified: bool,
    released: Vec<Option<TaskError>>,
    calls: Vec<String>,
}

struct W(Mutex<S>);
impl W {
    fn new() -> Arc<W> {
        Arc::new(W(Mutex::new(S { running: true, start_version: VERSION, last_write_version: VERSION, ..Default::default() })))
    }
    fn set(&self, f: impl FnOnce(&mut S)) {
        f(&mut self.0.lock().unwrap());
    }
    fn log(&self, c: impl Into<String>) {
        self.0.lock().unwrap().calls.push(c.into());
    }
    fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().calls.clone()
    }
    fn released(&self) -> Vec<Option<TaskError>> {
        self.0.lock().unwrap().released.clone()
    }
}

#[async_trait]
impl TimerWorkflow for W {
    fn release(&self, e: Option<TaskError>) {
        self.0.lock().unwrap().released.push(e);
    }
    fn running(&self) -> bool {
        self.0.lock().unwrap().running
    }
    fn domain_name(&self) -> String {
        "domain".into()
    }
    fn workflow_type(&self) -> Option<String> {
        Some("wf-type".into())
    }
    fn user_timers(&self) -> Vec<TimerSequenceId> {
        self.0.lock().unwrap().user_timers.clone()
    }
    fn activity_timers(&self) -> Vec<TimerSequenceId> {
        self.0.lock().unwrap().activity_timers.clone()
    }
    fn user_timer_info(&self, id: i64) -> Option<UserTimerInfo> {
        self.0.lock().unwrap().user_infos.get(&id).cloned()
    }
    fn activity_info(&self, id: i64) -> Option<TimerActivityInfo> {
        self.0.lock().unwrap().activity_infos.get(&id).cloned()
    }
    fn decision_info(&self, _: i64) -> Option<TimerDecisionInfo> {
        self.0.lock().unwrap().decision.clone()
    }
    async fn resurrected_timers(&self) -> Result<HashSet<String>> {
        Ok(HashSet::new())
    }
    async fn resurrected_activities(&self) -> Result<HashSet<i64>> {
        Ok(HashSet::new())
    }
    fn delete_user_timer(&self, _: &str) -> Result<()> {
        unimplemented!()
    }
    fn delete_activity(&self, _: i64) -> Result<()> {
        unimplemented!()
    }
    fn add_timer_fired_event(&self, _: &str) -> Result<()> {
        unimplemented!()
    }
    fn update_activity(&self, info: TimerActivityInfo) -> Result<()> {
        self.log(format!("update_activity:{}:{}", info.schedule_id, info.timer_task_status));
        self.0.lock().unwrap().activity_infos.insert(info.schedule_id, info);
        Ok(())
    }
    fn retry_activity(&self, _: &TimerActivityInfo, _: TimerType) -> Result<bool> {
        unimplemented!()
    }
    fn add_activity_task_timed_out_event(&self, _: &TimerActivityInfo, _: TimerType) -> Result<()> {
        unimplemented!()
    }
    fn sticky(&self) -> bool {
        false
    }
    fn add_decision_task_timed_out_event(&self, _: &TimerDecisionInfo) -> Result<()> {
        unimplemented!()
    }
    fn add_decision_task_schedule_to_start_timeout_event(&self, _: i64) -> Result<()> {
        unimplemented!()
    }
    fn decision_attempt(&self) -> i64 {
        0
    }
    fn terminate_decision_attempts_exceeded(&self, _: &str) -> Result<()> {
        unimplemented!()
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.0.lock().unwrap().has_processed
    }
    fn decision_schedule_id(&self) -> i64 {
        EMPTY_EVENT_ID
    }
    fn add_upsert_workflow_search_attributes_task(&self) -> Result<()> {
        unimplemented!()
    }
    fn schedule_decision(&self) -> Result<()> {
        unimplemented!()
    }
    async fn update_workflow_execution_as_active(&self, _: Time) -> Result<()> {
        unimplemented!()
    }
    fn start_version(&self) -> Result<i64> {
        Ok(self.0.lock().unwrap().start_version)
    }
    fn next_event_id(&self) -> i64 {
        10
    }
    fn retry_backoff_duration(&self, _: &str) -> Option<Duration> {
        None
    }
    async fn cron_backoff_duration(&self) -> Result<Option<Duration>> {
        Ok(None)
    }
    fn cancel_requested(&self) -> bool {
        false
    }
    fn timeout_workflow(&self, _: i64) -> Result<()> {
        unimplemented!()
    }
    async fn retry_workflow(&self, _: i64, _: Duration, _: bool, _: &str) -> Result<()> {
        unimplemented!()
    }
    async fn update_workflow_execution_with_new_as_active(&self, _: Time) -> Result<()> {
        unimplemented!()
    }
    async fn activity_scheduled_domain(&self, _: i64) -> Result<String> {
        Ok(String::new())
    }
    fn partition_config(&self) -> BytesMap {
        BytesMap::new()
    }
}

#[async_trait]
impl StandbyTimerWorkflow for W {
    fn last_write_version(&self) -> Result<i64> {
        Ok(self.0.lock().unwrap().last_write_version)
    }
    fn history_resend_info(&self) -> Result<HistoryResendInfo> {
        Ok(HistoryResendInfo { last_event_id: Some(9), last_event_version: Some(VERSION) })
    }
    fn create_next_activity_timer(&self) -> Result<bool> {
        self.log("create_next_activity_timer");
        Ok(self.0.lock().unwrap().next_timer_modified)
    }
    fn update_current_version(&self, version: i64, force: bool) -> Result<()> {
        self.log(format!("update_current_version:{version}:{force}"));
        Ok(())
    }
    async fn update_workflow_execution_as_passive(&self, now: Time) -> Result<()> {
        self.log(format!("update_as_passive:{}", now.0));
        Ok(())
    }
}

struct SDeps {
    w: Option<Arc<W>>,
    now: AtomicI64,
    v2: bool,
    load_err: Option<TaskError>,
    resent: Mutex<Vec<String>>,
    dlq: Arc<Dlq>,
}
#[async_trait]
impl TimerStandbyDependencies for SDeps {
    async fn load_workflow(&self, _: &dyn PersistenceTask, _: i64) -> Result<Option<Arc<dyn StandbyTimerWorkflow>>> {
        if let Some(e) = &self.load_err {
            return Err(e.clone());
        }
        Ok(self.w.clone().map(|w| w as Arc<dyn StandbyTimerWorkflow>))
    }
    fn standby_cluster_delay(&self) -> Duration {
        Duration::from_secs(5)
    }
    fn current_time(&self, _: &str) -> Time {
        Time(self.now.load(Ordering::SeqCst))
    }
    fn enable_timer_queue_v2(&self, _: i32) -> bool {
        self.v2
    }
    fn resend_delay(&self) -> Duration {
        Duration::from_secs(RESEND_DELAY_SECS as u64)
    }
    fn discard_delay(&self) -> Duration {
        Duration::from_secs(DISCARD_DELAY_SECS as u64)
    }
    fn delete_history_event_context_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn dlq_writer(&self) -> Arc<dyn TaskDLQWriter> {
        self.dlq.clone()
    }
    fn history_task_dlq_mode(&self) -> StringPropertyFnWithDomainFilter {
        dlq_mode_enabled()
    }
    async fn send_single_workflow_history(&self, cluster: &str, _: &dyn PersistenceTask, id: i64, version: i64) -> Result<()> {
        self.resent.lock().unwrap().push(format!("{cluster}:{id}:{version}"));
        Ok(())
    }
}

struct NoCleanup;
#[async_trait]
impl CleanupDependencies for NoCleanup {
    async fn get_workflow(&self, _: &dyn PersistenceTask) -> Result<Option<Arc<dyn CleanupWorkflow>>> {
        Ok(None)
    }
    fn history_archival_enabled(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    fn history_archival_uri(&self, _: &str) -> Result<String> {
        Ok(String::new())
    }
    fn archival_size_limit(&self) -> i64 {
        0
    }
    fn archival_time_limit(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn enable_workflow_timer_task_cleanup(&self) -> bool {
        false
    }
    async fn archive(&self, _: ArchiveRequest) -> Result<bool> {
        Ok(false)
    }
    async fn delete_workflow_execution(&self, _: &dyn PersistenceTask, _: &str, _: i32) -> Result<()> {
        Ok(())
    }
    async fn delete_current_workflow_execution(&self, _: &dyn PersistenceTask, _: &str, _: i32) -> Result<()> {
        Ok(())
    }
    async fn delete_active_cluster_selection_policy(&self, _: &dyn PersistenceTask, _: i32) -> Result<()> {
        Ok(())
    }
    async fn delete_history_branch(&self, _: Vec<u8>, _: &str, _: i32) -> Result<()> {
        Ok(())
    }
    async fn delete_workflow_visibility(&self, _: &dyn PersistenceTask, _: &str) -> Result<()> {
        Ok(())
    }
    async fn fetch_workflow_timer_tasks_for_cleanup(&self, _: &dyn PersistenceTask, _: i32) -> Result<Vec<HistoryTaskKey>> {
        Ok(vec![])
    }
    async fn complete_history_task(&self, _: Vec<HistoryTaskKey>, _: HistoryTaskCategory, _: i32) -> Result<()> {
        Ok(())
    }
}

struct H {
    w: Arc<W>,
    deps: Arc<SDeps>,
    exec: TimerStandbyTaskExecutor,
}
fn harness() -> H {
    harness_with(false, None)
}
fn harness_with(v2: bool, load_err: Option<TaskError>) -> H {
    let w = W::new();
    let deps = Arc::new(SDeps {
        w: Some(w.clone()),
        now: AtomicI64::new(NOW),
        v2,
        load_err,
        resent: Mutex::new(vec![]),
        dlq: Arc::new(Dlq::default()),
    });
    let base = new_timer_task_executor_base(shard(), Arc::new(NoCleanup), Logger::new(), Arc::new(Metrics));
    let exec = TimerStandbyTaskExecutor { base, cluster_name: STANDBY_CLUSTER.into(), dependencies: deps.clone() };
    H { w, deps, exec }
}
fn set_now(h: &H, t: i64) {
    h.deps.now.store(t, Ordering::SeqCst);
}
fn wi(domain: &str) -> WorkflowIdentifier {
    WorkflowIdentifier { domain_id: domain.into(), workflow_id: "wf".into(), run_id: "run".into() }
}
fn td(version: i64, vis: i64) -> TaskData {
    TaskData { version, task_id: 59, visibility_timestamp: Time(vis) }
}
fn seq(event_id: i64, t: TimerType, expiry: i64) -> TimerSequenceId {
    TimerSequenceId { event_id, attempt: 0, timer_type: t, expiry: Time(expiry) }
}
fn ainfo(id: i64, status: i32) -> TimerActivityInfo {
    TimerActivityInfo {
        schedule_id: id,
        started_id: EMPTY_EVENT_ID,
        attempt: 0,
        version: VERSION,
        domain_id: String::new(),
        task_list: "tl".into(),
        schedule_to_start_timeout: 2,
        last_heartbeat_timeout_visibility_in_seconds: 0,
        timer_task_status: status,
        details: vec![],
    }
}
fn is_redispatch(r: &Result<()>) -> bool {
    matches!(r, Err(e) if e.is_redispatch())
}
fn released_ok(h: &H) -> bool {
    let r = h.w.released();
    !r.is_empty() && r.iter().all(|e| e.is_none())
}

// ---------------------------------------------------------------------------
// shared pending/fetch/discard checks
// ---------------------------------------------------------------------------
#[derive(Clone, Copy, Debug)]
enum Kind {
    User,
    Activity,
    Decision,
    Backoff,
    Workflow,
}
const KINDS: [Kind; 5] = [Kind::User, Kind::Activity, Kind::Decision, Kind::Backoff, Kind::Workflow];

/// Makes the workflow look like it still has an unfired timer/pending state for `kind`.
fn make_pending(h: &H, kind: Kind) {
    h.w.set(|s| match kind {
        Kind::User => {
            s.user_timers = vec![seq(5, TimerType::StartToClose, NOW - SEC)];
            s.user_infos.insert(5, UserTimerInfo { timer_id: "t".into(), started_id: 5 });
        }
        Kind::Activity => {
            s.activity_timers = vec![seq(5, TimerType::StartToClose, NOW - SEC)];
            s.activity_infos.insert(5, ainfo(5, 0));
        }
        Kind::Decision => s.decision = Some(TimerDecisionInfo { schedule_id: 5, started_id: EMPTY_EVENT_ID, attempt: 0, version: VERSION }),
        Kind::Backoff | Kind::Workflow => {}
    });
}
async fn run_kind(h: &H, kind: Kind, domain: &str, version: i64) -> Result<()> {
    let wid = wi(domain);
    let d = td(version, NOW);
    match kind {
        Kind::User => h.exec.execute_user_timer_timeout_task(&UserTimerTask { workflow_identifier: wid, task_data: d, event_id: 5 }).await,
        Kind::Activity => h.exec.execute_activity_timeout_task(&ActivityTimeoutTask { workflow_identifier: wid, task_data: d, event_id: 5, attempt: 0, timeout_type: 2 }).await,
        Kind::Decision => h.exec.execute_decision_timeout_task(&DecisionTimeoutTask { workflow_identifier: wid, task_data: d, event_id: 5, schedule_attempt: 0, timeout_type: 2 }).await,
        Kind::Backoff => h.exec.execute_workflow_backoff_timer_task(&WorkflowBackoffTimerTask { workflow_identifier: wid, task_data: d, event_id: 0, timeout_type: 1 }).await,
        Kind::Workflow => h.exec.execute_workflow_timeout_task(&WorkflowTimeoutTask { workflow_identifier: wid, task_data: d }).await,
    }
}

#[tokio::test]
async fn pending_redispatches_before_resend_delay() {
    for kind in KINDS {
        let h = harness();
        make_pending(&h, kind);
        let r = run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await;
        assert!(is_redispatch(&r), "{kind:?}: {r:?}");
        assert!(h.deps.resent.lock().unwrap().is_empty(), "{kind:?}");
        assert!(released_ok(&h), "{kind:?}");
    }
}

#[tokio::test]
async fn pending_fetches_history_from_standby_cluster() {
    for kind in KINDS {
        let h = harness();
        make_pending(&h, kind);
        set_now(&h, fetch_time());
        let r = run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await;
        assert!(is_redispatch(&r), "{kind:?}: {r:?}");
        assert_eq!(*h.deps.resent.lock().unwrap(), vec![format!("{STANDBY_CLUSTER}:9:{VERSION}")], "{kind:?}");
    }
}

#[tokio::test]
async fn pending_active_active_domain_fetches_from_remote_active_cluster() {
    for kind in KINDS {
        let h = harness_with(true, None);
        make_pending(&h, kind);
        set_now(&h, fetch_time());
        let r = run_kind(&h, kind, AA_DOMAIN, VERSION).await;
        assert!(is_redispatch(&r), "{kind:?}: {r:?}");
        assert_eq!(*h.deps.resent.lock().unwrap(), vec![format!("{ALT_CLUSTER}:9:{VERSION}")], "{kind:?}");
    }
}

#[tokio::test]
async fn pending_active_active_domain_redispatches_before_resend_delay() {
    for kind in KINDS {
        let h = harness_with(true, None);
        make_pending(&h, kind);
        let r = run_kind(&h, kind, AA_DOMAIN, VERSION).await;
        assert!(is_redispatch(&r), "{kind:?}: {r:?}");
        assert!(h.deps.resent.lock().unwrap().is_empty(), "{kind:?}");
    }
}

#[tokio::test]
async fn pending_past_discard_delay_writes_to_dlq() {
    for kind in KINDS {
        let h = harness();
        make_pending(&h, kind);
        set_now(&h, discard_time());
        run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await.unwrap();
        assert!(h.deps.resent.lock().unwrap().is_empty(), "{kind:?}");
        let calls = h.deps.dlq.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2, "{kind:?}: {calls:?}");
        assert!(calls[0].starts_with(&format!("task:{GLOBAL_DOMAIN}:")), "{kind:?}: {calls:?}");
        assert_eq!(calls[1], format!("ack:{GLOBAL_DOMAIN}"), "{kind:?}");
    }
}

#[tokio::test]
async fn not_running_workflow_is_noop() {
    for kind in KINDS {
        let h = harness();
        make_pending(&h, kind);
        h.w.set(|s| s.running = false);
        set_now(&h, fetch_time());
        run_kind(&h, kind, GLOBAL_DOMAIN, VERSION).await.unwrap();
        assert!(h.deps.resent.lock().unwrap().is_empty(), "{kind:?}");
        assert!(released_ok(&h), "{kind:?}");
    }
}

#[tokio::test]
async fn load_none_is_noop_and_busy_mapped() {
    let deps = Arc::new(SDeps { w: None, now: AtomicI64::new(fetch_time()), v2: false, load_err: None, resent: Mutex::new(vec![]), dlq: Arc::new(Dlq::default()) });
    let base = new_timer_task_executor_base(shard(), Arc::new(NoCleanup), Logger::new(), Arc::new(Metrics));
    let exec = TimerStandbyTaskExecutor { base, cluster_name: STANDBY_CLUSTER.into(), dependencies: deps };
    exec.execute_workflow_timeout_task(&WorkflowTimeoutTask { workflow_identifier: wi(GLOBAL_DOMAIN), task_data: td(VERSION, NOW) }).await.unwrap();

    let busy = harness_with(false, Some(TaskError::ContextDeadlineExceeded));
    let r = run_kind(&busy, Kind::Workflow, GLOBAL_DOMAIN, VERSION).await;
    assert_eq!(r, Err(TaskError::WorkflowBusy));
    let other = harness_with(false, Some(TaskError::Other("boom".into())));
    let r = run_kind(&other, Kind::Workflow, GLOBAL_DOMAIN, VERSION).await;
    assert_eq!(r, Err(TaskError::Other("boom".into())));
}

// ---------------------------------------------------------------------------
// user timer
// ---------------------------------------------------------------------------
#[tokio::test]
async fn user_timer_success_when_not_yet_expired() {
    let h = harness();
    h.w.set(|s| {
        s.user_timers = vec![seq(5, TimerType::StartToClose, NOW + 10 * SEC)];
        s.user_infos.insert(5, UserTimerInfo { timer_id: "t".into(), started_id: 5 });
    });
    set_now(&h, fetch_time());
    run_kind(&h, Kind::User, GLOBAL_DOMAIN, VERSION).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn user_timer_success_when_no_timers() {
    let h = harness();
    set_now(&h, fetch_time());
    run_kind(&h, Kind::User, GLOBAL_DOMAIN, VERSION).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn user_timer_multiple_only_first_matters() {
    // first timer still in the future => nothing to resend even though later ones are expired
    let h = harness();
    h.w.set(|s| {
        s.user_timers = vec![seq(5, TimerType::StartToClose, NOW + 10 * SEC), seq(6, TimerType::StartToClose, NOW - SEC)];
        s.user_infos.insert(5, UserTimerInfo { timer_id: "t1".into(), started_id: 5 });
        s.user_infos.insert(6, UserTimerInfo { timer_id: "t2".into(), started_id: 6 });
    });
    set_now(&h, fetch_time());
    run_kind(&h, Kind::User, GLOBAL_DOMAIN, VERSION).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn user_timer_missing_info_is_error() {
    let h = harness();
    h.w.set(|s| s.user_timers = vec![seq(5, TimerType::StartToClose, NOW - SEC)]);
    let r = run_kind(&h, Kind::User, GLOBAL_DOMAIN, VERSION).await;
    assert!(matches!(r, Err(TaskError::Other(_))), "{r:?}");
    let rel = h.w.released();
    assert_eq!(rel.len(), 1);
    assert!(rel[0].is_some());
}

// ---------------------------------------------------------------------------
// activity timeout
// ---------------------------------------------------------------------------
async fn run_activity(h: &H, timeout_type: i32, vis: i64) -> Result<()> {
    h.exec
        .execute_activity_timeout_task(&ActivityTimeoutTask { workflow_identifier: wi(GLOBAL_DOMAIN), task_data: td(VERSION, vis), event_id: 5, attempt: 0, timeout_type })
        .await
}

#[tokio::test]
async fn activity_timer_success_creates_next_timer_and_updates_passive() {
    let h = harness();
    h.w.set(|s| {
        s.activity_timers = vec![seq(5, TimerType::StartToClose, NOW + 10 * SEC)];
        s.activity_infos.insert(5, ainfo(5, 0));
        s.next_timer_modified = true;
    });
    run_activity(&h, 2, NOW).await.unwrap();
    assert_eq!(
        h.w.calls(),
        vec![
            "create_next_activity_timer".to_string(),
            format!("update_current_version:{VERSION}:true"),
            format!("update_as_passive:{}", NOW + 5 * SEC),
        ]
    );
}

#[tokio::test]
async fn activity_timer_success_without_changes_does_not_update() {
    let h = harness();
    h.w.set(|s| {
        s.activity_timers = vec![seq(5, TimerType::StartToClose, NOW + 10 * SEC)];
        s.activity_infos.insert(5, ainfo(5, 0));
    });
    run_activity(&h, 2, NOW).await.unwrap();
    assert_eq!(h.w.calls(), vec!["create_next_activity_timer".to_string()]);
}

#[tokio::test]
async fn activity_heartbeat_timeout_clears_status_and_updates() {
    let h = harness();
    h.w.set(|s| {
        s.activity_timers = vec![seq(5, TimerType::Heartbeat, NOW + 10 * SEC)];
        s.activity_infos.insert(5, ainfo(5, 8));
    });
    run_activity(&h, 3, NOW).await.unwrap();
    let calls = h.w.calls();
    assert_eq!(calls[0], "update_activity:5:0");
    assert!(calls.contains(&format!("update_current_version:{VERSION}:true")));
    assert!(calls.last().unwrap().starts_with("update_as_passive:"));
}

#[tokio::test]
async fn activity_heartbeat_noop_when_heartbeat_visibility_in_future() {
    let h = harness();
    h.w.set(|s| {
        s.activity_timers = vec![seq(5, TimerType::Heartbeat, NOW + 10 * SEC)];
        let mut info = ainfo(5, 8);
        info.last_heartbeat_timeout_visibility_in_seconds = NOW / SEC + 100;
        s.activity_infos.insert(5, info);
    });
    run_activity(&h, 3, NOW).await.unwrap();
    assert_eq!(h.w.calls(), vec!["create_next_activity_timer".to_string()]);
}

#[tokio::test]
async fn activity_timer_missing_info_is_error() {
    let h = harness();
    h.w.set(|s| s.activity_timers = vec![seq(5, TimerType::StartToClose, NOW - SEC)]);
    let r = run_activity(&h, 2, NOW).await;
    assert!(matches!(r, Err(TaskError::Other(_))), "{r:?}");
}

// ---------------------------------------------------------------------------
// decision timeout
// ---------------------------------------------------------------------------
async fn run_decision(h: &H, domain: &str, timeout_type: i32) -> Result<()> {
    h.exec
        .execute_decision_timeout_task(&DecisionTimeoutTask { workflow_identifier: wi(domain), task_data: td(VERSION, NOW), event_id: 5, schedule_attempt: 0, timeout_type })
        .await
}

#[tokio::test]
async fn decision_schedule_to_start_timeout_is_noop() {
    // timeout_type 1 (schedule to start) is ignored on standby
    let h = harness();
    make_pending(&h, Kind::Decision);
    set_now(&h, fetch_time());
    run_decision(&h, GLOBAL_DOMAIN, 1).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
    assert!(h.w.released().is_empty());
}

#[tokio::test]
async fn decision_success_when_decision_missing() {
    let h = harness();
    set_now(&h, fetch_time());
    run_decision(&h, GLOBAL_DOMAIN, 2).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn decision_version_mismatch_global_domain_is_noop() {
    let h = harness();
    h.w.set(|s| s.decision = Some(TimerDecisionInfo { schedule_id: 5, started_id: EMPTY_EVENT_ID, attempt: 0, version: VERSION + 1 }));
    set_now(&h, fetch_time());
    run_decision(&h, GLOBAL_DOMAIN, 2).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn decision_version_mismatch_local_domain_still_resends() {
    let h = harness();
    h.w.set(|s| s.decision = Some(TimerDecisionInfo { schedule_id: 5, started_id: EMPTY_EVENT_ID, attempt: 0, version: VERSION + 1 }));
    set_now(&h, fetch_time());
    let r = run_decision(&h, LOCAL_DOMAIN, 2).await;
    assert!(is_redispatch(&r), "{r:?}");
    assert_eq!(h.deps.resent.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// workflow backoff / workflow timeout
// ---------------------------------------------------------------------------
#[tokio::test]
async fn backoff_success_when_decision_already_processed() {
    let h = harness();
    h.w.set(|s| s.has_processed = true);
    set_now(&h, fetch_time());
    run_kind(&h, Kind::Backoff, GLOBAL_DOMAIN, VERSION).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workflow_timeout_version_mismatch_global_domain_is_noop() {
    let h = harness();
    h.w.set(|s| s.start_version = VERSION + 1);
    set_now(&h, fetch_time());
    run_kind(&h, Kind::Workflow, GLOBAL_DOMAIN, VERSION).await.unwrap();
    assert!(h.deps.resent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workflow_timeout_version_mismatch_local_domain_still_resends() {
    let h = harness();
    h.w.set(|s| s.start_version = VERSION + 1);
    set_now(&h, fetch_time());
    let r = run_kind(&h, Kind::Workflow, LOCAL_DOMAIN, VERSION).await;
    assert!(is_redispatch(&r), "{r:?}");
}

#[tokio::test]
async fn standby_cluster_time_adds_standby_delay() {
    let h = harness();
    assert_eq!(h.exec.get_standby_cluster_time(), Time(NOW + 5 * SEC));
}

// Go TestProcessRetryTimeout: ActivityRetryTimerTask on the standby executor is dropped (nil error,
// mutable state never loaded).
#[derive(Debug)]
struct WrappedTask(Arc<dyn PersistenceTask>);
impl PersistenceTask for WrappedTask {
    fn as_any(&self) -> &dyn std::any::Any {
        self.0.as_any()
    }
    fn get_task_category(&self) -> HistoryTaskCategory {
        self.0.get_task_category()
    }
    fn get_task_type(&self) -> i32 {
        self.0.get_task_type()
    }
    fn get_domain_id(&self) -> &str {
        self.0.get_domain_id()
    }
    fn get_workflow_id(&self) -> &str {
        self.0.get_workflow_id()
    }
    fn get_run_id(&self) -> &str {
        self.0.get_run_id()
    }
    fn get_version(&self) -> i64 {
        self.0.get_version()
    }
    fn get_task_id(&self) -> i64 {
        self.0.get_task_id()
    }
    fn get_visibility_timestamp(&self) -> Time {
        self.0.get_visibility_timestamp()
    }
    fn clone_arc(&self) -> Arc<dyn PersistenceTask> {
        self.0.clone()
    }
}
#[async_trait]
impl super::interface::Task for WrappedTask {
    async fn execute(&self) -> Result<()> {
        unimplemented!()
    }
    fn handle_err(&self, _: Option<TaskError>) -> Option<TaskError> {
        unimplemented!()
    }
    fn retry_err(&self, _: &TaskError) -> bool {
        unimplemented!()
    }
    fn ack(&self) {}
    async fn nack(&self, _: Option<TaskError>) {}
    fn cancel(&self) {}
    fn state(&self) -> super::interface::TaskState {
        unimplemented!()
    }
    fn priority(&self) -> i32 {
        0
    }
    fn set_priority(&self, _: i32) {}
    fn get_queue_type(&self) -> super::interface::QueueType {
        super::interface::QueueType::StandbyTimer
    }
    fn get_shard(&self) -> Arc<dyn ShardContext> {
        unimplemented!()
    }
    fn get_attempt(&self) -> i32 {
        0
    }
    fn get_info(&self) -> Arc<dyn PersistenceTask> {
        self.0.clone()
    }
    fn set_initial_submit_time(&self, _: Time) {}
}

#[tokio::test]
async fn execute_activity_retry_timer_task_is_dropped_on_standby() {
    use super::interface::Executor;
    let h = harness();
    let inner: Arc<dyn PersistenceTask> =
        Arc::new(ActivityRetryTimerTask { workflow_identifier: wi(GLOBAL_DOMAIN), task_data: td(VERSION, NOW), event_id: 5, attempt: 1 });
    let task: Arc<dyn super::interface::Task> = Arc::new(WrappedTask(inner));
    let (resp, result) = h.exec.execute(task).await;
    assert!(result.is_ok());
    assert!(!resp.is_active_task);
    assert!(h.w.calls().is_empty());
}
