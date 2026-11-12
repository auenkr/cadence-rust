//! Ports of go/service/history/task/timer_active_task_executor_test.go against a fake
//! `TimerWorkflow` (the Rust executor is written against a trait, not concrete mutable state).
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use super::deps::*;
use super::timer_active_task_executor::*;
use super::timer_task_executor_base::*;
use super::transfer_task_executor_base::AddMatchingTaskRequest;

const NOW: i64 = 1_000_000_000_000;

struct FakeTime(AtomicI64);
impl TimeSource for FakeTime {
    fn now(&self) -> Time {
        Time(self.0.load(Ordering::SeqCst))
    }
}

struct Domains;
impl DomainCache for Domains {
    fn get_domain_by_id(&self, id: &str) -> Result<Arc<DomainCacheEntry>> {
        Ok(Arc::new(DomainCacheEntry {
            info: DomainInfo { id: id.into(), name: "domain".into() },
            is_global_domain: id == "global-domain",
            failover_version: 5,
            ..Default::default()
        }))
    }
    fn get_domain(&self, name: &str) -> Result<Arc<DomainCacheEntry>> {
        Ok(Arc::new(DomainCacheEntry {
            info: DomainInfo { id: format!("id-of-{name}"), name: name.into() },
            ..Default::default()
        }))
    }
    fn get_domain_name(&self, _id: &str) -> Result<String> {
        Ok("domain".into())
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
        unimplemented!()
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

#[derive(Default)]
struct S {
    running: bool,
    user_timers: Vec<TimerSequenceId>,
    activity_timers: Vec<TimerSequenceId>,
    user_infos: HashMap<i64, UserTimerInfo>,
    activity_infos: HashMap<i64, TimerActivityInfo>,
    decision: Option<TimerDecisionInfo>,
    resurrected_timers: HashSet<String>,
    resurrected_acts: HashSet<i64>,
    retry_result: bool,
    sticky: bool,
    decision_attempt: i64,
    has_processed: bool,
    decision_schedule_id: i64,
    retry_backoff: Option<Duration>,
    cron_backoff: Option<Duration>,
    cancel_requested: bool,
    next_event_id: i64,
    released: usize,
    calls: Vec<String>,
    updated_activity: Option<TimerActivityInfo>,
    scheduled_domain: String,
}

struct W(Mutex<S>);
impl W {
    fn new() -> Arc<W> {
        Arc::new(W(Mutex::new(S {
            running: true,
            next_event_id: 10,
            decision_schedule_id: EMPTY_EVENT_ID,
            ..Default::default()
        })))
    }
    fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().calls.clone()
    }
    fn log(&self, c: impl Into<String>) {
        self.0.lock().unwrap().calls.push(c.into());
    }
    fn set(&self, f: impl FnOnce(&mut S)) {
        f(&mut self.0.lock().unwrap());
    }
}

#[async_trait]
impl TimerWorkflow for W {
    fn release(&self, _: Option<TaskError>) {
        self.0.lock().unwrap().released += 1;
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
        Ok(self.0.lock().unwrap().resurrected_timers.clone())
    }
    async fn resurrected_activities(&self) -> Result<HashSet<i64>> {
        Ok(self.0.lock().unwrap().resurrected_acts.clone())
    }
    fn delete_user_timer(&self, id: &str) -> Result<()> {
        self.log(format!("delete_user_timer:{id}"));
        Ok(())
    }
    fn delete_activity(&self, id: i64) -> Result<()> {
        self.log(format!("delete_activity:{id}"));
        Ok(())
    }
    fn add_timer_fired_event(&self, id: &str) -> Result<()> {
        self.log(format!("timer_fired:{id}"));
        Ok(())
    }
    fn update_activity(&self, info: TimerActivityInfo) -> Result<()> {
        self.log("update_activity");
        self.0.lock().unwrap().updated_activity = Some(info);
        Ok(())
    }
    fn retry_activity(&self, _: &TimerActivityInfo, _: TimerType) -> Result<bool> {
        self.log("retry_activity");
        Ok(self.0.lock().unwrap().retry_result)
    }
    fn add_activity_task_timed_out_event(&self, info: &TimerActivityInfo, t: TimerType) -> Result<()> {
        self.log(format!("activity_timed_out:{}:{:?}", info.schedule_id, t));
        Ok(())
    }
    fn sticky(&self) -> bool {
        self.0.lock().unwrap().sticky
    }
    fn add_decision_task_timed_out_event(&self, _: &TimerDecisionInfo) -> Result<()> {
        self.log("decision_timed_out");
        Ok(())
    }
    fn add_decision_task_schedule_to_start_timeout_event(&self, id: i64) -> Result<()> {
        self.log(format!("decision_sts_timeout:{id}"));
        Ok(())
    }
    fn decision_attempt(&self) -> i64 {
        self.0.lock().unwrap().decision_attempt
    }
    fn terminate_decision_attempts_exceeded(&self, _: &str) -> Result<()> {
        self.log("terminate_attempts_exceeded");
        Ok(())
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.0.lock().unwrap().has_processed
    }
    fn decision_schedule_id(&self) -> i64 {
        self.0.lock().unwrap().decision_schedule_id
    }
    fn add_upsert_workflow_search_attributes_task(&self) -> Result<()> {
        self.log("upsert_search_attrs_task");
        Ok(())
    }
    fn schedule_decision(&self) -> Result<()> {
        self.log("schedule_decision");
        Ok(())
    }
    async fn update_workflow_execution_as_active(&self, _: Time) -> Result<()> {
        self.log("update_as_active");
        Ok(())
    }
    fn start_version(&self) -> Result<i64> {
        Ok(1)
    }
    fn next_event_id(&self) -> i64 {
        self.0.lock().unwrap().next_event_id
    }
    fn retry_backoff_duration(&self, _: &str) -> Option<Duration> {
        self.0.lock().unwrap().retry_backoff
    }
    async fn cron_backoff_duration(&self) -> Result<Option<Duration>> {
        Ok(self.0.lock().unwrap().cron_backoff)
    }
    fn cancel_requested(&self) -> bool {
        self.0.lock().unwrap().cancel_requested
    }
    fn timeout_workflow(&self, first: i64) -> Result<()> {
        self.log(format!("timeout_workflow:{first}"));
        Ok(())
    }
    async fn retry_workflow(&self, first: i64, backoff: Duration, cron: bool, reason: &str) -> Result<()> {
        self.log(format!("retry_workflow:{first}:{}:{cron}:{reason}", backoff.as_secs()));
        Ok(())
    }
    async fn update_workflow_execution_with_new_as_active(&self, _: Time) -> Result<()> {
        self.log("update_with_new_as_active");
        Ok(())
    }
    async fn activity_scheduled_domain(&self, _: i64) -> Result<String> {
        Ok(self.0.lock().unwrap().scheduled_domain.clone())
    }
    fn partition_config(&self) -> BytesMap {
        BytesMap::new()
    }
}

struct Deps {
    w: Option<Arc<W>>,
    enforce: bool,
    max_attempts: i32,
    resurrection_delay: Duration,
    advanced_visibility: bool,
    pushed: Mutex<Vec<AddMatchingTaskRequest>>,
    load_err: Option<TaskError>,
}
#[async_trait]
impl TimerActiveDependencies for Deps {
    async fn load_workflow(&self, _: &dyn PersistenceTask, _: i64) -> Result<Option<Arc<dyn TimerWorkflow>>> {
        if let Some(e) = &self.load_err {
            return Err(e.clone());
        }
        Ok(self.w.clone().map(|w| w as Arc<dyn TimerWorkflow>))
    }
    fn resurrection_check_min_delay(&self, _: &str) -> Duration {
        self.resurrection_delay
    }
    fn activity_max_schedule_to_start_timeout_for_retry(&self, _: &str) -> Duration {
        Duration::from_secs(1800)
    }
    fn decision_retry_max_attempts(&self, _: &str) -> i32 {
        self.max_attempts
    }
    fn enforce_decision_task_attempts(&self, _: &str) -> bool {
        self.enforce
    }
    fn advanced_visibility_writing_enabled(&self) -> bool {
        self.advanced_visibility
    }
    fn delete_history_event_context_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
    async fn add_activity_task(&self, r: AddMatchingTaskRequest) -> Result<()> {
        self.pushed.lock().unwrap().push(r);
        Ok(())
    }
}

fn deps(w: &Arc<W>) -> Deps {
    Deps {
        w: Some(w.clone()),
        enforce: false,
        max_attempts: 0,
        resurrection_delay: Duration::from_secs(3600),
        advanced_visibility: false,
        pushed: Mutex::new(vec![]),
        load_err: None,
    }
}

fn executor(d: Arc<Deps>) -> TimerActiveTaskExecutor {
    let shard: Arc<dyn ShardContext> = Arc::new(Shard { time: Arc::new(FakeTime(AtomicI64::new(NOW))) });
    let base = new_timer_task_executor_base(shard, Arc::new(NoCleanup), Logger::new(), Arc::new(Metrics));
    TimerActiveTaskExecutor { base, dependencies: d }
}

fn wf() -> WorkflowIdentifier {
    WorkflowIdentifier { domain_id: "d".into(), workflow_id: "wid".into(), run_id: "rid".into() }
}
fn td(vis: i64) -> TaskData {
    TaskData { version: 1, task_id: 100, visibility_timestamp: Time(vis) }
}
fn secs(n: i64) -> i64 {
    n * 1_000_000_000
}
fn seq(event_id: i64, t: TimerType, expiry: i64, attempt: i64) -> TimerSequenceId {
    TimerSequenceId { event_id, attempt, timer_type: t, expiry: Time(expiry) }
}
fn ainfo(id: i64) -> TimerActivityInfo {
    TimerActivityInfo {
        schedule_id: id,
        started_id: EMPTY_EVENT_ID,
        attempt: 0,
        version: 1,
        domain_id: String::new(),
        task_list: "tl".into(),
        schedule_to_start_timeout: 2,
        last_heartbeat_timeout_visibility_in_seconds: 0,
        timer_task_status: 0,
        details: vec![],
    }
}
fn dinfo(attempt: i64, started: i64) -> TimerDecisionInfo {
    TimerDecisionInfo { schedule_id: 5, started_id: started, attempt, version: 1 }
}

// ---- user timer ----

#[tokio::test]
async fn user_timer_fire() {
    let w = W::new();
    w.set(|s| {
        s.user_timers = vec![seq(5, TimerType::StartToClose, NOW - secs(1), 0)];
        s.user_infos.insert(5, UserTimerInfo { timer_id: "timer".into(), started_id: 5 });
    });
    let e = executor(Arc::new(deps(&w)));
    let t = UserTimerTask { workflow_identifier: wf(), task_data: td(NOW), event_id: 5 };
    e.execute_user_timer_timeout_task(&t).await.unwrap();
    assert_eq!(w.calls(), vec!["timer_fired:timer", "schedule_decision", "update_as_active"]);
    assert_eq!(w.0.lock().unwrap().released, 1);
}

#[tokio::test]
async fn user_timer_noop_not_expired_or_not_running() {
    let w = W::new();
    w.set(|s| {
        s.user_timers = vec![seq(5, TimerType::StartToClose, NOW + secs(5), 0)];
        s.user_infos.insert(5, UserTimerInfo { timer_id: "timer".into(), started_id: 5 });
    });
    let e = executor(Arc::new(deps(&w)));
    let t = UserTimerTask { workflow_identifier: wf(), task_data: td(NOW), event_id: 5 };
    e.execute_user_timer_timeout_task(&t).await.unwrap();
    assert!(w.calls().is_empty());
    w.set(|s| {
        s.running = false;
        s.user_timers = vec![seq(5, TimerType::StartToClose, NOW - secs(1), 0)];
    });
    e.execute_user_timer_timeout_task(&t).await.unwrap();
    assert!(w.calls().is_empty());
}

#[tokio::test]
async fn user_timer_resurrected() {
    let w = W::new();
    w.set(|s| {
        s.user_timers = vec![
            seq(5, TimerType::StartToClose, NOW - secs(10), 0),
            seq(6, TimerType::StartToClose, NOW, 0),
        ];
        s.user_infos.insert(5, UserTimerInfo { timer_id: "timer1".into(), started_id: 5 });
        s.user_infos.insert(6, UserTimerInfo { timer_id: "timer2".into(), started_id: 6 });
        s.resurrected_timers.insert("timer1".into());
    });
    let mut d = deps(&w);
    d.resurrection_delay = Duration::from_secs(5);
    let e = executor(Arc::new(d));
    let t = UserTimerTask { workflow_identifier: wf(), task_data: td(NOW), event_id: 6 };
    e.execute_user_timer_timeout_task(&t).await.unwrap();
    assert_eq!(
        w.calls(),
        vec!["delete_user_timer:timer1", "timer_fired:timer2", "schedule_decision", "update_as_active"]
    );
}

// ---- activity timeout ----

fn act_task(timeout_type: i32, vis: i64) -> ActivityTimeoutTask {
    ActivityTimeoutTask { workflow_identifier: wf(), task_data: td(vis), event_id: 5, attempt: 0, timeout_type }
}

#[tokio::test]
async fn activity_timeout_no_retry_fire() {
    let w = W::new();
    w.set(|s| {
        s.activity_timers = vec![seq(5, TimerType::StartToClose, NOW - secs(1), 0)];
        s.activity_infos.insert(5, ainfo(5));
    });
    let e = executor(Arc::new(deps(&w)));
    e.execute_activity_timeout_task(&act_task(0, NOW)).await.unwrap();
    assert_eq!(
        w.calls(),
        vec!["retry_activity", "activity_timed_out:5:StartToClose", "schedule_decision", "update_as_active"]
    );
}

#[tokio::test]
async fn activity_timeout_noop_when_activity_gone() {
    let w = W::new();
    w.set(|s| s.activity_timers = vec![seq(5, TimerType::StartToClose, NOW - secs(1), 0)]);
    let e = executor(Arc::new(deps(&w)));
    e.execute_activity_timeout_task(&act_task(0, NOW)).await.unwrap();
    assert!(w.calls().is_empty());
}

#[tokio::test]
async fn activity_timeout_retry_updates_without_decision() {
    for tp in [TimerType::StartToClose, TimerType::ScheduleToStart] {
        let w = W::new();
        w.set(|s| {
            s.activity_timers = vec![seq(5, tp, NOW - secs(1), 0)];
            s.activity_infos.insert(5, ainfo(5));
            s.retry_result = true;
        });
        let e = executor(Arc::new(deps(&w)));
        e.execute_activity_timeout_task(&act_task(0, NOW)).await.unwrap();
        assert_eq!(w.calls(), vec!["retry_activity", "update_as_active"]);
    }
}

#[tokio::test]
async fn activity_timeout_skips_stale_attempt() {
    let w = W::new();
    w.set(|s| {
        s.activity_timers = vec![seq(5, TimerType::StartToClose, NOW - secs(1), 0)];
        let mut i = ainfo(5);
        i.attempt = 2;
        s.activity_infos.insert(5, i);
    });
    let e = executor(Arc::new(deps(&w)));
    e.execute_activity_timeout_task(&act_task(0, NOW)).await.unwrap();
    assert!(w.calls().is_empty());
}

#[tokio::test]
async fn activity_timeout_heartbeat_noop_and_cleared() {
    let w = W::new();
    w.set(|s| {
        let mut i = ainfo(5);
        i.last_heartbeat_timeout_visibility_in_seconds = NOW / 1_000_000_000 + 10;
        s.activity_infos.insert(5, i);
    });
    let e = executor(Arc::new(deps(&w)));
    e.execute_activity_timeout_task(&act_task(3, NOW)).await.unwrap();
    assert!(w.calls().is_empty());
    w.set(|s| {
        let i = s.activity_infos.get_mut(&5).unwrap();
        i.last_heartbeat_timeout_visibility_in_seconds = NOW / 1_000_000_000 - 1;
        i.timer_task_status = 8 | 1;
    });
    e.execute_activity_timeout_task(&act_task(3, NOW)).await.unwrap();
    assert_eq!(w.calls(), vec!["update_activity", "update_as_active"]);
    assert_eq!(w.0.lock().unwrap().updated_activity.as_ref().unwrap().timer_task_status, 1);
}

#[tokio::test]
async fn activity_timeout_resurrected() {
    let w = W::new();
    w.set(|s| {
        s.activity_timers = vec![
            seq(5, TimerType::StartToClose, NOW - secs(10), 0),
            seq(6, TimerType::StartToClose, NOW, 0),
        ];
        s.activity_infos.insert(5, ainfo(5));
        s.activity_infos.insert(6, ainfo(6));
        s.resurrected_acts.insert(5);
    });
    let mut d = deps(&w);
    d.resurrection_delay = Duration::from_secs(5);
    let e = executor(Arc::new(d));
    e.execute_activity_timeout_task(&act_task(0, NOW)).await.unwrap();
    assert_eq!(
        w.calls(),
        vec![
            "delete_activity:5",
            "retry_activity",
            "activity_timed_out:6:StartToClose",
            "schedule_decision",
            "update_as_active"
        ]
    );
}

// ---- decision timeout ----

fn dec_task(timeout_type: i32, attempt: i64) -> DecisionTimeoutTask {
    DecisionTimeoutTask { workflow_identifier: wf(), task_data: td(NOW), event_id: 5, schedule_attempt: attempt, timeout_type }
}

#[tokio::test]
async fn decision_schedule_to_start_normal_and_sticky() {
    for sticky in [false, true] {
        let w = W::new();
        w.set(|s| {
            s.sticky = sticky;
            s.decision = Some(dinfo(1, EMPTY_EVENT_ID));
        });
        let e = executor(Arc::new(deps(&w)));
        e.execute_decision_timeout_task(&dec_task(1, 1)).await.unwrap();
        assert_eq!(w.calls(), vec!["decision_sts_timeout:5", "schedule_decision", "update_as_active"]);
    }
}

#[tokio::test]
async fn decision_schedule_to_start_noop_when_started() {
    let w = W::new();
    w.set(|s| s.decision = Some(dinfo(1, 7)));
    let e = executor(Arc::new(deps(&w)));
    e.execute_decision_timeout_task(&dec_task(1, 1)).await.unwrap();
    assert!(w.calls().is_empty());
}

#[tokio::test]
async fn decision_start_to_close_fire() {
    let w = W::new();
    w.set(|s| s.decision = Some(dinfo(0, 7)));
    let e = executor(Arc::new(deps(&w)));
    e.execute_decision_timeout_task(&dec_task(0, 0)).await.unwrap();
    assert_eq!(w.calls(), vec!["decision_timed_out", "schedule_decision", "update_as_active"]);
}

#[tokio::test]
async fn decision_start_to_close_attempts() {
    // exceeds max attempts with enforcement -> terminate, no new decision
    let w = W::new();
    w.set(|s| {
        s.decision = Some(dinfo(2, 7));
        s.decision_attempt = 6;
    });
    let mut d = deps(&w);
    d.enforce = true;
    d.max_attempts = 5;
    executor(Arc::new(d)).execute_decision_timeout_task(&dec_task(0, 2)).await.unwrap();
    assert_eq!(w.calls(), vec!["decision_timed_out", "terminate_attempts_exceeded", "update_as_active"]);
    // below max attempts
    let w = W::new();
    w.set(|s| {
        s.decision = Some(dinfo(2, 7));
        s.decision_attempt = 3;
    });
    let mut d = deps(&w);
    d.enforce = true;
    d.max_attempts = 5;
    executor(Arc::new(d)).execute_decision_timeout_task(&dec_task(0, 2)).await.unwrap();
    assert_eq!(w.calls(), vec!["decision_timed_out", "schedule_decision", "update_as_active"]);
    // not enforced
    let w = W::new();
    w.set(|s| {
        s.decision = Some(dinfo(2, 7));
        s.decision_attempt = 60;
    });
    let mut d = deps(&w);
    d.enforce = false;
    d.max_attempts = 5;
    executor(Arc::new(d)).execute_decision_timeout_task(&dec_task(0, 2)).await.unwrap();
    assert_eq!(w.calls(), vec!["decision_timed_out", "schedule_decision", "update_as_active"]);
}

#[tokio::test]
async fn decision_start_to_close_noop() {
    // attempt mismatch
    let w = W::new();
    w.set(|s| s.decision = Some(dinfo(1, 7)));
    let e = executor(Arc::new(deps(&w)));
    e.execute_decision_timeout_task(&dec_task(0, 2)).await.unwrap();
    assert!(w.calls().is_empty());
    // decision gone
    w.set(|s| s.decision = None);
    e.execute_decision_timeout_task(&dec_task(0, 1)).await.unwrap();
    assert!(w.calls().is_empty());
}

// ---- workflow backoff ----

fn backoff(tp: i32) -> WorkflowBackoffTimerTask {
    WorkflowBackoffTimerTask { workflow_identifier: wf(), task_data: td(NOW), event_id: 0, timeout_type: tp }
}

#[tokio::test]
async fn workflow_backoff_fire_and_noop() {
    let w = W::new();
    let e = executor(Arc::new(deps(&w)));
    e.execute_workflow_backoff_timer_task(&backoff(0)).await.unwrap();
    assert_eq!(w.calls(), vec!["schedule_decision", "update_as_active"]);
    let w = W::new();
    w.set(|s| s.has_processed = true);
    executor(Arc::new(deps(&w))).execute_workflow_backoff_timer_task(&backoff(0)).await.unwrap();
    assert!(w.calls().is_empty());
}

#[tokio::test]
async fn workflow_backoff_cron_upserts_search_attributes() {
    let w = W::new();
    let mut d = deps(&w);
    d.advanced_visibility = true;
    executor(Arc::new(d)).execute_workflow_backoff_timer_task(&backoff(1)).await.unwrap();
    assert_eq!(w.calls(), vec!["upsert_search_attrs_task", "schedule_decision", "update_as_active"]);
}

// ---- activity retry timer ----

fn retry_task(attempt: i64) -> ActivityRetryTimerTask {
    ActivityRetryTimerTask { workflow_identifier: wf(), task_data: td(NOW), event_id: 5, attempt }
}

#[tokio::test]
async fn activity_retry_timer_fire_pushes_to_matching() {
    let w = W::new();
    w.set(|s| {
        let mut i = ainfo(5);
        i.attempt = 1;
        s.activity_infos.insert(5, i);
    });
    let d = Arc::new(deps(&w));
    executor(d.clone()).execute_activity_retry_timer_task(&retry_task(1)).await.unwrap();
    let pushed = d.pushed.lock().unwrap();
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].domain_uuid, "d");
    assert_eq!(pushed[0].task_list.name, "tl");
    assert_eq!(pushed[0].schedule_id, 5);
    assert_eq!(pushed[0].schedule_to_start_timeout_seconds, 2);
}

#[tokio::test]
async fn activity_retry_timer_noop() {
    // stale attempt
    let w = W::new();
    w.set(|s| {
        let mut i = ainfo(5);
        i.attempt = 3;
        s.activity_infos.insert(5, i);
    });
    let d = Arc::new(deps(&w));
    executor(d.clone()).execute_activity_retry_timer_task(&retry_task(1)).await.unwrap();
    // already started
    w.set(|s| {
        let i = s.activity_infos.get_mut(&5).unwrap();
        i.attempt = 1;
        i.started_id = 8;
    });
    executor(d.clone()).execute_activity_retry_timer_task(&retry_task(1)).await.unwrap();
    // activity gone
    w.set(|s| s.activity_infos.clear());
    executor(d.clone()).execute_activity_retry_timer_task(&retry_task(1)).await.unwrap();
    assert!(d.pushed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn activity_retry_timer_cross_domain_target() {
    let w = W::new();
    w.set(|s| {
        s.activity_infos.insert(5, ainfo(5));
        s.scheduled_domain = "other".into();
    });
    let d = Arc::new(deps(&w));
    executor(d.clone()).execute_activity_retry_timer_task(&retry_task(0)).await.unwrap();
    let pushed = d.pushed.lock().unwrap();
    assert_eq!(pushed[0].domain_uuid, "id-of-other");
    assert_eq!(pushed[0].source_domain_uuid.as_deref(), Some("d"));
}

// ---- workflow timeout ----

fn wf_timeout() -> WorkflowTimeoutTask {
    WorkflowTimeoutTask { workflow_identifier: wf(), task_data: td(NOW) }
}

#[tokio::test]
async fn workflow_timeout_fire() {
    let w = W::new();
    executor(Arc::new(deps(&w))).execute_workflow_timeout_task(&wf_timeout()).await.unwrap();
    assert_eq!(w.calls(), vec!["timeout_workflow:10", "update_as_active"]);
}

#[tokio::test]
async fn workflow_timeout_cancel_requested_times_out_despite_backoff() {
    let w = W::new();
    w.set(|s| {
        s.retry_backoff = Some(Duration::from_secs(3));
        s.cancel_requested = true;
    });
    executor(Arc::new(deps(&w))).execute_workflow_timeout_task(&wf_timeout()).await.unwrap();
    assert_eq!(w.calls(), vec!["timeout_workflow:10", "update_as_active"]);
}

#[tokio::test]
async fn workflow_timeout_continue_as_new_retry_and_cron() {
    let w = W::new();
    w.set(|s| s.retry_backoff = Some(Duration::from_secs(3)));
    executor(Arc::new(deps(&w))).execute_workflow_timeout_task(&wf_timeout()).await.unwrap();
    assert_eq!(w.calls(), vec!["retry_workflow:10:3:false:START_TO_CLOSE", "update_with_new_as_active"]);
    let w = W::new();
    w.set(|s| s.cron_backoff = Some(Duration::from_secs(7)));
    executor(Arc::new(deps(&w))).execute_workflow_timeout_task(&wf_timeout()).await.unwrap();
    assert_eq!(w.calls(), vec!["retry_workflow:10:7:true:START_TO_CLOSE", "update_with_new_as_active"]);
}

#[tokio::test]
async fn load_none_is_noop_and_busy_mapped() {
    let w = W::new();
    let mut d = deps(&w);
    d.w = None;
    let e = executor(Arc::new(d));
    e.execute_workflow_timeout_task(&wf_timeout()).await.unwrap();
    assert!(w.calls().is_empty());

    // context deadline while loading is surfaced as WorkflowBusy
    let mut d = deps(&w);
    d.load_err = Some(TaskError::ContextDeadlineExceeded);
    let e = executor(Arc::new(d));
    assert_eq!(e.execute_workflow_timeout_task(&wf_timeout()).await, Err(TaskError::WorkflowBusy));
    // other load errors pass through unchanged
    let mut d = deps(&w);
    d.load_err = Some(TaskError::Other("boom".into()));
    let e = executor(Arc::new(d));
    assert_eq!(e.execute_workflow_timeout_task(&wf_timeout()).await, Err(TaskError::Other("boom".into())));
}

// ---- global domain version mismatch paths ----

fn gwf() -> WorkflowIdentifier {
    WorkflowIdentifier { domain_id: "global-domain".into(), workflow_id: "wid".into(), run_id: "rid".into() }
}
fn gtd(version: i64) -> TaskData {
    TaskData { version, task_id: 100, visibility_timestamp: Time(NOW) }
}

#[tokio::test]
async fn global_domain_decision_timeout_version_mismatch_is_noop() {
    let w = W::new();
    w.set(|s| s.decision = Some(dinfo(0, EMPTY_EVENT_ID)));
    let e = executor(Arc::new(deps(&w)));
    let t = DecisionTimeoutTask { workflow_identifier: gwf(), task_data: gtd(2), event_id: 5, schedule_attempt: 0, timeout_type: 0 };
    e.execute_decision_timeout_task(&t).await.unwrap();
    assert!(w.calls().is_empty());
}

#[tokio::test]
async fn global_domain_activity_retry_timer_version_mismatch_is_noop() {
    // Go: only the activity retry timer verifies the activity version (the timeout task does not).
    let w = W::new();
    w.set(|s| {
        s.activity_infos.insert(5, ainfo(5));
    });
    let d = Arc::new(deps(&w));
    let e = executor(d.clone());
    let t = ActivityRetryTimerTask { workflow_identifier: gwf(), task_data: gtd(2), event_id: 5, attempt: 0 };
    e.execute_activity_retry_timer_task(&t).await.unwrap();
    assert!(d.pushed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn global_domain_workflow_timeout_version_mismatch_is_noop() {
    // fake start_version() is 1
    let w = W::new();
    let e = executor(Arc::new(deps(&w)));
    let t = WorkflowTimeoutTask { workflow_identifier: gwf(), task_data: gtd(2) };
    e.execute_workflow_timeout_task(&t).await.unwrap();
    assert!(w.calls().is_empty());
}
