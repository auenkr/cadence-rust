//! Unit tests ported from Go `service/history/reset/resetter_test.go`.
use super::history::*;
use super::*;
use cadence_api::enums::EventType;
use cadence_api::service_types::{
    HistoryEvent, WorkflowExecutionContinuedAsNewEventAttributes, WorkflowExecutionSignaledEventAttributes,
};
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Default)]
struct MockState {
    children: usize,
    in_flight: Option<ResetDecisionInfo>,
    pending: Option<ResetDecisionInfo>,
    activities: Vec<PendingActivityInfo>,
    next_event_id: i64,
    log: Vec<String>,
    signals: Vec<(String, Vec<u8>, String, String)>,
    updated: Vec<PendingActivityInfo>,
    failed: Vec<(i64, i64, String, Vec<u8>, String)>,
    terminated: Vec<(i64, String, Vec<u8>, String)>,
}

impl ResetMutableState for MockState {
    fn pending_child_execution_count(&self) -> usize {
        self.children
    }
    fn in_flight_decision(&self) -> Option<ResetDecisionInfo> {
        self.in_flight
    }
    fn pending_decision(&self) -> Option<ResetDecisionInfo> {
        self.pending
    }
    fn add_decision_task_failed_event(
        &mut self,
        d: ResetDecisionInfo,
        base_run_id: &str,
        reset_run_id: &str,
        version: i64,
        reason: &str,
        request_id: &str,
    ) -> Result<(), ResetError> {
        self.log.push(format!(
            "failed:{}:{}:{base_run_id}:{reset_run_id}:{version}:{reason}:{request_id}",
            d.schedule_id, d.started_id
        ));
        Ok(())
    }
    fn add_decision_task_reset_timeout_event(
        &mut self,
        d: ResetDecisionInfo,
        base_run_id: &str,
        reset_run_id: &str,
        version: i64,
        reason: &str,
        request_id: &str,
    ) -> Result<(), ResetError> {
        self.log.push(format!(
            "timeout:{}:{base_run_id}:{reset_run_id}:{version}:{reason}:{request_id}",
            d.schedule_id
        ));
        Ok(())
    }
}

impl PendingActivityMutableState for MockState {
    fn pending_activity_infos(&self) -> Vec<PendingActivityInfo> {
        self.activities.clone()
    }
    fn update_pending_activity(&mut self, a: PendingActivityInfo) -> Result<(), ResetError> {
        self.updated.push(a);
        Ok(())
    }
    fn add_activity_task_failed_event(
        &mut self,
        schedule_id: i64,
        started_id: i64,
        reason: &str,
        details: &[u8],
        identity: &str,
    ) -> Result<(), ResetError> {
        self.failed.push((schedule_id, started_id, reason.into(), details.to_vec(), identity.into()));
        Ok(())
    }
}

impl SignalEventReapplier for MockState {
    fn add_workflow_execution_signaled(
        &mut self,
        name: &str,
        input: &[u8],
        identity: &str,
        request_id: &str,
    ) -> Result<(), ResetError> {
        self.signals.push((name.into(), input.to_vec(), identity.into(), request_id.into()));
        Ok(())
    }
}

impl ResetTermination for MockState {
    fn next_event_id(&self) -> i64 {
        self.next_event_id
    }
    fn terminate_workflow(&mut self, first: i64, reason: &str, details: &[u8], identity: &str) -> Result<(), ResetError> {
        self.terminated.push((first, reason.into(), details.to_vec(), identity.into()));
        Ok(())
    }
}

type ReadKey = (Vec<u8>, i64, i64, Vec<u8>);

/// Scripted history reader: `(branch, first, next, page_token) -> page`.
#[derive(Default)]
struct MockHistory {
    pages: HashMap<ReadKey, (Vec<Vec<HistoryEvent>>, Vec<u8>)>,
    run_info: HashMap<String, (i64, Vec<u8>)>,
    loaded: RefCell<Vec<String>>,
    forks: RefCell<Vec<(Vec<u8>, i64, String, String)>>,
    reads: RefCell<usize>,
}

impl ResetHistory for MockHistory {
    fn paginate_history(
        &self,
        first: i64,
        next: i64,
        branch: &[u8],
        _domain_id: &str,
        token: &[u8],
    ) -> Result<HistoryPage, ResetError> {
        *self.reads.borrow_mut() += 1;
        let (batches, next_token) = self
            .pages
            .get(&(branch.to_vec(), first, next, token.to_vec()))
            .cloned()
            .ok_or_else(|| ResetError("unexpected read".into()))?;
        Ok(HistoryPage { batches, next_page_token: next_token })
    }
    fn load_run_history_info(&self, _d: &str, _w: &str, run_id: &str) -> Result<(i64, Vec<u8>), ResetError> {
        self.loaded.borrow_mut().push(run_id.to_string());
        self.run_info.get(run_id).cloned().ok_or_else(|| ResetError("unexpected load".into()))
    }
    fn domain_name(&self, _d: &str) -> Result<String, ResetError> {
        Ok("test-domain".into())
    }
    fn fork_history_branch(&self, token: &[u8], node: i64, info: &str, name: &str) -> Result<Vec<u8>, ResetError> {
        self.forks.borrow_mut().push((token.to_vec(), node, info.into(), name.into()));
        Ok(b"some random reset branch token".to_vec())
    }
}

fn ev(id: i64, t: EventType) -> HistoryEvent {
    HistoryEvent { event_id: id, event_type: Some(t), ..Default::default() }
}

fn continued_as_new(id: i64, run: &str) -> HistoryEvent {
    HistoryEvent {
        workflow_execution_continued_as_new_event_attributes: Some(WorkflowExecutionContinuedAsNewEventAttributes {
            new_execution_run_id: run.into(),
            ..Default::default()
        }),
        ..ev(id, EventType::WorkflowExecutionContinuedAsNew)
    }
}

fn signal(id: i64, name: &str, input: &[u8], identity: &str) -> HistoryEvent {
    HistoryEvent {
        workflow_execution_signaled_event_attributes: Some(WorkflowExecutionSignaledEventAttributes {
            signal_name: name.into(),
            input: input.to_vec(),
            identity: identity.into(),
            ..Default::default()
        }),
        ..ev(id, EventType::WorkflowExecutionSignaled)
    }
}

fn key(branch: &[u8], first: i64, next: i64, token: &[u8]) -> ReadKey {
    (branch.to_vec(), first, next, token.to_vec())
}

fn base_events() -> Vec<HistoryEvent> {
    vec![
        ev(124, EventType::DecisionTaskScheduled),
        ev(125, EventType::DecisionTaskStarted),
        ev(126, EventType::DecisionTaskCompleted),
        continued_as_new(127, "new-run"),
    ]
}

fn run_events() -> Vec<HistoryEvent> {
    vec![
        ev(1, EventType::WorkflowExecutionStarted),
        ev(2, EventType::DecisionTaskScheduled),
        ev(3, EventType::DecisionTaskStarted),
        ev(4, EventType::DecisionTaskCompleted),
        ev(5, EventType::WorkflowExecutionFailed),
    ]
}

// ---- persistToDB ----

#[derive(Default)]
struct MockPersist {
    log: RefCell<Vec<String>>,
    batches: usize,
}
impl ResetPersistence for MockPersist {
    type Workflow = &'static str;
    type Snapshot = &'static str;
    type Events = &'static str;
    type PersistedHistory = usize;
    fn update_with_new_as_active(&self, c: &&'static str, r: &&'static str) -> Result<(), ResetError> {
        self.log.borrow_mut().push(format!("update_with_new_as_active:{c}:{r}"));
        Ok(())
    }
    fn current_run_and_last_write_version(&self, _c: &&'static str) -> Result<(String, i64), ResetError> {
        Ok(("current-run".into(), 1234))
    }
    fn close_transaction_as_active_snapshot(&self, _r: &&'static str) -> Result<(&'static str, Vec<&'static str>), ResetError> {
        self.log.borrow_mut().push("close_snapshot".into());
        Ok(("snapshot", vec!["events"; self.batches]))
    }
    fn persist_non_start_workflow_batch_events(&self, _r: &&'static str, e: &'static str) -> Result<usize, ResetError> {
        self.log.borrow_mut().push(format!("persist_events:{e}"));
        Ok(4321)
    }
    fn create_workflow_execution(&self, _r: &&'static str, s: &'static str, h: usize, prev: &str, v: i64) -> Result<(), ResetError> {
        self.log.borrow_mut().push(format!("create:{s}:{h}:{prev}:{v}"));
        Ok(())
    }
}

/// Go `TestPersistToDB_CurrentTerminated`.
#[test]
fn persist_to_db_current_terminated() {
    let p = MockPersist { batches: 1, ..Default::default() };
    persist_to_db(&p, true, &"cur", &"reset").unwrap();
    assert_eq!(*p.log.borrow(), vec!["update_with_new_as_active:cur:reset".to_string()]);
}

/// Go `TestPersistToDB_CurrentNotTerminated`.
#[test]
fn persist_to_db_current_not_terminated() {
    let p = MockPersist { batches: 1, ..Default::default() };
    persist_to_db(&p, false, &"cur", &"reset").unwrap();
    assert_eq!(
        *p.log.borrow(),
        vec![
            "close_snapshot".to_string(),
            "persist_events:events".to_string(),
            "create:snapshot:4321:current-run:1234".to_string()
        ]
    );
}

/// Go `persistToDB`: more than one batch is an error.
#[test]
fn persist_to_db_requires_exactly_one_batch() {
    let p = MockPersist { batches: 2, ..Default::default() };
    assert!(persist_to_db(&p, false, &"cur", &"reset").is_err());
}

// ---- replayResetWorkflow ----

struct MockRebuilder {
    history: MockHistory,
    sizes: RefCell<Vec<i64>>,
    rebuilt_target_branch: RefCell<Vec<u8>>,
}
impl ResetHistory for MockRebuilder {
    fn paginate_history(&self, a: i64, b: i64, c: &[u8], d: &str, e: &[u8]) -> Result<HistoryPage, ResetError> {
        self.history.paginate_history(a, b, c, d, e)
    }
    fn load_run_history_info(&self, d: &str, w: &str, r: &str) -> Result<(i64, Vec<u8>), ResetError> {
        self.history.load_run_history_info(d, w, r)
    }
    fn domain_name(&self, d: &str) -> Result<String, ResetError> {
        self.history.domain_name(d)
    }
    fn fork_history_branch(&self, t: &[u8], n: i64, i: &str, d: &str) -> Result<Vec<u8>, ResetError> {
        self.history.fork_history_branch(t, n, i, d)
    }
}
impl ResetRebuilder for MockRebuilder {
    type Context = Option<i64>;
    type MutableState = &'static str;
    type Workflow = (Option<i64>, &'static str);
    fn new_context(&self, _d: &str, _w: &str, _r: &str) -> Self::Context {
        None
    }
    fn rebuild(
        &self,
        request: &RebuildRequest<'_>,
        branch: &mut dyn FnMut() -> Result<Vec<u8>, ResetError>,
    ) -> Result<(Self::MutableState, i64), ResetError> {
        assert_eq!(request.base_branch_token, b"some random base branch token");
        assert_eq!(request.last_event_id, 1233);
        assert_eq!(request.last_event_version, 12);
        assert_eq!(request.reset_request_id, "req");
        *self.rebuilt_target_branch.borrow_mut() = branch()?;
        Ok(("reset-ms", 4411))
    }
    fn set_history_size(&self, context: &mut Self::Context, size: i64) {
        self.sizes.borrow_mut().push(size);
        *context = Some(size);
    }
    fn new_workflow(&self, context: Self::Context, state: Self::MutableState) -> Self::Workflow {
        (context, state)
    }
}

/// Go `TestReplayResetWorkflow`.
#[test]
fn replay_reset_workflow_forks_and_sets_history_size() {
    let r = MockRebuilder {
        history: MockHistory::default(),
        sizes: RefCell::new(Vec::new()),
        rebuilt_target_branch: RefCell::new(Vec::new()),
    };
    let wf = replay_reset_workflow(
        &r,
        &RebuildRequest {
            domain_id: "d",
            workflow_id: "w",
            base_run_id: "base",
            base_branch_token: b"some random base branch token",
            last_event_id: 1233,
            last_event_version: 12,
            reset_run_id: "reset",
            reset_request_id: "req",
        },
    )
    .unwrap();
    assert_eq!(wf, (Some(4411), "reset-ms"));
    assert_eq!(*r.rebuilt_target_branch.borrow(), b"some random reset branch token");
    assert_eq!(
        *r.history.forks.borrow(),
        vec![(b"some random base branch token".to_vec(), 1234, "d:w:reset".to_string(), "test-domain".to_string())]
    );
}

/// Go `TestGenerateBranchToken`.
#[test]
fn generate_branch_token() {
    let h = MockHistory::default();
    let t = fork_and_generate_branch_token(&h, "d", "w", b"some random base branch token", 1234, "reset").unwrap();
    assert_eq!(t, b"some random reset branch token");
    assert_eq!(
        *h.forks.borrow(),
        vec![(b"some random base branch token".to_vec(), 1234, "d:w:reset".to_string(), "test-domain".to_string())]
    );
}

/// Go `TestFailInflightActivity`.
#[test]
fn fail_inflight_activity_cases() {
    let now = 1_000_000_000_i64;
    let a1 = PendingActivityInfo {
        schedule_id: 123,
        started_id: 124,
        scheduled_time_unix_nanos: now - 10,
        details: b"some random activity 1 details".to_vec(),
        started_identity: "some random activity 1 started identity".into(),
        ..Default::default()
    };
    let a2 = PendingActivityInfo {
        schedule_id: 456,
        started_id: EMPTY_EVENT_ID,
        scheduled_time_unix_nanos: now - 10,
        timer_task_status: 1,
        ..Default::default()
    };
    let mut s = MockState { activities: vec![a1.clone(), a2.clone()], ..Default::default() };
    fail_inflight_activity(&mut s, now, "some random termination reason").unwrap();
    assert_eq!(
        s.failed,
        vec![(123, 124, "some random termination reason".to_string(), a1.details.clone(), a1.started_identity.clone())]
    );
    assert_eq!(s.updated.len(), 1);
    assert_eq!(s.updated[0].schedule_id, 456);
    assert_eq!(s.updated[0].scheduled_time_unix_nanos, now);
    assert_eq!(s.updated[0].timer_task_status, 0);

    // transient activity is an error
    let mut s = MockState {
        activities: vec![PendingActivityInfo { started_id: TRANSIENT_EVENT_ID, ..Default::default() }],
        ..Default::default()
    };
    assert_eq!(fail_inflight_activity(&mut s, now, "r").unwrap_err().0, ERR_TRANSIENT_ACTIVITY);
}

/// Go `TestTerminateWorkflow` (the decision-fail / flush steps live in the adapter).
#[test]
fn terminate_workflow_uses_next_event_id_and_history_service_identity() {
    let mut s = MockState { next_event_id: 666, ..Default::default() };
    terminate_workflow(&mut s, "some random terminate reason").unwrap();
    assert_eq!(s.terminated, vec![(666, "some random terminate reason".into(), vec![], "history-service".into())]);
}

/// Go `TestReapplyContinueAsNewWorkflowEvents`: chain base -> new run (loaded) .
#[test]
fn reapply_continue_as_new_workflow_events() {
    let mut h = MockHistory::default();
    h.pages.insert(key(b"base", 124, 456, b""), (vec![base_events()], vec![]));
    h.pages.insert(key(b"new", 1, 6, b""), (vec![run_events()], vec![]));
    h.run_info.insert("new-run".into(), (6, b"new".to_vec()));
    let mut s = MockState::default();
    reapply_reset_and_continue_as_new_workflow_events(
        &h, &mut s, "d", "w", b"base", 124, 456, "current-run", 1, b"some random current branch token",
    )
    .unwrap();
    assert_eq!(*h.loaded.borrow(), vec!["new-run".to_string()]);
    assert_eq!(*h.reads.borrow(), 2);
}

/// Go `TestReapplyContinueAsNewWorkflowEvents_CurrentRunShortCircuit`: the current run
/// must not be loaded (caller holds its lock); supplied next event id / branch are used.
#[test]
fn reapply_continue_as_new_current_run_short_circuit() {
    let mut h = MockHistory::default();
    let mut base = base_events();
    base[3] = continued_as_new(127, "current-run");
    h.pages.insert(key(b"base", 124, 456, b""), (vec![base], vec![]));
    h.pages.insert(key(b"some random current branch token", 1, 6, b""), (vec![run_events()], vec![]));
    let mut s = MockState::default();
    reapply_reset_and_continue_as_new_workflow_events(
        &h, &mut s, "d", "w", b"base", 124, 456, "current-run", 6, b"some random current branch token",
    )
    .unwrap();
    assert!(h.loaded.borrow().is_empty(), "current run lock must not be re-acquired");
    assert_eq!(*h.reads.borrow(), 2);
}

/// Go `TestReapplyWorkflowEvents`: returns the continued-as-new run id.
#[test]
fn reapply_workflow_events_returns_next_run_id() {
    let mut h = MockHistory::default();
    let mut evs = run_events();
    evs[4] = continued_as_new(5, "new-run");
    h.pages.insert(key(b"branch", 1, 6, b""), (vec![evs], vec![]));
    let mut s = MockState::default();
    assert_eq!(reapply_workflow_events(&h, &mut s, "d", 1, 6, b"branch").unwrap(), "new-run");
    // first == next short-circuits without reading
    assert_eq!(reapply_workflow_events(&h, &mut s, "d", 6, 6, b"branch").unwrap(), "");
    assert_eq!(*h.reads.borrow(), 1);
}

/// Go `TestClosePendingDecisionTask`.
#[test]
fn close_pending_decision_task_cases() {
    let (base, new, version, reason, req) = ("base", "new", 10, "test", "req-id");
    let mut s = MockState {
        in_flight: Some(ResetDecisionInfo { schedule_id: 2, started_id: 3 }),
        ..Default::default()
    };
    close_pending_decision_task(&mut s, base, new, version, reason, req).unwrap();
    assert_eq!(s.log, vec!["failed:2:3:base:new:10:test:req-id".to_string()]);

    let mut s = MockState {
        pending: Some(ResetDecisionInfo { schedule_id: 2, started_id: 0 }),
        ..Default::default()
    };
    close_pending_decision_task(&mut s, base, new, version, reason, req).unwrap();
    assert_eq!(s.log, vec!["timeout:2:base:new:10:test:req-id".to_string()]);

    // pending children block the reset
    let mut s = MockState { children: 1, ..Default::default() };
    assert!(close_pending_decision_task(&mut s, base, new, version, reason, req).is_err());
}

/// Go `TestReapplyEvents`: only signal events are re-added, with empty request id.
#[test]
fn reapply_events_only_signals() {
    let events = vec![
        signal(101, "some random signal name", b"some random signal input", "some random signal identity"),
        ev(102, EventType::DecisionTaskScheduled),
        signal(103, "another random signal name", b"another random signal input", "another random signal identity"),
    ];
    let mut s = MockState::default();
    reapply_events(&mut s, &events).unwrap();
    assert_eq!(
        s.signals,
        vec![
            ("some random signal name".into(), b"some random signal input".to_vec(), "some random signal identity".into(), String::new()),
            ("another random signal name".into(), b"another random signal input".to_vec(), "another random signal identity".into(), String::new()),
        ]
    );
}

/// Go `TestPagination`: follows next page tokens until empty, batches in order.
#[test]
fn pagination_follows_page_tokens() {
    let mut h = MockHistory::default();
    h.pages.insert(
        key(b"branch", 1, 101, b""),
        (vec![vec![ev(1, EventType::WorkflowExecutionStarted), ev(2, EventType::DecisionTaskScheduled)]], b"some random token".to_vec()),
    );
    h.pages.insert(
        key(b"branch", 1, 101, b"some random token"),
        (vec![vec![ev(4, EventType::DecisionTaskCompleted)]], vec![]),
    );
    let mut s = MockState::default();
    assert_eq!(reapply_workflow_events(&h, &mut s, "d", 1, 101, b"branch").unwrap(), "");
    assert_eq!(*h.reads.borrow(), 2);
}
