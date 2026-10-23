//! Go parity ports for partially covered matching tests (partial upgrade B):
//! `go/service/matching/handler/handler_test.go` (TestAddActivityTask, TestAddDecisionTask,
//! TestPollForActivityTask, TestPollForDecisionTask) and
//! `go/service/matching/poller/manager_test.go` (every TestManager_* function).
//!
//! The handler tests call the public `MatchingApi` methods on `Matching` backed by the real
//! SQLite-durable queues and dynamic config. Go's gomock limiter and engine are not reproduced:
//! the rate-limit case is driven by the real domain quota, and the engine-failure case has no
//! Rust seam (see notes.md). The poller tests call the production `cadence_common::poller::Manager`
//! that `QueueHandle.pollers` wraps.
use cadence_api::{
    admin::{DynamicConfigFilter, DynamicConfigValue},
    enums::EncodingType,
    types::{DataBlob, DomainInfo},
};
use cadence_common::poller::{HistoricalInfo, Info, Manager as PollerManager};
use cadence_matching::{
    grpc_matching::Matching, matching_history::MatchingHistoryClient, matching_registry::QueueRegistry,
};
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
};
use cadence_proto::{api as p, matching as m};
use cadence_service_core::handlers::AppState;
use m::matching_api_server::MatchingApi;
use std::{collections::HashMap, num::NonZeroU32, sync::Arc, sync::atomic::{AtomicUsize, Ordering}, time::Duration};
use tonic::{Code, Request};
use uuid::Uuid;

const START: i64 = 1_730_073_600_000_000_000; // Go startTime: 2024-10-28T00:00:00Z
const SECOND: i64 = 1_000_000_000;
const MINUTE: i64 = 60 * SECOND;
const GROUP: &str = "the group";

fn quota_config(value: i64, domain: Option<&str>) -> DynamicConfigValue {
    let blob = |value: serde_json::Value| Some(DataBlob::new(EncodingType::JSON, serde_json::to_vec(&value).unwrap()));
    DynamicConfigValue {
        value: blob(serde_json::json!(value)),
        filters: domain
            .into_iter()
            .map(|name| DynamicConfigFilter { name: "domainName".into(), value: blob(serde_json::json!(name)) })
            .collect(),
    }
}

fn domain_state(domain_id: Uuid) -> AppState {
    let state = AppState::default();
    state
        .domains
        .create(
            DomainInfo { name: "quota-domain".into(), uuid: domain_id.to_string(), ..Default::default() },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    state
}

// Go handler_test: the limiter denies the second call. The domain quota of 1 per second makes the
// second call deny before the engine is reached; the host quota is generous so only the domain denies.
fn denied_state(domain_id: Uuid) -> AppState {
    let state = domain_state(domain_id);
    {
        let mut config = state.admin.dynamic_config.lock().unwrap();
        config.update(false, "matching.rps", vec![quota_config(100, None)]);
        config.update(false, "matching.domainrps", vec![quota_config(1, Some("quota-domain"))]);
    }
    state
}

fn durable(state: AppState) -> (tempfile::TempDir, Arc<SqliteTaskStore>, Matching) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteTaskStore::create(dir.path().join("queue.db"), BlobEncoding::ThriftRW).unwrap());
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    let queues = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap());
    let matching = Matching::with_durable_queues(state, queues, Duration::from_secs(10));
    (dir, store, matching)
}

fn assert_host_busy(status: tonic::Status) {
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(status.message(), "Matching host rps exceeded");
    assert!(!status.details().is_empty(), "Go-compatible ServiceBusyError detail is required");
}

fn queue_key(domain_id: Uuid, task_type: i32) -> TaskListKey {
    TaskListKey { domain_id, name: "queue".into(), task_type }
}

fn task_list() -> Option<p::TaskList> {
    Some(p::TaskList { name: "queue".into(), ..Default::default() })
}

fn execution() -> Option<p::WorkflowExecution> {
    Some(p::WorkflowExecution {
        workflow_id: "remote-workflow".into(),
        run_id: Uuid::from_u128(2).to_string(),
    })
}

fn activity_add(domain_id: Uuid) -> Request<m::AddActivityTaskRequest> {
    Request::new(m::AddActivityTaskRequest {
        domain_id: domain_id.to_string(),
        source_domain_id: domain_id.to_string(),
        task_list: task_list(),
        workflow_execution: execution(),
        schedule_id: 10,
        ..Default::default()
    })
}

fn decision_add(domain_id: Uuid) -> Request<m::AddDecisionTaskRequest> {
    Request::new(m::AddDecisionTaskRequest {
        domain_id: domain_id.to_string(),
        workflow_execution: execution(),
        task_list: task_list(),
        schedule_id: 42,
        schedule_to_start_timeout: Some(prost_types::Duration { seconds: 3, nanos: 0 }),
        ..Default::default()
    })
}

fn activity_poll(domain_id: Uuid, timeout: Duration) -> Request<m::PollForActivityTaskRequest> {
    let mut request = Request::new(m::PollForActivityTaskRequest {
        domain_id: domain_id.to_string(),
        request: Some(p::PollForActivityTaskRequest { task_list: task_list(), ..Default::default() }),
        ..Default::default()
    });
    request.set_timeout(timeout);
    request
}

fn decision_poll(domain_id: Uuid, timeout: Duration) -> Request<m::PollForDecisionTaskRequest> {
    let mut request = Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain_id.to_string(),
        request: Some(p::PollForDecisionTaskRequest { task_list: task_list(), ..Default::default() }),
        ..Default::default()
    });
    request.set_timeout(timeout);
    request
}

// Go TestAddActivityTask, "Error case - rate limiter not allowed": a denied add returns
// ServiceBusyError and never reaches the engine. The first add persists one activity task; the
// denied second add must leave the activity queue unchanged.
#[tokio::test]
async fn go_handler_add_activity_task_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9201);
    let (_dir, store, matching) = durable(denied_state(domain_id));
    matching.add_activity_task(activity_add(domain_id)).await.unwrap();
    assert_host_busy(matching.add_activity_task(activity_add(domain_id)).await.unwrap_err());
    assert_eq!(store.get_tasks(&queue_key(domain_id, 1), 0, i64::MAX, 10).unwrap().len(), 1);
}

// Go TestAddDecisionTask, "Error case - rate limiter not allowed".
#[tokio::test]
async fn go_handler_add_decision_task_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9202);
    let (_dir, store, matching) = durable(denied_state(domain_id));
    matching.add_decision_task(decision_add(domain_id)).await.unwrap();
    assert_host_busy(matching.add_decision_task(decision_add(domain_id)).await.unwrap_err());
    assert_eq!(store.get_tasks(&queue_key(domain_id, 0), 0, i64::MAX, 10).unwrap().len(), 1);
}

// Go TestPollForActivityTask, "Error case - rate limiter not allowed". The first poll consumes the
// domain quota and returns empty after its short deadline.
#[tokio::test]
async fn go_handler_poll_for_activity_task_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9203);
    let (_dir, _store, matching) = durable(denied_state(domain_id));
    let _ = matching.poll_for_activity_task(activity_poll(domain_id, Duration::from_millis(50))).await;
    assert_host_busy(
        matching
            .poll_for_activity_task(activity_poll(domain_id, Duration::from_millis(50)))
            .await
            .unwrap_err(),
    );
}

// Go TestPollForDecisionTask, "Error case - rate limiter not allowed".
#[tokio::test]
async fn go_handler_poll_for_decision_task_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9204);
    let (_dir, _store, matching) = durable(denied_state(domain_id));
    let _ = matching.poll_for_decision_task(decision_poll(domain_id, Duration::from_millis(50))).await;
    assert_host_busy(
        matching
            .poll_for_decision_task(decision_poll(domain_id, Duration::from_millis(50)))
            .await
            .unwrap_err(),
    );
}

// Go TestPollForActivityTask / TestPollForDecisionTask, "LongPollContextTimeout not set": with the
// limiter allowing, a long poll without a transport deadline is rejected before any wait.
#[tokio::test]
async fn go_handler_long_polls_without_deadline_return_context_timeout_not_set() {
    let matching = Matching::new(AppState::default());
    let decision = matching
        .poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(decision.code(), Code::InvalidArgument);
    assert_eq!(decision.message(), "Context timeout is not set.");
    let activity = matching
        .poll_for_activity_task(Request::new(m::PollForActivityTaskRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(activity.code(), Code::InvalidArgument);
    assert_eq!(activity.message(), "Context timeout is not set.");
}

// Go TestPollForActivityTask, "Success case": a poll that finds a task returns its token. Go's
// engine mock returns a fixed token; here the token comes from the task persisted by the add.
#[tokio::test]
async fn go_handler_poll_for_activity_task_returns_added_task_token() {
    let domain_id = Uuid::from_u128(9205);
    let (_dir, _store, matching) = durable(domain_state(domain_id));
    matching.add_activity_task(activity_add(domain_id)).await.unwrap();
    let response = matching
        .poll_for_activity_task(activity_poll(domain_id, Duration::from_secs(2)))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.task_token.is_empty(), "the added activity task must be delivered");
}

// Poller manager: a Go-style "Require identity" start records no history. Go's history callback
// is covered by the ignored gap below; this asserts the observable history instead.
#[test]
fn poller_start_without_identity_records_no_history() {
    let mut manager = PollerManager::default();
    manager.start("b", Info { identity: String::new(), rate_per_second: 0.0, isolation_group: String::new() }, START);
    assert_eq!(manager.count(), 0);
    assert!(manager.list(START).is_empty());
}

// Go TestManager_HistoryCallback, "Require identity" and StartPoll/EndPoll counter (manager_test.go:38).
#[test]
#[ignore = "parity gap: cadence_common::poller::Manager has no history-change callback; Go calls historyCallback on StartPoll and EndPoll with an identity (manager_test.go:38)"]
fn poller_history_callback_fires_on_start_and_end_with_identity() {
    panic!("Go historyCallback counter is not exposed by cadence_common::poller::Manager");
}

// Go TestManager_CancelPoll, cases "happy path", "repeated", "unknown" (manager_test.go:58). Rust's
// cancel callback is the production hook for these cases; the Go bool return is the gap below.
#[test]
fn poller_cancel_invokes_registered_callback_per_poll() {
    let mut manager = PollerManager::default();
    let cancelled = Arc::new(AtomicUsize::new(0));
    let other = Arc::new(AtomicUsize::new(0));
    manager.start("a", Info { identity: String::new(), rate_per_second: 0.0, isolation_group: String::new() }, START);
    let counter = cancelled.clone();
    manager.set_cancel("a", Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    manager.start("b", Info { identity: String::new(), rate_per_second: 0.0, isolation_group: String::new() }, START);
    let counter = other.clone();
    manager.set_cancel("b", Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    }));

    // happy path: one cancel runs the callback once.
    manager.cancel("a");
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    // repeated: cancelling again runs it again, and the poll stays outstanding.
    manager.cancel("a");
    assert_eq!(cancelled.load(Ordering::SeqCst), 2);
    assert!(manager.contains_poll("a"));
    // unknown: cancelling an id with no callback leaves other polls untouched.
    manager.cancel("unknown");
    assert_eq!(other.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.load(Ordering::SeqCst), 2);
}

// Go TestManager_CancelPoll "happy path"/"unknown" assert `CancelPoll` returns true and false.
#[test]
#[ignore = "parity gap: cadence_common::poller::Manager::cancel returns () while Go CancelPoll returns bool (true for a known poll, false for an unknown one) (manager_test.go:58)"]
fn poller_cancel_reports_whether_the_poll_is_known() {
    panic!("Rust Manager::cancel has no return value");
}

type Setup = fn(&mut PollerManager);

fn info(identity: &str, group: &str) -> Info {
    Info { identity: identity.into(), rate_per_second: 0.0, isolation_group: group.into() }
}

// Go TestManager_HasPollerFromIsolationGroupAfter (manager_test.go:103), all six cases with the
// same timestamps. The boundary case proves "after" is strict.
#[test]
fn poller_has_group_after_matches_go_cases() {
    let cases: Vec<(&str, Setup, i64, bool)> = vec![
        ("outstanding poller", |m: &mut PollerManager| m.start("a", info("", GROUP), START), START, true),
        ("recent poller", |m: &mut PollerManager| {
            m.start("a", info("", GROUP), START);
            m.end("a", START + SECOND);
        }, START, true),
        ("boundary condition poller", |m: &mut PollerManager| {
            m.start("a", info("", GROUP), START);
            m.end("a", START + 1);
        }, START + 1, false),
        ("expired poller", |m: &mut PollerManager| {
            m.start("a", info("", GROUP), START);
            m.end("a", START);
        }, START + MINUTE, false),
        ("never polled", |_m: &mut PollerManager| {}, START, false),
        ("unrelated poller", |m: &mut PollerManager| {
            m.start("a", info("", "other"), START);
            m.end("a", START + SECOND);
        }, START, false),
    ];
    for (name, setup, after, expected) in cases {
        let mut manager = PollerManager::default();
        setup(&mut manager);
        assert_eq!(manager.has_group_after(GROUP, after), expected, "{name}");
    }
}

// Go TestManager_HasPollerAfter (manager_test.go:177), all five cases. The "finished before the
// query time" branch is the "expired poller" and "boundary condition poller" rows.
#[test]
fn poller_has_after_matches_go_cases() {
    let cases: Vec<(&str, Setup, i64, bool)> = vec![
        ("outstanding poller", |m: &mut PollerManager| m.start("a", info("", ""), START), START, true),
        ("recent poller", |m: &mut PollerManager| {
            m.start("a", info("", ""), START);
            m.end("a", START + SECOND);
        }, START, true),
        ("boundary condition poller", |m: &mut PollerManager| {
            m.start("a", info("", ""), START);
            m.end("a", START + 1);
        }, START + 1, false),
        ("expired poller", |m: &mut PollerManager| {
            m.start("a", info("", ""), START);
            m.end("a", START);
        }, START + MINUTE, false),
        ("never polled", |_m: &mut PollerManager| {}, START, false),
    ];
    for (name, setup, after, expected) in cases {
        let mut manager = PollerManager::default();
        setup(&mut manager);
        assert_eq!(manager.has_after(after), expected, "{name}");
    }
}

// Go TestManager_GetCount (manager_test.go:240): the cache does not evict on Size, so all four
// identities remain counted, including the expired "a".
#[test]
fn poller_count_keeps_expired_entries_like_go_size() {
    let mut manager = PollerManager::default();
    manager.start("a", info("aIdent", ""), START);
    manager.end("a", START);
    manager.start("b", info("bIdent", ""), START);
    manager.end("b", START + 4 * MINUTE);
    manager.start("c", info("cIdent", ""), START + 6 * MINUTE);
    manager.end("c", START + 6 * MINUTE);
    manager.start("d", info("dIdent", ""), START + 6 * MINUTE);
    assert_eq!(manager.count(), 4);
}

fn listed(identity: &str, rate_per_second: f64, last_access_time: i64) -> HistoricalInfo {
    HistoricalInfo { identity: identity.into(), rate_per_second, last_access_time }
}

// Go TestManager_ListInfo (manager_test.go:257), all three cases. Results are newest first, poll
// identities without an identity are excluded, and a poll without a poll id is still listed.
#[test]
fn poller_list_info_matches_go_cases() {
    // happy path
    let mut manager = PollerManager::default();
    manager.start("a", info("aIdent", ""), START);
    manager.end("a", START);
    manager.start("b", info("bIdent", ""), START);
    manager.end("b", START + MINUTE);
    manager.start("c", Info { identity: "cIdent".into(), rate_per_second: 1.0, isolation_group: String::new() }, START + 2 * MINUTE);
    manager.end("c", START + 2 * MINUTE);
    manager.start("d", info("dIdent", ""), START + 2 * MINUTE);
    assert_eq!(
        manager.list(START + 2 * MINUTE),
        vec![
            listed("dIdent", 0.0, START + 2 * MINUTE),
            listed("cIdent", 1.0, START + 2 * MINUTE),
            listed("bIdent", 0.0, START + MINUTE),
            listed("aIdent", 0.0, START),
        ]
    );

    // exclude pollers with no identity
    let mut manager = PollerManager::default();
    manager.start("a", info("", ""), START);
    manager.end("a", START);
    assert!(manager.list(START).is_empty());

    // include pollers with no pollerID
    let mut manager = PollerManager::default();
    manager.start("", info("aIdent", ""), START);
    assert_eq!(manager.list(START), vec![listed("aIdent", 0.0, START)]);
}

// Go TestManager_GetCountByIsolationGroup (manager_test.go:332), all four cases.
#[test]
fn poller_count_by_group_matches_go_cases() {
    fn grouped(identity: &str, group: &str, rate_per_second: f64) -> Info {
        Info { identity: identity.into(), rate_per_second, isolation_group: group.into() }
    }
    let happy = |manager: &mut PollerManager| {
        manager.start("a", grouped("aIdent", "groupA", 0.0), START);
        manager.end("a", START);
        manager.start("b", grouped("bIdent", "groupA", 0.0), START);
        manager.end("b", START + MINUTE);
        manager.start("c", grouped("cIdent", "groupB", 1.0), START + 2 * MINUTE);
        manager.end("c", START + 2 * MINUTE);
        manager.start("d", info("dIdent", ""), START + 2 * MINUTE);
    };

    // happy path
    let mut manager = PollerManager::default();
    happy(&mut manager);
    assert_eq!(
        manager.count_by_group(START + 2 * MINUTE, i64::MIN),
        HashMap::from([("groupA".to_string(), 2), ("groupB".to_string(), 1)])
    );

    // some expired: the first "a" ended at START, so only the polls after START + 1m - 1ns count.
    let mut manager = PollerManager::default();
    happy(&mut manager);
    assert_eq!(
        manager.count_by_group(START + 2 * MINUTE, START + MINUTE - 1),
        HashMap::from([("groupA".to_string(), 1), ("groupB".to_string(), 1)])
    );

    // exclude pollers with no identity
    let mut manager = PollerManager::default();
    manager.start("a", info("", ""), START);
    manager.end("a", START);
    assert!(manager.count_by_group(START, START - 1).is_empty());

    // include pollers with no pollerID
    let mut manager = PollerManager::default();
    manager.start("", grouped("aIdent", "groupA", 0.0), START);
    assert_eq!(manager.count_by_group(START, START - 1), HashMap::from([("groupA".to_string(), 1)]));
}
