//! Go parity ports of the first six test functions in
//! `go/service/matching/handler/engine_test.go`, in file order:
//! `TestGetTaskListsByDomain`, `TestListTaskListPartitions`,
//! `TestCancelOutstandingPoll`, `TestErrIfShardOwnershipLost`,
//! `TestIsExcludedFromShardDistributor`, and `TestRespondQueryTaskCompleted`.
//!
//! Rust has no `matchingEngineImpl`. These tests call the public `MatchingApi`
//! methods on `Matching`, backed by the real SQLite-durable `QueueRegistry`,
//! the in-memory domain store, and dynamic config. Go's gomock managers,
//! membership resolver, and shard executor are not reproduced. Where a Go
//! expectation depends on them, the test is `#[ignore]` with the gap named.
use cadence_api::{
    admin::DynamicConfigValue,
    enums::EncodingType,
    types::{DataBlob, DomainInfo},
};
use cadence_matching::{
    grpc_matching::Matching, matching_history::MatchingHistoryClient,
    matching_query::QueryOutcome, matching_registry::QueueRegistry,
    matching_shard_routing::task_list_excluded_from_shard_distributor,
};
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
};
use cadence_proto::{api as p, matching as m};
use cadence_service_core::handlers::AppState;
use m::matching_api_server::MatchingApi;
use serde_json::json;
use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tonic::{Code, Request};
use uuid::Uuid;

const DOMAIN: &str = "test-domain";
const OTHER_DOMAIN: &str = "other-domain";
const TASK_LIST: &str = "test-tasklist";
const INVALID_TASK_LIST: &str = "/__cadence_sys/invalid-tasklist-name";
const UUID_TASK_LIST: &str = "tasklist-550e8400-e29b-41d4-a716-446655440000";
const UUID_ONLY_TASK_LIST: &str = "550e8400-e29b-41d4-a716-446655440000";

fn domain_id() -> Uuid {
    Uuid::from_u128(1)
}

fn other_domain_id() -> Uuid {
    Uuid::from_u128(2)
}

fn add_domain(state: &AppState, name: &str, id: Uuid) {
    state
        .domains
        .create(
            DomainInfo {
                name: name.into(),
                uuid: id.to_string(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
}

fn set_config(state: &AppState, name: &str, value: serde_json::Value) {
    let blob = DataBlob::new(EncodingType::JSON, serde_json::to_vec(&value).unwrap());
    state.admin.dynamic_config.lock().unwrap().update(
        false,
        name,
        vec![DynamicConfigValue {
            value: Some(blob),
            filters: Vec::new(),
        }],
    );
}

fn key(domain: Uuid, name: &str, task_type: i32) -> TaskListKey {
    TaskListKey {
        domain_id: domain,
        name: name.into(),
        task_type,
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// Go `newMockManagerWithTaskListID` and the engine's durable backend: a
/// SQLite task store behind a `QueueRegistry`, wired into `Matching`. The
/// history peer is unreachable and never dialed by these RPCs.
struct Durable {
    _dir: tempfile::TempDir,
    matching: Matching,
    queues: QueueRegistry,
}

fn durable(state: AppState) -> Durable {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("queue.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    let queues = QueueRegistry::new(store, history, NonZeroU32::new(100).unwrap());
    let matching = Matching::with_durable_queues(state, queues.clone(), Duration::from_secs(10));
    Durable {
        _dir: dir,
        matching,
        queues,
    }
}

fn sorted_keys<V>(map: &HashMap<String, V>) -> Vec<&str> {
    let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys
}

// ---------------------------------------------------------------------------
// TestGetTaskListsByDomain
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_task_lists_by_domain__domain_cache_error_is_not_found() {
    // Go: GetDomainID returns an error, so the RPC returns an error.
    let matching = Matching::new(AppState::default());
    let err = matching
        .get_task_lists_by_domain(Request::new(m::GetTaskListsByDomainRequest {
            domain: DOMAIN.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

/// Loads the Go fixture's task lists (decision0, activity0, sticky0 in the
/// requested domain, plus other0 in another domain) and describes the domain.
async fn describe_loaded_lists(all_kinds: bool) -> m::GetTaskListsByDomainResponse {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    add_domain(&state, OTHER_DOMAIN, other_domain_id());
    if all_kinds {
        set_config(&state, "matching.matchingReturnAllTaskListKinds", json!(true));
    }
    let durable = durable(state);
    let now = now_nanos();
    // Kind 1 is sticky in Rust's QueueHandle; sticky lists are hidden unless all kinds are requested.
    for (domain, name, task_type, kind) in [
        (domain_id(), "decision0", 0, 0),
        (domain_id(), "activity0", 1, 0),
        (other_domain_id(), "other0", 0, 0),
        (domain_id(), "sticky0", 0, 1),
    ] {
        durable
            .queues
            .get(key(domain, name, task_type), kind, now)
            .await
            .unwrap();
    }
    durable
        .matching
        .get_task_lists_by_domain(Request::new(m::GetTaskListsByDomainRequest {
            domain: DOMAIN.into(),
        }))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn get_task_lists_by_domain__success_lists_normal_kinds() {
    let resp = describe_loaded_lists(false).await;
    assert_eq!(sorted_keys(&resp.decision_task_list_map), vec!["decision0"]);
    assert_eq!(sorted_keys(&resp.activity_task_list_map), vec!["activity0"]);
}

#[tokio::test]
async fn get_task_lists_by_domain__success_all_kinds_includes_sticky() {
    let resp = describe_loaded_lists(true).await;
    assert_eq!(
        sorted_keys(&resp.decision_task_list_map),
        vec!["decision0", "sticky0"]
    );
    assert_eq!(sorted_keys(&resp.activity_task_list_map), vec!["activity0"]);
}

#[tokio::test]
#[ignore = "parity gap: Go describes mock managers with scripted pollers (Identity \"test-poller-decision0\"); Rust describes queues whose poller history is only populated by live long-poll traffic, and no public seam injects poller identities"]
async fn get_task_lists_by_domain__success_reports_poller_identity() {
    let resp = describe_loaded_lists(false).await;
    assert_eq!(
        resp.decision_task_list_map["decision0"].pollers[0].identity,
        "test-poller-decision0"
    );
}

// ---------------------------------------------------------------------------
// TestListTaskListPartitions
// ---------------------------------------------------------------------------

fn list_partitions_request(task_list: &str) -> Request<m::ListTaskListPartitionsRequest> {
    Request::new(m::ListTaskListPartitionsRequest {
        domain: DOMAIN.into(),
        task_list: Some(p::TaskList {
            name: task_list.into(),
            ..Default::default()
        }),
    })
}

#[tokio::test]
async fn list_task_list_partitions__domain_cache_error_is_not_found() {
    let matching = Matching::new(AppState::default());
    let err = matching
        .list_task_list_partitions(list_partitions_request(TASK_LIST))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn list_task_list_partitions__invalid_tasklist_name_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let matching = Matching::new(state);
    let err = matching
        .list_task_list_partitions(list_partitions_request(INVALID_TASK_LIST))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
}

#[tokio::test]
async fn list_task_list_partitions__success_returns_root_then_write_partitions() {
    // Go `NumTasklistWritePartitions` = 3, so root plus partitions 1 and 2 for both kinds.
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    set_config(&state, "matching.numTasklistWritePartitions", json!(3));
    let durable = durable(state);
    let resp = durable
        .matching
        .list_task_list_partitions(list_partitions_request(TASK_LIST))
        .await
        .unwrap()
        .into_inner();
    let keys = |partitions: &[p::TaskListPartitionMetadata]| {
        partitions.iter().map(|partition| partition.key.clone()).collect::<Vec<_>>()
    };
    let expected = vec![
        TASK_LIST.to_string(),
        "/__cadence_sys/test-tasklist/1".to_string(),
        "/__cadence_sys/test-tasklist/2".to_string(),
    ];
    assert_eq!(keys(&resp.decision_task_list_partitions), expected);
    assert_eq!(keys(&resp.activity_task_list_partitions), expected);
}

#[tokio::test]
#[ignore = "parity gap: Go resolves each partition owner through a scripted membership Lookup (addr0..addr3, with one lookup error yielding an empty owner); Rust resolves owners from the real hash ring, so the scripted owner names cannot be reproduced through the public API"]
async fn list_task_list_partitions__success_owner_hosts_match_go_lookups() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    set_config(&state, "matching.numTasklistWritePartitions", json!(3));
    let durable = durable(state);
    let resp = durable
        .matching
        .list_task_list_partitions(list_partitions_request(TASK_LIST))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.decision_task_list_partitions[0].owner_host_name, "addr0");
    assert_eq!(resp.decision_task_list_partitions[1].owner_host_name, "");
    assert_eq!(resp.decision_task_list_partitions[2].owner_host_name, "addr1");
    assert_eq!(resp.activity_task_list_partitions[0].owner_host_name, "addr2");
    assert_eq!(resp.activity_task_list_partitions[2].owner_host_name, "addr3");
}

// ---------------------------------------------------------------------------
// TestCancelOutstandingPoll
// ---------------------------------------------------------------------------

fn cancel_request(task_list: &str) -> Request<m::CancelOutstandingPollRequest> {
    Request::new(m::CancelOutstandingPollRequest {
        domain_id: domain_id().to_string(),
        task_list_type: 0,
        task_list: Some(p::TaskList {
            name: task_list.into(),
            ..Default::default()
        }),
        poller_id: "test-poller-id".into(),
    })
}

#[tokio::test]
async fn cancel_outstanding_poll__invalid_tasklist_name_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    assert!(durable
        .matching
        .cancel_outstanding_poll(cancel_request(INVALID_TASK_LIST))
        .await
        .is_err());
}

#[tokio::test]
async fn cancel_outstanding_poll__success() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    durable
        .matching
        .cancel_outstanding_poll(cancel_request(TASK_LIST))
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// TestErrIfShardOwnershipLost
// ---------------------------------------------------------------------------

#[test]
#[ignore = "parity gap: Go errIfShardOwnershipLost is one engine method that a unit test calls with a mock executor, resolver, and shutdown channel; Rust ownership is the private closure shard_task_list_ownership_check in matching_runtime.rs, reachable only through a live shard distributor, so the 8 Go subtests (SD ProcessNotFound, nil process, shutdown, excluded owner changed/same, non-excluded UUID name) cannot run against production code from tests/"]
fn err_if_shard_ownership_lost__go_subtests_not_reproduced() {
    unimplemented!("no public seam for the production shard-ownership closure");
}

// ---------------------------------------------------------------------------
// TestIsExcludedFromShardDistributor
// ---------------------------------------------------------------------------

#[test]
fn is_excluded_from_shard_distributor__go_cases() {
    // Go pins percentage onboarded at 100, so the short-lived flag and a UUID in the name decide.
    let cases = [
        ("flag disabled, uuid name", UUID_TASK_LIST, false, false),
        ("flag enabled, no uuid in name", "my-regular-tasklist", true, false),
        ("flag enabled, uuid in name", UUID_TASK_LIST, true, true),
        ("flag enabled, uuid-only name", UUID_ONLY_TASK_LIST, true, true),
        ("flag disabled, no uuid in name", "my-regular-tasklist", false, false),
    ];
    for (name, task_list, flag_enabled, want) in cases {
        assert_eq!(
            task_list_excluded_from_shard_distributor(task_list, 100, flag_enabled),
            want,
            "{name}"
        );
    }
}

// ---------------------------------------------------------------------------
// TestRespondQueryTaskCompleted
// ---------------------------------------------------------------------------

fn respond_query_request(task_id: &str) -> Request<m::RespondQueryTaskCompletedRequest> {
    Request::new(m::RespondQueryTaskCompletedRequest {
        domain_id: domain_id().to_string(),
        task_list: Some(p::TaskList {
            name: TASK_LIST.into(),
            ..Default::default()
        }),
        task_id: task_id.into(),
        request: Some(p::RespondQueryTaskCompletedRequest::default()),
    })
}

#[tokio::test]
async fn respond_query_task_completed__success_delivers_answer_to_waiter() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    // Go pre-registers "id-0" in queryTaskMap. Registering a waiter on the
    // decision queue is the Rust equivalent of an in-flight QueryWorkflow.
    let queue = durable
        .queues
        .get(key(domain_id(), TASK_LIST, 0), 0, now_nanos())
        .await
        .unwrap();
    let answer = queue.queries.register_waiter("id-0");
    durable
        .matching
        .respond_query_task_completed(respond_query_request("id-0"))
        .await
        .unwrap();
    assert!(matches!(answer.await.unwrap(), QueryOutcome::Worker(_)));
}

#[tokio::test]
async fn respond_query_task_completed__query_task_not_found_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    assert!(durable
        .matching
        .respond_query_task_completed(respond_query_request("id-0"))
        .await
        .is_err());
}
