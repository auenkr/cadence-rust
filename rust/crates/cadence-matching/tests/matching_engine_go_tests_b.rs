//! Go parity ports of test functions 7 through 12 in
//! `go/service/matching/handler/engine_test.go`, in file order:
//! `TestQueryWorkflow`, `TestWaitForQueryResult`, `TestIsShuttingDown`,
//! `TestGetTasklistsNotOwned`, `TestShutDownTasklistsNotOwned`, and
//! `TestUpdateTaskListPartitionConfig`.
//!
//! Rust has no `matchingEngineImpl`. These tests call the public `MatchingApi`
//! methods on `Matching`, `query_outcome` from `matching_query`, and
//! `QueueRegistry` backed by the real SQLite store, with `Membership` supplying
//! ownership. Go's gomock managers, executor, and version checker are not
//! reproduced. Where a Go expectation depends on them, the test is `#[ignore]`
//! with the gap named.
use cadence_api::{
    admin::DynamicConfigValue,
    enums::EncodingType,
    types::{DataBlob, DomainInfo},
};
use cadence_common::membership::Host;
use cadence_matching::{
    grpc_matching::Matching,
    matching_history::MatchingHistoryClient,
    matching_membership::Membership,
    matching_query::{query_outcome, QueryOutcome, WorkerAnswer},
    matching_registry::QueueRegistry,
};
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
    StoredPartitionConfig,
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
use tonic::{Code, Request, Status};
use uuid::Uuid;

const DOMAIN: &str = "test-domain";
const TASK_LIST: &str = "test-tasklist";
const INVALID_TASK_LIST: &str = "/__cadence_sys/invalid-tasklist-name";
/// Go `TaskListKindSticky` on the wire.
const STICKY_KIND: i32 = 2;
/// Go `TaskListTypeActivity` on the wire.
const ACTIVITY_TYPE: i32 = 1;

fn domain_id() -> Uuid {
    Uuid::from_u128(1)
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

fn host(identity: &str) -> Host {
    Host {
        address: format!("{identity}:7935"),
        identity: identity.into(),
        grpc_address: format!("{identity}:7835"),
    }
}

/// Go `newMockManagerWithTaskListID` and the engine's durable backend: a
/// SQLite task store behind a `QueueRegistry`, wired into `Matching`. The
/// history peer is unreachable and never dialed by these RPCs.
struct Durable {
    _dir: tempfile::TempDir,
    matching: Matching,
    queues: QueueRegistry,
}

fn durable_with(state: AppState, membership: Option<Membership>) -> Durable {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("queue.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    let mut queues = QueueRegistry::new(store, history, NonZeroU32::new(100).unwrap());
    if let Some(membership) = membership {
        queues = queues.with_membership(membership);
    }
    let matching = Matching::with_durable_queues(state, queues.clone(), Duration::from_secs(10));
    Durable {
        _dir: dir,
        matching,
        queues,
    }
}

fn durable(state: AppState) -> Durable {
    durable_with(state, None)
}

// ---------------------------------------------------------------------------
// TestQueryWorkflow
// ---------------------------------------------------------------------------

fn query_request(task_list: &str, kind: i32) -> Request<m::QueryWorkflowRequest> {
    Request::new(m::QueryWorkflowRequest {
        domain_id: domain_id().to_string(),
        request: Some(p::QueryWorkflowRequest {
            query: Some(p::WorkflowQuery {
                query_type: "ping".into(),
                query_args: None,
            }),
            ..Default::default()
        }),
        task_list: Some(p::TaskList {
            name: task_list.into(),
            kind,
            ..Default::default()
        }),
        ..Default::default()
    })
}

#[tokio::test]
async fn query_workflow_invalid_tasklist_name_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    assert!(durable
        .matching
        .query_workflow(query_request(INVALID_TASK_LIST, 0))
        .await
        .is_err());
}

#[tokio::test]
async fn query_workflow_sticky_worker_unavailable_is_error() {
    // Go: a sticky list with no poller in the window fails before dispatch.
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    let err = durable
        .matching
        .query_workflow(query_request(TASK_LIST, STICKY_KIND))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
}

#[tokio::test]
#[ignore = "parity gap: Go injects a DispatchQueryTask error through a gomock TaskListManager; Rust has no seam to fail dispatch, and a non-sticky query with no poller waits for the deadline instead of erroring"]
async fn query_workflow_failed_to_dispatch_query_task_is_error() {
    unimplemented!("no public seam to inject a dispatch failure");
}

#[tokio::test]
async fn query_workflow_success_returns_worker_answer() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    // Go's mock DispatchQueryTask writes the answer into the waiting channel.
    // Here a parked poller takes the pending query and completes it through
    // the same `QueryChannel` a real `PollForDecisionTask` uses.
    let queue = durable
        .queues
        .get(key(domain_id(), TASK_LIST, 0), 0, now_nanos())
        .await
        .unwrap();
    let queries = queue.queries.clone();
    let poller = tokio::spawn(async move {
        let pending = queries.next().await;
        queries
            .complete(
                &pending.task_id,
                QueryOutcome::Worker(WorkerAnswer::new(answered(b"some result"))),
            )
            .unwrap();
    });
    let resp = durable
        .matching
        .query_workflow(query_request(TASK_LIST, 0))
        .await
        .unwrap()
        .into_inner();
    poller.await.unwrap();
    assert_eq!(resp.query_result.unwrap().data, b"some result");
}

#[tokio::test]
async fn query_workflow_success_reports_partition_config() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    let queue = durable
        .queues
        .get(key(domain_id(), TASK_LIST, 0), 0, now_nanos())
        .await
        .unwrap();
    // Go's gomock manager reports a root config (version 1, read/write {0}).
    // Rust does not persist a single-partition config, so seed the cache with it.
    queue.partitions.push(&StoredPartitionConfig {
        version: 1,
        read_partitions: HashMap::from([(0, Vec::new())]),
        write_partitions: HashMap::from([(0, Vec::new())]),
    });
    let queries = queue.queries.clone();
    let poller = tokio::spawn(async move {
        let pending = queries.next().await;
        queries
            .complete(
                &pending.task_id,
                QueryOutcome::Worker(WorkerAnswer::new(answered(b"some result"))),
            )
            .unwrap();
    });
    let resp = durable
        .matching
        .query_workflow(query_request(TASK_LIST, 0))
        .await
        .unwrap()
        .into_inner();
    poller.await.unwrap();
    assert_eq!(resp.partition_config.unwrap().version, 1);
}

// ---------------------------------------------------------------------------
// TestWaitForQueryResult
// ---------------------------------------------------------------------------

fn answered(data: &[u8]) -> p::WorkflowQueryResult {
    p::WorkflowQueryResult {
        result_type: p::QueryResultType::Answered as i32,
        answer: Some(p::Payload {
            data: data.to_vec(),
        }),
        error_message: String::new(),
    }
}

fn worker_outcome(
    result: p::WorkflowQueryResult,
    client_impl: &str,
    feature_version: &str,
) -> QueryOutcome {
    QueryOutcome::Worker(WorkerAnswer {
        result,
        worker_version_info: Some(p::WorkerVersionInfo {
            r#impl: client_impl.into(),
            feature_version: feature_version.into(),
        }),
    })
}

#[test]
fn wait_for_query_result_internal_error_is_passed_through() {
    let err =
        query_outcome(QueryOutcome::Internal(Status::internal("some error")), true).unwrap_err();
    assert_eq!(err.message(), "some error");
}

#[test]
fn wait_for_query_result_strong_consistency_version_unsupported() {
    // Go mocks `SupportsConsistentQuery` to fail; Rust runs the real version
    // gate, which rejects uber-go 1.0.0 (below 1.5.0).
    let err = query_outcome(
        worker_outcome(p::WorkflowQueryResult::default(), "uber-go", "1.0.0"),
        true,
    )
    .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(err.message(), "Client version not supported");
}

#[test]
fn wait_for_query_result_success_query_task_completed() {
    // Go mocks the version check to pass for 1.0.0; Rust needs a supported
    // version, so the real gate is exercised at 1.5.0.
    let resp = query_outcome(
        worker_outcome(answered(b"some result"), "uber-go", "1.5.0"),
        true,
    )
    .unwrap();
    assert_eq!(resp.query_result.unwrap().data, b"some result");
}

#[test]
fn wait_for_query_result_query_task_failed() {
    let result = p::WorkflowQueryResult {
        result_type: p::QueryResultType::Failed as i32,
        answer: None,
        error_message: "query failed".into(),
    };
    let err = query_outcome(worker_outcome(result, "uber-go", "1.5.0"), true).unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), "query failed");
}

#[test]
fn wait_for_query_result_unknown_query_result_is_internal_error() {
    let result = p::WorkflowQueryResult {
        result_type: 100,
        answer: None,
        error_message: String::new(),
    };
    let err = query_outcome(worker_outcome(result, "uber-go", "1.5.0"), true).unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert_eq!(err.message(), "unknown query completed type");
}

// ---------------------------------------------------------------------------
// TestIsShuttingDown
// ---------------------------------------------------------------------------

#[test]
#[ignore = "parity gap: Go isShuttingDown reads the engine's shutdown channel after Start/Stop on matchingEngineImpl; Rust MatchingRuntime::shutdown exposes no public shutting-down state to observe before and after"]
fn is_shutting_down_is_false_until_stop() {
    unimplemented!("no public seam for the Rust shutdown state");
}

// ---------------------------------------------------------------------------
// TestGetTasklistsNotOwned
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_tasklists_not_owned_returns_only_non_owned_managers() {
    // Same ring setup as shut_down_tasklists_not_owned_unloads_only_moved_owner: two
    // names stay with self, and one moves to host456 when membership changes.
    let probe = Membership::new("self", vec![host("self"), host("host456")]);
    let names: Vec<String> = (1..=64).map(|i| format!("tl{i}")).collect();
    let owned: Vec<&String> = names
        .iter()
        .filter(|name| probe.owns(name.as_str()).unwrap())
        .collect();
    let moved: Vec<&String> = names
        .iter()
        .filter(|name| !probe.owns(name.as_str()).unwrap())
        .collect();
    let (stays_one, moves, stays_two) = (owned[0], moved[0], owned[1]);

    let membership = Membership::new("self", vec![host("self")]);
    let durable = durable_with(AppState::default(), Some(membership.clone()));
    for name in [stays_one, moves, stays_two] {
        durable
            .queues
            .get(key(domain_id(), name, 0), 0, now_nanos())
            .await
            .unwrap();
    }
    membership.replace(vec![host("self"), host("host456")]);
    // Go returns [tl2m]: exactly the managers the ring no longer assigns to this host.
    let not_owned = durable.queues.unload_non_owned(now_nanos()).await.unwrap();
    assert_eq!(not_owned, vec![moves.to_string()]);
}

// ---------------------------------------------------------------------------
// TestShutDownTasklistsNotOwned
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shut_down_tasklists_not_owned_unloads_only_moved_owner() {
    // Go pins owners as tl1 -> self, tl2 -> host456, tl3 -> self through a
    // scripted resolver. The Rust ring is real, so pick names from the same ring
    // instead: two that stay with self and one that moves to host456 once the
    // ring changes.
    let probe = Membership::new("self", vec![host("self"), host("host456")]);
    let names: Vec<String> = (1..=64).map(|i| format!("tl{i}")).collect();
    let owned: Vec<&String> = names
        .iter()
        .filter(|name| probe.owns(name.as_str()).unwrap())
        .collect();
    let moved: Vec<&String> = names
        .iter()
        .filter(|name| !probe.owns(name.as_str()).unwrap())
        .collect();
    let (stays_one, moves, stays_two) = (owned[0], moved[0], owned[1]);

    // Create the queues while self owns every name, then move ownership.
    let membership = Membership::new("self", vec![host("self")]);
    let durable = durable_with(AppState::default(), Some(membership.clone()));
    for name in [stays_one, moves, stays_two] {
        durable
            .queues
            .get(key(domain_id(), name, 0), 0, now_nanos())
            .await
            .unwrap();
    }
    membership.replace(vec![host("self"), host("host456")]);
    durable.queues.unload_non_owned(now_nanos()).await.unwrap();

    assert!(durable
        .queues
        .contains_cached(&key(domain_id(), stays_one, 0)));
    assert!(!durable.queues.contains_cached(&key(domain_id(), moves, 0)));
    assert!(durable
        .queues
        .contains_cached(&key(domain_id(), stays_two, 0)));
}

// ---------------------------------------------------------------------------
// TestUpdateTaskListPartitionConfig
// ---------------------------------------------------------------------------

/// Go's proto mapper sends `NumReadPartitions = len(ReadPartitions)` (and the
/// write count likewise), and Rust's `partition_map` reads the count, so the
/// deprecated counts must be set to match the maps.
#[allow(deprecated)]
fn root_partition_config(version: i64) -> p::TaskListPartitionConfig {
    p::TaskListPartitionConfig {
        version,
        num_read_partitions: 1,
        num_write_partitions: 1,
        read_partitions: HashMap::from([(0, Default::default())]),
        write_partitions: HashMap::from([(0, Default::default())]),
    }
}

fn update_request(
    task_list: &str,
    kind: i32,
    config: Option<p::TaskListPartitionConfig>,
) -> Request<m::UpdateTaskListPartitionConfigRequest> {
    Request::new(m::UpdateTaskListPartitionConfigRequest {
        domain_id: domain_id().to_string(),
        task_list: Some(p::TaskList {
            name: task_list.into(),
            kind,
            ..Default::default()
        }),
        task_list_type: ACTIVITY_TYPE,
        partition_config: config,
    })
}

#[tokio::test]
async fn update_task_list_partition_config_success() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    durable
        .matching
        .update_task_list_partition_config(update_request(
            TASK_LIST,
            0,
            Some(root_partition_config(1)),
        ))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "parity gap: Go injects a TaskListManager.UpdateTaskListPartitionConfig error through a gomock; Rust has no seam to make the durable partition update fail"]
async fn update_task_list_partition_config_tasklist_manager_error() {
    unimplemented!("no public seam to inject a partition update failure");
}

#[tokio::test]
async fn update_task_list_partition_config_non_root_partition_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    let err = durable
        .matching
        .update_task_list_partition_config(update_request(
            "/__cadence_sys/test-tasklist/1",
            0,
            Some(root_partition_config(1)),
        ))
        .await
        .unwrap_err();
    assert_eq!(
        err.message(),
        "Only root partition's partition config can be updated."
    );
}

#[tokio::test]
async fn update_task_list_partition_config_invalid_tasklist_name_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    let err = durable
        .matching
        .update_task_list_partition_config(update_request(
            "/__cadence_sys/test-tasklist",
            0,
            Some(root_partition_config(1)),
        ))
        .await
        .unwrap_err();
    assert!(
        err.message()
            .contains("invalid partitioned task list name /__cadence_sys/test-tasklist"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn update_task_list_partition_config_nil_partition_config_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    let err = durable
        .matching
        .update_task_list_partition_config(update_request(TASK_LIST, 0, None))
        .await
        .unwrap_err();
    assert_eq!(
        err.message(),
        "Task list partition config is not set in the request."
    );
}

#[tokio::test]
async fn update_task_list_partition_config_sticky_kind_is_error() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let durable = durable(state);
    let err = durable
        .matching
        .update_task_list_partition_config(update_request(TASK_LIST, STICKY_KIND, None))
        .await
        .unwrap_err();
    assert_eq!(
        err.message(),
        "Only normal tasklist's partition config can be updated."
    );
}

#[tokio::test]
async fn update_task_list_partition_config_manual_update_rejected_when_scaler_enabled() {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    set_config(&state, "matching.enableAdaptiveScaler", json!(true));
    let durable = durable(state);
    let err = durable
        .matching
        .update_task_list_partition_config(update_request(TASK_LIST, STICKY_KIND, None))
        .await
        .unwrap_err();
    assert_eq!(
        err.message(),
        "Manual update is not allowed because adaptive scaler is enabled."
    );
}
