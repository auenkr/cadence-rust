//! Go parity for PARTIAL rows of `go/service/matching` that the public
//! `Matching` and `QueueRegistry` API reaches without private seams:
//! `tasklist/task_list_manager_test.go` (RefreshTaskListPartitionConfig and its
//! concurrency cases, ManagerStart, DispatchTask) and
//! `wrappers/thrift/thrift_handler_test.go`. Go's gomock managers are not
//! reproduced. Where a Go assertion needs one, the test is `#[ignore]` with the
//! gap named.
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
    StoredPartitionConfig,
};
use cadence_matching::{
    grpc_matching::Matching, matching_history::MatchingHistoryClient,
    matching_registry::QueueRegistry, thrift_matching::dispatch,
};
use cadence_proto::{api as p, matching as m};
use cadence_service_core::handlers::AppState;
use cadence_thrift::Struct;
use cadence_api::types::DomainInfo;
use m::matching_api_server::MatchingApi;
use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tonic::{Code, Request};
use uuid::Uuid;

const DOMAIN: &str = "test-domain";
const TASK_LIST: &str = "tl";
const CHILD_LIST: &str = "/__cadence_sys/tl/1";

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

fn key(name: &str, task_type: i32) -> TaskListKey {
    TaskListKey {
        domain_id: domain_id(),
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

fn registry(store: Arc<SqliteTaskStore>) -> QueueRegistry {
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    QueueRegistry::new(store, history, NonZeroU32::new(100).unwrap())
}

/// A SQLite task store behind a `QueueRegistry`, wired into `Matching`.
struct Durable {
    _dir: tempfile::TempDir,
    store: Arc<SqliteTaskStore>,
    matching: Matching,
    queues: QueueRegistry,
}

fn durable() -> Durable {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("queue.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let queues = registry(store.clone());
    let matching = Matching::with_durable_queues(state, queues.clone(), Duration::from_secs(10));
    Durable {
        _dir: dir,
        store,
        matching,
        queues,
    }
}

/// Go `partitions(count)` / `persistencePartitions(count)`: `count` read and write partitions.
fn partitions(version: i64, count: i32) -> StoredPartitionConfig {
    StoredPartitionConfig {
        version,
        read_partitions: (0..count).map(|id| (id, Vec::new())).collect(),
        write_partitions: (0..count).map(|id| (id, Vec::new())).collect(),
    }
}

// ---------------------------------------------------------------------------
// TestRefreshTaskListPartitionConfig (task_list_manager_test.go:1380)
// ---------------------------------------------------------------------------

// Go cases "success - refresh from request" and "success - ignore older version".
#[tokio::test]
async fn go_refresh_partition_config_from_request_ignores_older_version() {
    let durable = durable();
    let handle = durable
        .queues
        .get(key(CHILD_LIST, 0), 0, now_nanos())
        .await
        .unwrap();
    durable
        .queues
        .refresh_partition_config(key(CHILD_LIST, 0), handle.clone(), Some(partitions(2, 3)))
        .await
        .unwrap();
    let cached = handle.partitions.get().unwrap();
    assert_eq!((cached.version, cached.read_partitions.len(), cached.write_partitions.len()), (2, 3, 3));
    // Go "ignore older version": the cached version 3 with two partitions survives a version 2 push.
    durable
        .queues
        .refresh_partition_config(key(CHILD_LIST, 0), handle.clone(), Some(partitions(3, 2)))
        .await
        .unwrap();
    durable
        .queues
        .refresh_partition_config(key(CHILD_LIST, 0), handle.clone(), Some(partitions(2, 3)))
        .await
        .unwrap();
    let cached = handle.partitions.get().unwrap();
    assert_eq!((cached.version, cached.read_partitions.len(), cached.write_partitions.len()), (3, 2, 2));
}

// Go cases "success - refresh from database" and "failed to refresh from database".
#[tokio::test]
async fn go_refresh_partition_config_from_database_replaces_cache() {
    let durable = durable();
    let list = key(TASK_LIST, 0);
    let root = durable
        .queues
        .get(list.clone(), 0, now_nanos())
        .await
        .unwrap();
    durable
        .queues
        .update_partition_config(list.clone(), root.clone(), partitions(0, 10), now_nanos())
        .await
        .unwrap();
    // The cache is newer than storage, so only the database read can explain the replacement.
    durable
        .queues
        .refresh_partition_config(list.clone(), root.clone(), Some(partitions(99, 2)))
        .await
        .unwrap();
    assert_eq!(root.partitions.get().unwrap().version, 99);
    durable
        .queues
        .refresh_partition_config(list.clone(), root.clone(), None)
        .await
        .unwrap();
    let stored = durable
        .store
        .get_task_list(&list)
        .unwrap()
        .data
        .adaptive_partition_config
        .unwrap();
    let cached = root.partitions.get().unwrap();
    assert_eq!(cached.version, stored.version);
    assert_eq!((cached.read_partitions.len(), cached.write_partitions.len()), (10, 10));
    // A failed storage read leaves the cache untouched. Go injects "some error" through a mock;
    // Rust surfaces a missing row as NotFound.
    let error = durable
        .queues
        .refresh_partition_config(key("tl-missing", 0), root.clone(), None)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
    assert_eq!(root.partitions.get().unwrap().version, cached.version);
}

// ---------------------------------------------------------------------------
// TestRefreshTaskListPartitionConfigConcurrency / TestUpdateTaskListPartitionConfigConcurrency
// ---------------------------------------------------------------------------

// Go: 100 concurrent refreshes at versions 0..99 must end at 99 regardless of arrival order.
#[tokio::test]
async fn go_refresh_partition_config_concurrency_keeps_highest_version() {
    let durable = durable();
    let list = key(CHILD_LIST, 0);
    let handle = durable
        .queues
        .get(list.clone(), 0, now_nanos())
        .await
        .unwrap();
    let mut pushes = Vec::new();
    for version in 0..100i64 {
        let queues = durable.queues.clone();
        let handle = handle.clone();
        let list = list.clone();
        pushes.push(tokio::spawn(async move {
            queues
                .refresh_partition_config(list, handle, Some(partitions(version, version as i32)))
                .await
        }));
    }
    for push in pushes {
        push.await.unwrap().unwrap();
    }
    assert_eq!(handle.partitions.get().unwrap().version, 99);
}

// Go: 100 concurrent updates advance the stored version once each and end at 100. Rust keeps
// partition config on the root task list, so the updates use the root key.
#[tokio::test]
async fn go_update_partition_config_concurrency_advances_once_per_update() {
    let durable = durable();
    let list = key(TASK_LIST, 0);
    let handle = durable
        .queues
        .get(list.clone(), 0, now_nanos())
        .await
        .unwrap();
    let mut updates = Vec::new();
    for count in 2..102 {
        let queues = durable.queues.clone();
        let handle = handle.clone();
        let list = list.clone();
        updates.push(tokio::spawn(async move {
            queues
                .update_partition_config(list, handle, partitions(0, count), now_nanos())
                .await
        }));
    }
    for update in updates {
        update.await.unwrap().unwrap();
    }
    assert_eq!(handle.partitions.get().unwrap().version, 100);
    let stored = durable
        .store
        .get_task_list(&list)
        .unwrap()
        .data
        .adaptive_partition_config
        .unwrap();
    assert_eq!(stored.version, 100);
}

// ---------------------------------------------------------------------------
// TestManagerStart_RootPartition / TestManagerStart_NonRootPartition
// ---------------------------------------------------------------------------

// Go: a root owner starts with the config its lease returned. A fresh registry has no cache, so
// the restarted root must reload its stored config.
#[tokio::test]
async fn go_manager_start_root_adopts_persisted_partition_config() {
    let durable = durable();
    let list = key(TASK_LIST, 0);
    let root = durable
        .queues
        .get(list.clone(), 0, now_nanos())
        .await
        .unwrap();
    durable
        .queues
        .update_partition_config(list.clone(), root, partitions(0, 2), now_nanos())
        .await
        .unwrap();
    let restarted = registry(durable.store.clone());
    let handle = restarted.get(list, 0, now_nanos()).await.unwrap();
    let cached = handle.partitions.get().unwrap();
    assert_eq!(
        (cached.version, cached.read_partitions.len(), cached.write_partitions.len()),
        (1, 2, 2)
    );
}

// Go: a child partition reads the root's stored config, not its own storage.
#[tokio::test]
async fn go_manager_start_non_root_reads_root_partition_config() {
    let durable = durable();
    let root = durable
        .queues
        .get(key(TASK_LIST, 0), 0, now_nanos())
        .await
        .unwrap();
    durable
        .queues
        .update_partition_config(key(TASK_LIST, 0), root, partitions(0, 3), now_nanos())
        .await
        .unwrap();
    let restarted = registry(durable.store.clone());
    let child = restarted
        .get(key(CHILD_LIST, 0), 0, now_nanos())
        .await
        .unwrap();
    let cached = child.partitions.get().unwrap();
    assert_eq!(
        (cached.version, cached.read_partitions.len(), cached.write_partitions.len()),
        (1, 3, 3)
    );
}

#[test]
#[ignore = "parity gap: Go root Start pushes its leased partition config to child owners via MatchingClient.RefreshTaskListPartitionConfig (task_list_manager_test.go:1762); Rust has no start-time push, and the only RefreshTaskListPartitionConfigRequest sender is the update path (matching_registry.rs:1959). Needs two owner processes to observe, so no in-process body is possible"]
fn go_manager_start_root_pushes_config_to_child_partitions() {
    unimplemented!("needs two Matching owner processes; see ignore reason")
}

// ---------------------------------------------------------------------------
// TestDispatchTask (task_list_manager_test.go:1822)
// ---------------------------------------------------------------------------

// Go case "active cluster - disabled StandByTaskCompletion - task sent to MustOffer and no error
// returned". A decision added while a poller waits is delivered, and AddDecisionTask succeeds. The
// MustOffer-error and standby-completion cases are not ported here.
#[tokio::test]
async fn go_dispatch_task_active_offer_without_error_reaches_poller() {
    let durable = durable();
    let matching = durable.matching.clone();
    let poll = tokio::spawn(async move {
        let mut request = Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain_id().to_string(),
            request: Some(p::PollForDecisionTaskRequest {
                task_list: Some(p::TaskList {
                    name: TASK_LIST.into(),
                    kind: 0,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        request.set_timeout(Duration::from_secs(8));
        matching.poll_for_decision_task(request).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut add = Request::new(m::AddDecisionTaskRequest {
        domain_id: domain_id().to_string(),
        workflow_execution: Some(p::WorkflowExecution {
            workflow_id: "workflow".into(),
            run_id: Uuid::from_u128(2).to_string(),
        }),
        task_list: Some(p::TaskList {
            name: TASK_LIST.into(),
            kind: 0,
            ..Default::default()
        }),
        schedule_id: 9,
        ..Default::default()
    });
    add.set_timeout(Duration::from_secs(8));
    durable.matching.add_decision_task(add).await.unwrap();
    let response = poll.await.unwrap().unwrap().into_inner();
    assert!(!response.task_token.is_empty(), "poll returned {response:?}");
}

// ---------------------------------------------------------------------------
// thrift_handler_test.go TestThriftHandler
// ---------------------------------------------------------------------------

// Go routes each tested method to the handler. Every procedure here must reach a handler and
// return a Thrift response or exception, never the unknown-procedure error.
#[tokio::test]
async fn go_thrift_handler_routes_each_tested_method() {
    let matching = Matching::new(AppState::default());
    for procedure in [
        "Meta::health",
        "MatchingService::AddActivityTask",
        "MatchingService::AddDecisionTask",
        "MatchingService::CancelOutstandingPoll",
        "MatchingService::DescribeTaskList",
        "MatchingService::ListTaskListPartitions",
        "MatchingService::PollForActivityTask",
        "MatchingService::PollForDecisionTask",
        "MatchingService::QueryWorkflow",
        "MatchingService::RespondQueryTaskCompleted",
    ] {
        let deadline = Instant::now() + Duration::from_secs(5);
        let routed = tokio::time::timeout(
            Duration::from_secs(10),
            dispatch(&matching, procedure, &Struct::default(), deadline),
        )
        .await
        .unwrap_or_else(|_| panic!("{procedure} did not return"));
        routed.unwrap_or_else(|error| panic!("{procedure} was not routed: {error}"));
    }
}

#[test]
#[ignore = "parity gap: Go TestThriftHandler fails each handler method with InternalServiceError{Message:\"test\"} through a mock Handler and asserts the Thrift exception and zero-value response defaults; Rust dispatch() takes a concrete Matching with no injectable handler, so no public call can produce that failure"]
fn go_thrift_handler_internal_error_maps_to_internal_service_error() {
    unimplemented!("no injectable handler failure in the Rust thrift dispatcher; see ignore reason")
}
