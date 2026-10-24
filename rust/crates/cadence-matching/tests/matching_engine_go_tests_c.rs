//! Go parity ports of test functions 13 through 18 in
//! `go/service/matching/handler/engine_test.go`, in file order:
//! `TestRefreshTaskListPartitionConfig`, `Test_domainChangeCallback`,
//! `Test_registerDomainFailoverCallback` (two subtests),
//! `TestRefreshWorkflowTasks`, `TestSetupExecutorWithEmptyConfig`, and
//! `TestShardDistributorOnboarded`.
//!
//! Rust has no `matchingEngineImpl`. Partition-config refresh runs through the
//! public `MatchingApi` on `Matching`, backed by the real SQLite-durable
//! `QueueRegistry`. Workflow-task refresh runs through the public
//! `MatchingHistoryClient` against a scripted gRPC History peer, which plays
//! the role of Go's `history.MockClient`. Go's gomock managers, executor,
//! membership resolver, and domain-cache callbacks have no public Rust seam,
//! so those cases are `#[ignore]` with the gap named.
//!
//! Go task-list types and kinds are mapped to the proto enums: kind 2 is
//! `TASK_LIST_KIND_STICKY`, and task-list type 2 is
//! `TASK_LIST_TYPE_ACTIVITY`. Go's non-UUID domain and run IDs are kept only
//! where Rust does not parse them. The domain used for partition config must be
//! a UUID, because the handler parses it.
use cadence_api::types::DomainInfo;
use cadence_matching::{
    grpc_matching::Matching, matching_history::MatchingHistoryClient,
    matching_registry::QueueRegistry,
    matching_shard_distributor::{setup_shard_executor, GetShardProcessError, ShardExecutorSetup},
    matching_shard_routing::{shard_distributor_onboarded, task_list_excluded_from_shard_distributor},
};
use cadence_persistence::{
    matching_blob::{BlobEncoding, TaskData},
    matching_sqlite::SqliteTaskStore,
    matching_task_store::AllocatedTask,
};
use cadence_proto::{api as p, history as h, matching as m};
use cadence_service_core::handlers::AppState;
use m::matching_api_server::MatchingApi;
use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

const DOMAIN: &str = "test-domain";
const TASK_LIST_TYPE_ACTIVITY: i32 = 2;
const TASK_LIST_KIND_STICKY: i32 = 2;
const TASK_LIST_KIND_NORMAL: i32 = 0;

fn domain_id() -> Uuid {
    Uuid::from_u128(1)
}

fn run_id() -> Uuid {
    Uuid::from_u128(0x7e57)
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

/// Go `newMockManagerWithTaskListID` and the engine's durable backend: a
/// SQLite task store behind a `QueueRegistry`, wired into `Matching`. The
/// history peer is unreachable and never dialed by these RPCs.
fn durable_matching(state: AppState) -> (tempfile::TempDir, Matching) {
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
    let matching = Matching::with_durable_queues(state, queues, Duration::from_secs(10));
    (dir, matching)
}

/// Go `mustNewIdentifier(t, "test-domain-id", ...)` with the domain added to
/// the domain cache, so the handler's domain lookup succeeds.
fn matching_with_domain() -> (tempfile::TempDir, Matching) {
    let state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    durable_matching(state)
}

fn partition_config(version: i64) -> p::TaskListPartitionConfig {
    p::TaskListPartitionConfig {
        version,
        read_partitions: HashMap::from([(0, p::TaskListPartition::default())]),
        write_partitions: HashMap::from([(0, p::TaskListPartition::default())]),
        ..Default::default()
    }
}

fn refresh_request(
    name: &str,
    kind: i32,
    partition_config: Option<p::TaskListPartitionConfig>,
) -> Request<m::RefreshTaskListPartitionConfigRequest> {
    Request::new(m::RefreshTaskListPartitionConfigRequest {
        domain_id: domain_id().to_string(),
        task_list: Some(p::TaskList {
            name: name.into(),
            kind,
            ..Default::default()
        }),
        task_list_type: TASK_LIST_TYPE_ACTIVITY,
        partition_config,
    })
}

fn assert_invalid_argument(err: Status, message: &str) {
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), message);
}

// ---------------------------------------------------------------------------
// TestRefreshTaskListPartitionConfig
// ---------------------------------------------------------------------------

#[tokio::test]
async fn refresh_task_list_partition_config_success_accepts_partitioned_normal_list() {
    // Go's gomock expectation that RefreshTaskListPartitionConfig receives the
    // pushed config is not observable through the public API. This checks
    // the RPC succeeds on the same request shape.
    let (_dir, matching) = matching_with_domain();
    matching
        .refresh_task_list_partition_config(refresh_request(
            "/__cadence_sys/test-tasklist/1",
            TASK_LIST_KIND_NORMAL,
            Some(partition_config(1)),
        ))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "parity gap: Go injects an error through tasklist.MockManager.RefreshTaskListPartitionConfig; Rust refreshes the SQLite-durable queue registry directly, and no public seam injects a failure into that call"]
async fn refresh_task_list_partition_config_manager_error_is_propagated() {
    unimplemented!("no public failure-injection seam for the durable partition refresh");
}

#[tokio::test]
async fn refresh_task_list_partition_config_invalid_tasklist_name_is_invalid_argument() {
    let (_dir, matching) = matching_with_domain();
    let err = matching
        .refresh_task_list_partition_config(refresh_request(
            "/__cadence_sys/test-tasklist",
            TASK_LIST_KIND_NORMAL,
            Some(partition_config(1)),
        ))
        .await
        .unwrap_err();
    assert_invalid_argument(
        err,
        "invalid partitioned task list name /__cadence_sys/test-tasklist",
    );
}

#[tokio::test]
async fn refresh_task_list_partition_config_sticky_kind_is_invalid_argument() {
    let (_dir, matching) = matching_with_domain();
    let err = matching
        .refresh_task_list_partition_config(refresh_request(
            "test-tasklist",
            TASK_LIST_KIND_STICKY,
            None,
        ))
        .await
        .unwrap_err();
    assert_invalid_argument(
        err,
        "Only normal tasklist's partition config can be updated.",
    );
}

#[tokio::test]
async fn refresh_task_list_partition_config_root_with_partition_config_is_invalid_argument() {
    let (_dir, matching) = matching_with_domain();
    let err = matching
        .refresh_task_list_partition_config(refresh_request(
            "test-tasklist",
            TASK_LIST_KIND_NORMAL,
            Some(partition_config(1)),
        ))
        .await
        .unwrap_err();
    assert_invalid_argument(err, "PartitionConfig must be nil for root partition.");
}

// ---------------------------------------------------------------------------
// Test_domainChangeCallback
// ---------------------------------------------------------------------------

#[test]
#[ignore = "parity gap: Go domainChangeCallback is a matchingEngineImpl method that updates engine.failoverNotificationVersion and calls ReleaseBlockedPollers on each eligible mock manager; Rust folds the same logic into the private closure in matching_runtime.rs start_domain_cache, which exposes neither the version nor a released-poller count"]
fn domain_change_callback_updates_failover_version_and_releases_eligible_pollers() {
    unimplemented!("no public seam for the private domain-change closure in matching_runtime.rs");
}

// ---------------------------------------------------------------------------
// Test_registerDomainFailoverCallback
// ---------------------------------------------------------------------------

#[test]
#[ignore = "parity gap: Go registerDomainFailoverCallback is a private engine method that registers a catch-up function with a domainCache mock; Rust registers the equivalent catch-up inside the private start_domain_cache in matching_runtime.rs, and the registered callback is not reachable from tests/"]
fn register_domain_failover_callback_catch_up_without_updates_keeps_version() {
    unimplemented!("no public seam for the registered catch-up in matching_runtime.rs");
}

#[test]
#[ignore = "parity gap: Go registerDomainFailoverCallback is a private engine method that registers a catch-up function with a domainCache mock; Rust registers the equivalent catch-up inside the private start_domain_cache in matching_runtime.rs, and the registered callback is not reachable from tests/"]
fn register_domain_failover_callback_catch_up_advances_version_to_highest() {
    unimplemented!("no public seam for the registered catch-up in matching_runtime.rs");
}

// ---------------------------------------------------------------------------
// TestRefreshWorkflowTasks
// ---------------------------------------------------------------------------

/// Go `history.MockClient.RefreshWorkflowTasks`: records each request and
/// answers with the scripted status, or success when none is set.
struct ScriptedHistory {
    status: Option<Status>,
    seen: Arc<Mutex<Vec<h::RefreshWorkflowTasksRequest>>>,
}

#[tonic::async_trait]
impl h::history_api_server::HistoryApi for ScriptedHistory {
    async fn refresh_workflow_tasks(
        &self,
        request: Request<h::RefreshWorkflowTasksRequest>,
    ) -> Result<Response<h::RefreshWorkflowTasksResponse>, Status> {
        self.seen.lock().unwrap().push(request.into_inner());
        match &self.status {
            Some(status) => Err(status.clone()),
            None => Ok(Response::new(h::RefreshWorkflowTasksResponse {})),
        }
    }
}

async fn refresh_history_client(
    status: Option<Status>,
) -> (
    MatchingHistoryClient,
    Arc<Mutex<Vec<h::RefreshWorkflowTasksRequest>>>,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = ScriptedHistory {
        status,
        seen: seen.clone(),
    };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(h::history_api_server::HistoryApiServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (
        MatchingHistoryClient::new(channel, Duration::from_secs(1), 4 * 1024 * 1024),
        seen,
    )
}

/// Go `"test-workflow-id"` and `"test-run-id"`. Rust persists run IDs as
/// 16-byte UUIDs and renders them back as strings, so the run is a UUID.
fn allocated_task() -> AllocatedTask {
    AllocatedTask {
        task_id: 1,
        data: TaskData {
            workflow_id: "test-workflow-id".into(),
            run_id: Some(run_id().as_bytes().to_vec()),
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn refresh_workflow_tasks_success_sends_domain_and_execution() {
    let (client, seen) = refresh_history_client(None).await;
    client
        .refresh_workflow_tasks("test-domain-id", &allocated_task())
        .await
        .unwrap();
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].domain_id, "test-domain-id");
    let execution = requests[0].workflow_execution.as_ref().unwrap();
    assert_eq!(execution.workflow_id, "test-workflow-id");
    assert_eq!(execution.run_id, run_id().to_string());
}

#[tokio::test]
#[ignore = "parity gap: Go returns a typed EntityNotExistsError from the history mock and expects nil; Rust recognizes it only from a wire detail, and the crate-private grpc::detailed_status helper that builds that detail cannot be called from tests/"]
async fn refresh_workflow_tasks_entity_not_exists_returns_ok() {
    unimplemented!("needs the typed EntityNotExistsError wire detail");
}

#[tokio::test]
async fn refresh_workflow_tasks_internal_status_is_propagated() {
    // Partial: Go's typed InternalServiceError detail cannot be built from
    // tests/. This checks the untyped INTERNAL status is propagated unchanged.
    let (client, _) = refresh_history_client(Some(Status::internal("internal error"))).await;
    let err = client
        .refresh_workflow_tasks("test-domain-id", &allocated_task())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert_eq!(err.message(), "internal error");
}

#[tokio::test]
async fn refresh_workflow_tasks_generic_error_is_propagated() {
    let (client, _) = refresh_history_client(Some(Status::unknown("some error"))).await;
    let err = client
        .refresh_workflow_tasks("test-domain-id", &allocated_task())
        .await
        .unwrap_err();
    assert_eq!(err.message(), "some error");
}

#[tokio::test]
#[ignore = "parity gap: Go returns a typed WorkflowExecutionAlreadyCompletedError from the history mock; the typed wire detail needs the crate-private grpc::detailed_status helper, which tests/ cannot call"]
async fn refresh_workflow_tasks_already_completed_is_propagated() {
    unimplemented!("needs the typed WorkflowExecutionAlreadyCompletedError wire detail");
}

#[tokio::test]
async fn refresh_workflow_tasks_canceled_context_is_propagated() {
    // Go: the History error (context.Canceled here) is returned unchanged. Cancelled is a
    // retryable code, so this retries until the policy ends, like the internal-status case.
    let (client, _) = refresh_history_client(Some(Status::cancelled("context canceled"))).await;
    let err = client
        .refresh_workflow_tasks("test-domain-id", &allocated_task())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Cancelled);
    assert_eq!(err.message(), "context canceled");

    // Go: an already-cancelled caller context makes the engine return context.Canceled.
    // Rust returns the same status before the History call is made, so no request is seen.
    let (client, seen) = refresh_history_client(None).await;
    let err = client
        .refresh_workflow_tasks_until(
            "test-domain-id",
            &allocated_task(),
            None,
            std::future::ready(()),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Cancelled);
    assert_eq!(err.message(), "context canceled");
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn refresh_workflow_tasks_empty_domain_id_is_forwarded() {
    let (client, seen) = refresh_history_client(None).await;
    client
        .refresh_workflow_tasks("", &allocated_task())
        .await
        .unwrap();
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].domain_id, "");
}

#[tokio::test]
async fn refresh_workflow_tasks_nil_execution_is_forwarded() {
    // Go's engine call sites run only when task.Event is set, so task.WorkflowExecution() is
    // never nil there; it is {Event.WorkflowID, Event.RunID}, with an empty RunID stored as
    // an absent run. Rust therefore forwards an absent run as "" and an empty workflow id as is.
    let (client, seen) = refresh_history_client(None).await;
    let mut no_run = allocated_task();
    no_run.data.run_id = None;
    client
        .refresh_workflow_tasks("test-domain-id", &no_run)
        .await
        .unwrap();
    let mut empty = allocated_task();
    empty.data.workflow_id = String::new();
    empty.data.run_id = None;
    client
        .refresh_workflow_tasks("", &empty)
        .await
        .unwrap();
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let first = requests[0].workflow_execution.as_ref().unwrap();
    assert_eq!(first.workflow_id, "test-workflow-id");
    assert_eq!(first.run_id, "");
    let second = requests[1].workflow_execution.as_ref().unwrap();
    assert_eq!(second.workflow_id, "");
    assert_eq!(second.run_id, "");
    assert_eq!(requests[1].domain_id, "");
}

// ---------------------------------------------------------------------------
// TestSetupExecutorWithEmptyConfig
// ---------------------------------------------------------------------------

#[test]
fn setup_executor_with_empty_config_installs_noop_executor() {
    let config = cadence_service_core::config::ShardDistributorMatchingConfig {
        namespaces: vec![],
        peer_ttl_nanos: 0,
    };
    // Go: setupExecutor with no namespaces succeeds and installs the no-op executor.
    let ShardExecutorSetup::Noop(executor) = setup_shard_executor(&config).unwrap() else {
        panic!("empty namespaces must install the no-op executor");
    };
    // Go: the no-op executor owns no shards, so every lookup is ErrShardProcessNotFound.
    let err = executor.get_shard_process("any-shard").unwrap_err();
    assert!(matches!(err, GetShardProcessError::NotFound(ref key) if key == "any-shard"));
    assert!(!executor.has_local_shard_process("any-shard"));
}

// ---------------------------------------------------------------------------
// TestShardDistributorOnboarded
// ---------------------------------------------------------------------------

#[test]
fn shard_distributor_onboarded_zero_excludes_every_task_list() {
    // Go "offboarded at zero". Rust gates each task list by fingerprint bucket
    // (`fingerprint % 100 >= percentage`), and every bucket is >= 0.
    for name in ["test-tasklist", "other-tasklist", "/__cadence_sys/test-tasklist/1"] {
        assert!(
            task_list_excluded_from_shard_distributor(name, 0, false),
            "{name} must be excluded at 0 percent"
        );
    }
}

#[test]
fn shard_distributor_onboarded_full_excludes_no_task_list() {
    // Go "onboarded at full". Buckets are 0..=99, all below 100.
    for name in ["test-tasklist", "other-tasklist", "/__cadence_sys/test-tasklist/1"] {
        assert!(
            !task_list_excluded_from_shard_distributor(name, 100, false),
            "{name} must not be excluded at 100 percent"
        );
    }
}

#[test]
fn shard_distributor_onboarded_partial_percentage_is_engine_wide() {
    // Go TestShardDistributorOnboarded: the engine-wide gate is percentage > 0.
    assert!(!shard_distributor_onboarded(0));
    assert!(shard_distributor_onboarded(1));
    assert!(shard_distributor_onboarded(100));
    // The engine-wide gate reads no task list name. Per-task-list admission at
    // percentage 1 is the separate fingerprint rule in
    // `task_list_excluded_from_shard_distributor`, covered by its own tests.
}
