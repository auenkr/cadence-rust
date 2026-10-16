use super::*;
use crate::{matching_history::MatchingHistoryClient, matching_registry::QueueRegistry};
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
};
use m::matching_api_server::MatchingApi;
use std::{num::NonZeroU32, time::Duration};
use uuid::Uuid;
fn fixture() -> (
    tempfile::TempDir,
    Arc<SqliteTaskStore>,
    Matching,
    TaskListKey,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("queue.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let state = crate::handlers::AppState::default();
    let domain = Uuid::from_u128(1);
    state
        .domains
        .create(
            cadence_api::types::DomainInfo {
                name: "domain".into(),
                uuid: domain.to_string(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    let queues = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap());
    let matching = Matching::with_durable_queues(state, queues, Duration::from_secs(10));
    (
        dir,
        store,
        matching,
        TaskListKey {
            domain_id: domain,
            name: "queue".into(),
            task_type: 0,
        },
    )
}
fn execution() -> Option<p::WorkflowExecution> {
    Some(p::WorkflowExecution {
        workflow_id: "remote-workflow".into(),
        run_id: Uuid::from_u128(2).to_string(),
    })
}
fn list() -> Option<p::TaskList> {
    Some(p::TaskList {
        name: "queue".into(),
        kind: 1,
        ..Default::default()
    })
}
#[tokio::test]
async fn matching_rpc_durable_add_does_not_require_local_workflow_state() {
    let (_dir, store, matching, key) = fixture();
    matching
        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: key.domain_id.to_string(),
            workflow_execution: execution(),
            task_list: list(),
            schedule_id: 42,
            schedule_to_start_timeout: Some(prost_types::Duration {
                seconds: 3,
                nanos: 0,
            }),
            partition_config: HashMap::from([("zone".into(), "a".into())]),
            ..Default::default()
        }))
        .await
        .unwrap();
    let tasks = store.get_tasks(&key, 0, 100, 10).unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].data.workflow_id, "remote-workflow");
    assert_eq!(tasks[0].data.schedule_id, 42);
    assert_eq!(
        tasks[0].data.expiry_nanos - tasks[0].data.created_nanos,
        3_000_000_000
    );
    assert_eq!(
        tasks[0].data.partition_config.as_ref().unwrap()["zone"],
        "a"
    );
    assert!(matching
        .frontend
        .state
        .executions
        .task_lists("domain")
        .0
        .is_empty());
}
#[tokio::test]
async fn matching_rpc_durable_activity_preserves_source_domain_and_type() {
    let (_dir, store, matching, mut target) = fixture();
    target.task_type = 1;
    let source = Uuid::from_u128(3);
    matching
        .frontend
        .state
        .domains
        .create(
            cadence_api::types::DomainInfo {
                name: "source".into(),
                uuid: source.to_string(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    matching
        .add_activity_task(Request::new(m::AddActivityTaskRequest {
            domain_id: target.domain_id.to_string(),
            source_domain_id: source.to_string(),
            workflow_execution: execution(),
            task_list: list(),
            schedule_id: 7,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(store.get_tasks(&target, 0, 100, 10).unwrap().is_empty());
    target.domain_id = source;
    let tasks = store.get_tasks(&target, 0, 100, 10).unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].data.schedule_id, 7);
    assert_eq!(tasks[0].data.partition_config, None);
}
#[tokio::test]
async fn matching_rpc_durable_cancel_unregisters_waiting_poller() {
    let (_dir, _store, matching, key) = fixture();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "poller".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: list(),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !matching.polls.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    matching
        .cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
            domain_id: domain,
            task_list: list(),
            task_list_type: 1,
            poller_id: "poller".into(),
        }))
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), poll)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Cancelled);
    assert!(matching.polls.lock().unwrap().is_empty());
}
