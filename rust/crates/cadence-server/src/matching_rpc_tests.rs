use super::*;
use crate::{matching_history::MatchingHistoryClient, matching_registry::QueueRegistry};
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
};
use cadence_proto::history as h;
use m::matching_api_server::MatchingApi;
use std::{num::NonZeroU32, time::Duration};
use uuid::Uuid;
fn fixture() -> (
    tempfile::TempDir,
    Arc<SqliteTaskStore>,
    Matching,
    TaskListKey,
) {
    fixture_with_history("http://127.0.0.1:1")
}
fn fixture_with_history(
    history_peer: &str,
) -> (
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
        tonic::transport::Endpoint::from_shared(history_peer.to_string())
            .unwrap()
            .connect_lazy(),
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

#[tokio::test]
async fn matching_rpc_durable_describe_tracks_activity_poller_and_queue_status() {
    let (_dir, _store, matching, key) = fixture();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_activity_task(Request::new(m::PollForActivityTaskRequest {
                domain_id: poll_domain,
                poller_id: "activity-poller".into(),
                request: Some(p::PollForActivityTaskRequest {
                    task_list: list(),
                    identity: "sdk-worker".into(),
                    task_list_metadata: Some(p::TaskListMetadata {
                        max_tasks_per_second: Some(42.0),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
    });
    let describe = || {
        matching.describe_task_list(Request::new(m::DescribeTaskListRequest {
            domain_id: domain.clone(),
            request: Some(p::DescribeTaskListRequest {
                task_list: list(),
                task_list_type: 2,
                include_task_list_status: true,
                ..Default::default()
            }),
        }))
    };
    let active = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let info = describe().await.unwrap().into_inner();
            if !info.pollers.is_empty() {
                break info;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(active.pollers.len(), 1);
    assert_eq!(active.pollers[0].identity, "sdk-worker");
    assert_eq!(active.pollers[0].rate_per_second, 42.0);
    let status = active.task_list_status.unwrap();
    assert_eq!(status.read_level, 0);
    assert_eq!(status.ack_level, 0);
    assert!(status.empty);
    assert_eq!(
        status.task_id_block,
        Some(p::TaskIdBlock {
            start_id: 1,
            end_id: 100
        })
    );
    matching
        .cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
            domain_id: domain.clone(),
            task_list: list(),
            task_list_type: 2,
            poller_id: "activity-poller".into(),
        }))
        .await
        .unwrap();
    assert_eq!(
        poll.await.unwrap().unwrap_err().code(),
        tonic::Code::Cancelled
    );
    let ended = describe().await.unwrap().into_inner();
    assert_eq!(ended.pollers.len(), 1);
    let timestamp = |time: &Option<prost_types::Timestamp>| {
        time.as_ref().map(|time| (time.seconds, time.nanos))
    };
    assert!(
        timestamp(&ended.pollers[0].last_access_time)
            >= timestamp(&active.pollers[0].last_access_time)
    );
}

#[tokio::test]
async fn matching_rpc_lists_configured_write_partitions() {
    let (_dir, _store, matching, _key) = fixture();
    let request = || {
        Request::new(m::ListTaskListPartitionsRequest {
            domain: "domain".into(),
            task_list: list(),
        })
    };
    // Default dynamic property is a single write partition: just the root.
    let single = matching
        .list_task_list_partitions(request())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(single.decision_task_list_partitions.len(), 1);
    assert_eq!(single.decision_task_list_partitions[0].key, "queue");
    // Go `getAllPartitions` enumerates the root plus `__cadence|<root>/<i>`
    // children up to the configured `matching.numTasklistWritePartitions`.
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .update(
            false,
            "matching.numTasklistWritePartitions",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    b"3".to_vec(),
                )),
                filters: vec![],
            }],
        );
    let partitioned = matching
        .list_task_list_partitions(request())
        .await
        .unwrap()
        .into_inner();
    let keys: Vec<String> = partitioned
        .decision_task_list_partitions
        .iter()
        .map(|partition| partition.key.clone())
        .collect();
    assert_eq!(
        keys,
        vec![
            "queue".to_string(),
            "/__cadence_sys/queue/1".to_string(),
            "/__cadence_sys/queue/2".to_string()
        ]
    );
    assert_eq!(
        partitioned
            .activity_task_list_partitions
            .iter()
            .map(|partition| partition.key.clone())
            .collect::<Vec<_>>(),
        keys
    );
}

/// Minimal History used by the query test: only `GetMutableState` is
/// overridden, every other RPC keeps the generated `unimplemented` default.
struct FakeHistory {
    state: h::GetMutableStateResponse,
}
#[tonic::async_trait]
impl h::history_api_server::HistoryApi for FakeHistory {
    async fn get_mutable_state(
        &self,
        _request: tonic::Request<h::GetMutableStateRequest>,
    ) -> Result<tonic::Response<h::GetMutableStateResponse>, tonic::Status> {
        Ok(tonic::Response::new(self.state.clone()))
    }
}
async fn start_fake_history(state: h::GetMutableStateResponse) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(h::history_api_server::HistoryApiServer::new(FakeHistory { state }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{address}")
}
/// Matches Go matching's query task metadata: the frontend loads history with
/// `next_event_id`, and a sticky task list plus a supported client decides
/// `sticky_execution_enabled`.
fn mutable_state() -> h::GetMutableStateResponse {
    h::GetMutableStateResponse {
        workflow_execution: execution(),
        workflow_type: Some(p::WorkflowType {
            name: "service-compat".into(),
        }),
        next_event_id: 12,
        previous_started_event_id: Some(5),
        task_list: list(),
        client_impl: "uber-go".into(),
        client_feature_version: "1.0.0".into(),
        current_branch_token: vec![7, 8],
        history_size: 1234,
        ..Default::default()
    }
}

#[tokio::test]
async fn matching_rpc_delivers_query_to_decision_poller() {
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let worker = matching.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "sdk-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: list(),
                    identity: "sdk-worker".into(),
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
    // The query waits on the poller, like Go's `matcher.OfferQuery`.
    let querier = matching.clone();
    let query_domain = domain.clone();
    let query = tokio::spawn(async move {
        querier
            .query_workflow(Request::new(m::QueryWorkflowRequest {
                request: Some(p::QueryWorkflowRequest {
                    domain: "domain".into(),
                    workflow_execution: execution(),
                    query: Some(p::WorkflowQuery {
                        query_type: "ping".into(),
                        query_args: None,
                    }),
                    ..Default::default()
                }),
                domain_id: query_domain,
                task_list: list(),
                forwarded_from: String::new(),
            }))
            .await
    });
    let delivered = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("query should reach the decision poller")
        .unwrap()
        .expect("poll should succeed")
        .into_inner();
    assert_eq!(
        delivered.query.as_ref().map(|q| q.query_type.clone()),
        Some("ping".to_string())
    );
    assert_eq!(
        delivered
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.clone()),
        Some("remote-workflow".to_string())
    );
    // History's mutable state feeds the response so the frontend can load
    // history for the non-sticky query; no sticky task list means no sticky.
    assert_eq!(delivered.next_event_id, 12);
    assert_eq!(delivered.previous_started_event_id, Some(5));
    assert_eq!(delivered.total_history_bytes, 1234);
    assert!(!delivered.sticky_execution_enabled);
    assert_eq!(
        delivered.workflow_type.as_ref().map(|t| t.name.clone()),
        Some("service-compat".to_string())
    );
    let token =
        cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
            &delivered.task_token,
        )
        .expect("poll should carry a query task token");
    assert_eq!(token.task_list, "queue");
    matching
        .respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
            request: Some(p::RespondQueryTaskCompletedRequest {
                task_token: Vec::new(),
                result: Some(p::WorkflowQueryResult {
                    result_type: p::QueryResultType::Answered as i32,
                    answer: Some(p::Payload {
                        data: b"pong".to_vec(),
                    }),
                    error_message: String::new(),
                }),
                worker_version_info: None,
            }),
            domain_id: domain,
            task_list: list(),
            task_id: token.task_id,
        }))
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("query should complete")
        .unwrap()
        .expect("query should succeed")
        .into_inner();
    assert_eq!(
        response.query_result.map(|payload| payload.data),
        Some(b"pong".to_vec())
    );
}

#[tokio::test]
async fn matching_rpc_reports_arrival_qps_by_isolation_group() {
    let (_dir, _store, matching, key) = fixture();
    // Go `matching.qpsTrackerInterval` (default 10s; shortened here) is read
    // when the task list manager is created, so it is updated first.
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .update(
            false,
            "matching.qpsTrackerInterval",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    b"\"50ms\"".to_vec(),
                )),
                filters: vec![],
            }],
        );
    let add = |schedule_id: i64, partition_config: HashMap<String, String>| {
        let matching = matching.clone();
        let domain = key.domain_id.to_string();
        async move {
            matching
                .add_decision_task(Request::new(m::AddDecisionTaskRequest {
                    domain_id: domain,
                    workflow_execution: execution(),
                    task_list: list(),
                    schedule_id,
                    partition_config,
                    ..Default::default()
                }))
                .await
                .unwrap();
        }
    };
    for schedule_id in [1, 2, 3] {
        add(schedule_id, HashMap::new()).await;
    }
    for schedule_id in [4, 5] {
        add(
            schedule_id,
            HashMap::from([(ISOLATION_GROUP_KEY.to_string(), "z".to_string())]),
        )
        .await;
    }
    let queue = matching
        .durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0)
        .await
        .unwrap();
    // Go's tracker only reports when a window closes, so the open window's
    // arrivals are still invisible.
    assert_eq!(queue.qps.qps(), 0.0);
    assert_eq!(queue.qps.group_qps("z"), 0.0);
    tokio::time::sleep(Duration::from_millis(80)).await;
    // Three root arrivals and two isolation-group arrivals were reported, and
    // the group counts toward the root as well.
    assert!(queue.qps.qps() > 0.0);
    assert!(queue.qps.group_qps("z") > 0.0);
    let status = matching
        .describe_task_list(Request::new(m::DescribeTaskListRequest {
            domain_id: key.domain_id.to_string(),
            request: Some(p::DescribeTaskListRequest {
                task_list: list(),
                task_list_type: 1,
                include_task_list_status: true,
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    // DescribeTaskList reports the same arrival rate the tracker holds.
    assert!(
        status
            .task_list_status
            .expect("status requested")
            .new_tasks_per_second
            > 0.0
    );
}

#[tokio::test]
async fn matching_rpc_sticky_task_list_requires_a_recent_poller() {
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    let sticky = || {
        Some(p::TaskList {
            name: "queue:sticky".into(),
            kind: 2,
            ..Default::default()
        })
    };
    // Go `_stickyPollerUnavailableError`: no sticky worker has polled, so the
    // caller can fall back to the non-sticky list instead of stranding a task
    // or a query on the sticky list.
    let error = matching
        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: domain.clone(),
            workflow_execution: execution(),
            task_list: sticky(),
            schedule_id: 3,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("sticky worker is unavailable"));
    let query = matching
        .query_workflow(Request::new(m::QueryWorkflowRequest {
            request: Some(p::QueryWorkflowRequest {
                workflow_execution: execution(),
                ..Default::default()
            }),
            domain_id: domain.clone(),
            task_list: sticky(),
            forwarded_from: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(query.code(), tonic::Code::Unavailable);
    // A waiting sticky poller marks the list available again.
    let worker = matching.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "sticky-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: sticky(),
                    identity: "sticky-worker".into(),
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
    // The delivery worker registers the outstanding poll asynchronously.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match matching
            .add_decision_task(Request::new(m::AddDecisionTaskRequest {
                domain_id: domain.clone(),
                workflow_execution: execution(),
                task_list: sticky(),
                schedule_id: 4,
                ..Default::default()
            }))
            .await
        {
            Ok(_) => break,
            Err(error)
                if error.code() == tonic::Code::Unavailable
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("a waiting sticky poller must make the list available: {error}"),
        }
    }
    poll.abort();
}
