//! Durable concurrency and range-fencing counterparts for
//! `go/service/matching/handler/engine_integration_test.go`.
//!
//! The public Matching API is used for concurrent adds; task rows are checked
//! in the same SQLite store used by Matching. The range-stealing test exercises
//! the SQLite lease and write boundary that fences stale engine owners.
use cadence_api::types::DomainInfo;
use cadence_matching::{
    grpc_matching::Matching, matching_history::MatchingHistoryClient,
    matching_registry::QueueRegistry,
};
use cadence_persistence::{
    matching_blob::{BlobEncoding, TaskData},
    matching_sqlite::{SqliteTaskStore, TaskListKey},
    matching_task_store::AllocatedTask,
};
use cadence_proto::{api as p, history as h, matching as m};
use cadence_service_core::handlers::AppState;
use m::matching_api_server::MatchingApi;
use std::{num::NonZeroU32, sync::Arc, time::Duration};
use tonic::{Request, Response, Status};
use uuid::Uuid;

const DOMAIN_ID: Uuid = Uuid::from_u128(1);

fn fixture() -> (tempfile::TempDir, Arc<SqliteTaskStore>, Arc<Matching>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("matching.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    let queues = QueueRegistry::new(store.clone(), history, NonZeroU32::new(5).unwrap());
    let state = AppState::default();
    state
        .domains
        .create(
            DomainInfo {
                name: "concurrent-test".into(),
                uuid: DOMAIN_ID.to_string(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    let matching = Arc::new(Matching::with_durable_queues(
        state,
        queues,
        Duration::from_millis(50),
    ));
    (dir, store, matching)
}

/// Scripted History peer used by real Matching polls. It echoes the schedule ID
/// so every delivered activity can be checked for loss or duplicate delivery.
struct StartingHistory;

#[tonic::async_trait]
impl h::history_api_server::HistoryApi for StartingHistory {
    async fn record_decision_task_started(
        &self,
        request: Request<h::RecordDecisionTaskStartedRequest>,
    ) -> Result<Response<h::RecordDecisionTaskStartedResponse>, Status> {
        Ok(Response::new(h::RecordDecisionTaskStartedResponse {
            scheduled_event_id: request.get_ref().schedule_id,
            started_event_id: request.get_ref().schedule_id + 1,
            ..Default::default()
        }))
    }
    async fn record_activity_task_started(
        &self,
        request: Request<h::RecordActivityTaskStartedRequest>,
    ) -> Result<Response<h::RecordActivityTaskStartedResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(h::RecordActivityTaskStartedResponse {
            scheduled_event: Some(p::HistoryEvent {
                attributes: Some(
                    p::history_event::Attributes::ActivityTaskScheduledEventAttributes(
                        p::ActivityTaskScheduledEventAttributes {
                            activity_id: request.schedule_id.to_string(),
                            ..Default::default()
                        },
                    ),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

async fn start_history() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(h::history_api_server::HistoryApiServer::new(
                StartingHistory,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (format!("http://{address}"), server)
}

fn fixture_with_store(
    store: Arc<SqliteTaskStore>,
    history: &str,
    isolated: bool,
) -> (Arc<Matching>, QueueRegistry) {
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_shared(history.to_owned())
            .unwrap()
            .connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    let queues = QueueRegistry::new(store, history, NonZeroU32::new(5).unwrap());
    let state = AppState::default();
    if isolated {
        use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
        let mut config = state.admin.dynamic_config.lock().unwrap();
        for (key, value) in [
            ("system.enableTasklistIsolation", "true"),
            ("system.allIsolationGroups", "[\"a\",\"b\"]"),
        ] {
            config.update(
                false,
                key,
                vec![DynamicConfigValue {
                    value: Some(DataBlob::new(EncodingType::JSON, value.as_bytes().to_vec())),
                    filters: vec![],
                }],
            );
        }
    }
    state
        .domains
        .create(
            DomainInfo {
                name: "concurrent-test".into(),
                uuid: DOMAIN_ID.to_string(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    (
        Arc::new(Matching::with_durable_queues(
            state,
            queues.clone(),
            Duration::from_millis(50),
        )),
        queues,
    )
}

async fn add_task(
    matching: &Matching,
    task_list: &str,
    task_type: i32,
    schedule_id: i64,
    isolated: bool,
) {
    let execution = Some(p::WorkflowExecution {
        workflow_id: format!("wf-{schedule_id}"),
        run_id: Uuid::from_u128(schedule_id as u128).to_string(),
    });
    let partition_config = if isolated {
        std::collections::HashMap::from([(
            "isolation-group".into(),
            if schedule_id % 2 == 0 { "a" } else { "b" }.into(),
        )])
    } else {
        Default::default()
    };
    if task_type == 1 {
        matching
            .add_activity_task(Request::new(m::AddActivityTaskRequest {
                domain_id: DOMAIN_ID.to_string(),
                source_domain_id: DOMAIN_ID.to_string(),
                workflow_execution: execution,
                task_list: Some(list(task_list)),
                schedule_id,
                partition_config,
                ..Default::default()
            }))
            .await
            .unwrap();
    } else {
        matching
            .add_decision_task(Request::new(m::AddDecisionTaskRequest {
                domain_id: DOMAIN_ID.to_string(),
                workflow_execution: execution,
                task_list: Some(list(task_list)),
                schedule_id,
                partition_config,
                ..Default::default()
            }))
            .await
            .unwrap();
    }
}

async fn poll_task(
    matching: &Matching,
    task_list: &str,
    task_type: i32,
    worker: usize,
    isolated: bool,
) -> Option<i64> {
    let group = if isolated {
        if worker.is_multiple_of(2) {
            "a"
        } else {
            "b"
        }
    } else {
        ""
    };
    if task_type == 1 {
        let mut request = Request::new(m::PollForActivityTaskRequest {
            domain_id: DOMAIN_ID.to_string(),
            isolation_group: group.into(),
            request: Some(p::PollForActivityTaskRequest {
                task_list: Some(list(task_list)),
                ..Default::default()
            }),
            ..Default::default()
        });
        request.set_timeout(Duration::from_secs(20));
        let response = matching
            .poll_for_activity_task(request)
            .await
            .unwrap()
            .into_inner();
        (!response.task_token.is_empty()).then(|| response.activity_id.parse::<i64>().unwrap())
    } else {
        let mut request = Request::new(m::PollForDecisionTaskRequest {
            domain_id: DOMAIN_ID.to_string(),
            isolation_group: group.into(),
            request: Some(p::PollForDecisionTaskRequest {
                task_list: Some(list(task_list)),
                ..Default::default()
            }),
            ..Default::default()
        });
        request.set_timeout(Duration::from_secs(20));
        let response = matching
            .poll_for_decision_task(request)
            .await
            .unwrap()
            .into_inner();
        (!response.task_token.is_empty()).then_some(response.started_event_id - 1)
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn list(name: &str) -> p::TaskList {
    p::TaskList {
        name: name.into(),
        kind: 1,
        ..Default::default()
    }
}

/// Go's activity/decision concurrent-add scenarios use 20 writers × 100
/// requests. Concurrent producers must persist every unique schedule ID.
async fn concurrent_adds(task_type: i32) {
    let (_dir, store, matching) = fixture();
    let task_list = format!("concurrent-{task_type}");
    let mut writers = Vec::new();
    for writer in 0..20_i64 {
        let matching = matching.clone();
        let task_list = task_list.clone();
        writers.push(tokio::spawn(async move {
            for item in 0..100_i64 {
                let schedule_id = writer * 100 + item + 1;
                let execution = Some(p::WorkflowExecution {
                    workflow_id: format!("workflow-{schedule_id}"),
                    run_id: Uuid::from_u128(schedule_id as u128).to_string(),
                });
                if task_type == 1 {
                    matching
                        .add_activity_task(Request::new(m::AddActivityTaskRequest {
                            domain_id: DOMAIN_ID.to_string(),
                            source_domain_id: DOMAIN_ID.to_string(),
                            workflow_execution: execution,
                            task_list: Some(list(&task_list)),
                            schedule_id,
                            ..Default::default()
                        }))
                        .await
                        .unwrap();
                } else {
                    matching
                        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
                            domain_id: DOMAIN_ID.to_string(),
                            workflow_execution: execution,
                            task_list: Some(list(&task_list)),
                            schedule_id,
                            ..Default::default()
                        }))
                        .await
                        .unwrap();
                }
            }
        }));
    }
    for writer in writers {
        writer.await.unwrap();
    }

    let key = TaskListKey {
        domain_id: DOMAIN_ID,
        name: task_list,
        task_type,
    };
    let tasks = store.get_tasks(&key, 0, i64::MAX, 2_000).unwrap();
    assert_eq!(
        tasks.len(),
        2_000,
        "concurrent adds were lost or duplicated"
    );
    let mut workflow_ids: Vec<_> = tasks
        .iter()
        .map(|task| task.data.workflow_id.as_str())
        .collect();
    workflow_ids.sort_unstable();
    workflow_ids.dedup();
    assert_eq!(
        workflow_ids.len(),
        2_000,
        "concurrent add identity repeated"
    );
}

#[tokio::test]
async fn go_concurrent_add_and_poll_activity_producer_half_persists_all_tasks() {
    concurrent_adds(1).await;
}

#[tokio::test]
async fn go_concurrent_add_and_poll_decision_producer_half_persists_all_tasks() {
    concurrent_adds(0).await;
}

/// Unlike the producer-only checkpoint above, this drives concurrent public
/// AddActivityTask and PollForActivityTask RPCs through a reachable History peer.
async fn concurrent_delivery(task_type: i32, isolated: bool) {
    const WORKERS: usize = 4;
    const EACH: i64 = 25;
    let (history, history_server) = start_history().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("delivery.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let (matching, registry) = fixture_with_store(store.clone(), &history, isolated);
    let task_list = "concurrent-delivery";
    let mut polls = Vec::new();
    for worker in 0..WORKERS {
        let matching = matching.clone();
        polls.push(tokio::spawn(async move {
            let mut received = Vec::new();
            while received.len() < EACH as usize {
                if let Some(id) = poll_task(&matching, task_list, task_type, worker, isolated).await
                {
                    received.push(id);
                }
            }
            received
        }));
    }
    let mut writers = Vec::new();
    for writer in 0..WORKERS {
        let matching = matching.clone();
        writers.push(tokio::spawn(async move {
            for item in 0..EACH {
                let schedule_id = (writer as i64) * EACH + item + 1;
                add_task(&matching, task_list, task_type, schedule_id, isolated).await;
            }
        }));
    }
    for writer in writers {
        writer.await.unwrap();
    }
    let mut delivered = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        for poll in polls {
            delivered.extend(poll.await.unwrap());
        }
    })
    .await
    .expect("concurrent delivery did not finish");
    delivered.sort_unstable();
    assert_eq!(
        delivered,
        (1..=(WORKERS as i64 * EACH)).collect::<Vec<_>>(),
        "every distinct task must be delivered once"
    );
    registry.shutdown(now()).await.unwrap();
    let key = TaskListKey {
        domain_id: DOMAIN_ID,
        name: task_list.into(),
        task_type,
    };
    assert!(
        store.get_tasks(&key, 0, i64::MAX, 200).unwrap().is_empty(),
        "shutdown must flush task acknowledgements and GC"
    );
    history_server.abort();
    assert!(history_server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn go_concurrent_add_and_poll_delivers_every_activity_once() {
    concurrent_delivery(1, false).await;
}

#[tokio::test]
async fn go_concurrent_add_and_poll_delivers_every_decision_once() {
    concurrent_delivery(0, false).await;
}

#[tokio::test]
async fn go_concurrent_add_and_poll_activity_isolation() {
    concurrent_delivery(1, true).await;
}

#[tokio::test]
async fn go_concurrent_add_and_poll_decision_isolation() {
    concurrent_delivery(0, true).await;
}

/// Two independent Matching handlers share durable persistence. Engine two
/// advances the lease, the old owner's next public add is rejected, and the
/// replacement poller still receives all work created by both owners.
#[tokio::test]
async fn go_two_live_matching_owners_steal_lease_and_deliver_all_work() {
    let (history, history_server) = start_history().await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("live-owners.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let (old, old_registry) = fixture_with_store(store.clone(), &history, false);
    let (new, new_registry) = fixture_with_store(store.clone(), &history, false);
    let task_list = "live-owner-steal";
    for (owner_index, matching) in [&old, &new].into_iter().enumerate() {
        for schedule_id in 1..=10_i64 {
            let schedule_id = schedule_id + owner_index as i64 * 10;
            matching
                .add_activity_task(Request::new(m::AddActivityTaskRequest {
                    domain_id: DOMAIN_ID.to_string(),
                    source_domain_id: DOMAIN_ID.to_string(),
                    workflow_execution: Some(p::WorkflowExecution {
                        workflow_id: format!("wf-{schedule_id}"),
                        run_id: Uuid::from_u128(schedule_id as u128).to_string(),
                    }),
                    task_list: Some(list(task_list)),
                    schedule_id,
                    ..Default::default()
                }))
                .await
                .unwrap();
        }
    }
    let stale = old
        .add_activity_task(Request::new(m::AddActivityTaskRequest {
            domain_id: DOMAIN_ID.to_string(),
            source_domain_id: DOMAIN_ID.to_string(),
            workflow_execution: Some(p::WorkflowExecution {
                workflow_id: "stale".into(),
                run_id: Uuid::from_u128(99).to_string(),
            }),
            task_list: Some(list(task_list)),
            schedule_id: 99,
            ..Default::default()
        }))
        .await;
    assert!(
        stale.is_err(),
        "the former live owner must be fenced after the replacement takes the lease"
    );
    let mut delivered = Vec::new();
    for _ in 0..20 {
        let mut request = Request::new(m::PollForActivityTaskRequest {
            domain_id: DOMAIN_ID.to_string(),
            request: Some(p::PollForActivityTaskRequest {
                task_list: Some(list(task_list)),
                ..Default::default()
            }),
            ..Default::default()
        });
        request.set_timeout(Duration::from_secs(20));
        let response = new
            .poll_for_activity_task(request)
            .await
            .unwrap()
            .into_inner();
        delivered.push(response.activity_id.parse::<i64>().unwrap());
    }
    delivered.sort_unstable();
    assert_eq!(delivered, (1..=20).collect::<Vec<_>>());
    old_registry.shutdown(now()).await.unwrap();
    new_registry.shutdown(now()).await.unwrap();
    let key = TaskListKey {
        domain_id: DOMAIN_ID,
        name: task_list.into(),
        task_type: 1,
    };
    assert!(store.get_tasks(&key, 0, i64::MAX, 100).unwrap().is_empty());
    history_server.abort();
    assert!(history_server.await.unwrap_err().is_cancelled());
}

/// Go's two-engine range-stealing scenario relies on an old owner's writes
/// being rejected after the replacement owner advances the durable range.
#[test]
fn go_two_engine_range_steal_fences_old_owner_and_replacement_keeps_working() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        SqliteTaskStore::create(dir.path().join("matching.db"), BlobEncoding::ThriftRW).unwrap();
    let key = TaskListKey {
        domain_id: DOMAIN_ID,
        name: "range-steal".into(),
        task_type: 1,
    };
    let old_owner = store.lease(&key, 0, 0, 1).unwrap();
    let old_task = AllocatedTask {
        task_id: 1,
        data: TaskData {
            workflow_id: "old-owner-task".into(),
            run_id: Some(Uuid::from_u128(1).as_bytes().to_vec()),
            ..Default::default()
        },
    };
    store
        .create_tasks(&key, old_owner.range_id, &[old_task])
        .unwrap();

    let new_owner = store.lease(&key, 0, 0, 2).unwrap();
    assert!(new_owner.range_id > old_owner.range_id);
    let stale_task = AllocatedTask {
        task_id: 2,
        data: TaskData {
            workflow_id: "stale-owner-task".into(),
            run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
            ..Default::default()
        },
    };
    assert!(store
        .create_tasks(&key, old_owner.range_id, &[stale_task])
        .is_err());

    let replacement_task = AllocatedTask {
        task_id: 3,
        data: TaskData {
            workflow_id: "new-owner-task".into(),
            run_id: Some(Uuid::from_u128(3).as_bytes().to_vec()),
            ..Default::default()
        },
    };
    store
        .create_tasks(&key, new_owner.range_id, &[replacement_task])
        .unwrap();
    let tasks = store.get_tasks(&key, 0, i64::MAX, 10).unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].data.workflow_id, "old-owner-task");
    assert_eq!(tasks[1].data.workflow_id, "new-owner-task");
}
