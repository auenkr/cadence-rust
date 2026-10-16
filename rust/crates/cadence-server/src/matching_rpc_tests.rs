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
async fn matching_rpc_isolation_toggle_returns_empty_poll_and_replaces_owner() {
    use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
    let (_dir, _store, matching, key) = fixture();
    for task_type in [0, 1] {
        let original = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), task_type).await.unwrap();
        matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "system.enableTasklistIsolation", vec![DynamicConfigValue {
            value: Some(DataBlob::new(EncodingType::JSON, if task_type == 0 { b"true".to_vec() } else { b"false".to_vec() })), filters: vec![],
        }]);
        if task_type == 0 {
            let response = matching.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
                domain_id: key.domain_id.to_string(), request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
            })).await.unwrap().into_inner();
            assert!(response.task_token.is_empty());
        } else {
            let response = matching.poll_for_activity_task(Request::new(m::PollForActivityTaskRequest {
                domain_id: key.domain_id.to_string(), request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
            })).await.unwrap().into_inner();
            assert!(response.task_token.is_empty());
        }
        assert!(original.queue.is_stopped());
        let replacement = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), task_type).await.unwrap();
        assert!(!Arc::ptr_eq(&original.queue, &replacement.queue));
        assert!(!replacement.stop_if_isolation_changed());
    }
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_isolation_toggle_stops_owner_before_admitting_add() {
    use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
    let (_dir, store, matching, key) = fixture();
    let first = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    for enabled in [true, false] {
        matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "system.enableTasklistIsolation", vec![DynamicConfigValue {
            value: Some(DataBlob::new(EncodingType::JSON, enabled.to_string().into_bytes())), filters: vec![],
        }]);
        let request = || Request::new(m::AddDecisionTaskRequest {
            domain_id: key.domain_id.to_string(), task_list: list(), workflow_execution: execution(), schedule_id: 9, ..Default::default()
        });
        assert_eq!(matching.add_decision_task(request()).await.unwrap_err().code(), tonic::Code::Unavailable);
        matching.add_decision_task(request()).await.unwrap();
    }
    assert!(first.queue.is_stopped());
    let replacement = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    assert!(!Arc::ptr_eq(&first.queue, &replacement.queue));
    assert_eq!(store.get_tasks(&key, 0, i64::MAX, 10).unwrap().len(), 2);
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_isolation_policy_rechecks_drains_and_poller_recency() {
    let (_dir, _store, matching, key) = fixture();
    use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
    {
        let mut config = matching.frontend.state.admin.dynamic_config.lock().unwrap();
        for (name, value) in [("system.enableTasklistIsolation", "true"), ("system.allIsolationGroups", "[\"a\"]")] {
            config.update(false, name, vec![DynamicConfigValue { value: Some(DataBlob::new(EncodingType::JSON, value.as_bytes().to_vec())), filters: vec![] }]);
        }
    }
    let queue = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    let provider = matching.queues.as_ref().unwrap().isolation_provider_for_test();
    let isolation = provider(&key.domain_id.to_string(), "queue", 0, 0, queue.pollers.clone(), queue.partitions.clone());
    let task = cadence_persistence::matching_blob::TaskData { created_nanos: matching_now(), partition_config: Some(std::collections::BTreeMap::from([("isolation-group".into(), "a".into())])), ..Default::default() };
    assert_eq!((isolation.select)(&task).0, "", "no recent group poller leaks");
    queue.pollers.lock().unwrap().start("id", cadence_common::poller::Info { identity: "worker".into(), rate_per_second: 1.0, isolation_group: "a".into() }, matching_now());
    assert_eq!((isolation.select)(&task).0, "a");
    matching.frontend.state.domains.update_domain_isolation_groups("domain", Some(cadence_api::types::IsolationGroupConfiguration { isolation_groups: vec![cadence_api::types::IsolationGroupPartition { name: "a".into(), state: Some(cadence_api::enums::IsolationGroupState::Drained) }] })).unwrap();
    assert_eq!((isolation.select)(&task).0, "", "drain changes leak without restarting the owner");
    let sticky = provider(&key.domain_id.to_string(), "queue", 0, 1, queue.pollers.clone(), queue.partitions.clone());
    assert!(sticky.groups.is_empty());
    assert_eq!((sticky.select)(&task).0, "");
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
async fn matching_rpc_decision_poll_preserves_isolation_group_history() {
    let (_dir, _store, matching, key) = fixture();
    let queue = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "group-poller".into(), isolation_group: "zone-decision".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), identity: "decision-worker".into(), ..Default::default() }),
            ..Default::default()
        })).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let now = matching_now();
            let counts = queue.pollers.lock().unwrap().count_by_group(now, now + 1_000_000_000);
            if counts.get("zone-decision") == Some(&1) { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("outstanding decision poller retains its group outside the recent window");
    matching.cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
        domain_id: key.domain_id.to_string(), task_list: list(), task_list_type: 1, poller_id: "group-poller".into(),
    })).await.unwrap();
    assert_eq!(poll.await.unwrap().unwrap_err().code(), tonic::Code::Cancelled);
    let now = matching_now();
    let mut pollers = queue.pollers.lock().unwrap();
    assert_eq!(pollers.count_by_group(now, now - 2_000_000_000).get("zone-decision"), Some(&1));
    assert!(pollers.count_by_group(now, now + 1_000_000_000).is_empty());
}

#[tokio::test]
async fn matching_rpc_durable_describe_tracks_activity_poller_and_queue_status() {
    let (_dir, _store, matching, key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "system.allIsolationGroups", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#"["zone-a","zone-b"]"#.to_vec())), filters: vec![],
    }]);
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_activity_task(Request::new(m::PollForActivityTaskRequest {
                domain_id: poll_domain,
                poller_id: "activity-poller".into(),
                isolation_group: "zone-a".into(),
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
    assert_eq!(status.isolation_group_metrics.len(), 2);
    assert_eq!(status.isolation_group_metrics["zone-a"].poller_count, 1);
    assert_eq!(status.isolation_group_metrics["zone-b"].poller_count, 0);
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
    assert_eq!(ended.task_list_status.unwrap().isolation_group_metrics["zone-a"].poller_count, 1);
    // A live negative window excludes completed polls, but never outstanding
    // polls. This avoids sleeping and verifies the task-list window is read live.
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.taskIsolationPollerWindow", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#""-1s""#.to_vec())), filters: vec![],
    }]);
    assert_eq!(describe().await.unwrap().into_inner().task_list_status.unwrap().isolation_group_metrics["zone-a"].poller_count, 0);
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

#[tokio::test]
async fn matching_rpc_scopes_partition_counts_to_task_list_and_type() {
    let (_dir, _store, matching, key) = fixture();
    use cadence_api::{admin::{DynamicConfigFilter, DynamicConfigValue}, enums::EncodingType, types::DataBlob};
    let json_blob = |value: serde_json::Value| Some(DataBlob::new(EncodingType::JSON, serde_json::to_vec(&value).unwrap()));
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(
        false, "matching.numTasklistWritePartitions", vec![DynamicConfigValue {
            value: json_blob(serde_json::json!(4)),
            filters: vec![
                DynamicConfigFilter { name: "domainName".into(), value: json_blob(serde_json::json!("domain")) },
                DynamicConfigFilter { name: "taskListName".into(), value: json_blob(serde_json::json!("queue")) },
                DynamicConfigFilter { name: "taskType".into(), value: json_blob(serde_json::json!(1)) },
            ],
        }],
    );
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(
        false, "matching.numTasklistReadPartitions", vec![DynamicConfigValue {
            value: json_blob(serde_json::json!(3)),
            filters: vec![
                DynamicConfigFilter { name: "domainName".into(), value: json_blob(serde_json::json!("domain")) },
                DynamicConfigFilter { name: "taskListName".into(), value: json_blob(serde_json::json!("queue")) },
                DynamicConfigFilter { name: "taskType".into(), value: json_blob(serde_json::json!(1)) },
            ],
        }],
    );
    let response = matching.list_task_list_partitions(Request::new(m::ListTaskListPartitionsRequest {
        domain: "domain".into(), task_list: list(),
    })).await.unwrap().into_inner();
    assert_eq!(response.decision_task_list_partitions.len(), 1);
    assert_eq!(response.activity_task_list_partitions.len(), 4);
    let decision = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    let activity = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 1).await.unwrap();
    assert_eq!(decision.limiter.partitions(), 1);
    assert_eq!(activity.limiter.partitions(), 3);
    let other = matching.list_task_list_partitions(Request::new(m::ListTaskListPartitionsRequest {
        domain: "domain".into(), task_list: Some(p::TaskList { name: "other".into(), ..Default::default() }),
    })).await.unwrap().into_inner();
    assert_eq!(other.activity_task_list_partitions.len(), 1);
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
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "system.allIsolationGroups", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#"["z","unused"]"#.to_vec())), filters: vec![],
    }]);
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
                filters: vec![
                    cadence_api::admin::DynamicConfigFilter {
                        name: "taskListName".into(),
                        value: Some(cadence_api::types::DataBlob::new(
                            cadence_api::enums::EncodingType::JSON, b"\"queue\"".to_vec(),
                        )),
                    },
                    cadence_api::admin::DynamicConfigFilter {
                        name: "taskType".into(),
                        value: Some(cadence_api::types::DataBlob::new(
                            cadence_api::enums::EncodingType::JSON, b"0".to_vec(),
                        )),
                    },
                ],
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
    let activity = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 1).await.unwrap();
    activity.qps.report_counter(1);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(activity.qps.qps(), 0.0, "activity tracker retains the default 10s window");
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
    let status = status.task_list_status.expect("status requested");
    assert!(status.new_tasks_per_second > 0.0);
    assert!(status.isolation_group_metrics["z"].new_tasks_per_second > 0.0);
    assert_eq!(status.isolation_group_metrics["unused"].new_tasks_per_second, 0.0);
    assert_eq!(status.isolation_group_metrics["z"].poller_count, 0);
    // Group enumeration is live, and does not leak unconfigured tracker groups.
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "system.allIsolationGroups", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#"["new-group"]"#.to_vec())), filters: vec![],
    }]);
    let status = matching.describe_task_list(Request::new(m::DescribeTaskListRequest {
        domain_id: key.domain_id.to_string(), request: Some(p::DescribeTaskListRequest {
            task_list: list(), task_list_type: 1, include_task_list_status: true, ..Default::default()
        }),
    })).await.unwrap().into_inner().task_list_status.unwrap();
    assert_eq!(status.isolation_group_metrics.len(), 1);
    assert_eq!(status.isolation_group_metrics["new-group"].new_tasks_per_second, 0.0);
}

#[tokio::test]
async fn matching_rpc_reports_load_balancer_hints_only_for_old_task_lists() {
    let (_dir, _store, matching, key) = fixture();
    // Shorten the arrival-QPS window so the reported rate is observable.
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
    let registry = matching.queues.clone().expect("durable backend");
    // Go `taskListManagerImpl.LoadBalancerHints` withholds hints until the task
    // list manager is 10s old, and the hints carry the delivered-but-unacked
    // backlog plus the arrival rate.
    let old_key = TaskListKey {
        name: "old".into(),
        ..key.clone()
    };
    let old = registry
        .get(old_key, 0, matching_now() - 20_000_000_000)
        .await
        .unwrap();
    let fresh = registry.get(key.clone(), 0, matching_now()).await.unwrap();
    assert_eq!(fresh.load_balancer_hints(matching_now()), None);
    assert_eq!(
        old.load_balancer_hints(matching_now()),
        Some(m::LoadBalancerHints {
            backlog_count: 0,
            rate_per_second: 0.0,
        })
    );
    matching
        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: key.domain_id.to_string(),
            workflow_execution: execution(),
            task_list: Some(p::TaskList {
                name: "old".into(),
                ..Default::default()
            }),
            schedule_id: 7,
            ..Default::default()
        }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let hints = old
        .load_balancer_hints(matching_now())
        .expect("a 20s old task list reports hints");
    assert!(hints.rate_per_second > 0.0, "hints carry the arrival rate");
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

#[allow(deprecated)]
fn native_config(version: i64, read: &[i32], write: &[i32]) -> p::TaskListPartitionConfig {
    p::TaskListPartitionConfig {
        version,
        num_read_partitions: read.len() as i32,
        num_write_partitions: write.len() as i32,
        read_partitions: read.iter().map(|id| (*id, p::TaskListPartition { isolation_groups: vec![format!("group-{id}")] })).collect(),
        write_partitions: write.iter().map(|id| (*id, p::TaskListPartition::default())).collect(),
    }
}

#[tokio::test]
async fn matching_rpc_native_partition_cache_updates_refreshes_and_recovers() {
    let (_dir, store, matching, key) = fixture();
    let update = |config| Request::new(m::UpdateTaskListPartitionConfigRequest {
        domain_id: key.domain_id.to_string(), task_list: list(), task_list_type: 1, partition_config: Some(config),
    });
    matching.update_task_list_partition_config(update(native_config(999, &[0], &[0]))).await.unwrap();
    let root = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    assert!(root.partitions.get().is_none(), "initial single-partition config is a no-op");
    let child_list = p::TaskList { name: "/__cadence_sys/queue/5".into(), kind: 1, ..Default::default() };
    let child = matching.durable_queue(&key.domain_id.to_string(), &child_list, 0).await.unwrap();
    matching.update_task_list_partition_config(update(native_config(999, &[0, 5], &[0, 5]))).await.unwrap();
    assert_eq!(root.partitions.get().unwrap().version, 1);
    assert_eq!(child.partitions.get().unwrap().version, 1);
    assert_eq!(store.get_task_list(&key).unwrap().data.adaptive_partition_config.unwrap().version, 1);
    assert!(matching.frontend.state.executions.partition_config(&key.domain_id.to_string(), "queue", 1).is_none(), "native Matching must not write workflow execution metadata");
    matching.update_task_list_partition_config(update(native_config(-100, &[0, 5], &[0, 5]))).await.unwrap();
    assert_eq!(root.partitions.get().unwrap().version, 1, "request version is ignored for equality");
    matching.update_task_list_partition_config(update(native_config(1, &[0], &[0]))).await.unwrap();
    assert_eq!(child.partitions.get().unwrap().version, 2, "removed partition receives drain notification");
    assert!(!child.partitions.get().unwrap().read_partitions.contains_key(&5));
    let refresh = |config| Request::new(m::RefreshTaskListPartitionConfigRequest {
        domain_id: key.domain_id.to_string(), task_list: Some(child_list.clone()), task_list_type: 1, partition_config: config,
    });
    matching.refresh_task_list_partition_config(refresh(Some(native_config(1, &[0, 5], &[0])))).await.unwrap();
    assert_eq!(child.partitions.get().unwrap().version, 2, "old push ignored");
    matching.refresh_task_list_partition_config(refresh(Some(native_config(50, &[0, 5], &[0])))).await.unwrap();
    assert_eq!(child.partitions.get().unwrap().version, 50);
    matching.refresh_task_list_partition_config(refresh(None)).await.unwrap();
    assert_eq!(child.partitions.get().unwrap().version, 2, "explicit reload is not version gated");
    let described = matching.describe_task_list(Request::new(m::DescribeTaskListRequest {
        domain_id: key.domain_id.to_string(), request: Some(p::DescribeTaskListRequest { task_list: list(), task_list_type: 1, ..Default::default() }),
    })).await.unwrap().into_inner();
    assert_eq!(described.partition_config.unwrap().version, 2);
    matching.queues.as_ref().unwrap().unload(key.clone(), root, matching_now()).await.unwrap();
    let replacement = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    assert_eq!(replacement.partitions.get().unwrap().version, 2, "root startup reads native leased metadata");
    let late_child = matching.durable_queue(&key.domain_id.to_string(), &p::TaskList { name: "/__cadence_sys/queue/8".into(), kind: 1, ..Default::default() }, 0).await.unwrap();
    assert_eq!(late_child.partitions.get().unwrap().version, 2, "nonroot startup reads root row");
    let sticky = matching.durable_queue(&key.domain_id.to_string(), &p::TaskList { name: "sticky-config".into(), kind: 2, ..Default::default() }, 0).await.unwrap();
    assert!(sticky.partitions.get().is_none());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_native_partition_concurrent_versions_and_fencing() {
    let (_dir, store, matching, key) = fixture();
    let registry = matching.queues.as_ref().unwrap();
    let root = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    let first = registry.update_partition_config(key.clone(), root.clone(), partition_config_from_proto(native_config(0, &[0, 1], &[0])), matching_now());
    let second = registry.update_partition_config(key.clone(), root.clone(), partition_config_from_proto(native_config(0, &[0, 2], &[0])), matching_now());
    let (first, second) = tokio::join!(first, second);
    first.unwrap(); second.unwrap();
    assert_eq!(root.partitions.get().unwrap().version, 2);
    assert_eq!(store.get_task_list(&key).unwrap().data.adaptive_partition_config.unwrap().version, 2);
    store.lease(&key, 0, 0, matching_now()).unwrap();
    let error = registry.update_partition_config(key, root.clone(), partition_config_from_proto(native_config(0, &[0, 3], &[0])), matching_now()).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(root.queue.is_stopped());
    assert_eq!(root.partitions.get().unwrap().version, 2);
}

#[tokio::test]
async fn matching_rpc_native_partition_cache_controls_dispatch_rate_and_drained_admission() {
    use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
    let (_dir, store, matching, key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.enableGetNumberOfPartitionsFromCache", vec![DynamicConfigValue {
        value: Some(DataBlob::new(EncodingType::JSON, b"true".to_vec())), filters: vec![],
    }]);
    matching.update_task_list_partition_config(Request::new(m::UpdateTaskListPartitionConfigRequest {
        domain_id: key.domain_id.to_string(), task_list: list(), task_list_type: 1,
        partition_config: Some(native_config(0, &[0, 5, 9], &[0])),
    })).await.unwrap();
    let child_list = p::TaskList { name: "/__cadence_sys/queue/5".into(), kind: 1, ..Default::default() };
    let child = matching.durable_queue(&key.domain_id.to_string(), &child_list, 0).await.unwrap();
    assert_eq!(child.limiter.partitions(), 3, "limiter uses read map size, including sparse IDs");
    let error = matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), task_list: Some(child_list), workflow_execution: execution(), schedule_id: 7, ..Default::default()
    })).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert_eq!(error.message(), "Current partition is drained.");
    assert_eq!(child.qps.qps(), 0.0);
    assert!(store.get_tasks(&TaskListKey { name: "/__cadence_sys/queue/5".into(), ..key }, 0, i64::MAX, 10).unwrap().is_empty());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}
