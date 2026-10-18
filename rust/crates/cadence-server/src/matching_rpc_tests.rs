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

#[tokio::test]
async fn matching_rpc_live_gc_batch_config_is_filtered_and_resampled() {
    let (_dir, _store, matching, key) = fixture();
    use cadence_api::{admin::{DynamicConfigFilter, DynamicConfigValue}, enums::EncodingType, types::DataBlob};
    let blob = |value: serde_json::Value| Some(DataBlob::new(EncodingType::JSON, serde_json::to_vec(&value).unwrap()));
    let overlay = matching.frontend.state.admin.dynamic_config.clone();
    let update = |value: i64| {
        overlay.lock().unwrap().update(false, "matching.maxTaskDeleteBatchSize", vec![DynamicConfigValue {
            value: blob(serde_json::json!(value)),
            filters: vec![
                DynamicConfigFilter { name: "domainName".into(), value: blob(serde_json::json!("domain")) },
                DynamicConfigFilter { name: "taskListName".into(), value: blob(serde_json::json!("queue")) },
                DynamicConfigFilter { name: "taskType".into(), value: blob(serde_json::json!(0)) },
            ],
        }]);
    };
    update(0);
    let domain = key.domain_id.to_string();
    let provider = super::task_delete_batch_size_provider(matching.frontend.state.domains.clone(), overlay.clone());
    assert_eq!(provider(&domain, "queue", 0), 0);
    assert_eq!(provider(&domain, "other", 0), 100, "task-list filter must be honored");
    assert_eq!(provider("unknown-domain", "queue", 0), 100, "domain filter must be honored");
    assert_eq!(provider(&domain, "queue", 1), 100, "task-type filter must be honored");

    update(1);
    assert_eq!(provider(&domain, "queue", 0), 1, "an existing provider observes updated live config");
}

#[tokio::test]
async fn matching_rpc_live_writer_config_is_filtered_and_resampled() {
    let (_dir, _store, matching, key) = fixture();
    use cadence_api::{admin::{DynamicConfigFilter, DynamicConfigValue}, enums::EncodingType, types::DataBlob};
    let blob = |value: serde_json::Value| Some(DataBlob::new(EncodingType::JSON, serde_json::to_vec(&value).unwrap()));
    let overlay = matching.frontend.state.admin.dynamic_config.clone();
    let update = |property: &str, value: serde_json::Value| {
        overlay.lock().unwrap().update(false, property, vec![DynamicConfigValue {
            value: blob(value),
            filters: vec![
                DynamicConfigFilter { name: "domainName".into(), value: blob(serde_json::json!("domain")) },
                DynamicConfigFilter { name: "taskListName".into(), value: blob(serde_json::json!("queue")) },
                DynamicConfigFilter { name: "taskType".into(), value: blob(serde_json::json!(0)) },
            ],
        }]);
    };
    update("matching.maxTaskBatchSize", serde_json::json!(0));
    update("matching.appendTaskTimeout", serde_json::json!("17ms"));
    update("matching.outstandingTaskAppendsThreshold", serde_json::json!(4));
    let provider = super::task_writer_live_provider(matching.frontend.state.domains.clone(), overlay.clone());
    let domain = key.domain_id.to_string();
    assert_eq!(provider(&domain, "queue", 0), (0, 17_000_000, 4));
    assert_eq!(provider(&domain, "other", 0), (100, 5_000_000_000, 250), "task-list filters and Go default must be honored");
    assert_eq!(provider("unknown-domain", "queue", 0), (100, 5_000_000_000, 250), "domain filters must be honored");
    assert_eq!(provider(&domain, "queue", 1), (100, 5_000_000_000, 250), "task-type filters must be honored");
    update("matching.maxTaskBatchSize", serde_json::json!(2));
    update("matching.appendTaskTimeout", serde_json::json!("31ms"));
    update("matching.outstandingTaskAppendsThreshold", serde_json::json!(0));
    assert_eq!(provider(&domain, "queue", 0), (2, 31_000_000, 0), "provider updates are visible; each writer samples capacity only at construction");
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

#[tokio::test]
async fn matching_rpc_forward_task_limits_reload_and_rate_zero_blocks() {
    for property in ["matching.forwarderMaxOutstandingTasks", "matching.forwarderMaxRatePerSecond"] {
        let (_dir, _store, matching, key) = fixture();
        matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, property, vec![cadence_api::admin::DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"0".to_vec())), filters: vec![],
        }]);
        let queue = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
        assert!(queue.try_forward_task().is_none(), "{property}=0 prevents forwarding");
        if property == "matching.forwarderMaxOutstandingTasks" {
            matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, property, vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"1".to_vec())), filters: vec![],
            }]);
            let permit = queue.try_forward_task().expect("live limit replaces the zero-capacity generation");
            assert!(queue.try_forward_task().is_none());
            drop(permit);
            assert!(queue.try_forward_task().is_some());
        }
        matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    }
}

#[tokio::test]
async fn matching_rpc_semaphore_acquire_grants_and_reports_no_slot() {
    let (_dir, _store, matching, _key) = fixture();
    let mut matching = matching;
    matching.frontend.state.semaphore_enabled = true;
    let domain = Uuid::from_u128(1).to_string();
    // Registration normally happens on the RegisterDomain path; mirror it so
    // the bucket metadata is keyed by the domain UUID the request carries.
    matching
        .frontend
        .state
        .executions
        .register_domain("domain", &domain);
    matching
        .frontend
        .state
        .executions
        .create_semaphore("domain", "sem", 2, 2)
        .unwrap();
    let acquire = |matching: &Matching, owner: &str| {
        let domain = domain.clone();
        let owner = owner.to_string();
        let matching = matching.clone();
        async move {
            MatchingApi::acquire_semaphore(
                &matching,
                Request::new(m::AcquireSemaphoreRequest {
                    domain_id: domain,
                    semaphore_name: "sem".into(),
                    bucket: 0,
                    owner_id: owner,
                }),
            )
            .await
            .map(|response| response.into_inner())
        }
    };
    // The first two owners fill the two-slot bucket.
    let first = acquire(&matching, "1:w:r1:1").await.unwrap();
    assert_eq!(first.outcome, 1, "first acquire is Acquired");
    assert!((1..=2).contains(&first.token_id));
    let second = acquire(&matching, "1:w:r2:1").await.unwrap();
    assert_eq!(second.outcome, 1);
    assert_ne!(first.token_id, second.token_id);
    // A third owner gets NoSlot rather than blocking.
    let third = acquire(&matching, "1:w:r3:1").await.unwrap();
    assert_eq!((third.outcome, third.token_id), (2, 0));
    // A re-acquire by a holder returns its slot.
    let again = acquire(&matching, "1:w:r1:1").await.unwrap();
    assert_eq!((again.outcome, again.token_id), (1, first.token_id));
    // A bad owner id surfaces as a typed error.
    let bad_owner = acquire(&matching, "").await.unwrap_err();
    assert_eq!(bad_owner.code(), tonic::Code::InvalidArgument);
    // A missing semaphore is NotFound from the startup scan.
    let missing = MatchingApi::acquire_semaphore(
        &matching,
        Request::new(m::AcquireSemaphoreRequest {
            domain_id: domain.clone(),
            semaphore_name: "absent".into(),
            bucket: 0,
            owner_id: "1:w:r1:1".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);
    // Disabled domains are rejected before any bucket work.
    matching.frontend.state.semaphore_enabled = false;
    let disabled = acquire(&matching, "1:w:r9:1").await.unwrap_err();
    assert_eq!(disabled.code(), tonic::Code::InvalidArgument);
}

fn scaler_config_value(matching: &Matching, property: &str, json: &[u8]) {
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .unwrap()
        .update(
            false,
            property,
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    json.to_vec(),
                )),
                filters: vec![],
            }],
        );
}

#[tokio::test]
async fn matching_rpc_manual_partition_update_rejected_while_scaler_enabled() {
    let (_dir, _store, matching, key) = fixture();
    scaler_config_value(&matching, "matching.enableAdaptiveScaler", b"true");
    let error = matching
        .update_task_list_partition_config(Request::new(
            m::UpdateTaskListPartitionConfigRequest {
                domain_id: key.domain_id.to_string(),
                task_list: list(),
                task_list_type: 1,
                partition_config: Some(native_config(1, &[0, 1], &[0, 1])),
            },
        ))
        .await
        .expect_err("Go rejects manual updates while the scaler owns the config");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
}

#[tokio::test]
async fn matching_rpc_adaptive_scaler_upscales_write_partitions_on_load() {
    let address = start_starting_history().await;
    let (_dir, store, matching, key) = fixture_with_history(&address);
    for (property, value) in [
        ("matching.enableAdaptiveScaler", b"true".as_slice()),
        ("matching.enableGetNumberOfPartitionsFromCache", b"true".as_slice()),
        ("matching.adaptiveScalerUpdateInterval", b"\"50ms\"".as_slice()),
        ("matching.partitionUpscaleRPS", b"1".as_slice()),
        ("matching.partitionUpscaleSustainedDuration", b"\"0s\"".as_slice()),
        ("matching.partitionDownscaleSustainedDuration", b"\"1h\"".as_slice()),
    ] {
        scaler_config_value(&matching, property, value);
    }
    // Resolving the root owner starts its scaler loop (normal kind, root).
    let queue = matching
        .durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0)
        .await
        .unwrap();
    assert!(queue.partitions.get().is_none());
    // A burst of arrivals reports QPS well above 1*1, so the sustained
    // overload grows the dense write map through the versioned update path.
    for schedule_id in 101..113 {
        matching
            .add_decision_task(Request::new(m::AddDecisionTaskRequest {
                domain_id: key.domain_id.to_string(),
                workflow_execution: execution(),
                task_list: list(),
                schedule_id,
                ..Default::default()
            }))
            .await
            .unwrap();
    }
    let config = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(config) = queue.partitions.get() {
                if config.write_partitions.len() >= 2 {
                    return config;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sustained load must upscale write partitions");
    assert!(config.read_partitions.contains_key(&0));
    assert!(config.write_partitions.contains_key(&0));
    assert!(config
        .write_partitions
        .keys()
        .all(|id| config.read_partitions.contains_key(id)));
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 12);
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
}
#[tokio::test]
async fn matching_rpc_forwarded_root_backlog_waits_for_late_poller() {
    let address = start_starting_history().await;
    let (_dir, store, matching, key) = fixture_with_history(&address);
    let producer = matching.clone();
    let domain = key.domain_id.to_string();
    let add = tokio::spawn(async move {
        let mut request = Request::new(m::AddDecisionTaskRequest {
            domain_id: domain, workflow_execution: execution(), task_list: list(), schedule_id: 71,
            source: 2, forwarded_from: "/__cadence_sys/queue/1".into(), ..Default::default()
        });
        request.set_timeout(Duration::from_secs(2));
        producer.add_decision_task(request).await
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!add.is_finished(), "forwarded backlog waits beyond the local 10ms offer");
    let poll = matching.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), poller_id: "late-backlog".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
        ..Default::default()
    })).await.unwrap().into_inner();
    assert_eq!(poll.started_event_id, 72);
    add.await.unwrap().unwrap();
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}
struct StartingHistory;

#[tonic::async_trait]
impl h::history_api_server::HistoryApi for StartingHistory {
    async fn record_decision_task_started(
        &self,
        request: Request<h::RecordDecisionTaskStartedRequest>,
    ) -> Result<Response<h::RecordDecisionTaskStartedResponse>, Status> {
        let request = request.into_inner();
        if request.schedule_id == 41 {
            assert_eq!(request.poll_request.as_ref().unwrap().task_list.as_ref().unwrap().name, "queue", "History receives the rebuilt parent poll request");
        }
        Ok(Response::new(h::RecordDecisionTaskStartedResponse {
            scheduled_event_id: request.schedule_id,
            started_event_id: request.schedule_id + 1,
            ..Default::default()
        }))
    }
}

async fn start_starting_history() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(h::history_api_server::HistoryApiServer::new(StartingHistory))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{address}")
}

#[tokio::test]
async fn matching_rpc_sync_success_without_partition_metadata_does_not_persist() {
    let address = start_starting_history().await;
    let (_dir, store, matching, key) = fixture_with_history(&address);
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain,
            poller_id: "sync-worker".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
            ..Default::default()
        })).await.unwrap().into_inner()
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while matching.polls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let response = matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: key.domain_id.to_string(),
        workflow_execution: execution(),
        task_list: list(),
        schedule_id: 31,
        ..Default::default()
    })).await.unwrap().into_inner();
    assert!(response.partition_config.is_none());
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), poll).await.unwrap().unwrap().started_event_id, 32);
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_child_poll_forwards_to_parent_backlog() {
    let address = start_starting_history().await;
    let (_dir, store, matching, key) = fixture_with_history(&address);
    matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: key.domain_id.to_string(),
        workflow_execution: execution(), task_list: list(), schedule_id: 41,
        ..Default::default()
    })).await.unwrap();
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 1);
    let response = tokio::time::timeout(Duration::from_secs(2), matching.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), poller_id: "child-worker".into(),
        request: Some(p::PollForDecisionTaskRequest {
            task_list: Some(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() }),
            ..Default::default()
        }),
        ..Default::default()
    }))).await.expect("child forwards instead of blocking on its empty reader").unwrap().into_inner();
    assert_eq!(response.started_event_id, 42);
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_child_add_sync_matches_parent_without_duplicate_persistence() {
    let address = start_starting_history().await;
    let (_dir, store, matching, key) = fixture_with_history(&address);
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "parent-worker".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
            ..Default::default()
        })).await.unwrap().into_inner()
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while matching.polls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let child_name = "/__cadence_sys/queue/1";
    matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), workflow_execution: execution(),
        task_list: Some(p::TaskList { name: child_name.into(), kind: 1, ..Default::default() }),
        schedule_id: 51, ..Default::default()
    })).await.unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), poll).await.unwrap().unwrap().started_event_id, 52);
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
    assert_eq!(store.task_list_size(&TaskListKey { name: child_name.into(), ..key }, 0).unwrap(), 0);
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_cancelled_forwarded_poll_unregisters_parent_waiter() {
    let (_dir, _store, matching, key) = fixture();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "cancel-forward".into(),
            request: Some(p::PollForDecisionTaskRequest {
                task_list: Some(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() }),
                identity: "cancel-forward".into(), ..Default::default()
            }), ..Default::default()
        })).await
    });
    let root = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root.pollers.lock().unwrap().contains_poll("cancel-forward") {
            tokio::task::yield_now().await;
        }
    }).await.expect("forward registers a parent waiter");
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while root.pollers.lock().unwrap().contains_poll("cancel-forward") {
            tokio::task::yield_now().await;
        }
    }).await.expect("cancellation withdraws the forwarded waiter");
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_forwarding_disabled_keeps_parent_backlog_untouched() {
    for (property, value) in [
        ("matching.forwarderMaxChildrenPerNode", b"0".as_slice()),
        ("matching.forwarderMaxOutstandingPolls", b"0".as_slice()),
    ] {
        let address = start_starting_history().await;
        let (_dir, store, matching, key) = fixture_with_history(&address);
        matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, property, vec![cadence_api::admin::DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, value.to_vec())), filters: vec![],
        }]);
        matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: key.domain_id.to_string(), task_list: list(), workflow_execution: execution(), schedule_id: 61,
            ..Default::default()
        })).await.unwrap();
        let child_list = p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() };
        let child = matching.durable_queue(&key.domain_id.to_string(), &child_list, 0).await.unwrap();
        let response = child.dispatcher.poll_decision(p::PollForDecisionTaskRequest { task_list: Some(child_list), ..Default::default() }, Duration::from_millis(30)).await.unwrap();
        assert!(response.task_token.is_empty(), "{property}=0 disables parent polling");
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 1);
        matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, property, vec![cadence_api::admin::DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"20".to_vec())), filters: vec![],
        }]);
        let response = child.dispatcher.poll_decision(p::PollForDecisionTaskRequest {
            task_list: Some(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() }),
            ..Default::default()
        }, Duration::from_secs(2)).await.unwrap();
        assert_eq!(response.started_event_id, 62, "{property} reloads without replacing the owner");
        matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    }
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
    // Go's non-blocking query offer only succeeds with a poller already
    // parked; wait until the poll parks on the query channel.
    let root = matching
        .durable_queue(&domain, &list().unwrap(), 0)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root.queries.has_waiters() {
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
async fn matching_rpc_strong_query_rejects_a_worker_without_consistent_query() {
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
    let root = matching
        .durable_queue(&domain, &list().unwrap(), 0)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root.queries.has_waiters() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
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
                    // A strong-consistency query enforces the worker gate.
                    query_consistency_level: p::QueryConsistencyLevel::Strong as i32,
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
    let token =
        cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
            &delivered.task_token,
        )
        .expect("poll should carry a query task token");
    // The worker answers, but reports a version that predates consistent query.
    matching
        .respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
            request: Some(p::RespondQueryTaskCompletedRequest {
                task_token: Vec::new(),
                result: Some(p::WorkflowQueryResult {
                    result_type: p::QueryResultType::Answered as i32,
                    answer: Some(p::Payload {
                        data: b"stale".to_vec(),
                    }),
                    error_message: String::new(),
                }),
                worker_version_info: Some(p::WorkerVersionInfo {
                    r#impl: "uber-go".into(),
                    feature_version: "1.4.0".into(),
                }),
            }),
            domain_id: domain,
            task_list: list(),
            task_id: token.task_id,
        }))
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("query should complete")
        .unwrap()
        .expect_err("a strong query from an unsupported worker must fail");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn matching_rpc_child_query_forwards_to_parent_poller() {
    child_query_on(list().unwrap()).await;
}

#[tokio::test]
async fn matching_rpc_child_query_completes_on_the_accepting_partition() {
    child_query_on(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() }).await;
}

async fn child_query_on(partition: p::TaskList) {
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let poll_list = partition.clone();
    let worker = matching.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "parent-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: Some(poll_list),
                    identity: "parent-worker".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
    });
    // Wait until the parent poller parks on the query channel; Go's
    // non-blocking offer only succeeds with a waiting poller.
    let root = matching
        .durable_queue(&domain, &partition, 0)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root.queries.has_waiters() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let child_list = p::TaskList {
        name: "/__cadence_sys/queue/1".into(),
        kind: 1,
        ..Default::default()
    };
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
                task_list: Some(child_list),
                forwarded_from: String::new(),
            }))
            .await
    });
    let delivered = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("forwarded query should reach the parent poller")
        .unwrap()
        .expect("poll should succeed")
        .into_inner();
    assert_eq!(
        delivered.query.as_ref().map(|q| q.query_type.clone()),
        Some("ping".to_string())
    );
    let token =
        cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
            &delivered.task_token,
        )
        .expect("poll should carry a query task token");
    assert_eq!(token.task_list, partition.name);
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
            task_list: Some(p::TaskList { name: token.task_list, kind: 1, ..Default::default() }),
            task_id: token.task_id,
        }))
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("forwarded query should complete")
        .unwrap()
        .expect("forwarded query should succeed")
        .into_inner();
    assert_eq!(
        response.query_result.map(|payload| payload.data),
        Some(b"pong".to_vec())
    );
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
}

#[tokio::test]
async fn matching_rpc_query_without_poller_fails_fast() {
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    // Go `OfferQuery` at the root returns `ErrNoParent` when no poller waits
    // instead of holding the query for the whole long-poll expiration.
    let error = tokio::time::timeout(Duration::from_secs(2), matching.query_workflow(
        Request::new(m::QueryWorkflowRequest {
            request: Some(p::QueryWorkflowRequest {
                domain: "domain".into(),
                workflow_execution: execution(),
                query: Some(p::WorkflowQuery {
                    query_type: "ping".into(),
                    query_args: None,
                }),
                ..Default::default()
            }),
            domain_id: key.domain_id.to_string(),
            task_list: list(),
            forwarded_from: String::new(),
        }),
    ))
    .await
    .expect("query without a poller must not wait out the poll expiration")
    .expect_err("query without a poller must fail");
    assert_eq!(error.code(), tonic::Code::Internal);
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
}

#[tokio::test]
async fn matching_rpc_reports_arrival_qps_by_isolation_group() {
    let (_dir, _store, matching, key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "system.allIsolationGroups", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#"["z","unused"]"#.to_vec())), filters: vec![],
    }]);
    // Go `matching.localTaskWaitTime` (default 10ms) is spent on the sync-match
    // attempt of every add when no poller waits. Disable it here so the five
    // adds below stay inside the open QPS window; sync behavior itself is
    // covered by the `matching_sync` tests.
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .update(
            false,
            "matching.localTaskWaitTime",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    b"\"0\"".to_vec(),
                )),
                filters: vec![],
            }],
        );
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

#[tokio::test]
async fn matching_rpc_forwarded_add_never_persists_and_reports_remote_failure() {
    let (_dir, store, matching, key) = fixture();
    // Skip the sync wait so the unmatched forward fails fast; the failure
    // path itself is what this test covers.
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .update(
            false,
            "matching.localTaskWaitTime",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    b"\"0\"".to_vec(),
                )),
                filters: vec![],
            }],
        );
    // Go `errRemoteSyncMatchFailed`: with no poller waiting, a forwarded add
    // fails instead of persisting; the child partition keeps the task.
    let error = matching
        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: key.domain_id.to_string(),
            workflow_execution: execution(),
            task_list: list(),
            schedule_id: 11,
            forwarded_from: "/__cadence_sys/queue/1".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "remote sync match failed");
    let details = String::from_utf8_lossy(error.details());
    assert!(
        details.contains("uber.cadence.shared.v1.RemoteSyncMatchedError"),
        "Go decodes the shared.v1 detail, got {details:?}"
    );
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
    // A forwarded add reports neither liveness nor arrival QPS.
    let queue = matching
        .durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0)
        .await
        .unwrap();
    assert_eq!(queue.qps.qps(), 0.0);
}

#[tokio::test]
async fn matching_rpc_child_partition_add_falls_back_to_local_persist() {
    let (_dir, store, matching, key) = fixture();
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .update(
            false,
            "matching.localTaskWaitTime",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    b"\"0\"".to_vec(),
                )),
                filters: vec![],
            }],
        );
    // Go `Offer` falls back to the child partition when no poller takes the
    // task locally or at the root: the child persists and dispatches it.
    let child_name = "/__cadence_sys/queue/1".to_string();
    matching
        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: key.domain_id.to_string(),
            workflow_execution: execution(),
            task_list: Some(p::TaskList {
                name: child_name.clone(),
                kind: 1,
                ..Default::default()
            }),
            schedule_id: 12,
            ..Default::default()
        }))
        .await
        .unwrap();
    let child_key = TaskListKey {
        name: child_name,
        ..key
    };
    assert_eq!(store.task_list_size(&child_key, 0).unwrap(), 1);
}

#[tokio::test]
async fn matching_rpc_domain_listing_uses_loaded_matching_managers() {
    let (_dir, _store, matching, key) = fixture();
    let request = || Request::new(m::GetTaskListsByDomainRequest { domain: "domain".into() });
    assert!(matching.get_task_lists_by_domain(request()).await.unwrap().into_inner().decision_task_list_map.is_empty());
    let normal = list().unwrap();
    matching.durable_queue(&key.domain_id.to_string(), &normal, 0).await.unwrap();
    matching.durable_queue(&key.domain_id.to_string(), &normal, 1).await.unwrap();
    let sticky = p::TaskList { name: "sticky".into(), kind: 2, ..Default::default() };
    matching.durable_queue(&key.domain_id.to_string(), &sticky, 0).await.unwrap();
    let result = matching.get_task_lists_by_domain(request()).await.unwrap().into_inner();
    assert_eq!(result.decision_task_list_map.len(), 1);
    assert_eq!(result.activity_task_list_map.len(), 1);
    let description = &result.decision_task_list_map["queue"];
    assert_eq!(description.task_list.as_ref().unwrap(), &normal);
    assert!(description.task_list_status.is_none());
    let registry = matching.queues.as_ref().unwrap();
    let all = registry.describe_by_domain(key.domain_id, true, matching_now());
    assert_eq!(all.decision_task_list_map["sticky"].task_list.as_ref().unwrap().kind, 2);
    assert!(registry.describe_by_domain(Uuid::from_u128(999), true, matching_now()).decision_task_list_map.is_empty());
    let handle = matching.durable_queue(&key.domain_id.to_string(), &normal, 0).await.unwrap();
    registry.unload(key.clone(), handle, matching_now()).await.unwrap();
    assert!(!matching.get_task_lists_by_domain(request()).await.unwrap().into_inner().decision_task_list_map.contains_key("queue"));
}

#[tokio::test]
async fn matching_rpc_standby_polls_leave_durable_backlog_unclaimed() {
    let (_dir, store, mut matching, _key) = fixture();
    matching.poll_expiration = Duration::from_millis(30);
    let domain = Uuid::from_u128(3).to_string();
    matching.frontend.state.domains.create(
        cadence_api::types::DomainInfo { name: "standby".into(), uuid: domain.clone(), ..Default::default() },
        Default::default(),
        cadence_api::types::DomainReplicationConfiguration { active_cluster_name: "remote".into(), ..Default::default() },
        true,
    ).unwrap();
    matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: domain.clone(), task_list: list(), workflow_execution: execution(), schedule_id: 9, ..Default::default()
    })).await.unwrap();
    matching.add_activity_task(Request::new(m::AddActivityTaskRequest {
        domain_id: domain.clone(), source_domain_id: domain.clone(), task_list: list(), workflow_execution: execution(), schedule_id: 10, ..Default::default()
    })).await.unwrap();
    let decision = matching.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain.clone(), request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner();
    assert!(decision.task_token.is_empty());
    let activity = matching.poll_for_activity_task(Request::new(m::PollForActivityTaskRequest {
        domain_id: domain.clone(), request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner();
    assert!(activity.task_token.is_empty());
    for task_type in [0, 1] {
        let key = TaskListKey { domain_id: Uuid::parse_str(&domain).unwrap(), name: "queue".into(), task_type };
        assert_eq!(store.get_tasks(&key, 0, i64::MAX, 10).unwrap().len(), 1);
        let queue = matching.durable_queue(&domain, &list().unwrap(), task_type).await.unwrap();
        assert_eq!(queue.queue.status().1, 0, "standby poll must not acknowledge backlog");
    }
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[derive(Clone)]
struct StandbyHistory;
#[tonic::async_trait]
impl h::history_api_server::HistoryApi for StandbyHistory {
    async fn describe_workflow_execution(&self, request: Request<h::DescribeWorkflowExecutionRequest>) -> Result<Response<h::DescribeWorkflowExecutionResponse>, Status> {
        assert_eq!(request.get_ref().request.as_ref().unwrap().workflow_execution, execution());
        Ok(Response::new(h::DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(p::WorkflowExecutionInfo::default()),
            // No pending tasks: Go treats both task types as already started.
            ..Default::default()
        }))
    }
}

#[tokio::test]
async fn matching_rpc_standby_completes_started_tasks_without_worker_poll() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(h::history_api_server::HistoryApiServer::new(StandbyHistory))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)).await.unwrap();
    });
    let (_dir, _store, matching, _key) = fixture_with_history(&peer);
    let domain = Uuid::from_u128(3).to_string();
    matching.frontend.state.domains.create(
        cadence_api::types::DomainInfo { name: "standby".into(), uuid: domain.clone(), ..Default::default() },
        Default::default(),
        cadence_api::types::DomainReplicationConfiguration { active_cluster_name: "remote".into(), ..Default::default() }, true,
    ).unwrap();
    matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: domain.clone(), task_list: list(), workflow_execution: execution(), schedule_id: 9, ..Default::default()
    })).await.unwrap();
    matching.add_activity_task(Request::new(m::AddActivityTaskRequest {
        domain_id: domain.clone(), source_domain_id: domain.clone(), task_list: list(), workflow_execution: execution(), schedule_id: 10, ..Default::default()
    })).await.unwrap();
    for task_type in [0, 1] {
        let queue = matching.durable_queue(&domain, &list().unwrap(), task_type).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while queue.queue.status().1 == 0 { tokio::task::yield_now().await; }
        }).await.expect("standby backlog should be acknowledged without SDK polls");
    }
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    server.abort();
}

#[tokio::test]
async fn matching_rpc_domain_failover_releases_blocked_polls() {
    use cadence_api::types::{ClusterReplicationConfiguration, DomainReplicationConfiguration};
    use cadence_common::cluster::{ClusterInformation, Metadata};
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(3).to_string();
    matching.frontend.state.domains.create(
        cadence_api::types::DomainInfo { name: "global".into(), uuid: domain.clone(), ..Default::default() },
        cadence_api::types::DomainConfiguration { workflow_execution_retention_period_in_days: 3, ..Default::default() },
        DomainReplicationConfiguration {
            active_cluster_name: "active".into(),
            clusters: vec![ClusterReplicationConfiguration { cluster_name: "active".into() }, ClusterReplicationConfiguration { cluster_name: "standby".into() }],
            ..Default::default()
        }, true,
    ).unwrap();
    let worker = matching.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
            domain_id: poll_domain, poller_id: "failover-poller".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
        })).await
    });
    let queue = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async { while !queue.queries.has_waiters() { tokio::task::yield_now().await; } }).await.unwrap();
    let metadata = Metadata {
        failover_version_increment: 10, primary_cluster_name: "active".into(), current_cluster_name: "standby".into(),
        clusters: std::collections::BTreeMap::from([
            ("active".into(), ClusterInformation { enabled: true, initial_failover_version: 0, ..Default::default() }),
            ("standby".into(), ClusterInformation { enabled: true, initial_failover_version: 1, ..Default::default() }),
        ]),
    };
    matching.frontend.state.domains.failover(
        &cadence_api::types::FailoverDomainRequest { domain_name: "global".into(), domain_active_cluster_name: Some("standby".into()), ..Default::default() },
        "standby", &metadata, &cadence_persistence::domain_update::DomainPolicy { cooldown_nanos: 0, ..Default::default() },
    ).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), poll).await.expect("failover should release the long poll").unwrap().unwrap().into_inner();
    assert!(response.task_token.is_empty());
    assert!(!queue.queue.is_stopped(), "failover disconnect must retain the manager");
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_cross_host_poll_add_and_cancellation() {
    use cadence_common::membership::{Host, Ring};
    use crate::matching_membership::Membership;
    let history = start_starting_history().await;
    let (_root_dir, root_store, root, key) = fixture_with_history(&history);
    let (_child_dir, child_store, child, _) = fixture_with_history(&history);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let mut hosts = vec![
        Host { address: "node-0:7935".into(), identity: "node-0".into(), grpc_address: String::new() },
        Host { address: "node-1:7935".into(), identity: "node-1".into(), grpc_address: String::new() },
    ];
    let ring = Ring::new(hosts.clone());
    let root_id = ring.lookup("queue").unwrap().identity().to_owned();
    let child_name = (1..=20).map(|id| format!("/__cadence_sys/queue/{id}")).find(|name| ring.lookup(name).unwrap().identity() != root_id).unwrap();
    let child_id = ring.lookup(&child_name).unwrap().identity().to_owned();
    for host in &mut hosts { host.grpc_address = endpoint.clone(); }
    let root = root.with_membership(Membership::new(root_id, hosts.clone()));
    let child = child.with_membership(Membership::new(child_id, hosts));
    let service = root.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder().add_service(m::matching_api_server::MatchingApiServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)).await.unwrap();
    });
    let domain = key.domain_id.to_string();
    let child_list = || Some(p::TaskList { name: child_name.clone(), kind: 1, ..Default::default() });
    assert_eq!(child.durable_queue(&domain, &list().unwrap(), 0).await.err().unwrap().code(), tonic::Code::Aborted);
    root.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: domain.clone(), workflow_execution: execution(), task_list: list(), schedule_id: 41, ..Default::default()
    })).await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), child.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain.clone(), poller_id: "remote-backlog".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: child_list(), ..Default::default() }), ..Default::default()
    }))).await.unwrap().unwrap().into_inner();
    assert_eq!(response.started_event_id, 42);
    let root_queue = root.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let worker = root.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "remote-sync".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root_queue.pollers.lock().unwrap().contains_poll("remote-sync") { tokio::task::yield_now().await; }
    }).await.unwrap();
    child.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: domain.clone(), workflow_execution: execution(), task_list: child_list(), schedule_id: 51, ..Default::default()
    })).await.unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(3), poll).await.unwrap().unwrap().started_event_id, 52);
    assert_eq!(root_store.task_list_size(&key, 0).unwrap(), 0);
    assert_eq!(child_store.task_list_size(&TaskListKey { name: child_name.clone(), ..key.clone() }, 0).unwrap(), 0);
    let worker = child.clone();
    let poll_domain = domain.clone();
    let poll_list = child_list();
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "remote-cancel".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: poll_list, ..Default::default() }), ..Default::default()
    })).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root_queue.pollers.lock().unwrap().contains_poll("remote-cancel") { tokio::task::yield_now().await; }
    }).await.unwrap();
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while root_queue.pollers.lock().unwrap().contains_poll("remote-cancel") { tokio::task::yield_now().await; }
    }).await.expect("HTTP/2 cancellation withdraws the upstream waiter");
    child.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    root.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    server.abort();
}

#[tokio::test]
async fn matching_rpc_local_poll_wait_delays_forwarding_and_reloads() {
    let (_directory, _store, matching, key) = fixture();
    scaler_config_value(&matching, "matching.localPollWaitTime", br#""30s""#);
    let domain = key.domain_id.to_string();
    let child_list = p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() };
    let child = matching.durable_queue(&domain, &child_list, 0).await.unwrap();
    let root = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let start_poll = |id: &str| {
        let service = matching.clone();
        let domain = domain.clone();
        let task_list = child_list.clone();
        let id = id.to_owned();
        tokio::spawn(async move { service.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: id,
            request: Some(p::PollForDecisionTaskRequest { task_list: Some(task_list), ..Default::default() }), ..Default::default()
        })).await })
    };
    let delayed = start_poll("delayed");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !child.pollers.lock().unwrap().contains_poll("delayed") { tokio::task::yield_now().await; }
    }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!root.pollers.lock().unwrap().contains_poll("delayed"), "configured local wait precedes the parent poll");
    delayed.abort();
    assert!(delayed.await.unwrap_err().is_cancelled());
    scaler_config_value(&matching, "matching.localPollWaitTime", br#""0s""#);
    let immediate = start_poll("immediate");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root.pollers.lock().unwrap().contains_poll("immediate") { tokio::task::yield_now().await; }
    }).await.expect("next poll uses the current property instead of manager startup defaults");
    immediate.abort();
    assert!(immediate.await.unwrap_err().is_cancelled());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_cross_host_query_completes_on_accepting_host() {
    use cadence_common::membership::{Host, Ring};
    use crate::matching_membership::Membership;
    let history = start_fake_history(mutable_state()).await;
    let (_root_dir, _root_store, root, key) = fixture_with_history(&history);
    let (_child_dir, _child_store, child, _) = fixture_with_history(&history);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let hosts: Vec<_> = ["node-0", "node-1"].into_iter().map(|id| Host {
        address: format!("{id}:7935"), identity: id.into(), grpc_address: endpoint.clone(),
    }).collect();
    let ring = Ring::new(hosts.clone());
    let root_id = ring.lookup("queue").unwrap().identity().to_owned();
    let child_name = (1..=20).map(|id| format!("/__cadence_sys/queue/{id}")).find(|name| ring.lookup(name).unwrap().identity() != root_id).unwrap();
    let child_id = ring.lookup(&child_name).unwrap().identity().to_owned();
    let root = root.with_membership(Membership::new(root_id, hosts.clone()));
    let child = child.with_membership(Membership::new(child_id, hosts));
    let service = root.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder().add_service(m::matching_api_server::MatchingApiServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)).await.unwrap();
    });
    let domain = key.domain_id.to_string();
    let queue = root.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let worker = root.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "remote-query-worker".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !queue.queries.has_waiters() { tokio::task::yield_now().await; }
    }).await.unwrap();
    let caller = child.clone();
    let query_domain = domain.clone();
    let query = tokio::spawn(async move {
        let mut request = Request::new(m::QueryWorkflowRequest {
            domain_id: query_domain, task_list: Some(p::TaskList { name: child_name, kind: 1, ..Default::default() }),
            request: Some(p::QueryWorkflowRequest { domain: "domain".into(), workflow_execution: execution(),
                query: Some(p::WorkflowQuery { query_type: "ping".into(), query_args: None }), ..Default::default() }), ..Default::default()
        });
        request.set_timeout(Duration::from_secs(3));
        caller.query_workflow(request).await.unwrap().into_inner()
    });
    let delivered = tokio::time::timeout(Duration::from_secs(2), poll).await.unwrap().unwrap();
    assert_eq!(delivered.query.unwrap().query_type, "ping");
    let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(&delivered.task_token).unwrap();
    assert_eq!(token.task_list, "queue");
    root.respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
        domain_id: domain, task_list: list(), task_id: token.task_id,
        request: Some(p::RespondQueryTaskCompletedRequest { result: Some(p::WorkflowQueryResult {
            result_type: p::QueryResultType::Answered as i32, answer: Some(p::Payload { data: b"pong".to_vec() }), error_message: String::new(),
        }), ..Default::default() }),
    })).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), query).await.unwrap().unwrap();
    assert_eq!(result.query_result.unwrap().data, b"pong");
    child.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    root.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    server.abort();
}

#[tokio::test]
async fn matching_rpc_empty_polls_reserve_tailroom_and_read_live_expiration() {
    let (_directory, _store, matching, key) = fixture();
    for task_type in [0, 1] {
        let domain = key.domain_id.to_string();
        if task_type == 0 {
            let mut request = Request::new(m::PollForDecisionTaskRequest { domain_id: domain,
                request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default() });
            request.set_timeout(Duration::from_secs(1));
            let response = tokio::time::timeout(Duration::from_millis(500), matching.poll_for_decision_task(request)).await
                .expect("empty decision poll returns before the parent deadline").unwrap().into_inner();
            assert!(response.task_token.is_empty());
        } else {
            let mut request = Request::new(m::PollForActivityTaskRequest { domain_id: domain,
                request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }), ..Default::default() });
            request.set_timeout(Duration::from_secs(1));
            let response = tokio::time::timeout(Duration::from_millis(500), matching.poll_for_activity_task(request)).await
                .expect("empty activity poll returns before the parent deadline").unwrap().into_inner();
            assert!(response.task_token.is_empty());
        }
    }
    // The same loaded manager reads the updated property on its next poll.
    scaler_config_value(&matching, "matching.longPollExpirationInterval", br#""1ms""#);
    let request = Request::new(m::PollForDecisionTaskRequest { domain_id: key.domain_id.to_string(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default() });
    assert!(tokio::time::timeout(Duration::from_millis(500), matching.poll_for_decision_task(request)).await
        .expect("live expiration applies without reloading the manager").unwrap().into_inner().task_token.is_empty());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_in_memory_empty_polls_reserve_tailroom() {
    let (_directory, _store, durable, key) = fixture();
    let matching = Matching::new(durable.frontend.state.clone());
    let mut request = Request::new(m::PollForDecisionTaskRequest { domain_id: key.domain_id.to_string(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default() });
    request.set_timeout(Duration::from_secs(1));
    assert!(tokio::time::timeout(Duration::from_millis(500), matching.poll_for_decision_task(request)).await
        .expect("in-memory decision poll preserves deadline tailroom").unwrap().into_inner().task_token.is_empty());
    let mut request = Request::new(m::PollForActivityTaskRequest { domain_id: key.domain_id.to_string(),
        request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }), ..Default::default() });
    request.set_timeout(Duration::from_secs(1));
    assert!(tokio::time::timeout(Duration::from_millis(500), matching.poll_for_activity_task(request)).await
        .expect("in-memory activity poll preserves deadline tailroom").unwrap().into_inner().task_token.is_empty());
}

#[tokio::test]
async fn matching_rpc_child_poll_receives_parent_query_and_preserves_completion_partition() {
    let peer = start_fake_history(mutable_state()).await;
    let (_directory, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let root = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let worker = matching.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "child-query-poll".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: Some(p::TaskList {
            name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default()
        }), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root.queries.has_waiters() { tokio::task::yield_now().await; }
    }).await.expect("forwarded parent poll must wait for parent queries as well as backlog");
    let caller = matching.clone();
    let query_domain = domain.clone();
    let query = tokio::spawn(async move { caller.query_workflow(Request::new(m::QueryWorkflowRequest {
        domain_id: query_domain, task_list: list(),
        request: Some(p::QueryWorkflowRequest { domain: "domain".into(), workflow_execution: execution(),
            query: Some(p::WorkflowQuery { query_type: "ping".into(), query_args: None }), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner() });
    let response = tokio::time::timeout(Duration::from_secs(2), poll).await.unwrap().unwrap();
    assert_eq!(response.query.unwrap().query_type, "ping");
    let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(&response.task_token).unwrap();
    assert_eq!(token.task_list, "queue");
    matching.respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
        domain_id: domain, task_list: list(), task_id: token.task_id,
        request: Some(p::RespondQueryTaskCompletedRequest { result: Some(p::WorkflowQueryResult {
            result_type: p::QueryResultType::Answered as i32, answer: Some(p::Payload { data: b"pong".to_vec() }), error_message: String::new(),
        }), ..Default::default() }),
    })).await.unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), query).await.unwrap().unwrap().query_result.unwrap().data, b"pong");
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_local_parent_poll_applies_its_own_expiration_and_tailroom() {
    let (_directory, _store, matching, key) = fixture();
    let child_list = Some(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() });
    let mut request = Request::new(m::PollForDecisionTaskRequest { domain_id: key.domain_id.to_string(),
        request: Some(p::PollForDecisionTaskRequest { task_list: child_list.clone(), ..Default::default() }), ..Default::default() });
    // The child spends one second of tailroom and the parent spends another,
    // just as two Go Matching handlers traversed through the RPC client do.
    request.set_timeout(Duration::from_secs(2));
    assert!(tokio::time::timeout(Duration::from_millis(500), matching.poll_for_decision_task(request)).await
        .expect("each parent handler reserves its own tailroom").unwrap().into_inner().task_token.is_empty());
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.longPollExpirationInterval", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#""1ms""#.to_vec())),
        filters: vec![cadence_api::admin::DynamicConfigFilter { name: "taskListName".into(), value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#""queue""#.to_vec())) }],
    }]);
    let request = Request::new(m::PollForActivityTaskRequest { domain_id: key.domain_id.to_string(),
        request: Some(p::PollForActivityTaskRequest { task_list: child_list, ..Default::default() }), ..Default::default() });
    assert!(tokio::time::timeout(Duration::from_millis(500), matching.poll_for_activity_task(request)).await
        .expect("root-specific expiration applies to the forwarded activity poll").unwrap().into_inner().task_token.is_empty());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_live_rps_override_matches_go_precedence() {
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    // These are the pinned Go TestTaskListUsesOverrideRPS cases. Each poll
    // receives a fresh limiter so its prior minimum report cannot mask changes.
    for (override_rate, worker, expected) in [
        (0.0, Some(100.0), 100.0),
        (-1.0, Some(100.0), 100.0),
        (200.0, Some(100.0), 200.0),
        (25.0, Some(100.0), 25.0),
        (0.0, None, crate::matching_limiter::TASK_DISPATCH_RPS),
        (150.0, None, 150.0),
        (0.0, Some(-1.0), crate::matching_limiter::TASK_DISPATCH_RPS),
        (0.0, Some(0.0), 0.0),
    ] {
        matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.overrideTaskListRps", vec![cadence_api::admin::DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, override_rate.to_string().into_bytes())), filters: vec![],
        }]);
        for task_type in [0, 1] {
            let limiter = crate::matching_limiter::TaskListLimiter::with_defaults();
            assert_eq!(matching.poll_rate(&domain, &key.name, task_type, worker, &limiter), expected);
            if expected != crate::matching_limiter::TASK_DISPATCH_RPS {
                assert_eq!(limiter.limit(), expected);
            }
        }
    }
}

#[tokio::test]
async fn matching_rpc_report_limit_does_not_hold_the_admin_overlay() {
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    // The owner's live partition provider reads the same non-reentrant overlay
    // `poll_rate` reads the override from. Go's `ReportLimit` consults it while
    // applying an accepted report, so reporting under the overlay would
    // re-enter the mutex and hang the poll (and with it a single-threaded
    // runtime's timers). This provider fails loudly instead of deadlocking.
    let overlay = matching.frontend.state.admin.dynamic_config.clone();
    let limiter = crate::matching_limiter::TaskListLimiter::new(
        {
            let clock = crate::matching_limiter::MonotonicClock::new();
            Arc::new(move || clock.now())
        },
        crate::matching_limiter::TASK_DISPATCH_RPS,
        crate::matching_limiter::TASK_DISPATCH_RPS_TTL_NANOS,
        1,
        Arc::new(move || {
            overlay
                .try_lock()
                .expect("poll_rate must release the admin overlay before reporting")
                .property_values("matching.numTasklistReadPartitions")
                .len() as i64
                + 2
        }),
    );
    // Override path first: 200 is below the default, so it is accepted and the
    // live partition provider runs while the overlay is free.
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.overrideTaskListRps", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"200".to_vec())), filters: vec![],
    }]);
    assert_eq!(matching.poll_rate(&domain, &key.name, 1, Some(25.0), &limiter), 200.0);
    // Two read partitions, so the accepted report halves the backing limit.
    assert_eq!(limiter.partitions(), 2);
    assert_eq!(limiter.limit(), 100.0);
    // Cleared override falls back to the worker-reported rate, which is lower
    // than the accepted report, so this path also consults the provider.
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.overrideTaskListRps", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"0".to_vec())), filters: vec![],
    }]);
    assert_eq!(matching.poll_rate(&domain, &key.name, 1, Some(25.0), &limiter), 25.0);
    assert_eq!(limiter.limit(), 12.5);
}

#[tokio::test]
async fn matching_rpc_live_reader_limits_build_fresh_owners() {
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    let list = |name: &str| p::TaskList {
        name: name.into(),
        kind: 1,
        ..Default::default()
    };
    // Go's property defaults: a rate below the minimum burst still allows one
    // token, and the reader keeps the Go batch/range/timeout defaults.
    let default_owner = matching
        .durable_queue(&domain, &list(&key.name), 0)
        .await
        .unwrap();
    default_owner.limiter.report_limit(10.0);
    assert_eq!(default_owner.limiter.burst(), 10);

    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .unwrap()
        .update(
            false,
            "matching.minTaskThrottlingBurstSize",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    b"3000".to_vec(),
                )),
                filters: vec![],
            }],
        );
    // Go reads `MinTaskThrottlingBurstSize` when the limiter is constructed, so
    // only owners created afterwards pick it up; the existing owner is unchanged.
    let configured = matching
        .durable_queue(&domain, &list("/__cadence_sys/queue/1"), 0)
        .await
        .unwrap();
    configured.limiter.report_limit(10.0);
    assert_eq!(configured.limiter.burst(), 3000);
    assert_eq!(default_owner.limiter.burst(), 10);
}

#[tokio::test]
async fn matching_rpc_poll_auto_config_hint_matches_go_scoping() {
    use cadence_api::{
        admin::{DynamicConfigFilter, DynamicConfigValue},
        enums::EncodingType,
        types::DataBlob,
    };
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    let json_blob =
        |value: serde_json::Value| Some(DataBlob::new(EncodingType::JSON, serde_json::to_vec(&value).unwrap()));
    // Keep the empty long polls short.
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .unwrap()
        .update(false, "matching.longPollExpirationInterval", vec![DynamicConfigValue {
            value: json_blob(serde_json::json!("5ms")),
            filters: vec![],
        }]);
    let empty_activity = || async {
        matching
            .poll_for_activity_task(Request::new(m::PollForActivityTaskRequest {
                domain_id: domain.clone(),
                request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
    };
    // Go installs an auto-config hint on every poll, enabled by
    // `matching.enableClientAutoConfig` (default false).
    let first = empty_activity().await;
    assert!(first.task_token.is_empty());
    let hint = first.auto_config_hint.expect("every poll carries the auto-config hint");
    assert!(!hint.enable_auto_config);
    // Enable it only for activity reads.
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .unwrap()
        .update(false, "matching.enableClientAutoConfig", vec![DynamicConfigValue {
            value: json_blob(serde_json::json!(true)),
            filters: vec![DynamicConfigFilter { name: "taskType".into(), value: json_blob(serde_json::json!(1)) }],
        }]);
    // Go's empty-activity branch passes the decision task type, so the
    // activity-scoped value does not apply to an empty activity poll.
    assert!(!empty_activity().await.auto_config_hint.unwrap().enable_auto_config);
    let decision = matching
        .poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain.clone(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(decision.task_token.is_empty());
    assert!(!decision.auto_config_hint.unwrap().enable_auto_config);
    // A delivered task is stamped by the matcher with the manager's own type.
    assert!(matching.auto_config_hint(&domain, "queue", 1, std::time::Instant::now()).enable_auto_config);
    assert!(!matching.auto_config_hint(&domain, "queue", 0, std::time::Instant::now()).enable_auto_config);
}
