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

#[tokio::test]
async fn engine_partition_owners_and_order_match_executable_go_handler_oracle() {
    use crate::matching_membership::Membership;
    use cadence_common::membership::Host;
    let Ok(path) = std::env::var("CADENCE_MATCHING_OWNER_ORACLE") else {
        // Ordinary unit runs need not invoke Go; the dedicated CI oracle step sets this.
        return;
    };
    let oracle: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let (_dir, _store, matching, _key) = fixture();
    let membership = Membership::new("one", vec![]);
    let matching = matching.with_membership(membership.clone());
    let set_count = || {
        matching.frontend.state.admin.dynamic_config.lock().unwrap().update(
            false,
            "matching.numTasklistWritePartitions",
            vec![quota_config(3, None)],
        )
    };
    set_count();
    for scenario in oracle["scenarios"].as_array().unwrap() {
        let hosts = scenario["hosts"].as_array().unwrap().iter().map(|host| Host {
            address: host["address"].as_str().unwrap().into(),
            identity: host["identity"].as_str().unwrap().into(),
            grpc_address: host["grpc_address"].as_str().unwrap().into(),
        }).collect();
        membership.replace(hosts);
        for (field, name) in [("root", "queue"), ("child", "/__cadence_sys/queue/9"), ("empty", "")] {
            let response = matching.list_task_list_partitions(Request::new(m::ListTaskListPartitionsRequest {
                domain: "domain".into(), task_list: Some(p::TaskList { name: name.into(), ..Default::default() }),
            })).await.unwrap().into_inner();
            let as_json = |items: &[p::TaskListPartitionMetadata]| -> serde_json::Value {
                serde_json::Value::Array(items.iter().map(|item| serde_json::json!({"key":item.key,"owner":item.owner_host_name})).collect())
            };
            let expected = &scenario[field];
            assert_eq!(as_json(&response.activity_task_list_partitions), expected["activity"], "scenario={} request={field} activity", scenario["name"]);
            assert_eq!(as_json(&response.decision_task_list_partitions), expected["decision"], "scenario={} request={field} decision", scenario["name"]);
        }
    }
}

fn quota_config(value: i64, domain: Option<&str>) -> cadence_api::admin::DynamicConfigValue {
    use cadence_api::{admin::{DynamicConfigFilter, DynamicConfigValue}, enums::EncodingType, types::DataBlob};
    let blob = |value: serde_json::Value| Some(DataBlob::new(EncodingType::JSON, serde_json::to_vec(&value).unwrap()));
    DynamicConfigValue {
        value: blob(serde_json::json!(value)),
        filters: domain.into_iter().map(|name| DynamicConfigFilter {
            name: "domainName".into(),
            value: blob(serde_json::json!(name)),
        }).collect(),
    }
}

#[tokio::test]
async fn matching_long_polls_reject_missing_transport_deadlines_with_bad_request() {
    let matching = Matching::new(crate::handlers::AppState::default());
    let decision = matching.poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest::default())).await.unwrap_err();
    assert_eq!(decision.code(), tonic::Code::InvalidArgument);
    assert_eq!(decision.message(), "Context timeout is not set.");
    let activity = matching.poll_for_activity_task(Request::new(m::PollForActivityTaskRequest::default())).await.unwrap_err();
    assert_eq!(activity.code(), tonic::Code::InvalidArgument);
    assert_eq!(activity.message(), "Context timeout is not set.");
}

#[tokio::test]
async fn matching_long_polls_with_nearly_expired_deadline_return_internal_like_go() {
    let matching = Matching::new(crate::handlers::AppState::default());
    let mut decision = Request::new(m::PollForDecisionTaskRequest::default());
    decision.metadata_mut().insert("grpc-timeout", "5m".parse().unwrap());
    let error = matching.poll_for_decision_task(decision).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument); // missing request precedes, as before
    assert!(crate::grpc_matching::check_poll_context(Some(tokio::time::Instant::now())).is_err());
    let status = crate::grpc_matching::check_poll_context(Some(tokio::time::Instant::now())).unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "context deadline exceeded");
    assert!(crate::grpc_matching::check_poll_context(Some(tokio::time::Instant::now() + std::time::Duration::from_secs(1))).is_ok());
}

#[tokio::test]
async fn matching_rpc_quotas_classify_shared_protocol_handlers_and_preserve_count_only_calls() {
    let state = crate::handlers::AppState::default();
    let domain_id = Uuid::from_u128(9001);
    state.domains.create(
        cadence_api::types::DomainInfo { name: "quota-domain".into(), uuid: domain_id.to_string(), ..Default::default() },
        Default::default(), Default::default(), false,
    ).unwrap();
    {
        let mut config = state.admin.dynamic_config.lock().unwrap();
        config.update(false, "matching.rps", vec![quota_config(20, None)]);
        config.update(false, "matching.domainrps", vec![quota_config(1, Some("quota-domain"))]);
        config.update(false, "matching.workerrps", vec![quota_config(20, None)]);
        config.update(false, "matching.domainworkerrps", vec![quota_config(1, Some("quota-domain"))]);
    }
    let matching = Matching::new(state);

    // `GetTaskListsByDomain` is a user RPC and its domain filter uses the
    // request's name. The second call is rejected by only the domain bucket.
    matching.get_task_lists_by_domain(Request::new(m::GetTaskListsByDomainRequest {
        domain: "quota-domain".into(),
    })).await.unwrap();
    let user_error = matching.get_task_lists_by_domain(Request::new(m::GetTaskListsByDomainRequest {
        domain: "quota-domain".into(),
    })).await.unwrap_err();
    assert_eq!(user_error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(user_error.message(), "Matching host rps exceeded");
    assert!(!user_error.details().is_empty(), "Go-compatible ServiceBusyError detail is required");

    // Go counts these worker control calls but ignores a denied decision.
    let cancel = || Request::new(m::CancelOutstandingPollRequest {
        domain_id: domain_id.to_string(),
        task_list_type: 1,
        task_list: Some(p::TaskList { name: "queue".into(), ..Default::default() }),
        poller_id: "poller".into(),
    });
    matching.cancel_outstanding_poll(cancel()).await.unwrap();
    matching.cancel_outstanding_poll(cancel()).await.unwrap();

    // Poll is a rejecting worker RPC and shares the same class/domain bucket.
    let worker_error = matching.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain_id.to_string(),
        request: None,
        ..Default::default()
    }))).await.unwrap_err();
    assert_eq!(worker_error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(worker_error.message(), "Matching host rps exceeded");
}

struct FailingShardOwnerLookup;
impl crate::matching_shard_routing::ShardOwnerLookup for FailingShardOwnerLookup {
    fn get_shard_owner(
        &self,
        _shard_key: String,
        _deadline: tokio::time::Instant,
    ) -> crate::matching_shard_distributor::BoxFuture<
        Result<crate::matching_shard_distributor::ShardOwner, String>,
    > {
        Box::pin(async { Err("spectator unavailable".into()) })
    }
}

fn fixture() -> (
    tempfile::TempDir,
    Arc<SqliteTaskStore>,
    Matching,
    TaskListKey,
) {
    fixture_with_history("http://127.0.0.1:1")
}
pub(super) fn fixture_with_history(
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
pub(super) fn execution() -> Option<p::WorkflowExecution> {
    Some(p::WorkflowExecution {
        workflow_id: "remote-workflow".into(),
        run_id: Uuid::from_u128(2).to_string(),
    })
}

#[tokio::test]
async fn matching_remote_routing_propagates_spectator_failure_without_ring_fallback() {
    use cadence_common::membership::Host;

    let (_dir, _store, matching, _key) = fixture();
    let router = crate::matching_shard_routing::MatchingShardRouter::new(
        crate::matching_membership::Membership::new(
            "self",
            vec![Host {
                address: "self:7935".into(),
                identity: "self".into(),
                grpc_address: "self:7835".into(),
            }],
        ),
        Some(Arc::new(FailingShardOwnerLookup)),
        Arc::new(|| 100),
        Arc::new(|| false),
    );
    let matching = matching.with_shard_router(router);
    let error = matching
        .remote_channel_for_task_list("eligible-task-list")
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("spectator unavailable"));
}

#[tokio::test]
async fn matching_remote_adds_and_query_reach_go_yarpc_through_production_handlers() {
    let Ok(address) = std::env::var("CADENCE_MATCHING_GO_PEER") else {
        return;
    };
    let (_dir, store, matching, key) = fixture();
    let child_name = "/__cadence_sys/queue/1";
    let membership = membership_with_parent_remote(child_name, "queue", &address);
    let matching = matching.with_membership(membership);

    let mut decision = Request::new(m::AddDecisionTaskRequest {
        domain_id: key.domain_id.to_string(),
        workflow_execution: execution(),
        task_list: Some(p::TaskList {
            name: child_name.into(),
            kind: 0,
            ..Default::default()
        }),
        schedule_id: 31,
        ..Default::default()
    });
    decision.set_timeout(Duration::from_secs(8));
    matching.add_decision_task(decision).await.unwrap();
    assert_eq!(
        store
            .task_list_size(
                &TaskListKey {
                    domain_id: key.domain_id,
                    name: child_name.into(),
                    task_type: 0,
                },
                0
            )
            .unwrap(),
        0,
        "successful remote sync match must not leave a local queued copy");

    let mut activity = Request::new(m::AddActivityTaskRequest {
        domain_id: key.domain_id.to_string(),
        source_domain_id: key.domain_id.to_string(),
        workflow_execution: execution(),
        task_list: Some(p::TaskList {
            name: child_name.into(),
            kind: 1,
            ..Default::default()
        }),
        schedule_id: 32,
        ..Default::default()
    });
    activity.set_timeout(Duration::from_secs(8));
    matching.add_activity_task(activity).await.unwrap();
    assert_eq!(
        store
            .task_list_size(
                &TaskListKey {
                    domain_id: key.domain_id,
                    name: child_name.into(),
                    task_type: 1,
                },
                0
            )
            .unwrap(),
        0,
        "successful remote activity sync match must not leave a local queued copy");

    let mut query = Request::new(m::QueryWorkflowRequest {
        domain_id: key.domain_id.to_string(),
        request: Some(p::QueryWorkflowRequest::default()),
        task_list: Some(p::TaskList {
            name: child_name.into(),
            kind: 0,
            ..Default::default()
        }),
        ..Default::default()
    });
    query.set_timeout(Duration::from_secs(8));
    let response = matching.query_workflow(query).await.unwrap().into_inner();
    assert_eq!(response.query_result.unwrap().data, b"go-yarpc-query");
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
}

pub(super) fn membership_with_parent_remote(
    child: &str,
    parent: &str,
    remote_address: &str,
) -> crate::matching_membership::Membership {
    use cadence_common::membership::Host;
    for candidate in 0..100_000 {
        let membership = crate::matching_membership::Membership::new(
            format!("rust-matching-{candidate}"),
            vec![
                Host {
                    address: format!("rust-{candidate}:7935"),
                    identity: format!("rust-matching-{candidate}"),
                    grpc_address: format!("rust-{candidate}:7835"),
                },
                Host {
                    address: remote_address.into(),
                    identity: "go-matching".into(),
                    grpc_address: remote_address.into(),
                },
            ],
        );
        if membership
            .owner(child)
            .is_ok_and(|host| host.identity() == format!("rust-matching-{candidate}"))
            && membership.owner(parent).is_ok_and(|host| host.identity() == "go-matching")
        {
            return membership;
        }
    }
    panic!("could not construct a matching membership ring with local child and remote parent");
}

#[tokio::test]
async fn durable_queue_runs_ownership_preflight_before_registry_and_propagates_transient_error() {
    use cadence_common::membership::Host;
    use std::sync::Mutex;

    let (_dir, _store, matching, key) = fixture();
    let membership = crate::matching_membership::Membership::new(
        "self",
        vec![Host {
            address: "self:7935".into(),
            identity: "self".into(),
            grpc_address: "self:7835".into(),
        }],
    );
    let matching = matching.with_membership(membership);
    let list = p::TaskList {
        name: key.name.clone(),
        kind: 1,
        ..Default::default()
    };
    matching
        .durable_queue(&key.domain_id.to_string(), &list, 0)
        .await
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let check = {
        let seen = seen.clone();
        Arc::new(move |name: String, cached: bool| {
            seen.lock().unwrap().push((name, cached));
            Box::pin(async { Err(tonic::Status::unavailable("SD heartbeat failed")) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), tonic::Status>> + Send>,
                >
        })
    };
    let matching = matching.with_task_list_ownership(check);
    let error = matching
        .durable_queue(&key.domain_id.to_string(), &list, 0)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(*seen.lock().unwrap(), vec![(key.name.clone(), true)]);
    assert!(matching
        .queues
        .as_ref()
        .unwrap()
        .contains_cached(&key));
}

#[tokio::test]
async fn durable_queue_preserves_typed_not_owned_error_from_shutdown_policy() {
    use cadence_common::membership::Host;

    let (_dir, _store, matching, key) = fixture();
    let membership = crate::matching_membership::Membership::new(
        "self",
        vec![Host {
            address: "self:7935".into(),
            identity: "self".into(),
            grpc_address: "self:7835".into(),
        }],
    );
    let error_membership = membership.clone();
    let check: crate::grpc_matching::TaskListOwnershipCheck =
        Arc::new(move |name: String, _cached: bool| {
            let error_membership = error_membership.clone();
            Box::pin(async move {
                Err(error_membership.task_list_not_owned_by(&name, "not known"))
            })
                as std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = Result<(), tonic::Status>> + Send,
                    >,
                >
        });
    let matching = matching
        .with_membership(membership)
        .with_task_list_ownership(check);
    let list = p::TaskList {
        name: key.name.clone(),
        kind: 1,
        ..Default::default()
    };
    let error = matching
        .durable_queue(&key.domain_id.to_string(), &list, 0)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), tonic::Code::Aborted);
    use prost::Message;
    #[derive(prost::Message)]
    struct Details {
        #[prost(message, repeated, tag = "3")]
        errors: Vec<prost_types::Any>,
    }
    let detail = Details::decode(error.details()).unwrap().errors.remove(0);
    assert_eq!(
        detail.type_url,
        "type.googleapis.com/uber.cadence.shared.v1.TaskListNotOwnedByHostError"
    );
    assert!(!matching
        .queues
        .as_ref()
        .unwrap()
        .contains_cached(&key));
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
            let response = matching.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
                domain_id: key.domain_id.to_string(), request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
            }))).await.unwrap().into_inner();
            assert!(response.task_token.is_empty());
        } else {
            let response = matching.poll_for_activity_task(timed_poll(Request::new(m::PollForActivityTaskRequest {
                domain_id: key.domain_id.to_string(), request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
            }))).await.unwrap().into_inner();
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
        // Go errShutdown reaches handleErr's default branch: InternalServiceError.
        assert_eq!(matching.add_decision_task(request()).await.unwrap_err().code(), tonic::Code::Internal);
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
    let queue = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "poller".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: list(),
                    ..Default::default()
                }),
                ..Default::default()
            })))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            // The poll is cancellable only once its lifecycle is in the queue's poller
            // manager (Go StartPoll); a cancel before that is a no-op, as in Go.
            if matching.polls.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
                continue;
            }
            if queue.pollers.lock().unwrap().contains_poll("poller") {
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
    // Go returns an empty poll response (nil error) for a cancelled poll.
    let response = tokio::time::timeout(Duration::from_secs(10), poll)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_inner();
    assert!(response.task_token.is_empty());
    assert!(response.auto_config_hint.is_some());
    assert!(matching.polls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn matching_rpc_cancel_without_matching_poll_returns_ok() {
    let (_dir, _store, matching, key) = fixture();
    // Go CancelPoller returns nil whether or not a poll matched.
    matching
        .cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
            domain_id: key.domain_id.to_string(),
            task_list: list(),
            task_list_type: 1,
            poller_id: "nobody".into(),
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn matching_rpc_duplicate_poller_id_replaces_entry_without_cancelling_first() {
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    let poll_key = || PollKey {
        domain: domain.clone(),
        task_list: list().unwrap().name,
        task_type: 1,
        poller: "dup".into(),
    };
    let queue = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let begin = |guard: &PollGuard| crate::matching_dispatch::PollLifecycle {
        manager: queue.pollers.clone(), id: guard.key.poller.clone(), cancel: Some(guard.cancel.clone()),
        info: cadence_common::poller::Info { identity: "worker".into(), rate_per_second: 1.0, isolation_group: String::new() },
        forwarded: false, metric_context: None, metrics: crate::matching_metrics::MatchingMetrics::default(),
    }.begin();
    let first = matching.register_poll(poll_key());
    let first_wait = begin(&first);
    let second = matching.register_poll(poll_key());
    let second_wait = begin(&second);
    // Go poller.Manager.StartPoll overwrites the entry and does not cancel the earlier poll.
    assert!(tokio::time::timeout(Duration::from_millis(50), first.cancel.notified())
        .await
        .is_err());
    matching
        .cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
            domain_id: domain,
            task_list: list(),
            task_list_type: 1,
            poller_id: "dup".into(),
        }))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), second.cancel.notified())
        .await
        .expect("cancel targets the newest poll with the poller ID");
    // Go EndPoll deletes unconditionally, so finishing the first poll drops the second's entry.
    drop(first);
    drop(first_wait);
    assert!(matching.polls.lock().unwrap().is_empty());
    queue.pollers.lock().unwrap().cancel("dup");
    assert!(tokio::time::timeout(Duration::from_millis(30), second.cancel.notified()).await.is_err());
    drop(second_wait);
    drop(second);
}

#[tokio::test]
async fn matching_rpc_cancel_outstanding_poll_checks_task_list_ownership() {
    use std::{pin::Pin, sync::Arc as StdArc};

    let (_dir, _store, matching, key) = fixture();
    let check = StdArc::new(move |_name: String, _cached: bool| {
        Box::pin(async { Err(tonic::Status::unavailable("not owner")) })
            as Pin<Box<dyn std::future::Future<Output = Result<(), tonic::Status>> + Send>>
    });
    let matching = matching.with_task_list_ownership(check);
    // Go getOrCreateTaskListManager runs the ownership check before CancelPoller.
    let error = matching
        .cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
            domain_id: key.domain_id.to_string(),
            task_list: list(),
            task_list_type: 1,
            poller_id: "anyone".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.message(), "not owner");
}

#[tokio::test]
async fn matching_rpc_decision_poll_preserves_isolation_group_history() {
    let (_dir, _store, matching, key) = fixture();
    let queue = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "group-poller".into(), isolation_group: "zone-decision".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), identity: "decision-worker".into(), ..Default::default() }),
            ..Default::default()
        }))).await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
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
    let response = poll.await.unwrap().unwrap().into_inner();
    assert!(response.task_token.is_empty());
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
            .poll_for_activity_task(timed_poll(Request::new(m::PollForActivityTaskRequest {
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
            })))
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
    let active = tokio::time::timeout(Duration::from_secs(10), async {
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
    let response = poll.await.unwrap().unwrap().into_inner();
    assert!(response.task_token.is_empty());
    assert!(response.auto_config_hint.is_some());
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

/// Matching without durable queues: DescribeTaskList forwards to the frontend
/// poller history, which is keyed by domain, task list, identity and task type.
fn non_durable_fixture() -> (Matching, String) {
    let state = crate::handlers::AppState::default();
    let domain = Uuid::from_u128(1).to_string();
    state
        .domains
        .create(
            cadence_api::types::DomainInfo {
                name: "domain".into(),
                uuid: domain.clone(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    (Matching::new(state), domain)
}

fn non_durable_describe(
    domain: &str,
    task_list_type: i32,
    include_task_list_status: bool,
) -> Request<m::DescribeTaskListRequest> {
    Request::new(m::DescribeTaskListRequest {
        domain_id: domain.into(),
        request: Some(p::DescribeTaskListRequest {
            task_list: list(),
            task_list_type,
            include_task_list_status,
            ..Default::default()
        }),
    })
}

#[tokio::test]
async fn matching_rpc_non_durable_describe_lists_pollers_per_task_list_and_type() {
    let (matching, domain) = non_durable_fixture();
    let decision_poll = |identity: &str, task_list: &str| {
        let mut request = Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain.clone(),
            request: Some(p::PollForDecisionTaskRequest {
                task_list: Some(p::TaskList {
                    name: task_list.into(),
                    kind: 1,
                    ..Default::default()
                }),
                identity: identity.into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        request.set_timeout(Duration::from_secs(1));
        request
    };
    // The same identity polling twice must appear once, like Go's identity-keyed history.
    for request in [
        decision_poll("decision-a", "queue"),
        decision_poll("decision-a", "queue"),
        decision_poll("decision-other", "other"),
    ] {
        assert!(matching.poll_for_decision_task(request).await.unwrap().into_inner().task_token.is_empty());
    }
    let mut activity = Request::new(m::PollForActivityTaskRequest {
        domain_id: domain.clone(),
        poller_id: "activity-poller".into(),
        request: Some(p::PollForActivityTaskRequest {
            task_list: list(),
            identity: "activity-a".into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    activity.set_timeout(Duration::from_secs(1));
    assert!(matching.poll_for_activity_task(activity).await.unwrap().into_inner().task_token.is_empty());

    let decision = matching
        .describe_task_list(non_durable_describe(&domain, 1, false))
        .await
        .unwrap()
        .into_inner();
    let identities: Vec<&str> = decision.pollers.iter().map(|poller| poller.identity.as_str()).collect();
    assert_eq!(identities, ["decision-a"]);
    assert_eq!(decision.task_list.unwrap().name, "queue");
    assert!(decision.task_list_status.is_none());
    assert!(decision.partition_config.is_none());

    let activity = matching
        .describe_task_list(non_durable_describe(&domain, 2, true))
        .await
        .unwrap()
        .into_inner();
    let identities: Vec<&str> = activity.pollers.iter().map(|poller| poller.identity.as_str()).collect();
    assert_eq!(identities, ["activity-a"]);
    // Go reports the ack-manager backlog; an untouched list has none.
    assert_eq!(activity.task_list_status.unwrap().backlog_count_hint, 0);
}

#[tokio::test]
async fn matching_rpc_non_durable_describe_reports_poller_rate() {
    let (matching, domain) = non_durable_fixture();
    let mut poll = Request::new(m::PollForActivityTaskRequest {
        domain_id: domain.clone(),
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
    });
    poll.set_timeout(Duration::from_secs(1));
    assert!(matching.poll_for_activity_task(poll).await.unwrap().into_inner().task_token.is_empty());
    let described = matching
        .describe_task_list(non_durable_describe(&domain, 2, false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(described.pollers[0].rate_per_second, 42.0);
}

#[tokio::test]
async fn matching_rpc_non_durable_describe_reports_isolation_group_metrics() {
    let (matching, domain) = non_durable_fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "system.allIsolationGroups", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, br#"["zone-a","zone-b"]"#.to_vec())), filters: vec![],
    }]);
    let mut poll = Request::new(m::PollForActivityTaskRequest {
        domain_id: domain.clone(),
        poller_id: "activity-poller".into(),
        isolation_group: "zone-a".into(),
        request: Some(p::PollForActivityTaskRequest {
            task_list: list(),
            identity: "sdk-worker".into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    poll.set_timeout(Duration::from_secs(1));
    assert!(matching.poll_for_activity_task(poll).await.unwrap().into_inner().task_token.is_empty());
    let status = matching
        .describe_task_list(non_durable_describe(&domain, 2, true))
        .await
        .unwrap()
        .into_inner()
        .task_list_status
        .unwrap();
    assert_eq!(status.isolation_group_metrics.len(), 2);
    assert_eq!(status.isolation_group_metrics["zone-a"].poller_count, 1);
    assert_eq!(status.isolation_group_metrics["zone-b"].poller_count, 0);
}

#[tokio::test]
#[ignore = "parity gap: non-durable status has no ack manager, so task_id_block and empty stay unset; Go derives both"]
async fn matching_rpc_non_durable_describe_reports_task_id_block_and_empty() {
    let (matching, domain) = non_durable_fixture();
    let status = matching
        .describe_task_list(non_durable_describe(&domain, 1, true))
        .await
        .unwrap()
        .into_inner()
        .task_list_status
        .unwrap();
    // Go: an untouched list has ackLevel == maxReadLevel, so Empty is true.
    assert!(status.empty);
    assert!(status.task_id_block.is_some());
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
pub(super) async fn start_fake_history(state: h::GetMutableStateResponse) -> String {
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
    let (_dir, _store, mut matching, _key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "system.enableDistributedSemaphore", vec![bool_config(true, Some("domain"))]);
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
    // A cached manager skips the flag check, exactly like Go.
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "system.enableDistributedSemaphore", vec![bool_config(false, Some("domain"))]);
    matching.membership = Some(crate::matching_membership::Membership::new("local", vec![cadence_common::membership::Host {
        address: "remote:7935".into(), identity: "remote".into(), grpc_address: "127.0.0.1:1".into(),
    }]));
    assert_eq!(acquire(&matching, "1:w:r9:1").await.unwrap().outcome, 2);
    for manager in matching.semaphores.all_managers() { matching.semaphores.unregister(&manager); }
    // An unloaded bucket must pass domain admission again.
    let disabled = acquire(&matching, "1:w:r9:1").await.unwrap_err();
    assert_eq!(disabled.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn matching_rpc_sql_semaphore_matches_go_unsupported_backend_error() {
    let (_dir, _store, mut matching, _key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "system.enableDistributedSemaphore", vec![bool_config(true, Some("domain"))]);
    matching.semaphores = crate::matching_semaphore::Registry::new(Arc::new(
        cadence_persistence::SqlUnsupportedSemaphorePersistence,
    ));
    let response = MatchingApi::acquire_semaphore(
        &matching,
        Request::new(m::AcquireSemaphoreRequest {
            domain_id: Uuid::from_u128(1).to_string(),
            semaphore_name: "sem".into(),
            bucket: 0,
            owner_id: "1:w:r1:1".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(response.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        response.message(),
        cadence_persistence::SqlUnsupportedSemaphorePersistence::MESSAGE
    );
}

fn semaphore_request(domain: &str, name: &str, bucket: i32, owner: &str) -> Request<m::AcquireSemaphoreRequest> {
    Request::new(m::AcquireSemaphoreRequest {
        domain_id: domain.into(),
        semaphore_name: name.into(),
        bucket,
        owner_id: owner.into(),
    })
}

// Go `ParseRequestOwner` wraps the decode error in a BadRequestError, and the text is what callers see.
#[tokio::test]
async fn matching_rpc_semaphore_owner_errors_match_go_text() {
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    let missing_prefix = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, "x"))
        .await
        .unwrap_err();
    assert_eq!(missing_prefix.code(), tonic::Code::InvalidArgument);
    assert_eq!(missing_prefix.message(), r#"invalid owner id: owner_id "x" has no length prefix"#);
    let non_canonical = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, "01:w:r:1"))
        .await
        .unwrap_err();
    assert_eq!(
        non_canonical.message(),
        r#"invalid owner id: owner_id "01:w:r:1" has a non-canonical length prefix "01""#
    );
    // Go decodes the owner before it validates the bucket, so a bad owner wins over a bad name.
    let owner_first = MatchingApi::acquire_semaphore(&matching, semaphore_request("", "", -1, "x"))
        .await
        .unwrap_err();
    assert!(owner_first.message().starts_with("invalid owner id: "));
}

// Go `NewIdentifier` runs after the owner decode and before any domain or ring lookup.
#[tokio::test]
async fn matching_rpc_semaphore_identifier_errors_match_go_text() {
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    let cases = [
        (semaphore_request("", "sem", 0, "1:w:r:1"), "domainID is required"),
        (semaphore_request(&domain, "", 0, "1:w:r:1"), "semaphoreName is required"),
        (semaphore_request(&domain, "sem", -1, "1:w:r:1"), "bucket must not be negative, got -1"),
    ];
    for (request, message) in cases {
        let error = MatchingApi::acquire_semaphore(&matching, request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), message);
    }
}

// Go `ErrSemaphoreDisabled` is a BadRequestError with this exact text.
#[tokio::test]
async fn matching_rpc_semaphore_disabled_message_matches_go() {
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    matching.frontend.state.executions.register_domain("domain", &domain);
    let error = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, "1:w:r:1"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), "distributed semaphore is not enabled for this domain");
}

// Go handler.go AcquireSemaphore: the host limiter runs before the engine, so a denied
// request answers ServiceBusyError and never reaches the semaphore code.
#[tokio::test]
async fn go_handler_acquire_semaphore_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9102);
    let matching = Matching::new(denied_after_first_state(domain_id));
    let req = || semaphore_request(&domain_id.to_string(), "sem", 0, "1:w:r:1");
    let _ = matching.acquire_semaphore(req()).await;
    assert_host_busy(matching.acquire_semaphore(req()).await.unwrap_err());
}

// Go engine.go getOrCreateSemaphoreManager: a cache miss resolves the domain by ID before the
// enabled flag, and the store's "Domain <id> does not exist." comes back as NotFound.
#[tokio::test]
async fn matching_rpc_semaphore_domain_not_found_matches_go() {
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(77).to_string();
    let error = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, "1:w:r:1"))
        .await
        .unwrap_err();
    assert_domain_not_found(error, &domain);
}

/// Decodes the `google.rpc.Status` details the way Go's yarpc client does.
#[derive(prost::Message)]
struct RpcStatusDetails {
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// Asserts Go's NotFound domain-miss shape: the message, plus an empty
/// `EntityNotExistsError` detail that decodes from its api.v1 type URL.
fn assert_domain_not_found(status: tonic::Status, domain: &str) {
    use prost::Message;
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), format!("Domain {domain} does not exist."));
    let detail = RpcStatusDetails::decode(status.details()).unwrap().details.remove(0);
    assert_eq!(
        detail.type_url,
        "type.googleapis.com/uber.cadence.api.v1.EntityNotExistsError"
    );
    assert_eq!(
        p::EntityNotExistsError::decode(detail.value.as_slice()).unwrap(),
        p::EntityNotExistsError::default()
    );
}

#[test]
fn domain_not_found_status_carries_entity_not_exists_detail() {
    let domain = Uuid::from_u128(78).to_string();
    let status = crate::grpc::domain_not_found(format!("Domain {domain} does not exist."));
    assert_domain_not_found(status, &domain);
}

// Go semaphore_manager.go grant checks the caller's context before its first write. An expired
// caller gets Internal "context deadline exceeded" and must not leave a slot claimed.
#[tokio::test]
async fn matching_rpc_semaphore_expired_deadline_claims_no_slot() {
    let (_dir, _store, matching, _key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "system.enableDistributedSemaphore", vec![bool_config(true, Some("domain"))]);
    let domain = Uuid::from_u128(1).to_string();
    matching.frontend.state.executions.register_domain("domain", &domain);
    matching.frontend.state.executions.create_semaphore("domain", "sem", 2, 2).unwrap();
    let mut expired = semaphore_request(&domain, "sem", 0, "1:w:r0:1");
    expired.metadata_mut().insert("grpc-timeout", "0n".parse().unwrap());
    let error = MatchingApi::acquire_semaphore(&matching, expired).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "context deadline exceeded");
    // Both slots are still free: two live owners are granted and a third is refused.
    for owner in ["1:w:r1:1", "1:w:r2:1"] {
        let granted = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, owner))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(granted.outcome, 1);
    }
    let refused = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, "1:w:r3:1"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(refused.outcome, 2);
}

// Go `log.CapturePanic` on every matching handler: a panic in the RPC body is logged and returned
// as an error whose message is Go's `panic object is not error: %#v` text, and the server keeps serving.
#[tokio::test]
async fn matching_rpc_handler_panic_is_captured_as_go_unknown_error() {
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    // The registry lookup is the first thing acquire_semaphore does with a valid request.
    matching.semaphores.poison_for_test();
    for _ in 0..2 {
        let status = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, "1:w:r1:1"))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unknown);
        assert!(
            status
                .message()
                .starts_with(r#"panic object is not error: "semaphore registry lock: PoisonError"#),
            "{}",
            status.message()
        );
    }
}

// Go `TestMembershipSubscriptionRecoversAfterPanic`: a panic in the membership loop is logged and ends
// the loop, and the shutdown join sees success rather than a panic or an error.
#[tokio::test]
async fn matching_rpc_membership_loop_panic_ends_loop_without_error() {
    use cadence_common::membership::Host;
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    matching.frontend.state.executions.register_domain("domain", &domain);
    matching.frontend.state.executions.create_semaphore("domain", "sem", 2, 2).unwrap();
    // Every bucket hashes to the other host, so the loop has to consult the ring for it.
    let membership = crate::matching_membership::Membership::new(
        "rust-matching-0",
        vec![Host {
            address: "rust-1:7935".into(),
            identity: "rust-matching-1".into(),
            grpc_address: "rust-1:7835".into(),
        }],
    );
    let matching = matching.with_membership(membership.clone());
    let identifier = crate::matching_semaphore::Identifier::new(&domain, "sem", 0).unwrap();
    matching.semaphores.get_or_create_with_idle_ttl(identifier, 60_000_000_000).await.unwrap();
    membership.poison_ring_for_test();
    let result = matching.maintain_membership_until(std::future::ready(())).await;
    assert!(result.is_ok(), "{result:?}");
}

// Go `TestShutDownNonOwnedSemaphoreManagersReportsAPanic`: a Stop that panics is contained. The bucket
// still leaves the registry, and the other bucket this host does not own still stops.
#[tokio::test]
async fn matching_rpc_membership_semaphore_stop_panic_is_contained() {
    use cadence_common::membership::Host;
    use crate::matching_semaphore::{Identifier, ManagerError};
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    matching.frontend.state.executions.register_domain("domain", &domain);
    matching.frontend.state.executions.create_semaphore("domain", "sem", 2, 2).unwrap();
    let membership = crate::matching_membership::Membership::new(
        "rust-matching-0",
        vec![Host {
            address: "rust-1:7935".into(),
            identity: "rust-matching-1".into(),
            grpc_address: "rust-1:7835".into(),
        }],
    );
    let matching = matching.with_membership(membership);
    let panicking = matching
        .semaphores
        .get_or_create_with_idle_ttl(Identifier::new(&domain, "sem", 0).unwrap(), 60_000_000_000)
        .await
        .unwrap();
    let healthy = matching
        .semaphores
        .get_or_create_with_idle_ttl(Identifier::new(&domain, "sem", 1).unwrap(), 60_000_000_000)
        .await
        .unwrap();
    panicking.poison_for_test();
    matching.maintain_membership_until(std::future::ready(())).await.unwrap();
    assert!(matching.semaphores.all_managers().is_empty(), "both unowned buckets leave the registry");
    assert!(matches!(
        healthy.acquire("1:w:r1:1", None).await,
        Err(ManagerError::NotReady)
    ));
}

// Go `errIfSemaphoreOwnershipLost` answers "not known" as the owner once the engine is shutting down.
#[tokio::test]
async fn matching_rpc_semaphore_rejects_new_buckets_while_shutting_down() {
    let (_dir, _store, matching, _key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(
        false,
        "system.enableDistributedSemaphore",
        vec![bool_config(true, Some("domain"))],
    );
    let domain = Uuid::from_u128(1).to_string();
    matching.frontend.state.executions.register_domain("domain", &domain);
    matching.frontend.state.executions.create_semaphore("domain", "sem", 2, 2).unwrap();
    matching.begin_shutdown();

    let error = MatchingApi::acquire_semaphore(
        &matching,
        semaphore_request(&domain, "sem", 0, "1:w:r0:1"),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(error.message().contains("OwnedBy: not known"), "{}", error.message());
}

// Go `AcquireSemaphore` turns any engine error after the caller's context ends, cancel or deadline, into InternalServiceError.
// Go handler.go:550 reports ctx.Err() ("context canceled"). The caller's cancellation reaches the handler as a request
// extension; a dropped wire RPC is never answered in either implementation, so only this in-process view is observable.
#[tokio::test]
async fn matching_rpc_semaphore_cancelled_context_is_internal_error() {
    let (_dir, _store, matching, _key) = fixture();
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "system.enableDistributedSemaphore", vec![bool_config(true, Some("domain"))]);
    let domain = Uuid::from_u128(1).to_string();
    matching.frontend.state.executions.register_domain("domain", &domain);
    matching.frontend.state.executions.create_semaphore("domain", "sem", 2, 2).unwrap();
    // The caller's context is already cancelled when the grant is reached.
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    cancel.send(true).unwrap();
    let mut gone = semaphore_request(&domain, "sem", 0, "1:w:r0:1");
    gone.extensions_mut().insert(crate::grpc_matching::CallerCancel(cancelled));
    let error = MatchingApi::acquire_semaphore(&matching, gone).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "context canceled");
    // Both slots are still free: two live owners are granted and a third is refused.
    for owner in ["1:w:r1:1", "1:w:r2:1"] {
        let granted = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, owner))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(granted.outcome, 1);
    }
    let refused = MatchingApi::acquire_semaphore(&matching, semaphore_request(&domain, "sem", 0, "1:w:r3:1"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(refused.outcome, 2);
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
async fn matching_rpc_partition_config_kind_gate_matches_go() {
    // Go `ToTaskListKind` (go/common/types/mapper/proto/api.go:4051): INVALID(0) and any
    // unknown value map to nil, which `TaskList.GetKind()` reads as Normal, so they are
    // accepted. NORMAL(1) is accepted. STICKY(2) and EPHEMERAL(3) are rejected.
    for (kind, accepted) in [(0, true), (1, true), (2, false), (3, false), (4, true), (-1, true)] {
        let (_dir, _store, matching, key) = fixture();
        let task_list = Some(cadence_proto::api::TaskList {
            name: "queue".into(),
            kind,
            ..Default::default()
        });
        let update = matching
            .update_task_list_partition_config(Request::new(
                m::UpdateTaskListPartitionConfigRequest {
                    domain_id: key.domain_id.to_string(),
                    task_list: task_list.clone(),
                    task_list_type: 1,
                    partition_config: Some(native_config(1, &[0, 1], &[0, 1])),
                },
            ))
            .await;
        let refresh = matching
            .refresh_task_list_partition_config(Request::new(
                m::RefreshTaskListPartitionConfigRequest {
                    domain_id: key.domain_id.to_string(),
                    task_list,
                    task_list_type: 1,
                    partition_config: None,
                },
            ))
            .await;
        if accepted {
            update.unwrap_or_else(|e| panic!("kind {kind}: Go accepts, Rust rejected: {e}"));
            refresh.unwrap_or_else(|e| panic!("kind {kind}: Go accepts, Rust rejected: {e}"));
        } else {
            let update = update.expect_err("Go rejects sticky/ephemeral kinds");
            assert_eq!(update.code(), tonic::Code::InvalidArgument, "kind {kind}");
            assert_eq!(update.message(), "Only normal tasklist's partition config can be updated.");
            let refresh = refresh.expect_err("Go rejects sticky/ephemeral kinds");
            assert_eq!(refresh.code(), tonic::Code::InvalidArgument, "kind {kind}");
            assert_eq!(refresh.message(), "Only normal tasklist's partition config can be updated.");
        }
        matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    }
}

#[tokio::test]
async fn matching_thrift_has_no_partition_config_procedure() {
    // Go's Thrift matching IDL (go/idls/thrift/matching.thrift) has no partition-config
    // procedure, and the Go TChannel client returns "Feature not supported on TChannel".
    // The kind gate is therefore gRPC-only; Thrift dispatch must reject both procedures
    // as unknown.
    let (_dir, _store, matching, _key) = fixture();
    let args = cadence_thrift::value::Struct::default();
    for procedure in [
        "MatchingService::UpdateTaskListPartitionConfig",
        "MatchingService::RefreshTaskListPartitionConfig",
    ] {
        let err = crate::thrift_matching::dispatch(
            &matching,
            procedure,
            &args,
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect_err("partition-config procedures are not in the Thrift IDL");
        assert!(err.starts_with("Unknown Matching procedure"), "{procedure}: {err}");
    }
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
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
    let poll = matching.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), poller_id: "late-backlog".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
        ..Default::default()
    }))).await.unwrap().into_inner();
    assert_eq!(poll.started_event_id, 72);
    add.await.unwrap().unwrap();
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}
// Go MustOffer's forwarded success finishes the child durable task, while
// the root's forwarded AddTask sync-matches without creating another row.
#[tokio::test]
async fn matching_rpc_two_partition_backlog_forward_finishes_child_without_parent_row() {
    let address = start_starting_history().await;
    let (_dir, store, matching, key) = fixture_with_history(&address);
    let child_list = p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() };
    let child = matching.durable_queue(&key.domain_id.to_string(), &child_list, 0).await.unwrap();
    let row = child.enqueue(cadence_persistence::matching_blob::TaskData {
        workflow_id: "backlog-workflow".into(), run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
        schedule_id: 81, ..Default::default()
    }, key.domain_id, 0).await.unwrap();
    // No child poller exists. A root poller arriving after persistence must
    // receive this task through the reader's forwarding path.
    assert_eq!(store.task_list_size(&TaskListKey { name: child_list.name.clone(), ..key.clone() }, 0).unwrap(), 1);
    let poll = tokio::time::timeout(Duration::from_secs(5), matching.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), poller_id: "root-backlog-worker".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
        ..Default::default()
    })))).await.unwrap().unwrap().into_inner();
    assert_eq!(poll.started_event_id, 82);
    tokio::time::timeout(Duration::from_secs(3), async {
        while child.queue.levels().1 < row.task_id { tokio::task::yield_now().await; }
    }).await.unwrap();
    assert_eq!(child.queue.levels().2, 0);
    assert_eq!(store.task_list_size(&key, 0).unwrap(), 0, "forwarded adds never persist at the root");
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
        worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain,
            poller_id: "sync-worker".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
            ..Default::default()
        }))).await.unwrap().into_inner()
    });
    tokio::time::timeout(Duration::from_secs(10), async {
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
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap().started_event_id, 32);
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
    let response = tokio::time::timeout(Duration::from_secs(10), matching.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), poller_id: "child-worker".into(),
        request: Some(p::PollForDecisionTaskRequest {
            task_list: Some(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() }),
            ..Default::default()
        }),
        ..Default::default()
    })))).await.expect("child forwards instead of blocking on its empty reader").unwrap().into_inner();
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
        worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "parent-worker".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
            ..Default::default()
        }))).await.unwrap().into_inner()
    });
    tokio::time::timeout(Duration::from_secs(10), async {
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
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap().started_event_id, 52);
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
        worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "cancel-forward".into(),
            request: Some(p::PollForDecisionTaskRequest {
                task_list: Some(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() }),
                identity: "cancel-forward".into(), ..Default::default()
            }), ..Default::default()
        }))).await
    });
    let root = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !root.pollers.lock().unwrap().contains_poll("cancel-forward") {
            tokio::task::yield_now().await;
        }
    }).await.expect("forward registers a parent waiter");
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), async {
        while root.pollers.lock().unwrap().contains_poll("cancel-forward") {
            tokio::task::yield_now().await;
        }
    }).await.expect("cancellation withdraws the forwarded waiter");
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_forwarder_max_children_below_one_clamps_to_one() {
    // Go task_list_manager clamps matching.forwarderMaxChildrenPerNode to >= 1, so a
    // configured 0 still forwards the child poll to its parent partition.
    let address = start_starting_history().await;
    let (_dir, _store, matching, key) = fixture_with_history(&address);
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.forwarderMaxChildrenPerNode", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"0".to_vec())), filters: vec![],
    }]);
    matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), task_list: list(), workflow_execution: execution(), schedule_id: 61,
        ..Default::default()
    })).await.unwrap();
    let child_list = p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() };
    let child = matching.durable_queue(&key.domain_id.to_string(), &child_list, 0).await.unwrap();
    let response = child.dispatcher.poll_decision(p::PollForDecisionTaskRequest { task_list: Some(child_list), ..Default::default() }, Duration::from_secs(2)).await.unwrap();
    assert_eq!(response.started_event_id, 62, "a clamped degree of 1 forwards to the parent");
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_forwarding_disabled_keeps_parent_backlog_untouched() {
    for (property, value) in [
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
pub(super) fn mutable_state() -> h::GetMutableStateResponse {
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
            .poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "sdk-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: list(),
                    identity: "sdk-worker".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
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
    tokio::time::timeout(Duration::from_secs(10), async {
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
            .poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "sdk-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: list(),
                    identity: "sdk-worker".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })))
            .await
    });
    let root = matching
        .durable_queue(&domain, &list().unwrap(), 0)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
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
            .poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "parent-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: Some(poll_list),
                    identity: "parent-worker".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })))
            .await
    });
    // Wait until the parent poller parks on the query channel; Go's
    // non-blocking offer only succeeds with a waiting poller.
    let root = matching
        .durable_queue(&domain, &partition, 0)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
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

/// Go-shaped remote parent for forwarded queries: records each QueryWorkflow it
/// receives, then answers with "answer", or ServiceBusy when `busy` is set.
#[derive(Clone)]
struct QueryParent {
    forwarded: std::sync::Arc<std::sync::Mutex<Option<m::QueryWorkflowRequest>>>,
    received: std::sync::Arc<tokio::sync::Notify>,
    busy: bool,
}

#[tonic::async_trait]
impl MatchingApi for QueryParent {
    async fn query_workflow(
        &self,
        request: tonic::Request<m::QueryWorkflowRequest>,
    ) -> Result<tonic::Response<m::QueryWorkflowResponse>, tonic::Status> {
        *self.forwarded.lock().unwrap() = Some(request.into_inner());
        self.received.notify_one();
        if self.busy {
            return Err(crate::grpc::detailed_status(
                tonic::Code::ResourceExhausted,
                "parent busy",
                "ServiceBusyError",
                p::ServiceBusyError { reason: String::new() },
            ));
        }
        Ok(tonic::Response::new(m::QueryWorkflowResponse {
            query_result: Some(p::Payload { data: b"answer".to_vec() }),
            ..Default::default()
        }))
    }
}

async fn serve_query_parent(parent: QueryParent) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(m::matching_api_server::MatchingApiServer::new(parent))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (address, server)
}

fn child_query_list() -> p::TaskList {
    p::TaskList {
        name: "/__cadence_sys/queue/1".into(),
        kind: 1,
        ..Default::default()
    }
}

fn child_forwardable_query(domain_id: String) -> Request<m::QueryWorkflowRequest> {
    let mut request = Request::new(m::QueryWorkflowRequest {
        domain_id,
        task_list: Some(child_query_list()),
        request: Some(p::QueryWorkflowRequest {
            domain: "domain".into(),
            workflow_execution: execution(),
            query: Some(p::WorkflowQuery {
                query_type: "ping".into(),
                query_args: None,
            }),
            ..Default::default()
        }),
        forwarded_from: String::new(),
    });
    request.set_timeout(Duration::from_secs(3));
    request
}

#[tokio::test]
async fn matching_rpc_remote_query_sync_match_forwards_to_parent_and_returns_answer() {
    // Go TestQueryRemoteSyncMatch, forwarding leg: with no local query poller the
    // child forwards QueryWorkflow to its remote parent and returns the parent's
    // answer. The forwarded poll leg is covered by
    // matching_rpc_child_query_forwards_to_parent_poller.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, child, key) = fixture_with_history(&peer);
    let parent = QueryParent {
        forwarded: Default::default(),
        received: Default::default(),
        busy: false,
    };
    let (address, server) = serve_query_parent(parent.clone()).await;
    let child = child.with_membership(membership_with_parent_remote(
        "/__cadence_sys/queue/1",
        "queue",
        &address,
    ));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        child.query_workflow(child_forwardable_query(key.domain_id.to_string())),
    )
    .await
    .expect("forwarded query should complete")
    .unwrap()
    .into_inner();
    assert_eq!(
        result.query_result.map(|payload| payload.data),
        Some(b"answer".to_vec())
    );
    let forwarded = parent
        .forwarded
        .lock()
        .unwrap()
        .clone()
        .expect("query should reach the remote parent");
    assert_eq!(forwarded.forwarded_from, "/__cadence_sys/queue/1");
    assert_eq!(forwarded.task_list.unwrap().name, "queue");
    child
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn matching_rpc_remote_query_sync_match_error_falls_back_to_local_poller() {
    // Go TestQueryRemoteSyncMatchError: the parent answers ServiceBusy, so the
    // child keeps the query for a local poller. Go starts that poller only after
    // the forward was attempted (ready()); the poller here waits for the parent's
    // receipt in the same way and then receives and answers the query.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, child, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = QueryParent {
        forwarded: Default::default(),
        received: Default::default(),
        busy: true,
    };
    let (address, server) = serve_query_parent(parent.clone()).await;
    let child = child.with_membership(membership_with_parent_remote(
        "/__cadence_sys/queue/1",
        "queue",
        &address,
    ));
    let received = parent.received.clone();
    let worker = child.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        received.notified().await;
        worker
            .poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "child-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: Some(child_query_list()),
                    identity: "child-worker".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })))
            .await
    });
    let querier = child.clone();
    let query_request = child_forwardable_query(domain.clone());
    let query = tokio::spawn(async move { querier.query_workflow(query_request).await });
    let delivered = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("local poller should receive the query after ServiceBusy")
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
    child
        .respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
            request: Some(p::RespondQueryTaskCompletedRequest {
                task_token: Vec::new(),
                result: Some(p::WorkflowQueryResult {
                    result_type: p::QueryResultType::Answered as i32,
                    answer: Some(p::Payload {
                        data: b"answer".to_vec(),
                    }),
                    error_message: String::new(),
                }),
                worker_version_info: None,
            }),
            domain_id: domain,
            task_list: Some(p::TaskList {
                name: token.task_list,
                kind: 1,
                ..Default::default()
            }),
            task_id: token.task_id,
        }))
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("query should complete after the local answer")
        .unwrap()
        .expect("local answer should be returned")
        .into_inner();
    assert_eq!(
        result.query_result.map(|payload| payload.data),
        Some(b"answer".to_vec())
    );
    let forwarded = parent
        .forwarded
        .lock()
        .unwrap()
        .clone()
        .expect("the busy parent should have been asked first");
    assert_eq!(forwarded.forwarded_from, "/__cadence_sys/queue/1");
    child
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn matching_rpc_query_without_poller_waits_for_deadline() {
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    // Go `OfferQuery` at the root has no fail-fast: with no poller it blocks on the
    // unbuffered queryTaskC send until the RPC context ends. The raw ctx error then
    // maps to InternalServiceError{"context deadline exceeded"} through handleErr.
    let mut request = Request::new(m::QueryWorkflowRequest {
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
    });
    request.set_timeout(Duration::from_millis(500));
    let started = tokio::time::Instant::now();
    let error = tokio::time::timeout(Duration::from_secs(5), matching.query_workflow(request))
        .await
        .expect("query must return once the RPC deadline elapses")
        .expect_err("query without a poller must fail at the deadline");
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "query returned before the RPC deadline"
    );
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "context deadline exceeded");
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
    // when the task list manager is created, so it is updated first. The tracker
    // is wall-clock and its first window opens when the first add creates the
    // queue. The window is long enough that the adds below always land in it,
    // and the test then waits for the windows to close instead of sleeping.
    let window = Duration::from_secs(2);
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
                    b"\"2s\"".to_vec(),
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
    let opened = std::time::Instant::now();
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
    // arrivals are still invisible. The first window starts no earlier than
    // `opened`, so reads made before `opened + window` see only the open window.
    // The guard is checked after the reads for that reason.
    let open_qps = queue.qps.qps();
    let open_group_qps = queue.qps.group_qps("z");
    assert!(
        opened.elapsed() < window,
        "adds ran past the QPS window; the open-window check is not meaningful"
    );
    assert_eq!(open_qps, 0.0);
    assert_eq!(open_group_qps, 0.0);
    let activity = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 1).await.unwrap();
    activity.qps.report_counter(1);
    // Windows close on the wall clock, so wait for the root and the group to
    // report instead of sleeping for a fixed time. Three root arrivals and two
    // isolation-group arrivals were reported, and the group counts toward the
    // root as well.
    tokio::time::timeout(Duration::from_secs(10), async {
        while queue.qps.qps() <= 0.0 || queue.qps.group_qps("z") <= 0.0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("closed windows should report the arrivals");
    assert_eq!(activity.qps.qps(), 0.0, "activity tracker retains the default 10s window");
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
            .poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
                domain_id: poll_domain,
                poller_id: "sticky-worker".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    task_list: sticky(),
                    identity: "sticky-worker".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
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
async fn matching_rpc_partition_update_refreshes_remote_read_owner_and_ignores_write_only_owner() {
    use cadence_common::membership::{Host, Ring};
    use crate::matching_membership::Membership;

    let history = start_starting_history().await;
    let (_first_dir, _first_store, first, key) = fixture_with_history(&history);
    let (_second_dir, _second_store, second, _) = fixture_with_history(&history);
    let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hosts = vec![
        Host { address: "node-0:7935".into(), identity: "node-0".into(), grpc_address: first_listener.local_addr().unwrap().to_string() },
        Host { address: "node-1:7935".into(), identity: "node-1".into(), grpc_address: second_listener.local_addr().unwrap().to_string() },
    ];
    let ring = Ring::new(hosts.clone());
    let (root_name, root_host) = (0..100)
        .map(|index| {
            let name = format!("partition-refresh-root-{index}");
            let owner = ring.lookup(&name).unwrap().identity().to_owned();
            (name, owner)
        })
        .find(|(_, owner)| owner == "node-0")
        .unwrap();
    let read_partition = (1..=100)
        .find(|partition| ring.lookup(&format!("/__cadence_sys/{root_name}/{partition}" )).unwrap().identity() != root_host)
        .unwrap();
    let write_only_partition = (1..=100)
        .find(|partition| *partition != read_partition
            && ring.lookup(&format!("/__cadence_sys/{root_name}/{partition}" )).unwrap().identity() != root_host)
        .unwrap();
    let first = first.with_membership(Membership::new("node-0", hosts.clone()));
    let second = second.with_membership(Membership::new("node-1", hosts.clone()));
    let first_service = first.clone();
    let first_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(m::matching_api_server::MatchingApiServer::new(first_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(first_listener))
            .await.unwrap();
    });
    let second_service = second.clone();
    let second_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(m::matching_api_server::MatchingApiServer::new(second_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(second_listener))
            .await.unwrap();
    });
    let domain = key.domain_id.to_string();
    let root_list = p::TaskList { name: root_name.clone(), kind: 1, ..Default::default() };
    let root = if root_host == "node-0" { first.clone() } else { second.clone() };
    let read_owner = ring.lookup(&format!("/__cadence_sys/{root_name}/{read_partition}")).unwrap().identity();
    let remote = if read_owner == "node-0" { first.clone() } else { second.clone() };
    let update = |read: &[i32], write: &[i32]| Request::new(m::UpdateTaskListPartitionConfigRequest {
        domain_id: domain.clone(), task_list: Some(root_list.clone()), task_list_type: 1,
        partition_config: Some(native_config(999, read, write)),
    });

    root.update_task_list_partition_config(update(&[0, read_partition], &[0])).await.unwrap();
    let read_name = format!("/__cadence_sys/{root_name}/{read_partition}");
    let read_queue = remote.durable_queue(&domain, &p::TaskList { name: read_name.clone(), kind: 1, ..Default::default() }, 0).await.unwrap();
    assert_eq!(read_queue.partitions.get().unwrap().version, 1, "old/new read partition notification must load and refresh a remote owner");
    let write_name = format!("/__cadence_sys/{root_name}/{write_only_partition}");
    let write_host = ring.lookup(&write_name).unwrap().identity();
    let write_owner = if write_host == "node-0" { &first } else { &second };
    assert!(!write_owner.queues.as_ref().unwrap().contains_cached(&TaskListKey {
        domain_id: key.domain_id, name: write_name.clone(), task_type: 0,
    }), "Go refresh fanout does not create a manager for write-only partition IDs");

    root.update_task_list_partition_config(update(&[0], &[0])).await.unwrap();
    assert_eq!(read_queue.partitions.get().unwrap().version, 2, "removing a remote read partition sends the new config to the old partition owner");
    remote.refresh_task_list_partition_config(Request::new(m::RefreshTaskListPartitionConfigRequest {
        domain_id: domain.clone(), task_list: Some(p::TaskList { name: read_name, kind: 1, ..Default::default() }),
        task_list_type: 1, partition_config: Some(native_config(1, &[0, read_partition], &[0])),
    })).await.unwrap();
    assert_eq!(read_queue.partitions.get().unwrap().version, 2, "out-of-order older push must be ignored by the owner manager");

    // Remove the old owner from membership. The next root update must resolve
    // the partition's new owner and refresh it there, including a local
    // self-RPC that creates the manager if it is not loaded yet.
    let moved_hosts = vec![hosts[0].clone()];
    first.membership.as_ref().unwrap().replace(moved_hosts.clone());
    second.membership.as_ref().unwrap().replace(moved_hosts);
    root.update_task_list_partition_config(update(&[0, read_partition], &[0])).await.unwrap();
    let moved_queue = first.durable_queue(&domain, &p::TaskList { name: format!("/__cadence_sys/{root_name}/{read_partition}"), kind: 1, ..Default::default() }, 0).await.unwrap();
    assert_eq!(moved_queue.partitions.get().unwrap().version, 3, "partition config notification follows owner movement");

    first.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    second.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    first_server.abort();
    second_server.abort();
}

#[tokio::test]
async fn matching_partition_update_fans_out_to_fast_peer_while_slow_peer_uses_caller_deadline() {
    use cadence_common::membership::{Host, Ring};
    use crate::matching_membership::Membership;

    let history = start_starting_history().await;
    let (_root_dir, root_store, root, key) = fixture_with_history(&history);
    let (_fast_dir, _fast_store, fast, _) = fixture_with_history(&history);

    let fast_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hosts = vec![
        Host { address: "node-0:7935".into(), identity: "node-0".into(), grpc_address: "127.0.0.1:1".into() },
        Host { address: "node-1:7935".into(), identity: "node-1".into(), grpc_address: fast_listener.local_addr().unwrap().to_string() },
        Host { address: "node-2:7935".into(), identity: "node-2".into(), grpc_address: slow_listener.local_addr().unwrap().to_string() },
    ];
    let ring = Ring::new(hosts.clone());
    let (root_name, slow_partition, fast_partition) = (0..10_000)
        .map(|index| format!("partition-notify-deadline-{index}"))
        .find_map(|name| {
            if ring.lookup(&name)?.identity() != "node-0" {
                return None;
            }
            let candidates = (1..=100).map(|partition| {
                (partition, ring.lookup(&format!("/__cadence_sys/{name}/{partition}" )).unwrap().identity().to_owned())
            }).collect::<Vec<_>>();
            let slow = candidates.iter().find(|(_, owner)| owner == "node-2")?.0;
            let fast = candidates
                .iter()
                .find(|(partition, owner)| owner == "node-1" && partition > &slow)?
                .0;
            Some((name, slow, fast))
        })
        .expect("find root and ascending slow/fast partition owners");

    let root = root.with_membership(Membership::new("node-0", hosts.clone()));
    let fast = fast.with_membership(Membership::new("node-1", hosts.clone()));
    let fast_service = fast.clone();
    let fast_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(m::matching_api_server::MatchingApiServer::new(fast_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(fast_listener))
            .await
            .unwrap();
    });
    let (slow_connected_tx, slow_connected_rx) = tokio::sync::oneshot::channel();
    let slow_peer = tokio::spawn(async move {
        let (stream, _) = slow_listener.accept().await.unwrap();
        let _stream = stream; // Accept TCP, then leave the gRPC peer delayed indefinitely.
        let _ = slow_connected_tx.send(());
        std::future::pending::<()>().await;
    });

    let fast_name = format!("/__cadence_sys/{root_name}/{fast_partition}");
    let fast_queue = fast.durable_queue(
        &key.domain_id.to_string(),
        &p::TaskList { name: fast_name, kind: 1, ..Default::default() },
        0,
    ).await.unwrap();
    assert!(fast_queue.partitions.get().is_none(), "the fast owner starts without the new config");

    let root_task_list = p::TaskList { name: root_name.clone(), kind: 1, ..Default::default() };
    let mut request = Request::new(m::UpdateTaskListPartitionConfigRequest {
        domain_id: key.domain_id.to_string(),
        task_list: Some(root_task_list),
        task_list_type: 1,
        partition_config: Some(native_config(0, &[0, slow_partition, fast_partition], &[0])),
    });
    // This is the caller's context deadline, forwarded by the real handler to
    // every peer refresh. There is deliberately no test-only service hook.
    request.metadata_mut().insert("grpc-timeout", "800m".parse().unwrap());
    let started = tokio::time::Instant::now();
    let updater = {
        let root = root.clone();
        tokio::spawn(async move { root.update_task_list_partition_config(request).await })
    };
    tokio::time::timeout(Duration::from_millis(600), async {
        while fast_queue.partitions.get().is_none_or(|config| config.version != 1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the later fast owner must receive its RPC before the caller deadline");
    let fast_received_at = started.elapsed();
    assert!(fast_received_at < Duration::from_millis(600));
    updater.await.unwrap().unwrap();
    let elapsed = started.elapsed();
    tokio::time::timeout(Duration::from_secs(1), slow_connected_rx).await.unwrap().unwrap();

    assert_eq!(fast_queue.partitions.get().unwrap().version, 1,
        "the later fast owner receives its refresh while the lower-ID slow peer is still pending");
    assert!(elapsed >= Duration::from_millis(600), "the update waits for the caller deadline on the delayed peer");
    assert!(elapsed < Duration::from_secs(2), "the delayed peer is bounded by the caller deadline");
    let root_key = TaskListKey { name: root_name.clone(), ..key.clone() };
    assert_eq!(root_store.get_task_list(&root_key).unwrap().data.adaptive_partition_config.unwrap().version, 1,
        "peer timeout remains best effort after the root config is persisted");

    root.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    fast.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    fast_server.abort();
    slow_peer.abort();
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
    let decision = matching.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain.clone(), request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    }))).await.unwrap().into_inner();
    assert!(decision.task_token.is_empty());
    let activity = matching.poll_for_activity_task(timed_poll(Request::new(m::PollForActivityTaskRequest {
        domain_id: domain.clone(), request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    }))).await.unwrap().into_inner();
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
    let sink = Arc::new(crate::matching_metrics::RecordingMetricSink::default());
    let matching = matching.with_metrics(sink.clone());
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
    let events = sink.events();
    assert_eq!(
        events.iter().filter(|event| event.name == crate::matching_metrics::STANDBY_TASKS_COMPLETED).count(),
        2
    );
    assert!(!events.iter().any(|event| event.name == crate::matching_metrics::STANDBY_TASKS_NOT_STARTED));
    assert!(!events.iter().any(|event| event.name == crate::matching_metrics::STANDBY_TASKS_COMPLETION_FAILURE));
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
        worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: poll_domain, poller_id: "failover-poller".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
        }))).await
    });
    let queue = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async { while !queue.queries.has_waiters() { tokio::task::yield_now().await; } }).await.unwrap();
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
    let response = tokio::time::timeout(Duration::from_secs(3), child.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain.clone(), poller_id: "remote-backlog".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: child_list(), ..Default::default() }), ..Default::default()
    })))).await.unwrap().unwrap().into_inner();
    assert_eq!(response.started_event_id, 42);
    let root_queue = root.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let worker = root.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "remote-sync".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    }))).await.unwrap().into_inner() });
    tokio::time::timeout(Duration::from_secs(10), async {
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
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "remote-cancel".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: poll_list, ..Default::default() }), ..Default::default()
    }))).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !root_queue.pollers.lock().unwrap().contains_poll("remote-cancel") { tokio::task::yield_now().await; }
    }).await.unwrap();
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), async {
        while root_queue.pollers.lock().unwrap().contains_poll("remote-cancel") { tokio::task::yield_now().await; }
    }).await.expect("HTTP/2 cancellation withdraws the upstream waiter");
    child.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    root.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    server.abort();
}

/// Parent partition owner for a sync-offer forward: records the forwarded Add RPCs.
#[derive(Clone, Default)]
struct RecordingAddParent {
    add_decision: Arc<std::sync::Mutex<Option<m::AddDecisionTaskRequest>>>,
    add_activity: Arc<std::sync::Mutex<Option<m::AddActivityTaskRequest>>>,
}

#[tonic::async_trait]
impl MatchingApi for RecordingAddParent {
    async fn add_decision_task(&self, request: Request<m::AddDecisionTaskRequest>) -> Result<Response<m::AddDecisionTaskResponse>, Status> {
        *self.add_decision.lock().unwrap() = Some(request.into_inner());
        Ok(Response::new(m::AddDecisionTaskResponse::default()))
    }
    async fn add_activity_task(&self, request: Request<m::AddActivityTaskRequest>) -> Result<Response<m::AddActivityTaskResponse>, Status> {
        *self.add_activity.lock().unwrap() = Some(request.into_inner());
        Ok(Response::new(m::AddActivityTaskResponse::default()))
    }
    async fn poll_for_decision_task(&self, _: Request<m::PollForDecisionTaskRequest>) -> Result<Response<m::PollForDecisionTaskResponse>, Status> {
        Err(Status::unimplemented("poll_for_decision_task"))
    }
    async fn poll_for_activity_task(&self, _: Request<m::PollForActivityTaskRequest>) -> Result<Response<m::PollForActivityTaskResponse>, Status> {
        Err(Status::unimplemented("poll_for_activity_task"))
    }
}

#[tokio::test]
async fn matching_rpc_sync_offer_forward_sends_go_forwarder_fields_to_parent() {
    // Go matcher.Offer with no local poller -> forwarder.ForwardTask (forwarder.go:139-168):
    // decision DomainUUID is the task domain; activity DomainUUID is the list domain and
    // SourceDomainUUID is the task's own domain. The forwarded add is never persisted.
    use cadence_common::membership::{Host, Ring};
    use crate::matching_membership::Membership;
    let history = start_starting_history().await;
    let (_dir, store, child, key) = fixture_with_history(&history);
    let parent = RecordingAddParent::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let served = parent.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder().add_service(m::matching_api_server::MatchingApiServer::new(served))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)).await.unwrap();
    });
    let mut hosts = vec![
        Host { address: "node-0:7935".into(), identity: "node-0".into(), grpc_address: String::new() },
        Host { address: "node-1:7935".into(), identity: "node-1".into(), grpc_address: String::new() },
    ];
    let ring = Ring::new(hosts.clone());
    let root_id = ring.lookup("queue").unwrap().identity().to_owned();
    let child_name = (1..=20).map(|id| format!("/__cadence_sys/queue/{id}")).find(|name| ring.lookup(name).unwrap().identity() != root_id).unwrap();
    let child_id = ring.lookup(&child_name).unwrap().identity().to_owned();
    for host in &mut hosts { host.grpc_address = endpoint.clone(); }
    let child = child.with_membership(Membership::new(child_id, hosts));
    // The task's own domain differs from the list domain, so the activity mapping is observable.
    let source = Uuid::from_u128(5);
    child.frontend.state.domains.create(
        cadence_api::types::DomainInfo { name: "source".into(), uuid: source.to_string(), ..Default::default() },
        Default::default(),
        Default::default(),
        false,
    ).unwrap();
    let domain = key.domain_id.to_string();
    let child_list = || Some(p::TaskList { name: child_name.clone(), kind: 1, ..Default::default() });
    let schedule_to_start = || Some(prost_types::Duration { seconds: 7, nanos: 0 });

    child.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: domain.clone(), workflow_execution: execution(), task_list: child_list(),
        schedule_id: 61, schedule_to_start_timeout: schedule_to_start(), source: 1, ..Default::default()
    })).await.unwrap();
    let decision = parent.add_decision.lock().unwrap().clone().expect("decision add forwarded to parent");
    assert_eq!(decision.domain_id, domain);
    let list = decision.task_list.clone().unwrap();
    assert_eq!((list.name.as_str(), list.kind), ("queue", 1));
    assert_eq!(decision.workflow_execution, execution());
    assert_eq!(decision.schedule_id, 61);
    assert_eq!(decision.schedule_to_start_timeout.as_ref().map(|d| d.seconds), Some(7));
    assert_eq!(decision.forwarded_from, child_name);
    assert_eq!(decision.source, 1);

    child.add_activity_task(Request::new(m::AddActivityTaskRequest {
        domain_id: domain.clone(), source_domain_id: source.to_string(), workflow_execution: execution(),
        task_list: child_list(), schedule_id: 62, schedule_to_start_timeout: schedule_to_start(), source: 1,
        ..Default::default()
    })).await.unwrap();
    let activity = parent.add_activity.lock().unwrap().clone().expect("activity add forwarded to parent");
    assert_eq!(activity.domain_id, domain);
    assert_eq!(activity.source_domain_id, source.to_string());
    let list = activity.task_list.clone().unwrap();
    assert_eq!((list.name.as_str(), list.kind), ("queue", 1));
    assert_eq!(activity.workflow_execution, execution());
    assert_eq!(activity.schedule_id, 62);
    assert_eq!(activity.schedule_to_start_timeout.as_ref().map(|d| d.seconds), Some(7));
    assert_eq!(activity.forwarded_from, child_name);
    assert_eq!(activity.source, 1);

    assert_eq!(store.task_list_size(&TaskListKey { name: child_name.clone(), ..key.clone() }, 0).unwrap(), 0, "sync-forwarded adds never persist on the child");
    child.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
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
        tokio::spawn(async move { service.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: id,
            request: Some(p::PollForDecisionTaskRequest { task_list: Some(task_list), ..Default::default() }), ..Default::default()
        }))).await })
    };
    let delayed = start_poll("delayed");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !child.pollers.lock().unwrap().contains_poll("delayed") { tokio::task::yield_now().await; }
    }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!root.pollers.lock().unwrap().contains_poll("delayed"), "configured local wait precedes the parent poll");
    delayed.abort();
    assert!(delayed.await.unwrap_err().is_cancelled());
    scaler_config_value(&matching, "matching.localPollWaitTime", br#""0s""#);
    let immediate = start_poll("immediate");
    tokio::time::timeout(Duration::from_secs(10), async {
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
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "remote-query-worker".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    }))).await.unwrap().into_inner() });
    tokio::time::timeout(Duration::from_secs(10), async {
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
    let delivered = tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap();
    assert_eq!(delivered.query.unwrap().query_type, "ping");
    let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(&delivered.task_token).unwrap();
    assert_eq!(token.task_list, "queue");
    root.respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
        domain_id: domain, task_list: list(), task_id: token.task_id,
        request: Some(p::RespondQueryTaskCompletedRequest { result: Some(p::WorkflowQueryResult {
            result_type: p::QueryResultType::Answered as i32, answer: Some(p::Payload { data: b"pong".to_vec() }), error_message: String::new(),
        }), ..Default::default() }),
    })).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), query).await.unwrap().unwrap();
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
    let request = timed_poll(Request::new(m::PollForDecisionTaskRequest { domain_id: key.domain_id.to_string(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default() }));
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
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "child-query-poll".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: Some(p::TaskList {
            name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default()
        }), ..Default::default() }), ..Default::default()
    }))).await.unwrap().into_inner() });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !root.queries.has_waiters() { tokio::task::yield_now().await; }
    }).await.expect("forwarded parent poll must wait for parent queries as well as backlog");
    let caller = matching.clone();
    let query_domain = domain.clone();
    let query = tokio::spawn(async move { caller.query_workflow(Request::new(m::QueryWorkflowRequest {
        domain_id: query_domain, task_list: list(),
        request: Some(p::QueryWorkflowRequest { domain: "domain".into(), workflow_execution: execution(),
            query: Some(p::WorkflowQuery { query_type: "ping".into(), query_args: None }), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner() });
    let response = tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap();
    assert_eq!(response.query.unwrap().query_type, "ping");
    let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(&response.task_token).unwrap();
    assert_eq!(token.task_list, "queue");
    matching.respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
        domain_id: domain, task_list: list(), task_id: token.task_id,
        request: Some(p::RespondQueryTaskCompletedRequest { result: Some(p::WorkflowQueryResult {
            result_type: p::QueryResultType::Answered as i32, answer: Some(p::Payload { data: b"pong".to_vec() }), error_message: String::new(),
        }), ..Default::default() }),
    })).await.unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), query).await.unwrap().unwrap().query_result.unwrap().data, b"pong");
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_rpc_standby_child_query_poll_receives_parent_query() {
    // Standby getTask serves only queryTaskC; the child forwards PollForQuery
    // to the parent, and the parent's query channel answers the poll.
    let peer = start_fake_history(mutable_state()).await;
    let (_directory, _store, matching, _key) = fixture_with_history(&peer);
    let domain = Uuid::from_u128(3).to_string();
    matching.frontend.state.domains.create(
        cadence_api::types::DomainInfo { name: "standby".into(), uuid: domain.clone(), ..Default::default() },
        Default::default(),
        cadence_api::types::DomainReplicationConfiguration { active_cluster_name: "remote".into(), ..Default::default() },
        true,
    ).unwrap();
    let root = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    let worker = matching.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move { worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
        domain_id: poll_domain, poller_id: "standby-query-poll".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: Some(p::TaskList {
            name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default()
        }), ..Default::default() }), ..Default::default()
    }))).await.unwrap().into_inner() });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !root.queries.has_waiters() { tokio::task::yield_now().await; }
    }).await.expect("standby forwarded poll must wait for the parent's query channel");
    let caller = matching.clone();
    let query_domain = domain.clone();
    let query = tokio::spawn(async move { caller.query_workflow(Request::new(m::QueryWorkflowRequest {
        domain_id: query_domain, task_list: list(),
        request: Some(p::QueryWorkflowRequest { domain: "standby".into(), workflow_execution: execution(),
            query: Some(p::WorkflowQuery { query_type: "ping".into(), query_args: None }), ..Default::default() }), ..Default::default()
    })).await.unwrap().into_inner() });
    let response = tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap();
    assert_eq!(response.query.unwrap().query_type, "ping");
    let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(&response.task_token).unwrap();
    matching.respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
        domain_id: domain, task_list: list(), task_id: token.task_id,
        request: Some(p::RespondQueryTaskCompletedRequest { result: Some(p::WorkflowQueryResult {
            result_type: p::QueryResultType::Answered as i32, answer: Some(p::Payload { data: b"pong".to_vec() }), error_message: String::new(),
        }), ..Default::default() }),
    })).await.unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), query).await.unwrap().unwrap().query_result.unwrap().data, b"pong");
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
    let request = timed_poll(Request::new(m::PollForActivityTaskRequest { domain_id: key.domain_id.to_string(),
        request: Some(p::PollForActivityTaskRequest { task_list: child_list, ..Default::default() }), ..Default::default() }));
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
            assert_eq!(matching.poll_rate(&domain, &key.name, 0, task_type, worker, &limiter), expected);
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
                .snapshot_values()
                .len() as i64
                + 2
        }),
    );
    // Override path first: 200 is below the default, so it is accepted and the
    // live partition provider runs while the overlay is free.
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.overrideTaskListRps", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"200".to_vec())), filters: vec![],
    }]);
    assert_eq!(matching.poll_rate(&domain, &key.name, 0, 1, Some(25.0), &limiter), 200.0);
    // Two read partitions, so the accepted report halves the backing limit.
    assert_eq!(limiter.partitions(), 2);
    assert_eq!(limiter.limit(), 100.0);
    // Cleared override falls back to the worker-reported rate, which is lower
    // than the accepted report, so this path also consults the provider.
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false, "matching.overrideTaskListRps", vec![cadence_api::admin::DynamicConfigValue {
        value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"0".to_vec())), filters: vec![],
    }]);
    assert_eq!(matching.poll_rate(&domain, &key.name, 0, 1, Some(25.0), &limiter), 25.0);
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
            .poll_for_activity_task(timed_poll(Request::new(m::PollForActivityTaskRequest {
                domain_id: domain.clone(),
                request: Some(p::PollForActivityTaskRequest { task_list: list(), ..Default::default() }),
                ..Default::default()
            })))
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
        .poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain.clone(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }),
            ..Default::default()
        })))
        .await
        .unwrap()
        .into_inner();
    assert!(decision.task_token.is_empty());
    assert!(!decision.auto_config_hint.unwrap().enable_auto_config);
    // A delivered task is stamped by the matcher with the manager's own type.
    assert!(matching.auto_config_hint(&domain, "queue", 1, std::time::Instant::now()).enable_auto_config);
    assert!(!matching.auto_config_hint(&domain, "queue", 0, std::time::Instant::now()).enable_auto_config);
}

/// Long polls carry a caller deadline in Go, and Matching rejects them without one.
fn timed_poll<T>(mut request: Request<T>) -> Request<T> {
    request.set_timeout(Duration::from_secs(8));
    request
}

#[tokio::test]
async fn matching_rpc_task_list_type_non_activity_maps_to_decision() {
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    matching.frontend.state.executions.register_domain("domain", &domain);
    // Go `engine.go` treats any type other than Activity as Decision, so an
    // unset type is accepted instead of failing with "TaskListType is invalid.".
    let cancel = matching
        .cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
            domain_id: domain,
            task_list: list(),
            task_list_type: 0,
            poller_id: "poller".into(),
        }))
        .await;
    assert!(cancel.is_ok(), "{cancel:?}");
}

fn bool_config(value: bool, domain: Option<&str>) -> cadence_api::admin::DynamicConfigValue {
    let mut config = quota_config(0, domain);
    config.value = Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON,
        serde_json::to_vec(&value).unwrap()));
    config
}

#[tokio::test]
async fn matching_meta_health_waits_for_start_on_both_transports() {
    use cadence_proto::api::meta_api_server::MetaApi;
    let matching = Matching::new(crate::handlers::AppState::default()).with_start_pending();
    let grpc = matching.health(Request::new(p::HealthRequest {}));
    let args = cadence_thrift::value::Struct::default();
    let thrift = crate::thrift_matching::dispatch(&matching, "Meta::health", &args,
        std::time::Instant::now() + Duration::from_secs(5));
    tokio::pin!(grpc, thrift);
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut grpc).await.is_err());
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut thrift).await.is_err());
    matching.mark_started();
    let health = grpc.await.unwrap().into_inner();
    assert!(health.ok);
    assert_eq!(health.message, "matching good");
    let (response, error) = thrift.await.unwrap();
    assert!(error.is_none());
    let Some(cadence_thrift::value::Value::Struct(status)) = response.get(0) else { panic!("missing status") };
    assert_eq!(status.get(1), Some(&cadence_thrift::value::Value::Bool(true)));
    assert_eq!(status.get(2), Some(&cadence_thrift::value::Value::Bytes(b"matching good".to_vec())));
}

#[tokio::test]
async fn matching_cancel_loads_unknown_partition_without_cancelling_other_partition() {
    let (_dir, _store, matching, key) = fixture();
    let child = "/__cadence_sys/queue/1";
    let guard = matching.register_poll(PollKey { domain: key.domain_id.to_string(),
        task_list: "queue".into(), task_type: 1, poller: "same-id".into() });
    matching.cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
        domain_id: key.domain_id.to_string(), task_list: Some(p::TaskList { name: child.into(), kind: 1, ..Default::default() }),
        task_list_type: 1, poller_id: "same-id".into(),
    })).await.unwrap();
    assert!(matching.queues.as_ref().unwrap().contains_cached(&TaskListKey { name: child.into(), ..key }));
    assert!(tokio::time::timeout(Duration::from_millis(30), guard.cancel.notified()).await.is_err());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_cancel_outstanding_forwarded_parent_poll() {
    let (_dir, _store, matching, key) = fixture();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "forwarded".into(),
            request: Some(p::PollForDecisionTaskRequest {
                task_list: Some(p::TaskList { name: "/__cadence_sys/queue/1".into(), kind: 1, ..Default::default() }),
                identity: "worker".into(), ..Default::default()
            }), ..Default::default()
        }))).await
    });
    let root = matching.durable_queue(&key.domain_id.to_string(), &list().unwrap(), 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !root.pollers.lock().unwrap().contains_poll("forwarded") { tokio::task::yield_now().await; }
    }).await.unwrap();
    matching.cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
        domain_id: key.domain_id.to_string(), task_list: list(), task_list_type: 1, poller_id: "forwarded".into(),
    })).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap().unwrap().into_inner().task_token.is_empty());
    tokio::time::timeout(Duration::from_secs(10), async {
        while root.pollers.lock().unwrap().contains_poll("forwarded") { tokio::task::yield_now().await; }
    }).await.unwrap();
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_semaphore_domain_filter_and_ring_admission_precede_creation() {
    use cadence_common::membership::Host;
    let (_dir, _store, matching, key) = fixture();
    // The legacy process flag cannot enable Matching buckets.
    let mut matching = matching;
    matching.frontend.state.semaphore_enabled = true;
    let request = || Request::new(m::AcquireSemaphoreRequest { domain_id: key.domain_id.to_string(),
        semaphore_name: "sem".into(), bucket: 0, owner_id: "1:w:r:1".into() });
    assert_eq!(matching.acquire_semaphore(request()).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "system.enableDistributedSemaphore", vec![bool_config(true, Some("other-domain"))]);
    assert_eq!(matching.acquire_semaphore(request()).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "system.enableDistributedSemaphore", vec![bool_config(true, Some("domain"))]);
    let membership = crate::matching_membership::Membership::new("local", vec![Host {
        address: "remote:7935".into(), identity: "remote".into(), grpc_address: "127.0.0.1:1".into(),
    }]);
    let matching = matching.with_membership(membership);
    let error = matching.acquire_semaphore(request()).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(error.message().contains("not owned by this host"));
    assert!(matching.semaphores.all_managers().is_empty());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_domain_listing_filters_kinds_and_collapses_partition_roots() {
    let (_dir, _store, matching, key) = fixture();
    for (name, kind, task_type) in [("queue", 1, 0), ("/__cadence_sys/queue/1", 1, 0),
        ("sticky", 2, 0), ("activity", 1, 1)] {
        matching.durable_queue(&key.domain_id.to_string(), &p::TaskList { name: name.into(), kind, ..Default::default() }, task_type).await.unwrap();
    }
    let request = || Request::new(m::GetTaskListsByDomainRequest { domain: "domain".into() });
    let normal = matching.get_task_lists_by_domain(request()).await.unwrap().into_inner();
    assert_eq!(normal.decision_task_list_map.len(), 1);
    assert!(normal.decision_task_list_map.contains_key("queue"));
    assert_eq!(normal.activity_task_list_map.len(), 1);
    // Go iterates maps; either loaded partition may win the root-key collision.
    let name = &normal.decision_task_list_map["queue"].task_list.as_ref().unwrap().name;
    assert!(name == "queue" || name == "/__cadence_sys/queue/1");
    matching.frontend.state.admin.dynamic_config.lock().unwrap().update(false,
        "matching.matchingReturnAllTaskListKinds", vec![bool_config(true, None)]);
    let all = matching.get_task_lists_by_domain(request()).await.unwrap().into_inner();
    assert_eq!(all.decision_task_list_map.len(), 2);
    assert!(all.decision_task_list_map.contains_key("sticky"));
    assert!(all.decision_task_list_map.values().all(|description| description.task_list_status.is_none()));
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_failover_callback_disconnects_sticky_decision_and_keeps_manager() {
    let (_dir, _store, matching, key) = fixture();
    let sticky = p::TaskList { name: "sticky-failover".into(), kind: 2, ..Default::default() };
    let queue = matching.durable_queue(&key.domain_id.to_string(), &sticky, 0).await.unwrap();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll_list = sticky.clone();
    let poll = tokio::spawn(async move {
        worker.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain, poller_id: "sticky-failover".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: Some(poll_list), identity: "worker".into(), ..Default::default() }),
            ..Default::default()
        }))).await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !queue.pollers.lock().unwrap().contains_poll("sticky-failover") { tokio::task::yield_now().await; }
    }).await.unwrap();
    let registry = matching.queues.as_ref().unwrap();
    registry.release_domain_failover_polls(key.domain_id).await;
    assert!(tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap().unwrap().into_inner().task_token.is_empty());
    assert!(!queue.queue.is_stopped());
    let next = matching.durable_queue(&key.domain_id.to_string(), &sticky, 0).await.unwrap();
    assert!(Arc::ptr_eq(&queue.queue, &next.queue));
    registry.shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_failover_callback_excludes_sticky_activity_and_other_domains() {
    let (_dir, _store, matching, key) = fixture();
    let registry = matching.queues.as_ref().unwrap();
    let sticky = p::TaskList { name: "sticky-activity".into(), kind: 2, ..Default::default() };
    let queue = matching.durable_queue(&key.domain_id.to_string(), &sticky, 1).await.unwrap();
    let worker = matching.clone();
    let domain = key.domain_id.to_string();
    let poll_list = sticky.clone();
    let mut poll = tokio::spawn(async move {
        worker.poll_for_activity_task(timed_poll(Request::new(m::PollForActivityTaskRequest {
            domain_id: domain, poller_id: "sticky-activity".into(),
            request: Some(p::PollForActivityTaskRequest { task_list: Some(poll_list), identity: "worker".into(), ..Default::default() }),
            ..Default::default()
        }))).await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !queue.pollers.lock().unwrap().contains_poll("sticky-activity") { tokio::task::yield_now().await; }
    }).await.unwrap();
    registry.release_domain_failover_polls(Uuid::from_u128(999)).await;
    registry.release_domain_failover_polls(key.domain_id).await;
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut poll).await.is_err());
    assert!(!queue.queue.is_stopped());
    matching.cancel_outstanding_poll(Request::new(m::CancelOutstandingPollRequest {
        domain_id: key.domain_id.to_string(), task_list: Some(sticky), task_list_type: 2, poller_id: "sticky-activity".into(),
    })).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(10), poll).await.unwrap().unwrap().unwrap().into_inner().task_token.is_empty());
    registry.shutdown(matching_now()).await.unwrap();
}

#[tokio::test]
async fn matching_grpc_listener_registers_meta_health_and_gates_startup() {
    let matching = Matching::new(crate::handlers::AppState::default()).with_start_pending();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = matching.clone();
    let server = tokio::spawn(async move { serve_with_listener(listener, service).await });
    let mut client = p::meta_api_client::MetaApiClient::connect(format!("http://{address}")).await.unwrap();
    let mut health = tokio::spawn(async move { client.health(p::HealthRequest {}).await });
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut health).await.is_err());
    matching.mark_started();
    let response = tokio::time::timeout(Duration::from_secs(10), health).await.unwrap().unwrap().unwrap().into_inner();
    assert!(response.ok);
    assert_eq!(response.message, "matching good");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

// Values below are derived from Go poller/manager.go:198-249 and cache/lru.go:
// identity-keyed MRU history, strict after comparison, five-minute TTL, and
// outstanding history entries bypassing only the after filter (not TTL).
#[test]
fn engine_apis_poller_groups_and_list_info_match_go_boundaries() {
    use cadence_common::poller::{Info, Manager};
    let mut manager = Manager::default();
    let info = |identity: &str, group: &str, rate| Info {
        identity: identity.into(),
        isolation_group: group.into(),
        rate_per_second: rate,
    };
    manager.start("one", info("worker", "old-zone", 1.0), 10);
    manager.start("two", info("worker", "zone-a", 7.5), 20);
    manager.start("", info("completed", "zone-a", 2.5), 20);
    manager.start("anonymous", info("", "zone-b", 3.0), 20);
    manager.start("ungrouped", info("ungrouped", "", 4.0), 30);
    assert_eq!(
        manager.count_by_group(30, 20),
        HashMap::from([("zone-a".into(), 1)])
    );
    assert_eq!(
        manager.count_by_group(30, 19),
        HashMap::from([("zone-a".into(), 2)])
    );
    let listed = manager.list(30);
    assert_eq!(
        listed
            .iter()
            .map(|v| (v.identity.as_str(), v.rate_per_second, v.last_access_time))
            .collect::<Vec<_>>(),
        vec![
            ("ungrouped", 4.0, 30),
            ("completed", 2.5, 20),
            ("worker", 7.5, 20)
        ]
    );
    manager.end("two", 40);
    assert!(manager.count_by_group(40, 40).is_empty());
    assert_eq!(
        manager.count_by_group(40, 39),
        HashMap::from([("zone-a".into(), 1)])
    );
    assert_eq!(manager.list(40)[0].last_access_time, 40);
    manager.start("live", info("long-poll", "zone-b", 9.0), 40);
    assert_eq!(
        manager.count_by_group(300_000_000_040, i64::MAX),
        HashMap::from([("zone-b".into(), 1)])
    );
    assert!(manager.list(300_000_000_041).is_empty());
}

#[tokio::test]
async fn engine_apis_partition_order_owners_and_lookup_errors_match_go() {
    use crate::matching_membership::Membership;
    use cadence_common::membership::Host;
    let (_dir, _store, matching, _key) = fixture();
    let hosts = vec![
        Host {
            address: "one:7935".into(),
            identity: "one".into(),
            grpc_address: "one:7835".into(),
        },
        Host {
            address: "two:7935".into(),
            identity: "two".into(),
            grpc_address: "two:7835".into(),
        },
    ];
    let membership = Membership::new("one", hosts);
    let matching = matching.with_membership(membership.clone());
    let set_count = |count| {
        matching
            .frontend
            .state
            .admin
            .dynamic_config
            .lock()
            .unwrap()
            .update(
                false,
                "matching.numTasklistWritePartitions",
                vec![quota_config(count, None)],
            )
    };
    let request = |domain: &str, name: &str| {
        Request::new(m::ListTaskListPartitionsRequest {
            domain: domain.into(),
            task_list: Some(p::TaskList {
                name: name.into(),
                ..Default::default()
            }),
        })
    };
    set_count(3);
    // A child request still enumerates its root, not just loaded partitions.
    let response = matching
        .list_task_list_partitions(request("domain", "/__cadence_sys/queue/9"))
        .await
        .unwrap()
        .into_inner();
    let expected = ["queue", "/__cadence_sys/queue/1", "/__cadence_sys/queue/2"]
        .into_iter()
        .map(|key| p::TaskListPartitionMetadata {
            key: key.into(),
            owner_host_name: membership.owner(key).unwrap().address,
        })
        .collect::<Vec<_>>();
    assert_eq!(response.activity_task_list_partitions, expected);
    assert_eq!(response.decision_task_list_partitions, expected);
    // Go engine.go:1192 discards host lookup errors and keeps each key.
    membership.replace(vec![]);
    let response = matching
        .list_task_list_partitions(request("domain", "queue"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response
            .decision_task_list_partitions
            .iter()
            .map(|v| (v.key.as_str(), v.owner_host_name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("queue", ""),
            ("/__cadence_sys/queue/1", ""),
            ("/__cadence_sys/queue/2", "")
        ]
    );
    for count in [0, -2] {
        set_count(count);
        let response = matching
            .list_task_list_partitions(request("domain", ""))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.activity_task_list_partitions,
            vec![p::TaskListPartitionMetadata::default()]
        );
    }
    assert_eq!(
        matching
            .list_task_list_partitions(request("missing", "/__cadence_sys/bad/0"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        matching
            .list_task_list_partitions(request("domain", "/__cadence_sys/bad/0"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
    assert_eq!(
        matching
            .list_task_list_partitions(request("domain", "/__cadence_sys/bad/0"))
            .await
            .unwrap_err()
            .message(),
        "invalid partitioned task list name /__cadence_sys/bad/0"
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
async fn engine_apis_durable_describe_and_domain_listing_share_go_manager_fields() {
    use cadence_common::poller::Info;
    let (_dir, _store, matching, key) = fixture();
    let normal = p::TaskList {
        name: "queue".into(),
        ..Default::default()
    };
    let queue = matching
        .durable_queue(&key.domain_id.to_string(), &normal, 0)
        .await
        .unwrap();
    let config = cadence_persistence::StoredPartitionConfig {
        version: 123,
        read_partitions: HashMap::from([(0, vec!["zone-a".into()])]),
        write_partitions: HashMap::from([(0, vec!["zone-a".into()])]),
    };
    queue.partitions.push(&config);
    {
        let mut pollers = queue.pollers.lock().unwrap();
        let now = matching_now();
        for (id, group, rate) in [
            ("a", "zone-a", 7.5),
            ("b", "unconfigured", 2.5),
            ("c", "", 1.0),
        ] {
            pollers.start(
                id,
                Info {
                    identity: id.into(),
                    isolation_group: group.into(),
                    rate_per_second: rate,
                },
                now,
            );
        }
    }
    matching
        .frontend
        .state
        .admin
        .dynamic_config
        .lock()
        .unwrap()
        .update(
            false,
            "system.allIsolationGroups",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    br#"["zone-a","zone-empty"]"#.to_vec(),
                )),
                filters: vec![],
            }],
        );
    let describe = |include_task_list_status| {
        matching.describe_task_list(Request::new(m::DescribeTaskListRequest {
            domain_id: key.domain_id.to_string(),
            request: Some(p::DescribeTaskListRequest {
                task_list: Some(normal.clone()),
                task_list_type: 1,
                include_task_list_status,
                ..Default::default()
            }),
        }))
    };
    let described = describe(true).await.unwrap().into_inner();
    assert_eq!(
        described.task_list.as_ref().unwrap().kind,
        1,
        "Go describes the manager's Normal kind even when request kind is unspecified"
    );
    assert_eq!(
        described
            .pollers
            .iter()
            .map(|v| (v.identity.as_str(), v.rate_per_second))
            .collect::<Vec<_>>(),
        vec![("c", 1.0), ("b", 2.5), ("a", 7.5)]
    );
    let metrics = &described
        .task_list_status
        .as_ref()
        .unwrap()
        .isolation_group_metrics;
    assert_eq!(metrics.len(), 2);
    assert_eq!(metrics["zone-a"].poller_count, 1);
    assert_eq!(metrics["zone-empty"].poller_count, 0);
    assert_eq!(metrics["zone-empty"].new_tasks_per_second, 0.0);
    let without_status = describe(false).await.unwrap().into_inner();
    assert!(without_status.task_list_status.is_none());
    let listed = matching
        .get_task_lists_by_domain(Request::new(m::GetTaskListsByDomainRequest {
            domain: "domain".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.decision_task_list_map["queue"], without_status);
    assert_eq!(
        listed.decision_task_list_map["queue"].partition_config,
        Some(partition_config_to_proto(config))
    );
    assert!(listed.activity_task_list_map.is_empty());
    assert_eq!(
        matching
            .get_task_lists_by_domain(Request::new(m::GetTaskListsByDomainRequest {
                domain: "missing".into()
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    // The Thrift handler delegates to the same durable path and retains maps,
    // poller order, kind and absent status on the wire. The pinned Thrift
    // IDL omits partition config (unlike gRPC), as does Go's mapper.
    use cadence_thrift::value::{Struct, Value};
    let args = Struct {
        fields: vec![(
            1,
            Value::Struct(Struct {
                fields: vec![(10, Value::Bytes(b"domain".to_vec()))],
            }),
        )],
    };
    let (wire, error) = crate::thrift_matching::dispatch(
        &matching,
        "MatchingService::GetTaskListsByDomain",
        &args,
        std::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(error.is_none());
    let Some(Value::Struct(response)) = wire.get(0) else {
        panic!("missing result")
    };
    let Some(Value::Map(decisions)) = response.get(10) else {
        panic!("missing decision map")
    };
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].0, Value::Bytes(b"queue".to_vec()));
    let Value::Struct(description) = &decisions[0].1 else {
        panic!("missing description")
    };
    assert!(description.get(20).is_none());
    assert!(
        description.get(40).is_none(),
        "pinned Thrift IDL has no partition config field"
    );
    let Some(Value::List(pollers)) = description.get(10) else {
        panic!("missing pollers")
    };
    assert_eq!(pollers.len(), 3);
    let Value::Struct(first) = &pollers[0] else {
        panic!("missing poller")
    };
    assert_eq!(first.get(20), Some(&Value::Bytes(b"c".to_vec())));
    assert_eq!(first.get(30), Some(&Value::F64(1.0)));
    // Durable rows survive unloading, but Go only lists currently loaded managers.
    matching
        .queues
        .as_ref()
        .unwrap()
        .unload(key.clone(), queue, matching_now())
        .await
        .unwrap();
    assert!(matching
        .get_task_lists_by_domain(Request::new(m::GetTaskListsByDomainRequest {
            domain: "domain".into()
        }))
        .await
        .unwrap()
        .into_inner()
        .decision_task_list_map
        .is_empty());
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_apis_partition_spectator_failure_is_empty_owner_like_go() {
    let (_dir, _store, matching, _key) = fixture();
    let router = crate::matching_shard_routing::MatchingShardRouter::new(
        crate::matching_membership::Membership::new("self", vec![]),
        Some(Arc::new(FailingShardOwnerLookup)),
        Arc::new(|| 100),
        Arc::new(|| false),
    );
    let matching = matching.with_shard_router(router);
    let response = matching
        .list_task_list_partitions(Request::new(m::ListTaskListPartitionsRequest {
            domain: "domain".into(),
            task_list: list(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.decision_task_list_partitions,
        vec![p::TaskListPartitionMetadata {
            key: "queue".into(),
            owner_host_name: String::new(),
        }]
    );
    assert_eq!(
        response.activity_task_list_partitions,
        response.decision_task_list_partitions
    );
}

#[tokio::test]
async fn engine_apis_describe_identifier_errors_precede_domain_lookup_like_go() {
    let (_dir, _store, matching, key) = fixture();
    let error = matching
        .describe_task_list(Request::new(m::DescribeTaskListRequest {
            domain_id: Uuid::from_u128(999).to_string(),
            request: Some(p::DescribeTaskListRequest {
                task_list: Some(p::TaskList {
                    name: "/__cadence_sys/bad/0".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(
        error.message(),
        "invalid partitioned task list name /__cadence_sys/bad/0"
    );
    let empty = matching
        .describe_task_list(Request::new(m::DescribeTaskListRequest {
            domain_id: key.domain_id.to_string(),
            request: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        empty.task_list,
        Some(p::TaskList {
            name: String::new(),
            kind: 1,
            ..Default::default()
        })
    );
    assert!(empty.task_list_status.is_none());
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
}

/// Parent partition owner that fails every forwarded Add with a ServiceBusy or an internal error.
#[derive(Clone)]
struct FailingAddParent {
    busy: bool,
}

impl FailingAddParent {
    fn failure(&self) -> Status {
        if self.busy {
            crate::grpc::detailed_status(
                tonic::Code::ResourceExhausted,
                "parent busy",
                "ServiceBusyError",
                cadence_proto::api::ServiceBusyError { reason: String::new() },
            )
        } else {
            Status::internal("parent failed")
        }
    }
}

#[tonic::async_trait]
impl MatchingApi for FailingAddParent {
    async fn add_decision_task(&self, _: Request<m::AddDecisionTaskRequest>) -> Result<Response<m::AddDecisionTaskResponse>, Status> {
        Err(self.failure())
    }
    async fn add_activity_task(&self, _: Request<m::AddActivityTaskRequest>) -> Result<Response<m::AddActivityTaskResponse>, Status> {
        Err(self.failure())
    }
    async fn poll_for_decision_task(&self, _: Request<m::PollForDecisionTaskRequest>) -> Result<Response<m::PollForDecisionTaskResponse>, Status> {
        Err(Status::unimplemented("poll_for_decision_task"))
    }
    async fn poll_for_activity_task(&self, _: Request<m::PollForActivityTaskRequest>) -> Result<Response<m::PollForActivityTaskResponse>, Status> {
        Err(Status::unimplemented("poll_for_activity_task"))
    }
}

/// Sends one child Add that must sync-forward to a failing parent; returns the child's metric sink.
async fn sync_forward_to_failing_parent(busy: bool) -> Arc<crate::matching_metrics::RecordingMetricSink> {
    use cadence_common::membership::{Host, Ring};
    use crate::matching_membership::Membership;
    let history = start_starting_history().await;
    let (_dir, _store, child, key) = fixture_with_history(&history);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder().add_service(m::matching_api_server::MatchingApiServer::new(FailingAddParent { busy }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)).await.unwrap();
    });
    let mut hosts = vec![
        Host { address: "node-0:7935".into(), identity: "node-0".into(), grpc_address: String::new() },
        Host { address: "node-1:7935".into(), identity: "node-1".into(), grpc_address: String::new() },
    ];
    let ring = Ring::new(hosts.clone());
    let root_id = ring.lookup("queue").unwrap().identity().to_owned();
    let child_name = (1..=20).map(|id| format!("/__cadence_sys/queue/{id}")).find(|name| ring.lookup(name).unwrap().identity() != root_id).unwrap();
    let child_id = ring.lookup(&child_name).unwrap().identity().to_owned();
    for host in &mut hosts { host.grpc_address = endpoint.clone(); }
    let sink = Arc::new(crate::matching_metrics::RecordingMetricSink::default());
    let child = child.with_membership(Membership::new(child_id, hosts)).with_metrics(sink.clone());
    child.add_decision_task(Request::new(m::AddDecisionTaskRequest {
        domain_id: key.domain_id.to_string(), workflow_execution: execution(),
        task_list: Some(p::TaskList { name: child_name, kind: 1, ..Default::default() }),
        schedule_id: 71, ..Default::default()
    })).await.unwrap();
    child.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
    server.abort();
    sink
}

#[tokio::test]
async fn matching_rpc_sync_forward_parent_service_busy_counts_throttle_metric() {
    use crate::matching_metrics::{MetricKind, MetricValue, SYNC_FORWARD_TASK_THROTTLE_ERRORS};
    // Go matcher.go:230 increments SyncMatchForwardTaskThrottleErrorPerTasklist on ErrForwarderSlowDown.
    let sink = sync_forward_to_failing_parent(true).await;
    let throttled: Vec<_> = sink.events().into_iter().filter(|event| event.name == SYNC_FORWARD_TASK_THROTTLE_ERRORS).collect();
    assert_eq!(throttled.len(), 1);
    assert_eq!(throttled[0].kind, MetricKind::Counter);
    assert_eq!(throttled[0].value, MetricValue::Count(1));
}

#[tokio::test]
async fn matching_rpc_sync_forward_parent_other_error_does_not_count_throttle_metric() {
    use crate::matching_metrics::SYNC_FORWARD_TASK_THROTTLE_ERRORS;
    // Only ServiceBusy maps to ErrForwarderSlowDown; other parent failures leave the counter untouched.
    let sink = sync_forward_to_failing_parent(false).await;
    assert!(sink.events().iter().all(|event| event.name != SYNC_FORWARD_TASK_THROTTLE_ERRORS));
}

// Go handler_test.go: every handler RPC returns ServiceBusyError("Matching host rps exceeded")
// when the host limiter denies, before touching the engine.
fn denied_after_first_state(domain_id: Uuid) -> crate::handlers::AppState {
    let state = crate::handlers::AppState::default();
    state.domains.create(
        cadence_api::types::DomainInfo { name: "quota-domain".into(), uuid: domain_id.to_string(), ..Default::default() },
        Default::default(), Default::default(), false,
    ).unwrap();
    {
        let mut config = state.admin.dynamic_config.lock().unwrap();
        config.update(false, "matching.rps", vec![quota_config(100, None)]);
        config.update(false, "matching.domainrps", vec![quota_config(1, Some("quota-domain"))]);
    }
    state
}

fn assert_host_busy(status: tonic::Status) {
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(status.message(), "Matching host rps exceeded");
    assert!(!status.details().is_empty(), "Go-compatible ServiceBusyError detail is required");
}

#[tokio::test]
async fn go_handler_describe_task_list_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9101);
    let matching = Matching::new(denied_after_first_state(domain_id));
    let req = || Request::new(m::DescribeTaskListRequest {
        domain_id: domain_id.to_string(),
        request: Some(p::DescribeTaskListRequest { task_list: Some(p::TaskList { name: "test-task-list".into(), ..Default::default() }), ..Default::default() }),
    });
    let _ = matching.describe_task_list(req()).await;
    assert_host_busy(matching.describe_task_list(req()).await.unwrap_err());
}

#[tokio::test]
async fn go_handler_list_task_list_partitions_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9102);
    let matching = Matching::new(denied_after_first_state(domain_id));
    let req = || Request::new(m::ListTaskListPartitionsRequest {
        domain: "quota-domain".into(),
        task_list: Some(p::TaskList { name: "test-task-list".into(), ..Default::default() }),
    });
    let _ = matching.list_task_list_partitions(req()).await;
    assert_host_busy(matching.list_task_list_partitions(req()).await.unwrap_err());
}

#[tokio::test]
async fn go_handler_update_task_list_partition_config_rate_limited_returns_service_busy() {
    let domain_id = Uuid::from_u128(9103);
    let matching = Matching::new(denied_after_first_state(domain_id));
    let req = || Request::new(m::UpdateTaskListPartitionConfigRequest {
        domain_id: domain_id.to_string(),
        task_list: Some(p::TaskList { name: "test-task-list".into(), ..Default::default() }),
        ..Default::default()
    });
    let _ = matching.update_task_list_partition_config(req()).await;
    assert_host_busy(matching.update_task_list_partition_config(req()).await.unwrap_err());
}

#[tokio::test]
async fn go_handler_refresh_partition_config_rejects_missing_tasklist_and_non_normal_kinds() {
    let (_dir, _store, matching, _key) = fixture();
    let domain = Uuid::from_u128(1).to_string();
    let missing = matching.refresh_task_list_partition_config(Request::new(m::RefreshTaskListPartitionConfigRequest {
        domain_id: domain.clone(), task_list: None, ..Default::default()
    })).await.unwrap_err();
    assert_eq!(missing.code(), tonic::Code::InvalidArgument);
    assert_eq!(missing.message(), "TaskList is not set.");
    for kind in [2, 3] {
        let err = matching.refresh_task_list_partition_config(Request::new(m::RefreshTaskListPartitionConfigRequest {
            domain_id: domain.clone(),
            task_list: Some(p::TaskList { name: "tl".into(), kind, ..Default::default() }),
            ..Default::default()
        })).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "Only normal tasklist's partition config can be updated.");
    }
}

// Go handler_test.go TestDescribeTaskList "Success case": a healthy engine returns the
// describe response to the caller with no error.
#[tokio::test]
async fn go_handler_describe_task_list_success_returns_response() {
    let (_dir, _store, matching, key) = fixture();
    let response = matching
        .describe_task_list(Request::new(m::DescribeTaskListRequest {
            domain_id: key.domain_id.to_string(),
            request: Some(p::DescribeTaskListRequest {
                task_list: list(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.pollers.is_empty());
}

// Go handler_test.go TestDescribeTaskList / TestListTaskListPartitions / TestUpdateTaskListPartitionConfig
// "Error case - ... failed": an engine error must come back as InternalServiceError carrying the
// engine's message. The `engine_fault` seam makes the engine call fail with the Go mock's message.
#[tokio::test]
async fn go_handler_describe_task_list_engine_error_is_internal_service_error() {
    let (_dir, _store, matching, key) = fixture();
    *matching.engine_fault.lock().unwrap() = Some(("DescribeTaskList", "describe-tasklist-error".into()));
    let status = matching
        .describe_task_list(Request::new(m::DescribeTaskListRequest {
            domain_id: key.domain_id.to_string(),
            request: Some(p::DescribeTaskListRequest {
                task_list: list(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "describe-tasklist-error");
}

#[tokio::test]
async fn go_handler_list_task_list_partitions_engine_error_is_internal_service_error() {
    let (_dir, _store, matching, _key) = fixture();
    *matching.engine_fault.lock().unwrap() = Some(("ListTaskListPartitions", "list-tasklist-partitions-error".into()));
    let status = matching
        .list_task_list_partitions(Request::new(m::ListTaskListPartitionsRequest {
            domain: "domain".into(),
            task_list: list(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "list-tasklist-partitions-error");
}

#[tokio::test]
async fn go_handler_update_task_list_partition_config_engine_error_is_internal_service_error() {
    let (_dir, _store, matching, key) = fixture();
    *matching.engine_fault.lock().unwrap() = Some(("UpdateTaskListPartitionConfig", "update-tasklist-error".into()));
    let status = matching
        .update_task_list_partition_config(Request::new(m::UpdateTaskListPartitionConfigRequest {
            domain_id: key.domain_id.to_string(),
            task_list: list(),
            task_list_type: 1,
            partition_config: Some(native_config(1, &[0, 1], &[0, 1])),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "update-tasklist-error");
}

#[tokio::test]
#[ignore = "parity gap: Rust Matching has no injectable engine failure, so the Go mock error (errors.New(msg) -> InternalServiceError{Message: msg}) cannot be reproduced for refresh_task_list_partition_config"]
async fn go_handler_refresh_partition_config_engine_error_is_internal_service_error() {
    panic!("parity gap: no injectable engine failure seam");
}

// engine_integration_test.go AddTasksTest (non-forwarded): 111 adds all persist.
#[tokio::test]
async fn go_engine_add_decision_and_activity_tasks_persist_every_task() {
    let (_dir, store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    for i in 0..111i64 {
        matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: domain.clone(), workflow_execution: execution(), task_list: list(),
            schedule_id: i * 3, ..Default::default()
        })).await.unwrap();
        matching.add_activity_task(Request::new(m::AddActivityTaskRequest {
            domain_id: domain.clone(), source_domain_id: domain.clone(), workflow_execution: execution(),
            task_list: list(), schedule_id: i * 3, ..Default::default()
        })).await.unwrap();
    }
    assert_eq!(store.task_list_size(&key, -1).unwrap(), 111);
    assert_eq!(store.task_list_size(&TaskListKey { task_type: 1, ..key.clone() }, -1).unwrap(), 111);
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

// engine_integration_test.go AddAndPollTasks (decision, no isolation): add 6, poll all, FIFO, store drained.
#[tokio::test]
async fn go_engine_add_then_poll_decision_tasks_delivers_each_task_and_drains_store() {
    let address = start_starting_history().await;
    let (_dir, store, matching, key) = fixture_with_history(&address);
    let domain = key.domain_id.to_string();
    for i in 0..6i64 {
        matching.add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: domain.clone(), workflow_execution: execution(), task_list: list(),
            schedule_id: i * 3, ..Default::default()
        })).await.unwrap();
    }
    assert_eq!(store.task_list_size(&key, -1).unwrap(), 6);
    let mut seen = Vec::new();
    for _ in 0..6 {
        let response = matching.poll_for_decision_task(timed_poll(Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain.clone(), poller_id: "add-poll".into(),
            request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
        }))).await.unwrap().into_inner();
        assert!(!response.task_token.is_empty());
        seen.push(response.started_event_id);
    }
    assert_eq!(seen, vec![1, 4, 7, 10, 13, 16]);
    // Go also asserts the store is drained; Rust deletes acked rows via batched GC, so assert no redelivery instead.
    let mut again = Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain.clone(), poller_id: "add-poll".into(),
        request: Some(p::PollForDecisionTaskRequest { task_list: list(), ..Default::default() }), ..Default::default()
    });
    again.set_timeout(Duration::from_secs(1));
    assert!(matching.poll_for_decision_task(again).await.unwrap().into_inner().task_token.is_empty());
    matching.queues.as_ref().unwrap().shutdown(matching_now()).await.unwrap();
}

/// Go never forwards a sticky query: `newForwarder` is only built when
/// `isFowardingAllowed` holds, which excludes sticky lists
/// (`task_list_manager.go:237`, `:891-892`). The nil forwarder yields the
/// never-ready `noopForwarderTokenC` (`matcher.go:798-800`), so `OfferQuery`
/// waits on the local queue only. A partitioned sticky name must behave the same.
fn sticky_partition_list() -> p::TaskList {
    p::TaskList {
        name: "/__cadence_sys/queue/1".into(),
        kind: 2,
        ..Default::default()
    }
}

fn sticky_query_request(
    domain: String,
    task_list: p::TaskList,
    timeout: Duration,
) -> Request<m::QueryWorkflowRequest> {
    let mut request = Request::new(m::QueryWorkflowRequest {
        request: Some(p::QueryWorkflowRequest {
            domain: "domain".into(),
            workflow_execution: execution(),
            query: Some(p::WorkflowQuery {
                query_type: "ping".into(),
                query_args: None,
            }),
            ..Default::default()
        }),
        domain_id: domain,
        task_list: Some(task_list),
        forwarded_from: String::new(),
    });
    request.set_timeout(timeout);
    request
}

fn decision_poll(
    matching: &Matching,
    domain: &str,
    list: p::TaskList,
    poller: &str,
    timeout: Duration,
) -> tokio::task::JoinHandle<Result<tonic::Response<m::PollForDecisionTaskResponse>, tonic::Status>> {
    let worker = matching.clone();
    let domain = domain.to_owned();
    let poller = poller.to_owned();
    tokio::spawn(async move {
        let mut request = Request::new(m::PollForDecisionTaskRequest {
            domain_id: domain,
            poller_id: poller.clone(),
            request: Some(p::PollForDecisionTaskRequest {
                task_list: Some(list),
                identity: poller,
                ..Default::default()
            }),
            ..Default::default()
        });
        request.set_timeout(timeout);
        worker.poll_for_decision_task(request).await
    })
}

async fn wait_for_parked_poller(queue: &crate::matching_registry::QueueHandle) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !queue.queries.has_waiters() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// Go `OfferQuery` offers to the sticky partition's parked poller first. The
/// root poller must never see the query, because the sticky list is not forwarded.
#[tokio::test]
async fn matching_rpc_sticky_partition_query_reaches_only_the_sticky_poller() {
    // A delivered query makes the poll load mutable state from history
    // (grpc_matching.rs `query_mutable_state_with_deadline`), so serve it.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent_poll = decision_poll(&matching, &domain, list().unwrap(), "parent-worker", Duration::from_secs(8));
    let root = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    wait_for_parked_poller(&root).await;
    let child = sticky_partition_list();
    let child_queue = matching.durable_queue(&domain, &child, 0).await.unwrap();
    let sticky_poll = decision_poll(&matching, &domain, child.clone(), "sticky-worker", Duration::from_secs(8));
    wait_for_parked_poller(&child_queue).await;
    let query = tokio::spawn({
        let querier = matching.clone();
        let request = sticky_query_request(domain.clone(), child, Duration::from_secs(8));
        async move { querier.query_workflow(request).await }
    });
    let delivered = tokio::time::timeout(Duration::from_secs(5), sticky_poll)
        .await
        .expect("sticky query should reach the sticky poller")
        .unwrap()
        .expect("poll should succeed")
        .into_inner();
    assert_eq!(
        delivered.query.as_ref().map(|q| q.query_type.clone()),
        Some("ping".to_string())
    );
    assert!(
        !parent_poll.is_finished(),
        "a sticky query must never reach the parent partition"
    );
    query.abort();
    parent_poll.abort();
}

/// Go `_stickyPollerUnavailableWindow` passes (the sticky poll is recent) but
/// nothing is parked, so `OfferQuery` blocks on the local queue until the RPC
/// context ends. It must not forward to the parent, and the error is the raw
/// context error that Go's `handleErr` maps to `InternalServiceError`.
#[tokio::test]
async fn matching_rpc_sticky_partition_query_without_parked_poller_never_forwards() {
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    let parent_poll = decision_poll(&matching, &domain, list().unwrap(), "parent-worker", Duration::from_secs(8));
    let root = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    wait_for_parked_poller(&root).await;
    let child = sticky_partition_list();
    let recent = decision_poll(&matching, &domain, child.clone(), "sticky-worker", Duration::from_millis(300));
    recent
        .await
        .unwrap()
        .expect("an expired sticky poll returns an empty response");
    let error = matching
        .query_workflow(sticky_query_request(
            domain.clone(),
            child,
            Duration::from_millis(500),
        ))
        .await
        .expect_err("with no parked sticky poller the query must wait locally until its deadline");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        !parent_poll.is_finished(),
        "a sticky query must never reach the parent partition"
    );
    parent_poll.abort();
}

/// Go `engine.go:562` fails fast with `StickyWorkerUnavailableError` before any
/// forwarding when no sticky poller is recent, including for a partitioned name.
#[tokio::test]
async fn matching_rpc_sticky_partition_query_without_any_sticky_poller_fails_fast() {
    let (_dir, _store, matching, key) = fixture();
    let domain = key.domain_id.to_string();
    let parent_poll = decision_poll(&matching, &domain, list().unwrap(), "parent-worker", Duration::from_secs(8));
    let root = matching.durable_queue(&domain, &list().unwrap(), 0).await.unwrap();
    wait_for_parked_poller(&root).await;
    let error = matching
        .query_workflow(sticky_query_request(
            domain.clone(),
            sticky_partition_list(),
            Duration::from_secs(8),
        ))
        .await
        .expect_err("a sticky list without a recent poller must fail fast");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("sticky worker is unavailable"));
    assert!(
        !parent_poll.is_finished(),
        "a sticky query must never reach the parent partition"
    );
    parent_poll.abort();
}
