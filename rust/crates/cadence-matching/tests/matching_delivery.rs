//! Run by the original-Go service harness; never substitutes an embedded engine.
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
};
use cadence_proto::{api as p, matching as m};
use cadence_matching::{
    grpc_matching::Matching,
    matching_history::{start_error_kind, MatchingHistoryClient, StartErrorKind},
    matching_registry::QueueRegistry,
};
use prost::Message;
use serde::Deserialize;
use std::{
    num::NonZeroU32,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tonic::Request;
use uuid::Uuid;
#[derive(Deserialize)]
struct Fixture {
    domain: String,
    domain_id: String,
    workflow_id: String,
    run_id: String,
    task_list: String,
    history_peer: String,
    kind: String,
    schedule_id: i64,
    database: String,
    output: String,
    #[serde(default)]
    listen: Option<String>,
    #[serde(default)]
    stop_file: Option<String>,
    #[serde(default)]
    idle_trigger: Option<String>,
    #[serde(default)]
    idle_done: Option<String>,
    #[serde(default)]
    isolation_groups: Vec<String>,
}
#[tokio::test]
#[ignore = "requires original Go History; run tests/run-go-service-baseline.py --matching-delivery"]
async fn matching_delivery_go_history() {
    let fixture: Fixture = serde_json::from_slice(
        &std::fs::read(
            std::env::var("CADENCE_MATCHING_DELIVERY_FIXTURE").expect("Go fixture path"),
        )
        .unwrap(),
    )
    .unwrap();
    let store = if Path::new(&fixture.database).exists() {
        SqliteTaskStore::open(&fixture.database, BlobEncoding::ThriftRWSnappy)
    } else {
        SqliteTaskStore::create(&fixture.database, BlobEncoding::ThriftRWSnappy)
    }
    .unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let key = TaskListKey {
        domain_id: Uuid::parse_str(&fixture.domain_id).unwrap(),
        name: fixture.task_list.clone(),
        task_type: if fixture.kind == "activity" { 1 } else { 0 },
    };
    let store = Arc::new(store);
    let channel =
        tonic::transport::Endpoint::from_shared(format!("http://{}", fixture.history_peer))
            .unwrap()
            .connect()
            .await
            .unwrap();
    let client = MatchingHistoryClient::new(channel, Duration::from_secs(1), 32 * 1024 * 1024);
    let registry = QueueRegistry::new(
        store.clone(),
        client.clone(),
        NonZeroU32::new(100000).unwrap(),
    );
    let state = cadence_service_core::handlers::AppState::default();
    if !fixture.isolation_groups.is_empty() {
        let blob = |value: serde_json::Value| Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, serde_json::to_vec(&value).unwrap()));
        let mut config = state.admin.dynamic_config.lock().unwrap();
        for (key, value) in [
            ("system.allIsolationGroups", serde_json::json!(fixture.isolation_groups)),
            ("system.enableTasklistIsolation", serde_json::json!(true)),
        ] {
            config.update(false, key, vec![cadence_api::admin::DynamicConfigValue { value: blob(value), filters: vec![] }]);
        }
    }
    state
        .domains
        .create(
            cadence_api::types::DomainInfo {
                name: fixture.domain.clone(),
                uuid: fixture.domain_id.clone(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    let matching = Matching::with_durable_queues(state, registry.clone(), Duration::from_secs(15));
    // Exercise the actual Matching protobuf listener and generated client, not
    // only the queue/dispatcher library calls used by the earlier checkpoint.
    let listener =
        tokio::net::TcpListener::bind(fixture.listen.as_deref().unwrap_or("127.0.0.1:0"))
            .await
            .unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(m::matching_api_server::MatchingApiServer::new(matching))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    if fixture.kind == "server" {
        let (stop_pump, shutdown) = tokio::sync::oneshot::channel();
        let controller = registry.clone();
        let pump = tokio::spawn(async move {
            controller
                .maintain_until(Duration::from_secs(60), async {
                    let _ = shutdown.await;
                })
                .await
        });
        std::fs::write(&fixture.output, address.to_string()).unwrap();
        let stop = fixture.stop_file.expect("server stop file");
        while !Path::new(&stop).exists() {
            if let Some(trigger) = &fixture.idle_trigger {
                if Path::new(trigger).exists() {
                    // Test clock advance: exercise actual idle teardown without
                    // waiting five minutes. No workflow/business logic is changed.
                    let future = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64
                        + 300_000_000_000;
                    registry.sweep_idle(future, false).await.unwrap();
                    std::fs::remove_file(trigger).unwrap();
                    std::fs::write(
                        fixture.idle_done.as_ref().expect("idle completion file"),
                        "done",
                    )
                    .unwrap();
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        stop_pump.send(()).unwrap();
        pump.await.unwrap().unwrap();
        server.abort();
        return;
    }
    let mut matching =
        m::matching_api_client::MatchingApiClient::connect(format!("http://{address}"))
            .await
            .unwrap();
    let execution = Some(p::WorkflowExecution {
        workflow_id: fixture.workflow_id.clone(),
        run_id: fixture.run_id.clone(),
    });
    let add_list = Some(p::TaskList {
        name: fixture.task_list.clone(),
        kind: 1,
        ..Default::default()
    });
    if fixture.kind == "activity" {
        matching
            .add_activity_task(Request::new(m::AddActivityTaskRequest {
                domain_id: fixture.domain_id.clone(),
                source_domain_id: fixture.domain_id.clone(),
                workflow_execution: execution,
                task_list: add_list,
                schedule_id: fixture.schedule_id,
                ..Default::default()
            }))
            .await
            .unwrap();
    } else {
        matching
            .add_decision_task(Request::new(m::AddDecisionTaskRequest {
                domain_id: fixture.domain_id.clone(),
                workflow_execution: execution,
                task_list: add_list,
                schedule_id: fixture.schedule_id,
                ..Default::default()
            }))
            .await
            .unwrap();
    }
    let original = store.get_tasks(&key, 0, i64::MAX, 10).unwrap().remove(0);
    let queue = registry.get(key, 0, now).await.unwrap().queue;
    let list = Some(p::TaskList {
        name: fixture.task_list,
        ..Default::default()
    });
    let verify = client.clone();
    let domain_id = fixture.domain_id.clone();
    let poll_domain = fixture.domain.clone();
    let poll_list = list.clone();
    let bytes = if fixture.kind == "activity" {
        matching
            .poll_for_activity_task(Request::new(m::PollForActivityTaskRequest {
                domain_id: fixture.domain_id.clone(),
                poller_id: "go-boundary-poller".into(),
                request: Some(p::PollForActivityTaskRequest {
                    domain: fixture.domain,
                    task_list: list,
                    identity: "rust-durable-matching".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .encode_to_vec()
    } else {
        matching
            .poll_for_decision_task(Request::new(m::PollForDecisionTaskRequest {
                domain_id: fixture.domain_id.clone(),
                poller_id: "go-boundary-poller".into(),
                request: Some(p::PollForDecisionTaskRequest {
                    domain: fixture.domain,
                    task_list: list,
                    identity: "rust-durable-matching".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .encode_to_vec()
    };
    // Confirm actual Go error envelopes classify terminal duplicates/missing
    // executions correctly; a bare gRPC status code is insufficient.
    let mut missing = original.clone();
    missing.data.workflow_id = format!("missing-{}", Uuid::new_v4());
    for task in [&original, &missing] {
        let error = if fixture.kind == "activity" {
            verify
                .start_activity(
                    &domain_id,
                    task,
                    p::PollForActivityTaskRequest {
                        domain: poll_domain.clone(),
                        task_list: poll_list.clone(),
                        identity: "terminal-error-probe".into(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err()
        } else {
            verify
                .start_decision(
                    &domain_id,
                    task,
                    p::PollForDecisionTaskRequest {
                        domain: poll_domain.clone(),
                        task_list: poll_list.clone(),
                        identity: "terminal-error-probe".into(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err()
        };
        assert_eq!(
            start_error_kind(&error),
            StartErrorKind::Discard,
            "unexpected Go terminal envelope: {error:?}"
        );
    }
    queue.persist_ack(now).unwrap();
    server.abort();
    std::fs::write(fixture.output, bytes).unwrap();
}
