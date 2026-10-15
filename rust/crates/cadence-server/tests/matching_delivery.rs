//! Run by the original-Go service harness; never substitutes an embedded engine.
use cadence_persistence::{
    matching_blob::{BlobEncoding, TaskData},
    matching_queue::DurableTaskQueue,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
};
use cadence_proto::api as p;
use cadence_server::{
    matching_dispatch::TaskDispatcher,
    matching_history::{start_error_kind, MatchingHistoryClient, StartErrorKind},
};
use prost::Message;
use serde::Deserialize;
use std::{
    num::NonZeroU32,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
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
    let queue = Arc::new(
        DurableTaskQueue::open(
            Arc::new(store),
            key,
            0,
            NonZeroU32::new(100000).unwrap(),
            now,
        )
        .unwrap(),
    );
    let data = TaskData {
        workflow_id: fixture.workflow_id,
        run_id: Some(
            Uuid::parse_str(&fixture.run_id)
                .unwrap()
                .as_bytes()
                .to_vec(),
        ),
        schedule_id: fixture.schedule_id,
        ..Default::default()
    }
    .at_creation(now, 0);
    let original = queue.append(&[data], now).unwrap().remove(0);
    let channel =
        tonic::transport::Endpoint::from_shared(format!("http://{}", fixture.history_peer))
            .unwrap()
            .connect()
            .await
            .unwrap();
    let client = MatchingHistoryClient::new(channel, Duration::from_secs(1), 32 * 1024 * 1024);
    let list = Some(p::TaskList {
        name: fixture.task_list,
        ..Default::default()
    });
    let verify = client.clone();
    let domain_id = fixture.domain_id.clone();
    let poll_domain = fixture.domain.clone();
    let poll_list = list.clone();
    let dispatcher = TaskDispatcher::new(queue.clone(), client, fixture.domain_id);
    let bytes = if fixture.kind == "activity" {
        dispatcher
            .poll_activity(
                p::PollForActivityTaskRequest {
                    domain: fixture.domain,
                    task_list: list,
                    identity: "rust-durable-matching".into(),
                    ..Default::default()
                },
                Duration::from_secs(15),
            )
            .await
            .unwrap()
            .encode_to_vec()
    } else {
        dispatcher
            .poll_decision(
                p::PollForDecisionTaskRequest {
                    domain: fixture.domain,
                    task_list: list,
                    identity: "rust-durable-matching".into(),
                    ..Default::default()
                },
                Duration::from_secs(15),
            )
            .await
            .unwrap()
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
    std::fs::write(fixture.output, bytes).unwrap();
}
