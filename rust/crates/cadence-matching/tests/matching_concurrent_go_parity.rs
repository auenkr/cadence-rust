//! Durable concurrency and range-fencing counterparts for
//! `go/service/matching/handler/engine_integration_test.go`.
//!
//! The public Matching API is used for concurrent adds; task rows are checked
//! in the same SQLite store used by Matching. The range-stealing test exercises
//! the SQLite lease and write boundary that fences stale engine owners.
use cadence_api::types::DomainInfo;
use cadence_matching::{
    grpc_matching::Matching,
    matching_history::MatchingHistoryClient,
    matching_registry::QueueRegistry,
};
use cadence_persistence::{
    matching_blob::{BlobEncoding, TaskData},
    matching_sqlite::{SqliteTaskStore, TaskListKey},
    matching_task_store::AllocatedTask,
};
use cadence_proto::{api as p, matching as m};
use cadence_service_core::handlers::AppState;
use m::matching_api_server::MatchingApi;
use std::{num::NonZeroU32, sync::Arc, time::Duration};
use tonic::Request;
use uuid::Uuid;

const DOMAIN_ID: Uuid = Uuid::from_u128(1);

fn fixture() -> (tempfile::TempDir, Arc<SqliteTaskStore>, Arc<Matching>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("matching.db"), BlobEncoding::ThriftRW)
            .unwrap(),
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
    assert_eq!(tasks.len(), 2_000, "concurrent adds were lost or duplicated");
    let mut workflow_ids: Vec<_> = tasks.iter().map(|task| task.data.workflow_id.as_str()).collect();
    workflow_ids.sort_unstable();
    workflow_ids.dedup();
    assert_eq!(workflow_ids.len(), 2_000, "concurrent add identity repeated");
}

#[tokio::test]
async fn go_concurrent_add_and_poll_activity_producer_half_persists_all_tasks() {
    concurrent_adds(1).await;
}

#[tokio::test]
async fn go_concurrent_add_and_poll_decision_producer_half_persists_all_tasks() {
    concurrent_adds(0).await;
}

/// Go's two-engine range-stealing scenario relies on an old owner's writes
/// being rejected after the replacement owner advances the durable range.
#[test]
fn go_two_engine_range_steal_fences_old_owner_and_replacement_keeps_working() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteTaskStore::create(dir.path().join("matching.db"), BlobEncoding::ThriftRW)
        .unwrap();
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
    store.create_tasks(&key, old_owner.range_id, &[old_task]).unwrap();

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
