//! Go throttleRetry observes ctx.Done during its retry wait, even without a deadline.
use super::*;
use crate::grpc_matching::Matching;
use cadence_persistence::{
    matching_blob::{BlobEncoding, TaskListData},
    matching_sqlite::SqliteTaskStore,
    matching_task_store::{
        BackendErrorKind, MatchingTaskStoreError, Result as StoreResult, TaskListRecord,
    },
    StoredPartitionConfig,
};
use cadence_proto::api as p;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Condvar,
};
use std::time::Duration;
use tonic::Request;

struct BlockedPartitionStore {
    inner: Arc<SqliteTaskStore>,
    entered: tokio::sync::Notify,
    released: (Mutex<bool>, Condvar),
    updates: AtomicUsize,
    read_failure: Mutex<Option<BackendErrorKind>>,
    reads: AtomicUsize,
}
impl MatchingTaskStore for BlockedPartitionStore {
    fn get_task_list(&self, key: &TaskListKey) -> StoreResult<TaskListRecord> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(kind) = *self.read_failure.lock().unwrap() {
            return Err(MatchingTaskStoreError::Backend {
                kind,
                message: "partition refresh failed".into(),
            });
        }
        self.inner.get_task_list(key)
    }
    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now: i64,
    ) -> StoreResult<TaskListRecord> {
        self.inner.lease(key, kind, previous_range, now)
    }
    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        data: TaskListData,
        now: i64,
    ) -> StoreResult<()> {
        if data.adaptive_partition_config.is_some() {
            self.updates.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let (released, wake) = &self.released;
            let mut done = released.lock().unwrap();
            while !*done {
                done = wake.wait(done).unwrap();
            }
            return Err(MatchingTaskStoreError::Backend {
                kind: BackendErrorKind::Timeout,
                message: "partition update timed out".into(),
            });
        }
        self.inner.update_task_list(key, range, data, now)
    }
    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> StoreResult<()> {
        MatchingTaskStore::create_tasks_with_domains(self.inner.as_ref(), key, range, now, tasks)
    }
    fn get_tasks(
        &self,
        key: &TaskListKey,
        read: i64,
        max: i64,
        batch: i64,
    ) -> StoreResult<Vec<AllocatedTask>> {
        self.inner.get_tasks(key, read, max, batch)
    }
    fn task_list_size(&self, key: &TaskListKey, ack: i64) -> StoreResult<i64> {
        self.inner.task_list_size(key, ack)
    }
    fn complete_task(&self, key: &TaskListKey, id: i64) -> StoreResult<()> {
        self.inner.complete_task(key, id)
    }
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        id: i64,
        limit: i64,
    ) -> StoreResult<usize> {
        self.inner.complete_tasks_less_than(key, id, limit)
    }
    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> StoreResult<()> {
        self.inner.delete_task_list(key, range)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_partition_update_without_deadline_cancels_retry_and_counts_failure_once() {
    use crate::matching_metrics::{MatchingMetrics, RecordingMetricSink, PARTITION_UPDATE_FAILED};
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(BlockedPartitionStore {
        inner: Arc::new(
            SqliteTaskStore::create(directory.path().join("cancel.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        ),
        entered: tokio::sync::Notify::new(),
        released: (Mutex::new(false), Condvar::new()),
        updates: AtomicUsize::new(0),
        read_failure: Mutex::new(None),
        reads: AtomicUsize::new(0),
    });
    let sink = Arc::new(RecordingMetricSink::default());
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        1024,
    );
    let registry = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap())
        .with_metrics(MatchingMetrics::new(sink.clone()));
    let key = TaskListKey {
        domain_id: Uuid::from_u128(1),
        name: "cancel-update".into(),
        task_type: 0,
    };
    let handle = registry.get(key.clone(), 0, 1).await.unwrap();
    let queue = handle.queue.clone();
    let caller_registry = registry.clone();
    let request = tokio::spawn(async move {
        caller_registry
            .update_partition_config_with_deadline(
                key,
                handle,
                StoredPartitionConfig {
                    version: 0,
                    read_partitions: HashMap::from([(0, vec![]), (1, vec![])]),
                    write_partitions: HashMap::from([(0, vec![])]),
                },
                2,
                None,
            )
            .await
    });
    // Hold the database attempt until the caller is certainly gone. This proves
    // cancellation affects the detached retry, not just the awaiting RPC future.
    let entered = tokio::time::timeout(Duration::from_secs(5), store.entered.notified()).await;
    request.abort();
    let _ = request.await;
    *store.released.0.lock().unwrap() = true;
    store.released.1.notify_all();
    entered.expect("partition persistence attempt was never reached");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !queue.is_stopped() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("caller cancellation did not stop the persistence retry");
    assert_eq!(
        store.updates.load(Ordering::SeqCst),
        1,
        "canceled context must not start another DB attempt"
    );
    assert_eq!(
        sink.events()
            .iter()
            .filter(|event| event.name == PARTITION_UPDATE_FAILED)
            .count(),
        1
    );
    registry.shutdown(3).await.unwrap();
}

/// Go RefreshTaskListPartitionConfig returns the last persistence error through
/// handleErr; cancellation stops retry waiting without stopping the manager.
#[tokio::test]
async fn refresh_rpc_persistence_failure_matches_go_and_observes_expired_caller() {
    use cadence_api::types::DomainInfo;
    use cadence_proto::matching::matching_api_server::MatchingApi;
    for kind in [
        BackendErrorKind::Permanent,
        BackendErrorKind::Timeout,
        BackendErrorKind::Throttled,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(BlockedPartitionStore {
            inner: Arc::new(
                SqliteTaskStore::create(
                    directory.path().join("refresh.db"),
                    BlobEncoding::ThriftRW,
                )
                .unwrap(),
            ),
            entered: tokio::sync::Notify::new(),
            released: (Mutex::new(true), Condvar::new()),
            updates: AtomicUsize::new(1),
            read_failure: Mutex::new(None),
            reads: AtomicUsize::new(0),
        });
        let history = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            1024,
        );
        let registry = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap());
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "refresh-failure".into(),
            task_type: 0,
        };
        let state = crate::handlers::AppState::default();
        state
            .domains
            .create(
                DomainInfo {
                    name: "refresh-domain".into(),
                    uuid: key.domain_id.to_string(),
                    ..Default::default()
                },
                Default::default(),
                Default::default(),
                false,
            )
            .unwrap();
        let matching =
            Matching::with_durable_queues(state, registry.clone(), Duration::from_secs(5));
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        let cached = StoredPartitionConfig {
            version: 7,
            read_partitions: HashMap::from([(0, vec![])]),
            write_partitions: HashMap::from([(0, vec![])]),
        };
        handle.partitions.push(&cached);
        store.reads.store(0, Ordering::SeqCst);
        *store.read_failure.lock().unwrap() = Some(kind);
        let mut request = Request::new(m::RefreshTaskListPartitionConfigRequest {
            domain_id: key.domain_id.to_string(),
            task_list: Some(p::TaskList {
                name: key.name.clone(),
                kind: 1,
                ..Default::default()
            }),
            task_list_type: 1,
            ..Default::default()
        });
        request.set_timeout(Duration::ZERO);
        let status = matching
            .refresh_task_list_partition_config(request)
            .await
            .unwrap_err();
        if kind == BackendErrorKind::Throttled {
            assert_eq!(status.code(), tonic::Code::ResourceExhausted);
            assert!(!status.details().is_empty());
        } else {
            assert_eq!(status.code(), tonic::Code::Internal);
            assert!(status.details().is_empty());
        }
        assert!(status.message().contains("partition refresh failed"));
        assert_eq!(
            store.reads.load(Ordering::SeqCst),
            1,
            "expired caller still runs first read, then cancels retry waiting"
        );
        let after = handle.partitions.get().unwrap();
        assert_eq!(after.version, cached.version);
        assert_eq!(after.read_partitions, cached.read_partitions);
        assert_eq!(after.write_partitions, cached.write_partitions);
        assert!(
            !handle.queue.is_stopped(),
            "refresh failure does not stop the Go manager"
        );
        *store.read_failure.lock().unwrap() = None;
        registry.shutdown(3).await.unwrap();
    }
}

/// The Go engine test injects a manager error and checks that the public RPC
/// returns it. Exercise the same observable propagation through the existing
/// test-only engine fault hook, then clear the fault and verify the same RPC
/// can refresh the durable queue successfully.
#[tokio::test]
async fn refresh_rpc_propagates_engine_error_and_recovers_after_fault_clears() {
    use cadence_api::types::DomainInfo;
    use cadence_proto::{api as p, matching as m};
    use cadence_proto::matching::matching_api_server::MatchingApi;

    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(
            directory.path().join("refresh-engine-fault.db"),
            BlobEncoding::ThriftRW,
        )
        .unwrap(),
    );
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        1024,
    );
    let registry = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap());
    let domain_id = Uuid::from_u128(1);
    let state = crate::handlers::AppState::default();
    state
        .domains
        .create(
            DomainInfo {
                name: "refresh-engine-fault-domain".into(),
                uuid: domain_id.to_string(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    let matching = Matching::with_durable_queues(state, registry.clone(), Duration::from_secs(5));
    *matching.engine_fault.lock().unwrap() = Some((
        "RefreshTaskListPartitionConfig",
        "injected refresh failure".into(),
    ));

    let request = || {
        Request::new(m::RefreshTaskListPartitionConfigRequest {
            domain_id: domain_id.to_string(),
            task_list: Some(p::TaskList {
                name: "/__cadence_sys/refresh-engine-fault/1".into(),
                kind: 0,
                ..Default::default()
            }),
            task_list_type: 1,
            partition_config: Some(p::TaskListPartitionConfig {
                version: 1,
                read_partitions: HashMap::from([(0, p::TaskListPartition::default())]),
                write_partitions: HashMap::from([(0, p::TaskListPartition::default())]),
                ..Default::default()
            }),
        })
    };

    let status = matching
        .refresh_task_list_partition_config(request())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "injected refresh failure");
    let key = TaskListKey {
        domain_id,
        name: "/__cadence_sys/refresh-engine-fault/1".into(),
        task_type: 0,
    };
    assert!(
        matches!(
            store.get_task_list(&key),
            Err(cadence_persistence::matching_task_store::MatchingTaskStoreError::NotFound)
        ),
        "failed refresh did not create a durable task list or leave a partial refresh behind"
    );

    *matching.engine_fault.lock().unwrap() = None;
    matching
        .refresh_task_list_partition_config(request())
        .await
        .unwrap();
    registry.shutdown(3).await.unwrap();
}
