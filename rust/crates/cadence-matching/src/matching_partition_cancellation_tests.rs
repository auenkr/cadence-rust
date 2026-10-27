//! Go throttleRetry observes ctx.Done during its retry wait, even without a deadline.
use super::*;
use cadence_persistence::{
    matching_blob::{BlobEncoding, TaskListData},
    matching_sqlite::SqliteTaskStore,
    matching_task_store::{
        BackendErrorKind, MatchingTaskStoreError, Result as StoreResult, TaskListRecord,
    },
    StoredPartitionConfig,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Condvar,
};
use std::time::Duration;

struct BlockedPartitionStore {
    inner: Arc<SqliteTaskStore>,
    entered: tokio::sync::Notify,
    released: (Mutex<bool>, Condvar),
    updates: AtomicUsize,
}
impl MatchingTaskStore for BlockedPartitionStore {
    fn get_task_list(&self, key: &TaskListKey) -> StoreResult<TaskListRecord> {
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
