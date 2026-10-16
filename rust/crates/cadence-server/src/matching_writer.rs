//! Sequential Go-style Matching append actor. Caller cancellation does not
//! withdraw an enqueued request or block the writer's buffered response.
use cadence_persistence::{matching_queue::DurableTaskQueue, matching_sqlite::AllocatedTask};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tonic::Status;
#[derive(Clone, Copy)]
pub struct WriterConfig {
    pub outstanding_appends: usize,
    /// Go drains this many *additional* requests after taking the first one.
    pub max_batch_size: i64,
    pub append_timeout: Duration,
}
impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            outstanding_appends: 250,
            max_batch_size: 100,
            append_timeout: Duration::from_secs(5),
        }
    }
}
pub use cadence_persistence::matching_queue::PendingTask as WriteTask;
struct Request {
    task: WriteTask,
    response: oneshot::Sender<Result<AllocatedTask, Status>>,
}
struct Inner {
    sender: mpsc::Sender<Request>,
    stop: watch::Sender<bool>,
    queue: Arc<DurableTaskQueue>,
    config: WriterConfig,
    // Go deliberately leaves appendCh open on stop. Keep the receiver alive
    // while handles exist so a caller already waiting to enqueue times out.
    parked: Mutex<Option<mpsc::Receiver<Request>>>,
}
#[derive(Clone)]
pub struct TaskWriter {
    inner: Arc<Inner>,
}
fn stopped() -> Status {
    Status::unavailable("task list shutting down")
}
fn full() -> Status {
    crate::grpc::detailed_status(
        tonic::Code::ResourceExhausted,
        "task queue is full and timed out waiting to enqueue",
        "ServiceBusyError",
        cadence_proto::api::ServiceBusyError {
            reason: String::new(),
        },
    )
}
fn drain_batch<T>(mut batch: Vec<T>, receiver: &mut mpsc::Receiver<T>, maximum: i64) -> Vec<T> {
    for _ in 0..maximum {
        match receiver.try_recv() {
            Ok(request) => batch.push(request),
            Err(_) => break,
        }
    }
    batch
}
impl TaskWriter {
    pub fn new(queue: Arc<DurableTaskQueue>, notify: Arc<Notify>, config: WriterConfig) -> Self {
        assert!(
            config.outstanding_appends > 0,
            "rendezvous append channels require separate support"
        );
        let (sender, receiver) = mpsc::channel(config.outstanding_appends);
        let (stop, mut stopped_rx) = watch::channel(false);
        let inner = Arc::new(Inner {
            sender,
            stop,
            queue: queue.clone(),
            config,
            parked: Mutex::new(None),
        });
        let stop_signal = inner.stop.clone();
        queue.set_stop_waker(Arc::new(move || {
            let _ = stop_signal.send(true);
        }));
        let weak = Arc::downgrade(&inner);
        let kind = queue.task_list_kind();
        tokio::spawn(async move {
            let mut receiver = receiver;
            loop {
                let first = tokio::select! {
                    _ = stopped_rx.changed() => break,
                    request = receiver.recv() => match request { Some(request) => request, None => break },
                };
                if *stopped_rx.borrow() || queue.is_stopped() {
                    break;
                }
                let batch = drain_batch(vec![first], &mut receiver, config.max_batch_size);
                let data = batch
                    .iter()
                    .map(|request| request.task.clone())
                    .collect::<Vec<_>>();
                let queue_copy = queue.clone();
                let result =
                    tokio::task::spawn_blocking(move || queue_copy.append_pending(&data)).await;
                let result = match result {
                    Ok(Ok(tasks)) => Ok(tasks),
                    Ok(Err(error)) => {
                        use cadence_persistence::{
                            matching_queue::QueueError, matching_sqlite::StorageError,
                        };
                        let status = match (&error, kind) {
                            (QueueError::Storage(StorageError::ConditionFailed(_)), 1) => {
                                Status::internal("StickyTaskConditionFailedError")
                            }
                            _ => Status::unavailable(error.to_string()),
                        };
                        Err(status)
                    }
                    Err(error) => {
                        queue.stop();
                        Err(Status::internal(error.to_string()))
                    }
                };
                if result.is_ok() {
                    notify.notify_waiters();
                }
                match result {
                    Ok(tasks) => {
                        for (request, task) in batch.into_iter().zip(tasks) {
                            let _ = request.response.send(Ok(task));
                        }
                    }
                    Err(error) => {
                        for request in batch {
                            let _ = request.response.send(Err(error.clone()));
                        }
                    }
                }
            }
            if let Some(inner) = weak.upgrade() {
                *inner.parked.lock().expect("Matching writer receiver lock") = Some(receiver);
            }
        });
        Self { inner }
    }
    pub fn stop_signal(&self) -> watch::Sender<bool> {
        self.inner.stop.clone()
    }
    pub fn stop(&self) {
        let _ = self.inner.stop.send(true);
    }
    pub async fn append(&self, task: WriteTask) -> Result<AllocatedTask, Status> {
        if self.inner.queue.is_stopped() || *self.inner.stop.borrow() {
            return Err(stopped());
        }
        let deadline = tokio::time::Instant::now() + self.inner.config.append_timeout;
        let (response, receiver) = oneshot::channel();
        match tokio::time::timeout_at(deadline, self.inner.sender.send(Request { task, response }))
            .await
        {
            Err(_) => return Err(full()),
            Ok(Err(_)) => return Err(stopped()),
            Ok(Ok(())) => (),
        }
        let mut stopped_rx = self.inner.stop.subscribe();
        if *stopped_rx.borrow() {
            return Err(stopped());
        }
        tokio::select! {
            result = receiver => result.unwrap_or_else(|_| Err(stopped())),
            _ = stopped_rx.changed() => Err(stopped()),
            _ = tokio::time::sleep_until(deadline) => Err(Status::deadline_exceeded("failed to receive a success response from the channel within a timeout threshold: context deadline exceeded")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskData},
        matching_sqlite::{SqliteTaskStore, TaskListKey},
    };
    use std::num::NonZeroU32;
    use uuid::Uuid;

    #[test]
    fn matching_writer_original_go_batches() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-writer.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let queued = case["queued"].as_u64().unwrap() as usize;
            let initial = case["initial"].as_u64().unwrap() as usize;
            let (sender, mut receiver) = mpsc::channel(queued.max(1));
            for value in initial..initial + queued {
                sender.try_send(value).unwrap();
            }
            let batch = drain_batch(
                (0..initial).collect(),
                &mut receiver,
                case["maximum"].as_i64().unwrap(),
            );
            assert_eq!(serde_json::to_value(batch).unwrap(), case["batch"]);
            assert_eq!(receiver.len(), case["remaining"].as_u64().unwrap() as usize);
        }
    }

    #[tokio::test]
    async fn matching_writer_enqueue_and_response_timeouts_are_distinct() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let domain = Uuid::from_u128(1);
        let key = TaskListKey {
            domain_id: domain,
            name: "timeouts".into(),
            task_type: 0,
        };
        let queue = Arc::new(
            DurableTaskQueue::open(store, key, 0, NonZeroU32::new(100).unwrap(), 1).unwrap(),
        );
        // Park the receiver to model a writer stalled in persistence, without
        // making timing assumptions about SQLite or the background scheduler.
        let (sender, receiver) = mpsc::channel(1);
        let (stop, _stopped) = watch::channel(false);
        let writer = TaskWriter {
            inner: Arc::new(Inner {
                sender,
                stop,
                queue,
                config: WriterConfig {
                    append_timeout: Duration::from_millis(10),
                    ..WriterConfig::default()
                },
                parked: Mutex::new(Some(receiver)),
            }),
        };
        let task = WriteTask {
            data: TaskData::default(),
            source_domain: domain,
            schedule_to_start_seconds: 0,
        };
        let error = writer.append(task.clone()).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert!(error.message().contains("failed to receive"));
        let error = writer.append(task).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("waiting to enqueue"));
    }

    #[tokio::test]
    async fn matching_writer_cancelled_caller_does_not_withdraw_durable_append() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "writer".into(),
            task_type: 0,
        };
        let queue = Arc::new(
            DurableTaskQueue::open(
                store.clone(),
                key.clone(),
                0,
                NonZeroU32::new(2).unwrap(),
                1,
            )
            .unwrap(),
        );
        let writer = TaskWriter::new(
            queue.clone(),
            Arc::new(Notify::new()),
            WriterConfig::default(),
        );
        let (response, receiver) = oneshot::channel();
        writer
            .inner
            .sender
            .send(Request {
                task: WriteTask {
                    data: TaskData {
                        workflow_id: "cancelled".into(),
                        ..Default::default()
                    },
                    source_domain: key.domain_id,
                    schedule_to_start_seconds: 0,
                },
                response,
            })
            .await
            .unwrap();
        drop(receiver);
        let result = writer
            .append(WriteTask {
                data: TaskData {
                    workflow_id: "next".into(),
                    ..Default::default()
                },
                source_domain: key.domain_id,
                schedule_to_start_seconds: 10,
            })
            .await
            .unwrap();
        assert_eq!(result.task_id, 2);
        let rows = store.get_tasks(&key, 0, 10, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].data.workflow_id, "cancelled");
        assert!(rows[1].data.expiry_nanos > rows[1].data.created_nanos);
        writer.stop();
        assert_eq!(
            writer
                .append(WriteTask {
                    data: TaskData::default(),
                    source_domain: key.domain_id,
                    schedule_to_start_seconds: 0
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
    }
}
