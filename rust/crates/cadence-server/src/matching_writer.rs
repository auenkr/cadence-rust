//! Sequential Go-style Matching append actor. Caller cancellation does not
//! withdraw an enqueued request or block the writer's buffered response.
use cadence_persistence::{matching_queue::DurableTaskQueue, matching_sqlite::AllocatedTask};
use rand::SeedableRng;
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
#[derive(Clone, Debug)]
struct WriteError {
    status: Status,
    retryable: bool,
}
impl From<Status> for WriteError {
    fn from(status: Status) -> Self {
        // Queue saturation is Go ServiceBusyError. Shutdown and a plain
        // append context deadline are not persistence transient errors.
        let retryable = status.code() == tonic::Code::ResourceExhausted;
        Self { status, retryable }
    }
}
async fn retry_write<T, F: std::future::Future<Output = Result<T, WriteError>>>(
    mut operation: impl FnMut() -> F,
    mut next_delay: impl FnMut(usize, &WriteError) -> Option<Duration>,
) -> Result<T, WriteError> {
    let mut previous = None;
    let mut attempt = 0;
    loop {
        let error = match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !error.retryable {
            return Err(previous.unwrap_or(error));
        }
        let Some(delay) = next_delay(attempt, &error) else {
            return Err(previous.unwrap_or(error));
        };
        previous = Some(error);
        attempt += 1;
        tokio::time::sleep(delay).await;
    }
}
struct Request {
    task: WriteTask,
    response: oneshot::Sender<Result<AllocatedTask, WriteError>>,
}
struct Inner {
    sender: mpsc::Sender<Request>,
    stop: watch::Sender<bool>,
    queue: Arc<DurableTaskQueue>,
    config: WriterConfig,
    live_config: Option<Arc<dyn Fn() -> (i64, i64) + Send + Sync>>,
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
#[cfg(test)]
fn drain_batch<T>(mut batch: Vec<T>, receiver: &mut mpsc::Receiver<T>, maximum: i64) -> Vec<T> {
    drain_batch_with(&mut batch, receiver, || maximum)
}
fn drain_batch_with<T>(
    batch: &mut Vec<T>,
    receiver: &mut mpsc::Receiver<T>,
    mut maximum: impl FnMut() -> i64,
) -> Vec<T> {
    let mut additional = 0i64;
    loop {
        // Go calls MaxTaskBatchSize in the for-loop condition on every pass,
        // including the check that ends the drain.
        if additional >= maximum() {
            break;
        }
        match receiver.try_recv() {
            Ok(request) => {
                batch.push(request);
                additional += 1;
            }
            Err(_) => break,
        }
    }
    std::mem::take(batch)
}
impl TaskWriter {
    pub fn new(queue: Arc<DurableTaskQueue>, notify: Arc<Notify>, config: WriterConfig) -> Self {
        Self::new_with_live_config(queue, notify, config, None)
    }
    /// Install Go's per-task-list writer settings. Batch size is sampled at
    /// each getWriteBatch loop condition; append timeout is sampled per call.
    pub fn new_with_live_config(
        queue: Arc<DurableTaskQueue>, notify: Arc<Notify>, config: WriterConfig,
        live_config: Option<Arc<dyn Fn() -> (i64, i64) + Send + Sync>>,
    ) -> Self {
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
            live_config: live_config.clone(),
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
                // Sample outside any config lock held by this actor and before
                // probing the queue, matching Go getWriteBatch timing.
                let batch = drain_batch_with(&mut vec![first], &mut receiver, || {
                    live_config
                        .as_ref()
                        .map(|provider| provider().0)
                        .unwrap_or(config.max_batch_size)
                });
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
                        let retryable = matches!(&error, QueueError::Storage(StorageError::Sql(_)))
                            || matches!(
                                (&error, kind),
                                (QueueError::Storage(StorageError::ConditionFailed(_)), 1)
                            );
                        let status = match (&error, kind) {
                            (QueueError::Storage(StorageError::ConditionFailed(_)), 1) => {
                                Status::internal("StickyTaskConditionFailedError")
                            }
                            _ => Status::unavailable(error.to_string()),
                        };
                        Err(WriteError { status, retryable })
                    }
                    Err(error) => {
                        queue.stop();
                        Err(Status::internal(error.to_string()).into())
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
        self.append_inner(task).await.map_err(|error| error.status)
    }
    /// Only failed-delivery rewrites use this outer retry. Each individual
    /// CreateTasks transaction, including normal AddTask, stays single-attempt.
    pub async fn append_rewrite(&self, task: WriteTask) -> Result<AllocatedTask, Status> {
        use cadence_common::ExponentialRetryPolicy;
        let policy = ExponentialRetryPolicy::new(Duration::from_millis(50))
            .with_maximum_interval(Duration::from_secs(10))
            .with_expiration_interval(Duration::from_secs(30));
        let throttle = ExponentialRetryPolicy::new(Duration::from_secs(1))
            .with_maximum_interval(Duration::from_secs(10))
            .with_expiration_interval(Duration::ZERO);
        let started = std::time::Instant::now();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let mut throttles = 0;
        retry_write(
            || self.append_inner(task.clone()),
            |attempt, error| {
                let mut delay = policy.compute_next_delay(&mut rng, started.elapsed(), attempt)?;
                if error.status.code() == tonic::Code::ResourceExhausted {
                    if let Some(throttled) =
                        throttle.compute_next_delay(&mut rng, started.elapsed(), throttles)
                    {
                        delay = delay.max(throttled);
                    }
                    throttles += 1;
                }
                Some(delay)
            },
        )
        .await
        .map_err(|error| error.status)
    }
    async fn append_inner(&self, task: WriteTask) -> Result<AllocatedTask, WriteError> {
        if self.inner.queue.is_stopped() || *self.inner.stop.borrow() {
            return Err(stopped().into());
        }
        // Each append attempt (including rewrite retries) gets a fresh sample.
        let timeout_nanos = self.inner.live_config.as_ref().map(|provider| provider().1)
            .unwrap_or_else(|| self.inner.config.append_timeout.as_nanos().min(i64::MAX as u128) as i64);
        let now = tokio::time::Instant::now();
        let deadline = if timeout_nanos <= 0 { now } else { now + Duration::from_nanos(timeout_nanos as u64) };
        let (response, receiver) = oneshot::channel();
        match tokio::time::timeout_at(deadline, self.inner.sender.send(Request { task, response }))
            .await
        {
            Err(_) => return Err(full().into()),
            Ok(Err(_)) => return Err(stopped().into()),
            Ok(Ok(())) => (),
        }
        let mut stopped_rx = self.inner.stop.subscribe();
        if *stopped_rx.borrow() {
            return Err(stopped().into());
        }
        tokio::select! {
            result = receiver => result.unwrap_or_else(|_| Err(stopped().into())),
            _ = stopped_rx.changed() => Err(stopped().into()),
            _ = tokio::time::sleep_until(deadline) => Err(Status::deadline_exceeded("failed to receive a success response from the channel within a timeout threshold: context deadline exceeded").into()),
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

    #[tokio::test]
    async fn matching_writer_rewrite_retry_uses_original_go_outcomes() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-retry.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let errors = case["errors"].as_array().unwrap();
            let mut calls = 0;
            let mut pauses = 0;
            let result = retry_write(
                || {
                    let name = errors
                        .get(calls)
                        .map(|value| value.as_str().unwrap())
                        .unwrap_or("success");
                    calls += 1;
                    std::future::ready(if name == "success" {
                        Ok(())
                    } else {
                        Err(WriteError {
                            status: Status::unavailable(name),
                            retryable: !matches!(name, "condition" | "not-found"),
                        })
                    })
                },
                |attempt, _| {
                    if attempt < case["retries"].as_u64().unwrap() as usize {
                        pauses += 1;
                        Some(Duration::from_nanos(1))
                    } else {
                        None
                    }
                },
            )
            .await;
            assert_eq!(calls, case["calls"].as_u64().unwrap() as usize);
            assert_eq!(pauses, case["pauses"].as_u64().unwrap() as usize);
            assert_eq!(
                result
                    .map(|_| "success".to_owned())
                    .unwrap_or_else(|error| error.status.message().to_owned()),
                case["outcome"].as_str().unwrap()
            );
        }
        assert!(WriteError::from(full()).retryable);
        assert!(!WriteError::from(stopped()).retryable);
        assert!(
            !WriteError::from(Status::deadline_exceeded("context deadline exceeded")).retryable
        );
    }
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
                live_config: None,
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
    async fn matching_writer_resamples_live_append_timeout_for_saturated_enqueues() {
        use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let domain_id = Uuid::from_u128(1);
        let key = TaskListKey { domain_id, name: "timeouts".into(), task_type: 0 };
        let queue = Arc::new(DurableTaskQueue::open(store, key, 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let domains = cadence_persistence::InMemoryDomainStore::default();
        domains.create(cadence_api::types::DomainInfo {
            name: "domain".into(), uuid: domain_id.to_string(), ..Default::default()
        }, Default::default(), Default::default(), false).unwrap();
        let overlay = Arc::new(Mutex::new(crate::admin::DynamicConfigOverlay::default()));
        let timeout_value = |value: &str| DynamicConfigValue {
            value: Some(DataBlob::new(EncodingType::JSON, serde_json::to_vec(value).unwrap())),
            filters: vec![],
        };
        overlay.lock().unwrap().update(false, "matching.appendTaskTimeout", vec![timeout_value("20ms")]);
        let provider = crate::grpc_matching::task_writer_live_provider(domains, overlay.clone());
        let domain = domain_id.to_string();
        let live = Arc::new(move || provider(&domain, "timeouts", 0));
        // Leave the receiver parked to model a saturated queue and ensure the
        // timeout decision is made by append's real enqueue path.
        let (sender, receiver) = mpsc::channel(1);
        let (stop, _) = watch::channel(false);
        let writer = TaskWriter { inner: Arc::new(Inner {
            sender, stop, queue, config: WriterConfig::default(), live_config: Some(live),
            parked: Mutex::new(Some(receiver)),
        }) };
        let task = WriteTask { data: TaskData::default(), source_domain: domain_id, schedule_to_start_seconds: 0 };
        assert_eq!(writer.append(task.clone()).await.unwrap_err().code(), tonic::Code::DeadlineExceeded,
            "an accepted request keeps waiting for its response until its original deadline");
        overlay.lock().unwrap().update(false, "matching.appendTaskTimeout", vec![timeout_value("0s")]);
        assert_eq!(writer.append(task).await.unwrap_err().code(), tonic::Code::ResourceExhausted,
            "the next saturated enqueue uses the updated nonpositive timeout");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn matching_writer_batches_follow_live_dynamic_config_without_recreation() {
        use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let domain_id = Uuid::from_u128(1);
        let key = TaskListKey { domain_id, name: "batch-live".into(), task_type: 0 };
        let queue = Arc::new(DurableTaskQueue::open(store.clone(), key.clone(), 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let domains = cadence_persistence::InMemoryDomainStore::default();
        domains.create(cadence_api::types::DomainInfo {
            name: "domain".into(), uuid: domain_id.to_string(), ..Default::default()
        }, Default::default(), Default::default(), false).unwrap();
        let overlay = Arc::new(Mutex::new(crate::admin::DynamicConfigOverlay::default()));
        overlay.lock().unwrap().update(false, "matching.maxTaskBatchSize", vec![DynamicConfigValue {
            value: Some(DataBlob::new(EncodingType::JSON, b"0".to_vec())), filters: vec![],
        }]);
        let provider = crate::grpc_matching::task_writer_live_provider(domains, overlay.clone());
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));
        let live = {
            let entered = entered.clone(); let release = release.clone(); let calls = calls.clone();
            let domain = domain_id.to_string();
            Arc::new(move || {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 { entered.wait(); release.wait(); }
                provider(&domain, "batch-live", 0)
            }) as Arc<dyn Fn() -> (i64, i64) + Send + Sync>
        };
        let writer = TaskWriter::new_with_live_config(queue, Arc::new(Notify::new()), WriterConfig::default(), Some(live));
        let make_request = |n: usize| {
            let (response, receiver) = oneshot::channel();
            (Request { task: WriteTask { data: TaskData { workflow_id: format!("w{n}"), ..Default::default() }, source_domain: domain_id, schedule_to_start_seconds: 0 }, response }, receiver)
        };
        let (first, first_rx) = make_request(0);
        writer.inner.sender.send(first).await.unwrap();
        tokio::task::spawn_blocking({ let entered = entered.clone(); move || entered.wait() }).await.unwrap();
        let mut responses = vec![first_rx];
        for n in 1..5 {
            let (request, response) = make_request(n);
            writer.inner.sender.send(request).await.unwrap();
            responses.push(response);
        }
        // Change the config while this same actor is paused immediately before
        // Go-style getWriteBatch sampling.
        overlay.lock().unwrap().update(false, "matching.maxTaskBatchSize", vec![DynamicConfigValue {
            value: Some(DataBlob::new(EncodingType::JSON, b"1".to_vec())), filters: vec![],
        }]);
        tokio::task::spawn_blocking({ let release = release.clone(); move || release.wait() }).await.unwrap();
        for response in responses { response.await.unwrap().unwrap(); }
        assert_eq!(calls.load(Ordering::SeqCst), 5, "Go resamples once per batch-drain loop condition, including the final condition");
        assert_eq!(store.get_tasks(&key, 0, 10, 10).unwrap().len(), 5);
        writer.stop();
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
