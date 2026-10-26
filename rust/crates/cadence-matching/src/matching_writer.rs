//! Sequential Go-style Matching append actor. Caller cancellation does not
//! withdraw an enqueued request or block the writer's buffered response.
use cadence_persistence::{matching_queue::DurableTaskQueue, matching_sqlite::AllocatedTask};
use rand::SeedableRng;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tonic::Status;
#[derive(Clone, Copy)]
pub struct WriterConfig {
    pub outstanding_appends: i64,
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
        let error = match crate::matching_persistence_limiter::with_persistence_retry_count(attempt, operation()).await {
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
    accepted: Option<oneshot::Sender<()>>,
}
struct Rendezvous {
    waiters: Mutex<VecDeque<(u64, Request)>>,
    notify: Notify,
    next_id: AtomicU64,
}
struct AdmissionGuard {
    rendezvous: Arc<Rendezvous>,
    id: u64,
    armed: bool,
}
impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if self.armed {
            let mut waiters = self.rendezvous.waiters.lock().expect("Matching rendezvous queue lock");
            if let Some(index) = waiters.iter().position(|(id, _)| *id == self.id) { waiters.remove(index); }
        }
    }
}
async fn receive_rendezvous(
    rendezvous: &Rendezvous,
    stopped: &mut watch::Receiver<bool>,
) -> Option<Request> {
    loop {
        let notified = rendezvous.notify.notified();
        if let Some((_, mut request)) = rendezvous.waiters.lock().expect("Matching rendezvous queue lock").pop_front() {
            if let Some(accepted) = request.accepted.take() { let _ = accepted.send(()); }
            return Some(request);
        }
        tokio::select! {
            _ = stopped.changed() => return None,
            _ = notified => (),
        }
    }
}
fn try_receive_rendezvous(rendezvous: &Rendezvous) -> Option<Request> {
    let (_, mut request) = rendezvous.waiters.lock().expect("Matching rendezvous queue lock").pop_front()?;
    if let Some(accepted) = request.accepted.take() { let _ = accepted.send(()); }
    Some(request)
}
struct Inner {
    sender: mpsc::Sender<Request>,
    rendezvous: Option<Arc<Rendezvous>>,
    stop: watch::Sender<bool>,
    queue: Arc<DurableTaskQueue>,
    config: WriterConfig,
    live_config: Option<crate::matching_registry::WriterLiveConfig>,
    // Go deliberately leaves appendCh open on stop. Keep the receiver alive
    // while handles exist so a caller already waiting to enqueue times out.
    parked: Mutex<Option<mpsc::Receiver<Request>>>,
}

/// Tracks writer actors that can still publish a fatal persistence result.
/// Registry shutdown first stops their queues, then waits for this count to
/// reach zero before the launcher makes its final fatal-channel decision.
#[derive(Default)]
pub(crate) struct WriterTracker {
    active: std::sync::atomic::AtomicUsize,
    idle: Notify,
}

pub(crate) struct WriterTaskGuard(Arc<WriterTracker>);

impl WriterTracker {
    pub(crate) fn start(self: &Arc<Self>) -> WriterTaskGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        WriterTaskGuard(self.clone())
    }

    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for WriterTaskGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}
#[derive(Clone)]
pub struct TaskWriter {
    inner: Arc<Inner>,
}
fn stopped() -> Status {
    // Go errShutdown reaches handleErr's default branch: InternalServiceError.
    Status::internal("task list shutting down")
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
        live_config: Option<crate::matching_registry::WriterLiveConfig>,
    ) -> Self {
        Self::new_with_fatal_channel(queue, notify, config, live_config, None, None)
    }
    pub(crate) fn new_with_fatal_channel(
        queue: Arc<DurableTaskQueue>, notify: Arc<Notify>, config: WriterConfig,
        live_config: Option<crate::matching_registry::WriterLiveConfig>,
    fatal: Option<tokio::sync::mpsc::UnboundedSender<cadence_persistence::matching_blob::SqlRunIdError>>,
    tracker: Option<Arc<WriterTracker>>,
    ) -> Self {
        let capacity = live_config.as_ref().map(|provider| provider().2)
            .unwrap_or(config.outstanding_appends);
        let capacity = usize::try_from(capacity)
            .expect("negative matching outstanding task append threshold");
        let rendezvous = (capacity == 0).then(|| Arc::new(Rendezvous {
            waiters: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            next_id: AtomicU64::new(1),
        }));
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (stop, mut stopped_rx) = watch::channel(false);
        let inner = Arc::new(Inner {
            sender,
            rendezvous: rendezvous.clone(),
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
        let task_guard = tracker.map(|tracker| tracker.start());
        tokio::spawn(async move {
            let _task_guard = task_guard;
            let mut receiver = receiver;
            loop {
                let first = if let Some(rendezvous) = &rendezvous {
                    match receive_rendezvous(rendezvous, &mut stopped_rx).await {
                        Some(request) => request,
                        None => break,
                    }
                } else {
                    tokio::select! {
                        _ = stopped_rx.changed() => break,
                        request = receiver.recv() => match request { Some(request) => request, None => break },
                    }
                };
                if *stopped_rx.borrow() || queue.is_stopped() {
                    break;
                }
                // Sample outside any config lock held by this actor and before
                // probing the queue, matching Go getWriteBatch timing.
                let batch = if let Some(rendezvous) = &rendezvous {
                    let mut batch = vec![first];
                    let mut additional = 0i64;
                    loop {
                        let maximum = live_config.as_ref().map(|provider| provider().0).unwrap_or(config.max_batch_size);
                        if additional >= maximum { break; }
                        match try_receive_rendezvous(rendezvous) {
                            Some(request) => { batch.push(request); additional += 1; }
                            None => break,
                        }
                    }
                    batch
                } else { drain_batch_with(&mut vec![first], &mut receiver, || {
                    live_config
                        .as_ref()
                        .map(|provider| provider().0)
                        .unwrap_or(config.max_batch_size)
                }) };
                let data = batch
                    .iter()
                    .map(|request| request.task.clone())
                    .collect::<Vec<_>>();
                let queue_copy = queue.clone();
                let result =
                    crate::matching_blocking::run(move || queue_copy.append_pending(&data, crate::matching_dispatch::now())).await;
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
                        if let QueueError::Storage(StorageError::MalformedSqlMatchingRunId(cause)) = &error {
                            if let Some(fatal) = &fatal { let _ = fatal.send(cause.clone()); }
                        }
                        let status = match (&error, kind) {
                            (QueueError::Storage(StorageError::ConditionFailed(_)), 1) => {
                                Status::internal("StickyTaskConditionFailedError")
                            }
                            // Go returns the raw persistence error, which handleErr maps to Internal.
                            _ => Status::internal(error.to_string()),
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
        let request = Request { task, response, accepted: None };
        if let Some(rendezvous) = &self.inner.rendezvous {
            let (accepted, accepted_rx) = oneshot::channel();
            let mut request = request;
            request.accepted = Some(accepted);
            let id = rendezvous.next_id.fetch_add(1, Ordering::Relaxed);
            rendezvous.waiters.lock().expect("Matching rendezvous queue lock").push_back((id, request));
            let mut admission = AdmissionGuard { rendezvous: rendezvous.clone(), id, armed: true };
            rendezvous.notify.notify_one();
            match tokio::time::timeout_at(deadline, accepted_rx).await {
                Ok(Ok(())) => admission.armed = false,
                _ => return Err(full().into()),
            }
            drop(admission);
        } else {
            let mut request = request;
            request.accepted = None;
            match tokio::time::timeout_at(deadline, self.inner.sender.send(request)).await {
                Err(_) => return Err(full().into()),
                Ok(Err(_)) => return Err(stopped().into()),
                Ok(Ok(())) => (),
            }
        }
        let mut stopped_rx = self.inner.stop.subscribe();
        if *stopped_rx.borrow() {
            return Err(stopped().into());
        }
        tokio::select! {
            result = receiver => result.unwrap_or_else(|_| Err(stopped().into())),
            _ = stopped_rx.changed() => Err(stopped().into()),
            // Go wraps the ctx error (task_writer.go), which handleErr maps to Internal.
            _ = tokio::time::sleep_until(deadline) => Err(Status::internal("failed to receive a success response from the channel within a timeout threshold: context deadline exceeded").into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_waits_for_inflight_writer_fatal_notification() {
        // This test deliberately exercises registration made by TaskWriter::new_with_fatal_channel,
        // rather than manually incrementing the tracker. The standalone append test below covers
        // the typed SQL error path; this verifies shutdown's join observes the actor's send.
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("join.db"), BlobEncoding::ThriftRW).unwrap());
        let domain = Uuid::from_u128(9);
        let queue = Arc::new(DurableTaskQueue::open(store, TaskListKey { domain_id: domain, name: "joined-fatal".into(), task_type: 0 }, 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let tracker = Arc::new(WriterTracker::default());
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
        let writer = TaskWriter::new_with_fatal_channel(queue, Arc::new(Notify::new()), WriterConfig::default(), None, Some(fatal_tx), Some(tracker.clone()));
        let expected = writer.append(WriteTask {
            data: TaskData::default().with_request_run_id("delayed-malformed-run-id"),
            source_domain: domain,
            schedule_to_start_seconds: 0,
        }).await.unwrap_err();
        assert_eq!(expected.code(), tonic::Code::Internal);

        // This is the same join QueueRegistry/MatchingRuntime use before the launcher
        // samples its fatal receiver. The actor's failure send precedes releasing its guard.
        tracker.wait_idle().await;
        assert!(matches!(fatal_rx.try_recv().unwrap(), cadence_persistence::matching_blob::SqlRunIdError::Malformed { raw, .. } if raw == "delayed-malformed-run-id"));
        writer.stop();
    }
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskData},
        matching_sqlite::{SqliteTaskStore, TaskListKey},
    };
    use std::num::NonZeroU32;
    use uuid::Uuid;

    #[tokio::test]
    async fn standalone_fatal_channel_receives_typed_sql_run_id_failure_before_append_error() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("fatal.db"), BlobEncoding::ThriftRW).unwrap());
        let domain = Uuid::from_u128(9);
        let queue = Arc::new(DurableTaskQueue::open(store, TaskListKey { domain_id: domain, name: "fatal".into(), task_type: 0 }, 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
        let writer = TaskWriter::new_with_fatal_channel(queue, Arc::new(Notify::new()), WriterConfig::default(), None, Some(fatal_tx), None);
        let error = writer.append(WriteTask {
            data: TaskData::default().with_request_run_id("malformed-run-id"),
            source_domain: domain,
            schedule_to_start_seconds: 0,
        }).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        let fatal = tokio::time::timeout(Duration::from_secs(1), fatal_rx.recv()).await.unwrap().unwrap();
        assert!(matches!(fatal, cadence_persistence::matching_blob::SqlRunIdError::Malformed { raw, .. } if raw == "malformed-run-id"));
        writer.stop();
    }

    #[tokio::test]
    async fn zero_capacity_handoffs_are_fifo_and_never_overwrite_waiters() {
        for pass in 0..3 {
            let rendezvous = Arc::new(Rendezvous {
                waiters: Mutex::new(VecDeque::new()),
                notify: Notify::new(),
                next_id: AtomicU64::new(1),
            });
            let mut accepted = Vec::new();
            for contender in 0..24 {
                let rendezvous = rendezvous.clone();
                accepted.push(tokio::spawn(async move {
                    let (response, _response_rx) = oneshot::channel();
                    let (accept_tx, accept_rx) = oneshot::channel();
                    let id = (pass * 100 + contender) as u64;
                    let request = Request {
                        task: WriteTask { data: TaskData { workflow_id: id.to_string(), ..Default::default() }, source_domain: Uuid::from_u128(1), schedule_to_start_seconds: 0 },
                        response,
                        accepted: Some(accept_tx),
                    };
                    let ticket = rendezvous.next_id.fetch_add(1, Ordering::Relaxed);
                    rendezvous.waiters.lock().unwrap().push_back((ticket, request));
                    let _guard = AdmissionGuard { rendezvous: rendezvous.clone(), id: ticket, armed: true };
                    rendezvous.notify.notify_one();
                    accept_rx.await.unwrap();
                    id
                }));
            }
            tokio::task::yield_now().await;
            while rendezvous.waiters.lock().unwrap().len() != 24 { tokio::task::yield_now().await; }
            let mut stopped = watch::channel(false).1;
            let first = receive_rendezvous(&rendezvous, &mut stopped).await.unwrap();
            let mut ids = vec![first.task.data.workflow_id.parse::<u64>().unwrap()];
            while let Some(request) = try_receive_rendezvous(&rendezvous) {
                ids.push(request.task.data.workflow_id.parse::<u64>().unwrap());
            }
            assert_eq!(ids.len(), 24);
            assert_eq!(rendezvous.waiters.lock().unwrap().len(), 0);
            let mut completed = Vec::new();
            for waiter in accepted { completed.push(waiter.await.unwrap()); }
            ids.sort_unstable(); completed.sort_unstable();
            assert_eq!(ids, completed);
        }
    }

    #[tokio::test]
    async fn zero_capacity_unaccepted_cancellation_removes_published_offer() {
        let rendezvous = Arc::new(Rendezvous { waiters: Mutex::new(VecDeque::new()), notify: Notify::new(), next_id: AtomicU64::new(1) });
        let (response, _response_rx) = oneshot::channel();
        let (accepted, _accepted_rx) = oneshot::channel();
        let request = Request { task: WriteTask { data: TaskData::default(), source_domain: Uuid::from_u128(1), schedule_to_start_seconds: 0 }, response, accepted: Some(accepted) };
        let id = rendezvous.next_id.fetch_add(1, Ordering::Relaxed);
        rendezvous.waiters.lock().unwrap().push_back((id, request));
        {
            let _cancelled = AdmissionGuard { rendezvous: rendezvous.clone(), id, armed: true };
            assert_eq!(rendezvous.waiters.lock().unwrap().len(), 1);
        }
        assert!(rendezvous.waiters.lock().unwrap().is_empty());
        assert!(try_receive_rendezvous(&rendezvous).is_none());
    }

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
    async fn matching_writer_samples_signed_capacity_once_and_preserves_zero() {
        use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let domain = Uuid::from_u128(1);
        let key = TaskListKey { domain_id: domain, name: "capacity".into(), task_type: 0 };
        let queue = Arc::new(DurableTaskQueue::open(store, key, 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let capacity = Arc::new(AtomicI64::new(0));
        let samples = Arc::new(AtomicUsize::new(0));
        let live = {
            let capacity = capacity.clone();
            let samples = samples.clone();
            Arc::new(move || {
                samples.fetch_add(1, Ordering::SeqCst);
                (100i64, 5_000_000_000i64, capacity.load(Ordering::SeqCst))
            }) as Arc<dyn Fn() -> (i64, i64, i64) + Send + Sync>
        };
        let writer = TaskWriter::new_with_live_config(queue, Arc::new(Notify::new()), WriterConfig::default(), Some(live));
        assert!(writer.inner.rendezvous.is_some(), "zero means a rendezvous, not a one-slot queue");
        assert_eq!(samples.load(Ordering::SeqCst), 1, "capacity is sampled at owner construction");
        capacity.store(4, Ordering::SeqCst);
        assert!(writer.inner.rendezvous.is_some(), "live updates do not resize an existing owner");
        writer.stop();

        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let queue = Arc::new(DurableTaskQueue::open(store, TaskListKey { domain_id: domain, name: "negative-capacity".into(), task_type: 0 }, 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let negative = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            TaskWriter::new(queue, Arc::new(Notify::new()), WriterConfig { outstanding_appends: -1, ..WriterConfig::default() })
        }));
        assert!(negative.is_err(), "negative capacity must fail construction like Go make(chan, negative)");

        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let queue = Arc::new(DurableTaskQueue::open(store, TaskListKey { domain_id: domain, name: "positive-capacity".into(), task_type: 0 }, 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let writer = TaskWriter::new(queue, Arc::new(Notify::new()), WriterConfig { outstanding_appends: 2, ..WriterConfig::default() });
        assert_eq!(writer.inner.sender.capacity(), 2, "positive capacity retains exactly its configured buffer");
        writer.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn matching_writer_zero_capacity_times_out_unaccepted_and_keeps_accepted_write_on_stop() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let domain = Uuid::from_u128(1);
        let key = TaskListKey { domain_id: domain, name: "rendezvous-gates".into(), task_type: 0 };
        let queue = Arc::new(DurableTaskQueue::open(store.clone(), key.clone(), 0, NonZeroU32::new(100).unwrap(), 1).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let live = {
            let calls = calls.clone();
            let entered = entered.clone();
            let release = release.clone();
            Arc::new(move || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                // construction=0, first append timeout=1, writer batch sample=2
                if call == 2 { entered.wait(); release.wait(); }
                let timeout = if call == 3 { 80_000_000i64 } else { 5_000_000_000i64 };
                (0i64, timeout, 0i64)
            }) as Arc<dyn Fn() -> (i64, i64, i64) + Send + Sync>
        };
        let writer = TaskWriter::new_with_live_config(queue, Arc::new(Notify::new()), WriterConfig::default(), Some(live));
        let first_writer = writer.clone();
        let first = tokio::spawn(async move { first_writer.append(WriteTask {
            data: TaskData { workflow_id: "accepted".into(), ..Default::default() },
            source_domain: domain,
            schedule_to_start_seconds: 0,
        }).await });
        crate::matching_blocking::run({ let entered = entered.clone(); move || entered.wait() }).await.unwrap();

        let busy = writer.append(WriteTask { data: TaskData::default(), source_domain: domain, schedule_to_start_seconds: 0 }).await.unwrap_err();
        assert_eq!(busy.code(), tonic::Code::ResourceExhausted, "no second append is accepted while persistence admission is busy");

        let cancelled_writer = writer.clone();
        let cancelled = tokio::spawn(async move { cancelled_writer.append(WriteTask {
            data: TaskData { workflow_id: "cancelled-before-acceptance".into(), ..Default::default() },
            source_domain: domain,
            schedule_to_start_seconds: 0,
        }).await });
        tokio::task::yield_now().await;
        cancelled.abort();

        writer.stop();
        crate::matching_blocking::run({ let release = release.clone(); move || release.wait() }).await.unwrap();
        let first_result = tokio::time::timeout(Duration::from_secs(1), first).await.unwrap().unwrap();
        assert!(first_result.is_ok() || first_result.as_ref().unwrap_err().code() == tonic::Code::Internal,
            "the accepted write may race its successful response with Stop");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let tasks = store.get_tasks(&key, 0, 10, 10).unwrap();
                if !tasks.is_empty() {
                    assert_eq!(tasks.len(), 1, "the accepted write remains durable and cancelled admission leaves no hidden request");
                    assert_eq!(tasks[0].data.workflow_id, "accepted");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
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
                rendezvous: None,
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
        // Go wraps the ctx error, which handleErr maps to InternalServiceError.
        assert_eq!(error.code(), tonic::Code::Internal);
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
            sender, rendezvous: None, stop, queue, config: WriterConfig::default(), live_config: Some(live),
            parked: Mutex::new(Some(receiver)),
        }) };
        let task = WriteTask { data: TaskData::default(), source_domain: domain_id, schedule_to_start_seconds: 0 };
        assert_eq!(writer.append(task.clone()).await.unwrap_err().code(), tonic::Code::Internal,
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
                if calls.fetch_add(1, Ordering::SeqCst) == 1 { entered.wait(); release.wait(); }
                provider(&domain, "batch-live", 0)
            }) as Arc<dyn Fn() -> (i64, i64, i64) + Send + Sync>
        };
        let writer = TaskWriter::new_with_live_config(queue, Arc::new(Notify::new()), WriterConfig::default(), Some(live));
        let make_request = |n: usize| {
            let (response, receiver) = oneshot::channel();
            (Request { task: WriteTask { data: TaskData { workflow_id: format!("w{n}"), ..Default::default() }, source_domain: domain_id, schedule_to_start_seconds: 0 }, response, accepted: None }, receiver)
        };
        let (first, first_rx) = make_request(0);
        writer.inner.sender.send(first).await.unwrap();
        crate::matching_blocking::run({ let entered = entered.clone(); move || entered.wait() }).await.unwrap();
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
        crate::matching_blocking::run({ let release = release.clone(); move || release.wait() }).await.unwrap();
        for response in responses { response.await.unwrap().unwrap(); }
        assert_eq!(calls.load(Ordering::SeqCst), 6, "one construction-time capacity sample plus Go's per-loop batch samples");
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
                accepted: None,
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
            tonic::Code::Internal
        );
    }
}
