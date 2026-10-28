//! Byte-preserving sequential Go-style Matching append actor. Mirrors
//! `matching_writer.rs` over `DurableTaskQueueBytes`: domain, task-list name,
//! workflow ID and RunID bytes never convert through `String`/legacy types.
//! Caller cancellation does not withdraw an enqueued request.
use crate::matching_registry::WriterLiveConfig;
use crate::matching_writer::{WriterConfig, WriterTracker};
use cadence_persistence::{
    matching_blob::SqlRunIdBytesError,
    matching_native_queue::{DurableTaskQueueBytes, NativeQueueError, PendingTaskBytes},
    matching_native_task_store::{AllocatedTaskBytes, MatchingTaskStoreBytesError as StoreError},
};
use rand::SeedableRng;
use std::collections::VecDeque;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tonic::Status;

/// Receives the raw malformed SQL RunID failure. It runs before the failed
/// append is completed to callers and before the actor releases its tracker.
pub type NativeFatalSink = Arc<dyn Fn(SqlRunIdBytesError) + Send + Sync>;

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
        let error = match crate::matching_persistence_limiter::with_persistence_retry_count(
            attempt,
            operation(),
        )
        .await
        {
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
    task: PendingTaskBytes,
    response: oneshot::Sender<Result<AllocatedTaskBytes, WriteError>>,
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
            let mut waiters = self
                .rendezvous
                .waiters
                .lock()
                .expect("Matching rendezvous queue lock");
            if let Some(index) = waiters.iter().position(|(id, _)| *id == self.id) {
                waiters.remove(index);
            }
        }
    }
}
async fn receive_rendezvous(
    rendezvous: &Rendezvous,
    stopped: &mut watch::Receiver<bool>,
) -> Option<Request> {
    loop {
        let notified = rendezvous.notify.notified();
        if let Some(request) = try_receive_rendezvous(rendezvous) {
            return Some(request);
        }
        tokio::select! {
            _ = stopped.changed() => return None,
            _ = notified => (),
        }
    }
}
fn try_receive_rendezvous(rendezvous: &Rendezvous) -> Option<Request> {
    let (_, mut request) = rendezvous
        .waiters
        .lock()
        .expect("Matching rendezvous queue lock")
        .pop_front()?;
    if let Some(accepted) = request.accepted.take() {
        let _ = accepted.send(());
    }
    Some(request)
}
struct Inner {
    sender: mpsc::Sender<Request>,
    rendezvous: Option<Arc<Rendezvous>>,
    stop: watch::Sender<bool>,
    completed: watch::Receiver<bool>,
    queue: Arc<DurableTaskQueueBytes>,
    config: WriterConfig,
    live_config: Option<WriterLiveConfig>,
    // Go deliberately leaves appendCh open on stop. Keep the receiver alive
    // while handles exist so a caller already waiting to enqueue times out.
    parked: Mutex<Option<mpsc::Receiver<Request>>>,
}

struct WriterCompletion(watch::Sender<bool>);
impl Drop for WriterCompletion {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

#[derive(Clone)]
pub struct NativeTaskWriter {
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
fn is_condition_failed(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::ConditionFailed { .. }
            | StoreError::Legacy(
                cadence_persistence::matching_task_store::MatchingTaskStoreError::ConditionFailed(
                    _
                )
            )
    )
}

impl NativeTaskWriter {
    pub fn new(
        queue: Arc<DurableTaskQueueBytes>,
        notify: Arc<Notify>,
        config: WriterConfig,
    ) -> Self {
        Self::new_with_fatal_sink(queue, notify, config, None, None, None)
    }
    /// Install Go's per-task-list writer settings. Batch size is sampled at
    /// each getWriteBatch loop condition; append timeout is sampled per call.
    pub fn new_with_live_config(
        queue: Arc<DurableTaskQueueBytes>,
        notify: Arc<Notify>,
        config: WriterConfig,
        live_config: Option<WriterLiveConfig>,
    ) -> Self {
        Self::new_with_fatal_sink(queue, notify, config, live_config, None, None)
    }
    pub(crate) fn new_with_fatal_sink(
        queue: Arc<DurableTaskQueueBytes>,
        notify: Arc<Notify>,
        config: WriterConfig,
        live_config: Option<WriterLiveConfig>,
        fatal: Option<NativeFatalSink>,
        tracker: Option<Arc<WriterTracker>>,
    ) -> Self {
        let capacity = live_config
            .as_ref()
            .map(|provider| provider().2)
            .unwrap_or(config.outstanding_appends);
        let capacity =
            usize::try_from(capacity).expect("negative matching outstanding task append threshold");
        let rendezvous = (capacity == 0).then(|| {
            Arc::new(Rendezvous {
                waiters: Mutex::new(VecDeque::new()),
                notify: Notify::new(),
                next_id: AtomicU64::new(1),
            })
        });
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (stop, mut stopped_rx) = watch::channel(false);
        let (completed, completed_rx) = watch::channel(false);
        let inner = Arc::new(Inner {
            sender,
            rendezvous: rendezvous.clone(),
            stop,
            completed: completed_rx,
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
        let completion = WriterCompletion(completed);
        tokio::spawn(async move {
            let _completion = completion;
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
                let maximum = || {
                    live_config
                        .as_ref()
                        .map(|provider| provider().0)
                        .unwrap_or(config.max_batch_size)
                };
                let batch = if let Some(rendezvous) = &rendezvous {
                    let mut batch = vec![first];
                    let mut additional = 0i64;
                    loop {
                        if additional >= maximum() {
                            break;
                        }
                        match try_receive_rendezvous(rendezvous) {
                            Some(request) => {
                                batch.push(request);
                                additional += 1;
                            }
                            None => break,
                        }
                    }
                    batch
                } else {
                    drain_batch_with(&mut vec![first], &mut receiver, maximum)
                };
                let data = batch
                    .iter()
                    .map(|request| request.task.clone())
                    .collect::<Vec<_>>();
                let queue_copy = queue.clone();
                let result = crate::matching_blocking::run(move || {
                    queue_copy.append_pending(&data, crate::matching_dispatch::now())
                })
                .await;
                let result = match result {
                    Ok(Ok(tasks)) => Ok(tasks),
                    Ok(Err(error)) => {
                        // Go persistence.IsTransientError: Internal, ServiceBusy,
                        // Timeout and DBUnavailable.
                        let condition = matches!(&error, NativeQueueError::Storage(e) if is_condition_failed(e));
                        let retryable = matches!(&error, NativeQueueError::Storage(e) if e.is_transient())
                            || (condition && kind == 1);
                        if let NativeQueueError::Storage(StoreError::MalformedSqlRunId(cause)) =
                            &error
                        {
                            // Fatal publication precedes any completion below.
                            if let Some(fatal) = &fatal {
                                fatal(cause.clone());
                            }
                        }
                        let status = if condition && kind == 1 {
                            Status::internal("StickyTaskConditionFailedError")
                        } else {
                            // Go returns the raw persistence error, which handleErr maps to Internal.
                            Status::internal(error.to_string())
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
    /// Wait for the actor to finish its accepted batch, publish any fatal cause,
    /// park the receiver and release its tracker. Call stop before waiting.
    pub async fn wait_stopped(&self) {
        let mut completed = self.inner.completed.clone();
        while !*completed.borrow_and_update() {
            if completed.changed().await.is_err() {
                return;
            }
        }
    }
    pub async fn append(&self, task: PendingTaskBytes) -> Result<AllocatedTaskBytes, Status> {
        self.append_inner(task).await.map_err(|error| error.status)
    }
    /// Only failed-delivery rewrites use this outer retry. Each individual
    /// CreateTasks transaction, including normal AddTask, stays single-attempt.
    pub async fn append_rewrite(
        &self,
        task: PendingTaskBytes,
    ) -> Result<AllocatedTaskBytes, Status> {
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
    async fn append_inner(&self, task: PendingTaskBytes) -> Result<AllocatedTaskBytes, WriteError> {
        if self.inner.queue.is_stopped() || *self.inner.stop.borrow() {
            return Err(stopped().into());
        }
        // Each append attempt (including rewrite retries) gets a fresh sample.
        let timeout_nanos = self
            .inner
            .live_config
            .as_ref()
            .map(|provider| provider().1)
            .unwrap_or_else(|| {
                self.inner
                    .config
                    .append_timeout
                    .as_nanos()
                    .min(i64::MAX as u128) as i64
            });
        let now = tokio::time::Instant::now();
        let deadline = if timeout_nanos <= 0 {
            now
        } else {
            now + Duration::from_nanos(timeout_nanos as u64)
        };
        let (response, receiver) = oneshot::channel();
        let mut request = Request {
            task,
            response,
            accepted: None,
        };
        if let Some(rendezvous) = &self.inner.rendezvous {
            let (accepted, accepted_rx) = oneshot::channel();
            request.accepted = Some(accepted);
            let id = rendezvous.next_id.fetch_add(1, Ordering::Relaxed);
            rendezvous
                .waiters
                .lock()
                .expect("Matching rendezvous queue lock")
                .push_back((id, request));
            let mut admission = AdmissionGuard {
                rendezvous: rendezvous.clone(),
                id,
                armed: true,
            };
            rendezvous.notify.notify_one();
            match tokio::time::timeout_at(deadline, accepted_rx).await {
                Ok(Ok(())) => admission.armed = false,
                _ => return Err(full().into()),
            }
            drop(admission);
        } else {
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
    use cadence_common::go_string::GoString;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskDataBytes},
        matching_native_task_store::{MatchingTaskStoreBytes, TaskListKeyBytes},
        matching_sqlite::SqliteTaskStore,
    };
    use std::num::NonZeroU32;
    use uuid::Uuid;

    fn key(domain: Uuid, name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: domain,
            name: GoString::from(name),
            task_type: 0,
        }
    }
    fn store(dir: &tempfile::TempDir) -> Arc<SqliteTaskStore> {
        Arc::new(SqliteTaskStore::create(dir.path().join("t.db"), BlobEncoding::ThriftRW).unwrap())
    }
    fn queue(
        store: &Arc<SqliteTaskStore>,
        key: TaskListKeyBytes,
        kind: i16,
    ) -> Arc<DurableTaskQueueBytes> {
        Arc::new(
            DurableTaskQueueBytes::open(store.clone(), key, kind, NonZeroU32::new(100).unwrap(), 1)
                .unwrap(),
        )
    }
    fn pending(workflow: &[u8], source: Uuid) -> PendingTaskBytes {
        PendingTaskBytes {
            data: TaskDataBytes {
                workflow_id: GoString::from(workflow),
                run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
                schedule_id: 7,
                ..Default::default()
            },
            source_domain: source,
            schedule_to_start_seconds: 0,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_byte_task_and_source_domain_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let queue_domain = Uuid::from_u128(1);
        let source = Uuid::from_u128(77);
        let k = key(queue_domain, b"tl\xff\xfe");
        let writer = NativeTaskWriter::new(
            queue(&s, k.clone(), 0),
            Arc::new(Notify::new()),
            WriterConfig::default(),
        );
        let allocated = writer.append(pending(b"wf\xff\x00", source)).await.unwrap();
        assert_eq!(allocated.data.workflow_id.as_bytes(), b"wf\xff\x00");
        assert!(
            MatchingTaskStoreBytes::get_tasks(s.as_ref(), &k, 0, i64::MAX, 100)
                .unwrap()
                .is_empty()
        );
        let source_key = TaskListKeyBytes {
            domain_id: source,
            ..k.clone()
        };
        let rows =
            MatchingTaskStoreBytes::get_tasks(s.as_ref(), &source_key, 0, i64::MAX, 100).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data.workflow_id.as_bytes(), b"wf\xff\x00");
        assert_eq!(rows[0].task_id, allocated.task_id);
        writer.stop();
        drop(writer);
        let connection = rusqlite::Connection::open(dir.path().join("t.db")).unwrap();
        let stored: Vec<u8> = connection
            .query_row(
                "SELECT domain_id FROM tasks WHERE task_list_name=CAST(? AS TEXT)",
                [b"tl\xff\xfe".as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, source.as_bytes().to_vec());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_accepted_append_still_persists() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let domain = Uuid::from_u128(1);
        let k = key(domain, b"cancel\xff");
        let writer = NativeTaskWriter::new(
            queue(&s, k.clone(), 0),
            Arc::new(Notify::new()),
            WriterConfig::default(),
        );
        // Hold the database writer lock so completion cannot race caller
        // cancellation. The available buffered slot accepts the first poll.
        let lock = rusqlite::Connection::open(dir.path().join("t.db")).unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();
        {
            let future = writer.append(pending(b"cancelled", domain));
            tokio::pin!(future);
            tokio::select! {
                biased;
                _ = &mut future => panic!("append completed before cancellation"),
                _ = tokio::task::yield_now() => (),
            }
            // Dropping the pinned future cancels the caller after enqueue.
        }
        lock.execute_batch("COMMIT").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let rows =
                    MatchingTaskStoreBytes::get_tasks(s.as_ref(), &k, 0, i64::MAX, 100).unwrap();
                if rows.len() == 1 {
                    assert_eq!(rows[0].data.workflow_id.as_bytes(), b"cancelled");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("accepted append was withdrawn by caller cancellation");
        writer.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rendezvous_zero_capacity_append_persists() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let domain = Uuid::from_u128(1);
        let writer = NativeTaskWriter::new(
            queue(&s, key(domain, b"rv"), 0),
            Arc::new(Notify::new()),
            WriterConfig {
                outstanding_appends: 0,
                ..WriterConfig::default()
            },
        );
        assert!(writer.inner.rendezvous.is_some());
        let task = writer.append(pending(b"rv", domain)).await.unwrap();
        assert_eq!(task.data.workflow_id.as_bytes(), b"rv");
        writer.stop();
    }

    async fn fenced_append(kind: i16) -> Status {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let domain = Uuid::from_u128(1);
        let k = key(domain, b"fence\xff");
        let stale = queue(&s, k.clone(), kind);
        // A second owner leases a newer range and fences the first.
        let _owner = queue(&s, k, kind);
        let writer = NativeTaskWriter::new(
            stale.clone(),
            Arc::new(Notify::new()),
            WriterConfig::default(),
        );
        // Isolate the mapped write response from owner shutdown, which Go
        // permits to race that response. The queue still records the lost lease.
        stale.set_stop_waker(Arc::new(|| {}));
        let error = writer.append(pending(b"fenced", domain)).await.unwrap_err();
        assert!(stale.is_stopped(), "lost lease must stop the owner");
        writer.stop();
        error
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_fence_maps_sticky_condition_failure() {
        let sticky = fenced_append(1).await;
        assert_eq!(sticky.code(), tonic::Code::Internal);
        assert_eq!(sticky.message(), "StickyTaskConditionFailedError");
        let normal = fenced_append(0).await;
        assert_eq!(normal.code(), tonic::Code::Internal);
        assert_ne!(normal.message(), "StickyTaskConditionFailedError");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fatal_sink_precedes_completion_and_shutdown_wait() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let domain = Uuid::from_u128(9);
        let received = Arc::new(Mutex::new(Vec::<SqlRunIdBytesError>::new()));
        let tracker = Arc::new(WriterTracker::default());
        let sink: NativeFatalSink = {
            let received = received.clone();
            Arc::new(move |cause| received.lock().unwrap().push(cause))
        };
        let writer = NativeTaskWriter::new_with_fatal_sink(
            queue(&s, key(domain, b"fatal\xff"), 0),
            Arc::new(Notify::new()),
            WriterConfig::default(),
            None,
            Some(sink),
            Some(tracker.clone()),
        );
        let mut task = pending(b"bad-run", domain);
        task.data = task
            .data
            .with_request_run_id(GoString::from(b"bad\xffrun".as_slice()));
        let error = writer.append(task).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        // The caller observed the failure, so the sink must already have run.
        assert_eq!(received.lock().unwrap().len(), 1);
        writer.stop();
        tokio::time::timeout(Duration::from_secs(2), writer.wait_stopped())
            .await
            .expect("native writer completion was not published");
        tokio::time::timeout(Duration::from_secs(2), tracker.wait_idle())
            .await
            .expect("writer actor did not exit after stop");
        let received = received.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].raw.as_bytes(), b"bad\xffrun");
    }
}
