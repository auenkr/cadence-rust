//! Cancellation-safe ownership of asynchronous Matching task delivery.
//! A detached delivery worker retains responsibility for a claimed task even
//! when the waiting RPC future disappears. This is not the service registry.
use crate::matching_history::{start_error_kind, MatchingHistoryClient, StartErrorKind};
use cadence_persistence::{
    matching_queue::{DurableTaskQueue, QueueError},
    matching_sqlite::AllocatedTask,
};
use cadence_proto::{api as p, matching as m};
use std::{
    future::Future,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Notify};
use tonic::Status;

/// Observation of one getTask wait. EndPoll precedes History's start RPC, as
/// in Go taskListManager.getTask; failed deliveries begin another wait cycle.
#[derive(Clone)]
pub struct PollLifecycle {
    pub manager: Arc<std::sync::Mutex<cadence_common::poller::Manager>>,
    pub id: String,
    pub info: cadence_common::poller::Info,
}
struct PollWaitGuard(PollLifecycle);
impl PollLifecycle {
    fn begin(&self) -> PollWaitGuard {
        self.manager
            .lock()
            .expect("poller history lock")
            .start(&self.id, self.info.clone(), now());
        PollWaitGuard(self.clone())
    }
}
impl Drop for PollWaitGuard {
    fn drop(&mut self) {
        self.0
            .manager
            .lock()
            .expect("poller history lock")
            .end(&self.0.id, now());
    }
}

#[derive(Clone)]
pub struct TaskDispatcher {
    queue: Arc<DurableTaskQueue>,
    history: MatchingHistoryClient,
    domain_id: String,
    wake: Arc<Notify>,
    liveness: Option<Arc<cadence_common::liveness::Liveness>>,
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}
enum ClaimError {
    Queue(QueueError),
    Join(tokio::task::JoinError),
}
fn queue_status(error: QueueError) -> Status {
    Status::unavailable(error.to_string())
}
impl TaskDispatcher {
    pub fn new(
        queue: Arc<DurableTaskQueue>,
        history: MatchingHistoryClient,
        domain_id: String,
    ) -> Self {
        Self {
            queue,
            history,
            domain_id,
            wake: Arc::new(Notify::new()),
            liveness: None,
        }
    }
    pub fn with_liveness(mut self, liveness: Arc<cadence_common::liveness::Liveness>) -> Self {
        self.liveness = Some(liveness);
        self
    }
    pub fn notify_new_tasks(&self) {
        self.wake.notify_waiters();
    }
    async fn claim(&self) -> Result<(Option<AllocatedTask>, bool), ClaimError> {
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || {
            queue.read_batch(
                NonZeroU32::new(100000).unwrap(),
                NonZeroU32::new(1).unwrap(),
                now(),
            )
        })
        .await
        .map_err(ClaimError::Join)?
        .map(|mut batch| (batch.tasks.pop(), batch.scan_pending))
        .map_err(ClaimError::Queue)
    }
    async fn rewrite(&self, task: i64) {
        let queue = self.queue.clone();
        let result = tokio::task::spawn_blocking(move || {
            if let Err(error) = queue.rewrite_failed(task, now()) {
                queue.stop();
                return Err(error);
            }
            queue.run_garbage_collection(false)?;
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => (),
            Ok(Err(error)) => eprintln!("Matching task rewrite failed: {error}"),
            Err(error) => {
                eprintln!("Matching task rewrite worker failed: {error}");
                self.queue.stop();
            }
        }
        self.wake.notify_waiters();
    }
    async fn complete(&self, task: i64) {
        let queue = self.queue.clone();
        let result = tokio::task::spawn_blocking(move || {
            queue.complete(task)?;
            queue.run_garbage_collection(false)?;
            Ok::<_, QueueError>(())
        })
        .await;
        match result {
            Ok(Ok(())) => (),
            Ok(Err(error)) => eprintln!("Matching task completion failed: {error}"),
            Err(error) => {
                eprintln!("Matching task completion worker failed: {error}");
                self.queue.stop();
            }
        }
    }
    /// Caller/controller supplies the configured long-poll expiration. Dropping
    /// this future closes the receiver; the worker rewrites any claimed task.
    pub async fn poll_decision(
        &self,
        poll: p::PollForDecisionTaskRequest,
        expiration: Duration,
    ) -> Result<m::PollForDecisionTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        self.poll(poll, expiration, move |task, poll| {
            let history = history.clone();
            let domain = domain.clone();
            async move { history.start_decision(&domain, &task, poll).await }
        })
        .await
    }

    pub async fn poll_decision_with_poller(
        &self,
        poll: p::PollForDecisionTaskRequest,
        expiration: Duration,
        lifecycle: PollLifecycle,
    ) -> Result<m::PollForDecisionTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        self.poll_with_lifecycle(
            poll,
            expiration,
            move |task, poll| {
                let history = history.clone();
                let domain = domain.clone();
                async move { history.start_decision(&domain, &task, poll).await }
            },
            Some(lifecycle),
        )
        .await
    }
    pub async fn poll_activity(
        &self,
        poll: p::PollForActivityTaskRequest,
        expiration: Duration,
    ) -> Result<m::PollForActivityTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        self.poll(poll, expiration, move |task, poll| {
            let history = history.clone();
            let domain = domain.clone();
            async move { history.start_activity(&domain, &task, poll).await }
        })
        .await
    }

    pub async fn poll_activity_with_poller(
        &self,
        poll: p::PollForActivityTaskRequest,
        expiration: Duration,
        lifecycle: PollLifecycle,
    ) -> Result<m::PollForActivityTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        self.poll_with_lifecycle(
            poll,
            expiration,
            move |task, poll| {
                let history = history.clone();
                let domain = domain.clone();
                async move { history.start_activity(&domain, &task, poll).await }
            },
            Some(lifecycle),
        )
        .await
    }
    async fn poll<Q, R, F, Fut>(
        &self,
        poll: Q,
        expiration: Duration,
        deliver: F,
    ) -> Result<R, Status>
    where
        Q: Clone + Send + 'static,
        R: Default + Send + 'static,
        F: FnMut(AllocatedTask, Q) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        self.poll_with_lifecycle(poll, expiration, deliver, None)
            .await
    }
    async fn poll_with_lifecycle<Q, R, F, Fut>(
        &self,
        poll: Q,
        expiration: Duration,
        deliver: F,
        lifecycle: Option<PollLifecycle>,
    ) -> Result<R, Status>
    where
        Q: Clone + Send + 'static,
        R: Default + Send + 'static,
        F: FnMut(AllocatedTask, Q) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let (sender, receiver) = oneshot::channel();
        let worker = self.clone();
        tokio::spawn(async move {
            worker
                .delivery_worker(poll, sender, deliver, lifecycle)
                .await;
        });
        match tokio::time::timeout(expiration, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Status::unavailable("Matching delivery worker stopped.")),
            Err(_) => Ok(R::default()),
        }
    }
    async fn delivery_worker<Q, R, F, Fut>(
        &self,
        poll: Q,
        mut sender: oneshot::Sender<Result<R, Status>>,
        mut deliver: F,
        lifecycle: Option<PollLifecycle>,
    ) where
        Q: Clone + Send,
        R: Default + Send,
        F: FnMut(AllocatedTask, Q) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let mut waiting = None;
        let mut waiting_started = false;
        loop {
            if sender.is_closed() {
                return;
            }
            if !waiting_started {
                if let Some(liveness) = &self.liveness {
                    liveness.mark_alive(now());
                }
                waiting = lifecycle.as_ref().map(PollLifecycle::begin);
                waiting_started = true;
            }
            // Register before reading storage: an append between an empty read
            // and waiting must not lose its notification (important for short polls).
            let notification = self.wake.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            let (task, scan_pending) = match self.claim().await {
                Ok(value) => value,
                Err(ClaimError::Queue(QueueError::Stopped)) => {
                    drop(waiting.take());
                    // Go maps a closed task-list pump to an empty successful
                    // poll, allowing workers to poll its replacement normally.
                    let _ = sender.send(Ok(R::default()));
                    return;
                }
                Err(error) => {
                    drop(waiting.take());
                    let status = match error {
                        ClaimError::Queue(error) => queue_status(error),
                        ClaimError::Join(error) => Status::internal(error.to_string()),
                    };
                    let _ = sender.send(Err(status));
                    return;
                }
            };
            let Some(task) = task else {
                if scan_pending {
                    tokio::task::yield_now().await;
                    continue;
                }
                tokio::select! {_=sender.closed()=>return,_=notification=>(),_=tokio::time::sleep(Duration::from_millis(100))=>()};
                continue;
            };
            drop(waiting.take());
            waiting_started = false;
            if sender.is_closed() {
                self.rewrite(task.task_id).await;
                return;
            }
            let result = tokio::select! {
                biased;
                _=sender.closed()=>{self.rewrite(task.task_id).await;return;},
                result=deliver(task.clone(),poll.clone())=>result,
            };
            match result {
                Ok(response) => {
                    self.complete(task.task_id).await;
                    let _ = sender.send(Ok(response));
                    return;
                }
                Err(error) => match start_error_kind(&error) {
                    StartErrorKind::Discard => self.complete(task.task_id).await,
                    StartErrorKind::InactiveDomain => {
                        if self
                            .history
                            .refresh_workflow_tasks(&self.domain_id, &task)
                            .await
                            .is_ok()
                        {
                            self.complete(task.task_id).await;
                        } else {
                            self.rewrite(task.task_id).await;
                        }
                    }
                    StartErrorKind::Retry => self.rewrite(task.task_id).await,
                },
            }
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
    use prost::Message;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use uuid::Uuid;
    fn fixture(
        count: usize,
    ) -> (
        tempfile::TempDir,
        Arc<SqliteTaskStore>,
        TaskListKey,
        TaskDispatcher,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "queue".into(),
            task_type: 0,
        };
        let queue = Arc::new(
            DurableTaskQueue::open(
                store.clone(),
                key.clone(),
                0,
                NonZeroU32::new(100).unwrap(),
                now(),
            )
            .unwrap(),
        );
        let task = TaskData {
            workflow_id: "workflow".into(),
            run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
            schedule_id: 2,
            ..Default::default()
        }
        .at_creation(now(), 0);
        queue.append(&vec![task; count], now()).unwrap();
        let history = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        (
            directory,
            store,
            key,
            TaskDispatcher::new(queue, history, Uuid::from_u128(1).to_string()),
        )
    }
    #[tokio::test]
    async fn matching_dispatch_cancelled_poll_rewrites_claim_before_worker_exits() {
        let (_directory, store, key, dispatcher) = fixture(1);
        let claimed = Arc::new(Notify::new());
        let copy = claimed.clone();
        let worker = dispatcher.clone();
        let poll = tokio::spawn(async move {
            worker
                .poll((), Duration::from_secs(10), move |_, _| {
                    copy.notify_one();
                    std::future::pending::<Result<m::PollForDecisionTaskResponse, Status>>()
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .unwrap();
        poll.abort();
        assert!(poll.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let tasks = store.get_tasks(&key, 0, 100, 10).unwrap();
                if tasks.len() == 1 && tasks[0].task_id == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(dispatcher.queue.levels(), (1, 1, 0));
    }
    #[tokio::test]
    async fn matching_dispatch_transient_failure_rewrites_before_successful_redelivery() {
        let (_directory, store, key, dispatcher) = fixture(1);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let copy = seen.clone();
        let response = dispatcher
            .poll((), Duration::from_secs(2), move |task, _| {
                let mut seen = copy.lock().unwrap();
                seen.push(task.task_id);
                std::future::ready(if seen.len() == 1 {
                    Err(Status::internal("retry after exhausted History retries"))
                } else {
                    Ok(m::PollForDecisionTaskResponse {
                        task_token: vec![42],
                        ..Default::default()
                    })
                })
            })
            .await
            .unwrap();
        assert_eq!(response.task_token, vec![42]);
        assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
        dispatcher.queue.run_garbage_collection(true).unwrap();
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
    }
    #[tokio::test]
    async fn matching_dispatch_typed_terminal_error_drops_without_rewrite() {
        #[derive(Clone, PartialEq, Message)]
        struct StatusDetails {
            #[prost(message, repeated, tag = "3")]
            details: Vec<prost_types::Any>,
        }
        let (_directory, store, key, dispatcher) = fixture(2);
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        dispatcher
            .poll((), Duration::from_secs(2), move |task, _| {
                copy.fetch_add(1, Ordering::Relaxed);
                std::future::ready(if task.task_id == 1 {
                    Err(Status::with_details(
                        tonic::Code::NotFound,
                        "gone",
                        StatusDetails {
                            details: vec![prost_types::Any {
                                type_url:
                                    "type.googleapis.com/uber.cadence.api.v1.EntityNotExistsError"
                                        .into(),
                                value: Vec::new(),
                            }],
                        }
                        .encode_to_vec()
                        .into(),
                    ))
                } else {
                    Ok(m::PollForDecisionTaskResponse::default())
                })
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        dispatcher.queue.run_garbage_collection(true).unwrap();
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        assert_eq!(
            dispatcher
                .queue
                .append(&[TaskData::default()], now())
                .unwrap()[0]
                .task_id,
            3
        );
    }
    #[tokio::test]
    async fn matching_dispatch_empty_long_poll_expires_without_delivery() {
        let (_directory, _store, _key, dispatcher) = fixture(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let response = dispatcher
            .poll((), Duration::from_millis(10), move |_, _| {
                copy.fetch_add(1, Ordering::Relaxed);
                std::future::ready(Ok(m::PollForActivityTaskResponse::default()))
            })
            .await
            .unwrap();
        assert!(response.task_token.is_empty());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(dispatcher.queue.levels().2, 0);
    }
    #[tokio::test]
    async fn matching_dispatch_poller_wait_ends_before_history_and_restarts_after_failure() {
        let (_directory, _store, _key, dispatcher) = fixture(1);
        let manager = Arc::new(Mutex::new(cadence_common::poller::Manager::default()));
        let lifecycle = PollLifecycle {
            manager: manager.clone(),
            id: "poll".into(),
            info: cadence_common::poller::Info {
                identity: "worker".into(),
                rate_per_second: 42.0,
                isolation_group: "a".into(),
            },
        };
        let seen = Arc::new(Mutex::new(Vec::new()));
        let timestamps = seen.clone();
        dispatcher
            .poll_with_lifecycle(
                (),
                Duration::from_secs(2),
                move |_, _| {
                    let mut manager = manager.lock().unwrap();
                    assert!(
                        !manager.contains_poll("poll"),
                        "Go EndPoll precedes History start"
                    );
                    assert!(!manager.has_after(i64::MAX));
                    let mut seen = timestamps.lock().unwrap();
                    seen.push(manager.list(now())[0].last_access_time);
                    std::future::ready(if seen.len() == 1 {
                        Err(Status::internal("exhausted History retries"))
                    } else {
                        Ok(m::PollForDecisionTaskResponse::default())
                    })
                },
                Some(lifecycle),
            )
            .await
            .unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[1] >= seen[0]);
    }
    #[tokio::test]
    async fn matching_dispatch_empty_wait_does_not_refresh_liveness_repeatedly() {
        let (_directory, _store, _key, dispatcher) = fixture(0);
        let liveness = Arc::new(cadence_common::liveness::Liveness::new(0, 300_000_000_000));
        let worker = dispatcher.with_liveness(liveness.clone());
        let poll = tokio::spawn(async move {
            worker
                .poll_decision(
                    p::PollForDecisionTaskRequest::default(),
                    Duration::from_secs(10),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !liveness.is_alive(now()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Force the stored time backwards while the same GetTask wait is active.
        // Internal 100ms persistence reads must not count as another client poll.
        liveness.mark_alive(0);
        tokio::time::sleep(Duration::from_millis(220)).await;
        assert!(!liveness.is_alive(now()));
        poll.abort();
        assert!(poll.await.unwrap_err().is_cancelled());
    }
}
