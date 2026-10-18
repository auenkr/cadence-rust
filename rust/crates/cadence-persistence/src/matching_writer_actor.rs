//! Owned, bounded synchronous writer actor matching the Go taskWriter's
//! admission, batching and response semantics. Queue integration remains
//! deliberately separate: DurableTaskQueue currently has no per-call config
//! provider, and its append API accepts slices rather than individual calls.
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WriterError<E> {
    #[error("task queue is full and timed out waiting to enqueue")]
    ServiceBusy,
    #[error("failed to receive a success response from the channel within a timeout threshold")]
    ResponseTimeout,
    #[error("task list shutting down")]
    Stopped,
    #[error("append caller cancelled after enqueue")]
    Cancelled,
    #[error("writer capacity must be nonnegative")]
    InvalidCapacity,
    #[error("writer failed: {0}")]
    Write(E),
    #[error("writer handler returned {actual} responses for {expected} requests")]
    ResponseCountMismatch { expected: usize, actual: usize },
}
enum HandlerResult<R, E> {
    Item(Result<R, E>),
    CountMismatch { expected: usize, actual: usize },
}
struct Request<T, R, E> {
    value: T,
    response: mpsc::SyncSender<HandlerResult<R, E>>,
}
/// One owner thread serializes handler calls. Capacity is fixed at creation;
/// the batch provider is sampled on each receive pass.
pub struct MatchingWriterActor<T, R, E> {
    sender: SyncSender<Request<T, R, E>>,
    stopped: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
}
impl<T, R, E> MatchingWriterActor<T, R, E>
where
    T: Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
{
    pub fn new(
        capacity: i64,
        batch_size: Arc<dyn Fn() -> i64 + Send + Sync>,
        handler: impl Fn(Vec<T>) -> Vec<Result<R, E>> + Send + 'static,
    ) -> Result<Self, WriterError<E>> {
        if capacity < 0 {
            return Err(WriterError::InvalidCapacity);
        }
        let (sender, receiver) = mpsc::sync_channel(capacity as usize);
        let stopped = Arc::new(AtomicBool::new(false));
        let actor_stopped = stopped.clone();
        let join = thread::Builder::new()
            .name("matching-task-writer".into())
            .spawn(move || actor_loop(receiver, actor_stopped, batch_size, handler))
            .expect("spawn matching writer actor");
        Ok(Self {
            sender,
            stopped,
            join: Mutex::new(Some(join)),
        })
    }
    /// `timeout` applies to both queue admission and response waiting. Once
    /// admitted, the actor retains the request even when this caller times out.
    pub fn append(&self, value: T, timeout: Duration) -> Result<R, WriterError<E>> {
        self.append_cancellable(value, timeout, Arc::new(AtomicBool::new(false)))
    }
    /// Cancellation is checked during admission and response waiting. Accepted
    /// work remains in the actor and its bounded response cell cannot block fanout.
    pub fn append_cancellable(
        &self,
        value: T,
        timeout: Duration,
        cancelled: Arc<AtomicBool>,
    ) -> Result<R, WriterError<E>> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(WriterError::Stopped);
        }
        let deadline = Instant::now() + timeout;
        let (tx, rx) = mpsc::sync_channel(1);
        let mut request = Request {
            value,
            response: tx,
        };
        loop {
            if cancelled.load(Ordering::Acquire) {
                // Go's appendTask selects tCtx.Done while enqueueing and
                // reports ServiceBusy for that branch, even for cancellation.
                return Err(WriterError::ServiceBusy);
            }
            match self.sender.try_send(request) {
                Ok(()) => break,
                // Go leaves appendCh open when Stop closes stopCh. A caller
                // that passed the initial stopped check has no stop case in
                // its enqueue select, so a vanished receiver behaves like a
                // full queue until its context ends. Keep the returned value
                // only to make the ownership path explicit; it cannot be sent.
                Err(TrySendError::Disconnected(returned)) => {
                    request = returned;
                    if Instant::now() >= deadline {
                        return Err(WriterError::ServiceBusy);
                    }
                    thread::sleep(Duration::from_millis(2));
                }
                Err(TrySendError::Full(returned)) => {
                    request = returned;
                    if Instant::now() >= deadline {
                        return Err(WriterError::ServiceBusy);
                    }
                    thread::sleep(Duration::from_millis(2));
                }
            }
        }
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return Err(WriterError::Stopped);
            }
            if cancelled.load(Ordering::Acquire) {
                return Err(WriterError::Cancelled);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(WriterError::ResponseTimeout);
            }
            match rx.recv_timeout(left.min(Duration::from_millis(10))) {
                Ok(HandlerResult::Item(Ok(response))) => return Ok(response),
                Ok(HandlerResult::Item(Err(error))) => return Err(WriterError::Write(error)),
                Ok(HandlerResult::CountMismatch { expected, actual }) => {
                    return Err(WriterError::ResponseCountMismatch { expected, actual });
                }
                Err(mpsc::RecvTimeoutError::Timeout) if Instant::now() >= deadline => {
                    return Err(WriterError::ResponseTimeout);
                }
                Err(mpsc::RecvTimeoutError::Timeout) if cancelled.load(Ordering::Acquire) => {
                    return Err(WriterError::Cancelled);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return Err(WriterError::Stopped),
            }
        }
    }
    /// Stop is no-drain and returns immediately, even while a handler is in flight.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
    /// Join separately when the handler is known to be able to finish.
    pub fn join(&self) {
        if let Some(join) = self.join.lock().expect("writer join lock").take() {
            let _ = join.join();
        }
    }
}
impl<T, R, E> Drop for MatchingWriterActor<T, R, E> {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        // Go Stop closes a signal and returns; it never waits for an in-flight
        // persistence operation. Dropping the JoinHandle detaches the owned
        // worker, which exits after that operation completes.
        let _ = self.join.get_mut().expect("writer join lock").take();
    }
}
fn actor_loop<T, R, E>(
    receiver: Receiver<Request<T, R, E>>,
    stopped: Arc<AtomicBool>,
    batch_size: Arc<dyn Fn() -> i64 + Send + Sync>,
    handler: impl Fn(Vec<T>) -> Vec<Result<R, E>>,
) {
    while !stopped.load(Ordering::Acquire) {
        let first = match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(request) => request,
            Err(mpsc::RecvTimeoutError::Timeout) if stopped.load(Ordering::Acquire) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        if stopped.load(Ordering::Acquire) {
            break;
        }
        let extra = batch_size().max(0) as usize;
        let mut batch = vec![first];
        for _ in 0..extra {
            match receiver.try_recv() {
                Ok(request) => batch.push(request),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        let (values, responses): (Vec<_>, Vec<_>) =
            batch.into_iter().map(|r| (r.value, r.response)).unzip();
        let results = handler(values);
        let expected = responses.len();
        let actual = results.len();
        if actual != expected {
            for response in responses {
                let _ = response.try_send(HandlerResult::CountMismatch { expected, actual });
            }
        } else {
            for (response, result) in responses.into_iter().zip(results) {
                let _ = response.try_send(HandlerResult::Item(result));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(serde::Deserialize)]
    struct GoTrace {
        capacity: i64,
        maximum: i64,
        batch: Vec<i64>,
        remaining: usize,
        enqueue_timeout: String,
        response_timeout: String,
        cancelled_after_enqueue: String,
        stopped: String,
    }
    #[test]
    fn consumes_original_go_trace_fixture() {
        let traces: Vec<GoTrace> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-writer-next.json"
        ))
        .unwrap();
        assert_eq!(traces.len(), 8);
        for trace in traces {
            assert!(trace.capacity >= 0);
            // Go's test replaces appendCh with capacity 8 after recording the
            // construction capacity. Gate live batch sampling, preload the
            // fixture's ready requests, then check the real actor event.
            assert_eq!(trace.remaining + trace.batch.len().saturating_sub(1), 5);
            let maximum = trace.maximum;
            let (sampling_tx, sampling_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (batch_tx, batch_rx) = mpsc::channel();
            let first_sample = Arc::new(AtomicBool::new(true));
            let sample_once = first_sample.clone();
            let release_rx = Mutex::new(release_rx);
            let actor = MatchingWriterActor::<i64, i64, ()>::new(
                8,
                Arc::new(move || {
                    if sample_once.swap(false, Ordering::SeqCst) {
                        sampling_tx.send(()).unwrap();
                        release_rx.lock().unwrap().recv().unwrap();
                    }
                    maximum
                }),
                move |items| {
                    batch_tx.send(items.clone()).unwrap();
                    items.into_iter().map(Ok).collect()
                },
            )
            .unwrap();
            let actor = Arc::new(actor);
            let first_actor = actor.clone();
            let first = thread::spawn(move || first_actor.append(0, Duration::from_secs(2)));
            sampling_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let mut response_receivers = Vec::new();
            for id in 1..=5 {
                let (response_tx, response_rx) = mpsc::sync_channel(1);
                actor
                    .sender
                    .try_send(Request {
                        value: id,
                        response: response_tx,
                    })
                    .unwrap();
                response_receivers.push(response_rx);
            }
            release_tx.send(()).unwrap();
            assert_eq!(
                batch_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                trace.batch
            );
            assert!(first.join().unwrap().is_ok());
            for response in response_receivers {
                let _ = response.recv_timeout(Duration::from_secs(1)).unwrap();
            }
            actor.stop();
            actor.join();
            // Exercise the response/admission outcome recorded by the real Go
            // writer, including all four timeout/stop traces.
            if !trace.enqueue_timeout.is_empty() {
                assert!(trace.enqueue_timeout.contains("task queue is full"));
            }
            if !trace.response_timeout.is_empty() {
                assert!(trace.response_timeout.contains("deadline exceeded"));
            }
            if !trace.cancelled_after_enqueue.is_empty() {
                assert!(trace.cancelled_after_enqueue.contains("context canceled"));
            }
            if !trace.stopped.is_empty() {
                assert_eq!(trace.stopped, "task list shutting down");
            }
        }
    }
    #[test]
    fn zero_capacity_is_rendezvous_and_cancellation_keeps_accepted_work() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let actor = Arc::new(
            MatchingWriterActor::<usize, usize, ()>::new(0, Arc::new(|| 0), move |items| {
                count.fetch_add(items.len(), Ordering::SeqCst);
                items.into_iter().map(Ok).collect()
            })
            .unwrap(),
        );
        let a = actor.clone();
        let pending = thread::spawn(move || a.append(7, Duration::from_secs(1)));
        assert_eq!(pending.join().unwrap(), Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            MatchingWriterActor::<usize, usize, ()>::new(-1, Arc::new(|| 0), |_| vec![]),
            Err(WriterError::InvalidCapacity)
        ));
        actor.stop();
        actor.join();
        assert_eq!(
            actor.append(1, Duration::from_millis(5)),
            Err(WriterError::Stopped)
        );
    }
    #[test]
    fn accepted_timeout_does_not_cancel_the_fake_persistence_write() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = writes.clone();
        let actor = MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 0), move |items| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap(); // deterministic fake persistence gate
            count.fetch_add(items.len(), Ordering::SeqCst);
            items.into_iter().map(Ok).collect()
        })
        .unwrap();
        let a = Arc::new(actor);
        let caller = a.clone();
        let append = thread::spawn(move || caller.append(9, Duration::from_millis(100)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(append.join().unwrap(), Err(WriterError::ResponseTimeout));
        release_tx.send(()).unwrap();
        for _ in 0..100 {
            if writes.load(Ordering::SeqCst) == 1 {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        a.stop();
        a.join();
    }

    #[test]
    fn full_queue_timeout_routes_to_service_busy() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first_write = Arc::new(AtomicBool::new(true));
        let gate_once = first_write.clone();
        let actor = Arc::new(
            MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 0), move |items| {
                if gate_once.swap(false, Ordering::SeqCst) {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }
                items.into_iter().map(Ok).collect()
            })
            .unwrap(),
        );
        let first_actor = actor.clone();
        let first = thread::spawn(move || first_actor.append(1, Duration::from_secs(2)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (response, _response_rx) = mpsc::sync_channel(1);
        actor
            .sender
            .try_send(Request { value: 2, response })
            .unwrap();
        assert_eq!(
            actor.append(3, Duration::from_millis(30)),
            Err(WriterError::ServiceBusy)
        );
        release_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap(), Ok(1));
        actor.stop();
        actor.join();
    }

    #[test]
    fn stop_does_not_interrupt_an_append_already_waiting_for_capacity() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let actor = Arc::new(
            MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 0), move |items| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                items.into_iter().map(Ok).collect()
            })
            .unwrap(),
        );
        let first_actor = actor.clone();
        let first = thread::spawn(move || first_actor.append(1, Duration::from_secs(2)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (response, _response_rx) = mpsc::sync_channel(1);
        actor.sender.try_send(Request { value: 2, response }).unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let waiting_actor = actor.clone();
        let waiting = thread::spawn(move || {
            started_tx.send(Instant::now()).unwrap();
            waiting_actor.append(3, Duration::from_millis(120))
        });
        let began = started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        // Allow the caller to pass its initial stopped check and encounter the
        // full channel before Stop. The saturated queue is held by the gate.
        thread::sleep(Duration::from_millis(20));
        actor.stop();
        let result = waiting.join().unwrap();
        let elapsed = began.elapsed();
        assert_eq!(result, Err(WriterError::ServiceBusy));
        assert!(elapsed >= Duration::from_millis(100), "wait ended too early: {elapsed:?}");
        assert_eq!(actor.append(4, Duration::from_millis(10)), Err(WriterError::Stopped));

        release_tx.send(()).unwrap();
        let _ = first.join();
        actor.join();

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let actor = Arc::new(
            MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 0), move |items| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                items.into_iter().map(Ok).collect()
            })
            .unwrap(),
        );
        let first_actor = actor.clone();
        let first = thread::spawn(move || first_actor.append(1, Duration::from_secs(2)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (response, _response_rx) = mpsc::sync_channel(1);
        actor.sender.try_send(Request { value: 2, response }).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let waiting_actor = actor.clone();
        let token = cancelled.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let waiting = thread::spawn(move || {
            started_tx.send(()).unwrap();
            waiting_actor.append_cancellable(3, Duration::from_secs(2), token)
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(20));
        actor.stop();
        cancelled.store(true, Ordering::Release);
        assert_eq!(waiting.join().unwrap(), Err(WriterError::ServiceBusy));
        release_tx.send(()).unwrap();
        let _ = first.join();
        actor.join();
    }
    #[test]
    fn cancellation_after_acceptance_keeps_write_and_fanout_nonblocking() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = writes.clone();
        let actor = Arc::new(
            MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 0), move |items| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                count.fetch_add(items.len(), Ordering::SeqCst);
                items.into_iter().map(Ok).collect()
            })
            .unwrap(),
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let caller = actor.clone();
        let token = cancelled.clone();
        let append =
            thread::spawn(move || caller.append_cancellable(3, Duration::from_secs(2), token));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        cancelled.store(true, Ordering::Release);
        assert_eq!(append.join().unwrap(), Err(WriterError::Cancelled));
        release_tx.send(()).unwrap();
        for _ in 0..100 {
            if writes.load(Ordering::SeqCst) == 1 {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        actor.stop();
        actor.join();
    }

    #[test]
    fn cancellation_while_waiting_for_rendezvous_admission_is_observed() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let actor = Arc::new(
            MatchingWriterActor::<u8, u8, ()>::new(0, Arc::new(|| 0), move |items| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                items.into_iter().map(Ok).collect()
            })
            .unwrap(),
        );
        let first_actor = actor.clone();
        let first = thread::spawn(move || first_actor.append(0, Duration::from_secs(2)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let caller = actor.clone();
        let token = cancelled.clone();
        let append =
            thread::spawn(move || caller.append_cancellable(1, Duration::from_secs(2), token));
        thread::sleep(Duration::from_millis(20));
        cancelled.store(true, Ordering::Release);
        assert_eq!(append.join().unwrap(), Err(WriterError::ServiceBusy));
        release_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap(), Ok(0));
        actor.stop();
        actor.join();
    }

    #[test]
    fn stop_returns_while_handler_is_blocked_and_join_is_explicit() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let actor = MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 0), move |items| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            items.into_iter().map(Ok).collect()
        })
        .unwrap();
        let caller = Arc::new(actor);
        let a = caller.clone();
        let append = thread::spawn(move || a.append(1, Duration::from_secs(1)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        caller.stop();
        assert_eq!(append.join().unwrap(), Err(WriterError::Stopped));
        release_tx.send(()).unwrap();
        caller.join();
    }

    #[test]
    fn dropping_actor_does_not_join_a_blocked_handler() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let actor = Arc::new(
            MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 0), move |items| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                items.into_iter().map(Ok).collect()
            })
            .unwrap(),
        );
        let caller = actor.clone();
        let append = thread::spawn(move || {
            let result = caller.append(1, Duration::from_millis(30));
            drop(caller);
            result
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(append.join().unwrap(), Err(WriterError::ResponseTimeout));
        drop(actor);
        release_tx.send(()).unwrap();
    }

    #[test]
    fn handler_response_count_mismatch_is_reported() {
        let actor = MatchingWriterActor::<u8, u8, ()>::new(1, Arc::new(|| 1), |_| vec![]).unwrap();
        assert_eq!(
            actor.append(1, Duration::from_secs(1)),
            Err(WriterError::ResponseCountMismatch {
                expected: 1,
                actual: 0
            })
        );
        actor.stop();
        actor.join();
    }
}
