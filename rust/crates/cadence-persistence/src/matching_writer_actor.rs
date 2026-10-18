//! Owned, bounded synchronous writer actor matching the Go taskWriter's
//! admission, batching and response semantics. Queue integration remains
//! deliberately separate: DurableTaskQueue currently has no per-call config
//! provider, and its append API accepts slices rather than individual calls.
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc, Mutex,
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
}
struct Request<T, R, E> {
    value: T,
    response: mpsc::SyncSender<Result<R, E>>,
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
    /// Cancellation is observed only after admission; accepted work remains in
    /// the actor and its bounded response cell cannot block response fanout.
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
            if self.stopped.load(Ordering::Acquire) {
                return Err(WriterError::Stopped);
            }
            match self.sender.try_send(request) {
                Ok(()) => break,
                Err(TrySendError::Disconnected(_)) => return Err(WriterError::Stopped),
                Err(TrySendError::Full(returned)) => {
                    request = returned;
                    if Instant::now() >= deadline {
                        return Err(WriterError::ServiceBusy);
                    }
                    thread::sleep(Duration::from_millis(1));
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
            match rx.recv_timeout(left.min(Duration::from_millis(2))) {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(error)) => return Err(WriterError::Write(error)),
                Err(mpsc::RecvTimeoutError::Timeout) if Instant::now() >= deadline => {
                    return Err(WriterError::ResponseTimeout)
                }
                Err(mpsc::RecvTimeoutError::Timeout) if cancelled.load(Ordering::Acquire) => {
                    return Err(WriterError::Cancelled)
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return Err(WriterError::Stopped),
            }
        }
    }
    /// Stop is no-drain. Join can wait for an in-progress synchronous handler.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(join) = self.join.lock().expect("writer join lock").take() {
            let _ = join.join();
        }
    }
}
impl<T, R, E> Drop for MatchingWriterActor<T, R, E> {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(join) = self.join.get_mut().expect("writer join lock").take() {
            let _ = join.join();
        }
    }
}
fn actor_loop<T, R, E>(
    receiver: Receiver<Request<T, R, E>>,
    stopped: Arc<AtomicBool>,
    batch_size: Arc<dyn Fn() -> i64 + Send + Sync>,
    handler: impl Fn(Vec<T>) -> Vec<Result<R, E>>,
) {
    while !stopped.load(Ordering::Acquire) {
        let first = match receiver.recv_timeout(Duration::from_millis(2)) {
            Ok(request) => request,
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
        for (response, result) in responses.into_iter().zip(handler(values)) {
            let _ = response.try_send(result);
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
            assert_eq!(trace.batch.first(), Some(&0));
            let extras = (trace.batch.len() - 1) as i64;
            assert!(extras <= trace.maximum.max(0));
            assert_eq!(trace.remaining + extras as usize, 5);
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
    }
}
