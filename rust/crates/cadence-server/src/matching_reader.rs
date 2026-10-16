//! Matching's local backlog reader: one SQL pump, a bounded task buffer,
//! and a separate head waiting for an unbuffered local poller handoff.
use cadence_persistence::{matching_queue::DurableTaskQueue, matching_sqlite::AllocatedTask};
use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch, Notify};

#[derive(Clone, Copy)]
pub struct ReaderConfig {
    pub batch_size: NonZeroU32,
    pub read_range_size: NonZeroU32,
    pub dispatch_timeout: Duration,
}
impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            batch_size: NonZeroU32::new(1000).unwrap(),
            read_range_size: NonZeroU32::new(50000).unwrap(),
            dispatch_timeout: Duration::from_secs(3),
        }
    }
}
struct Pending<T> {
    item: Mutex<Option<T>>,
    accepted: Notify,
}
struct Rendezvous<T> {
    offers: Mutex<VecDeque<Arc<Pending<T>>>>,
    available: Notify,
}
impl<T> Default for Rendezvous<T> {
    fn default() -> Self {
        Self {
            offers: Mutex::new(VecDeque::new()),
            available: Notify::new(),
        }
    }
}
struct OfferGuard<'a, T> {
    channel: &'a Rendezvous<T>,
    pending: Arc<Pending<T>>,
}
impl<T> Drop for OfferGuard<'_, T> {
    fn drop(&mut self) {
        self.channel
            .offers
            .lock()
            .expect("Matching handoff lock")
            .retain(|offer| !Arc::ptr_eq(offer, &self.pending));
    }
}
impl<T> Rendezvous<T> {
    async fn offer(&self, item: T, mut stop: watch::Receiver<bool>) -> bool {
        if *stop.borrow() {
            return false;
        }
        let pending = Arc::new(Pending {
            item: Mutex::new(Some(item)),
            accepted: Notify::new(),
        });
        let _guard = OfferGuard {
            channel: self,
            pending: pending.clone(),
        };
        self.offers
            .lock()
            .expect("Matching handoff lock")
            .push_back(pending.clone());
        self.available.notify_one();
        loop {
            let accepted = pending.accepted.notified();
            tokio::pin!(accepted);
            accepted.as_mut().enable();
            if pending
                .item
                .lock()
                .expect("Matching offered task lock")
                .is_none()
            {
                return true;
            }
            tokio::select! {
                _ = accepted => (),
                _ = stop.changed() => return false,
            }
        }
    }
    async fn poll(&self, mut stop: watch::Receiver<bool>) -> Option<T> {
        loop {
            if *stop.borrow() {
                return None;
            }
            let available = self.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            // Ownership moves synchronously during a poll of this future. A
            // cancelled waiting future never owns a task hidden in a oneshot.
            if let Some(pending) = self
                .offers
                .lock()
                .expect("Matching handoff lock")
                .pop_front()
            {
                let item = pending
                    .item
                    .lock()
                    .expect("Matching offered task lock")
                    .take();
                pending.accepted.notify_one();
                return item;
            }
            tokio::select! {
                _ = available => (),
                _ = stop.changed() => return None,
            }
        }
    }
}
#[derive(Default)]
struct Completion {
    count: Mutex<usize>,
    changed: Notify,
}
struct Completed(Arc<Completion>);
impl Drop for Completed {
    fn drop(&mut self) {
        *self
            .0
            .count
            .lock()
            .expect("Matching reader completion lock") += 1;
        self.0.changed.notify_waiters();
    }
}
struct Inner {
    matcher: Arc<Rendezvous<AllocatedTask>>,
    stop: watch::Sender<bool>,
    completion: Arc<Completion>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
#[derive(Clone)]
pub struct TaskReader {
    inner: Arc<Inner>,
}
enum BufferSender {
    Buffered(mpsc::Sender<AllocatedTask>),
    Unbuffered(Arc<Rendezvous<AllocatedTask>>),
}
enum BufferReceiver {
    Buffered(mpsc::Receiver<AllocatedTask>),
    Unbuffered(Arc<Rendezvous<AllocatedTask>>),
}
impl TaskReader {
    pub fn new(queue: Arc<DurableTaskQueue>, wake: Arc<Notify>, config: ReaderConfig) -> Self {
        Self::with_domain_activity(queue, wake, config, Arc::new(|| true))
    }
    pub fn with_domain_activity(
        queue: Arc<DurableTaskQueue>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let matcher = Arc::new(Rendezvous::default());
        let completion = Arc::new(Completion::default());
        let inner = Arc::new(Inner {
            matcher: matcher.clone(),
            stop,
            completion: completion.clone(),
        });
        let (buffer_send, mut buffer_receive) = if config.batch_size.get() == 1 {
            let buffer = Arc::new(Rendezvous::default());
            (
                BufferSender::Unbuffered(buffer.clone()),
                BufferReceiver::Unbuffered(buffer),
            )
        } else {
            let (sender, receiver) = mpsc::channel(config.batch_size.get() as usize - 1);
            (
                BufferSender::Buffered(sender),
                BufferReceiver::Buffered(receiver),
            )
        };
        let dispatched = Completed(completion.clone());
        let mut dispatch_stop = stopped.clone();
        let dispatch_queue = queue.clone();
        tokio::spawn(async move {
            let _completed = dispatched;
            'dispatch: loop {
                if *dispatch_stop.borrow() {
                    break;
                }
                let task = match &mut buffer_receive {
                    BufferReceiver::Buffered(receiver) => tokio::select! {
                        task = receiver.recv() => task,
                        _ = dispatch_stop.changed() => None,
                    },
                    BufferReceiver::Unbuffered(buffer) => buffer.poll(dispatch_stop.clone()).await,
                };
                let Some(task) = task else { break };
                loop {
                    // Go rechecks expiry at each dispatch attempt, including
                    // after a row spent time waiting behind another head.
                    if task.data.expiry_nanos > 0 && now() > task.data.expiry_nanos {
                        let queue = dispatch_queue.clone();
                        let id = task.task_id;
                        let result = tokio::task::spawn_blocking(move || queue.complete(id)).await;
                        if !matches!(result, Ok(Ok(_))) {
                            dispatch_queue.stop();
                            break 'dispatch;
                        }
                        // Expired buffered rows are acknowledged without an
                        // immediate GC run, matching taskReader's expiry branch.
                        break;
                    }
                    let offer = matcher.offer(task.clone(), dispatch_stop.clone());
                    let offered = if active() {
                        // Go only applies the async dispatch deadline to active
                        // domains (or an unknown cache entry).
                        tokio::time::timeout(config.dispatch_timeout, offer)
                            .await
                            .ok()
                    } else {
                        Some(offer.await)
                    };
                    match offered {
                        Some(true) => break,
                        Some(false) => break 'dispatch,
                        None => continue,
                    }
                }
            }
            // Dropping the receiver releases a pump blocked on its next send.
        });
        let pumped = Completed(completion);
        let mut pump_stop = stopped;
        tokio::spawn(async move {
            let _completed = pumped;
            'pump: loop {
                if *pump_stop.borrow() {
                    break;
                }
                // Subscribe before SQL so appends during a scan are not lost.
                let notification = wake.notified();
                tokio::pin!(notification);
                notification.as_mut().enable();
                let queue_copy = queue.clone();
                let result = tokio::task::spawn_blocking(move || {
                    queue_copy.reader_turn(config.read_range_size, config.batch_size)
                })
                .await;
                if *pump_stop.borrow() || queue.is_stopped() {
                    break;
                }
                let mut turn = match result {
                    Ok(Ok(turn)) => turn,
                    Ok(Err(error)) => {
                        // Go re-signals even after a bounded retry terminates.
                        eprintln!("Matching backlog read failed: {error}");
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => {
                        eprintln!("Matching backlog pump failed: {error}");
                        queue.stop();
                        break;
                    }
                };
                let pending = turn.scan_pending();
                loop {
                    // Admission shares queue state with lease renewal; keep a
                    // potentially contended mutex off the async runtime too.
                    let admitted = tokio::task::spawn_blocking(move || {
                        let result = turn.next(now);
                        (turn, result)
                    })
                    .await;
                    let result = match admitted {
                        Ok((loaded, result)) => {
                            turn = loaded;
                            result
                        }
                        Err(error) => {
                            eprintln!("Matching backlog admission worker failed: {error}");
                            queue.stop();
                            break 'pump;
                        }
                    };
                    let task = match result {
                        Ok(Some(task)) => task,
                        Ok(None) => break,
                        Err(error) => {
                            if !queue.is_stopped() {
                                eprintln!("Matching backlog admission failed: {error}");
                                queue.stop();
                            }
                            break 'pump;
                        }
                    };
                    let sent = match &buffer_send {
                        BufferSender::Buffered(sender) => tokio::select! {
                            result = sender.send(task) => result.is_ok(),
                            _ = pump_stop.changed() => false,
                        },
                        BufferSender::Unbuffered(buffer) => {
                            buffer.offer(task, pump_stop.clone()).await
                        }
                    };
                    if !sent {
                        break 'pump;
                    }
                }
                // Release the exclusive cursor before the next SQL turn.
                drop(turn);
                if pending {
                    continue;
                }
                tokio::select! {
                    _ = notification => (),
                    _ = pump_stop.changed() => break,
                }
            }
        });
        Self { inner }
    }
    pub async fn poll(&self) -> Option<AllocatedTask> {
        self.inner.matcher.poll(self.inner.stop.subscribe()).await
    }
    pub fn stop_signal(&self) -> watch::Sender<bool> {
        self.inner.stop.clone()
    }
    pub fn stop(&self) {
        self.inner.stop.send_replace(true);
    }
    /// Join reader loops asynchronously after final ack persistence/GC. History
    /// delivery RPCs have separate ownership and are not included in this join.
    pub async fn wait_stopped(&self) {
        let completion = &self.inner.completion;
        loop {
            let changed = completion.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if *completion
                .count
                .lock()
                .expect("Matching reader completion lock")
                >= 2
            {
                return;
            }
            changed.await;
        }
    }
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskData},
        matching_sqlite::{SqliteTaskStore, TaskListKey},
    };
    use std::time::Duration;
    use uuid::Uuid;

    fn fixture(
        count: usize,
        batch: u32,
    ) -> (
        tempfile::TempDir,
        Arc<DurableTaskQueue>,
        TaskReader,
        Arc<Notify>,
    ) {
        configured_fixture(count, batch, Duration::from_secs(3), true)
    }
    fn configured_fixture(
        count: usize,
        batch: u32,
        dispatch_timeout: Duration,
        active: bool,
    ) -> (
        tempfile::TempDir,
        Arc<DurableTaskQueue>,
        TaskReader,
        Arc<Notify>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = Arc::new(
            DurableTaskQueue::open(
                store,
                TaskListKey {
                    domain_id: Uuid::from_u128(1),
                    name: "reader".into(),
                    task_type: 0,
                },
                0,
                NonZeroU32::new(100).unwrap(),
                now(),
            )
            .unwrap(),
        );
        queue
            .append(&vec![TaskData::default(); count], now())
            .unwrap();
        let wake = Arc::new(Notify::new());
        let reader = TaskReader::with_domain_activity(
            queue.clone(),
            wake.clone(),
            ReaderConfig {
                batch_size: NonZeroU32::new(batch).unwrap(),
                read_range_size: NonZeroU32::new(100).unwrap(),
                dispatch_timeout,
            },
            Arc::new(move || active),
        );
        (directory, queue, reader, wake)
    }
    async fn stop(queue: Arc<DurableTaskQueue>, reader: TaskReader) {
        reader.stop();
        queue.stop();
        tokio::time::timeout(Duration::from_secs(2), reader.wait_stopped())
            .await
            .unwrap();
    }
    async fn wait_read(queue: &DurableTaskQueue, expected: i64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while queue.levels().0 < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn matching_reader_prefetch_backpressure_and_stop_preserve_unacked_rows() {
        for batch in [1, 3] {
            let (_directory, queue, reader, _) = fixture(8, batch);
            // buffer=batch-1, separate dispatch head=1, blocked reader send=1.
            wait_read(&queue, i64::from(batch + 1)).await;
            assert_eq!(queue.levels().2, i64::from(batch + 1));
            let first = reader.poll().await.unwrap();
            assert_eq!(first.task_id, 1);
            queue.complete(first.task_id).unwrap();
            wait_read(&queue, i64::from(batch + 2)).await;
            assert_eq!(queue.levels().2, i64::from(batch + 1));
            stop(queue.clone(), reader).await;
            assert_eq!(queue.levels().1, 1);
            assert!(queue.retry_data(2).is_ok());
        }
    }
    #[tokio::test]
    async fn matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads() {
        for active in [true, false] {
            let (_directory, queue, reader, wake) =
                configured_fixture(0, 3, Duration::from_millis(10), active);
            let task = TaskData {
                expiry_nanos: now() + 500_000_000,
                ..TaskData::default()
            };
            queue.append(&[task], now()).unwrap();
            wake.notify_waiters();
            wait_read(&queue, 1).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while reader.inner.matcher.offers.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(550)).await;
            if active {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while queue.levels().1 != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            } else {
                assert_eq!(
                    queue.levels().2,
                    1,
                    "passive handoff has no active-domain deadline"
                );
                assert_eq!(reader.poll().await.unwrap().task_id, 1);
            }
            stop(queue, reader).await;
        }
    }
    #[tokio::test]
    async fn matching_reader_drops_row_that_expired_behind_dispatch_head() {
        let (_directory, queue, reader, wake) = fixture(0, 3);
        queue
            .append(
                &[
                    TaskData::default(),
                    TaskData {
                        expiry_nanos: now() + 200_000_000,
                        ..TaskData::default()
                    },
                ],
                now(),
            )
            .unwrap();
        wake.notify_waiters();
        wait_read(&queue, 2).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let first = reader.poll().await.unwrap();
        queue.complete(first.task_id).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while queue.levels().1 != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(queue.levels().2, 0);
        stop(queue, reader).await;
    }
    #[test]
    fn matching_reader_full_buffer_does_not_occupy_blocking_pool() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (_directory, queue, reader, _) = fixture(8, 3);
            wait_read(&queue, 4).await;
            let available = tokio::time::timeout(
                Duration::from_millis(500),
                tokio::task::spawn_blocking(|| 42),
            )
            .await;
            stop(queue, reader).await;
            assert_eq!(
                available
                    .expect("full reader buffer occupied blocking pool")
                    .unwrap(),
                42
            );
        });
    }
    #[tokio::test]
    async fn matching_reader_cancelled_wait_does_not_steal_later_task() {
        let (_directory, queue, reader, wake) = fixture(0, 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), reader.poll())
                .await
                .is_err()
        );
        let written = queue.append(&[TaskData::default()], now()).unwrap();
        wake.notify_waiters();
        let task = tokio::time::timeout(Duration::from_secs(2), reader.poll())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task, written[0]);
        stop(queue, reader).await;
    }
    #[tokio::test]
    async fn matching_reader_stop_disconnects_waiting_poll_and_joins_empty_pump() {
        let (_directory, queue, reader, _) = fixture(0, 3);
        let copy = reader.clone();
        let poll = tokio::spawn(async move { copy.poll().await });
        tokio::task::yield_now().await;
        stop(queue, reader).await;
        assert!(poll.await.unwrap().is_none());
    }
    #[tokio::test]
    async fn matching_reader_rendezvous_offer_waits_for_actual_acceptance() {
        let channel = Arc::new(Rendezvous::default());
        let (_stop, stopped) = watch::channel(false);
        let copy = channel.clone();
        let receiver = stopped.clone();
        let offered = tokio::spawn(async move { copy.offer(7, receiver).await });
        while channel.offers.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        assert!(!offered.is_finished());
        assert_eq!(channel.poll(stopped).await, Some(7));
        assert!(offered.await.unwrap());
    }
    #[tokio::test]
    async fn matching_reader_cancelled_offer_withdraws_before_handoff() {
        let channel = Arc::new(Rendezvous::default());
        let (stop, stopped) = watch::channel(false);
        let copy = channel.clone();
        let receiver = stopped.clone();
        let offered = tokio::spawn(async move { copy.offer(7, receiver).await });
        while channel.offers.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        offered.abort();
        let _ = offered.await;
        assert!(channel.offers.lock().unwrap().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), channel.poll(stopped.clone()))
                .await
                .is_err()
        );
        stop.send_replace(true);
        assert_eq!(channel.poll(stopped).await, None);
    }
}
