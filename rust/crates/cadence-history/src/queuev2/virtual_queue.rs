//! Port of queuev2/virtual_queue.go

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use super::deps::*;
use super::interface::Daemon;
use super::monitor::Monitor;
use super::pause_controller::{new_pause_controller, PauseController};
use super::queue_state::VirtualSliceState;
use super::virtual_slice::{slice_key, VirtualSlice};

pub const DAEMON_STATUS_INITIALIZED: i32 = 0;
pub const DAEMON_STATUS_STARTED: i32 = 1;
pub const DAEMON_STATUS_STOPPED: i32 = 2;

const TASK_SCHEDULER_THROTTLE_BACKOFF_INTERVAL: Duration = Duration::from_secs(5);
const TASK_READER_ERROR_BACKOFF_INTERVAL: Duration = Duration::from_secs(1);

pub type SliceVec = Vec<Arc<dyn VirtualSlice>>;

#[async_trait]
pub trait VirtualQueue: Daemon {
    async fn get_state(&self) -> Vec<VirtualSliceState>;
    async fn update_and_get_state(&self) -> Vec<VirtualSliceState>;
    async fn merge_slices(&self, slices: SliceVec);
    async fn merge_with_last_slice(&self, slice: Arc<dyn VirtualSlice>);
    async fn append_slices(&self, slices: SliceVec);
    async fn iterate_slices(&self, f: &mut (dyn FnMut(&Arc<dyn VirtualSlice>) + Send));
    async fn clear_slices(&self, f: &mut (dyn FnMut(&Arc<dyn VirtualSlice>) -> bool + Send));
    /// `f` returns (remaining slices, split).
    async fn split_slices(
        &self,
        f: &mut (dyn FnMut(&Arc<dyn VirtualSlice>) -> (SliceVec, bool) + Send),
    );
    fn pause(&self, d: Duration);
}

#[derive(Clone)]
pub struct VirtualQueueOptions {
    pub page_size: IntFn,
    pub max_pending_tasks_count: IntFn,
    pub poll_backoff_interval: DurationFn,
    pub poll_backoff_interval_jitter_coefficient: FloatFn,
}

struct QInner {
    virtual_slices: SliceVec,
    /// Go's `*list.Element`; identity of the slice being read. If that slice was
    /// removed from the list, "next" is nil (matching container/list semantics).
    slice_to_read: Option<Arc<dyn VirtualSlice>>,
}

pub struct VirtualQueueImpl {
    weak_self: Weak<VirtualQueueImpl>,
    queue_options: VirtualQueueOptions,
    processor: Arc<dyn Processor>,
    rescheduler: Arc<dyn Rescheduler>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    time_source: Arc<dyn TimeSource>,
    task_load_rate_limiter: Arc<dyn Limiter>,
    monitor: Arc<dyn Monitor>,

    status: AtomicI32,
    ctx: CancelToken,
    notify_tx: mpsc::Sender<()>,
    notify_rx: StdMutex<Option<mpsc::Receiver<()>>>,
    handle: StdMutex<Option<JoinHandle<()>>>,
    pause_controller: Arc<dyn PauseController>,
    inner: Mutex<QInner>,
}

#[allow(clippy::too_many_arguments)]
pub fn new_virtual_queue(
    processor: Arc<dyn Processor>,
    rescheduler: Arc<dyn Rescheduler>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    time_source: Arc<dyn TimeSource>,
    task_load_rate_limiter: Arc<dyn Limiter>,
    monitor: Arc<dyn Monitor>,
    virtual_slices: SliceVec,
    queue_options: VirtualQueueOptions,
) -> Arc<VirtualQueueImpl> {
    let (tx, rx) = mpsc::channel(1);
    let slice_to_read = virtual_slices.first().cloned();
    Arc::new_cyclic(|weak| VirtualQueueImpl {
        weak_self: weak.clone(),
        queue_options,
        processor,
        rescheduler,
        logger,
        metrics_scope,
        pause_controller: new_pause_controller(time_source.clone()),
        time_source,
        task_load_rate_limiter,
        monitor,
        status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
        ctx: CancelToken::new(),
        notify_tx: tx,
        notify_rx: StdMutex::new(Some(rx)),
        handle: StdMutex::new(None),
        inner: Mutex::new(QInner {
            virtual_slices,
            slice_to_read,
        }),
    })
}

#[async_trait]
impl Daemon for VirtualQueueImpl {
    async fn start(&self) {
        if self
            .status
            .compare_exchange(
                DAEMON_STATUS_INITIALIZED,
                DAEMON_STATUS_STARTED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }

        self.pause_controller
            .subscribe("virtual-queue", self.notify_tx.clone());
        let rx = self.notify_rx.lock().unwrap().take();
        if let (Some(rx), Some(me)) = (rx, self.weak_self.upgrade()) {
            let h = tokio::spawn(async move { me.run(rx).await });
            *self.handle.lock().unwrap() = Some(h);
        }

        self.notify();

        self.logger.info("Virtual queue state changed: started");
    }

    async fn stop(&self) {
        if self
            .status
            .compare_exchange(
                DAEMON_STATUS_STARTED,
                DAEMON_STATUS_STOPPED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }

        self.pause_controller.unsubscribe("virtual-queue");
        self.pause_controller.stop();

        self.ctx.cancel();
        let h = self.handle.lock().unwrap().take();
        if let Some(h) = h {
            let _ = h.await;
        }

        let inner = self.inner.lock().await;
        for slice in &inner.virtual_slices {
            slice.clear();
        }

        self.logger.info("Virtual queue state changed: stopped");
    }
}

#[async_trait]
impl VirtualQueue for VirtualQueueImpl {
    async fn get_state(&self) -> Vec<VirtualSliceState> {
        let inner = self.inner.lock().await;
        inner.virtual_slices.iter().map(|s| s.get_state()).collect()
    }

    async fn update_and_get_state(&self) -> Vec<VirtualSliceState> {
        let mut inner = self.inner.lock().await;
        let mut states = Vec::with_capacity(inner.virtual_slices.len());
        let mut kept: SliceVec = Vec::with_capacity(inner.virtual_slices.len());
        for slice in std::mem::take(&mut inner.virtual_slices) {
            let state = slice.update_and_get_state();
            if slice.is_empty() {
                self.monitor.remove_slice(&slice);
            } else {
                states.push(state);
                self.monitor
                    .set_slice_pending_task_count(&slice, slice.get_pending_task_count() as i64);
                kept.push(slice);
            }
        }
        inner.virtual_slices = kept;
        states
    }

    async fn merge_slices(&self, incoming_slices: SliceVec) {
        if incoming_slices.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().await;

        let mut merged: SliceVec = Vec::new();
        let current = std::mem::take(&mut inner.virtual_slices);
        let (mut ci, mut ii) = (0, 0);
        while ci < current.len() && ii < incoming_slices.len() {
            let cur = &current[ci];
            let inc = &incoming_slices[ii];
            if cur
                .get_state()
                .range
                .inclusive_min_task_key
                .compare(&inc.get_state().range.inclusive_min_task_key)
                < 0
            {
                self.append_or_merge_slice(&mut merged, cur.clone());
                ci += 1;
            } else {
                self.append_or_merge_slice(&mut merged, inc.clone());
                ii += 1;
            }
        }
        for s in &current[ci..] {
            self.append_or_merge_slice(&mut merged, s.clone());
        }
        for s in &incoming_slices[ii..] {
            self.append_or_merge_slice(&mut merged, s.clone());
        }

        inner.virtual_slices = merged;
        self.reset_next_read_slice_locked(&mut inner);
    }

    async fn merge_with_last_slice(&self, incoming_slice: Arc<dyn VirtualSlice>) {
        let mut inner = self.inner.lock().await;
        let mut slices = std::mem::take(&mut inner.virtual_slices);
        self.append_or_merge_slice(&mut slices, incoming_slice);
        inner.virtual_slices = slices;
        self.reset_next_read_slice_locked(&mut inner);
    }

    async fn append_slices(&self, incoming_slices: SliceVec) {
        if incoming_slices.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().await;
        inner.virtual_slices.extend(incoming_slices);
        self.reset_next_read_slice_locked(&mut inner);
    }

    async fn iterate_slices(&self, f: &mut (dyn FnMut(&Arc<dyn VirtualSlice>) + Send)) {
        let inner = self.inner.lock().await;
        for s in &inner.virtual_slices {
            f(s);
        }
    }

    async fn clear_slices(&self, f: &mut (dyn FnMut(&Arc<dyn VirtualSlice>) -> bool + Send)) {
        let mut inner = self.inner.lock().await;
        for slice in &inner.virtual_slices {
            if f(slice) {
                slice.clear();
                self.monitor
                    .set_slice_pending_task_count(slice, slice.get_pending_task_count() as i64);
            }
        }
        self.reset_next_read_slice_locked(&mut inner);
    }

    async fn split_slices(
        &self,
        f: &mut (dyn FnMut(&Arc<dyn VirtualSlice>) -> (SliceVec, bool) + Send),
    ) {
        let mut inner = self.inner.lock().await;
        let mut remaining_slices: SliceVec = Vec::new();
        for slice in &inner.virtual_slices {
            let (remaining, split) = f(slice);
            if !split {
                remaining_slices.push(slice.clone());
                continue;
            }
            self.monitor.remove_slice(slice);
            for r in remaining {
                remaining_slices.push(r.clone());
                self.monitor
                    .set_slice_pending_task_count(&r, r.get_pending_task_count() as i64);
            }
        }
        inner.virtual_slices = remaining_slices;
        self.reset_next_read_slice_locked(&mut inner);
    }

    fn pause(&self, d: Duration) {
        self.pause_controller.pause(d);
    }
}

impl VirtualQueueImpl {
    fn notify(&self) {
        let _ = self.notify_tx.try_send(());
    }

    async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<()>) {
        loop {
            tokio::select! {
                _ = self.ctx.cancelled() => return,
                m = rx.recv() => {
                    if m.is_none() { return; }
                    self.load_and_submit_tasks().await;
                }
            }
        }
    }

    async fn load_and_submit_tasks(&self) {
        if let Err(e) = self.task_load_rate_limiter.wait(&self.ctx).await {
            if self.ctx.is_cancelled() {
                return;
            }
            self.logger.error(&format!(
                "Virtual queue failed to wait for rate limiter error={e}"
            ));
        }

        let mut inner = self.inner.lock().await;

        let slice_to_read = match &inner.slice_to_read {
            None => return,
            Some(s) => s.clone(),
        };

        let pending_task_count = self.monitor.get_total_pending_task_count();
        let max_task_count = (self.queue_options.max_pending_tasks_count)();
        self.metrics_scope
            .update_gauge(metric::PENDING_TASK_GAUGE, pending_task_count as f64);
        if pending_task_count >= max_task_count {
            self.logger.warn(&format!(
                "Too many pending tasks, pause loading tasks for a while pendingTaskCount={pending_task_count} maxTaskCount={max_task_count}"
            ));
            self.pause_controller
                .pause((self.queue_options.poll_backoff_interval)());
        }

        if self.pause_controller.is_paused() {
            self.metrics_scope
                .update_gauge(metric::VIRTUAL_QUEUE_PAUSED_GAUGE, 1.0);
            self.logger.debug("virtual queue is paused");
            return;
        }

        self.metrics_scope
            .update_gauge(metric::VIRTUAL_QUEUE_RUNNING_GAUGE, 1.0);

        let mut remaining_size = max_task_count - pending_task_count;
        if remaining_size <= 0 {
            remaining_size = 1;
            self.logger.error(&format!(
                "unexpected error, virtual queue is not paused when pending task count exceeds max task count limit pendingTaskCount={pending_task_count} maxTaskCount={max_task_count}"
            ));
        }
        let page_size = std::cmp::min((self.queue_options.page_size)(), remaining_size);
        self.logger.debug(&format!(
            "getting tasks from virtual queue pageSize={page_size}"
        ));
        let tasks = match slice_to_read.get_tasks(page_size.max(0) as usize).await {
            Ok(t) => t,
            Err(e) => {
                self.logger
                    .error(&format!("Virtual queue failed to get tasks error={e}"));
                self.pause_controller.pause(TASK_READER_ERROR_BACKOFF_INTERVAL);
                return;
            }
        };
        self.logger
            .debug(&format!("got tasks from virtual queue counter={}", tasks.len()));

        self.monitor.set_slice_pending_task_count(
            &slice_to_read,
            slice_to_read.get_pending_task_count() as i64,
        );

        let now = self.time_source.now();
        for task in tasks {
            if is_task_corrupted(task.as_ref()) {
                self.logger
                    .error("Virtual queue encountered a corrupted task");
                self.metrics_scope
                    .inc_counter(metric::CORRUPTED_HISTORY_TASK_COUNTER);
                task.ack();
                continue;
            }

            let scheduled_time = task.get_task_key().get_scheduled_time();
            if now < scheduled_time {
                self.rescheduler.reschedule_task(task, scheduled_time);
                continue;
            }
            self.metrics_scope.record_histogram_duration(
                metric::TASK_ENQUEUE_TO_FETCH_LATENCY,
                (now - task.get_visibility_timestamp())
                    .to_std()
                    .unwrap_or(Duration::ZERO),
            );
            task.set_initial_submit_time(now);
            let (tk, tv) = get_task_list_tag(
                &task.get_original_task_list(),
                task.get_original_task_list_kind(),
            );
            let task_list_tagged_scope = self.metrics_scope.tagged(&[(tk, tv)]);
            let submitted = match self.processor.try_submit(task.clone()) {
                Ok(s) => s,
                Err(e) => {
                    if self.ctx.is_cancelled() {
                        return;
                    }
                    self.logger
                        .error(&format!("Virtual queue failed to submit task error={e}"));
                    false
                }
            };
            if !submitted {
                self.metrics_scope
                    .inc_counter(metric::PROCESSING_QUEUE_THROTTLED_COUNTER);
                task_list_tagged_scope.inc_counter(metric::TASK_SCHEDULE_THROTTLED_PER_TASK_LIST);
                self.rescheduler.reschedule_task(
                    task,
                    self.time_source.now()
                        + chrono::Duration::from_std(TASK_SCHEDULER_THROTTLE_BACKOFF_INTERVAL)
                            .unwrap(),
                );
            } else {
                task_list_tagged_scope.inc_counter(metric::TASK_SCHEDULE_SUBMITTED_PER_TASK_LIST);
            }
        }

        if slice_to_read.has_more_tasks() {
            self.notify();
            return;
        }

        // sliceToRead = sliceToRead.Next()
        let key = slice_key(&slice_to_read);
        let next = inner
            .virtual_slices
            .iter()
            .position(|s| slice_key(s) == key)
            .and_then(|i| inner.virtual_slices.get(i + 1).cloned());
        inner.slice_to_read = next;
        if inner.slice_to_read.is_some() {
            self.notify();
        }
    }

    fn reset_next_read_slice_locked(&self, inner: &mut QInner) {
        inner.slice_to_read = inner
            .virtual_slices
            .iter()
            .find(|s| s.has_more_tasks())
            .cloned();
        if inner.slice_to_read.is_some() {
            self.notify();
        }
    }

    fn append_or_merge_slice(&self, slices: &mut SliceVec, incoming_slice: Arc<dyn VirtualSlice>) {
        let last_slice = match slices.last() {
            None => {
                slices.push(incoming_slice.clone());
                self.monitor.set_slice_pending_task_count(
                    &incoming_slice,
                    incoming_slice.get_pending_task_count() as i64,
                );
                return;
            }
            Some(l) => l.clone(),
        };
        let merged_slices = match last_slice.try_merge_with_virtual_slice(&incoming_slice) {
            None => {
                slices.push(incoming_slice.clone());
                self.monitor.set_slice_pending_task_count(
                    &incoming_slice,
                    incoming_slice.get_pending_task_count() as i64,
                );
                return;
            }
            Some(m) => m,
        };

        slices.pop();
        self.monitor.remove_slice(&last_slice);
        // incomingSlice may already be tracked by the monitor
        self.monitor.remove_slice(&incoming_slice);
        for m in merged_slices {
            slices.push(m.clone());
            self.monitor
                .set_slice_pending_task_count(&m, m.get_pending_task_count() as i64);
        }
    }
}
