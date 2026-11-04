//! Port of `go/service/history/queue/transfer_queue_processor_base.go`.
//!
//! Rust-forced differences (no logic change):
//! * `taskInitializer` and the rate limiter are injected (task/ and quotas are ported elsewhere);
//! * Go's timers (`time.AfterFunc`) are tokio tasks; `select { case ch <- x: default: }` is `try_send`;
//! * the test hooks `processQueueCollectionsFn` / `updateAckLevelFn` are not ported (test-only).
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::action::ActionType;
use super::constants::{DEFAULT_PROCESSING_QUEUE_LEVEL, GRACEFUL_SHUTDOWN_TIMEOUT};
use super::deps::{
    Daemon, Limiter, NotifyTaskInfo, QueueError, Redispatcher, ShardContext, TaskInitializer, TaskKey, TaskProcessor,
    TaskRef, Time, GetHistoryTasksRequest, HistoryTaskCategory, HistoryTaskKey, PersistenceTask, MetricsScope,
};
use super::processing_queue::min_task_key;
use super::processing_queue_state::ProcessingQueueState;
use super::processor_base::{
    ActionNotification, ProcessorBase, QueueShutdownFn, UpdateClusterAckLevelFn, UpdateMaxReadLevelFn,
    UpdateProcessingQueueStatesFn, DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED,
};
use super::processor_options::QueueProcessorOptions;
use super::split_policy::LookAheadFunc;
use super::transfer_queue_validator::TransferQueueValidator;
use super::util::{jit_duration, new_transfer_task_key, retry_persistence_operation};

pub const NUM_TASKS_ESTIMATION_DECAY: f64 = 0.6;
/// Go `loadQueueTaskThrottleRetryDelay`.
pub const LOAD_QUEUE_TASK_THROTTLE_RETRY_DELAY: Duration = Duration::from_secs(5);

/// `task.Key.(transferTaskKey).taskID`
pub fn transfer_task_id(k: TaskKey) -> i64 {
    match k {
        TaskKey::Transfer { task_id } => task_id,
        other => panic!("expected transfer task key, got {:?}", other),
    }
}

struct ProcessingState {
    backoff_timer: HashMap<i32, JoinHandle<()>>,
    should_process: HashMap<i32, bool>,
}

struct SplitState {
    last_split_time: Time,
    last_max_read_level: i64,
    estimated_tasks_per_minute: i64,
}

pub struct TransferQueueProcessorBase {
    pub base: ProcessorBase,
    task_initializer: TaskInitializer,

    notify_tx: mpsc::Sender<()>,
    process_tx: mpsc::Sender<()>,
    receivers: StdMutex<Option<(mpsc::Receiver<()>, mpsc::Receiver<()>)>>,

    start_jitter_timer: StdMutex<Option<JoinHandle<()>>>,
    processing: StdMutex<ProcessingState>,
    split: StdMutex<SplitState>,

    validator: Option<TransferQueueValidator>,
}

impl TransferQueueProcessorBase {
    /// Go `newTransferQueueProcessorBase`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard: Arc<dyn ShardContext>,
        processing_queue_states: Vec<ProcessingQueueState>,
        task_processor: Arc<dyn TaskProcessor>,
        redispatcher: Arc<dyn Redispatcher>,
        options: Arc<QueueProcessorOptions>,
        update_max_read_level: UpdateMaxReadLevelFn,
        update_cluster_ack_level: UpdateClusterAckLevelFn,
        update_processing_queue_states: Option<UpdateProcessingQueueStatesFn>,
        queue_shutdown: QueueShutdownFn,
        task_initializer: TaskInitializer,
        metrics_scope: Arc<dyn MetricsScope>,
        rate_limiter: Arc<dyn Limiter>,
    ) -> Arc<Self> {
        let enable_validator = shard.enable_debug_mode() && (options.enable_validator)();
        let validation_interval = options.validation_interval.clone();
        let shard_for_now = shard.clone();
        let base = ProcessorBase::new(
            shard,
            processing_queue_states,
            task_processor,
            redispatcher,
            options,
            update_max_read_level,
            update_cluster_ack_level,
            update_processing_queue_states,
            queue_shutdown,
            metrics_scope.clone(),
            rate_limiter,
        );
        let (notify_tx, notify_rx) = mpsc::channel(1);
        let (process_tx, process_rx) = mpsc::channel(1);
        let validator = if enable_validator {
            Some(TransferQueueValidator::new(
                Arc::new(move || shard_for_now.now()),
                validation_interval,
                metrics_scope,
            ))
        } else {
            None
        };
        Arc::new(TransferQueueProcessorBase {
            base,
            task_initializer,
            notify_tx,
            process_tx,
            receivers: StdMutex::new(Some((notify_rx, process_rx))),
            start_jitter_timer: StdMutex::new(None),
            processing: StdMutex::new(ProcessingState { backoff_timer: HashMap::new(), should_process: HashMap::new() }),
            split: StdMutex::new(SplitState {
                last_split_time: Time::ZERO,
                last_max_read_level: 0,
                estimated_tasks_per_minute: 0,
            }),
            validator,
        })
    }

    /// Go `Start`.
    pub fn start(self: &Arc<Self>) {
        if self
            .base
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        tracing::info!("Transfer queue processor state changed: starting");

        self.base.redispatcher.start();

        // trigger an initial (maybe delayed) load of tasks
        let start_jitter = (self.base.options.max_start_jitter_interval)();
        if !start_jitter.is_zero() {
            let nanos = rand::thread_rng().gen_range(0..start_jitter.as_nanos() as u64);
            let t = self.clone();
            let h = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_nanos(nanos)).await;
                t.notify_all_queue_collections().await;
            });
            *self.start_jitter_timer.lock().unwrap() = Some(h);
        } else {
            let t = self.clone();
            tokio::spawn(async move { t.notify_all_queue_collections().await });
        }

        let (notify_rx, process_rx) = self.receivers.lock().unwrap().take().expect("receivers taken once");
        let t = self.clone();
        self.base.shutdown_wg.spawn(async move { t.processor_pump(notify_rx, process_rx).await });
        tracing::info!("Transfer queue processor state changed: started");
    }

    /// Go `Stop`.
    pub async fn stop(&self) {
        if self
            .base
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        tracing::info!("Transfer queue processor state changed: stopping");

        self.base.shutdown_ch.cancel();
        if let Some(h) = self.start_jitter_timer.lock().unwrap().take() {
            h.abort();
        }
        {
            let mut p = self.processing.lock().unwrap();
            for (_, h) in p.backoff_timer.iter() {
                h.abort();
            }
            for v in p.should_process.values_mut() {
                *v = false;
            }
        }

        self.base.shutdown_wg.close();
        if tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, self.base.shutdown_wg.wait()).await.is_err() {
            tracing::warn!("transferQueueProcessorBase timed out on shut down");
        }

        self.base.redispatcher.stop();
        tracing::info!("Transfer queue processor state changed: stopped");
    }

    /// Go `notifyNewTask`.
    pub fn notify_new_task(&self, info: &NotifyTaskInfo) {
        let _ = self.notify_tx.try_send(());

        if info.execution_info.is_some() {
            if let Some(v) = &self.validator {
                // executionInfo will be nil when notifyNewTask is called to trigger a scan,
                // for example during domain failover or sync shard.
                v.add_tasks(info);
            }
        }
    }

    /// Go `readyForProcess`.
    pub fn ready_for_process(&self, level: i32) {
        let mut p = self.processing.lock().unwrap();
        if p.backoff_timer.contains_key(&level) {
            // current level is being throttled
            return;
        }
        p.should_process.insert(level, true);

        // trigger the actual processing
        let _ = self.process_tx.try_send(());
    }

    /// Go `setupBackoffTimer`.
    pub fn setup_backoff_timer(self: &Arc<Self>, level: i32) {
        let mut p = self.processing.lock().unwrap();
        if p.backoff_timer.contains_key(&level) {
            // there's an existing backoff timer, no-op
            // this case should not happen
            return;
        }

        self.base.metrics_scope.inc_counter("ProcessingQueueThrottledCounter");
        tracing::info!(queue_level = level, "Throttled processing queue");

        let backoff = jit_duration(
            (self.base.options.poll_backoff_interval)(),
            (self.base.options.poll_backoff_interval_jitter_coefficient)(),
        );
        let t = self.clone();
        let h = tokio::spawn(async move {
            tokio::time::sleep(backoff).await;
            if t.base.shutdown_ch.is_cancelled() {
                return;
            }
            let mut p = t.processing.lock().unwrap();
            p.should_process.insert(level, true);
            p.backoff_timer.remove(&level);

            // trigger the actual processing
            let _ = t.process_tx.try_send(());
        });
        p.backoff_timer.insert(level, h);
    }

    async fn processor_pump(self: Arc<Self>, mut notify_rx: mpsc::Receiver<()>, mut process_rx: mpsc::Receiver<()>) {
        let opts = self.base.options.clone();
        let ack_d = || jit_duration((opts.update_ack_interval)(), (opts.update_ack_interval_jitter_coefficient)());
        let split_d = || jit_duration((opts.split_queue_interval)(), (opts.split_queue_interval_jitter_coefficient)());
        let poll_d = || jit_duration((opts.max_poll_interval)(), (opts.max_poll_interval_jitter_coefficient)());

        let update_ack_timer = tokio::time::sleep(ack_d());
        tokio::pin!(update_ack_timer);
        let split_queue_timer = tokio::time::sleep(split_d());
        tokio::pin!(split_queue_timer);
        let max_poll_timer = tokio::time::sleep(poll_d());
        tokio::pin!(max_poll_timer);

        let mut action_rx = self.base.action_notify_rx.lock().await;

        loop {
            tokio::select! {
                _ = self.base.shutdown_ch.cancelled() => return,
                _ = notify_rx.recv() => {
                    // notify all queue collections as they are waiting for the notification when there's
                    // no more task to process.
                    self.notify_all_queue_collections().await;
                }
                _ = &mut max_poll_timer => {
                    self.notify_all_queue_collections().await;
                    max_poll_timer.as_mut().reset(tokio::time::Instant::now() + poll_d());
                }
                _ = process_rx.recv() => {
                    let max_redispatch = (opts.max_redispatch_queue_size)() as usize;
                    let size = self.base.redispatcher.size();
                    if size > max_redispatch {
                        tracing::debug!(size, max_redispatch, "Transfer queue has too many pending tasks in re-dispatch queue, block loading tasks from persistence");
                        self.base.redispatcher.redispatch(max_redispatch).await;
                        if self.base.redispatcher.size() > max_redispatch {
                            // this only happens when system is under very high load;
                            // back off instead of keeping submitting tasks to task processor
                            tokio::time::sleep(jit_duration(
                                (opts.poll_backoff_interval)(),
                                (opts.poll_backoff_interval_jitter_coefficient)(),
                            )).await;
                        }
                        // re-enqueue the event to see if we need keep re-dispatching or load new tasks
                        let _ = self.process_tx.try_send(());
                    } else {
                        self.process_queue_collections().await;
                    }
                }
                _ = &mut update_ack_timer => {
                    let (finished, err) = {
                        let mut colls = self.base.processing_queue_collections.lock().await;
                        let (finished, _, err) = self.base.update_ack_level(&mut colls).await;
                        (finished, err)
                    };
                    let shard_closed = err.as_ref().map(|e| e.is_shard_closed()).unwrap_or(false);
                    if shard_closed || (err.is_none() && finished) {
                        if !(opts.enable_graceful_sync_shutdown)() {
                            let t = self.clone();
                            tokio::spawn(async move { t.stop().await });
                            return;
                        }
                        self.stop().await;
                        return;
                    }
                    update_ack_timer.as_mut().reset(tokio::time::Instant::now() + ack_d());
                }
                _ = &mut split_queue_timer => {
                    self.split_queue().await;
                    split_queue_timer.as_mut().reset(tokio::time::Instant::now() + split_d());
                }
                notification = action_rx.recv() => {
                    if let Some(n) = notification {
                        self.handle_action_notification(n).await;
                    }
                }
            }
        }
    }

    async fn notify_all_queue_collections(&self) {
        let colls = self.base.processing_queue_collections.lock().await;
        for c in colls.iter() {
            self.ready_for_process(c.level());
        }
    }

    /// Go `processQueueCollections`.
    async fn process_queue_collections(self: &Arc<Self>) {
        let mut colls = self.base.processing_queue_collections.lock().await;
        for i in 0..colls.len() {
            let level = colls[i].level();
            {
                let mut p = self.processing.lock().unwrap();
                match p.should_process.get(&level) {
                    Some(true) => {}
                    _ => continue,
                }
                p.should_process.insert(level, false);
            }

            let (active_id, read_level, max_level, domain_filter) = match colls[i].active_queue() {
                // process for this queue collection has finished; new queue may be added after split/merge
                None => continue,
                Some(q) => (q.id, q.state().read_level(), q.state().max_level(), q.state().domain_filter().clone()),
            };

            let max_read_level = min_task_key(max_level, (self.base.update_max_read_level)());

            if !read_level.less(&max_read_level) {
                // no task need to be processed for now, wait for new task notification
                continue;
            }

            if self.base.rate_limiter.wait(LOAD_QUEUE_TASK_THROTTLE_RETRY_DELAY).await.is_err() {
                if level != DEFAULT_PROCESSING_QUEUE_LEVEL {
                    self.setup_backoff_timer(level);
                } else {
                    self.ready_for_process(level);
                }
                continue;
            }

            let (infos, more) = match self.read_tasks(read_level, max_read_level).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(error = %e, "Processor unable to retrieve tasks");
                    self.ready_for_process(level); // re-enqueue the event
                    continue;
                }
            };
            tracing::debug!(
                read_level = transfer_task_id(read_level),
                max_level = transfer_task_id(max_read_level),
                count = infos.len(),
                "load transfer tasks from database"
            );

            let mut tasks: HashMap<TaskKey, TaskRef> = HashMap::new();
            let mut task_ch_full = false;
            let now = self.base.shard.now();
            for info in &infos {
                if !domain_filter.filter(&info.get_domain_id()) {
                    tracing::debug!(task_id = info.get_task_id(), "transfer task filtered");
                    continue;
                }

                if info.is_corrupted() {
                    tracing::error!(task = ?info, "Processing queue encountered a corrupted task");
                    self.base.metrics_scope.inc_counter("CorruptedHistoryTaskCounter");
                    continue;
                }

                let task = (self.task_initializer)(info.clone());
                tasks.insert(new_transfer_task_key(info.get_task_id()), task.clone());
                let lat = now.sub_nanos(info.get_visibility_timestamp()).max(0) as u64;
                self.base.metrics_scope.record_timer("TaskEnqueueToFetchLatency", Duration::from_nanos(lat));
                match self.base.submit_task(task).await {
                    // only err here is due to the fact that processor has been shutdown
                    Err(_) => return,
                    Ok(submitted) => task_ch_full = task_ch_full || !submitted,
                }
            }

            let new_read_level = if !more {
                max_read_level
            } else {
                new_transfer_task_key(infos[infos.len() - 1].get_task_id())
            };
            let validator_tasks = self.validator.as_ref().map(|_| tasks.clone());
            let task_count = tasks.len();
            colls[i].add_tasks(tasks, new_read_level);
            if let (Some(v), Some(vt)) = (&self.validator, validator_tasks) {
                tracing::debug!(
                    read_level = transfer_task_id(read_level),
                    max_level = transfer_task_id(new_read_level),
                    count = task_count,
                    "ack transfer tasks"
                );
                v.ack_tasks(level, read_level, new_read_level, &vt, &colls[..]);
            }

            let active_changed = colls[i].active_queue().map(|q| q.id != active_id);
            if more || active_changed == Some(true) {
                // more tasks for the current active queue or the active queue has changed
                if level != DEFAULT_PROCESSING_QUEUE_LEVEL && task_ch_full {
                    self.setup_backoff_timer(level);
                } else {
                    self.ready_for_process(level);
                }
            }
            // else we don't have tasks to process for now; wait for new task notification
        }
    }

    /// Go `splitQueue`.
    async fn split_queue(&self) {
        let mut colls = self.base.processing_queue_collections.lock().await;
        let current_time = self.base.shard.now();
        let current_max_read_level = transfer_task_id((self.base.update_max_read_level)());
        let range_bits = self.base.shard.range_size_bits();

        let mut st = self.split.lock().unwrap();
        let last_split_time = st.last_split_time;
        let last_max_read_level = st.last_max_read_level;

        // deferred in Go
        let policy = (|| {
            if current_max_read_level - last_max_read_level < (2i64 << (range_bits - 1)) {
                // only update the estimation when rangeID is not renewed
                let secs = (current_time.sub_nanos(last_split_time) as f64 / 1e9) as i64;
                let num_tasks_per_minute = (current_max_read_level - last_max_read_level) / secs * 60;

                if st.estimated_tasks_per_minute == 0 {
                    // set the initial value for the estimation
                    st.estimated_tasks_per_minute = num_tasks_per_minute;
                } else {
                    st.estimated_tasks_per_minute = (NUM_TASKS_ESTIMATION_DECAY * st.estimated_tasks_per_minute as f64
                        + (1.0 - NUM_TASKS_ESTIMATION_DECAY) * num_tasks_per_minute as f64)
                        as i64;
                }
            }

            if last_split_time == Time::ZERO || st.estimated_tasks_per_minute == 0 {
                // skip the split as we can't estimate the look ahead taskID
                return None;
            }

            let estimated = st.estimated_tasks_per_minute;
            let opts = self.base.options.clone();
            let look_ahead: LookAheadFunc = Arc::new(move |key: TaskKey, domain_id: &str| {
                let minutes = ((opts.split_look_ahead_duration_by_domain_id)(domain_id).as_secs_f64() / 60.0) as i64;
                let total = estimated * minutes;
                // ensure the above calculation doesn't overflow and cap the maximum look ahead interval
                let total = total.min(2i64 << range_bits).max(0);
                new_transfer_task_key(transfer_task_id(key) + total)
            });
            self.base.initialize_split_policy(Some(look_ahead))
        })();
        st.last_split_time = current_time;
        st.last_max_read_level = current_max_read_level;
        drop(st);

        let Some(policy) = policy else { return };
        self.base.split_processing_queue_collection(&mut colls, Some(policy.as_ref()), |level, _| {
            self.ready_for_process(level);
        });
    }

    async fn handle_action_notification(&self, notification: ActionNotification) {
        let is_reset = matches!(notification.action.action_type, ActionType::Reset);
        let mut colls = self.base.processing_queue_collections.lock().await;
        self.base.handle_action_notification(&mut colls, notification, || {
            if is_reset {
                self.ready_for_process(DEFAULT_PROCESSING_QUEUE_LEVEL);
            }
        });
    }

    /// Go `readTasks`: (tasks, more).
    async fn read_tasks(
        &self,
        read_level: TaskKey,
        max_read_level: TaskKey,
    ) -> Result<(Vec<Arc<dyn PersistenceTask>>, bool), QueueError> {
        let shard = self.base.shard.clone();
        let batch = (self.base.options.batch_size)();
        let min_id = transfer_task_id(read_level) + 1;
        let max_id = transfer_task_id(max_read_level) + 1;
        let resp = retry_persistence_operation(|| {
            let shard = shard.clone();
            async move {
                shard
                    .get_history_tasks(GetHistoryTasksRequest {
                        task_category: HistoryTaskCategory::Transfer,
                        inclusive_min_task_key: HistoryTaskKey::immediate(min_id),
                        exclusive_max_task_key: HistoryTaskKey::immediate(max_id),
                        page_size: batch,
                        next_page_token: Vec::new(),
                        shard_id: shard.get_shard_id(),
                    })
                    .await
            }
        })
        .await?;
        let more = !resp.next_page_token.is_empty();
        Ok((resp.tasks, more))
    }
}

#[async_trait]
impl Daemon for TransferQueueProcessorBase {
    fn start(self: Arc<Self>) {
        TransferQueueProcessorBase::start(&self)
    }
    async fn stop(&self) {
        TransferQueueProcessorBase::stop(self).await
    }
}
