//! Port of `go/service/history/queue/timer_queue_processor_base.go`.
//!
//! Rust-forced differences (no logic change):
//! * `taskInitializer` and the rate limiter are injected (task/ and quotas are ported elsewhere);
//! * Go's timers (`time.AfterFunc`) are tokio tasks; `select { case ch <- x: default: }` is `try_send`;
//! * Go compares `ProcessingQueue` pointers; here the queue `id` stands in for pointer identity;
//! * per-task-type metric scopes (`task.GetTimerTaskMetricScope`) collapse into one counter on the
//!   queue's metrics scope;
//! * the test hooks `updateAckLevelFn` / `splitProcessingQueueCollectionFn` are not ported (test-only).
use std::collections::{HashMap, HashSet};
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
    Daemon, GetHistoryTasksRequest, HistoryTaskCategory, HistoryTaskKey, Limiter, MetricsScope, PersistenceTask,
    QueueError, Redispatcher, ShardContext, TaskInitializer, TaskKey, TaskProcessor, TaskRef, Time,
};
use super::processing_queue::min_task_key;
use super::processing_queue_state::ProcessingQueueState;
use super::processor_base::{
    ActionNotification, ProcessorBase, QueueShutdownFn, UpdateClusterAckLevelFn, UpdateMaxReadLevelFn,
    UpdateProcessingQueueStatesFn, DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED,
};
use super::processor_options::QueueProcessorOptions;
use super::split_policy::LookAheadFunc;
use super::timer_gate::TimerGate;
use super::transfer_queue_processor_base::LOAD_QUEUE_TASK_THROTTLE_RETRY_DELAY;
use super::util::{jit_duration, new_timer_task_key, retry_persistence_operation, MAXIMUM_TIMER_TASK_KEY};

/// `task.Key.(timerTaskKey).visibilityTimestamp`
pub fn timer_visibility_timestamp(k: TaskKey) -> Time {
    match k {
        TaskKey::Timer { visibility_timestamp, .. } => visibility_timestamp,
        other => panic!("expected timer task key, got {:?}", other),
    }
}

struct PollState {
    backoff_timer: HashMap<i32, JoinHandle<()>>,
    next_poll_time: HashMap<i32, Time>,
}

/// Go `timeTaskReadProgress` (the queue pointer becomes the queue id).
struct TimeTaskReadProgress {
    current_queue_id: u64,
    read_level: TaskKey,
    max_read_level: TaskKey,
    next_page_token: Vec<u8>,
}

/// Go `filteredTimerTasksResponse`.
struct FilteredTimerTasksResponse {
    timer_tasks: Vec<Arc<dyn PersistenceTask>>,
    look_ahead_timestamp: Time,
    next_page_token: Vec<u8>,
}

pub struct TimerQueueProcessorBase {
    pub base: ProcessorBase,
    task_initializer: TaskInitializer,
    cluster_name: String,
    timer_gate: Arc<dyn TimerGate>,
    poll: StdMutex<PollState>,

    // timer notification
    new_timer_tx: mpsc::Sender<()>,
    new_timer_rx: StdMutex<Option<mpsc::Receiver<()>>>,
    new_time: StdMutex<Time>,

    processing_queue_read_progress: StdMutex<HashMap<i32, TimeTaskReadProgress>>,
}

impl TimerQueueProcessorBase {
    /// Go `newTimerQueueProcessorBase`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_name: String,
        shard: Arc<dyn ShardContext>,
        processing_queue_states: Vec<ProcessingQueueState>,
        task_processor: Arc<dyn TaskProcessor>,
        redispatcher: Arc<dyn Redispatcher>,
        timer_gate: Arc<dyn TimerGate>,
        options: Arc<QueueProcessorOptions>,
        update_max_read_level: UpdateMaxReadLevelFn,
        update_cluster_ack_level: UpdateClusterAckLevelFn,
        update_processing_queue_states: Option<UpdateProcessingQueueStatesFn>,
        queue_shutdown: QueueShutdownFn,
        task_initializer: TaskInitializer,
        metrics_scope: Arc<dyn MetricsScope>,
        rate_limiter: Arc<dyn Limiter>,
    ) -> Arc<Self> {
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
            metrics_scope,
            rate_limiter,
        );
        let (new_timer_tx, new_timer_rx) = mpsc::channel(1);
        Arc::new(TimerQueueProcessorBase {
            base,
            task_initializer,
            cluster_name,
            timer_gate,
            poll: StdMutex::new(PollState { backoff_timer: HashMap::new(), next_poll_time: HashMap::new() }),
            new_timer_tx,
            new_timer_rx: StdMutex::new(Some(new_timer_rx)),
            new_time: StdMutex::new(Time::ZERO),
            processing_queue_read_progress: StdMutex::new(HashMap::new()),
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
        tracing::info!("Timer queue processor state changed: starting");

        self.base.redispatcher.start();

        let mut new_poll_time = Time::ZERO;
        let start_jitter = (self.base.options.max_start_jitter_interval)();
        if !start_jitter.is_zero() {
            let now = self.base.shard.now();
            let nanos = rand::thread_rng().gen_range(0..start_jitter.as_nanos() as u64);
            new_poll_time = now.add(Duration::from_nanos(nanos));
        }
        let levels: Vec<i32> = match self.base.processing_queue_collections.try_lock() {
            Ok(colls) => colls.iter().map(|c| c.level()).collect(),
            // nobody else touches the collections before Start; keep the default level pollable
            Err(_) => vec![DEFAULT_PROCESSING_QUEUE_LEVEL],
        };
        for level in levels {
            self.upsert_poll_time(level, new_poll_time);
        }

        let new_timer_rx = self.new_timer_rx.lock().unwrap().take().expect("receiver taken once");
        let t = self.clone();
        self.base.shutdown_wg.spawn(async move { t.processor_pump(new_timer_rx).await });
        tracing::info!("Timer queue processor state changed: started");
    }

    /// Go `Stop`.
    /// Edge Case: Stop doesn't stop TimerGate if the processor is only initialized without starting.
    pub async fn stop(&self) {
        if self
            .base
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        tracing::info!("Timer queue processor state changed: stopping");

        self.timer_gate.stop();
        self.base.shutdown_ch.cancel();
        {
            let p = self.poll.lock().unwrap();
            for h in p.backoff_timer.values() {
                h.abort();
            }
        }

        self.base.shutdown_wg.close();
        if tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, self.base.shutdown_wg.wait()).await.is_err() {
            tracing::warn!("timerQueueProcessorBase timed out on shut down");
        }

        self.base.redispatcher.stop();
        tracing::info!("Timer queue processor state changed: stopped");
    }

    async fn processor_pump(self: Arc<Self>, mut new_timer_rx: mpsc::Receiver<()>) {
        let opts = self.base.options.clone();
        let ack_d = || jit_duration((opts.update_ack_interval)(), (opts.update_ack_interval_jitter_coefficient)());
        let split_d = || jit_duration((opts.split_queue_interval)(), (opts.split_queue_interval_jitter_coefficient)());

        let update_ack_timer = tokio::time::sleep(ack_d());
        tokio::pin!(update_ack_timer);
        let split_queue_timer = tokio::time::sleep(split_d());
        tokio::pin!(split_queue_timer);

        let mut action_rx = self.base.action_notify_rx.lock().await;

        loop {
            tokio::select! {
                _ = self.base.shutdown_ch.cancelled() => return,
                _ = self.timer_gate.recv() => {
                    self.update_timer_gates().await;
                }
                _ = &mut update_ack_timer => {
                    // Go handleAckLevelUpdate
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
                _ = new_timer_rx.recv() => {
                    self.handle_new_timer().await;
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

    /// Go `processQueueCollections`.
    async fn process_queue_collections(self: &Arc<Self>, levels: HashSet<i32>) {
        let mut colls = self.base.processing_queue_collections.lock().await;
        for i in 0..colls.len() {
            let level = colls[i].level();
            if !levels.contains(&level) {
                continue;
            }

            if colls[i].active_queue().is_none() {
                // process for this queue collection has finished
                // it's possible that new queue will be added to this collection later though,
                // pollTime will be updated after split/merge
                tracing::debug!(queue_level = level, "Active queue is nil for timer queue at this level");
                continue;
            }

            self.upsert_poll_time(
                level,
                self.base.shard.get_current_time(&self.cluster_name).add(jit_duration(
                    (self.base.options.max_poll_interval)(),
                    (self.base.options.max_poll_interval_jitter_coefficient)(),
                )),
            );

            let (active_id, mut read_level, mut max_read_level, domain_filter) = {
                let q = colls[i].active_queue().expect("checked above");
                (
                    q.id,
                    q.state().read_level(),
                    min_task_key(q.state().max_level(), (self.base.update_max_read_level)()),
                    q.state().domain_filter().clone(),
                )
            };
            let mut next_page_token: Vec<u8> = Vec::new();

            if let Some(progress) = self.processing_queue_read_progress.lock().unwrap().remove(&level) {
                if progress.current_queue_id == active_id {
                    read_level = progress.read_level;
                    max_read_level = progress.max_read_level;
                    next_page_token = progress.next_page_token;
                }
            }

            if !read_level.less(&max_read_level) {
                // notify timer gate about the min time
                self.upsert_poll_time(level, timer_visibility_timestamp(read_level));
                tracing::debug!(
                    queue_level = level,
                    "Skipping processing timer queue at this level because readLevel >= maxReadLevel"
                );
                continue;
            }

            if self.base.rate_limiter.wait(LOAD_QUEUE_TASK_THROTTLE_RETRY_DELAY).await.is_err() {
                if level == DEFAULT_PROCESSING_QUEUE_LEVEL {
                    self.upsert_poll_time(level, Time::ZERO);
                } else {
                    self.setup_backoff_timer(level);
                }
                continue;
            }

            let resp = match self.read_and_filter_tasks(read_level, max_read_level, next_page_token).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(error = %e, "Processor unable to retrieve tasks");
                    self.upsert_poll_time(level, Time::ZERO); // re-enqueue the event
                    continue;
                }
            };

            let mut tasks: HashMap<TaskKey, TaskRef> = HashMap::new();
            let mut task_ch_full = false;
            let mut submitted_count = 0usize;
            let now = self.base.shard.now();
            for info in &resp.timer_tasks {
                if !domain_filter.filter(&info.get_domain_id()) {
                    continue;
                }

                if info.is_corrupted() {
                    tracing::error!(task = ?info, "Processing queue encountered a corrupted task");
                    self.base.metrics_scope.inc_counter("CorruptedHistoryTaskCounter");
                    continue;
                }

                let task = (self.task_initializer)(info.clone());
                tasks.insert(new_timer_task_key(info.get_visibility_timestamp(), info.get_task_id()), task.clone());
                // shard level metrics for the duration between a task being written to a queue and being fetched from it
                let lat = now.sub_nanos(info.get_visibility_timestamp()).max(0) as u64;
                self.base.metrics_scope.record_timer("TaskEnqueueToFetchLatency", Duration::from_nanos(lat));
                match self.base.submit_task(task).await {
                    // only err here is due to the fact that processor has been shutdown
                    // return instead of continue
                    Err(_) => return,
                    Ok(submitted) => {
                        task_ch_full = task_ch_full || !submitted;
                        if submitted {
                            submitted_count += 1;
                        }
                    }
                }
            }
            tracing::debug!(
                "Submitted {} timer tasks successfully out of {} tasks",
                submitted_count,
                resp.timer_tasks.len()
            );

            let mut new_read_level: Option<TaskKey> = None;
            if resp.next_page_token.is_empty() {
                new_read_level = Some(max_read_level);
                if resp.look_ahead_timestamp != Time::ZERO {
                    // lookAheadTask may exist only when nextPageToken is empty
                    // notice that lookAheadTask.VisibilityTimestamp may be larger than shard max read level,
                    // which means new tasks can be generated before that timestamp. This issue is solved by
                    // upsertPollTime whenever there are new tasks
                    let look_ahead_timestamp = resp.look_ahead_timestamp;
                    self.upsert_poll_time(level, look_ahead_timestamp);
                    new_read_level = Some(min_task_key(max_read_level, new_timer_task_key(look_ahead_timestamp, 0)));
                }
                // else we have no idea when the next poll should happen
                // rely on notifyNewTask to trigger the next poll even for non-default queue.
            } else {
                // more tasks should be loaded for this processing queue
                // record the current progress and update the poll time
                if level == DEFAULT_PROCESSING_QUEUE_LEVEL || !task_ch_full {
                    self.upsert_poll_time(level, Time::ZERO);
                } else {
                    self.setup_backoff_timer(level);
                }
                self.processing_queue_read_progress.lock().unwrap().insert(
                    level,
                    TimeTaskReadProgress {
                        current_queue_id: active_id,
                        read_level,
                        max_read_level,
                        next_page_token: resp.next_page_token.clone(),
                    },
                );
                if let Some(last) = resp.timer_tasks.last() {
                    new_read_level = Some(new_timer_task_key(last.get_visibility_timestamp(), 0));
                }
            }
            // Go passes a nil newReadLevel through (and would dereference it); keep the current read level instead.
            let new_read_level = new_read_level.unwrap_or_else(|| {
                colls[i].active_queue().map(|q| q.state().read_level()).unwrap_or(read_level)
            });
            colls[i].add_tasks(tasks, new_read_level);
        }
    }

    /// Go `splitQueue`: splits the processing queue collection based on some policy.
    async fn split_queue(&self) {
        let opts = self.base.options.clone();
        let look_ahead: LookAheadFunc = Arc::new(move |key: TaskKey, domain_id: &str| {
            new_timer_task_key(
                timer_visibility_timestamp(key).add((opts.split_look_ahead_duration_by_domain_id)(domain_id)),
                0,
            )
        });
        let split_policy = self.base.initialize_split_policy(Some(look_ahead));

        let mut colls = self.base.processing_queue_collections.lock().await;
        self.base.split_processing_queue_collection(&mut colls, split_policy.as_deref(), |level, t| {
            self.upsert_poll_time(level, t);
        });
    }

    /// Go `updateTimerGates`.
    async fn update_timer_gates(self: &Arc<Self>) {
        let opts = self.base.options.clone();
        let max_redispatch_queue_size = (opts.max_redispatch_queue_size)() as usize;
        if self.base.redispatcher.size() > max_redispatch_queue_size {
            self.base.redispatcher.redispatch(max_redispatch_queue_size).await;
            if self.base.redispatcher.size() > max_redispatch_queue_size {
                // if redispatcher still has a large number of tasks
                // this only happens when system is under very high load
                // we should backoff here instead of keeping submitting tasks to task processor
                // don't call timerGate.Update(time.Now() + loadQueueTaskThrottleRetryDelay) as the time in
                // standby timer processor is not real time and is managed separately
                tokio::time::sleep(jit_duration(
                    (opts.poll_backoff_interval)(),
                    (opts.poll_backoff_interval_jitter_coefficient)(),
                ))
                .await;
            }
            self.timer_gate.update(Time::ZERO);
            return;
        }

        let mut levels: HashSet<i32> = HashSet::new();
        {
            let mut p = self.poll.lock().unwrap();
            let now = self.base.shard.get_current_time(&self.cluster_name);
            let entries: Vec<(i32, Time)> = p.next_poll_time.iter().map(|(l, t)| (*l, *t)).collect();
            for (level, poll_time) in entries {
                if !now.before(poll_time) {
                    levels.insert(level);
                    p.next_poll_time.remove(&level);
                } else {
                    self.timer_gate.update(poll_time);
                }
            }
        }

        self.process_queue_collections(levels).await;
    }

    /// Go `handleNewTimer`.
    async fn handle_new_timer(&self) {
        let new_time = {
            let mut g = self.new_time.lock().unwrap();
            let t = *g;
            *g = Time::ZERO;
            t
        };

        // New Timer has arrived.
        self.base.metrics_scope.inc_counter("NewTimerNotifyCounter");
        // notify all queue collections as they are waiting for the notification when there's
        // no more task to process. For non-default queue, we choose to do periodic polling
        // in the future, then we don't need to notify them.
        let levels: Vec<i32> = {
            let colls = self.base.processing_queue_collections.lock().await;
            colls.iter().map(|c| c.level()).collect()
        };
        for level in levels {
            self.upsert_poll_time(level, new_time);
        }
    }

    async fn handle_action_notification(&self, notification: ActionNotification) {
        let is_reset = matches!(notification.action.action_type, ActionType::Reset);
        let mut colls = self.base.processing_queue_collections.lock().await;
        self.base.handle_action_notification(&mut colls, notification, || {
            if is_reset {
                self.upsert_poll_time(DEFAULT_PROCESSING_QUEUE_LEVEL, Time::ZERO);
            }
        });
    }

    /// Go `readAndFilterTasks`.
    async fn read_and_filter_tasks(
        &self,
        read_level: TaskKey,
        max_read_level: TaskKey,
        next_page_token: Vec<u8>,
    ) -> Result<FilteredTimerTasksResponse, QueueError> {
        let batch = (self.base.options.batch_size)();
        let mut resp_tasks = self.get_timer_tasks(read_level, max_read_level, next_page_token, batch).await?;

        let mut look_ahead_timestamp = Time::ZERO;
        let mut filtered_tasks: Vec<Arc<dyn PersistenceTask>> = Vec::new();
        for timer_task in std::mem::take(&mut resp_tasks.0) {
            if !self.is_process_now(timer_task.get_visibility_timestamp()) {
                // found the first task that is not ready to be processed yet.
                // reset NextPageToken so we can load more tasks starting from this lookAheadTask next time.
                look_ahead_timestamp = timer_task.get_visibility_timestamp();
                resp_tasks.1 = Vec::new();
                break;
            }
            filtered_tasks.push(timer_task);
        }

        if resp_tasks.1.is_empty() && look_ahead_timestamp == Time::ZERO {
            // only look ahead within the processing queue boundary
            match self.read_look_ahead_task(max_read_level, MAXIMUM_TIMER_TASK_KEY).await {
                Err(_) => {
                    // we don't know if look ahead task exists or not, but we know if it exists,
                    // it's visibility timestamp is larger than or equal to maxReadLevel.
                    // so, create a fake look ahead task so another load can be triggered at that time.
                    look_ahead_timestamp = timer_visibility_timestamp(max_read_level);
                }
                Ok(Some(t)) => look_ahead_timestamp = t.get_visibility_timestamp(),
                Ok(None) => {}
            }
        }

        tracing::debug!(
            "readAndFilterTasks returning {} tasks and lookAheadTimestamp: {:?} for readLevel: {:?}, maxReadLevel: {:?}",
            filtered_tasks.len(),
            look_ahead_timestamp,
            read_level,
            max_read_level
        );
        Ok(FilteredTimerTasksResponse {
            timer_tasks: filtered_tasks,
            look_ahead_timestamp,
            next_page_token: resp_tasks.1,
        })
    }

    /// Go `readLookAheadTask`.
    async fn read_look_ahead_task(
        &self,
        look_ahead_start_level: TaskKey,
        look_ahead_max_level: TaskKey,
    ) -> Result<Option<Arc<dyn PersistenceTask>>, QueueError> {
        let (mut tasks, _) = self.get_timer_tasks(look_ahead_start_level, look_ahead_max_level, Vec::new(), 1).await?;
        if tasks.len() == 1 {
            return Ok(tasks.pop());
        }
        Ok(None)
    }

    /// Go `getTimerTasks`: (tasks, nextPageToken).
    async fn get_timer_tasks(
        &self,
        read_level: TaskKey,
        max_read_level: TaskKey,
        next_page_token: Vec<u8>,
        batch_size: i64,
    ) -> Result<(Vec<Arc<dyn PersistenceTask>>, Vec<u8>), QueueError> {
        let shard = self.base.shard.clone();
        let min_ts = timer_visibility_timestamp(read_level);
        let max_ts = timer_visibility_timestamp(max_read_level);
        let resp = retry_persistence_operation(|| {
            let shard = shard.clone();
            let next_page_token = next_page_token.clone();
            async move {
                shard
                    .get_history_tasks(GetHistoryTasksRequest {
                        task_category: HistoryTaskCategory::Timer,
                        inclusive_min_task_key: HistoryTaskKey::new(min_ts, 0),
                        exclusive_max_task_key: HistoryTaskKey::new(max_ts, 0),
                        page_size: batch_size,
                        next_page_token,
                        shard_id: shard.get_shard_id(),
                    })
                    .await
            }
        })
        .await?;
        Ok((resp.tasks, resp.next_page_token))
    }

    /// Go `isProcessNow`.
    fn is_process_now(&self, expiry_time: Time) -> bool {
        if expiry_time == Time::ZERO {
            // return true, but somewhere probably have bug creating empty timerTask.
            tracing::warn!("Timer task has timestamp zero");
        }
        !self.base.shard.get_current_time(&self.cluster_name).before(expiry_time)
    }

    /// Go `notifyNewTimers`.
    pub fn notify_new_timers(&self, timer_tasks: &[Arc<dyn PersistenceTask>]) {
        if timer_tasks.is_empty() {
            return;
        }

        let mut min_new_time = timer_tasks[0].get_visibility_timestamp();
        for timer_task in timer_tasks {
            let ts = timer_task.get_visibility_timestamp();
            if ts.before(min_new_time) {
                min_new_time = ts;
            }
            self.base.metrics_scope.inc_counter("NewTimerNotifyCounter");
        }

        self.notify_new_timer(min_new_time);
    }

    /// Go `notifyNewTimer`.
    pub fn notify_new_timer(&self, new_time: Time) {
        let mut g = self.new_time.lock().unwrap();
        if *g == Time::ZERO || new_time.before(*g) {
            *g = new_time;
            // Channel "full" -> drop and move on, this will happen only if service is in high load.
            let _ = self.new_timer_tx.try_send(());
        }
    }

    /// Go `upsertPollTime`.
    pub fn upsert_poll_time(&self, level: i32, new_poll_time: Time) {
        let mut p = self.poll.lock().unwrap();
        if p.backoff_timer.contains_key(&level) {
            // honor existing backoff timer
            return;
        }

        match p.next_poll_time.get(&level) {
            Some(current) if !new_poll_time.before(*current) => {}
            _ => {
                p.next_poll_time.insert(level, new_poll_time);
                self.timer_gate.update(new_poll_time);
            }
        }
    }

    /// Go `setupBackoffTimer`: triggers a poll for the queue collection after a (real) time delay.
    pub fn setup_backoff_timer(self: &Arc<Self>, level: i32) {
        let mut p = self.poll.lock().unwrap();
        if p.backoff_timer.contains_key(&level) {
            // honor existing backoff timer
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
            let mut p = t.poll.lock().unwrap();
            p.next_poll_time.insert(level, Time::ZERO);
            t.timer_gate.update(Time::ZERO);
            p.backoff_timer.remove(&level);
        });
        p.backoff_timer.insert(level, h);
    }
}

#[async_trait]
impl Daemon for TimerQueueProcessorBase {
    fn start(self: Arc<Self>) {
        TimerQueueProcessorBase::start(&self)
    }
    async fn stop(&self) {
        TimerQueueProcessorBase::stop(self).await
    }
}
