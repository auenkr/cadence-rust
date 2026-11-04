//! Port of queuev2/queue_reader_cached_scheduled.go

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::deps::*;
use super::predicate::Predicate;
use super::queue_mem::InMemQueue;
use super::queue_reader::{
    GetTaskProgress, GetTaskRequest, GetTaskResponse, LookAHeadRequest, LookAHeadResponse,
    QueueReader,
};
use super::queue_reader_cached_base::*;
use super::range::Range;
use super::virtual_queue::{DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED};

#[derive(Clone)]
pub struct ScheduledCacheQueueReaderOptions {
    pub max_look_ahead_window: DurationFn,
    pub prefetch_trigger_window: DurationFn,
    pub prefetch_page_size: IntFn,
    pub time_eviction_window: DurationFn,
    pub min_prefetch_interval: DurationFn,
    pub prefetch_jitter_coefficient: FloatFn,
}

pub struct CachedScheduledQueueReader {
    b: CachedQueueReaderBase,
    scheduled_opts: ScheduledCacheQueueReaderOptions,
    ctx: CancelToken,
    prefetch_rx: StdMutex<Option<mpsc::Receiver<()>>>,
    handle: StdMutex<Option<JoinHandle<()>>>,
    weak: Weak<CachedScheduledQueueReader>,
}

fn to_chrono(d: Duration) -> chrono::Duration {
    chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::MAX)
}

pub fn new_cached_scheduled_queue_reader(
    base: Arc<dyn QueueReader>,
    queue: Box<dyn InMemQueue>,
    shard: Arc<dyn ShardContext>,
    metrics_scope: Arc<dyn MetricsScope>,
) -> Arc<CachedScheduledQueueReader> {
    let config = shard.get_config();
    let clock = shard.get_time_source();
    let logger = shard
        .get_logger()
        .with_tags(&[("component", "cached-scheduled-queue-reader".to_string())]);
    new_cached_scheduled_queue_reader_with_options(
        base,
        queue,
        shard,
        clock,
        logger,
        metrics_scope,
        CachedQueueReaderOptions {
            mode: config.timer_processor_cached_queue_reader_mode.clone(),
            max_size: config.timer_processor_cache_max_size.clone(),
            shadow_sample_interval: config
                .timer_processor_cached_queue_reader_shadow_sample_interval
                .clone(),
        },
        ScheduledCacheQueueReaderOptions {
            max_look_ahead_window: config.timer_processor_max_poll_interval.clone(),
            prefetch_trigger_window: config.timer_processor_cache_prefetch_trigger_window.clone(),
            prefetch_page_size: config.timer_task_batch_size.clone(),
            time_eviction_window: config.timer_processor_cache_time_eviction_window.clone(),
            min_prefetch_interval: config.timer_processor_cache_min_prefetch_interval.clone(),
            prefetch_jitter_coefficient: config
                .timer_processor_max_poll_interval_jitter_coefficient
                .clone(),
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub fn new_cached_scheduled_queue_reader_with_options(
    base: Arc<dyn QueueReader>,
    queue: Box<dyn InMemQueue>,
    shard: Arc<dyn ShardContext>,
    clock_source: Arc<dyn TimeSource>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    options: CachedQueueReaderOptions,
    scheduled_opts: ScheduledCacheQueueReaderOptions,
) -> Arc<CachedScheduledQueueReader> {
    let (tx, rx) = mpsc::channel(1);
    let b = new_cached_queue_reader_base(
        base,
        queue,
        shard,
        clock_source,
        logger,
        metrics_scope,
        options,
    );
    // Go: onUpperBoundUpdatedLockedFn = q.notifyPrefetch
    *b.on_upper_bound_updated.lock().unwrap() = Some(tx);
    Arc::new_cyclic(|weak| CachedScheduledQueueReader {
        b,
        scheduled_opts,
        ctx: CancelToken::new(),
        prefetch_rx: StdMutex::new(Some(rx)),
        handle: StdMutex::new(None),
        weak: weak.clone(),
    })
}

impl CachedScheduledQueueReader {
    async fn prefetch_loop(self: Arc<Self>, mut prefetch_rx: mpsc::Receiver<()>) {
        let timer = self.b.clock.new_timer(Duration::from_millis(1));

        self.b
            .logger
            .info("Cached Queue Reader state changed LifeCycleStarted");

        loop {
            tokio::select! {
                _ = self.ctx.cancelled() => {
                    timer.stop();
                    return;
                }
                Some(()) = prefetch_rx.recv() => {
                    timer.reset(self.next_prefetch_delay());
                }
                _ = timer.fired() => {
                    self.try_time_evict_if_cache_full();
                    if let Err(e) = self.prefetch().await {
                        self.b.logger.warn(&format!("prefetch failed, retrying shortly error={e}"));
                        timer.reset((self.scheduled_opts.min_prefetch_interval)());
                    } else {
                        timer.reset(self.next_prefetch_delay());
                    }
                }
            }
        }
    }

    fn next_prefetch_delay(&self) -> Duration {
        let upper = self.b.lock().exclusive_upper_bound;
        let o = &self.scheduled_opts;
        let min_interval = (o.min_prefetch_interval)();

        let trigger_time = upper.get_scheduled_time() - to_chrono((o.prefetch_trigger_window)());
        let until = (trigger_time - self.b.clock.now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        let delay = min_interval.max(until);
        let jittered = jit_duration(delay, (o.prefetch_jitter_coefficient)());

        min_interval.max(delay.min(jittered))
    }

    async fn prefetch(&self) -> Result<(), DepError> {
        if self.b.is_disabled() {
            self.b.clear_if_not_empty();
            self.b.logger.debug("prefetch skipped, cache disabled");
            return Ok(());
        }

        let (available_cache_size, upper_bound) = {
            let g = self.b.lock();
            (
                (self.b.options.max_size)() - g.queue.len() as i64,
                g.exclusive_upper_bound,
            )
        };

        if available_cache_size <= 0 {
            self.b.logger.debug("prefetch skipped, cache full");
            return Ok(());
        }

        let now = self.b.clock.now();

        let sw = self.b.metrics.start_timer_with_exponential_histogram(
            metric::CACHED_QUEUE_PREFETCH_LATENCY,
            metric::CACHED_QUEUE_PREFETCH_LATENCY_HISTOGRAM,
        );

        let o = &self.scheduled_opts;
        let exclusive_max_task_key =
            HistoryTaskKey::new(now + to_chrono((o.max_look_ahead_window)()), 0);

        let mut inclusive_min_task_key = upper_bound;
        if inclusive_min_task_key.equal(&HistoryTaskKey::minimum()) {
            inclusive_min_task_key =
                HistoryTaskKey::new(now - to_chrono((o.time_eviction_window)()), 0);
        }

        let page_size = available_cache_size.min((o.prefetch_page_size)());

        self.b.lock().prefetch_target_upper = exclusive_max_task_key;

        let resp = self
            .b
            .base
            .get_task(&GetTaskRequest {
                progress: GetTaskProgress {
                    range: Range {
                        inclusive_min_task_key,
                        exclusive_max_task_key,
                    },
                    next_page_token: None,
                    next_task_key: inclusive_min_task_key,
                },
                predicate: Predicate::new_universal(),
                page_size: page_size.max(0) as usize,
            })
            .await;

        let result = {
            let mut g = self.b.lock();
            let r = self.finish_prefetch(&mut g, resp, upper_bound, inclusive_min_task_key);
            // Go: deferred insertBufferedTasks runs before the deferred Unlock.
            self.insert_buffered_tasks(&mut g);
            r
        };
        sw.stop();
        result
    }

    fn finish_prefetch(
        &self,
        g: &mut CachedInner,
        resp: Result<GetTaskResponse, DepError>,
        upper_bound: HistoryTaskKey,
        inclusive_min_task_key: HistoryTaskKey,
    ) -> Result<(), DepError> {
        g.prefetch_target_upper = HistoryTaskKey::minimum();

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                self.b
                    .metrics
                    .inc_counter(metric::CACHED_QUEUE_PREFETCH_FAILURE_COUNTER);
                self.b.logger.error(&format!("prefetch failed error={e}"));
                return Err(format!("prefetch failed: {e}"));
            }
        };

        if self.b.is_disabled() {
            self.b
                .logger
                .info("prefetch result discarded, mode switched to disabled during prefetch");
            g.pending_inject_buffer.clear();
            return Ok(());
        }

        if !g.exclusive_upper_bound.equal(&upper_bound) {
            self.b
                .metrics
                .inc_counter(metric::CACHED_QUEUE_PREFETCH_GAP_DETECTED_COUNTER);
            self.b.logger.info(&format!(
                "gap detected, discarding fetched data prevUpper={:?} cacheState={:?}",
                upper_bound,
                CachedQueueReaderBase::get_state(g)
            ));
            return Err("gap detected: upper bound changed during fetch".to_string());
        }

        if g.inclusive_lower_bound.equal(&HistoryTaskKey::minimum()) {
            self.b
                .update_inclusive_lower_bound(g, inclusive_min_task_key);
        }

        if !self.put_tasks(g, &resp.tasks) {
            self.b
                .update_exclusive_upper_bound(g, resp.progress.next_task_key);
        }

        self.b
            .metrics
            .inc_counter(metric::CACHED_QUEUE_PREFETCH_SUCCESS_COUNTER);

        self.b.logger.debug(&format!(
            "prefetch complete tasksFetched={} cacheState={:?}",
            resp.tasks.len(),
            CachedQueueReaderBase::get_state(g)
        ));
        Ok(())
    }

    fn is_to_buffer_task(g: &CachedInner, key: HistoryTaskKey) -> bool {
        if g.prefetch_target_upper.equal(&HistoryTaskKey::minimum()) {
            return false;
        }
        key.greater_or_equal(&g.exclusive_upper_bound) && key.less(&g.prefetch_target_upper)
    }

    /// Go `putTasks`; returns whether the queue was trimmed.
    fn put_tasks(&self, g: &mut CachedInner, tasks: &[PTask]) -> bool {
        if tasks.is_empty() {
            return false;
        }

        self.try_time_evict(g, tasks.len() as i64);
        g.queue.put_tasks(tasks);
        let (new_upper, trimmed) = g.queue.r_trim_by_size((self.b.options.max_size)());

        if !trimmed {
            return false;
        }

        if new_upper.equal(&HistoryTaskKey::minimum()) {
            self.b
                .update_inclusive_lower_bound(g, HistoryTaskKey::minimum());
        }
        self.b.update_exclusive_upper_bound(g, new_upper);

        true
    }

    fn insert_buffered_tasks(&self, g: &mut CachedInner) {
        if g.pending_inject_buffer.is_empty() {
            return;
        }
        let buffered = std::mem::take(&mut g.pending_inject_buffer);
        let covered: Vec<PTask> = buffered
            .into_iter()
            .filter(|t| CachedQueueReaderBase::is_task_covered(g, t.get_task_key()))
            .collect();
        self.put_tasks(g, &covered);
    }

    fn try_time_evict(&self, g: &mut CachedInner, extra_tasks: i64) {
        if (g.queue.len() as i64) + extra_tasks < (self.b.options.max_size)() {
            return;
        }
        let evict_before = HistoryTaskKey::new(
            self.b.clock.now() - to_chrono((self.scheduled_opts.time_eviction_window)()),
            0,
        );
        self.b.advance_inclusive_lower_bound(g, evict_before);
    }

    fn try_time_evict_if_cache_full(&self) {
        let mut g = self.b.lock();
        self.try_time_evict(&mut g, 1);
    }
}

#[async_trait]
impl QueueReader for CachedScheduledQueueReader {
    async fn get_task(&self, req: &GetTaskRequest) -> Result<GetTaskResponse, DepError> {
        self.b.get_task(req).await
    }

    async fn look_ahead(&self, req: &LookAHeadRequest) -> Result<LookAHeadResponse, DepError> {
        if self.b.is_disabled() || self.b.is_shadow() {
            return self.b.base.look_ahead(req).await;
        }

        if self.b.fallback_if_range_id_changed() {
            self.b
                .logger
                .info("LookAHead falling back to base reader after rangeID change");
            return self.b.base.look_ahead(req).await;
        }

        {
            let g = self.b.lock();
            if CachedQueueReaderBase::is_task_covered(&g, req.inclusive_min_task_key) {
                let cache_task = g.queue.look_ahead(req.inclusive_min_task_key);
                let look_ahead_max_time = g.exclusive_upper_bound.get_scheduled_time();
                return Ok(LookAHeadResponse {
                    task: cache_task,
                    look_ahead_max_time,
                });
            }
            self.b.logger.debug(&format!(
                "look-ahead cache miss cacheState={:?}",
                CachedQueueReaderBase::get_state(&g)
            ));
        }
        self.b.base.look_ahead(req).await
    }
}

impl CachedQueueReader for CachedScheduledQueueReader {
    fn start(&self) {
        if self
            .b
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
        self.b
            .logger
            .info("Cached Queue Reader state changed LifeCycleStarting");
        let rx = self.prefetch_rx.lock().unwrap().take().expect("receiver taken");
        let me = self.weak.upgrade().expect("reader dropped");
        *self.handle.lock().unwrap() = Some(tokio::spawn(me.prefetch_loop(rx)));
    }

    fn stop(&self) {
        if self
            .b
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
        self.b
            .logger
            .info("Cached Queue Reader state changed LifeCycleStopping");
        self.ctx.cancel();
        // Go waits on wg; Stop is sync here, so the loop is left to observe cancellation.
        let _ = self.handle.lock().unwrap().take();
        self.b
            .logger
            .info("Cached Queue Reader state changed LifeCycleStopped");
    }

    fn clear(&self) {
        self.b.clear();
    }

    fn update_read_level(&self, read_level: HistoryTaskKey) {
        self.b.update_read_level(read_level);
    }

    fn inject(&self, tasks: &[PTask]) {
        if self.b.is_disabled() {
            self.b.clear_if_not_empty();
            return;
        }

        let mut g = self.b.lock();
        let now = self.b.clock.now();
        let (mut injected, mut buffered, mut dropped_below, mut dropped_upper) = (0i64, 0i64, 0i64, 0i64);

        let mut covered: Vec<PTask> = Vec::new();
        for t in tasks {
            if t.get_task_id() == 0 {
                continue;
            }
            let key = t.get_task_key();
            if CachedQueueReaderBase::is_task_covered(&g, key) {
                injected += 1;
                if self.b.logger.debug_on() {
                    self.b.logger.debug(&format!(
                        "injecting task taskKey={:?} cacheState={:?}",
                        key,
                        CachedQueueReaderBase::get_state(&g)
                    ));
                }
                covered.push(t.clone());
                continue;
            }
            if Self::is_to_buffer_task(&g, key) {
                buffered += 1;
                if self.b.logger.debug_on() {
                    self.b.logger.debug(&format!(
                        "buffering task taskKey={:?} cacheState={:?} prefetchTargetUpper={:?}",
                        key,
                        CachedQueueReaderBase::get_state(&g),
                        g.prefetch_target_upper
                    ));
                }
                g.pending_inject_buffer.push(t.clone());
                continue;
            }

            if key.less(&g.inclusive_lower_bound) {
                dropped_below += 1;
                self.b.logger.warn(&format!(
                    "task key is below the lower bound, dropping task taskKey={:?} cacheState={:?}",
                    key,
                    CachedQueueReaderBase::get_state(&g)
                ));
                continue;
            }

            dropped_upper += 1;
            self.b.metrics.exponential_histogram(
                metric::CACHED_QUEUE_DROPPED_FUTURE_TIMER_TASKS_DURATION_HISTOGRAM,
                key.get_scheduled_time() - now,
            );
            if self.b.logger.debug_on() {
                self.b.logger.debug(&format!(
                    "task key is beyond the upper/target prefetch bound, dropping task taskKey={:?} cacheState={:?}",
                    key,
                    CachedQueueReaderBase::get_state(&g)
                ));
            }
        }

        self.b.emit_inject_status_count(INJECT_STATUS_INJECTED, injected);
        self.b.emit_inject_status_count(INJECT_STATUS_BUFFERED, buffered);
        self.b
            .emit_inject_status_count(INJECT_STATUS_DROPPED_BELOW, dropped_below);
        self.b
            .emit_inject_status_count(INJECT_STATUS_DROPPED_UPPER, dropped_upper);

        self.put_tasks(&mut g, &covered);
    }
}
