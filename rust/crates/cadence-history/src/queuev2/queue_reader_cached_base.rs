//! Port of queuev2/queue_reader_cached_base.go

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicI32, AtomicI64};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};

use tokio::sync::mpsc;

use super::deps::*;
use super::queue_mem::InMemQueue;
use super::queue_reader::{
    GetTaskProgress, GetTaskRequest, GetTaskResponse, LookAHeadRequest, LookAHeadResponse,
    QueueReader,
};
use super::range::Range;

/// Go `CachedQueueReader`.
pub trait CachedQueueReader: QueueReader {
    fn inject(&self, tasks: &[PTask]);
    fn clear(&self);
    fn update_read_level(&self, read_level: HistoryTaskKey);
    fn start(&self);
    fn stop(&self);
}

/// Go `cachedQueueReaderOptions`.
#[derive(Clone)]
pub struct CachedQueueReaderOptions {
    /// Go `Mode` (`StringPropertyFnWithShardIDFilter`).
    pub mode: StringShardFn,
    pub max_size: IntFn,
    pub shadow_sample_interval: DurationFn,
}

/// State guarded by Go's `mu` (plus the scheduled reader's prefetch fields, which
/// are guarded by the same mutex in Go).
pub struct CachedInner {
    pub queue: Box<dyn InMemQueue>,
    pub inclusive_lower_bound: HistoryTaskKey,
    pub exclusive_upper_bound: HistoryTaskKey,
    pub last_range_id: i64,
    /// scheduled reader only
    pub prefetch_target_upper: HistoryTaskKey,
    /// scheduled reader only
    pub pending_inject_buffer: Vec<PTask>,
}

pub struct CachedQueueReaderBase {
    pub base: Arc<dyn QueueReader>,
    pub shard: Arc<dyn ShardContext>,
    pub options: CachedQueueReaderOptions,
    pub clock: Arc<dyn TimeSource>,
    pub logger: Arc<dyn Logger>,
    pub metrics: Arc<dyn MetricsScope>,

    pub mu: StdMutex<CachedInner>,
    pub last_shadow_sample_unix_nano: AtomicI64,
    /// Go `onUpperBoundUpdatedLockedFn` (scheduled reader sets it to `notifyPrefetch`).
    pub on_upper_bound_updated: StdMutex<Option<mpsc::Sender<()>>>,
    pub status: AtomicI32,
}

pub const INJECT_STATUS_INJECTED: &str = "injected";
pub const INJECT_STATUS_BUFFERED: &str = "buffered";
pub const INJECT_STATUS_DROPPED_BELOW: &str = "dropped_below";
pub const INJECT_STATUS_DROPPED_UPPER: &str = "dropped_upper";

pub const SHADOW_MISMATCH_LOG_LIMIT: usize = 100;

pub fn new_cached_queue_reader_base(
    base: Arc<dyn QueueReader>,
    queue: Box<dyn InMemQueue>,
    shard: Arc<dyn ShardContext>,
    clock_source: Arc<dyn TimeSource>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    options: CachedQueueReaderOptions,
) -> CachedQueueReaderBase {
    let last_range_id = shard.get_range_id();
    CachedQueueReaderBase {
        base,
        shard,
        options,
        clock: clock_source,
        logger,
        metrics: metrics_scope,
        mu: StdMutex::new(CachedInner {
            queue,
            inclusive_lower_bound: HistoryTaskKey::minimum(),
            exclusive_upper_bound: HistoryTaskKey::minimum(),
            last_range_id,
            prefetch_target_upper: HistoryTaskKey::minimum(),
            pending_inject_buffer: Vec::new(),
        }),
        last_shadow_sample_unix_nano: AtomicI64::new(0),
        on_upper_bound_updated: StdMutex::new(None),
        status: AtomicI32::new(super::virtual_queue::DAEMON_STATUS_INITIALIZED),
    }
}

pub fn is_cached_queue_reader_disabled(mode: &str) -> bool {
    !matches!(mode, "enabled" | "shadow")
}

/// Go `resolveInclusiveMinTaskKey`.
pub fn resolve_inclusive_min_task_key(req: &GetTaskRequest) -> Option<HistoryTaskKey> {
    let mut inclusive_min = req.progress.range.inclusive_min_task_key;
    if req.progress.next_page_token.is_some() {
        if req.progress.next_task_key.equal(&HistoryTaskKey::minimum()) {
            return None;
        }
        inclusive_min = req.progress.next_task_key;
    }
    Some(inclusive_min)
}

#[derive(Clone, Debug, Default)]
pub struct ShadowMismatchTaskInfo {
    pub task_key: Option<HistoryTaskKey>,
    pub run_id: String,
}

fn to_shadow_mismatch_task_info(t: &PTask) -> ShadowMismatchTaskInfo {
    ShadowMismatchTaskInfo {
        task_key: Some(t.get_task_key()),
        run_id: t.get_run_id(),
    }
}

fn cap_shadow_mismatch_slice<T: Clone>(s: &[T]) -> Vec<T> {
    s.iter().take(SHADOW_MISMATCH_LOG_LIMIT).cloned().collect()
}

#[derive(Clone, Debug, Default)]
pub struct TaskMismatches {
    pub range_ids: Vec<i64>,
    pub extra: Vec<ShadowMismatchTaskInfo>,
    pub missed: Vec<ShadowMismatchTaskInfo>,
    pub task_id_boundary_noise: Vec<ShadowMismatchTaskInfo>,
}

impl TaskMismatches {
    pub fn is_empty(&self) -> bool {
        self.extra.is_empty() && self.missed.is_empty()
    }

    pub fn cap(&self) -> TaskMismatches {
        TaskMismatches {
            range_ids: cap_shadow_mismatch_slice(&self.range_ids),
            extra: cap_shadow_mismatch_slice(&self.extra),
            missed: cap_shadow_mismatch_slice(&self.missed),
            task_id_boundary_noise: cap_shadow_mismatch_slice(&self.task_id_boundary_noise),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FindMismatchesInShadowResult {
    pub new_range: TaskMismatches,
    pub current_range: TaskMismatches,
    pub previous_range: TaskMismatches,
    pub current_range_id: i64,
    pub cache_task_count: usize,
    pub db_task_count: usize,
}

#[derive(Clone, Debug)]
pub struct CachedQueueReaderState {
    pub inclusive_lower_bound: HistoryTaskKey,
    pub exclusive_upper_bound: HistoryTaskKey,
    pub cache_size: usize,
}

impl CachedQueueReaderBase {
    pub fn lock(&self) -> MutexGuard<'_, CachedInner> {
        self.mu.lock().unwrap()
    }

    fn mode(&self) -> String {
        (self.options.mode)(self.shard.get_shard_id())
    }

    pub fn is_enabled(&self) -> bool {
        self.mode() == "enabled"
    }

    pub fn is_shadow(&self) -> bool {
        self.mode() == "shadow"
    }

    pub fn is_disabled(&self) -> bool {
        is_cached_queue_reader_disabled(&self.mode())
    }

    pub fn is_empty(&self) -> bool {
        self.lock()
            .exclusive_upper_bound
            .equal(&HistoryTaskKey::minimum())
    }

    pub fn clear_if_not_empty(&self) {
        if !self.is_empty() {
            self.clear();
        }
    }

    pub fn clear(&self) {
        let mut g = self.lock();
        self.logger.info(&format!(
            "cache fully cleared cacheState={:?}",
            Self::get_state(&g)
        ));
        self.clear_locked(&mut g);
    }

    pub fn clear_locked(&self, g: &mut CachedInner) {
        g.queue.clear();
        self.update_inclusive_lower_bound(g, HistoryTaskKey::minimum());
        self.update_exclusive_upper_bound(g, HistoryTaskKey::minimum());
        // Go onClearLockedFn (scheduled reader's resetPrefetchState; no-op state for immediate).
        g.pending_inject_buffer.clear();
        g.prefetch_target_upper = HistoryTaskKey::minimum();
    }

    fn is_range_id_changed_locked(&self, g: &CachedInner) -> bool {
        self.shard.get_range_id() != g.last_range_id
    }

    pub fn fallback_if_range_id_changed(&self) -> bool {
        let mut g = self.lock();
        if !self.is_range_id_changed_locked(&g) {
            return false;
        }

        let new_range_id = self.shard.get_range_id();
        let prev_range_id = g.last_range_id;
        g.last_range_id = new_range_id;

        if new_range_id == prev_range_id + 1 {
            self.logger.info(&format!(
                "rangeID changed on same host, cache not cleared previousRangeID={prev_range_id} newRangeID={new_range_id}"
            ));
            return false;
        }

        self.logger.info(&format!(
            "rangeID changed, clearing cache previousRangeID={prev_range_id} newRangeID={new_range_id}"
        ));
        self.clear_locked(&mut g);
        true
    }

    pub fn is_range_covered(
        g: &CachedInner,
        inclusive_min: HistoryTaskKey,
        exclusive_max: HistoryTaskKey,
    ) -> bool {
        !inclusive_min.less(&g.inclusive_lower_bound)
            && !exclusive_max.greater(&g.exclusive_upper_bound)
    }

    pub fn is_task_covered(g: &CachedInner, key: HistoryTaskKey) -> bool {
        !key.less(&g.inclusive_lower_bound) && key.less(&g.exclusive_upper_bound)
    }

    pub fn update_exclusive_upper_bound(&self, g: &mut CachedInner, new_key: HistoryTaskKey) {
        if self.logger.debug_on() {
            self.logger.debug(&format!(
                "upper bound is updated cacheState={:?} newUpperBound={:?}",
                Self::get_state(g),
                new_key
            ));
        }
        g.exclusive_upper_bound = new_key;
        self.metrics
            .record_histogram_value(metric::CACHED_QUEUE_SIZE_HISTOGRAM, g.queue.len() as f64);
        if let Some(tx) = self.on_upper_bound_updated.lock().unwrap().as_ref() {
            let _ = tx.try_send(());
        }
    }

    pub fn update_inclusive_lower_bound(&self, g: &mut CachedInner, new_key: HistoryTaskKey) {
        if self.logger.debug_on() {
            self.logger.debug(&format!(
                "lower bound is updated cacheState={:?} newLowerBound={:?}",
                Self::get_state(g),
                new_key
            ));
        }
        g.inclusive_lower_bound = new_key;
        self.metrics
            .record_histogram_value(metric::CACHED_QUEUE_SIZE_HISTOGRAM, g.queue.len() as f64);
    }

    pub fn advance_inclusive_lower_bound(&self, g: &mut CachedInner, mut new_key: HistoryTaskKey) {
        if !new_key.greater(&g.inclusive_lower_bound) {
            return;
        }
        if !new_key.less(&g.exclusive_upper_bound) {
            new_key = g.exclusive_upper_bound;
        }
        g.queue.l_trim(new_key);
        self.update_inclusive_lower_bound(g, new_key);
    }

    pub fn update_read_level(&self, read_level: HistoryTaskKey) {
        let mut g = self.lock();
        if read_level.equal(&HistoryTaskKey::maximum()) {
            return;
        }
        self.advance_inclusive_lower_bound(&mut g, read_level);
    }

    pub fn emit_inject_status_count(&self, status: &str, count: i64) {
        if count == 0 {
            return;
        }
        self.metrics
            .tagged(&[("status", status.to_string())])
            .add_counter(metric::CACHED_QUEUE_INJECT_ATTEMPT_COUNTER, count);
    }

    pub async fn look_ahead(&self, req: &LookAHeadRequest) -> Result<LookAHeadResponse, DepError> {
        self.base.look_ahead(req).await
    }

    pub async fn get_task(&self, req: &GetTaskRequest) -> Result<GetTaskResponse, DepError> {
        if self.is_disabled() {
            return self.base.get_task(req).await;
        }

        if self.fallback_if_range_id_changed() {
            self.logger
                .info("GetTask falling back to base reader after rangeID change");
            return self.base.get_task(req).await;
        }

        let inclusive_min_task_key = match resolve_inclusive_min_task_key(req) {
            Some(k) => k,
            None => {
                self.logger.info(
                    "NextPageToken is set but NextTaskKey is not set, delegating to base reader",
                );
                return self.base.get_task(req).await;
            }
        };
        let exclusive_max_task_key = req.progress.range.exclusive_max_task_key;

        let (tasks, next_task_key, log_state) = {
            let g = self.lock();
            let covered = Self::is_range_covered(&g, inclusive_min_task_key, exclusive_max_task_key);
            let log_state = format!(
                "inclusiveMinTaskKey={:?} cacheState={:?}",
                inclusive_min_task_key,
                Self::get_state(&g)
            );
            if !covered {
                drop(g);
                self.metrics.inc_counter(metric::CACHED_QUEUE_MISSES_COUNTER);
                self.logger.debug(&format!("cache miss {log_state}"));
                return self.base.get_task(req).await;
            }
            let (tasks, next) = g.queue.get_tasks(
                inclusive_min_task_key,
                exclusive_max_task_key,
                &req.predicate,
                req.page_size as i64,
            );
            (tasks, next, log_state)
        };

        self.metrics.inc_counter(metric::CACHED_QUEUE_HITS_COUNTER);
        self.logger.debug(&format!("cache hit {log_state}"));

        let cache_resp = GetTaskResponse {
            tasks,
            progress: GetTaskProgress {
                range: Range {
                    inclusive_min_task_key: next_task_key,
                    exclusive_max_task_key: req.progress.range.exclusive_max_task_key,
                },
                next_page_token: None,
                next_task_key,
            },
        };

        if self.is_shadow() || self.is_periodic_shadow_sample() {
            return self.get_task_in_shadow(req, cache_resp, &log_state).await;
        }

        Ok(cache_resp)
    }

    fn is_periodic_shadow_sample(&self) -> bool {
        use std::sync::atomic::Ordering;
        let interval = (self.options.shadow_sample_interval)();
        if interval.is_zero() {
            return false;
        }
        let now = time_unix_nano(&self.clock.now());
        let last = self.last_shadow_sample_unix_nano.load(Ordering::SeqCst);
        if now - last < interval.as_nanos() as i64 {
            return false;
        }
        self.last_shadow_sample_unix_nano
            .compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn get_state(g: &CachedInner) -> CachedQueueReaderState {
        CachedQueueReaderState {
            inclusive_lower_bound: g.inclusive_lower_bound,
            exclusive_upper_bound: g.exclusive_upper_bound,
            cache_size: g.queue.len(),
        }
    }

    async fn get_task_in_shadow(
        &self,
        req: &GetTaskRequest,
        cache_resp: GetTaskResponse,
        log_tags: &str,
    ) -> Result<GetTaskResponse, DepError> {
        let db_resp = match self.base.get_task(req).await {
            Ok(r) => r,
            Err(e) => {
                self.logger.error(&format!(
                    "shadow comparison skipped, base returned error {log_tags} error={e}"
                ));
                return Err(e);
            }
        };
        let result = self.find_mismatches_in_shadow(&cache_resp, &db_resp, req);
        self.report_shadow_comparison(result, log_tags);
        Ok(db_resp)
    }

    fn get_task_range_id(&self, task_id: i64) -> i64 {
        task_id >> (self.shard.get_config().range_size_bits as i64)
    }

    fn report_shadow_comparison(&self, mut result: FindMismatchesInShadowResult, log_tags: &str) {
        self.emit_shadow_mismatch_metrics(&result);

        result.new_range = result.new_range.cap();
        result.current_range = result.current_range.cap();
        result.previous_range = result.previous_range.cap();

        let tags = format!("{log_tags} shadowComparison={result:?}");

        if !result.new_range.is_empty() {
            self.logger
                .info(&format!("stale shard owner, no check for mismatches {tags}"));
        } else if !result.current_range.missed.is_empty() {
            self.logger.warn(&format!(
                "potential severe mismatch between db and cache states {tags}"
            ));
        } else if !result.current_range.extra.is_empty() || !result.previous_range.is_empty() {
            self.logger.info(&format!(
                "potential non-critical mismatch between db and cache states {tags}"
            ));
        } else {
            self.logger
                .debug(&format!("shadow comparison matched {tags}"));
        }
    }

    fn emit_shadow_mismatch_metrics(&self, result: &FindMismatchesInShadowResult) {
        if !result.new_range.is_empty() {
            return;
        }
        if !result.current_range.missed.is_empty() {
            self.metrics
                .tagged(&[("range", "current".to_string())])
                .add_counter(
                    metric::CACHED_QUEUE_SHADOW_MISMATCH_COUNTER,
                    result.current_range.missed.len() as i64,
                );
        }
        if !result.previous_range.missed.is_empty() {
            self.metrics
                .tagged(&[("range", "previous".to_string())])
                .add_counter(
                    metric::CACHED_QUEUE_SHADOW_MISMATCH_COUNTER,
                    result.previous_range.missed.len() as i64,
                );
        }
    }

    fn find_mismatches_in_shadow(
        &self,
        cache_resp: &GetTaskResponse,
        db_resp: &GetTaskResponse,
        req: &GetTaskRequest,
    ) -> FindMismatchesInShadowResult {
        let inclusive_min_task_key =
            resolve_inclusive_min_task_key(req).unwrap_or_else(HistoryTaskKey::minimum);

        let cache_task_keys: std::collections::HashSet<i64> =
            cache_resp.tasks.iter().map(|t| t.get_task_id()).collect();
        let db_task_keys: std::collections::HashSet<i64> =
            db_resp.tasks.iter().map(|t| t.get_task_id()).collect();

        let mut result = FindMismatchesInShadowResult::default();
        let current_range_id = self.shard.get_range_id();
        let mut new_range_ids: BTreeSet<i64> = BTreeSet::new();
        let mut previous_range_ids: BTreeSet<i64> = BTreeSet::new();

        // 0 = new, 1 = previous, 2 = current
        let mut bucket = |task_range_id: i64| -> u8 {
            if task_range_id > current_range_id {
                new_range_ids.insert(task_range_id);
                0
            } else if task_range_id < current_range_id {
                previous_range_ids.insert(task_range_id);
                1
            } else {
                2
            }
        };

        for t in &db_resp.tasks {
            if cache_task_keys.contains(&t.get_task_id()) {
                continue;
            }
            let b = bucket(self.get_task_range_id(t.get_task_id()));
            let m = match b {
                0 => &mut result.new_range,
                1 => &mut result.previous_range,
                _ => &mut result.current_range,
            };
            if t.get_task_key().less(&inclusive_min_task_key) {
                m.task_id_boundary_noise.push(to_shadow_mismatch_task_info(t));
                continue;
            }
            m.missed.push(to_shadow_mismatch_task_info(t));
        }

        for t in &cache_resp.tasks {
            if db_task_keys.contains(&t.get_task_id()) {
                continue;
            }
            let b = bucket(self.get_task_range_id(t.get_task_id()));
            let m = match b {
                0 => &mut result.new_range,
                1 => &mut result.previous_range,
                _ => &mut result.current_range,
            };
            m.extra.push(to_shadow_mismatch_task_info(t));
        }

        result.new_range.range_ids = new_range_ids.into_iter().collect();
        result.previous_range.range_ids = previous_range_ids.into_iter().collect();
        result.current_range_id = current_range_id;
        result.db_task_count = db_resp.tasks.len();
        result.cache_task_count = cache_resp.tasks.len();
        result
    }
}

#[allow(dead_code)]
type _Unused = HashMap<i64, ()>;
