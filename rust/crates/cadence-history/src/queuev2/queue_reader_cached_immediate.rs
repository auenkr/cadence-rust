//! Port of queuev2/queue_reader_cached_immediate.go

use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;

use super::deps::*;
use super::queue_mem::InMemQueue;
use super::queue_reader::{
    GetTaskRequest, GetTaskResponse, LookAHeadRequest, LookAHeadResponse, QueueReader,
};
use super::queue_reader_cached_base::*;
use super::virtual_queue::{DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED};

pub struct CachedImmediateQueueReader {
    b: CachedQueueReaderBase,
}

pub fn new_cached_immediate_queue_reader(
    base: Arc<dyn QueueReader>,
    queue: Box<dyn InMemQueue>,
    shard: Arc<dyn ShardContext>,
    metrics_scope: Arc<dyn MetricsScope>,
) -> Arc<CachedImmediateQueueReader> {
    let config = shard.get_config();
    let clock = shard.get_time_source();
    let logger = shard
        .get_logger()
        .with_tags(&[("component", "cached-immediate-queue-reader".to_string())]);
    new_cached_immediate_queue_reader_with_options(
        base,
        queue,
        shard,
        clock,
        logger,
        metrics_scope,
        CachedQueueReaderOptions {
            mode: config.transfer_processor_cached_queue_reader_mode.clone(),
            max_size: config.transfer_processor_cache_max_size.clone(),
            shadow_sample_interval: config
                .transfer_processor_cached_queue_reader_shadow_sample_interval
                .clone(),
        },
    )
}

pub fn new_cached_immediate_queue_reader_with_options(
    base: Arc<dyn QueueReader>,
    queue: Box<dyn InMemQueue>,
    shard: Arc<dyn ShardContext>,
    clock_source: Arc<dyn TimeSource>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    options: CachedQueueReaderOptions,
) -> Arc<CachedImmediateQueueReader> {
    Arc::new(CachedImmediateQueueReader {
        b: new_cached_queue_reader_base(
            base,
            queue,
            shard,
            clock_source,
            logger,
            metrics_scope,
            options,
        ),
    })
}

impl CachedImmediateQueueReader {
    /// Go `putTasks` (caller holds the lock).
    fn put_tasks(&self, g: &mut CachedInner, tasks: &[PTask]) {
        if tasks.is_empty() {
            return;
        }
        g.queue.put_tasks(tasks);

        if g.exclusive_upper_bound.equal(&HistoryTaskKey::minimum()) {
            let first = g
                .queue
                .look_ahead(HistoryTaskKey::minimum())
                .expect("queue non-empty after put")
                .get_task_key();
            self.b.update_inclusive_lower_bound(g, first);
            self.b
                .update_exclusive_upper_bound(g, HistoryTaskKey::maximum());
        }

        let (new_lower, trimmed) = g.queue.l_trim_by_size((self.b.options.max_size)());
        if !trimmed {
            return;
        }
        if new_lower.equal(&HistoryTaskKey::minimum()) {
            self.b
                .update_exclusive_upper_bound(g, HistoryTaskKey::minimum());
        }
        self.b.update_inclusive_lower_bound(g, new_lower);
    }
}

#[async_trait]
impl QueueReader for CachedImmediateQueueReader {
    async fn get_task(&self, req: &GetTaskRequest) -> Result<GetTaskResponse, DepError> {
        self.b.get_task(req).await
    }

    async fn look_ahead(&self, req: &LookAHeadRequest) -> Result<LookAHeadResponse, DepError> {
        self.b.look_ahead(req).await
    }
}

impl CachedQueueReader for CachedImmediateQueueReader {
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
            .info("Immediate Cached Queue Reader state changed LifeCycleStarted");
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
            .info("Immediate Cached Queue Reader state changed LifeCycleStopped");
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
        let (mut injected, mut dropped_below) = (0i64, 0i64);
        let mut to_put: Vec<PTask> = Vec::new();
        for t in tasks {
            if t.get_task_id() == 0 {
                continue;
            }
            let key = t.get_task_key();
            if key.less(&g.inclusive_lower_bound) {
                dropped_below += 1;
                self.b.logger.warn(&format!(
                    "task key is below the lower bound, dropping task taskKey={:?} cacheState={:?}",
                    key,
                    CachedQueueReaderBase::get_state(&g)
                ));
                continue;
            }
            injected += 1;
            if self.b.logger.debug_on() {
                self.b.logger.debug(&format!(
                    "injecting task taskKey={:?} cacheState={:?}",
                    key,
                    CachedQueueReaderBase::get_state(&g)
                ));
            }
            to_put.push(t.clone());
        }

        self.b.emit_inject_status_count(INJECT_STATUS_INJECTED, injected);
        self.b
            .emit_inject_status_count(INJECT_STATUS_DROPPED_BELOW, dropped_below);

        self.put_tasks(&mut g, &to_put);
    }
}
