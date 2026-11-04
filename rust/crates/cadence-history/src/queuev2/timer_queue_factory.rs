//! Port of queuev2/timer_queue_factory.go (see transfer_queue_factory.rs for the deps note).

use std::sync::Arc;
use std::time::Duration;

use super::deps::*;
use super::interface::Queue;
use super::queue_base::Options;
use super::queue_cached::new_cached_queue;
use super::queue_mem::new_in_mem_queue;
use super::queue_reader::new_queue_reader;
use super::queue_reader_cached_base::{is_cached_queue_reader_disabled, CachedQueueReader};
use super::queue_reader_cached_scheduled::new_cached_scheduled_queue_reader;
use super::queue_scheduled::new_scheduled_queue;
use super::transfer_queue_factory::{QueueFactory, QueueFactoryDeps};

pub struct TimerQueueFactory {
    task_processor: Arc<dyn Processor>,
    env: Arc<dyn QueueEnv>,
    deps: Arc<dyn QueueFactoryDeps>,
}

pub fn new_timer_queue_factory(
    task_processor: Arc<dyn Processor>,
    env: Arc<dyn QueueEnv>,
    deps: Arc<dyn QueueFactoryDeps>,
) -> Arc<dyn QueueFactory> {
    Arc::new(TimerQueueFactory {
        task_processor,
        env,
        deps,
    })
}

impl TimerQueueFactory {
    fn is_queue_v2_enabled(&self, shard: &Arc<dyn ShardContext>) -> bool {
        (shard.get_config().enable_timer_queue_v2)(shard.get_shard_id())
    }

    fn create_queue_v2(&self, shard: Arc<dyn ShardContext>) -> Arc<dyn Queue> {
        let logger = shard
            .get_logger()
            .with_tags(&[("component", "timer-queue-v2".to_string())]);
        let executor_wrapper =
            self.deps
                .new_executor_wrapper(HistoryTaskCategory::TIMER, &shard, logger.clone());
        let config = shard.get_config();
        let metrics_scope = shard
            .get_metrics_client()
            .scope(metric::TIMER_QUEUE_PROCESSOR_V2_SCOPE);
        let shard_metrics_scope =
            metrics_scope.tagged(&[("shard_id", shard.get_shard_id().to_string())]);
        let alert_shard = shard.clone();
        let alert_cfg = config.clone();
        let options = Options {
            page_size: config.timer_task_batch_size.clone(),
            delete_batch_size: config.timer_task_delete_batch_size.clone(),
            max_poll_rps: config.timer_processor_max_poll_rps.clone(),
            max_poll_interval: config.timer_processor_max_poll_interval.clone(),
            max_poll_interval_jitter_coefficient: config
                .timer_processor_max_poll_interval_jitter_coefficient
                .clone(),
            update_ack_interval: config.timer_processor_update_ack_interval.clone(),
            update_ack_interval_jitter_coefficient: config
                .timer_processor_update_ack_interval_jitter_coefficient
                .clone(),
            max_pending_tasks_count: config.queue_max_pending_task_count.clone(),
            poll_backoff_interval: config.queue_processor_poll_backoff_interval.clone(),
            poll_backoff_interval_jitter_coefficient: config
                .queue_processor_poll_backoff_interval_jitter_coefficient
                .clone(),
            virtual_slice_force_append_interval: config.virtual_slice_force_append_interval.clone(),
            // Go leaves EnableValidator / ValidationInterval unset for timer.
            enable_validator: Arc::new(|| false),
            validation_interval: Arc::new(|| Duration::ZERO),
            max_start_jitter_interval: Arc::new(|| Duration::ZERO),
            redispatch_interval: config.active_task_redispatch_interval.clone(),
            critical_pending_task_count: config.queue_critical_pending_task_count.clone(),
            enable_pending_task_count_alert: Arc::new(move || {
                (alert_cfg.enable_timer_queue_v2_pending_task_count_alert)(
                    alert_shard.get_shard_id(),
                )
            }),
            max_virtual_queue_count: config.queue_max_virtual_queue_count.clone(),
        };

        let mut reader = new_queue_reader(
            shard.clone(),
            HistoryTaskCategory::TIMER,
            options.max_poll_interval.clone(),
            options.max_poll_interval_jitter_coefficient.clone(),
        );
        let mut cached_reader: Option<Arc<dyn CachedQueueReader>> = None;
        if !is_cached_queue_reader_disabled(&(config.timer_processor_cached_queue_reader_mode)(
            shard.get_shard_id(),
        )) {
            let cr = new_cached_scheduled_queue_reader(
                reader.clone(),
                Box::new(new_in_mem_queue()),
                shard.clone(),
                metrics_scope.clone(),
            );
            reader = cr.clone();
            cached_reader = Some(cr);
        }

        let queue = new_scheduled_queue(
            &self.env,
            shard.clone(),
            HistoryTaskCategory::TIMER,
            self.task_processor.clone(),
            executor_wrapper,
            logger,
            shard.get_metrics_client(),
            shard_metrics_scope,
            reader,
            options,
        );

        if let Some(cr) = cached_reader {
            let base = queue.base.clone();
            return new_cached_queue(queue, &base, cr);
        }
        queue
    }
}

impl QueueFactory for TimerQueueFactory {
    fn category(&self) -> HistoryTaskCategory {
        HistoryTaskCategory::TIMER
    }

    fn create_queue(&self, shard: Arc<dyn ShardContext>) -> Arc<dyn Queue> {
        if self.is_queue_v2_enabled(&shard) {
            return self.create_queue_v2(shard);
        }
        self.deps
            .create_queue_v1(HistoryTaskCategory::TIMER, &shard)
    }
}
