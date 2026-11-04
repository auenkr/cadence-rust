//! `newTransferQueueProcessorOptions` (transfer_queue_processor_base.go) and
//! `newTimerQueueProcessorOptions` (timer_queue_processor_base.go).
use std::sync::Arc;
use std::time::Duration;

use super::config::QueueConfig;
use super::deps::MetricScopeIdx;
use super::processor_options::QueueProcessorOptions;

fn apply_common_split(o: &mut QueueProcessorOptions, c: &QueueConfig, is_failover: bool) {
    if is_failover {
        o.enable_split = Arc::new(|| false);
        o.enable_persist_queue_states = Arc::new(|| false);
        o.enable_load_queue_states = Arc::new(|| false);
    } else {
        o.enable_split = c.queue_processor_enable_split.clone();
        o.split_max_level = c.queue_processor_split_max_level.clone();
        o.enable_random_split_by_domain_id = c.queue_processor_enable_random_split_by_domain_id.clone();
        o.random_split_probability = c.queue_processor_random_split_probability.clone();
        o.enable_pending_task_split_by_domain_id = c.queue_processor_enable_pending_task_split_by_domain_id.clone();
        o.pending_task_split_threshold = c.queue_processor_pending_task_split_threshold.clone();
        o.enable_stuck_task_split_by_domain_id = c.queue_processor_enable_stuck_task_split_by_domain_id.clone();
        o.stuck_task_split_threshold = c.queue_processor_stuck_task_split_threshold.clone();
        o.split_look_ahead_duration_by_domain_id = c.queue_processor_split_look_ahead_duration_by_domain_id.clone();
        o.enable_persist_queue_states = c.queue_processor_enable_persist_queue_states.clone();
        o.enable_load_queue_states = c.queue_processor_enable_load_queue_states.clone();
        o.max_start_jitter_interval = Arc::new(|| Duration::ZERO);
    }
}

pub fn new_transfer_queue_processor_options(c: &QueueConfig, is_active: bool, is_failover: bool) -> QueueProcessorOptions {
    let scope = if is_active {
        MetricScopeIdx::TransferActiveQueueProcessorScope
    } else {
        MetricScopeIdx::TransferStandbyQueueProcessorScope
    };
    let mut o = QueueProcessorOptions::new(scope);
    o.batch_size = c.transfer_task_batch_size.clone();
    o.delete_batch_size = c.transfer_task_delete_batch_size.clone();
    o.max_poll_rps = c.transfer_processor_max_poll_rps.clone();
    o.max_poll_interval = c.transfer_processor_max_poll_interval.clone();
    o.max_poll_interval_jitter_coefficient = c.transfer_processor_max_poll_interval_jitter_coefficient.clone();
    o.update_ack_interval = c.transfer_processor_update_ack_interval.clone();
    o.update_ack_interval_jitter_coefficient = c.transfer_processor_update_ack_interval_jitter_coefficient.clone();
    o.max_redispatch_queue_size = c.transfer_processor_max_redispatch_queue_size.clone();
    o.split_queue_interval = c.transfer_processor_split_queue_interval.clone();
    o.split_queue_interval_jitter_coefficient = c.transfer_processor_split_queue_interval_jitter_coefficient.clone();
    o.poll_backoff_interval = c.queue_processor_poll_backoff_interval.clone();
    o.poll_backoff_interval_jitter_coefficient = c.queue_processor_poll_backoff_interval_jitter_coefficient.clone();
    o.enable_validator = c.transfer_processor_enable_validator.clone();
    o.validation_interval = c.transfer_processor_validation_interval.clone();
    o.enable_graceful_sync_shutdown = c.queue_processor_enable_graceful_sync_shutdown.clone();

    apply_common_split(&mut o, c, is_failover);
    if is_failover {
        o.max_start_jitter_interval = c.transfer_processor_failover_max_start_jitter_interval.clone();
    }
    o.redispatch_interval = if is_active {
        c.active_task_redispatch_interval.clone()
    } else {
        c.standby_task_redispatch_interval.clone()
    };
    o
}

pub fn new_timer_queue_processor_options(c: &QueueConfig, is_active: bool, is_failover: bool) -> QueueProcessorOptions {
    let scope = if is_active {
        MetricScopeIdx::TimerActiveQueueProcessorScope
    } else {
        MetricScopeIdx::TimerStandbyQueueProcessorScope
    };
    let mut o = QueueProcessorOptions::new(scope);
    o.batch_size = c.timer_task_batch_size.clone();
    o.delete_batch_size = c.timer_task_delete_batch_size.clone();
    o.max_poll_rps = c.timer_processor_max_poll_rps.clone();
    o.max_poll_interval = c.timer_processor_max_poll_interval.clone();
    o.max_poll_interval_jitter_coefficient = c.timer_processor_max_poll_interval_jitter_coefficient.clone();
    o.update_ack_interval = c.timer_processor_update_ack_interval.clone();
    o.update_ack_interval_jitter_coefficient = c.timer_processor_update_ack_interval_jitter_coefficient.clone();
    o.max_redispatch_queue_size = c.timer_processor_max_redispatch_queue_size.clone();
    o.split_queue_interval = c.timer_processor_split_queue_interval.clone();
    o.split_queue_interval_jitter_coefficient = c.timer_processor_split_queue_interval_jitter_coefficient.clone();
    o.poll_backoff_interval = c.queue_processor_poll_backoff_interval.clone();
    o.poll_backoff_interval_jitter_coefficient = c.queue_processor_poll_backoff_interval_jitter_coefficient.clone();
    o.enable_graceful_sync_shutdown = c.queue_processor_enable_graceful_sync_shutdown.clone();

    apply_common_split(&mut o, c, is_failover);
    if is_failover {
        o.max_start_jitter_interval = c.timer_processor_failover_max_start_jitter_interval.clone();
    }
    o.redispatch_interval = if is_active {
        c.active_task_redispatch_interval.clone()
    } else {
        c.standby_task_redispatch_interval.clone()
    };
    o
}
