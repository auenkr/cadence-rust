//! Live History configuration for the queue v1 processors.
//! Properties stay as callbacks so reloads and domain filters reach running queues.
use std::sync::Arc;

use crate::config::Config;
use crate::queue::config::QueueConfig;
use crate::queue::shard_deps::ProcessorConfig;

pub fn queue_config(config: &Config) -> Arc<QueueConfig> {
    let queue_processor_pending_task_split_threshold = config.queue_processor_pending_task_split_threshold.clone();
    let queue_processor_stuck_task_split_threshold = config.queue_processor_stuck_task_split_threshold.clone();
    Arc::new(QueueConfig {
        transfer_task_batch_size: config.transfer_task_batch_size.clone(),
        transfer_task_delete_batch_size: config.transfer_task_delete_batch_size.clone(),
        transfer_processor_max_poll_rps: config.transfer_processor_max_poll_rps.clone(),
        transfer_processor_max_poll_interval: config.transfer_processor_max_poll_interval.clone(),
        transfer_processor_max_poll_interval_jitter_coefficient: config.transfer_processor_max_poll_interval_jitter_coefficient.clone(),
        transfer_processor_update_ack_interval: config.transfer_processor_update_ack_interval.clone(),
        transfer_processor_update_ack_interval_jitter_coefficient: config.transfer_processor_update_ack_interval_jitter_coefficient.clone(),
        transfer_processor_max_redispatch_queue_size: config.transfer_processor_max_redispatch_queue_size.clone(),
        transfer_processor_split_queue_interval: config.transfer_processor_split_queue_interval.clone(),
        transfer_processor_split_queue_interval_jitter_coefficient: config.transfer_processor_split_queue_interval_jitter_coefficient.clone(),
        transfer_processor_enable_validator: config.transfer_processor_enable_validator.clone(),
        transfer_processor_validation_interval: config.transfer_processor_validation_interval.clone(),
        transfer_processor_failover_max_start_jitter_interval: config.transfer_processor_failover_max_start_jitter_interval.clone(),
        timer_task_batch_size: config.timer_task_batch_size.clone(),
        timer_task_delete_batch_size: config.timer_task_delete_batch_size.clone(),
        timer_processor_max_poll_rps: config.timer_processor_max_poll_rps.clone(),
        timer_processor_max_poll_interval: config.timer_processor_max_poll_interval.clone(),
        timer_processor_max_poll_interval_jitter_coefficient: config.timer_processor_max_poll_interval_jitter_coefficient.clone(),
        timer_processor_update_ack_interval: config.timer_processor_update_ack_interval.clone(),
        timer_processor_update_ack_interval_jitter_coefficient: config.timer_processor_update_ack_interval_jitter_coefficient.clone(),
        timer_processor_max_redispatch_queue_size: config.timer_processor_max_redispatch_queue_size.clone(),
        timer_processor_split_queue_interval: config.timer_processor_split_queue_interval.clone(),
        timer_processor_split_queue_interval_jitter_coefficient: config.timer_processor_split_queue_interval_jitter_coefficient.clone(),
        timer_processor_failover_max_start_jitter_interval: config.timer_processor_failover_max_start_jitter_interval.clone(),
        queue_processor_poll_backoff_interval: config.queue_processor_poll_backoff_interval.clone(),
        queue_processor_poll_backoff_interval_jitter_coefficient: config.queue_processor_poll_backoff_interval_jitter_coefficient.clone(),
        queue_processor_enable_graceful_sync_shutdown: config.queue_processor_enable_graceful_sync_shutdown.clone(),
        queue_processor_enable_split: config.queue_processor_enable_split.clone(),
        queue_processor_split_max_level: config.queue_processor_split_max_level.clone(),
        queue_processor_enable_random_split_by_domain_id: config.queue_processor_enable_random_split_by_domain_id.clone(),
        queue_processor_random_split_probability: config.queue_processor_random_split_probability.clone(),
        queue_processor_enable_pending_task_split_by_domain_id: config.queue_processor_enable_pending_task_split_by_domain_id.clone(),
        queue_processor_pending_task_split_threshold: Arc::new(move || (queue_processor_pending_task_split_threshold)().into_iter().collect()),
        queue_processor_enable_stuck_task_split_by_domain_id: config.queue_processor_enable_stuck_task_split_by_domain_id.clone(),
        queue_processor_stuck_task_split_threshold: Arc::new(move || (queue_processor_stuck_task_split_threshold)().into_iter().collect()),
        queue_processor_split_look_ahead_duration_by_domain_id: config.queue_processor_split_look_ahead_duration_by_domain_id.clone(),
        queue_processor_enable_persist_queue_states: config.queue_processor_enable_persist_queue_states.clone(),
        queue_processor_enable_load_queue_states: config.queue_processor_enable_load_queue_states.clone(),
        active_task_redispatch_interval: config.active_task_redispatch_interval.clone(),
        standby_task_redispatch_interval: config.standby_task_redispatch_interval.clone(),
    })
}

pub fn processor_config(config: &Config) -> ProcessorConfig {
    ProcessorConfig {
        disable_transfer_failover_queue: config.disable_transfer_failover_queue.clone(),
        disable_timer_failover_queue: config.disable_timer_failover_queue.clone(),
        queue_processor_enable_graceful_sync_shutdown: config.queue_processor_enable_graceful_sync_shutdown.clone(),
        transfer_processor_complete_transfer_interval: config.transfer_processor_complete_transfer_interval.clone(),
        transfer_processor_complete_transfer_failure_retry_count: config.transfer_processor_complete_transfer_failure_retry_count.clone(),
        timer_processor_complete_timer_interval: config.timer_processor_complete_timer_interval.clone(),
        timer_processor_complete_timer_failure_retry_count: config.timer_processor_complete_timer_failure_retry_count.clone(),
        transfer_task_delete_batch_size: config.transfer_task_delete_batch_size.clone(),
        timer_task_delete_batch_size: config.timer_task_delete_batch_size.clone(),
        task_critical_retry_count: config.task_critical_retry_count.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_collection::{Collection, InMemoryClient};
    use serde_json::json;

    #[test]
    fn running_queue_observes_dynamic_property_and_map_updates() {
        let client = Arc::new(InMemoryClient::new());
        let config = Config::new(&Collection::new(client.clone()), 4, 4 << 20, false, "host");
        let queue = queue_config(&config);
        let processor = processor_config(&config);
        client.update_value("history.transferTaskBatchSize", json!(27));
        client.update_value("history.taskCriticalRetryCount", json!(12));
        client.update_value("history.queueProcessorPendingTaskSplitThreshold", json!({"0": 41}));
        assert_eq!((queue.transfer_task_batch_size)(), 27);
        assert_eq!((processor.task_critical_retry_count)(), 12);
        assert_eq!((queue.queue_processor_pending_task_split_threshold)().get("0"), Some(&json!(41)));
        client.update_value("history.transferTaskBatchSize", json!(31));
        client.update_value("history.queueProcessorPendingTaskSplitThreshold", json!({"1": 53}));
        assert_eq!((queue.transfer_task_batch_size)(), 31);
        let thresholds = (queue.queue_processor_pending_task_split_threshold)();
        assert!(!thresholds.contains_key("0"));
        assert_eq!(thresholds.get("1"), Some(&json!(53)));
    }
}
