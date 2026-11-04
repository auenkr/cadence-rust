//! Subset of go `service/history/config.Config` read by the queue package
//! (used by `newTransferQueueProcessorOptions` / `newTimerQueueProcessorOptions`).
//! TODO(link): replace by config::Config
use super::deps::{BoolDomainIdFn, BoolFn, DurationDomainIdFn, DurationFn, FloatFn, IntFn, MapFn};

#[derive(Clone)]
pub struct QueueConfig {
    pub transfer_task_batch_size: IntFn,
    pub transfer_task_delete_batch_size: IntFn,
    pub transfer_processor_max_poll_rps: IntFn,
    pub transfer_processor_max_poll_interval: DurationFn,
    pub transfer_processor_max_poll_interval_jitter_coefficient: FloatFn,
    pub transfer_processor_update_ack_interval: DurationFn,
    pub transfer_processor_update_ack_interval_jitter_coefficient: FloatFn,
    pub transfer_processor_max_redispatch_queue_size: IntFn,
    pub transfer_processor_split_queue_interval: DurationFn,
    pub transfer_processor_split_queue_interval_jitter_coefficient: FloatFn,
    pub transfer_processor_enable_validator: BoolFn,
    pub transfer_processor_validation_interval: DurationFn,
    pub transfer_processor_failover_max_start_jitter_interval: DurationFn,

    pub timer_task_batch_size: IntFn,
    pub timer_task_delete_batch_size: IntFn,
    pub timer_processor_max_poll_rps: IntFn,
    pub timer_processor_max_poll_interval: DurationFn,
    pub timer_processor_max_poll_interval_jitter_coefficient: FloatFn,
    pub timer_processor_update_ack_interval: DurationFn,
    pub timer_processor_update_ack_interval_jitter_coefficient: FloatFn,
    pub timer_processor_max_redispatch_queue_size: IntFn,
    pub timer_processor_split_queue_interval: DurationFn,
    pub timer_processor_split_queue_interval_jitter_coefficient: FloatFn,
    pub timer_processor_failover_max_start_jitter_interval: DurationFn,

    pub queue_processor_poll_backoff_interval: DurationFn,
    pub queue_processor_poll_backoff_interval_jitter_coefficient: FloatFn,
    pub queue_processor_enable_graceful_sync_shutdown: BoolFn,
    pub queue_processor_enable_split: BoolFn,
    pub queue_processor_split_max_level: IntFn,
    pub queue_processor_enable_random_split_by_domain_id: BoolDomainIdFn,
    pub queue_processor_random_split_probability: FloatFn,
    pub queue_processor_enable_pending_task_split_by_domain_id: BoolDomainIdFn,
    pub queue_processor_pending_task_split_threshold: MapFn,
    pub queue_processor_enable_stuck_task_split_by_domain_id: BoolDomainIdFn,
    pub queue_processor_stuck_task_split_threshold: MapFn,
    pub queue_processor_split_look_ahead_duration_by_domain_id: DurationDomainIdFn,
    pub queue_processor_enable_persist_queue_states: BoolFn,
    pub queue_processor_enable_load_queue_states: BoolFn,

    pub active_task_redispatch_interval: DurationFn,
    pub standby_task_redispatch_interval: DurationFn,
}
