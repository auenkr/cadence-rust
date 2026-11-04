//! Port of `go/service/history/queue/processor_options.go`.
//! Go leaves unused option fields nil; here they hold inert defaults (see `Default`).
use std::sync::Arc;
use std::time::Duration;

use super::deps::{BoolDomainIdFn, BoolFn, DurationDomainIdFn, DurationFn, FloatFn, IntFn, MapFn, MetricScopeIdx};

#[derive(Clone)]
pub struct QueueProcessorOptions {
    pub batch_size: IntFn,
    pub delete_batch_size: IntFn,
    pub max_poll_rps: IntFn,
    pub max_poll_interval: DurationFn,
    pub max_poll_interval_jitter_coefficient: FloatFn,
    pub update_ack_interval: DurationFn,
    pub update_ack_interval_jitter_coefficient: FloatFn,
    pub redispatch_interval: DurationFn,
    pub max_redispatch_queue_size: IntFn,
    pub max_start_jitter_interval: DurationFn,
    pub split_queue_interval: DurationFn,
    pub split_queue_interval_jitter_coefficient: FloatFn,
    pub enable_split: BoolFn,
    pub split_max_level: IntFn,
    pub enable_random_split_by_domain_id: BoolDomainIdFn,
    pub random_split_probability: FloatFn,
    pub enable_pending_task_split_by_domain_id: BoolDomainIdFn,
    pub pending_task_split_threshold: MapFn,
    pub enable_stuck_task_split_by_domain_id: BoolDomainIdFn,
    pub stuck_task_split_threshold: MapFn,
    pub split_look_ahead_duration_by_domain_id: DurationDomainIdFn,
    pub poll_backoff_interval: DurationFn,
    pub poll_backoff_interval_jitter_coefficient: FloatFn,
    pub enable_persist_queue_states: BoolFn,
    pub enable_load_queue_states: BoolFn,
    pub enable_graceful_sync_shutdown: BoolFn,
    pub enable_validator: BoolFn,
    pub validation_interval: DurationFn,
    pub max_pending_task_size: IntFn,
    pub metric_scope: MetricScopeIdx,
}

impl QueueProcessorOptions {
    pub fn new(metric_scope: MetricScopeIdx) -> Self {
        let int0: IntFn = Arc::new(|| 0);
        let f0: FloatFn = Arc::new(|| 0.0);
        let b0: BoolFn = Arc::new(|| false);
        let d0: DurationFn = Arc::new(|| Duration::ZERO);
        let map0: MapFn = Arc::new(Default::default);
        let bd0: BoolDomainIdFn = Arc::new(|_| false);
        let dd0: DurationDomainIdFn = Arc::new(|_| Duration::ZERO);
        QueueProcessorOptions {
            batch_size: int0.clone(),
            delete_batch_size: int0.clone(),
            max_poll_rps: int0.clone(),
            max_poll_interval: d0.clone(),
            max_poll_interval_jitter_coefficient: f0.clone(),
            update_ack_interval: d0.clone(),
            update_ack_interval_jitter_coefficient: f0.clone(),
            redispatch_interval: d0.clone(),
            max_redispatch_queue_size: int0.clone(),
            max_start_jitter_interval: d0.clone(),
            split_queue_interval: d0.clone(),
            split_queue_interval_jitter_coefficient: f0.clone(),
            enable_split: b0.clone(),
            split_max_level: int0.clone(),
            enable_random_split_by_domain_id: bd0.clone(),
            random_split_probability: f0.clone(),
            enable_pending_task_split_by_domain_id: bd0.clone(),
            pending_task_split_threshold: map0.clone(),
            enable_stuck_task_split_by_domain_id: bd0,
            stuck_task_split_threshold: map0,
            split_look_ahead_duration_by_domain_id: dd0,
            poll_backoff_interval: d0.clone(),
            poll_backoff_interval_jitter_coefficient: f0,
            enable_persist_queue_states: b0.clone(),
            enable_load_queue_states: b0.clone(),
            enable_graceful_sync_shutdown: b0.clone(),
            enable_validator: b0,
            validation_interval: d0,
            max_pending_task_size: int0,
            metric_scope,
        }
    }
}
