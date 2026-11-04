//! Adapters exposing `config::Config` through the per-module config traits
//! (Go passes the single `*config.Config` to every package).
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::engine::runtime::EngineConfig;
use crate::failover::FailoverConfig;
use crate::replication::deps::ReplicationConfig;
use crate::task::deps::{BoolPropertyFn, BoolPropertyFnWithDomainFilter, HistoryConfig};

#[derive(Clone)]
pub struct ConfigAdapter(pub Arc<Config>);

impl ConfigAdapter {
    pub fn new(cfg: Arc<Config>) -> Self {
        Self(cfg)
    }
}

fn pos(v: i64) -> usize {
    v.max(0) as usize
}

impl HistoryConfig for ConfigAdapter {
    fn enable_debug_mode(&self) -> bool {
        self.0.enable_debug_mode
    }
    fn enable_task_info_log_by_domain_id(&self, domain: &str) -> bool {
        (self.0.enable_task_info_log_by_domain_id)(domain)
    }
    fn enable_drop_stuck_task_by_domain_id(&self, domain: &str) -> bool {
        (self.0.enable_drop_stuck_task_by_domain_id)(domain)
    }
    fn task_process_rps(&self) -> i64 {
        (self.0.task_process_rps)("")
    }
    fn task_critical_retry_count(&self) -> i32 {
        (self.0.task_critical_retry_count)() as i32
    }
    fn number_of_shards(&self) -> i32 {
        self.0.number_of_shards as i32
    }
    fn task_scheduler_global_domain_rps(&self, domain: &str) -> i32 {
        (self.0.task_scheduler_global_domain_rps)(domain) as i32
    }
    fn task_scheduler_enable_rate_limiter(&self) -> BoolPropertyFn {
        self.0.task_scheduler_enable_rate_limiter.clone()
    }
    fn task_scheduler_enable_rate_limiter_shadow_mode(&self) -> BoolPropertyFnWithDomainFilter {
        self.0.task_scheduler_enable_rate_limiter_shadow_mode.clone()
    }
}

impl EngineConfig for ConfigAdapter {
    fn enable_consistent_query(&self) -> bool {
        (self.0.enable_consistent_query)()
    }
    fn enable_consistent_query_by_domain(&self, domain: &str) -> bool {
        (self.0.enable_consistent_query_by_domain)(domain)
    }
    fn enable_sticky_query(&self, domain: &str) -> bool {
        (self.0.enable_sticky_query)(domain)
    }
    fn max_buffered_query_count(&self) -> usize {
        pos((self.0.max_buffered_query_count)())
    }
    fn long_poll_expiration_interval(&self, domain: &str) -> Duration {
        (self.0.long_poll_expiration_interval)(domain)
    }
    fn maximum_signals_per_execution(&self, domain: &str) -> i32 {
        (self.0.maximum_signals_per_execution)(domain) as i32
    }
    fn enable_history_corruption_check(&self, domain: &str) -> bool {
        (self.0.enable_history_corruption_check)(domain)
    }
}

impl FailoverConfig for ConfigAdapter {
    fn number_of_shards(&self) -> usize {
        pos(self.0.number_of_shards)
    }
    fn notify_failover_marker_interval(&self) -> Duration {
        (self.0.notify_failover_marker_interval)()
    }
    fn notify_failover_marker_timer_jitter_coefficient(&self) -> f64 {
        (self.0.notify_failover_marker_timer_jitter_coefficient)()
    }
}

impl ReplicationConfig for ConfigAdapter {
    fn number_of_shards(&self) -> usize {
        pos(self.0.number_of_shards)
    }
    fn max_response_size(&self) -> usize {
        pos(self.0.max_response_size)
    }
    fn replication_task_processor_error_retry_wait(&self, s: i32) -> Duration {
        (self.0.replication_task_processor_error_retry_wait)(s)
    }
    fn replication_task_processor_error_retry_max_attempts(&self, s: i32) -> usize {
        pos((self.0.replication_task_processor_error_retry_max_attempts)(s))
    }
    fn replication_task_processor_error_second_retry_wait(&self, s: i32) -> Duration {
        (self.0.replication_task_processor_error_second_retry_wait)(s)
    }
    fn replication_task_processor_error_second_retry_max_wait(&self, s: i32) -> Duration {
        (self.0.replication_task_processor_error_second_retry_max_wait)(s)
    }
    fn replication_task_processor_error_second_retry_expiration(&self, s: i32) -> Duration {
        (self.0.replication_task_processor_error_second_retry_expiration)(s)
    }
    fn replication_task_processor_no_task_retry_wait(&self, s: i32) -> Duration {
        (self.0.replication_task_processor_no_task_retry_wait)(s)
    }
    fn replication_task_processor_cleanup_interval(&self, s: i32) -> Duration {
        (self.0.replication_task_processor_cleanup_interval)(s)
    }
    fn replication_task_processor_cleanup_jitter_coefficient(&self, s: i32) -> f64 {
        (self.0.replication_task_processor_cleanup_jitter_coefficient)(s)
    }
    fn replication_task_processor_start_wait(&self, s: i32) -> Duration {
        (self.0.replication_task_processor_start_wait)(s)
    }
    fn replication_task_processor_start_wait_jitter_coefficient(&self, s: i32) -> f64 {
        (self.0.replication_task_processor_start_wait_jitter_coefficient)(s)
    }
    fn replication_task_processor_latency_log_threshold(&self) -> Duration {
        (self.0.replication_task_processor_latency_log_threshold)()
    }
    fn replication_task_processor_shard_qps(&self) -> f64 {
        (self.0.replication_task_processor_shard_qps)()
    }
    fn replication_task_processor_host_qps(&self) -> f64 {
        (self.0.replication_task_processor_host_qps)()
    }
    fn replicator_task_delete_batch_size(&self) -> usize {
        pos((self.0.replicator_task_delete_batch_size)())
    }
    fn shard_sync_min_interval(&self) -> Duration {
        (self.0.shard_sync_min_interval)()
    }
    fn shard_sync_timer_jitter_coefficient(&self) -> f64 {
        (self.0.shard_sync_timer_jitter_coefficient)()
    }
    fn replication_task_fetcher_parallelism(&self) -> usize {
        pos((self.0.replication_task_fetcher_parallelism)())
    }
    fn replication_task_fetcher_aggregation_interval(&self) -> Duration {
        (self.0.replication_task_fetcher_aggregation_interval)()
    }
    fn replication_task_fetcher_timer_jitter_coefficient(&self) -> f64 {
        (self.0.replication_task_fetcher_timer_jitter_coefficient)()
    }
    fn replication_task_fetcher_error_retry_wait(&self) -> Duration {
        (self.0.replication_task_fetcher_error_retry_wait)()
    }
    fn replication_task_fetcher_service_busy_wait(&self) -> Duration {
        (self.0.replication_task_fetcher_service_busy_wait)()
    }
    fn replicator_cache_capacity(&self) -> usize {
        pos((self.0.replicator_cache_capacity)())
    }
    fn replicator_cache_max_size(&self) -> usize {
        pos((self.0.replicator_cache_max_size)())
    }
    fn replicator_read_task_max_retry_count(&self) -> usize {
        pos((self.0.replicator_read_task_max_retry_count)())
    }
    fn replication_task_generation_qps(&self) -> f64 {
        (self.0.replication_task_generation_qps)()
    }
    fn replicator_processor_min_task_batch_size(&self, s: i32) -> i32 {
        (self.0.replicator_processor_min_task_batch_size)(s) as i32
    }
    fn replicator_processor_max_task_batch_size(&self, s: i32) -> i32 {
        (self.0.replicator_processor_max_task_batch_size)(s) as i32
    }
    fn replicator_processor_batch_size_step_count(&self, s: i32) -> usize {
        pos((self.0.replicator_processor_batch_size_step_count)(s))
    }
}
