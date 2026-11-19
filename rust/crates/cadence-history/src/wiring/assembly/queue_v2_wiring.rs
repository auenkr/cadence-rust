//! Queue v2 switch (queue v2 piece C): Go `queuev2.NewTransferQueueFactory` /
//! `NewTimerQueueFactory` as used by `historyEngineFactory.CreateEngine`.
//!
//! When `history.enableTransferQueueV2` / `history.enableTimerQueueV2` is true for the shard
//! (Go default true), the category's queue is the v2 immediate / scheduled queue (cached reader
//! per `history.{transfer,timer}ProcessorCachedQueueReaderMode`). When false, queue v1 is kept
//! (built by `queues::build_queue_processors`).
//!
//! The engine talks to both through `engine::runtime::QueueProcessor`; [`EngineQueueV2`]
//! adapts a `queuev2::interface::Queue`. Go's v2 `HandleAction` returns `(nil, nil)` and
//! `LockTaskProcessing` / `UnlockTaskProcessing` are no-ops; the adapter forwards unchanged.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::queue_v2_adapters::{notify_task_info, V2ExecutorAdapter};
use crate::config::Config;
use crate::engine::deps::{HResult, HistoryError, HistoryTaskCategory};
use crate::engine::runtime::{QueueAction, QueueProcessor};
use crate::queuev2::deps as v2;
use crate::queuev2::interface::Queue;
use crate::queuev2::timer_queue_factory::new_timer_queue_factory;
use crate::queuev2::transfer_queue_factory::{new_transfer_queue_factory, QueueFactoryDeps};
use crate::task::interface as ti;

/// Inputs for the queue v2 transfer / timer queues of one shard.
pub struct QueueV2Exec {
    pub shard: Arc<dyn v2::ShardContext>,
    pub env: Arc<dyn v2::QueueEnv>,
    pub processor: Arc<dyn v2::Processor>,
    /// `task.NewExecutorWrapper(current, activeClusterMgr, active, standby(current), logger)`.
    pub transfer_executor: Arc<dyn ti::Executor>,
    pub timer_executor: Arc<dyn ti::Executor>,
}

/// `config.Config` subset read by queuev2.
pub fn queue_v2_config(c: &Config) -> v2::QueueConfig {
    v2::QueueConfig {
        range_size_bits: c.range_size_bits,
        task_critical_retry_count: c.task_critical_retry_count.clone(),
        transfer_task_batch_size: c.transfer_task_batch_size.clone(),
        transfer_task_delete_batch_size: c.transfer_task_delete_batch_size.clone(),
        transfer_processor_max_poll_rps: c.transfer_processor_max_poll_rps.clone(),
        transfer_processor_max_poll_interval: c.transfer_processor_max_poll_interval.clone(),
        transfer_processor_max_poll_interval_jitter_coefficient: c.transfer_processor_max_poll_interval_jitter_coefficient.clone(),
        transfer_processor_update_ack_interval: c.transfer_processor_update_ack_interval.clone(),
        transfer_processor_update_ack_interval_jitter_coefficient: c.transfer_processor_update_ack_interval_jitter_coefficient.clone(),
        transfer_processor_enable_validator: c.transfer_processor_enable_validator.clone(),
        transfer_processor_validation_interval: c.transfer_processor_validation_interval.clone(),
        transfer_processor_cached_queue_reader_mode: c.transfer_processor_cached_queue_reader_mode.clone(),
        transfer_processor_cache_max_size: c.transfer_processor_cache_max_size.clone(),
        transfer_processor_cached_queue_reader_shadow_sample_interval: c.transfer_processor_cached_queue_reader_shadow_sample_interval.clone(),
        timer_task_batch_size: c.timer_task_batch_size.clone(),
        timer_task_delete_batch_size: c.timer_task_delete_batch_size.clone(),
        timer_processor_max_poll_rps: c.timer_processor_max_poll_rps.clone(),
        timer_processor_max_poll_interval: c.timer_processor_max_poll_interval.clone(),
        timer_processor_max_poll_interval_jitter_coefficient: c.timer_processor_max_poll_interval_jitter_coefficient.clone(),
        timer_processor_update_ack_interval: c.timer_processor_update_ack_interval.clone(),
        timer_processor_update_ack_interval_jitter_coefficient: c.timer_processor_update_ack_interval_jitter_coefficient.clone(),
        timer_processor_cached_queue_reader_mode: c.timer_processor_cached_queue_reader_mode.clone(),
        timer_processor_cache_max_size: c.timer_processor_cache_max_size.clone(),
        timer_processor_cached_queue_reader_shadow_sample_interval: c.timer_processor_cached_queue_reader_shadow_sample_interval.clone(),
        timer_processor_cache_prefetch_trigger_window: c.timer_processor_cache_prefetch_trigger_window.clone(),
        timer_processor_cache_time_eviction_window: c.timer_processor_cache_time_eviction_window.clone(),
        timer_processor_cache_min_prefetch_interval: c.timer_processor_cache_min_prefetch_interval.clone(),
        queue_max_pending_task_count: c.queue_max_pending_task_count.clone(),
        queue_processor_poll_backoff_interval: c.queue_processor_poll_backoff_interval.clone(),
        queue_processor_poll_backoff_interval_jitter_coefficient: c.queue_processor_poll_backoff_interval_jitter_coefficient.clone(),
        virtual_slice_force_append_interval: c.virtual_slice_force_append_interval.clone(),
        active_task_redispatch_interval: c.active_task_redispatch_interval.clone(),
        queue_critical_pending_task_count: c.queue_critical_pending_task_count.clone(),
        queue_max_virtual_queue_count: c.queue_max_virtual_queue_count.clone(),
        enable_transfer_queue_v2: c.enable_transfer_queue_v2.clone(),
        enable_timer_queue_v2: c.enable_timer_queue_v2.clone(),
        enable_transfer_queue_v2_pending_task_count_alert: c.enable_transfer_queue_v2_pending_task_count_alert.clone(),
        enable_timer_queue_v2_pending_task_count_alert: c.enable_timer_queue_v2_pending_task_count_alert.clone(),
    }
}

/// `log.Logger` for queuev2 over `tracing`.
#[derive(Clone, Default)]
pub struct TracingV2Logger {
    tags: Vec<(String, String)>,
}

impl TracingV2Logger {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn fmt(&self, msg: &str) -> String {
        if self.tags.is_empty() {
            return msg.to_string();
        }
        let tags: Vec<String> = self.tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
        format!("{msg} {}", tags.join(" "))
    }
}

impl v2::Logger for TracingV2Logger {
    fn debug_on(&self) -> bool {
        tracing::enabled!(tracing::Level::DEBUG)
    }
    fn debug(&self, msg: &str) {
        tracing::debug!("{}", self.fmt(msg));
    }
    fn info(&self, msg: &str) {
        tracing::info!("{}", self.fmt(msg));
    }
    fn warn(&self, msg: &str) {
        tracing::warn!("{}", self.fmt(msg));
    }
    fn error(&self, msg: &str) {
        tracing::error!("{}", self.fmt(msg));
    }
    fn fatal(&self, msg: &str) {
        tracing::error!("{}", self.fmt(msg));
        panic!("{}", self.fmt(msg));
    }
    fn with_tags(&self, tags: &[(&str, String)]) -> Arc<dyn v2::Logger> {
        let mut next = self.clone();
        next.tags.extend(tags.iter().map(|(k, v)| (k.to_string(), v.clone())));
        Arc::new(next)
    }
}

/// The executor wrapper of each category; the v1 branch of the Go factory is taken by the
/// caller before the factory is asked (see [`build_queue_v2`]).
struct FactoryDeps {
    transfer: Arc<dyn ti::Executor>,
    timer: Arc<dyn ti::Executor>,
}

impl QueueFactoryDeps for FactoryDeps {
    fn create_queue_v1(&self, category: v2::HistoryTaskCategory, _shard: &Arc<dyn v2::ShardContext>) -> Arc<dyn Queue> {
        // `build_queue_v2` is only called when queue v2 is enabled for the shard; the v1 queue is
        // built by `queues::build_queue_processors` from the same flag.
        panic!("queue v1 for {category:?} is built by queues::build_queue_processors");
    }
    fn new_executor_wrapper(
        &self,
        category: v2::HistoryTaskCategory,
        _shard: &Arc<dyn v2::ShardContext>,
        _logger: Arc<dyn v2::Logger>,
    ) -> Arc<dyn v2::TaskExecutor> {
        let e = if category == v2::HistoryTaskCategory::TIMER { self.timer.clone() } else { self.transfer.clone() };
        V2ExecutorAdapter::new(e)
    }
}

/// Go `f.isQueueV2Enabled(shard)` for the category.
pub fn is_queue_v2_enabled(exec: &QueueV2Exec, category: HistoryTaskCategory) -> bool {
    let cfg = exec.shard.get_config();
    let id = exec.shard.get_shard_id();
    match category {
        HistoryTaskCategory::Timer => (cfg.enable_timer_queue_v2)(id),
        _ => (cfg.enable_transfer_queue_v2)(id),
    }
}

/// Go `createQueuev2` for the category.
pub fn build_queue_v2(exec: &QueueV2Exec, category: HistoryTaskCategory) -> Arc<dyn QueueProcessor> {
    let deps: Arc<dyn QueueFactoryDeps> =
        Arc::new(FactoryDeps { transfer: exec.transfer_executor.clone(), timer: exec.timer_executor.clone() });
    let factory = match category {
        HistoryTaskCategory::Timer => new_timer_queue_factory(exec.processor.clone(), exec.env.clone(), deps),
        _ => new_transfer_queue_factory(exec.processor.clone(), exec.env.clone(), deps),
    };
    EngineQueueV2::new(factory.create_queue(exec.shard.clone()))
}

/// Adapts `queuev2::interface::Queue` to `engine::runtime::QueueProcessor`.
pub struct EngineQueueV2 {
    inner: Arc<dyn Queue>,
    /// `Start` runs on a task because the engine's `start` is synchronous; `stop` waits for it.
    starting: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl EngineQueueV2 {
    pub fn new(inner: Arc<dyn Queue>) -> Arc<Self> {
        Arc::new(Self { inner, starting: Mutex::new(None) })
    }
}

#[async_trait]
impl QueueProcessor for EngineQueueV2 {
    fn start(&self) {
        let q = self.inner.clone();
        let h = tokio::spawn(async move { q.start().await });
        *self.starting.lock().unwrap() = Some(h);
    }

    async fn stop(&self) {
        let h = self.starting.lock().unwrap().take();
        if let Some(h) = h {
            let _ = h.await;
        }
        self.inner.stop().await;
    }

    async fn handle_action(&self, cluster: &str, action: QueueAction) -> HResult<Vec<String>> {
        // Go queuev2 `HandleAction` returns (nil, nil) for every action: no states, no error.
        let _ = action;
        self.inner
            .handle_action(cluster, &v2::Action)
            .await
            .map_err(HistoryError::Other)?;
        Ok(Vec::new())
    }

    fn notify_new_task(&self, cluster: &str, info: &crate::engine::notify_tasks::NotifyTaskInfo) {
        self.inner.notify_new_task(cluster, &notify_task_info(&info.tasks, info.persistence_error));
    }

    async fn failover_domain(&self, domain_ids: &HashSet<String>) {
        self.inner.failover_domain(domain_ids);
    }

    async fn lock_task_processing(&self) {
        self.inner.lock_task_processing();
    }

    fn unlock_task_processing(&self) {
        self.inner.unlock_task_processing();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_collection::{Collection, InMemoryClient};
    use serde_json::json;

    #[test]
    fn queue_v2_flags_default_true_and_follow_dynamic_config() {
        let client = Arc::new(InMemoryClient::new());
        let config = Config::new(&Collection::new(client.clone()), 4, 4 << 20, false, "host");
        let cfg = queue_v2_config(&config);
        assert!((cfg.enable_transfer_queue_v2)(1));
        assert!((cfg.enable_timer_queue_v2)(1));
        assert_eq!((cfg.transfer_processor_cached_queue_reader_mode)(1), "enabled");
        client.update_value("history.enableTransferQueueV2", json!(false));
        client.update_value("history.enableTimerQueueV2", json!(false));
        assert!(!(cfg.enable_transfer_queue_v2)(1));
        assert!(!(cfg.enable_timer_queue_v2)(1));
    }
}
