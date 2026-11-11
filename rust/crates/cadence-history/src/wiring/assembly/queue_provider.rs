//! Real `QueueExecProvider` (Go `historyEngineFactory.CreateEngine` queue part plus the
//! handler-level `task.NewProcessor` / `task.NewRateLimitedProcessor`).
//!
//! * One shared task processor per service: priority assigner -> weighted round robin scheduler
//!   -> parallel task processor, wrapped by the task rate limiter (Go `handler.Start`).
//! * Per shard: queue shard context, task allocator, redispatcher factory and the active and
//!   per-remote-cluster standby executors of the transfer and timer categories.
//!
//! Known deviations / gaps (see memory/history-queueexec-assembly-20261111.md):
//! * The hierarchical weighted round robin scheduler (Go default) is not ported; the flat weighted
//!   round robin scheduler (Go's non-hierarchical branch) is used and a warning is logged.
//! * The shard `TaskReinjector` slot is not bound (no reinjector implementation exists yet), so
//!   reinjection fails explicitly.
//! * Timer standby workflow loading and other TODO dependencies keep returning explicit errors.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use async_trait::async_trait;
use futures_util::FutureExt;

use super::controller::ShardRegistry;
use super::env::ShardBuildCtx;
use super::queue_allocator_adapters::{new_history_task_allocator, HistoryQueueActiveClusterManager, HistoryQueueDomainCache};
use super::queue_deps::{CleanupDepsImpl, TransferDepsImpl};
use super::queue_deps_standby::{TimerStandbyDepsImpl, TransferStandbyDepsImpl};
use super::queue_deps_timer_active::TimerActiveDepsImpl;
use super::queue_deps_transfer_active::TransferActiveDepsImpl;
use super::queue_exec::{CategoryExecutors, QueueExecCommon, TaskRedispatcherFactory};
use super::queue_factory::QueueExecProvider;
use super::queue_policy::{processor_config, queue_config};
use super::queues::QueueExec;
use super::resender::HistoryResenderImpl;
use super::shard_adapters::RealQueueShardContext;
use super::shard_factory::{DomainActiveClusterManager, ShardClusterInfo, TaskShardContext};
use crate::config::Config;
use crate::queue::deps as q;
use crate::queue::shard_deps::QueueShardContext;
use crate::queue::processor_base::convert_dynamic_config_map_property_to_int_map;
use crate::replication::deps::{ClientBean, DomainCache as ReplDomainCache, HistoryClient, HistoryResender};
use crate::task::deps as t;
use crate::task::interface as ti;
use crate::task::parallel_processor::{
    CommonProcessor, CommonTask, ParallelTaskProcessor, ParallelTaskProcessorOptions, TaskRef,
    WeightedRoundRobinTaskScheduler, WeightedRoundRobinTaskSchedulerOptions,
};
use crate::task::priority_assigner::new_priority_assigner;
use crate::task::processor::{get_domain_priority_weight, new_processor, DomainPriorityKey, SchedulerConfig};
use crate::task::rate_limited_processor::new_rate_limited_processor;
use crate::task::redispatcher::{new_redispatcher, RedispatcherOptions};
use crate::task::task_rate_limiter::new_rate_limiter;
use crate::wiring::persistence::queues::SqlQueueShardContext;
use crate::wiring::resources::metrics::Scope;
use crate::wiring::resources::replication_clients::ReplicationClientBean;

/// Go `config.Config` task scheduler properties used by `task.NewProcessor`.
pub struct SchedulerCfg(pub Arc<Config>);

impl SchedulerConfig for SchedulerCfg {
    fn enable_task_list_aware_task_scheduler_by_domain(&self, domain: &str) -> bool {
        (self.0.enable_task_list_aware_task_scheduler_by_domain)(domain)
    }
    fn task_list_nice_value(&self, domain: &str, task_list: &str) -> i32 {
        (self.0.task_list_nice_value)(domain, task_list) as i32
    }
    fn task_scheduler_domain_round_robin_weights(&self, domain: &str) -> t::Result<HashMap<i32, i32>> {
        let raw: HashMap<String, serde_json::Value> =
            (self.0.task_scheduler_domain_round_robin_weights)(domain).into_iter().collect();
        convert_dynamic_config_map_property_to_int_map(&raw)
            .map(|m| m.into_iter().map(|(k, v)| (k, v as i32)).collect())
            .map_err(t::TaskError::Other)
    }
}

/// `common/task.Task` over a history `task::Task`.
pub struct SchedTask(Arc<dyn ti::Task>);

impl SchedTask {
    pub fn wrap(task: Arc<dyn ti::Task>) -> TaskRef {
        Arc::new(Self(task))
    }
}

impl CommonTask for SchedTask {
    fn execute(&self) -> futures_util::future::BoxFuture<'_, Result<(), t::TaskError>> {
        async move { self.0.execute().await }.boxed()
    }
    fn handle_err(&self, err: t::TaskError) -> Option<t::TaskError> {
        self.0.handle_err(Some(err))
    }
    fn retry_err(&self, err: &t::TaskError) -> bool {
        self.0.retry_err(err)
    }
    fn ack(&self) {
        self.0.ack()
    }
    fn nack(&self, err: Option<t::TaskError>) {
        // task::Task::nack is async (redispatch / DLQ path); run it detached.
        let task = self.0.clone();
        tokio::spawn(async move { task.nack(err).await });
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

/// `task.Scheduler[Task]` (flat weighted round robin) as a history `task::Processor`.
pub struct SchedulerProcessor {
    scheduler: WeightedRoundRobinTaskScheduler<DomainPriorityKey>,
}

#[async_trait]
impl t::Daemon for SchedulerProcessor {
    async fn start(&self) {
        t::Daemon::start(&self.scheduler).await
    }
    async fn stop(&self) {
        t::Daemon::stop(&self.scheduler).await
    }
}

#[async_trait]
impl ti::Processor for SchedulerProcessor {
    async fn submit(&self, task: Arc<dyn ti::Task>) -> t::Result<()> {
        self.scheduler.submit(SchedTask::wrap(task)).await.map_err(|e| t::TaskError::Other(e.to_string()))
    }
    async fn try_submit(&self, task: Arc<dyn ti::Task>) -> t::Result<bool> {
        self.scheduler.try_submit(SchedTask::wrap(task)).map_err(|e| t::TaskError::Other(e.to_string()))
    }
}

fn domain_priority_key(task: &TaskRef) -> DomainPriorityKey {
    match task.as_any().and_then(|a| a.downcast_ref::<SchedTask>()) {
        Some(s) => DomainPriorityKey { domain_id: s.0.get_domain_id().to_string(), priority: s.0.priority() },
        None => DomainPriorityKey { domain_id: String::new(), priority: 0 },
    }
}

/// Builds scheduler + parallel processor (Go `NewParallelTaskProcessor` and
/// `NewWeightedRoundRobinTaskScheduler`) and returns `(task_processor, scheduler)`.
pub fn build_scheduler(
    config: Arc<Config>,
    domains: Arc<dyn t::DomainCache>,
    logger: t::Logger,
) -> (Arc<ParallelTaskProcessor>, Arc<SchedulerProcessor>) {
    if (config.enable_hierarchical_weighted_round_robin_task_scheduler)() {
        tracing::warn!("hierarchical weighted round robin task scheduler is not ported; using the flat weighted round robin scheduler");
    }
    let workers = config.task_scheduler_worker_count.clone();
    let task_processor = Arc::new(ParallelTaskProcessor::new(ParallelTaskProcessorOptions {
        queue_size: 1,
        worker_count: Arc::new(move || workers().max(0) as usize),
        retry_policy: t::create_task_processing_retry_policy(),
    }));
    let cfg = Arc::new(SchedulerCfg(config.clone()));
    let weight_logger = logger;
    let scheduler = WeightedRoundRobinTaskScheduler::new(
        task_processor.clone() as Arc<dyn CommonProcessor>,
        WeightedRoundRobinTaskSchedulerOptions {
            queue_size: (config.task_scheduler_queue_size)().max(1) as usize,
            dispatcher_count: (config.task_scheduler_dispatcher_count)().max(1) as usize,
            task_to_channel_key_fn: Arc::new(domain_priority_key),
            channel_key_to_weight_fn: Arc::new(move |k: &DomainPriorityKey| {
                get_domain_priority_weight(&weight_logger, cfg.as_ref(), domains.as_ref(), k).max(0) as usize
            }),
        },
    );
    (task_processor, Arc::new(SchedulerProcessor { scheduler }))
}

/// `shard.Controller` view used by the task rate limiter: number of owned shards.
pub struct RegistryShardController(Arc<OnceLock<Weak<ShardRegistry>>>);

impl t::ShardController for RegistryShardController {
    fn num_shards(&self) -> usize {
        self.0.get().and_then(Weak::upgrade).map(|r| r.owned_shard_ids().len()).unwrap_or(0)
    }
}

struct Shared {
    processor: Arc<dyn ti::Processor>,
    resender: Arc<dyn HistoryResender>,
}

/// `QueueExecProvider` wiring the real task processor and executors.
pub struct HistoryQueueExecProvider {
    history: Arc<dyn HistoryClient>,
    registry: Arc<OnceLock<Weak<ShardRegistry>>>,
    shared: Mutex<Option<Arc<Shared>>>,
}

impl HistoryQueueExecProvider {
    pub fn new(history: Arc<dyn HistoryClient>) -> Arc<Self> {
        Arc::new(Self { history, registry: Arc::new(OnceLock::new()), shared: Mutex::new(None) })
    }

    /// Must be called once after the `ShardRegistry` has been built (history client calls).
    pub fn bind(&self, registry: &Arc<ShardRegistry>) {
        let _ = self.registry.set(Arc::downgrade(registry));
    }

    /// Stops the service level task processor (Go `handler.Stop`).
    pub async fn stop(&self) {
        let shared = self.shared.lock().unwrap().take();
        if let Some(s) = shared {
            s.processor.stop().await;
        }
    }

    fn shared(&self, ctx: &ShardBuildCtx) -> anyhow::Result<Arc<Shared>> {
        let mut guard = self.shared.lock().unwrap();
        if let Some(s) = guard.as_ref() {
            return Ok(s.clone());
        }
        let env = &ctx.env;
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| anyhow::anyhow!("starting the history task processor needs a tokio runtime: {e}"))?;
        let logger = t::Logger::new();
        let domains: Arc<dyn t::DomainCache> = env.domain_cache.clone();
        let (task_processor, scheduler) = build_scheduler(env.config.clone(), domains.clone(), logger.clone());
        let assigner = new_priority_assigner(
            env.cluster.metadata().current_cluster_name.clone(),
            domains.clone(),
            Arc::new(DomainActiveClusterManager::new(env.domain_cache.clone())),
            logger.clone(),
            env.metrics.clone(),
            env.config_adapter.clone(),
        );
        let base = new_processor(assigner, task_processor, scheduler, logger.clone());
        let limiter = new_rate_limiter(
            logger,
            env.metrics.clone(),
            domains,
            env.config_adapter.clone(),
            Arc::new(RegistryShardController(self.registry.clone())),
        );
        let processor = new_rate_limited_processor(base, limiter);
        handle.block_on(processor.start());

        let bean: Arc<dyn ClientBean> =
            Arc::new(ReplicationClientBean::new(self.history.clone(), env.cluster.metadata().clone(), "cadence-history"));
        let repl_domains: Arc<dyn ReplDomainCache> = env.domain_cache.clone();
        let resender: Arc<dyn HistoryResender> = Arc::new(HistoryResenderImpl::new(repl_domains, bean, self.history.clone()));
        let shared = Arc::new(Shared { processor, resender });
        *guard = Some(shared.clone());
        Ok(shared)
    }
}

impl QueueExecProvider for HistoryQueueExecProvider {
    fn queue_exec(&self, ctx: &ShardBuildCtx) -> anyhow::Result<Option<QueueExec>> {
        let shared = self.shared(ctx)?;
        let env = ctx.env.clone();
        let config = env.config.clone();
        let logger = t::Logger::new();
        let registry = self.registry.get().and_then(Weak::upgrade);

        let task_shard = TaskShardContext::new(env.clone(), ctx.shard.clone());
        let task_shard_dyn: Arc<dyn t::ShardContext> = task_shard;
        let tasks = Arc::new(SqlQueueShardContext::new(
            env.persistence.executions.clone(),
            ctx.shard.shard_id(),
            config.range_size_bits,
            config.enable_debug_mode,
        ));
        let queue_shard = RealQueueShardContext::new(
            ctx.shard.clone(),
            tasks,
            Arc::new(ShardClusterInfo::from_env(&env)),
            processor_config(&config),
            queue_config(&config),
            Arc::new(HistoryQueueDomainCache::new(env.domain_cache.clone())),
            Arc::new(HistoryQueueActiveClusterManager::new(env.domain_cache.clone())),
        );
        let allocator_factory: Arc<dyn Fn() -> Arc<dyn crate::queue::task_allocator::TaskAllocator> + Send + Sync> = {
            let cluster = env.cluster.metadata().current_cluster_name.clone();
            let domains = env.domain_cache.clone();
            Arc::new(move || new_history_task_allocator(cluster.clone(), domains.clone()) as Arc<dyn crate::queue::task_allocator::TaskAllocator>)
        };

        let redispatcher_factory: TaskRedispatcherFactory = {
            let processor = shared.processor.clone();
            let clock = env.clock.clone();
            let registry = env.metrics_registry.clone();
            let config = config.clone();
            let logger = logger.clone();
            Arc::new(move |queue_type: ti::QueueType| {
                let interval = match queue_type {
                    ti::QueueType::ActiveTransfer | ti::QueueType::ActiveTimer => config.active_task_redispatch_interval.clone(),
                    _ => config.standby_task_redispatch_interval.clone(),
                };
                new_redispatcher(
                    processor.clone(),
                    clock.clone() as Arc<dyn t::TimeSource>,
                    &RedispatcherOptions { task_redispatch_interval: interval },
                    logger.clone(),
                    Arc::new(Scope::new(registry.clone(), "HistoryTaskRedispatcher", Vec::new())) as Arc<dyn t::MetricsScope>,
                )
            })
        };
        let critical = config.task_critical_retry_count.clone();
        let critical_retry_count: t::IntPropertyFn = Arc::new(move || critical() as i32);

        let remote_clusters = queue_shard.remote_cluster_names();

        // Transfer executors.
        let transfer_deps = TransferDepsImpl::new(env.clone());
        let active_deps = TransferActiveDepsImpl::new(env.clone(), ctx.cache.clone());
        if let Some(r) = &registry {
            active_deps.bind(r);
        }
        let transfer_active = crate::task::transfer_active_task_executor::new_transfer_active_task_executor(
            crate::task::transfer_task_executor_base::new_transfer_task_executor_base(
                task_shard_dyn.clone(),
                transfer_deps.clone(),
                logger.clone(),
            ),
            active_deps,
        );
        let mut transfer_standby = HashMap::new();
        for cluster in &remote_clusters {
            transfer_standby.insert(
                cluster.clone(),
                crate::task::transfer_standby_task_executor::new_transfer_standby_task_executor(
                    crate::task::transfer_task_executor_base::new_transfer_task_executor_base(
                        task_shard_dyn.clone(),
                        transfer_deps.clone(),
                        logger.clone(),
                    ),
                    cluster.clone(),
                    TransferStandbyDepsImpl::new(env.clone(), ctx.shard.clone(), ctx.cache.clone(), shared.resender.clone()),
                ),
            );
        }

        // Timer executors.
        let cleanup = CleanupDepsImpl::new(env.clone(), ctx.cache.clone());
        let timer_base = || {
            crate::task::timer_task_executor_base::new_timer_task_executor_base(
                task_shard_dyn.clone(),
                cleanup.clone(),
                logger.clone(),
                env.metrics.clone(),
            )
        };
        let timer_active = crate::task::timer_active_task_executor::new_timer_active_task_executor(
            timer_base(),
            TimerActiveDepsImpl::new(env.clone(), ctx.cache.clone()),
        );
        let mut timer_standby = HashMap::new();
        for cluster in &remote_clusters {
            timer_standby.insert(
                cluster.clone(),
                crate::task::timer_standby_task_executor::new_timer_standby_task_executor(
                    timer_base(),
                    cluster.clone(),
                    TimerStandbyDepsImpl::new(env.clone(), ctx.shard.clone(), shared.resender.clone()),
                ),
            );
        }

        let common = QueueExecCommon {
            queue_shard,
            task_shard: task_shard_dyn,
            task_processor: shared.processor.clone(),
            task_allocator_factory: allocator_factory,
            metrics: Arc::new(Scope::new(env.metrics_registry.clone(), "HistoryQueueProcessor", Vec::new())) as Arc<dyn q::MetricsScope>,
            redispatcher_factory,
            critical_retry_count,
            logger,
        };
        Ok(Some(QueueExec {
            common,
            transfer: CategoryExecutors { active: transfer_active, standby: transfer_standby },
            timer: CategoryExecutors { active: timer_active, standby: timer_standby },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_collection::{Collection, InMemoryClient};
    use serde_json::json;

    fn config(client: &Arc<InMemoryClient>) -> Arc<Config> {
        Arc::new(Config::new(&Collection::new(client.clone()), 4, 4 << 20, false, "host"))
    }

    #[test]
    fn scheduler_config_tracks_dynamic_domain_weights() {
        let client = Arc::new(InMemoryClient::new());
        let cfg = SchedulerCfg(config(&client));
        let weights = cfg.task_scheduler_domain_round_robin_weights("d").unwrap();
        assert_eq!(weights.get(&1), Some(&500));
        client.update_value("history.taskSchedulerDomainRoundRobinWeight", json!({"1": 7}));
        let weights = cfg.task_scheduler_domain_round_robin_weights("d").unwrap();
        assert_eq!(weights.get(&1), Some(&7));
        assert!(!weights.contains_key(&9));
        client.update_value("history.taskSchedulerDomainRoundRobinWeight", json!({"x": 7}));
        assert!(cfg.task_scheduler_domain_round_robin_weights("d").is_err());
    }

    #[test]
    fn unbound_registry_has_no_shards() {
        let ctl = RegistryShardController(Arc::new(OnceLock::new()));
        assert_eq!(t::ShardController::num_shards(&ctl), 0);
    }

    struct NoDomains;
    impl t::DomainCache for NoDomains {
        fn get_domain_by_id(&self, _id: &str) -> t::Result<Arc<t::DomainCacheEntry>> {
            Err(t::TaskError::EntityNotExists("none".into()))
        }
        fn get_domain(&self, _name: &str) -> t::Result<Arc<t::DomainCacheEntry>> {
            Err(t::TaskError::EntityNotExists("none".into()))
        }
        fn get_domain_name(&self, _id: &str) -> t::Result<String> {
            Err(t::TaskError::EntityNotExists("none".into()))
        }
    }

    #[tokio::test]
    async fn scheduler_constructs_and_start_stop_is_idempotent() {
        let client = Arc::new(InMemoryClient::new());
        let (processor, scheduler) = build_scheduler(config(&client), Arc::new(NoDomains), t::Logger::new());
        t::Daemon::start(processor.as_ref()).await;
        t::Daemon::start(scheduler.as_ref()).await;
        t::Daemon::start(scheduler.as_ref()).await;
        t::Daemon::stop(scheduler.as_ref()).await;
        t::Daemon::stop(scheduler.as_ref()).await;
        t::Daemon::stop(processor.as_ref()).await;
    }
}
