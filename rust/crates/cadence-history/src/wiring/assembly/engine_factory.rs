//! group: assembly/engine_factory
//!
//! Fills every `HistoryEngine` field. Real implementations are used wherever one
//! exists in the crate. The execution cache and workflow updater are real
//! (`engine_cache`, over `EngineMutableState`), as are the decision handler and
//! workflow resetter (the former `Unwired*` placeholders were deleted).

use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::deps::{DomainCache, HResult, HistoryError, HistoryTaskCategory, TimeSource, WorkflowExecution};
use crate::wiring::assembly::engine_cache::{EngineCore, EngineExecutionCache, EngineWorkflowUpdater};
use crate::wiring::assembly::engine_decision::EngineDecisionHandler;
use crate::wiring::assembly::engine_state::StateParams;
use crate::engine::runtime::*;
use crate::events::{Notification, Notifier, WorkflowIdentifier};
use crate::shard_context::ShardContext;
use crate::wiring::assembly::env::{HistoryEnv, ReplicationParts};
use crate::wiring::assembly::shard_factory::ShardClockAdapter;
use crate::wiring::persistence::shard_events::{SqlCurrentExecutionReader, SqlHistoryReader};

/// `HistoryEventNotifier` over the shard's `events::Notifier`.
pub struct NotifierAdapter {
    notifier: Arc<Notifier>,
}

impl NotifierAdapter {
    pub fn new(notifier: Arc<Notifier>) -> Self {
        Self { notifier }
    }
}

fn identifier(domain_id: &str, execution: &WorkflowExecution) -> WorkflowIdentifier {
    WorkflowIdentifier {
        domain_id: domain_id.to_string(),
        workflow_id: execution.workflow_id.clone(),
        run_id: execution.run_id.clone(),
    }
}

impl HistoryEventNotifier for NotifierAdapter {
    fn watch_history_event(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(String, tokio::sync::mpsc::UnboundedReceiver<HistoryNotification>)> {
        let (subscriber, mut rx) = self
            .notifier
            .watch_history_event(identifier(domain_id, execution))
            .map_err(|e| HistoryError::internal(e.to_string()))?;
        let (tx, out) = tokio::sync::mpsc::unbounded_channel();
        // Ends when `unwatch_history_event` drops the notifier's sender.
        tokio::spawn(async move {
            while let Some(n) = rx.recv().await {
                let converted = HistoryNotification {
                    domain_id: n.id.domain_id.clone(),
                    workflow_id: n.id.workflow_id.clone(),
                    run_id: n.id.run_id.clone(),
                    last_first_event_id: n.last_first_event_id,
                    next_event_id: n.next_event_id,
                    previous_started_event_id: n.previous_started_event_id,
                    workflow_state: n.workflow_state,
                    workflow_close_state: n.workflow_close_state,
                    version_histories: n.version_histories.clone().unwrap_or_default(),
                };
                if tx.send(converted).is_err() {
                    break;
                }
            }
        });
        Ok((subscriber, out))
    }

    fn unwatch_history_event(&self, domain_id: &str, execution: &WorkflowExecution, subscriber: &str) {
        let _ = self.notifier.unwatch_history_event(&identifier(domain_id, execution), subscriber);
    }

    fn notify_new_history_event(&self, e: HistoryNotification) {
        self.notifier.dispatch_history_event_notification(Arc::new(Notification {
            id: WorkflowIdentifier { domain_id: e.domain_id, workflow_id: e.workflow_id, run_id: e.run_id },
            last_first_event_id: e.last_first_event_id,
            next_event_id: e.next_event_id,
            previous_started_event_id: e.previous_started_event_id,
            timestamp_unix_millis: 0,
            workflow_state: e.workflow_state,
            workflow_close_state: e.workflow_close_state,
            version_histories: Some(e.version_histories),
        }));
    }
}

/// Builds the shard's execution core and cache. They are created before the
/// queue processors so the transfer / timer executors share the engine's cache.
pub fn build_core(env: &Arc<HistoryEnv>, shard: &Arc<ShardContext>) -> (Arc<EngineCore>, Arc<EngineExecutionCache>) {
    let core = Arc::new(EngineCore {
        metrics: env.metrics.clone() as Arc<dyn crate::task::deps::MetricsClient>,
        cached_queue_reader_shadow: {
            let cfg = env.config.clone();
            let shard_id = shard.shard_id();
            Arc::new(move || {
                (
                    (cfg.timer_processor_cached_queue_reader_mode)(shard_id) == "shadow",
                    (cfg.transfer_processor_cached_queue_reader_mode)(shard_id) == "shadow",
                )
            })
        },
        engine_slot: Arc::new(std::sync::OnceLock::new()),
        semaphore_metadata: env.semaphore_metadata.clone(),
        cluster_metadata: env.cluster.clone() as Arc<dyn crate::engine::deps::ClusterMetadata>,
        enable_replication_task_generation: env.config.enable_replication_task_generation.clone(),
        strong_idempotency_sanity_check: env.config.enable_strong_idempotency_sanity_check.clone(),
        transaction_size_limit: env.config.transaction_size_limit.clone(),
        advanced_visibility_writing: {
            // Go `AdvancedVisibilityWritingMode() != "off"`: advanced store configured and written to.
            let cfg = env.config.clone();
            Arc::new(move || cfg.is_advanced_vis_config_exist && !matches!((cfg.write_visibility_store_name)().as_str(), "db" | "off"))
        },
        active_cluster_manager: env.active_cluster_manager.clone(),
        remote_admin_clients: Arc::new(crate::wiring::resources::replication_clients::RemoteAdminClients::new(
            env.cluster.metadata().clone(),
            "cadence-history",
        )),
        shard: shard.clone(),
        executions: env.persistence.executions.clone(),
        domains: env.domain_cache.clone() as Arc<dyn DomainCache>,
        clock: env.clock.clone() as Arc<dyn TimeSource>,
        sticky_ttl_nanos: {
            let cfg = env.config.clone();
            Arc::new(move |domain: &str| (cfg.sticky_ttl)(domain).as_nanos() as i64)
        },
        current_cluster: env.cluster.metadata().current_cluster_name.clone(),
        history_v2: env.persistence.history_v2.clone(),
        state_params: {
            let cfg = env.config.clone();
            let domains = env.domain_cache.clone();
            let history_nodes = env.persistence.history_nodes.clone();
            let shard_id = shard.shard_id();
            let cluster = env.cluster.clone();
            let metrics = env.metrics.clone();
            Arc::new(move |domain_id: &str| {
                let mut p = StateParams {
                    history_nodes: Some(history_nodes.clone()),
                    shard_id,
                    max_buffered_events_batch: (cfg.maximum_buffered_events_batch)(),
                    close_policy: Some(super::engine_state::ClosePolicy {
                        cluster: cluster.clone() as Arc<dyn crate::engine::deps::ClusterMetadata>,
                        replication_enabled: cfg.enable_replication_task_generation.clone(),
                    }),
                    data_inconsistency: crate::execution::mutable_state_builder::DataInconsistencyHook(Some({
                        let m = metrics.clone();
                        Arc::new(move || {
                            crate::task::deps::MetricsClient::inc_counter(&*m, "WorkflowContextScope", "DataInconsistentCounter")
                        })
                    })),
                    ..StateParams::default()
                };
                if let Ok(entry) = crate::task::deps::DomainCache::get_domain_by_id(&*domains, domain_id) {
                    p.retention_days = entry.retention_days;
                    p.failover_version = entry.failover_version;
                    let name = entry.info.name.clone();
                    p.normal_decision_s2s_timeout_nanos =
                        (cfg.normal_decision_schedule_to_start_timeout)(&name).as_nanos() as i64;
                    p.normal_decision_s2s_max_attempts = (cfg.normal_decision_schedule_to_start_max_attempts)(&name);
                    p.checksum_gen_probability = (cfg.mutable_state_checksum_gen_probability)(&name);
                    p.checksum_verify_probability = (cfg.mutable_state_checksum_verify_probability)(&name);
                    p.checksum_invalidate_before_secs = (cfg.mutable_state_checksum_invalidate_before)() as i64;
                    p.enable_corruption_auto_repair = (cfg.enable_corruption_auto_repair)(&name);
                    p.corruption_repair_timeout = (cfg.corruption_repair_timeout)(&name);
                    p.require_checksum_match_after_rebuild_repair =
                        (cfg.require_checksum_match_after_rebuild_repair)(&name);
                    p.deletion_jitter_range = (cfg.workflow_deletion_jitter_range)(&name);
                }
                p
            })
        },
    });
    let cache = EngineExecutionCache::new(core.clone(), env.config.clone());
    (core, cache)
}

/// Builds the engine of one shard.
pub fn build_engine(
    env: &Arc<HistoryEnv>,
    shard: &Arc<ShardContext>,
    core: Arc<EngineCore>,
    cache: Arc<EngineExecutionCache>,
    queue_processors: HashMap<HistoryTaskCategory, Arc<dyn QueueProcessor>>,
    replication: ReplicationParts,
) -> Arc<HistoryEngine> {
    let start_host = super::engine_start::EngineStartHost::new(core.clone(), env.config.clone());
    let dlq_processor = build_dlq_processor(env, shard);
    let failover_host = super::engine_start::EngineFailoverHost::new(
        shard.clone(),
        env.domain_cache.clone(),
        env.persistence.executions.clone(),
        Some(dlq_processor.clone()),
    );
    let decision_handler = Arc::new(EngineDecisionHandler::new(cache.clone(), core.clone(), env.config.clone()));
    let workflow_resetter =
        Arc::new(super::engine_resetter::EngineWorkflowResetter::new(core.clone(), cache.clone()));
    Arc::new(HistoryEngine {
        current_cluster_name: env.cluster.metadata().current_cluster_name.clone(),
        domain_cache: env.domain_cache.clone(),
        cluster_metadata: env.cluster.clone(),
        active_cluster_manager: env.active_cluster_manager.clone(),
        time_source: env.clock.clone(),
        updater: Arc::new(EngineWorkflowUpdater::new(cache.clone(), core)),
        start_host,
        failover_host,
        lifecycle_host: Arc::new(EngineLifecycleParts {
            graceful_failover: env.config.enable_graceful_failover.clone(),
            dlq_processor,
            dlq_daemon: replication.dlq_daemon.clone(),
            metrics_emitter: replication.metrics_emitter.clone(),
            task_processors: replication.task_processors.clone(),
            marker_notifier: super::failover::build_marker_notifier(
                shard.clone(),
                env.domain_cache.clone(),
                env.cluster.metadata().current_cluster_name.clone(),
                env.config_adapter.clone(),
                env.failover_coordinator.clone(),
                env.metrics.clone(),
            ),
        }),
        lifecycle_cancel: std::sync::Mutex::new(None),
        queue_processors,
        replication_dlq_handler: replication.dlq_handler,
        replication_ack_manager: replication.ack_manager,
        replication_hydrator: replication.hydrator,
        replicator: replication.replicator,
        decision_handler,
        shard_clock: Arc::new(ShardClockAdapter(shard.clone())),
        metrics: env.metrics.clone(),
        config: env.config_adapter.clone(),
        execution_cache: cache,
        // Go wraps the raw client with retries; the gRPC client is used for both.
        matching_client: env.query_matching.clone(),
        raw_matching_client: env.query_matching.clone(),
        client_checker: env.client_checker.clone(),
        workflow_resetter,
        history_reader: Arc::new(SqlHistoryReader::new(shard.shard_id(), env.persistence.history_nodes.clone())),
        current_execution_reader: Arc::new(SqlCurrentExecutionReader::new(
            env.persistence.executions.clone(),
            env.config.number_of_shards as u32,
        )),
        history_event_notifier: Arc::new(NotifierAdapter::new(Notifier::new())),
        task_refresher: Arc::new(super::engine_refresher::EngineTaskRefresher::new(env.domain_cache.clone())),
        events_reapplier: Arc::new(EngineEventsReapplier),
        replication_task_store: replication.task_store.clone(),
    })
}

/// Go `ndc.eventsReapplierImpl` over the engine mutable state.
struct EngineEventsReapplier;

#[async_trait::async_trait]
impl crate::engine::runtime::EventsReapplier for EngineEventsReapplier {
    async fn reapply_events(
        &self,
        state: crate::engine::deps::MutableStateRef,
        events: &[cadence_api::service_types::HistoryEvent],
        run_id: &str,
    ) -> HResult<Vec<cadence_api::service_types::HistoryEvent>> {
        reapply_signal_events(&*state, events, run_id)
    }
}

fn reapply_signal_events(
    state: &dyn crate::engine::deps::MutableState,
    events: &[cadence_api::service_types::HistoryEvent],
    run_id: &str,
) -> HResult<Vec<cadence_api::service_types::HistoryEvent>> {
    use crate::engine::reapply_events::event_reapplied_id;
    let mut reapplied = Vec::new();
    for event in events {
        if event.event_type == Some(cadence_api::enums::EventType::WorkflowExecutionSignaled) {
            if state.is_resource_duplicated(&event_reapplied_id(run_id, event.event_id, event.version)) {
                continue;
            }
            reapplied.push(event.clone());
        }
    }
    if reapplied.is_empty() {
        return Ok(Vec::new());
    }
    if !state.is_workflow_execution_running() {
        return Err(HistoryError::internal("unable to reapply events to closed workflow."));
    }
    for event in &reapplied {
        let signal = event.workflow_execution_signaled_event_attributes.clone().unwrap_or_default();
        // Do not set requestID for requests reapplied, because they have already been applied previously
        state.add_workflow_execution_signaled(&signal.signal_name, &signal.input, &signal.identity, "")?;
        state.update_duplicated_resource(&event_reapplied_id(run_id, event.event_id, event.version));
    }
    Ok(reapplied)
}

/// Go `taskdlq.NewProcessorFromShard(shard, 100, config.HistoryTaskDLQProcessorInterval, ...)`.
/// The shard's task reinjector is bound to `ShardTaskReinjector`, as for the queue processors.
fn build_dlq_processor(env: &Arc<HistoryEnv>, shard: &Arc<ShardContext>) -> Arc<dyn crate::taskdlq::Processor> {
    let task_shard = crate::wiring::assembly::shard_factory::TaskShardContext::new(env.clone(), shard.clone());
    task_shard.reinjector.bind(crate::wiring::assembly::shard_factory::ShardTaskReinjector::new(
        shard.clone(),
        env.domain_cache.clone(),
        env.persistence.executions.clone(),
        env.cluster.metadata().current_cluster_name.clone(),
    ));
    let config = &env.config;
    Arc::new(crate::taskdlq::new_processor_from_shard(
        task_shard,
        100,
        config.history_task_dlq_processor_interval.clone(),
        config.history_task_dlq_processor_failover_jitter_max_delay.clone(),
        config.history_task_dlq_mode.clone(),
        config.history_task_dlq_processor_enabled.clone(),
        env.dlq_host_limiter.clone(),
    ))
}

/// `EngineLifecycleHost`: replication DLQ handler, metrics emitter, task processors and the
/// history task DLQ processor are the real daemons.
struct EngineLifecycleParts {
    dlq_processor: Arc<dyn crate::taskdlq::Processor>,
    graceful_failover: crate::config_collection::Prop<bool>,
    dlq_daemon: Arc<dyn crate::replication::deps::Daemon>,
    metrics_emitter: Arc<dyn crate::replication::deps::Daemon>,
    task_processors: Vec<Arc<dyn crate::replication::deps::Daemon>>,
    marker_notifier: Arc<dyn crate::failover::MarkerNotifier>,
}

#[async_trait::async_trait]
impl crate::engine::lifecycle::EngineLifecycleHost for EngineLifecycleParts {
    async fn start_dlq_processor(&self, ctx: tokio_util::sync::CancellationToken) -> HResult<()> {
        self.dlq_processor.start(ctx).await.map_err(|e| HistoryError::internal(e.to_string()))
    }
    async fn stop_dlq_processor(&self, ctx: tokio_util::sync::CancellationToken) -> HResult<()> {
        self.dlq_processor.stop(ctx).await.map_err(|e| HistoryError::internal(e.to_string()))
    }
    fn start_replication_dlq_handler(&self) { self.dlq_daemon.start(); }
    fn stop_replication_dlq_handler(&self) { self.dlq_daemon.stop(); }
    fn start_replication_metrics_emitter(&self) { self.metrics_emitter.start(); }
    fn stop_replication_metrics_emitter(&self) { self.metrics_emitter.stop(); }
    fn start_replication_task_processors(&self) { self.task_processors.iter().for_each(|p| p.start()); }
    fn stop_replication_task_processors(&self) { self.task_processors.iter().for_each(|p| p.stop()); }
    fn enable_graceful_failover(&self) -> bool { (self.graceful_failover)() }
    fn start_failover_marker_notifier(&self) { crate::failover::MarkerNotifier::start(self.marker_notifier.clone()); }
    fn stop_failover_marker_notifier(&self) { crate::failover::MarkerNotifier::stop(&*self.marker_notifier); }
}

#[cfg(test)]
mod reapplier_tests {
    use super::*;
    use crate::engine::deps::{DomainCacheEntry, DomainInfo};
    use crate::engine::deps_ms::MutableState;
    use crate::wiring::assembly::engine_state::EngineMutableState;
    use cadence_api::enums::EventType;
    use cadence_api::service_types::{HistoryEvent, WorkflowExecutionSignaledEventAttributes};
    use cadence_persistence::history_contract as pc;

    fn state() -> EngineMutableState {
        let domain = DomainCacheEntry {
            info: DomainInfo { id: "d".into(), name: "n".into(), status: 0 },
            ..Default::default()
        };
        let mut snapshot = pc::WorkflowSnapshot::default();
        snapshot.execution_info.domain_id = "d".into();
        snapshot.execution_info.workflow_id = "wf".into();
        snapshot.execution_info.run_id = "run".into();
        snapshot.execution_info.next_event_id = 5;
        EngineMutableState::from_snapshot(snapshot, domain, 0, Arc::new(|| 1_700_000_000_000_000_000)).unwrap()
    }

    fn signal(id: i64) -> HistoryEvent {
        HistoryEvent {
            event_id: id,
            version: 7,
            event_type: Some(EventType::WorkflowExecutionSignaled),
            workflow_execution_signaled_event_attributes: Some(WorkflowExecutionSignaledEventAttributes {
                signal_name: "sig".into(),
                identity: "i".into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn reapplies_signal_once_and_dedups() {
        let s = state();
        let out = reapply_signal_events(&s, &[signal(2)], "run").unwrap();
        assert_eq!(out.len(), 1);
        assert!(s.is_resource_duplicated(&crate::engine::reapply_events::event_reapplied_id("run", 2, 7)));
        // second delivery of the same event is skipped
        assert!(reapply_signal_events(&s, &[signal(2)], "run").unwrap().is_empty());
    }

    #[test]
    fn ignores_non_signal_events() {
        let s = state();
        let ev = HistoryEvent { event_id: 3, event_type: Some(EventType::DecisionTaskScheduled), ..Default::default() };
        assert!(reapply_signal_events(&s, &[ev], "run").unwrap().is_empty());
    }
}
