//! group: assembly/engine_factory
//!
//! Fills every `HistoryEngine` field. Real implementations are used wherever one
//! exists in the crate. The execution cache and workflow updater are real
//! (`engine_cache`, over `EngineMutableState`). The decision handler, workflow
//! resetter and task refresher still need the mutable-state builder / history
//! builder port; they are explicit `Unwired*` stubs that return an internal
//! error (never a silent no-op). See memory "Unit D5".

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crate::engine::types::{
    HistoryRespondDecisionTaskCompletedRequest, HistoryRespondDecisionTaskCompletedResponse,
    HistoryRespondDecisionTaskFailedRequest, RecordDecisionTaskStartedRequest,
    RecordDecisionTaskStartedResponse, ScheduleDecisionTaskRequest,
};

use crate::engine::deps::{DomainCache, HResult, HistoryError, HistoryTaskCategory, TimeSource, WorkflowExecution};
use crate::wiring::assembly::engine_cache::{EngineCore, EngineExecutionCache, EngineWorkflowUpdater};
use crate::wiring::assembly::engine_decision::EngineDecisionHandler;
use crate::wiring::assembly::engine_state::StateParams;
use crate::engine::runtime::*;
use crate::events::{Notification, Notifier, WorkflowIdentifier};
use crate::shard_context::ShardContext;
use crate::wiring::assembly::env::{HistoryEnv, ReplicationParts};
use crate::wiring::assembly::shard_factory::{DomainActiveClusterManager, ShardClockAdapter};
use crate::wiring::persistence::shard_events::{SqlCurrentExecutionReader, SqlHistoryReader};

fn unwired<T>(what: &str) -> HResult<T> {
    Err(HistoryError::internal(format!(
        "{what} is not wired: the mutable-state engine port is not linked yet"
    )))
}

pub struct UnwiredDecisionHandler;
#[async_trait]
impl DecisionHandler for UnwiredDecisionHandler {
    async fn handle_decision_task_started(
        &self,
        _r: &RecordDecisionTaskStartedRequest,
    ) -> HResult<RecordDecisionTaskStartedResponse> {
        unwired("DecisionHandler")
    }
    async fn handle_decision_task_completed(
        &self,
        _r: &HistoryRespondDecisionTaskCompletedRequest,
    ) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        unwired("DecisionHandler")
    }
    async fn handle_decision_task_failed(
        &self,
        _r: &HistoryRespondDecisionTaskFailedRequest,
    ) -> HResult<()> {
        unwired("DecisionHandler")
    }
    async fn handle_decision_task_scheduled(&self, _r: &ScheduleDecisionTaskRequest) -> HResult<()> {
        unwired("DecisionHandler")
    }
}

pub struct UnwiredWorkflowResetter;
#[async_trait]
impl WorkflowResetter for UnwiredWorkflowResetter {
    async fn reset_workflow(&self, _request: ResetWorkflow) -> HResult<()> {
        unwired("WorkflowResetter")
    }
}

pub struct UnwiredTaskRefresher;
#[async_trait]
impl MutableStateTaskRefresher for UnwiredTaskRefresher {
    async fn refresh_tasks(&self, _start_timestamp: i64, _state: crate::engine::deps::MutableStateRef) -> HResult<()> {
        unwired("MutableStateTaskRefresher")
    }
}

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

/// Builds the engine of one shard.
pub fn build_engine(
    env: &Arc<HistoryEnv>,
    shard: &Arc<ShardContext>,
    queue_processors: HashMap<HistoryTaskCategory, Arc<dyn QueueProcessor>>,
    replication: ReplicationParts,
) -> Arc<HistoryEngine> {
    let core = Arc::new(EngineCore {
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
            Arc::new(move |domain_id: &str| {
                let mut p = StateParams {
                    history_nodes: Some(history_nodes.clone()),
                    shard_id,
                    ..StateParams::default()
                };
                if let Ok(entry) = crate::task::deps::DomainCache::get_domain_by_id(&*domains, domain_id) {
                    p.retention_days = entry.retention_days;
                    p.failover_version = entry.failover_version;
                    let name = entry.info.name.clone();
                    p.normal_decision_s2s_timeout_nanos =
                        (cfg.normal_decision_schedule_to_start_timeout)(&name).as_nanos() as i64;
                    p.normal_decision_s2s_max_attempts = (cfg.normal_decision_schedule_to_start_max_attempts)(&name);
                    p.deletion_jitter_range = (cfg.workflow_deletion_jitter_range)(&name);
                }
                p
            })
        },
    });
    let start_host = super::engine_start::EngineStartHost::new(core.clone(), env.config.clone());
    let failover_host = super::engine_start::EngineFailoverHost::new(shard.clone(), env.domain_cache.clone());
    let cache = EngineExecutionCache::new(core.clone());
    let decision_handler = Arc::new(EngineDecisionHandler::new(cache.clone(), core.clone()));
    Arc::new(HistoryEngine {
        current_cluster_name: env.cluster.metadata().current_cluster_name.clone(),
        domain_cache: env.domain_cache.clone(),
        cluster_metadata: env.cluster.clone(),
        active_cluster_manager: Arc::new(DomainActiveClusterManager::new(env.domain_cache.clone())),
        time_source: env.clock.clone(),
        updater: Arc::new(EngineWorkflowUpdater::new(cache.clone(), core)),
        start_host,
        failover_host,
        lifecycle_host: Arc::new(EngineLifecycleParts {
            graceful_failover: env.config.enable_graceful_failover.clone(),
            dlq_daemon: replication.dlq_daemon.clone(),
            metrics_emitter: replication.metrics_emitter.clone(),
            task_processors: replication.task_processors.clone(),
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
        workflow_resetter: Arc::new(UnwiredWorkflowResetter),
        history_reader: Arc::new(SqlHistoryReader::new(shard.shard_id(), env.persistence.history_nodes.clone())),
        current_execution_reader: Arc::new(SqlCurrentExecutionReader::new(
            env.persistence.executions.clone(),
            env.config.number_of_shards as u32,
        )),
        history_event_notifier: Arc::new(NotifierAdapter::new(Notifier::new())),
        task_refresher: Arc::new(UnwiredTaskRefresher),
        events_reapplier: Arc::new(UnwiredEventsReapplier),
        replication_task_store: Arc::new(NoopReplicationTaskStore),
    })
}

struct UnwiredEventsReapplier;

#[async_trait::async_trait]
impl crate::engine::runtime::EventsReapplier for UnwiredEventsReapplier {
    async fn reapply_events(
        &self,
        _state: crate::engine::deps::MutableStateRef,
        _events: &[cadence_api::service_types::HistoryEvent],
        _run_id: &str,
    ) -> HResult<Vec<cadence_api::service_types::HistoryEvent>> {
        Err(HistoryError::internal("ndc events reapplier is not wired"))
    }
}

struct NoopReplicationTaskStore;

impl crate::engine::runtime::ReplicationTaskStore for NoopReplicationTaskStore {
    fn put(&self, _task: cadence_api::types::ReplicationTask) {}
}

/// `EngineLifecycleHost`: replication DLQ handler, metrics emitter and task processors are the
/// real daemons built by `ReplicationFactoryImpl`. Still no-ops (recorded in memory): the history
/// task DLQ processor (no Rust impl) and the failover marker notifier (`MarkerNotifierImpl` exists
/// but the failover coordinator is not available in `HistoryEnv`).
struct EngineLifecycleParts {
    graceful_failover: crate::config_collection::Prop<bool>,
    dlq_daemon: Arc<dyn crate::replication::deps::Daemon>,
    metrics_emitter: Arc<dyn crate::replication::deps::Daemon>,
    task_processors: Vec<Arc<dyn crate::replication::deps::Daemon>>,
}

#[async_trait::async_trait]
impl crate::engine::lifecycle::EngineLifecycleHost for EngineLifecycleParts {
    async fn start_dlq_processor(&self, _ctx: tokio_util::sync::CancellationToken) -> HResult<()> { Ok(()) }
    async fn stop_dlq_processor(&self, _ctx: tokio_util::sync::CancellationToken) -> HResult<()> { Ok(()) }
    fn start_replication_dlq_handler(&self) { self.dlq_daemon.start(); }
    fn stop_replication_dlq_handler(&self) { self.dlq_daemon.stop(); }
    fn start_replication_metrics_emitter(&self) { self.metrics_emitter.start(); }
    fn stop_replication_metrics_emitter(&self) { self.metrics_emitter.stop(); }
    fn start_replication_task_processors(&self) { self.task_processors.iter().for_each(|p| p.start()); }
    fn stop_replication_task_processors(&self) { self.task_processors.iter().for_each(|p| p.stop()); }
    fn enable_graceful_failover(&self) -> bool { (self.graceful_failover)() }
    fn start_failover_marker_notifier(&self) {}
    fn stop_failover_marker_notifier(&self) {}
}
