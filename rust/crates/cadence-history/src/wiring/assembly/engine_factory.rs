//! group: assembly/engine_factory
//!
//! Fills every `HistoryEngine` field. Real implementations are used wherever one
//! exists in the crate. The mutable-state-backed components (workflow updater,
//! decision handler, execution cache, workflow resetter, task refresher) have no
//! concrete implementation of the engine's local `MutableState` /
//! `ExecutionContext` traits yet; they are explicit `Unwired*` stubs that return
//! an internal error (never a silent no-op). See memory "Unit D1".

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::{admin::*, types::*};

use crate::engine::deps::{
    ExecutionCache, ExecutionContextRef, HResult, HistoryError, HistoryTaskCategory, ReleaseFunc,
    WorkflowExecution,
};
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

pub struct UnwiredUpdater;
#[async_trait]
impl WorkflowUpdater for UnwiredUpdater {
    async fn update<'a>(
        &self,
        _domain_id: &str,
        _execution: &WorkflowExecution,
        _current: bool,
        _now: i64,
        _action: UpdateFn<'a>,
    ) -> HResult<()> {
        unwired("WorkflowUpdater")
    }
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

pub struct UnwiredExecutionCache;
#[async_trait]
impl ExecutionCache for UnwiredExecutionCache {
    async fn get_or_create_workflow_execution(
        &self,
        _domain_id: &str,
        _execution: &WorkflowExecution,
    ) -> HResult<(ExecutionContextRef, ReleaseFunc)> {
        unwired("ExecutionCache")
    }
    async fn get_and_create_workflow_execution(
        &self,
        _domain_id: &str,
        _execution: &WorkflowExecution,
    ) -> HResult<(ExecutionContextRef, ExecutionContextRef, ReleaseFunc, bool)> {
        unwired("ExecutionCache")
    }
    async fn get_or_create_current_workflow_execution(
        &self,
        _domain_id: &str,
        _workflow_id: &str,
    ) -> HResult<ReleaseFunc> {
        unwired("ExecutionCache")
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
    Arc::new(HistoryEngine {
        current_cluster_name: env.cluster.metadata().current_cluster_name.clone(),
        domain_cache: env.domain_cache.clone(),
        cluster_metadata: env.cluster.clone(),
        active_cluster_manager: Arc::new(DomainActiveClusterManager::new(env.domain_cache.clone())),
        time_source: env.clock.clone(),
        updater: Arc::new(UnwiredUpdater),
        queue_processors,
        replication_dlq_handler: replication.dlq_handler,
        replication_ack_manager: replication.ack_manager,
        replication_hydrator: replication.hydrator,
        replicator: replication.replicator,
        decision_handler: Arc::new(UnwiredDecisionHandler),
        shard_clock: Arc::new(ShardClockAdapter(shard.clone())),
        metrics: env.metrics.clone(),
        config: env.config_adapter.clone(),
        execution_cache: Arc::new(UnwiredExecutionCache),
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
    })
}
