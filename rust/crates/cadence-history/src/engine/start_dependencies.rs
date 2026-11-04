//! TODO(link): persistence/execution contracts for the start transaction port.
use super::{
    deps::*, runtime::HistoryEngine, start_workflow_execution::StartValidationConfig, types::*,
};
use async_trait::async_trait;
use cadence_api::{service_types::HistoryEvent, types::*};
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub struct PersistedStartBlob {
    pub branch_token: Vec<u8>,
    pub data: Vec<u8>,
}
pub struct StartSnapshot {
    pub snapshot: serde_json::Value,
    pub event_batches: Vec<Vec<HistoryEvent>>,
}
#[derive(Clone)]
pub struct ActiveStartCluster {
    pub name: String,
    pub failover_version: i64,
}

#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait StartBackend: StartValidationConfig + Send + Sync {
    fn maximum_decision_timeout(&self, domain: &str) -> i32;
    fn record_uninitialized_enabled(&self, domain: &str) -> bool;
    fn cleanup_enabled(&self, domain: &str) -> bool;
    fn cleanup_metric(&self, outcome: &str);
    fn concurrency_update_failure(&self);
    async fn active_cluster_by_attribute(
        &self,
        domain: &str,
        policy: Option<&ActiveClusterSelectionPolicy>,
    ) -> HResult<ActiveStartCluster>;
    async fn workflow_selection_policy(
        &self,
        domain: &str,
        workflow: &str,
        run: &str,
    ) -> HResult<Option<ActiveClusterSelectionPolicy>>;
    fn new_mutable_state(&self, domain: &DomainCacheEntry, version: i64) -> MutableStateRef;
    async fn record_uninitialized(
        &self,
        domain: &DomainCacheEntry,
        execution: &WorkflowExecution,
        workflow_type: &str,
        timestamp: i64,
    ) -> HResult<()>;
    async fn delete_visibility(
        &self,
        domain: &DomainCacheEntry,
        execution: &WorkflowExecution,
    ) -> HResult<()>;
    async fn close_snapshot(&self, state: MutableStateRef, now: i64) -> HResult<StartSnapshot>;
    async fn persist_start_batch(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        events: &[HistoryEvent],
    ) -> HResult<PersistedStartBlob>;
    async fn create_workflow(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        snapshot: &StartSnapshot,
        blob: &PersistedStartBlob,
        mode: CreateWorkflowMode,
        previous_run: &str,
        previous_version: i64,
    ) -> HResult<()>;
    async fn delete_history_branch(&self, domain: &str, branch: &[u8]) -> HResult<()>;
    /// common.CreateHistoryStartWorkflowRequest including retry/cron/backoff calculation.
    fn history_start_request(
        &self,
        domain: &str,
        request: StartWorkflowExecutionRequest,
        partition: std::collections::HashMap<String, String>,
    ) -> HResult<HistoryStartWorkflowExecutionRequest>;
}

pub fn child_lock_timeout(parent_deadline: Option<Instant>) -> Duration {
    let timeout = Duration::from_millis(500);
    parent_deadline
        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
        .filter(|remaining| !remaining.is_zero() && *remaining < timeout)
        .unwrap_or(timeout)
}

impl HistoryEngine {
    pub async fn create_start_mutable_state(
        &self,
        domain: &DomainCacheEntry,
        run: &str,
        request: &HistoryStartWorkflowExecutionRequest,
    ) -> HResult<MutableStateRef> {
        let policy = request
            .start_request
            .as_ref()
            .and_then(|r| r.active_cluster_selection_policy.as_ref());
        let active = match self
            .start_backend
            .active_cluster_by_attribute(&domain.info.id, policy)
            .await
        {
            Err(HistoryError::ClusterAttributeNotFound) => {
                tracing::warn!("Failed to get active cluster info by cluster attribute, falling back to domain-level active cluster info");
                return Err(HistoryError::bad_request("Cannot start workflow with a cluster attribute that is not found in the domain's metadata."));
            }
            other => other?,
        };
        if active.name != self.current_cluster_name {
            return Err(self.new_domain_not_active_error(domain, active.failover_version));
        }
        let state = self
            .start_backend
            .new_mutable_state(domain, active.failover_version);
        state.set_history_tree(run)?;
        Ok(state)
    }

    pub fn add_start_events_and_tasks(
        &self,
        state: &dyn MutableState,
        execution: &WorkflowExecution,
        request: &HistoryStartWorkflowExecutionRequest,
        signal: Option<&HistorySignalWithStartWorkflowExecutionRequest>,
    ) -> HResult<()> {
        let event = state
            .add_workflow_execution_started_event(execution, request)
            .map_err(|_| {
                HistoryError::internal("Failed to add workflow execution started event.")
            })?;
        if let Some(signal) = signal {
            let signal = signal.signal_with_start_request.clone().unwrap_or_default();
            if !signal.request_id.is_empty() {
                state.add_signal_requested(&signal.request_id);
            }
            state
                .add_workflow_execution_signaled(
                    &signal.signal_name,
                    &signal.signal_input,
                    &signal.identity,
                    &signal.request_id,
                )
                .map_err(|_| {
                    HistoryError::internal("Failed to add workflow execution signaled event.")
                })?;
        }
        if request.parent_execution_info.is_none() {
            state.add_first_decision_task_scheduled(&event)?;
        }
        Ok(())
    }
}
