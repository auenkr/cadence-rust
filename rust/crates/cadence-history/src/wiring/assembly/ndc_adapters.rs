//! group: assembly/ndc_adapters
//!
//! Wiring of the NDC `Replicator` (Go `ndc.NewHistoryReplicator` /
//! `ndc.NewActivityReplicator`, built in `engineimpl.NewEngineWithShardContext`).
//!
//! Real: cluster metadata, batch deserializer (ThriftRW), branch manager and
//! workflow resetter persistence envs, shard clock, request conversion and the
//! `HistoryReplicatorImpl` / `ActivityReplicatorImpl` algorithms (version-history
//! validation, duplicate detection, retry-task hints run before the first call
//! into the pieces below).
//!
//! `NdcExecutionCache` / `NdcWorkflow` are real (ndc_workflow.rs, shared engine
//! cache lock; persistence methods are explicit errors until plan item 5a).
//!
//! NOT yet backed by the real mutable state (each returns a `TODO(...)` error):
//! * `NdcStateBuilder` (needs that bridge). `NdcStateRebuilder` is real (ndc_rebuild.rs).
//!
//! `NdcTransactionManager` is real (`EngineNdcTransactionManager`); only the
//! events-reapplication reset collaborator is an explicit gap.
//!
//! See memory/history-gap-ndcadapters-20261107.md for the follow-up task list.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::WorkflowExecution;

use crate::engine::deps::{ClusterMetadata, ExecutionCache, HResult, HistoryError};
use crate::engine::runtime::{ResetWorkflow, Replicator, WorkflowResetter};
use crate::wiring::assembly::engine_resetter::EngineWorkflowResetter;
use crate::engine::types as et;
use crate::execution::context::WorkflowEvents;
use crate::ndc::activity_replicator::{ActivityReplicator, ActivityReplicatorImpl};
use crate::ndc::branch_manager::BranchManagerEnv;
use crate::ndc::conflict_resolver::NdcStateRebuilder;
use crate::ndc::history_replicator::{
    HistoryReplicator, HistoryReplicatorEnv, HistoryReplicatorImpl, NdcStateBuilder,
};
use crate::ndc::replication_task::HistoryBatchDeserializer;
use crate::ndc::events_reapplier::EventsReapplierImpl;
use crate::ndc::transaction_manager::{NdcTransactionManager, TransactionManagerImpl};
use crate::wiring::assembly::ndc_workflow::history_to_ndc;
use crate::wiring::persistence::engine_ndc::{block_on, SqlTransactionManagerEnv, TransactionManagerCollaborators};
use crate::ndc::workflow_resetter::WorkflowResetterEnv;
use crate::ndc::{
    NdcClusterMetadata, NdcError, NdcExecutionCache, NdcMutableState, NdcResult, NdcWorkflow,
    ReplicateEventsV2Request, SyncActivityRequest,
};
use crate::shard_context::ShardContext;
use crate::wiring::assembly::engine_cache::{EngineCore, EngineExecutionCache, EngineExecutionContext};
use crate::wiring::assembly::ndc_workflow::EngineNdcExecutionCache;
use crate::wiring::assembly::ndc_rebuild::{
    EngineTaskDomains, NdcRebuildEnv, RebuildPager, SqlNdcStateRebuilder, UnavailableRebuildPager,
};
use crate::wiring::assembly::ndc_state::OwnedNdcMutableState;
use crate::wiring::persistence::engine_ndc::{
    SqlBranchManagerEnv, SqlHistoryManager, SqlHistoryPager, SqlWorkflowResetterEnv,
};

/// Go `config.StandbyClusterDelay` default (5 minutes) in nanoseconds.
// TODO(ndc-config): read the dynamic config `StandbyClusterDelay`.
const DEFAULT_STANDBY_CLUSTER_DELAY_NANOS: i64 = 5 * 60 * 1_000_000_000;

// ---------------------------------------------------------------- cluster metadata

/// Go `cluster.Metadata` as used by ndc.
pub struct EngineNdcClusterMetadata {
    inner: Arc<dyn ClusterMetadata>,
}

impl EngineNdcClusterMetadata {
    pub fn new(inner: Arc<dyn ClusterMetadata>) -> Self {
        Self { inner }
    }
}

impl NdcClusterMetadata for EngineNdcClusterMetadata {
    fn get_current_cluster_name(&self) -> String {
        self.inner.get_current_cluster_name()
    }
    fn cluster_name_for_failover_version(&self, version: i64) -> NdcResult<String> {
        self.inner
            .cluster_name_for_failover_version(version)
            .map_err(|e| NdcError::bad_request(e.to_string()))
    }
    /// Go `Metadata.IsVersionFromSameCluster`: both versions map to the same cluster.
    fn is_version_from_same_cluster(&self, v1: i64, v2: i64) -> bool {
        match (
            self.inner.cluster_name_for_failover_version(v1),
            self.inner.cluster_name_for_failover_version(v2),
        ) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------- serializer

/// Go `persistence.PayloadSerializer.DeserializeBatchEvents` for ThriftRW blobs.
pub struct ThriftBatchDeserializer;

impl HistoryBatchDeserializer for ThriftBatchDeserializer {
    fn deserialize_batch_events(&self, data: &[u8]) -> NdcResult<Vec<HistoryEvent>> {
        // `ReplicationTask` hands over the raw blob payload; the encoding is ThriftRW
        // (the only encoding Go replication accepts for events).
        let blob = cadence_api::types::DataBlob {
            encoding_type: Some(cadence_api::enums::EncodingType::ThriftRW),
            data: data.to_vec(),
        };
        cadence_thrift::history_blob::decode(&blob)
            .map_err(|e| NdcError::bad_request(format!("unable to deserialize history events: {e}")))
    }
}

// ---------------------------------------------------------------- TODO components

/// Go `execution.StateBuilder.ApplyEvents` over the typed replay entry.
pub struct TypedNdcStateBuilder;

impl NdcStateBuilder for TypedNdcStateBuilder {
    fn apply_events(
        &self,
        mutable_state: &mut dyn NdcMutableState,
        domain_id: &str,
        request_id: &str,
        execution: &WorkflowExecution,
        events: &[HistoryEvent],
        new_events: &[HistoryEvent],
    ) -> NdcResult<Option<Box<dyn NdcMutableState>>> {
        use cadence_api::enums::EventType;
        let owned = mutable_state
            .as_any_mut()
            .and_then(|a| a.downcast_mut::<OwnedNdcMutableState>())
            .ok_or_else(|| NdcError::internal("NdcStateBuilder requires an OwnedNdcMutableState"))?;
        // Go: the new run id comes from the continued-as-new event; workflow id is shared.
        let new_execution = if new_events.is_empty() {
            None
        } else {
            events
                .iter()
                .rev()
                .find(|e| e.event_type == Some(EventType::WorkflowExecutionContinuedAsNew))
                .and_then(|e| e.workflow_execution_continued_as_new_event_attributes.as_ref())
                .map(|a| WorkflowExecution {
                    workflow_id: execution.workflow_id.clone(),
                    run_id: a.new_execution_run_id.clone(),
                })
        };
        let now = owned.now_nanos();
        let env = owned.task_gen_env().clone();
        let new_typed = crate::execution::state_builder::StateBuilder::apply_events_typed_with_new_run(
            owned.typed_mut(),
            domain_id,
            request_id,
            execution,
            events,
            new_execution.as_ref(),
            new_events,
        )
        .map_err(|e| {
            use crate::execution::state_builder::StateBuilderError as E;
            match e {
                E::UnknownEventType | E::InvalidEvent { .. } => NdcError::bad_request(e.to_string()),
                other => NdcError::internal(other.to_string()),
            }
        })?;
        Ok(match (new_typed, new_execution) {
            (Some(typed), Some(exec)) => {
                Some(Box::new(OwnedNdcMutableState::from_typed(domain_id, &exec, typed, now, env)) as Box<dyn NdcMutableState>)
            }
            _ => None,
        })
    }
}

/// Real `NdcStateRebuilder` over the history store (see `ndc_rebuild.rs`).
pub(super) fn new_state_rebuilder(core: &EngineCore) -> SqlNdcStateRebuilder {
    let state_params = core.state_params.clone();
    let pager: Arc<dyn RebuildPager> = match (core.state_params)("").history_nodes {
        Some(nodes) => Arc::new(SqlHistoryPager { nodes, shard_id: core.shard.shard_id() }),
        None => Arc::new(UnavailableRebuildPager),
    };
    SqlNdcStateRebuilder::new(NdcRebuildEnv {
        pager,
        domains: core.domains.clone(),
        task_domains: Arc::new(EngineTaskDomains { domains: core.domains.clone(), state_params: state_params.clone() }),
        state_params,
        advanced_visibility_writing: core.advanced_visibility_writing.clone(),
    })
}

/// Non-persistence collaborators of Go `transactionManagerImpl`.
///
/// Gap: the Go `reset.WorkflowResetter` (events-reapplication reset of a closed
/// current workflow) is a separate collaborator; `reset_workflow` stays an
/// explicit error until it is wired (follow-up).
pub struct EngineTransactionCollaborators {
    core: Arc<EngineCore>,
    cache: EngineNdcExecutionCache,
    engine_cache: Arc<dyn ExecutionCache>,
}

impl TransactionManagerCollaborators for EngineTransactionCollaborators {
    fn current_cluster_name(&self) -> String {
        self.core.current_cluster.clone()
    }

    /// `activeClusterManager.GetActiveClusterInfoByWorkflow(...).ActiveClusterName`.
    fn active_cluster_name(&self, ms: &dyn NdcMutableState) -> NdcResult<String> {
        let info = ms.get_execution_info();
        block_on(self.core.active_cluster_manager.active_cluster_by_workflow(
            &info.domain_id,
            &info.workflow_id,
            &info.run_id,
        ))
        .map_err(history_to_ndc)
    }

    /// Go `GetOrCreateWorkflowExecution` + `LoadWorkflowExecution` (release on failure).
    fn load_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<Box<dyn NdcWorkflow>> {
        let execution = WorkflowExecution { workflow_id: workflow_id.to_string(), run_id: run_id.to_string() };
        let mut workflow = self.cache.get_or_create_workflow_execution(domain_id, &execution)?;
        if let Err(err) = workflow.load_workflow_execution(None) {
            workflow.release(Some(&err));
            return Err(err);
        }
        Ok(workflow)
    }

    /// Go `reset.WorkflowResetter.ResetWorkflow` over the current run's engine
    /// context (the NDC workflow holds its lock; its persisted state is clean).
    fn reset_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        base_run_id: &str,
        base_branch_token: &[u8],
        base_rebuild_last_event_id: i64,
        base_rebuild_last_event_version: i64,
        base_next_event_id: i64,
        reset_run_id: &str,
        reset_request_id: &str,
        current: &mut dyn NdcWorkflow,
        reset_reason: &str,
        additional_reapply_events: &[HistoryEvent],
        skip_signal_reapply: bool,
    ) -> NdcResult<()> {
        let context = current
            .engine_context()
            .and_then(|c| c.downcast::<EngineExecutionContext>().ok())
            .ok_or_else(|| NdcError::internal("reset_workflow: current workflow has no engine execution context"))?;
        let state = block_on(context.load_state()).map_err(history_to_ndc)?;
        let resetter = EngineWorkflowResetter::new(self.core.clone(), self.engine_cache.clone());
        let request = ResetWorkflow {
            domain_id: domain_id.to_string(),
            workflow_id: workflow_id.to_string(),
            base_run_id: base_run_id.to_string(),
            base_branch_token: base_branch_token.to_vec(),
            rebuild_last_event_id: base_rebuild_last_event_id,
            rebuild_last_event_version: base_rebuild_last_event_version,
            base_next_event_id,
            reset_run_id: reset_run_id.to_string(),
            request_id: reset_request_id.to_string(),
            current_context: context,
            current_state: state,
            current_release: None, // execution.NoopReleaseFn
            reason: reset_reason.to_string(),
            events: additional_reapply_events.to_vec(),
            skip_signal_reapply,
        };
        let result = block_on(resetter.reset_workflow(request)).map_err(history_to_ndc);
        // The engine write bypassed the NDC workflow's in-memory state.
        current.clear();
        result
    }
}

/// Go `transactionManagerImpl` over the SQL stores and the shared engine cache.
pub struct EngineNdcTransactionManager {
    env: SqlTransactionManagerEnv,
    reapplier: EventsReapplierImpl,
}

impl EngineNdcTransactionManager {
    pub fn new(core: Arc<EngineCore>, cache: EngineNdcExecutionCache, engine_cache: Arc<dyn ExecutionCache>) -> Self {
        let shard = core.shard.clone();
        let range_shard = shard.clone();
        let collaborators = Arc::new(EngineTransactionCollaborators { core: core.clone(), cache, engine_cache });
        Self {
            env: SqlTransactionManagerEnv {
                executions: core.executions.clone(),
                domains: core.domains.clone(),
                shard_id: shard.shard_id(),
                range_id: Arc::new(move || range_shard.range_id()),
                collaborators,
            },
            reapplier: EventsReapplierImpl::new(),
        }
    }

    fn inner(&self) -> TransactionManagerImpl<'_> {
        TransactionManagerImpl::new(&self.env, &self.reapplier)
    }
}

impl NdcTransactionManager for EngineNdcTransactionManager {
    fn create_workflow(&self, now_nanos: i64, target: Box<dyn NdcWorkflow>) -> NdcResult<()> {
        self.inner().create_workflow(now_nanos, target)
    }
    fn update_workflow(
        &self,
        now_nanos: i64,
        is_workflow_rebuilt: bool,
        target: Box<dyn NdcWorkflow>,
        new_workflow: Option<Box<dyn NdcWorkflow>>,
    ) -> NdcResult<()> {
        self.inner().update_workflow(now_nanos, is_workflow_rebuilt, target, new_workflow)
    }
    fn backfill_workflow(&self, now_nanos: i64, target: Box<dyn NdcWorkflow>, events: &WorkflowEvents) -> NdcResult<()> {
        self.inner().backfill_workflow(now_nanos, target, events)
    }
    fn check_workflow_exists(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<bool> {
        self.inner().check_workflow_exists(domain_id, workflow_id, run_id)
    }
    fn get_current_workflow_run_id(&self, domain_id: &str, workflow_id: &str) -> NdcResult<String> {
        self.inner().get_current_workflow_run_id(domain_id, workflow_id)
    }
    fn load_ndc_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> NdcResult<Box<dyn NdcWorkflow>> {
        self.inner().load_ndc_workflow(domain_id, workflow_id, run_id)
    }
}

// ---------------------------------------------------------------- env

/// Everything `HistoryReplicatorImpl` / `ActivityReplicatorImpl` need for one shard.
pub struct EngineNdcEnv {
    shard: Arc<ShardContext>,
    cluster: EngineNdcClusterMetadata,
    serializer: ThriftBatchDeserializer,
    cache: EngineNdcExecutionCache,
    transactions: EngineNdcTransactionManager,
    state_builder: TypedNdcStateBuilder,
    state_rebuilder: SqlNdcStateRebuilder,
    branch_env: SqlBranchManagerEnv,
    resetter_env: SqlWorkflowResetterEnv,
    clock: Arc<dyn crate::engine::deps::TimeSource>,
}

impl EngineNdcEnv {
    pub fn new(
        core: &EngineCore,
        engine_cache: Arc<EngineExecutionCache>,
        cluster: Arc<dyn ClusterMetadata>,
    ) -> Self {
        let history = SqlHistoryManager::new(core.history_v2.clone(), core.shard.shard_id());
        Self {
            shard: core.shard.clone(),
            cluster: EngineNdcClusterMetadata::new(cluster.clone()),
            serializer: ThriftBatchDeserializer,
            cache: EngineNdcExecutionCache::new(
                engine_cache.clone(),
                Arc::new(EngineNdcClusterMetadata::new(cluster.clone())),
                core.clock.clone(),
            ),
            transactions: EngineNdcTransactionManager::new(
                engine_cache.core(),
                EngineNdcExecutionCache::new(
                    engine_cache.clone(),
                    Arc::new(EngineNdcClusterMetadata::new(cluster.clone())),
                    core.clock.clone(),
                ),
                engine_cache,
            ),
            state_builder: TypedNdcStateBuilder,
            state_rebuilder: new_state_rebuilder(core),
            branch_env: SqlBranchManagerEnv { history: history.clone(), domains: core.domains.clone() },
            resetter_env: SqlWorkflowResetterEnv { history, domains: core.domains.clone() },
            clock: core.clock.clone(),
        }
    }
}

impl HistoryReplicatorEnv for EngineNdcEnv {
    fn cluster_metadata(&self) -> &dyn NdcClusterMetadata {
        &self.cluster
    }
    fn serializer(&self) -> &dyn HistoryBatchDeserializer {
        &self.serializer
    }
    fn execution_cache(&self) -> &dyn NdcExecutionCache {
        &self.cache
    }
    fn transaction_manager(&self) -> &dyn NdcTransactionManager {
        &self.transactions
    }
    fn state_builder(&self) -> &dyn NdcStateBuilder {
        &self.state_builder
    }
    fn state_rebuilder(&self) -> &dyn NdcStateRebuilder {
        &self.state_rebuilder
    }
    fn branch_manager_env(&self) -> &dyn BranchManagerEnv {
        &self.branch_env
    }
    fn resetter_env(&self) -> &dyn WorkflowResetterEnv {
        &self.resetter_env
    }
    fn now_nanos(&self) -> i64 {
        self.clock.now()
    }
    fn new_mutable_state(&self, domain_id: &str, execution: &WorkflowExecution) -> NdcResult<Box<dyn NdcMutableState>> {
        // Go `NewMutableStateBuilderWithVersionHistories(.., EmptyVersion)`: one empty version history.
        Ok(Box::new(new_state_with_empty_version_histories(
            domain_id,
            execution,
            self.now_nanos(),
        )))
    }
    fn standby_cluster_delay_nanos(&self) -> i64 {
        DEFAULT_STANDBY_CLUSTER_DELAY_NANOS
    }
    fn set_current_time(&self, cluster: &str, nanos: i64) {
        self.shard.set_current_time(cluster, nanos);
    }
}

fn new_state_with_empty_version_histories(
    domain_id: &str,
    execution: &WorkflowExecution,
    now_nanos: i64,
) -> OwnedNdcMutableState {
    let mut state = OwnedNdcMutableState::new(
        domain_id,
        execution,
        now_nanos,
        crate::execution::mutable_state_methods_common::TaskGenEnv::default(),
    );
    state.typed_mut().version_histories = Some(cadence_api::types::VersionHistories {
        current_version_history_index: 0,
        histories: vec![cadence_persistence::version_history::new_history(Vec::new(), &[])],
    });
    state
}

// ---------------------------------------------------------------- request conversion

fn to_ndc_replicate(r: &et::ReplicateEventsV2Request) -> ReplicateEventsV2Request {
    ReplicateEventsV2Request {
        domain_uuid: r.domain_uuid.clone(),
        workflow_execution: r.workflow_execution.clone(),
        version_history_items: r.version_history_items.clone(),
        events: r.events.clone(),
        new_run_events: r.new_run_events.clone(),
    }
}

fn to_ndc_sync_activity(r: &et::SyncActivityRequest) -> SyncActivityRequest {
    SyncActivityRequest {
        domain_id: r.domain_id.clone(),
        workflow_id: r.workflow_id.clone(),
        run_id: r.run_id.clone(),
        version: r.version,
        scheduled_id: r.scheduled_id,
        scheduled_time: r.scheduled_time.unwrap_or_default(),
        started_id: r.started_id,
        started_time: r.started_time.unwrap_or_default(),
        last_heartbeat_time: r.last_heartbeat_time.unwrap_or_default(),
        attempt: r.attempt,
        version_history: r.version_history.clone(),
        details: r.details.clone(),
        last_failure_reason: r.last_failure_reason.clone(),
        last_worker_identity: r.last_worker_identity.clone(),
        last_failure_details: r.last_failure_details.clone(),
        last_failure_options: r.last_failure_options.clone(),
    }
}

fn ndc_to_history_error(err: NdcError) -> HistoryError {
    match err {
        NdcError::RetryTaskV2(e) => HistoryError::from(cadence_api::errors::CadenceError::from(e)),
        NdcError::BadRequest(e) => HistoryError::bad_request(e.to_string()),
        NdcError::Internal(e) => HistoryError::internal(e.to_string()),
        NdcError::EntityNotExists(m) => HistoryError::entity_not_exists(m),
        NdcError::WorkflowExecutionAlreadyStarted(m) | NdcError::Other(m) => HistoryError::internal(m),
    }
}

// ---------------------------------------------------------------- replicator

/// Engine-facing NDC replicator (Go `engine.ReplicateEventsV2` / `SyncActivity`).
pub struct NdcReplicator {
    env: Arc<EngineNdcEnv>,
}

impl NdcReplicator {
    pub fn new(
        core: &EngineCore,
        engine_cache: Arc<EngineExecutionCache>,
        cluster: Arc<dyn ClusterMetadata>,
    ) -> Self {
        Self { env: Arc::new(EngineNdcEnv::new(core, engine_cache, cluster)) }
    }
}

#[async_trait]
impl Replicator for NdcReplicator {
    async fn apply_events(&self, request: &et::ReplicateEventsV2Request) -> HResult<()> {
        let env = self.env.clone();
        let request = to_ndc_replicate(request);
        // The ndc layer is synchronous and bridges to async stores with `block_on`.
        tokio::task::spawn_blocking(move || {
            HistoryReplicatorImpl::new(env.as_ref()).apply_events(&request)
        })
        .await
        .map_err(|e| HistoryError::internal(format!("ndc apply events task failed: {e}")))?
        .map_err(ndc_to_history_error)
    }

    async fn sync_activity(&self, request: &et::SyncActivityRequest) -> HResult<()> {
        let env = self.env.clone();
        let request = to_ndc_sync_activity(request);
        tokio::task::spawn_blocking(move || {
            ActivityReplicatorImpl::new(env.execution_cache(), env.cluster_metadata()).sync_activity(&request)
        })
        .await
        .map_err(|e| HistoryError::internal(format!("ndc sync activity task failed: {e}")))?
        .map_err(ndc_to_history_error)
    }
}

#[cfg(test)]
mod empty_version_histories_tests {
    use super::*;
    use crate::ndc::NdcMutableState;

    #[test]
    fn replicator_new_state_has_empty_current_version_history() {
        let exec = WorkflowExecution { workflow_id: "w".into(), run_id: "r".into() };
        let state = new_state_with_empty_version_histories("d", &exec, 1);
        let h = state.get_version_histories().expect("version histories present");
        assert_eq!(h.current_version_history_index, 0);
        assert_eq!(h.histories.len(), 1);
        assert!(h.histories[0].items.is_empty());
        assert!(h.histories[0].branch_token.is_empty());
    }
}
