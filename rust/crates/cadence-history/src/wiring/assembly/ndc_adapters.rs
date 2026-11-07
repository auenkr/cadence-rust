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
//! NOT yet backed by the real mutable state (each returns a `TODO(...)` error):
//! * `NdcExecutionCache` / `NdcWorkflow` / `NdcMutableState` over
//!   `EngineExecutionCache` + the replay-capable mutable state. The engine uses
//!   `EngineMutableState` (snapshot-backed) while `StateBuilder` replays onto
//!   `mutable_state::MutableStateBuilder`; no bridge between them exists.
//! * `NdcStateBuilder`, `NdcStateRebuilder` (need that bridge).
//! * `NdcTransactionManager` (needs `TransactionManagerCollaborators`).
//!
//! See memory/history-gap-ndcadapters-20261107.md for the follow-up task list.

use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::WorkflowExecution;

use crate::engine::deps::{ClusterMetadata, HResult, HistoryError};
use crate::engine::runtime::Replicator;
use crate::engine::types as et;
use crate::execution::context::WorkflowEvents;
use crate::ndc::activity_replicator::{ActivityReplicator, ActivityReplicatorImpl};
use crate::ndc::branch_manager::BranchManagerEnv;
use crate::ndc::conflict_resolver::NdcStateRebuilder;
use crate::ndc::history_replicator::{
    HistoryReplicator, HistoryReplicatorEnv, HistoryReplicatorImpl, NdcStateBuilder,
};
use crate::ndc::replication_task::HistoryBatchDeserializer;
use crate::ndc::transaction_manager::NdcTransactionManager;
use crate::ndc::workflow_resetter::WorkflowResetterEnv;
use crate::ndc::{
    NdcClusterMetadata, NdcError, NdcExecutionCache, NdcMutableState, NdcResult, NdcWorkflow,
    ReplicateEventsV2Request, SyncActivityRequest,
};
use crate::shard_context::ShardContext;
use crate::wiring::assembly::engine_cache::EngineCore;
use crate::wiring::persistence::engine_ndc::{
    SqlBranchManagerEnv, SqlHistoryManager, SqlWorkflowResetterEnv,
};

/// Go `config.StandbyClusterDelay` default (5 minutes) in nanoseconds.
// TODO(ndc-config): read the dynamic config `StandbyClusterDelay`.
const DEFAULT_STANDBY_CLUSTER_DELAY_NANOS: i64 = 5 * 60 * 1_000_000_000;

fn todo_error(what: &str) -> NdcError {
    NdcError::internal(format!("TODO(ndc-adapters): {what} is not implemented over the real mutable state yet"))
}

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

/// `NdcExecutionCache` placeholder (see module docs).
pub struct TodoNdcExecutionCache;

impl NdcExecutionCache for TodoNdcExecutionCache {
    fn get_or_create_workflow_execution(
        &self,
        _domain_id: &str,
        _execution: &WorkflowExecution,
    ) -> NdcResult<Box<dyn NdcWorkflow>> {
        Err(todo_error("NdcExecutionCache::get_or_create_workflow_execution"))
    }
    fn new_workflow(
        &self,
        _domain_id: &str,
        _execution: &WorkflowExecution,
        _mutable_state: Box<dyn NdcMutableState>,
        _release_noop: bool,
    ) -> Box<dyn NdcWorkflow> {
        // The trait is infallible; the failure is reported by `new_mutable_state`
        // (which is the only producer of the `mutable_state` argument).
        unreachable!("TODO(ndc-adapters): TodoNdcExecutionCache::new_workflow needs a NdcMutableState, none can be created")
    }
}

/// `NdcStateBuilder` placeholder.
pub struct TodoNdcStateBuilder;

impl NdcStateBuilder for TodoNdcStateBuilder {
    fn apply_events(
        &self,
        _mutable_state: &mut dyn NdcMutableState,
        _domain_id: &str,
        _request_id: &str,
        _execution: &WorkflowExecution,
        _events: &[HistoryEvent],
        _new_events: &[HistoryEvent],
    ) -> NdcResult<Option<Box<dyn NdcMutableState>>> {
        Err(todo_error("NdcStateBuilder::apply_events"))
    }
}

/// `NdcStateRebuilder` placeholder.
pub struct TodoNdcStateRebuilder;

impl NdcStateRebuilder for TodoNdcStateRebuilder {
    fn rebuild(
        &self,
        _now_nanos: i64,
        _base_domain_id: &str,
        _base_workflow_id: &str,
        _base_run_id: &str,
        _base_branch_token: &[u8],
        _base_last_event_id: i64,
        _base_last_event_version: i64,
        _target_run_id: &str,
        _target_branch_token: &dyn Fn() -> NdcResult<Vec<u8>>,
        _request_id: &str,
    ) -> NdcResult<(Box<dyn NdcMutableState>, i64)> {
        Err(todo_error("NdcStateRebuilder::rebuild"))
    }
}

/// `NdcTransactionManager` placeholder.
pub struct TodoNdcTransactionManager;

impl NdcTransactionManager for TodoNdcTransactionManager {
    fn create_workflow(&self, _now_nanos: i64, _target: Box<dyn NdcWorkflow>) -> NdcResult<()> {
        Err(todo_error("NdcTransactionManager::create_workflow"))
    }
    fn update_workflow(
        &self,
        _now_nanos: i64,
        _is_workflow_rebuilt: bool,
        _target: Box<dyn NdcWorkflow>,
        _new_workflow: Option<Box<dyn NdcWorkflow>>,
    ) -> NdcResult<()> {
        Err(todo_error("NdcTransactionManager::update_workflow"))
    }
    fn backfill_workflow(
        &self,
        _now_nanos: i64,
        _target: Box<dyn NdcWorkflow>,
        _events: &WorkflowEvents,
    ) -> NdcResult<()> {
        Err(todo_error("NdcTransactionManager::backfill_workflow"))
    }
    fn check_workflow_exists(&self, _domain_id: &str, _workflow_id: &str, _run_id: &str) -> NdcResult<bool> {
        Err(todo_error("NdcTransactionManager::check_workflow_exists"))
    }
    fn get_current_workflow_run_id(&self, _domain_id: &str, _workflow_id: &str) -> NdcResult<String> {
        Err(todo_error("NdcTransactionManager::get_current_workflow_run_id"))
    }
    fn load_ndc_workflow(&self, _domain_id: &str, _workflow_id: &str, _run_id: &str) -> NdcResult<Box<dyn NdcWorkflow>> {
        Err(todo_error("NdcTransactionManager::load_ndc_workflow"))
    }
}

// ---------------------------------------------------------------- env

/// Everything `HistoryReplicatorImpl` / `ActivityReplicatorImpl` need for one shard.
pub struct EngineNdcEnv {
    shard: Arc<ShardContext>,
    cluster: EngineNdcClusterMetadata,
    serializer: ThriftBatchDeserializer,
    cache: TodoNdcExecutionCache,
    transactions: TodoNdcTransactionManager,
    state_builder: TodoNdcStateBuilder,
    state_rebuilder: TodoNdcStateRebuilder,
    branch_env: SqlBranchManagerEnv,
    resetter_env: SqlWorkflowResetterEnv,
    clock: Arc<dyn crate::engine::deps::TimeSource>,
}

impl EngineNdcEnv {
    pub fn new(
        core: &EngineCore,
        cluster: Arc<dyn ClusterMetadata>,
    ) -> Self {
        let history = SqlHistoryManager::new(core.history_v2.clone(), core.shard.shard_id());
        Self {
            shard: core.shard.clone(),
            cluster: EngineNdcClusterMetadata::new(cluster),
            serializer: ThriftBatchDeserializer,
            cache: TodoNdcExecutionCache,
            transactions: TodoNdcTransactionManager,
            state_builder: TodoNdcStateBuilder,
            state_rebuilder: TodoNdcStateRebuilder,
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
    fn new_mutable_state(&self, _domain_id: &str) -> NdcResult<Box<dyn NdcMutableState>> {
        Err(todo_error("HistoryReplicatorEnv::new_mutable_state"))
    }
    fn standby_cluster_delay_nanos(&self) -> i64 {
        DEFAULT_STANDBY_CLUSTER_DELAY_NANOS
    }
    fn set_current_time(&self, cluster: &str, nanos: i64) {
        self.shard.set_current_time(cluster, nanos);
    }
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
    pub fn new(core: &EngineCore, cluster: Arc<dyn ClusterMetadata>) -> Self {
        Self { env: Arc::new(EngineNdcEnv::new(core, cluster)) }
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
