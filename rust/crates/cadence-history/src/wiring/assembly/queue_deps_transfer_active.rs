//! `TransferActiveDependencies` adapter (Go `task.NewTransferActiveTaskExecutor`
//! dependencies) over the shard's execution cache, the local shard registry
//! (history client calls) and the SQL execution store.
//!
//! Done: execution cache access (`EngineActiveTransferContext`), read accessors of
//! `ActiveMutableState` over `EngineMutableState`, the history client calls that
//! the in-process `HistoryEngine` supports (start, signal, request cancel,
//! terminate, remove signal, schedule decision), `get_current_execution`, config.
//!
//! Also done: child started / start child failed / external cancel+signal events
//! (`engine_events_transfer.rs`), record child execution completed (full close
//! event), initiated event decoding, auto reset point, current version history.
//!
//! Not done (explicit errors, TODO(link)):
//! * `send_parent_close_policy_request` (parent close policy worker client),
//!   `reset_workflow` (workflow resetter).
//! * Multi host forwarding of history client calls: shards owned by other hosts
//!   surface `ShardOwnershipLost` (same as `RegistryHistoryClient`).
//! * Request fields the engine request types do not carry (delay start,
//!   cron overlap policy of a child, cluster attribute of the selection policy).
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::errors::CadenceError;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types as api;

use super::controller::ShardRegistry;
use super::engine_cache::{EngineExecutionCache, EngineExecutionContext};
use super::engine_state::EngineMutableState;
use super::env::HistoryEnv;
use crate::engine::deps::{HistoryError, ReleaseFunc, WorkflowExecution};
use crate::engine::deps_ms::MutableState as EngineMs;
use crate::engine::runtime::HistoryEngine;
use crate::engine::types as et;
use crate::task::deps::{
    metrics, ptask_type, tag, ActiveClusterSelectionPolicy, ClusterAttribute, BytesMap, DomainCache, DomainCacheEntry, Logger,
    MetricsScope, PersistenceTask, Result as TaskResult, TaskError, TaskListKind, Time, TimeSource,
};
use crate::task::transfer_active_task_executor as ta;
use crate::task::transfer_active_task_executor::{
    ActiveActivityInfo, ActiveDecisionInfo, ActiveExecutionInfo, ActiveMutableState, ActiveTransferContext,
    ChildExecutionInfo, CloseEvent, CloseEventType, FrontendStartRequest, HistoryRequestCancelRequest,
    HistorySignalRequest, HistoryStartWorkflowRequest, HistoryTerminateRequest, ParentClosePolicy,
    ParentClosePolicyRequest, RecordChildExecutionCompletedRequest, RemoveSignalMutableStateRequest,
    RequestCancelInfo, ResetPointInfo, ResetVersionHistory, ScheduleDecisionTaskRequest, SignalInfo,
    StartChildAttributes, TransferActiveDependencies, WorkflowExecutionRef, WorkflowResetRequest,
};
use crate::wiring::resources::domain_cache::HistoryDomainCache;
use crate::task::transfer_task_executor_base::{DecisionState, StartEvent};

fn todo_err(what: &str) -> TaskError {
    TaskError::Other(format!("{what} is not supported yet (TODO(link): transfer active executor dependency)"))
}

fn he(e: HistoryError) -> TaskError {
    match e {
        HistoryError::Cadence(c) => match *c {
            CadenceError::EntityNotExists(x) => TaskError::EntityNotExists(x.message),
            CadenceError::ServiceBusy(x) => TaskError::ServiceBusy(x.message),
            CadenceError::WorkflowAlreadyCompleted(x) => TaskError::WorkflowExecutionAlreadyCompleted(x.to_string()),
            other => TaskError::InternalService(other.to_string()),
        },
        HistoryError::DeadlineExceeded => TaskError::ContextDeadlineExceeded,
        HistoryError::Canceled => TaskError::ContextCanceled,
        HistoryError::PersistenceShardOwnershipLost(m) => TaskError::ShardOwnershipLost(m),
        other => TaskError::InternalService(other.to_string()),
    }
}

fn pe(e: cadence_persistence::history_contract::PersistenceError) -> TaskError {
    use cadence_persistence::history_contract::PersistenceError as P;
    match e {
        P::EntityNotExists(m) => TaskError::EntityNotExists(m),
        P::ServiceBusy(m) => TaskError::ServiceBusy(m),
        P::Timeout(_) => TaskError::ContextDeadlineExceeded,
        other => TaskError::InternalService(other.to_string()),
    }
}

fn api_execution(e: &WorkflowExecutionRef) -> Option<WorkflowExecution> {
    Some(WorkflowExecution::new(e.workflow_id.clone(), e.run_id.clone()))
}

fn map_policy(v: i32) -> ParentClosePolicy {
    match v {
        0 => ParentClosePolicy::Abandon,
        1 => ParentClosePolicy::RequestCancel,
        2 => ParentClosePolicy::Terminate,
        other => ParentClosePolicy::Unknown(other),
    }
}

fn map_kind(v: i32) -> TaskListKind {
    match v {
        1 => TaskListKind::Sticky,
        2 => TaskListKind::Ephemeral,
        _ => TaskListKind::Normal,
    }
}

// ---------------------------------------------------------------- mutable state

/// `ActiveMutableState` over a loaded `EngineMutableState`.
pub struct EngineActiveMutableState {
    state: Arc<EngineMutableState>,
    domain_cache: Arc<HistoryDomainCache>,
}

impl EngineActiveMutableState {
    pub fn new(state: Arc<EngineMutableState>, domain_cache: Arc<HistoryDomainCache>) -> Arc<Self> {
        Arc::new(Self { state, domain_cache })
    }

    fn child_from_pc(c: &cadence_persistence::history_contract::ChildExecutionInfo) -> ChildExecutionInfo {
        ChildExecutionInfo {
            version: c.version,
            started_id: c.started_id,
            started_workflow_id: c.started_workflow_id.clone(),
            started_run_id: String::from_utf8_lossy(&c.started_run_id).into_owned(),
            create_request_id: c.create_request_id.clone(),
            parent_close_policy: map_policy(c.parent_close_policy),
            domain_id: c.domain_id.clone(),
            domain_name: c.domain_name.clone(),
        }
    }
}

impl DecisionState for EngineActiveMutableState {
    fn has_pending_decision(&self) -> bool {
        EngineMs::has_pending_decision(&*self.state)
    }
    fn has_in_flight_decision(&self) -> bool {
        EngineMs::has_in_flight_decision(&*self.state)
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        EngineMs::has_processed_or_pending_decision(&*self.state)
    }
}

fn event_type_of(event: &HistoryEvent) -> CloseEventType {
    use cadence_api::enums::EventType as E;
    match event.event_type {
        Some(E::WorkflowExecutionCompleted) => CloseEventType::Completed,
        Some(E::WorkflowExecutionFailed) => CloseEventType::Failed,
        Some(E::WorkflowExecutionCanceled) => CloseEventType::Canceled,
        Some(E::WorkflowExecutionTerminated) => CloseEventType::Terminated,
        Some(E::WorkflowExecutionTimedOut) => CloseEventType::TimedOut,
        Some(E::WorkflowExecutionContinuedAsNew) => CloseEventType::ContinuedAsNew,
        _ => CloseEventType::Other,
    }
}

#[async_trait]
impl ActiveMutableState for EngineActiveMutableState {
    fn is_workflow_execution_running(&self) -> bool {
        EngineMs::is_workflow_execution_running(&*self.state)
    }

    fn domain_name(&self) -> String {
        EngineMs::get_domain_entry(&*self.state).info.name.clone()
    }

    fn execution_info(&self) -> ActiveExecutionInfo {
        let i = EngineMs::get_execution_info(&*self.state);
        let pc_kind = self.state.inner.lock().unwrap().pc_exec.task_list_kind;
        ActiveExecutionInfo {
            domain_id: i.domain_id,
            workflow_id: i.workflow_id,
            run_id: i.run_id,
            task_list: i.task_list,
            task_list_kind: map_kind(pc_kind),
            workflow_timeout: i.workflow_timeout,
            sticky_schedule_to_start_timeout: i.sticky_schedule_to_start_timeout,
            partition_config: i
                .partition_config
                .map(|m| m.into_iter().map(|(k, v)| (k, v.into_bytes())).collect())
                .unwrap_or_default(),
            parent_domain_id: i.parent_domain_id,
            parent_workflow_id: i.parent_workflow_id,
            parent_run_id: i.parent_run_id,
            initiated_id: i.initiated_id,
            workflow_type_name: i.workflow_type_name,
            close_status: i.close_status,
            // persistence close status: ContinuedAsNew = 5
            is_continued_as_new: i.close_status == 5,
            cron_schedule: i.cron_schedule,
            memo: i.memo.map(|m| m.into_iter().collect::<BytesMap>()),
            search_attributes: i.search_attributes.map(|m| m.into_iter().collect::<BytesMap>()).unwrap_or_default(),
            active_cluster_selection_policy: i.active_cluster_selection_policy.map(|p| ActiveClusterSelectionPolicy {
                cluster_attribute: p.cluster_attribute.map(|a| ClusterAttribute { scope: a.scope, name: a.name }),
            }),
        }
    }

    fn next_event_id(&self) -> i64 {
        EngineMs::get_next_event_id(&*self.state)
    }

    fn activity_info(&self, schedule_id: i64) -> Option<ActiveActivityInfo> {
        let g = self.state.inner.lock().unwrap();
        g.pc_acts.get(&schedule_id).map(|a| ActiveActivityInfo {
            version: a.version,
            schedule_to_start_timeout: a.schedule_to_start_timeout_seconds,
            task_list: a.task_list.clone(),
            task_list_kind: map_kind(a.task_list_kind),
        })
    }

    fn decision_info(&self, schedule_id: i64) -> Option<ActiveDecisionInfo> {
        let g = self.state.inner.lock().unwrap();
        (g.pc_exec.decision_schedule_id == schedule_id).then(|| ActiveDecisionInfo { version: g.pc_exec.decision_version })
    }

    fn request_cancel_info(&self, initiated_id: i64) -> Option<RequestCancelInfo> {
        // The engine state does not track request cancel infos, read the loaded snapshot.
        let kb = self.state.snapshot().request_cancel_infos.iter().find(|k| k.key == initiated_id)?;
        let s = super::engine_state::decode(&kb.value).ok()?;
        let i = cadence_persistence::sqlblobs::request_cancel_info_from_struct(&s).ok()?;
        Some(RequestCancelInfo { version: i.version, cancel_request_id: i.cancel_request_id })
    }

    fn signal_info(&self, initiated_id: i64) -> Option<SignalInfo> {
        let kb = self.state.snapshot().signal_infos.iter().find(|k| k.key == initiated_id)?;
        let s = super::engine_state::decode(&kb.value).ok()?;
        let i = cadence_persistence::sqlblobs::signal_info_from_struct(&s).ok()?;
        Some(SignalInfo {
            version: i.version,
            signal_name: i.name,
            signal_request_id: i.request_id,
            input: i.input,
            control: i.control,
        })
    }

    fn child_execution_info(&self, initiated_id: i64) -> Option<ChildExecutionInfo> {
        let g = self.state.inner.lock().unwrap();
        g.pc_children.get(&initiated_id).map(Self::child_from_pc)
    }

    fn pending_child_execution_infos(&self) -> BTreeMap<i64, ChildExecutionInfo> {
        let g = self.state.inner.lock().unwrap();
        g.pc_children.iter().map(|(k, c)| (*k, Self::child_from_pc(c))).collect()
    }

    fn has_parent_execution(&self) -> bool {
        let i = EngineMs::get_execution_info(&*self.state);
        !i.parent_run_id.is_empty()
    }

    fn last_write_version(&self) -> TaskResult<i64> {
        EngineMs::get_last_write_version(&*self.state).map_err(he)
    }

    fn start_version(&self) -> TaskResult<i64> {
        Ok(self.state.snapshot().start_version)
    }

    fn current_version_history(&self) -> TaskResult<Option<Arc<dyn ResetVersionHistory>>> {
        // Go `versionHistories.GetCurrentVersionHistory`.
        let Some(vh) = EngineMs::get_version_histories(&*self.state) else { return Ok(None) };
        match vh.histories.get(vh.current_version_history_index as usize) {
            Some(h) => Ok(Some(Arc::new(CurrentVersionHistory { history: h.clone() }) as Arc<dyn ResetVersionHistory>)),
            None => Err(TaskError::InternalService("current version history index out of range".into())),
        }
    }

    fn find_auto_reset_point(&self, time_source: &dyn TimeSource, domain: &DomainCacheEntry) -> Option<(String, ResetPointInfo)> {
        // execution.FindAutoResetPoint(timeSource, &domainEntry.GetConfig().BadBinaries, AutoResetPoints)
        let points = EngineMs::get_execution_info(&*self.state).auto_reset_points?;
        if points.points.is_empty() {
            return None;
        }
        let meta = self.domain_cache.inner().get_domain_by_id(&domain.info.id).ok()?;
        let binaries = meta.decoded_bad_binaries.as_ref()?.binaries.as_ref()?;
        let now = time_source.now().0;
        for p in &points.points {
            let Some(Some(bin)) = binaries.get(&p.binary_checksum) else { continue };
            if !p.resettable {
                continue;
            }
            let expiring = p.expiring_time_nano.unwrap_or(0);
            if expiring == 0 || now < expiring {
                return Some((
                    bin.reason.clone(),
                    ResetPointInfo {
                        run_id: p.run_id.clone(),
                        binary_checksum: p.binary_checksum.clone(),
                        first_decision_completed_id: p.first_decision_completed_id,
                    },
                ));
            }
        }
        None
    }

    fn add_child_workflow_execution_started_event(&self, domain: &str, execution: &WorkflowExecutionRef, attributes: &StartChildAttributes, initiated_id: i64) -> TaskResult<()> {
        let header = attributes
            .header
            .as_ref()
            .map(|h| api::Header { fields: Some(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()) });
        let exec = WorkflowExecution::new(execution.workflow_id.clone(), execution.run_id.clone());
        self.state.add_child_started(domain, &exec, initiated_id, header).map_err(he)
    }
    fn add_start_child_workflow_execution_failed_event(&self, initiated_id: i64, _attributes: &StartChildAttributes) -> TaskResult<()> {
        self.state.add_start_child_failed(initiated_id).map_err(he)
    }
    fn add_external_workflow_execution_cancel_requested(&self, initiated_id: i64, domain: &str, workflow_id: &str, run_id: &str) -> TaskResult<()> {
        self.state.add_external_cancel_requested(initiated_id, domain, workflow_id, run_id).map_err(he)
    }
    fn add_external_workflow_execution_signaled(&self, initiated_id: i64, domain: &str, workflow_id: &str, run_id: &str, control: &[u8]) -> TaskResult<()> {
        self.state.add_external_signaled(initiated_id, domain, workflow_id, run_id, control).map_err(he)
    }
    fn add_request_cancel_external_workflow_execution_failed_event(&self, initiated_id: i64, domain: &str, workflow_id: &str, run_id: &str) -> TaskResult<()> {
        self.state.add_request_cancel_external_failed(initiated_id, domain, workflow_id, run_id).map_err(he)
    }
    fn add_signal_external_workflow_execution_failed_event(&self, initiated_id: i64, domain: &str, workflow_id: &str, run_id: &str, control: &[u8]) -> TaskResult<()> {
        self.state.add_signal_external_failed(initiated_id, domain, workflow_id, run_id, control).map_err(he)
    }

    fn schedule_decision(&self) -> TaskResult<()> {
        EngineMs::add_decision_task_scheduled_event(&*self.state, false).map_err(he)
    }

    async fn completion_event(&self) -> TaskResult<CloseEvent> {
        let event = EngineMs::get_completion_event(&*self.state).await.map_err(he)?;
        let attrs = event.workflow_execution_continued_as_new_event_attributes.as_ref();
        Ok(CloseEvent {
            event_id: event.event_id,
            event_type: event_type_of(&event),
            timestamp: event.timestamp.unwrap_or_default(),
            continued_as_new_attributes_present: attrs.is_some(),
            continued_as_new_failure_reason: attrs.and_then(|a| a.failure_reason.clone()).unwrap_or_default(),
            full_event: Some(Arc::new(event.clone())),
        })
    }

    async fn start_event(&self) -> TaskResult<StartEvent> {
        let event = EngineMs::get_start_event(&*self.state).await.map_err(he)?;
        let attrs = event.workflow_execution_started_event_attributes.as_ref().ok_or(TaskError::MissingWorkflowStartEvent)?;
        Ok(StartEvent {
            timestamp: event.timestamp.unwrap_or_default(),
            first_decision_task_backoff_seconds: attrs.first_decision_task_backoff_seconds.unwrap_or_default(),
            headers: attrs
                .header
                .as_ref()
                .and_then(|h| h.fields.clone())
                .map(|f| f.into_iter().collect::<BytesMap>()),
        })
    }

    async fn child_execution_initiated_event(&self, initiated_id: i64) -> TaskResult<StartChildAttributes> {
        let a = self.state.decode_child_initiated_event(initiated_id).map_err(he)?;
        let fields = |m: Option<std::collections::HashMap<String, Vec<u8>>>| m.map(|m| m.into_iter().collect::<BytesMap>());
        Ok(StartChildAttributes {
            domain: a.domain,
            workflow_id: a.workflow_id,
            workflow_type: a.workflow_type.map(|t| t.name).unwrap_or_default(),
            task_list: a.task_list.map(|t| t.name).unwrap_or_default(),
            input: a.input,
            header: fields(a.header.and_then(|h| h.fields)),
            execution_start_to_close_timeout_seconds: a.execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: a.task_start_to_close_timeout_seconds,
            workflow_id_reuse_policy: a.workflow_id_reuse_policy.map(|p| p as i32),
            retry_policy: a.retry_policy.map(|r| StartChildRetryPolicy {
                initial_interval_seconds: r.initial_interval_in_seconds,
                backoff_coefficient: r.backoff_coefficient,
                maximum_interval_seconds: r.maximum_interval_in_seconds,
                maximum_attempts: r.maximum_attempts,
                non_retriable_error_reasons: r.non_retriable_error_reasons,
                expiration_interval_seconds: r.expiration_interval_in_seconds,
            }),
            cron_schedule: a.cron_schedule,
            cron_overlap_policy: a.cron_overlap_policy.map(|p| p as i32),
            memo: fields(a.memo.and_then(|m| m.fields)),
            search_attributes: fields(a.search_attributes.and_then(|m| m.indexed_fields)),
            delay_start_seconds: a.delay_start_seconds,
            jitter_start_seconds: a.jitter_start_seconds,
            first_run_at_timestamp: a.first_run_at_timestamp,
        })
    }
}

/// `execution.VersionHistory` subset for the reset path.
struct CurrentVersionHistory {
    history: api::VersionHistory,
}

impl ResetVersionHistory for CurrentVersionHistory {
    fn get_event_version(&self, event_id: i64) -> TaskResult<i64> {
        // Go `VersionHistory.GetEventVersion`.
        if event_id < 1 {
            return Err(TaskError::InternalService(format!("invalid event id {event_id}")));
        }
        self.history
            .items
            .iter()
            .find(|i| event_id <= i.event_id)
            .map(|i| i.version)
            .ok_or_else(|| TaskError::InternalService(format!("event id {event_id} not found in version history")))
    }
    fn get_branch_token(&self) -> Vec<u8> {
        self.history.branch_token.clone()
    }
}

// ---------------------------------------------------------------- context

/// `ActiveTransferContext` over a locked execution context.
pub struct EngineActiveTransferContext {
    context: Arc<EngineExecutionContext>,
    release: ReleaseFunc,
    released: AtomicBool,
    domain_cache: Arc<HistoryDomainCache>,
}

impl EngineActiveTransferContext {
    async fn load(&self) -> TaskResult<Arc<EngineActiveMutableState>> {
        let state = self.context.load_state().await.map_err(he)?;
        Ok(EngineActiveMutableState::new(state, self.domain_cache.clone()))
    }
}

#[async_trait]
impl ActiveTransferContext for EngineActiveTransferContext {
    fn release(&self, error: Option<TaskError>) {
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        match error {
            Some(e) => (self.release)(Some(&HistoryError::internal(e.to_string()))),
            None => (self.release)(None),
        }
    }

    async fn load_mutable_state(
        &self,
        task: &dyn PersistenceTask,
        scope: &Arc<dyn MetricsScope>,
        logger: &Logger,
        event_id: i64,
    ) -> TaskResult<Option<Arc<dyn ActiveMutableState>>> {
        // Go task_util.loadMutableState.
        let mut ms = match self.load().await {
            Ok(ms) => ms,
            Err(TaskError::EntityNotExists(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let (schedule_id, attempt) = {
            let g = ms.state.inner.lock().unwrap();
            (g.pc_exec.decision_schedule_id, g.pc_exec.decision_attempt)
        };
        let is_decision_retry = (task.get_task_type() == ptask_type::TASK_TYPE_DECISION_TIMEOUT
            || task.get_task_type() == ptask_type::TRANSFER_TASK_TYPE_DECISION_TASK)
            && schedule_id == event_id
            && attempt > 0;
        if event_id >= ms.next_event_id() && !is_decision_retry {
            scope.inc_counter(metrics::STALE_MUTABLE_STATE_COUNTER);
            self.context.clear();
            ms = self.load().await?;
            if event_id >= ms.next_event_id() {
                logger.error(
                    "Task Processor: task event ID >= MS NextEventID, skip.",
                    &[
                        tag::workflow_domain_id(task.get_domain_id()),
                        tag::workflow_id(task.get_workflow_id()),
                        tag::workflow_run_id(task.get_run_id()),
                        tag::task_type(task.get_task_type()),
                        tag::task_id(task.get_task_id()),
                        tag::workflow_event_id(event_id),
                        tag::workflow_next_event_id(ms.next_event_id()),
                    ],
                );
                return Ok(None);
            }
        }
        Ok(Some(ms as Arc<dyn ActiveMutableState>))
    }

    async fn load_workflow_execution(&self) -> TaskResult<Arc<dyn ActiveMutableState>> {
        Ok(self.load().await? as Arc<dyn ActiveMutableState>)
    }

    async fn update_workflow_execution_as_active(&self, now: Time) -> TaskResult<()> {
        self.context.update_as_active(now.0).await.map_err(he)
    }
}

// ---------------------------------------------------------------- dependencies

/// `TransferActiveDependencies` implementation.
pub struct TransferActiveDepsImpl {
    env: Arc<HistoryEnv>,
    domains: Arc<dyn DomainCache>,
    cache: Arc<EngineExecutionCache>,
    registry: OnceLock<Weak<ShardRegistry>>,
}

impl TransferActiveDepsImpl {
    pub fn new(env: Arc<HistoryEnv>, cache: Arc<EngineExecutionCache>) -> Arc<Self> {
        let domains = env.domain_cache.clone() as Arc<dyn DomainCache>;
        Arc::new(Self { env, domains, cache, registry: OnceLock::new() })
    }

    /// Must be called once after the `ShardRegistry` has been built (history client calls).
    pub fn bind(&self, registry: &Arc<ShardRegistry>) {
        let _ = self.registry.set(Arc::downgrade(registry));
    }

    async fn engine_for(&self, workflow_id: &str) -> TaskResult<Arc<HistoryEngine>> {
        let registry = self
            .registry
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| TaskError::InternalService("shard registry is not bound".into()))?;
        registry.engine_for_workflow(workflow_id).await.map_err(he)
    }

    fn partition_config(config: &BytesMap) -> std::collections::HashMap<String, String> {
        config.iter().map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned())).collect()
    }
}

fn to_api_start(a: &StartChildAttributes, domain: &str, request_id: &str, policy: Option<&ActiveClusterSelectionPolicy>) -> api::StartWorkflowExecutionRequest {
    let to_fields = |m: &Option<BytesMap>| m.as_ref().map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    api::StartWorkflowExecutionRequest {
        domain: domain.to_string(),
        workflow_id: a.workflow_id.clone(),
        workflow_type: Some(api::WorkflowType { name: a.workflow_type.clone() }),
        task_list: Some(api::TaskList { name: a.task_list.clone(), kind: None, base_name: String::new() }),
        input: a.input.clone(),
        execution_start_to_close_timeout_seconds: a.execution_start_to_close_timeout_seconds,
        task_start_to_close_timeout_seconds: a.task_start_to_close_timeout_seconds,
        identity: ta::IDENTITY_HISTORY_SERVICE.to_string(),
        request_id: request_id.to_string(),
        workflow_id_reuse_policy: a.workflow_id_reuse_policy.and_then(cadence_api::enums::WorkflowIDReusePolicy::from_i32),
        retry_policy: a.retry_policy.as_ref().map(|r| api::RetryPolicy {
            initial_interval_in_seconds: r.initial_interval_seconds,
            backoff_coefficient: r.backoff_coefficient,
            maximum_interval_in_seconds: r.maximum_interval_seconds,
            maximum_attempts: r.maximum_attempts,
            non_retriable_error_reasons: r.non_retriable_error_reasons.clone(),
            expiration_interval_in_seconds: r.expiration_interval_seconds,
            ..Default::default()
        }),
        cron_schedule: a.cron_schedule.clone(),
        cron_overlap_policy: a.cron_overlap_policy.and_then(cadence_api::enums::CronOverlapPolicy::from_i32),
        delay_start_seconds: a.delay_start_seconds,
        jitter_start_seconds: a.jitter_start_seconds,
        first_run_at_timestamp: a.first_run_at_timestamp,
        active_cluster_selection_policy: policy.map(|p| api::ActiveClusterSelectionPolicy {
            cluster_attribute: p.cluster_attribute.as_ref().map(|c| api::ClusterAttribute { scope: c.scope.clone(), name: c.name.clone() }),
        }),
        memo: to_fields(&a.memo).map(|fields| api::Memo { fields: Some(fields) }),
        search_attributes: to_fields(&a.search_attributes).map(|f| api::SearchAttributes { indexed_fields: Some(f) }),
        header: to_fields(&a.header).map(|fields| api::Header { fields: Some(fields) }),
        ..Default::default()
    }
}

#[async_trait]
impl TransferActiveDependencies for TransferActiveDepsImpl {
    async fn get_or_create_workflow_execution_with_timeout(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
        timeout: Duration,
    ) -> TaskResult<Arc<dyn ActiveTransferContext>> {
        let execution = WorkflowExecution::new(workflow_id.to_string(), run_id.to_string());
        let (context, release) = tokio::time::timeout(timeout, self.cache.get_or_create(domain_id, &execution))
            .await
            .map_err(|_| TaskError::ContextDeadlineExceeded)?
            .map_err(he)?;
        Ok(Arc::new(EngineActiveTransferContext { context, release, released: AtomicBool::new(false), domain_cache: self.env.domain_cache.clone() }))
    }

    fn allow_internal(&self, _domain_id: &str, _workflow_id: &str) -> bool {
        // TODO(link): workflowcache.WFCache (internal rate limit); allow everything.
        true
    }

    async fn record_child_execution_completed(&self, r: RecordChildExecutionCompletedRequest) -> TaskResult<()> {
        let engine = self.engine_for(&r.workflow_execution.workflow_id).await?;
        let event = r.completion_event.full_event.as_deref().cloned();
        engine
            .record_child_execution_completed(&et::RecordChildExecutionCompletedRequest {
                domain_uuid: r.domain_uuid,
                workflow_execution: api_execution(&r.workflow_execution),
                initiated_id: r.initiated_id,
                completed_execution: api_execution(&r.completed_execution),
                completion_event: event,
                started_id: crate::engine::deps::EMPTY_EVENT_ID,
            })
            .await
            .map_err(he)
    }

    async fn request_cancel_workflow_execution(&self, r: HistoryRequestCancelRequest) -> TaskResult<()> {
        let engine = self.engine_for(&r.workflow_execution.workflow_id).await?;
        engine
            .request_cancel_workflow_execution(&et::HistoryRequestCancelWorkflowExecutionRequest {
                domain_uuid: r.domain_uuid,
                cancel_request: Some(cadence_api::service_types::RequestCancelWorkflowExecutionRequest {
                    domain: r.domain,
                    workflow_execution: api_execution(&r.workflow_execution),
                    identity: r.identity,
                    request_id: r.request_id,
                    ..Default::default()
                }),
                external_initiated_event_id: r.external_initiated_event_id,
                external_workflow_execution: r.external_workflow_execution.as_ref().and_then(api_execution),
                child_workflow_only: r.child_workflow_only,
            })
            .await
            .map_err(he)
    }

    async fn signal_workflow_execution(&self, r: HistorySignalRequest) -> TaskResult<()> {
        let engine = self.engine_for(&r.workflow_execution.workflow_id).await?;
        engine
            .signal_workflow_execution(&et::HistorySignalWorkflowExecutionRequest {
                domain_uuid: r.domain_uuid,
                signal_request: Some(api::SignalWorkflowExecutionRequest {
                    domain: r.domain,
                    workflow_execution: api_execution(&r.workflow_execution),
                    identity: r.identity,
                    signal_name: r.signal_name,
                    input: r.input,
                    request_id: r.request_id,
                    control: r.control,
                }),
                external_workflow_execution: r.external_workflow_execution.as_ref().and_then(api_execution),
                child_workflow_only: r.child_workflow_only,
            })
            .await
            .map_err(he)
    }

    async fn remove_signal_mutable_state(&self, r: RemoveSignalMutableStateRequest) -> TaskResult<()> {
        let engine = self.engine_for(&r.workflow_execution.workflow_id).await?;
        engine
            .remove_signal_mutable_state(&et::RemoveSignalMutableStateRequest {
                domain_uuid: r.domain_uuid,
                workflow_execution: api_execution(&r.workflow_execution),
                request_id: r.request_id,
            })
            .await
            .map_err(he)
    }

    async fn start_workflow_execution(&self, r: HistoryStartWorkflowRequest) -> TaskResult<String> {
        let workflow_id = r.request.attributes.workflow_id.clone();
        let engine = self.engine_for(&workflow_id).await?;
        let start = to_api_start(&r.request.attributes, &r.request.domain, &r.request.request_id, r.request.active_cluster_selection_policy.as_ref());
        let response = engine
            .start_workflow_execution(&et::HistoryStartWorkflowExecutionRequest {
                domain_uuid: r.domain_uuid,
                start_request: Some(start),
                parent_execution_info: r.parent_execution_info.map(|p| et::ParentExecutionInfo {
                    domain_uuid: p.domain_uuid,
                    domain: p.domain,
                    execution: api_execution(&p.execution),
                    initiated_id: p.initiated_id,
                }),
                partition_config: Self::partition_config(&r.partition_config),
                ..Default::default()
            })
            .await
            .map_err(he)?;
        Ok(response.run_id)
    }

    async fn schedule_decision_task(&self, r: ScheduleDecisionTaskRequest) -> TaskResult<()> {
        let engine = self.engine_for(&r.workflow_execution.workflow_id).await?;
        engine
            .schedule_decision_task(&et::ScheduleDecisionTaskRequest {
                domain_uuid: r.domain_uuid,
                workflow_execution: api_execution(&r.workflow_execution),
                is_first_decision: r.is_first_decision,
            })
            .await
            .map_err(he)
    }

    async fn terminate_workflow_execution(&self, r: HistoryTerminateRequest) -> TaskResult<()> {
        let engine = self.engine_for(&r.workflow_execution.workflow_id).await?;
        engine
            .terminate_workflow_execution(&et::HistoryTerminateWorkflowExecutionRequest {
                domain_uuid: r.domain_uuid,
                terminate_request: Some(api::TerminateWorkflowExecutionRequest {
                    domain: r.domain,
                    workflow_execution: api_execution(&r.workflow_execution),
                    reason: r.reason,
                    identity: r.identity,
                    ..Default::default()
                }),
                external_workflow_execution: r.external_workflow_execution.as_ref().and_then(api_execution),
                child_workflow_only: r.child_workflow_only,
            })
            .await
            .map_err(he)
    }

    fn create_history_start_workflow_request(
        &self,
        target_domain_id: &str,
        request: FrontendStartRequest,
        now: Time,
        partition_config: BytesMap,
    ) -> TaskResult<HistoryStartWorkflowRequest> {
        // common.CreateHistoryStartWorkflowRequest (parent info is set by the caller).
        Ok(HistoryStartWorkflowRequest {
            domain_uuid: target_domain_id.to_string(),
            request,
            now,
            partition_config,
            parent_execution_info: None,
        })
    }

    async fn send_parent_close_policy_request(&self, _request: ParentClosePolicyRequest) -> TaskResult<()> {
        // TODO(link): parent close policy worker client (sys-worker batcher workflow signal-with-start).
        Err(todo_err("parent close policy worker request"))
    }

    async fn reset_workflow(&self, _request: WorkflowResetRequest) -> TaskResult<()> {
        // TODO(link): reset.WorkflowResetter.
        Err(todo_err("WorkflowResetter.ResetWorkflow"))
    }

    async fn get_current_execution(&self, domain_id: &str, workflow_id: &str, _domain_name: &str, shard_id: i32) -> TaskResult<String> {
        self.env
            .persistence
            .executions
            .get_current_execution(shard_id, domain_id.to_string(), workflow_id.to_string())
            .await
            .map(|c| c.run_id)
            .map_err(pe)
    }

    fn get_child_execution_domain_id(&self, child: &ChildExecutionInfo, parent_domain: &DomainCacheEntry) -> TaskResult<String> {
        // execution.GetChildExecutionDomainID
        if !child.domain_id.is_empty() {
            return Ok(child.domain_id.clone());
        }
        if !child.domain_name.is_empty() {
            return self.domains.get_domain(&child.domain_name).map(|d| d.info.id.clone());
        }
        Ok(parent_domain.info.id.clone())
    }

    fn advanced_visibility_writing_enabled(&self) -> bool {
        // common.IsAdvancedVisibilityWritingEnabled
        self.env.config.is_advanced_vis_config_exist
            && !matches!((self.env.config.write_visibility_store_name)().as_str(), "db" | "off")
    }

    fn enable_parent_close_policy_worker(&self) -> bool {
        (self.env.config.enable_parent_close_policy_worker)()
    }

    fn parent_close_policy_threshold(&self, domain: &str) -> usize {
        (self.env.config.parent_close_policy_threshold)(domain) as usize
    }

    fn parent_close_policy_batch_size(&self, domain: &str) -> usize {
        (self.env.config.parent_close_policy_batch_size)(domain) as usize
    }

    fn log_execute_history_task(&self, _shard_id: i32, _task: &dyn PersistenceTask) {
        // simulation.LogEvents is a test-only hook (no-op outside simulation).
    }
}
