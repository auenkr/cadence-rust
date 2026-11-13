//! group: assembly/engine_start
//!
//! Start path of the engine over the SQL stores: the `EngineMutableState`
//! started-event / first-decision methods (Go `AddWorkflowExecutionStartedEvent`,
//! `ReplicateWorkflowExecutionStartedEvent`, `AddFirstDecisionTaskScheduled`),
//! `EngineStartHost` (`WorkflowStartHost`) and `EngineFailoverHost`
//! (`DomainFailoverHost`).
//!
//! Documented gaps: workflow request dedup rows (`insertWorkflowRequest`),
//! orphan history branch cleanup and visibility writes are not ported (there is
//! no visibility writer yet, so `has_visibility` is false).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_api::enums::{ContinueAsNewInitiator, EventType};
use cadence_api::service_types::{HistoryEvent, WorkflowExecutionStartedEventAttributes};
use cadence_api::types::{ClusterAttribute, WorkflowExecution};
use cadence_persistence::history_contract as pc;
use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
use cadence_persistence::sqlblobs::{timer_task_to_struct, transfer_task_to_struct, Encoding};

use crate::engine::deps::{
    CreateWorkflowMode, CreateWorkflowRequestMode, DomainCacheEntry, ExecutionContextRef, HResult, HistoryError,
    MutableStateRef, EMPTY_EVENT_ID, WORKFLOW_STATE_CREATED,
};
use crate::engine::notify_tasks::PersistedBlobRef;
use crate::engine::register_domain_failover_callback::{
    DomainChangedFn, DomainFailoverHost, DomainLockFn, FailoverDlqPartition, FailoverDomainEntry,
    FailoverMarkerTask,
};
use crate::engine::start_workflow_execution::{ActiveClusterInfo, NewRunSnapshot, StartLimits, WorkflowStartHost};
use crate::engine::types::HistoryStartWorkflowExecutionRequest;
use crate::wiring::assembly::engine_cache::{shard_err, task_blob, EngineCore, EngineExecutionContext};
use crate::wiring::assembly::engine_close::ClosedTxn;
use crate::wiring::assembly::engine_state::{EngineMutableState, StateParams, EMPTY_VERSION};
use crate::wiring::assembly::engine_tasks::{BACKOFF_TYPE_CRON, BACKOFF_TYPE_RETRY};
use crate::wiring::persistence::engine_ndc::{to_history_error, SqlHistoryManager};
use crate::wiring::resources::domain_cache::HistoryDomainCache;

const EMPTY_UUID: &str = "00000000-0000-0000-0000-000000000000";
const NANOS: i64 = 1_000_000_000;

impl EngineMutableState {
    /// Go `NewMutableStateBuilderWithVersionHistories` + `SetHistoryTree(runID)`.
    pub fn new_for_start(
        domain: DomainCacheEntry,
        failover_version: i64,
        run_id: &str,
        sticky_ttl_nanos: i64,
        now_nanos: Arc<dyn Fn() -> i64 + Send + Sync>,
        params: StateParams,
    ) -> HResult<Self> {
        let branch_id = uuid::Uuid::new_v4().to_string();
        let branch_token = cadence_persistence::history_branch::new_token(run_id, &branch_id);
        let snapshot = pc::WorkflowSnapshot {
            execution_info: pc::WorkflowExecutionInfo {
                domain_id: domain.info.id.clone(),
                run_id: run_id.to_string(),
                next_event_id: 1,
                branch_token: branch_token.clone(),
                ..Default::default()
            },
            start_version: failover_version,
            last_write_version: failover_version,
            ..Default::default()
        };
        let state = Self::from_snapshot(snapshot, domain, sticky_ttl_nanos, now_nanos)?.with_params(params);
        {
            let mut g = state.inner.lock().unwrap();
            g.version_histories = Some(cadence_api::types::VersionHistories {
                current_version_history_index: 0,
                histories: vec![cadence_persistence::version_history::new_history(branch_token, &[])],
            });
            g.current_version = failover_version;
        }
        Ok(state)
    }

    /// Go `AddWorkflowExecutionStartedEvent` (+ `ReplicateWorkflowExecutionStartedEvent`).
    pub(super) fn start_event(
        &self,
        execution: &WorkflowExecution,
        request: &HistoryStartWorkflowExecutionRequest,
    ) -> HResult<HistoryEvent> {
        self.start_event_with(execution, request, None)
    }

    /// `start_event` for a run created by ContinueAsNew (Go
    /// `addWorkflowExecutionStartedEventForContinueAsNew` -> `AddWorkflowExecutionStartedEvent`
    /// with the previous execution info).
    pub(super) fn start_event_with(
        &self,
        execution: &WorkflowExecution,
        request: &HistoryStartWorkflowExecutionRequest,
        prev: Option<&ContinuedFrom>,
    ) -> HResult<HistoryEvent> {
        let start = request
            .start_request
            .as_ref()
            .ok_or_else(|| HistoryError::internal("history start request without start request"))?;
        let now = self.now();
        let domain_id = self.domain.info.id.clone();
        let mut g = self.inner.lock().unwrap();
        if g.next_event_id != 1 {
            return Err(HistoryError::internal(format!(
                "invalid history action: WorkflowExecutionStarted with next event id {}",
                g.next_event_id
            )));
        }
        let run_id = execution.run_id.clone();
        let backoff = request.first_decision_task_backoff_seconds;
        let parent = request.parent_execution_info.clone();
        let attrs = WorkflowExecutionStartedEventAttributes {
            workflow_type: start.workflow_type.clone(),
            parent_workflow_domain: parent.as_ref().map(|p| p.domain.clone()),
            parent_workflow_execution: parent.as_ref().and_then(|p| p.execution.clone()),
            parent_initiated_event_id: parent.as_ref().map(|p| p.initiated_id),
            task_list: start.task_list.clone(),
            input: start.input.clone(),
            execution_start_to_close_timeout_seconds: start.execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: start.task_start_to_close_timeout_seconds,
            initiator: request.continue_as_new_initiator,
            continued_failure_reason: request.continued_failure_reason.clone().unwrap_or_default(),
            continued_failure_details: request.continued_failure_details.clone(),
            last_completion_result: request.last_completion_result.clone(),
            original_execution_run_id: run_id.clone(),
            identity: start.identity.clone(),
            first_execution_run_id: prev.map(|p| p.first_run_id.clone()).unwrap_or_else(|| run_id.clone()),
            continued_execution_run_id: prev.map(|p| p.prev_run_id.clone()).unwrap_or_default(),
            parent_workflow_domain_id: parent.as_ref().map(|p| p.domain_uuid.clone()),
            first_scheduled_time_nano: match prev {
                Some(_) if start.cron_schedule.is_empty() => None,
                Some(p) => Some(p.first_scheduled_time_nano),
                None => Some(now + backoff.unwrap_or(0) as i64 * NANOS),
            },
            retry_policy: start.retry_policy.clone(),
            attempt: request.attempt,
            expiration_timestamp: request.expiration_timestamp,
            cron_schedule: start.cron_schedule.clone(),
            first_decision_task_backoff_seconds: backoff,
            memo: start.memo.clone(),
            search_attributes: start.search_attributes.clone(),
            header: start.header.clone(),
            partition_config: Some(request.partition_config.clone()),
            request_id: start.request_id.clone(),
            cron_overlap_policy: start.cron_overlap_policy,
            jitter_start_seconds: start.jitter_start_seconds,
            ..Default::default()
        };
        let event = g.push_event(EventType::WorkflowExecutionStarted, now, |e| {
            e.workflow_execution_started_event_attributes = Some(attrs.clone());
        });

        let version = event.version;
        let timeout_secs = start.execution_start_to_close_timeout_seconds.unwrap_or(0) as i64;
        let decision_timeout_secs = start.task_start_to_close_timeout_seconds.unwrap_or(0) as i64;
        let task_list = start.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        let type_name = start.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        {
            let i = &mut g.pc_exec;
            i.create_request_id = start.request_id.clone();
            i.domain_id = domain_id;
            i.workflow_id = execution.workflow_id.clone();
            i.run_id = run_id.clone();
            i.first_execution_run_id = prev.map(|p| p.first_run_id.clone()).unwrap_or(run_id);
            i.task_list = task_list;
            i.workflow_type_name = type_name;
            i.workflow_timeout_nanos = timeout_secs * NANOS;
            i.decision_start_to_close_timeout_nanos = decision_timeout_secs * NANOS;
            i.start_time_unix_nanos = now;
            i.state = WORKFLOW_STATE_CREATED;
            i.close_status = 0;
            i.last_processed_event = EMPTY_EVENT_ID;
            i.last_first_event_id = event.event_id;
            i.decision_version = EMPTY_VERSION;
            i.decision_schedule_id = EMPTY_EVENT_ID;
            i.decision_started_id = EMPTY_EVENT_ID;
            i.decision_request_id = EMPTY_UUID.to_string();
            i.decision_timeout_nanos = 0;
            i.cron_schedule = start.cron_schedule.clone();
            if let Some(p) = start.cron_overlap_policy {
                i.cron_overlap_policy = p as i32;
            }
            if let Some(p) = &parent {
                i.parent_domain_id = p.domain_uuid.clone();
                if let Some(pe) = &p.execution {
                    i.parent_workflow_id = pe.workflow_id.clone();
                    i.parent_run_id = pe.run_id.clone();
                }
                i.initiated_id = p.initiated_id;
            } else {
                i.initiated_id = EMPTY_EVENT_ID;
            }
            i.attempt = request.attempt;
            if let Some(ts) = request.expiration_timestamp {
                if ts != 0 {
                    i.expiration_time_unix_nanos = ts;
                }
            }
            if let Some(rp) = &start.retry_policy {
                i.has_retry_policy = true;
                i.backoff_coefficient = rp.backoff_coefficient;
                i.jitter_coefficient = rp.jitter_coefficient;
                i.expiration_interval_nanos = rp.expiration_interval_in_seconds as i64 * NANOS;
                i.initial_interval_nanos = rp.initial_interval_in_seconds as i64 * NANOS;
                i.maximum_attempts = rp.maximum_attempts;
                i.maximum_interval_nanos = rp.maximum_interval_in_seconds as i64 * NANOS;
                i.non_retriable_errors = rp.non_retriable_error_reasons.clone();
            }
            if let Some(m) = start.memo.as_ref().and_then(|m| m.fields.as_ref()) {
                i.memo = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            }
            if let Some(sa) = start.search_attributes.as_ref().and_then(|s| s.indexed_fields.as_ref()) {
                i.search_attributes = sa.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            }
            i.partition_config = request.partition_config.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }
        // The close transaction copies `last_processed_event` back into the
        // execution info; a run without a first decision (child workflow) must keep
        // EmptyEventID or ScheduleDecisionTask would see a "processed" decision.
        g.last_processed_event = EMPTY_EVENT_ID;
        let p = g.pc_exec.clone();
        {
            let e = &mut g.exec;
            e.domain_id = p.domain_id.clone();
            e.workflow_id = p.workflow_id.clone();
            e.run_id = p.run_id.clone();
            e.first_execution_run_id = p.first_execution_run_id.clone();
            e.create_request_id = p.create_request_id.clone();
            e.task_list = p.task_list.clone();
            e.workflow_type_name = p.workflow_type_name.clone();
            e.workflow_timeout = timeout_secs as i32;
            e.decision_start_to_close_timeout = decision_timeout_secs as i32;
            e.start_timestamp = now;
            e.state = p.state;
            e.close_status = p.close_status;
            e.last_updated_timestamp = now;
            e.cron_schedule = p.cron_schedule.clone();
            e.parent_domain_id = p.parent_domain_id.clone();
            e.parent_workflow_id = p.parent_workflow_id.clone();
            e.parent_run_id = p.parent_run_id.clone();
            e.initiated_id = p.initiated_id;
            e.attempt = p.attempt;
            e.has_retry_policy = p.has_retry_policy;
            e.memo = start.memo.as_ref().and_then(|m| m.fields.clone());
            e.search_attributes = start.search_attributes.as_ref().and_then(|s| s.indexed_fields.clone());
            e.partition_config = Some(request.partition_config.clone());
            e.cron_overlap_policy = start.cron_overlap_policy;
            e.active_cluster_selection_policy = start.active_cluster_selection_policy.clone();
        }
        g.mirror_decision();

        let mut timeout_at = now + timeout_secs * NANOS + backoff.unwrap_or(0) as i64 * NANOS;
        let expiration = g.pc_exec.expiration_time_unix_nanos;
        if request.attempt > 0 && expiration != 0 && timeout_at > expiration {
            timeout_at = expiration;
        }
        g.gen_workflow_start_timer(timeout_at, version);
        g.gen_record_workflow_started(version);
        g.dirty = true;
        Ok(event)
    }

    /// Go `AddFirstDecisionTaskScheduled`.
    pub(super) fn first_decision(&self, start_event: &HistoryEvent) -> HResult<()> {
        let attrs = start_event.workflow_execution_started_event_attributes.clone().unwrap_or_default();
        let backoff = attrs.first_decision_task_backoff_seconds.unwrap_or(0) as i64;
        if backoff == 0 {
            return self.add_decision_task_scheduled(false);
        }
        let backoff_type = match attrs.initiator {
            None | Some(ContinueAsNewInitiator::CronSchedule) => BACKOFF_TYPE_CRON,
            Some(ContinueAsNewInitiator::RetryPolicy) => BACKOFF_TYPE_RETRY,
            Some(_) => {
                return Err(HistoryError::internal("encounter continue as new iterator & first decision delay not 0"))
            }
        };
        let start_ts = start_event.timestamp.unwrap_or(0);
        let mut g = self.inner.lock().unwrap();
        g.gen_delayed_decision_timer(start_ts + backoff * NANOS, backoff_type, start_event.version);
        Ok(())
    }
}

/// Previous-run facts a ContinueAsNew start event needs.
pub(super) struct ContinuedFrom {
    pub prev_run_id: String,
    pub first_run_id: String,
    pub first_scheduled_time_nano: i64,
}

/// The closed transaction of a state that is created (start / continue-as-new).
pub(super) struct NewRunTxn {
    state: Arc<EngineMutableState>,
    txn: Mutex<Option<ClosedTxn>>,
    history: Mutex<(i64, Option<i64>)>,
}

pub(super) async fn append_new_branch(
    core: &EngineCore,
    txn: &mut ClosedTxn,
    info: &str,
) -> HResult<(i64, Option<i64>, PersistedBlobRef)> {
    let shard = &core.shard;
    let ids = shard.generate_task_ids(txn.events.len()).map_err(shard_err(shard.shard_id()))?;
    for (ev, id) in txn.events.iter_mut().zip(ids.iter()) {
        ev.task_id = *id;
    }
    let last = ids.last().copied();
    let transaction_id = shard.generate_task_id().map_err(shard_err(shard.shard_id()))?;
    let manager = SqlHistoryManager::new(core.history_v2.clone(), shard.shard_id())
        .with_transaction_size_limit(core.transaction_size_limit.clone());
    let blob = manager
        .append_history_nodes(&txn.branch_token, &txn.events, transaction_id, true, info)
        .await
        .map_err(to_history_error)?;
    let size = blob.data.len() as i64;
    txn.mutation.execution_info.history_size = size;
    if let Some(id) = last {
        txn.mutation.execution_info.last_event_task_id = id;
    }
    let first = txn.events.first().map(|e| e.event_id).unwrap_or(1);
    Ok((
        size,
        last,
        PersistedBlobRef { branch_token: txn.branch_token.clone(), first_event_id: first, events: txn.events.clone() },
    ))
}

/// Allocates ids for the category-3 replication tasks (Go allocates them after transfer and
/// timer tasks) and encodes them as `Task` rows. `max_read_level` is raised to the last id.
pub(super) fn replication_rows(
    core: &EngineCore,
    tasks: &mut [pc::ReplicationTask],
    max_read_level: &mut i64,
) -> HResult<Vec<pc::Task>> {
    let mut slots: Vec<(i64, i64)> = vec![(0, 0); tasks.len()];
    core.shard.allocate_transfer_ids(&mut slots, max_read_level).map_err(shard_err(core.shard.shard_id()))?;
    let mut rows = Vec::with_capacity(tasks.len());
    for (t, slot) in tasks.iter_mut().zip(slots.iter()) {
        t.task_id = slot.0;
        t.creation_time_unix_nanos = slot.1;
        let s = cadence_persistence::sqlblobs::replication_task_to_struct(t)
            .map_err(|e| HistoryError::internal(e.to_string()))?;
        rows.push(pc::Task {
            visibility_timestamp_unix_nanos: slot.1,
            task_type: t.task_type,
            task_id: slot.0,
            data: task_blob(Encoding::ThriftRW.encode(&s))?,
        });
    }
    Ok(rows)
}

#[allow(clippy::type_complexity)]
pub(super) fn snapshot_of(core: &EngineCore, mut txn: ClosedTxn) -> HResult<(pc::WorkflowSnapshot, i64, Vec<pc::TransferTask>, Vec<pc::TimerTask>)> {
    let shard = &core.shard;
    let mut max_read_level = 0i64;
    let mut transfer_slots: Vec<(i64, i64)> = txn.transfer.iter().map(|_| (0, 0)).collect();
    shard.allocate_transfer_ids(&mut transfer_slots, &mut max_read_level).map_err(shard_err(shard.shard_id()))?;
    let mut timer_slots: Vec<(i64, i64)> =
        txn.timer.iter().map(|t| (0, t.visibility_timestamp_unix_nanos)).collect();
    shard.allocate_timer_ids(&core.current_cluster, &mut timer_slots).map_err(shard_err(shard.shard_id()))?;
    let mut transfer_tasks = Vec::new();
    for (t, slot) in txn.transfer.iter_mut().zip(transfer_slots.iter()) {
        t.task_id = slot.0;
        t.visibility_time_unix_nanos = slot.1;
        let s = transfer_task_to_struct(t).map_err(|e| HistoryError::internal(e.to_string()))?;
        transfer_tasks.push(pc::Task {
            visibility_timestamp_unix_nanos: slot.1,
            task_type: t.task_type,
            task_id: slot.0,
            data: task_blob(Encoding::ThriftRW.encode(&s))?,
        });
    }
    let mut timer_tasks = Vec::new();
    for (t, slot) in txn.timer.iter_mut().zip(timer_slots.iter()) {
        t.task_id = slot.0;
        t.visibility_timestamp_unix_nanos = slot.1;
        let s = timer_task_to_struct(t).map_err(|e| HistoryError::internal(e.to_string()))?;
        timer_tasks.push(pc::Task {
            visibility_timestamp_unix_nanos: slot.1,
            task_type: t.task_type,
            task_id: slot.0,
            data: task_blob(Encoding::ThriftRW.encode(&s))?,
        });
    }
    let replication_tasks = replication_rows(core, &mut txn.replication, &mut max_read_level)?;
    let m = txn.mutation;
    let mut snapshot = pc::WorkflowSnapshot {
        activity_heartbeats: m.activity_heartbeats,
        execution_info: m.execution_info,
        version_histories: m.version_histories,
        start_version: m.start_version,
        last_write_version: m.last_write_version,
        activity_infos: m.upsert_activity_infos,
        child_execution_infos: m.upsert_child_execution_infos,
        signal_requested_ids: m.upsert_signal_requested_ids,
        condition: m.condition,
        ..Default::default()
    };
    if !transfer_tasks.is_empty() {
        snapshot
            .tasks_by_category
            .insert(pc::HistoryTaskCategory { category_type: 1, category_id: 1 }, transfer_tasks);
    }
    if !timer_tasks.is_empty() {
        snapshot
            .tasks_by_category
            .insert(pc::HistoryTaskCategory { category_type: 2, category_id: 2 }, timer_tasks);
    }
    if !replication_tasks.is_empty() {
        snapshot
            .tasks_by_category
            .insert(pc::HistoryTaskCategory { category_type: 1, category_id: 3 }, replication_tasks);
    }
    Ok((snapshot, max_read_level, txn.transfer, txn.timer))
}

fn downcast_state(state: &MutableStateRef) -> HResult<Arc<EngineMutableState>> {
    state
        .clone()
        .as_any_arc()
        .and_then(|a| a.downcast::<EngineMutableState>().ok())
        .ok_or_else(|| HistoryError::internal("mutable state is not an EngineMutableState"))
}

fn downcast_context(ctx: &ExecutionContextRef) -> HResult<&EngineExecutionContext> {
    ctx.as_any()
        .and_then(|a| a.downcast_ref::<EngineExecutionContext>())
        .ok_or_else(|| HistoryError::internal("execution context is not an EngineExecutionContext"))
}

fn downcast_txn(snapshot: &NewRunSnapshot) -> HResult<&NewRunTxn> {
    snapshot
        .downcast_ref::<NewRunTxn>()
        .ok_or_else(|| HistoryError::internal("new run snapshot is not a NewRunTxn"))
}

fn execution_info_string(ctx: &ExecutionContextRef) -> String {
    let e = ctx.get_execution();
    format!("{}:{}", e.workflow_id, e.run_id)
}

/// Implements [`WorkflowStartHost`] over the shard and the SQL stores.
pub struct EngineStartHost {
    core: Arc<EngineCore>,
    config: Arc<crate::config::Config>,
}

impl EngineStartHost {
    pub fn new(core: Arc<EngineCore>, config: Arc<crate::config::Config>) -> Arc<Self> {
        Arc::new(Self { core, config })
    }
}

#[async_trait]
impl WorkflowStartHost for EngineStartHost {
    fn shard_id(&self) -> i32 {
        self.core.shard.shard_id()
    }

    fn start_limits(&self, domain: &str) -> StartLimits {
        let c = &self.config;
        StartLimits {
            max_id_length_warn_limit: (c.max_id_length_warn_limit)() as usize,
            domain_name_max_length: (c.domain_name_max_length)(domain) as usize,
            workflow_id_max_length: (c.workflow_id_max_length)(domain) as usize,
            task_list_name_max_length: (c.task_list_name_max_length)(domain) as usize,
            workflow_type_max_length: (c.workflow_type_max_length)(domain) as usize,
            max_decision_start_to_close_seconds: (c.max_decision_start_to_close_seconds)(domain) as i32,
        }
    }

    fn enable_record_workflow_execution_uninitialized(&self, domain: &str) -> bool {
        (self.config.enable_record_workflow_execution_uninitialized)(domain)
    }

    fn enable_cleanup_orphaned_history_branch(&self, domain: &str) -> bool {
        (self.config.enable_cleanup_orphaned_history_branch_on_workflow_creation)(domain)
    }

    fn has_visibility(&self) -> bool {
        false
    }

    async fn active_cluster_info_by_cluster_attribute(
        &self,
        domain_id: &str,
        attribute: Option<&ClusterAttribute>,
    ) -> HResult<ActiveClusterInfo> {
        // activecluster.managerImpl.GetActiveClusterInfoByClusterAttribute
        let entry = self.core.domains.get_domain_by_id(domain_id)?;
        match entry.get_active_cluster_info_by_cluster_attribute(attribute) {
            Some((active_cluster_name, failover_version)) => Ok(ActiveClusterInfo { active_cluster_name, failover_version }),
            None => Err(HistoryError::ClusterAttributeNotFound(crate::engine::deps::cluster_attribute_not_found_message(domain_id, attribute))),
        }
    }

    async fn active_cluster_selection_policy_for_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> HResult<Option<cadence_api::types::ActiveClusterSelectionPolicy>> {
        self.core.active_cluster_manager.active_cluster_selection_policy_for_workflow(domain_id, workflow_id, run_id).await
    }

    fn new_mutable_state(
        &self,
        domain: &DomainCacheEntry,
        failover_version: i64,
        run_id: &str,
    ) -> HResult<MutableStateRef> {
        let core = &self.core;
        let ttl = (core.sticky_ttl_nanos)(&domain.info.name);
        let clock = core.clock.clone();
        let state = EngineMutableState::new_for_start(
            domain.clone(),
            failover_version,
            run_id,
            ttl,
            Arc::new(move || clock.now()),
            (core.state_params)(&domain.info.id),
        )?;
        Ok(Arc::new(state) as MutableStateRef)
    }

    fn new_execution_context(&self, domain_id: &str, execution: &WorkflowExecution) -> ExecutionContextRef {
        EngineExecutionContext::new(self.core.clone(), domain_id, execution) as ExecutionContextRef
    }

    fn close_transaction_as_snapshot(&self, state: &MutableStateRef, now: i64) -> HResult<NewRunSnapshot> {
        let state = downcast_state(state)?;
        let txn = state.close_transaction(now, true)?;
        Ok(Arc::new(NewRunTxn { state, txn: Mutex::new(Some(txn)), history: Mutex::new((0, None)) })
            as NewRunSnapshot)
    }

    async fn persist_start_workflow_batch_events(
        &self,
        context: &ExecutionContextRef,
        snapshot: &NewRunSnapshot,
    ) -> HResult<PersistedBlobRef> {
        let new_run = downcast_txn(snapshot)?;
        let mut txn = new_run
            .txn
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| HistoryError::internal("start transaction already persisted"))?;
        let (size, last, blob) = append_new_branch(&self.core, &mut txn, &execution_info_string(context)).await?;
        *new_run.history.lock().unwrap() = (size, last);
        *new_run.txn.lock().unwrap() = Some(txn);
        Ok(blob)
    }

    async fn create_workflow_execution(
        &self,
        context: &ExecutionContextRef,
        snapshot: &NewRunSnapshot,
        _history_blob: &PersistedBlobRef,
        mode: CreateWorkflowMode,
        prev_run_id: &str,
        prev_last_write_version: i64,
        request_mode: CreateWorkflowRequestMode,
    ) -> HResult<()> {
        let ctx = downcast_context(context)?;
        let new_run = downcast_txn(snapshot)?;
        // Keep the prepared transaction in place: the start flow may call this again with the same
        // snapshot (workflow-ID-reuse retry after an already-started failure), as Go does.
        let txn = new_run
            .txn
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| HistoryError::internal("start transaction not prepared"))?;
        let _task_write_gate = self.core.shard.lock_task_writes().await;
        let (snap, max_read_level, notify_transfer, notify_timer) = snapshot_of(&self.core, txn)?;
        let (size, last) = *new_run.history.lock().unwrap();
        let shard = &self.core.shard;
        let request = pc::CreateWorkflowExecutionRequest {
            shard_id: shard.shard_id(),
            range_id: shard.range_id(),
            mode: match mode {
                CreateWorkflowMode::BrandNew => pc::CreateWorkflowMode::BrandNew,
                CreateWorkflowMode::WorkflowIdReuse => pc::CreateWorkflowMode::WorkflowIdReuse,
                CreateWorkflowMode::ContinueAsNew => pc::CreateWorkflowMode::ContinueAsNew,
                CreateWorkflowMode::Zombie => pc::CreateWorkflowMode::Zombie,
            },
            previous_run_id: prev_run_id.to_string(),
            previous_last_write_version: prev_last_write_version,
            new_workflow_snapshot: snap,
            workflow_request_mode: match request_mode {
                CreateWorkflowRequestMode::New => pc::WorkflowRequestMode::New,
                CreateWorkflowRequestMode::Replicated => pc::WorkflowRequestMode::Replicated,
            },
            current_timestamp_unix_nanos: self.core.clock.now(),
        };
        match self.core.executions.create_workflow_execution(request).await {
            Ok(_) => {
                shard.update_max_read_level(max_read_level);
                self.core.notify_tasks(&notify_transfer, &notify_timer);
                new_run.state.finish_transaction(size, last);
                ctx.set_state(new_run.state.clone());
                Ok(())
            }
            Err(e) => {
                self.core.notify_tasks_after_failed_write(&e, &notify_transfer, &notify_timer);
                if matches!(e, pc::PersistenceError::ShardOwnershipLost { .. }) {
                    shard.close_shard(None);
                }
                Err(to_history_error(e))
            }
        }
    }

    async fn update_workflow_execution_with_new_as_active(
        &self,
        running: &ExecutionContextRef,
        now: i64,
        new_context: &ExecutionContextRef,
        new_state: &MutableStateRef,
    ) -> HResult<()> {
        let running_ctx = downcast_context(running)?;
        let new_ctx = downcast_context(new_context)?;
        let state = downcast_state(new_state)?;
        let mut txn = state.close_transaction(now, true)?;
        let (size, last, _blob) = append_new_branch(&self.core, &mut txn, &execution_info_string(new_context)).await?;
        let gate = self.core.shard.lock_task_writes().await;
        let (snap, max_read_level, _, _) = snapshot_of(&self.core, txn)?;
        running_ctx.update_with_new_as_active(now, snap, &gate).await?;
        self.core.shard.update_max_read_level(max_read_level);
        drop(gate);
        state.finish_transaction(size, last);
        new_ctx.set_state(state);
        Ok(())
    }

    async fn delete_history_branch(&self, _branch_token: &[u8], _domain_name: &str) -> HResult<()> {
        Ok(())
    }

    async fn record_workflow_execution_uninitialized(
        &self,
        _domain_id: &str,
        _domain: &str,
        _execution: &WorkflowExecution,
        _workflow_type_name: &str,
        _update_timestamp: i64,
    ) -> HResult<()> {
        Ok(())
    }

    async fn delete_visibility_workflow_execution(
        &self,
        _domain_id: &str,
        _domain: &str,
        _execution: &WorkflowExecution,
    ) -> HResult<()> {
        Ok(())
    }
}

/// Implements [`DomainFailoverHost`] over the shard and the domain cache.
pub struct EngineFailoverHost {
    shard: Arc<crate::shard_context::ShardContext>,
    domains: Arc<HistoryDomainCache>,
    executions: Arc<dyn cadence_persistence::history_contract::GoExecutionStore>,
    dlq_processor: Option<Arc<dyn crate::taskdlq::Processor>>,
}

impl EngineFailoverHost {
    pub fn new(
        shard: Arc<crate::shard_context::ShardContext>,
        domains: Arc<HistoryDomainCache>,
        executions: Arc<dyn cadence_persistence::history_contract::GoExecutionStore>,
        dlq_processor: Option<Arc<dyn crate::taskdlq::Processor>>,
    ) -> Arc<Self> {
        Arc::new(Self { shard, domains, executions, dlq_processor })
    }
}

/// Go `persistence.FailoverMarkerTask` as a `Task` row: replication task of type
/// `ReplicationTaskTypeFailoverMarker` (2) carrying domain id, version and creation time.
pub(crate) fn failover_marker_task(
    marker: &FailoverMarkerTask,
    task_id: i64,
    visibility_timestamp: i64,
) -> HResult<pc::Task> {
    let rt = pc::ReplicationTask {
        domain_id: marker.domain_id.clone(),
        // The store rewrites run/workflow ids with the fixed failover-marker values (Go
        // `createFailoverMarkerTasks`); the blob only needs parseable UUIDs.
        run_id: "30000000-5000-f000-f000-000000000000".into(),
        task_id,
        task_type: 2,
        version: marker.version,
        creation_time_unix_nanos: visibility_timestamp,
        ..Default::default()
    };
    let s = cadence_persistence::sqlblobs::replication_task_to_struct(&rt)
        .map_err(|e| HistoryError::internal(format!("encode failover marker: {e}")))?;
    Ok(pc::Task {
        visibility_timestamp_unix_nanos: visibility_timestamp,
        task_type: 2,
        task_id,
        data: task_blob(Encoding::ThriftRW.encode(&s))?,
    })
}

fn failover_entry(m: &MatchingDomainMetadata) -> FailoverDomainEntry {
    let scopes = m.replication.active_clusters.as_ref().map(|ac| {
        let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for (scope, s) in ac.active_clusters_by_cluster_attribute.iter().flatten() {
            let mut attrs = BTreeMap::new();
            for (name, info) in s.cluster_attributes.iter().flatten() {
                attrs.insert(name.clone(), info.active_cluster_name.clone());
            }
            out.insert(scope.clone(), attrs);
        }
        out
    });
    FailoverDomainEntry {
        id: m.info.uuid.clone(),
        name: m.info.name.clone(),
        is_global_domain: m.is_global_domain,
        is_pending_active: m.failover_end_time_nanos.is_some(),
        notification_version: m.notification_version,
        failover_notification_version: m.failover_notification_version,
        failover_version: m.failover_version,
        previous_failover_version: m.previous_failover_version,
        active_cluster_name: m.replication.active_cluster_name.clone(),
        active_cluster_scopes: scopes,
    }
}

#[async_trait]
impl DomainFailoverHost for EngineFailoverHost {
    fn domain_notification_version(&self) -> i64 {
        self.shard.get_domain_notification_version()
    }

    fn update_domain_notification_version(&self, version: i64) -> HResult<()> {
        self.shard.update_domain_notification_version(version).map_err(shard_err(self.shard.shard_id()))
    }

    /// Go `shard.ReplicateFailoverMarkers`.
    async fn replicate_failover_markers(&self, markers: Vec<FailoverMarkerTask>) -> HResult<()> {
        let _gate = self.shard.lock_task_writes().await;
        if let Some(e) = self.shard.closed_error() {
            return Err(shard_err(self.shard.shard_id())(e));
        }
        let mut slots: Vec<(i64, i64)> = vec![(0, 0); markers.len()];
        let mut max_read_level = 0i64;
        self.shard.allocate_transfer_ids(&mut slots, &mut max_read_level).map_err(shard_err(self.shard.shard_id()))?;
        let tasks = markers
            .iter()
            .zip(slots.iter())
            .map(|(m, (id, ts))| failover_marker_task(m, *id, *ts))
            .collect::<HResult<Vec<_>>>()?;
        let res = self
            .executions
            .create_failover_markers_with_range(self.shard.shard_id(), self.shard.range_id(), tasks)
            .await;
        match res {
            Ok(()) => {
                self.shard.update_max_read_level(max_read_level);
                Ok(())
            }
            Err(e @ cadence_persistence::history_contract::PersistenceError::ShardOwnershipLost { .. }) => {
                self.shard.close_shard(None);
                Err(to_history_error(e))
            }
            Err(e) => {
                tracing::error!(error = ?e, "failed to add failover marker tasks");
                Err(to_history_error(e))
            }
        }
    }

    fn has_dlq_processor(&self) -> bool {
        self.dlq_processor.is_some()
    }

    fn failover_dlq_partitions(&self, partitions: Vec<FailoverDlqPartition>) {
        if let Some(p) = &self.dlq_processor {
            p.failover_partitions(
                partitions
                    .into_iter()
                    .map(|x| crate::taskdlq::Partition {
                        domain_id: x.domain_id,
                        cluster_attribute_scope: x.cluster_attribute_scope,
                        cluster_attribute_name: x.cluster_attribute_name,
                    })
                    .collect(),
            );
        }
    }

    fn register_domain_change_callback(
        &self,
        shard_name: &str,
        initial_notification_version: i64,
        lock: DomainLockFn,
        changed: DomainChangedFn,
    ) {
        use cadence_matching::matching_domain_cache::{CatchUpCallback, DomainChangeCallback, PrepareCallback};
        let handle = tokio::runtime::Handle::current();
        let prepare: PrepareCallback = Arc::new(|| {});
        let on_changed: DomainChangeCallback = Arc::new(move |domains: &[MatchingDomainMetadata]| {
            let entries: Vec<FailoverDomainEntry> = domains.iter().map(failover_entry).collect();
            let (lock, changed) = (lock.clone(), changed.clone());
            handle.spawn(async move {
                lock().await;
                changed(entries).await;
            });
        });
        let catch_up: CatchUpCallback = Arc::new(move |all, _prepare, on_changed| {
            let mut metas: Vec<MatchingDomainMetadata> =
                all.iter().filter(|m| m.notification_version >= initial_notification_version).cloned().collect();
            if metas.is_empty() {
                return;
            }
            metas.sort_by_key(|m| m.notification_version);
            on_changed(&metas);
        });
        self.domains.inner().register_callback(shard_name.to_string(), catch_up, prepare, on_changed);
    }
}

#[cfg(test)]
mod failover_marker_tests {
    use super::*;

    #[test]
    fn failover_marker_task_is_replication_failover_marker_row() {
        let m = FailoverMarkerTask { version: 7, domain_id: "11111111-2222-3333-4444-555555555555".into() };
        let t = failover_marker_task(&m, 42, 1_000).unwrap();
        assert_eq!((t.task_id, t.task_type, t.visibility_timestamp_unix_nanos), (42, 2, 1_000));
        let v = Encoding::parse(&t.data.encoding).unwrap().decode(&t.data.data).unwrap();
        let rt = cadence_persistence::sqlblobs::replication_task_from_struct(&v).unwrap();
        assert_eq!(rt.domain_id, m.domain_id);
        assert_eq!((rt.version, rt.task_type, rt.creation_time_unix_nanos), (7, 2, 1_000));
    }

    #[test]
    fn failover_marker_task_rejects_bad_domain_id() {
        let m = FailoverMarkerTask { version: 1, domain_id: "not-a-uuid".into() };
        assert!(failover_marker_task(&m, 1, 1).is_err());
    }
}
