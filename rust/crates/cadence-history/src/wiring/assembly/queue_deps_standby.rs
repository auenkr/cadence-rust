//! Standby executor dependency adapters (Go `transferStandbyTaskExecutor` /
//! `timerStandbyTaskExecutor` dependencies).
//!
//! Done here: `TransferStandbyDependencies` (`TransferStandbyDepsImpl`) with the
//! `TransferWorkflow` adapter (`EngineTransferWorkflow`) over `EngineMutableState`.
//!
//! `TimerStandbyDependencies` (`TimerStandbyDepsImpl`): config, clock, history resend and DLQ are
//! real; `load_workflow` returns an explicit error because `EngineMutableState` does not track
//! user timer infos nor implement the timer sequence / `update_as_passive` (TODO(link): needs
//! timer state in engine_state.rs, shared with TimerActiveDependencies).
//!
//! Known deviations (TODO(link)):
//! - request-cancel / signal infos are read from the loaded snapshot, which is what
//!   a freshly loaded standby state holds (standby processing never mutates them).
//! - `advanced_visibility_writing_enabled` is false: the advanced visibility
//!   writing mode is not part of the Rust config yet.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;
use cadence_persistence::sqlblobs::{request_cancel_info_from_struct, signal_info_from_struct};

use super::engine_cache::{EngineExecutionCache, EngineExecutionContext};
use super::engine_state::{decode, EngineMutableState};
use super::env::HistoryEnv;
use crate::engine::deps::{HistoryError, ReleaseFunc, WorkflowExecution, EMPTY_EVENT_ID};
use crate::engine::deps_ms::MutableState as _;
use crate::replication::deps::HistoryResender;
use crate::shard_context::ShardContext;
use crate::task::deps::{
    ClusterAttribute, DomainCache, PersistenceTask, Result as TaskResult, StringPropertyFnWithDomainFilter, TaskError,
    TaskList, TaskListKind, Time,
};
use crate::task::standby_task_util::{HistoryResendInfo, TaskDLQWriter};
use crate::task::timer_standby_task_executor::{StandbyTimerWorkflow, TimerStandbyDependencies};
use crate::task::transfer_standby_task_executor::{
    TransferActivityInfo, TransferDecisionInfo, TransferPendingInfo, TransferStandbyDependencies, TransferWorkflow,
};
use crate::task::transfer_task_executor_base::{
    get_workflow_execution_timestamp, DecisionState, StartEvent, VisibilityRecord, WorkflowExecutionStatus,
};
use crate::wiring::persistence::domain_dlq::SqlTaskDlqWriter;

fn he(e: HistoryError) -> TaskError {
    match e {
        HistoryError::Cadence(c) => match *c {
            cadence_api::errors::CadenceError::EntityNotExists(x) => TaskError::EntityNotExists(x.message),
            cadence_api::errors::CadenceError::ServiceBusy(x) => TaskError::ServiceBusy(x.message),
            other => TaskError::InternalService(other.to_string()),
        },
        HistoryError::DeadlineExceeded => TaskError::ContextDeadlineExceeded,
        HistoryError::Canceled => TaskError::ContextCanceled,
        HistoryError::PersistenceShardOwnershipLost(m) => TaskError::ShardOwnershipLost(m),
        other => TaskError::InternalService(other.to_string()),
    }
}

fn task_list_kind(kind: i32) -> TaskListKind {
    match kind {
        1 => TaskListKind::Sticky,
        2 => TaskListKind::Ephemeral,
        _ => TaskListKind::Normal,
    }
}

/// Go `loadMutableState`: loads the state of a locked context and refreshes a stale cache.
pub(super) async fn load_state_for_task(
    context: &EngineExecutionContext,
    task: &dyn PersistenceTask,
    event_id: i64,
) -> TaskResult<Option<Arc<EngineMutableState>>> {
    let state = match context.load_state().await {
        Ok(s) => s,
        Err(HistoryError::Cadence(c)) if matches!(*c, cadence_api::errors::CadenceError::EntityNotExists(_)) => {
            return Ok(None)
        }
        Err(e) => return Err(he(e)),
    };
    let info = state.get_execution_info();
    let is_decision_retry = (task.get_task_type() == crate::task::deps::ptask_type::TASK_TYPE_DECISION_TIMEOUT
        || task.get_task_type() == crate::task::deps::ptask_type::TRANSFER_TASK_TYPE_DECISION_TASK)
        && info.decision_schedule_id == event_id
        && info.decision_attempt > 0;
    if event_id < state.get_next_event_id() || is_decision_retry {
        return Ok(Some(state));
    }
    // Stale mutable state: reload from persistence.
    context.clear();
    let state = context.load_state().await.map_err(he)?;
    if event_id >= state.get_next_event_id() {
        // Go logs data inconsistency and skips the task.
        return Ok(None);
    }
    Ok(Some(state))
}

/// `TransferWorkflow` over a locked execution context and its loaded mutable state.
pub(super) struct EngineTransferWorkflow {
    state: Arc<EngineMutableState>,
    release: ReleaseFunc,
    shard_clusters: usize,
    domains: Arc<dyn DomainCache>,
}

impl DecisionState for EngineTransferWorkflow {
    fn has_pending_decision(&self) -> bool {
        self.state.has_pending_decision()
    }
    fn has_in_flight_decision(&self) -> bool {
        self.state.has_in_flight_decision()
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.state.has_processed_or_pending_decision()
    }
}

fn pending(version: i64, started_id: i64) -> TransferPendingInfo {
    TransferPendingInfo { version, started_id }
}

#[async_trait]
impl TransferWorkflow for EngineTransferWorkflow {
    fn release(&self, error: Option<TaskError>) {
        match error {
            Some(e) => (self.release)(Some(&HistoryError::internal(e.to_string()))),
            None => (self.release)(None),
        }
    }
    fn running(&self) -> bool {
        self.state.is_workflow_execution_running()
    }
    fn activity_info(&self, id: i64) -> Option<TransferActivityInfo> {
        let inner = self.state.inner.lock().unwrap();
        let a = inner.pc_acts.get(&id)?;
        Some(TransferActivityInfo {
            version: a.version,
            started_id: a.started_id,
            task_list: TaskList { name: a.task_list.clone(), kind: task_list_kind(a.task_list_kind) },
            schedule_to_start_timeout: a.schedule_to_start_timeout_seconds,
        })
    }
    fn decision_info(&self, id: i64) -> Option<TransferDecisionInfo> {
        let inner = self.state.inner.lock().unwrap();
        (inner.pc_exec.decision_schedule_id == id && id != EMPTY_EVENT_ID).then(|| TransferDecisionInfo {
            version: inner.pc_exec.decision_version,
            started_id: inner.pc_exec.decision_started_id,
        })
    }
    fn request_cancel_info(&self, id: i64) -> Option<TransferPendingInfo> {
        let blob = self.state.snapshot.request_cancel_infos.iter().find(|b| b.key == id)?;
        let info = request_cancel_info_from_struct(&decode(&blob.value).ok()?).ok()?;
        Some(pending(info.version, EMPTY_EVENT_ID))
    }
    fn signal_info(&self, id: i64) -> Option<TransferPendingInfo> {
        let blob = self.state.snapshot.signal_infos.iter().find(|b| b.key == id)?;
        let info = signal_info_from_struct(&decode(&blob.value).ok()?).ok()?;
        Some(pending(info.version, EMPTY_EVENT_ID))
    }
    fn child_execution_info(&self, id: i64) -> Option<TransferPendingInfo> {
        let inner = self.state.inner.lock().unwrap();
        inner.pc_children.get(&id).map(|c| pending(c.version, c.started_id))
    }
    fn task_list(&self) -> TaskList {
        let inner = self.state.inner.lock().unwrap();
        // Go builds the task list from the sticky list when enabled, else the normal one.
        if self.state.is_sticky_task_list_enabled() {
            TaskList { name: inner.exec.sticky_task_list.clone(), kind: TaskListKind::Sticky }
        } else {
            TaskList { name: inner.exec.task_list.clone(), kind: task_list_kind(inner.pc_exec.task_list_kind) }
        }
    }
    fn workflow_timeout(&self) -> i32 {
        self.state.get_execution_info().workflow_timeout
    }
    fn sticky_schedule_to_start_timeout(&self) -> i32 {
        self.state.get_execution_info().sticky_schedule_to_start_timeout
    }
    fn partition_config(&self) -> BTreeMap<String, Vec<u8>> {
        self.state
            .get_execution_info()
            .partition_config
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect()
    }
    fn history_resend_info(&self) -> TaskResult<HistoryResendInfo> {
        let histories = self.state.get_version_histories().ok_or(TaskError::MissingVersionHistories)?;
        let current = histories
            .histories
            .get(histories.current_version_history_index as usize)
            .ok_or_else(|| TaskError::Other("current version history index out of range".into()))?;
        let last = current.items.last().ok_or_else(|| TaskError::Other("version history is empty".into()))?;
        Ok(HistoryResendInfo { last_event_id: Some(last.event_id), last_event_version: Some(last.version) })
    }
    fn last_write_version(&self) -> TaskResult<i64> {
        self.state.get_last_write_version().map_err(he)
    }
    fn start_version(&self) -> TaskResult<i64> {
        // Go `GetStartVersion` = version of the first item of the current version history.
        let histories = self.state.get_version_histories().ok_or(TaskError::MissingVersionHistories)?;
        let current = histories
            .histories
            .get(histories.current_version_history_index as usize)
            .ok_or_else(|| TaskError::Other("current version history index out of range".into()))?;
        Ok(current.items.first().map(|i| i.version).unwrap_or(self.state.get_current_version()))
    }
    async fn start_event(&self) -> TaskResult<StartEvent> {
        let event = self.state.get_start_event().await.map_err(he)?;
        Ok(start_event_of(&event))
    }
    async fn visibility_record(&self, task: &dyn PersistenceTask, now: Time, closed: bool) -> TaskResult<VisibilityRecord> {
        let start = self.state.get_start_event().await.map_err(he)?;
        let start_event = start_event_of(&start);
        let info = self.state.get_execution_info();
        let (close_timestamp, execution_status) = if closed {
            let completion = self.state.get_completion_event().await.map_err(he)?;
            (completion.timestamp.unwrap_or_default(), close_execution_status(&completion))
        } else {
            (0, WorkflowExecutionStatus::Pending)
        };
        let execution_timestamp = get_workflow_execution_timestamp(Some(&start_event));
        let is_cron = !info.cron_schedule.is_empty();
        let num_clusters = self
            .domains
            .get_domain_by_id(task.get_domain_id())
            .map(|d| d.get_replication_config().clusters.len())
            .unwrap_or(self.shard_clusters);
        let scheduled = if start_event.first_decision_task_backoff_seconds > 0 {
            start_event.timestamp + i64::from(start_event.first_decision_task_backoff_seconds) * 1_000_000_000
        } else {
            start_event.timestamp
        };
        let (workflow_timeout, history_length) = (i64::from(info.workflow_timeout), self.state.get_next_event_id() - 1);
        Ok(VisibilityRecord {
            domain_id: task.get_domain_id().to_string(),
            domain: String::new(),
            workflow_id: task.get_workflow_id().to_string(),
            run_id: task.get_run_id().to_string(),
            workflow_type_name: info.workflow_type_name.clone(),
            start_timestamp: start_event.timestamp,
            execution_timestamp: execution_timestamp.unix_nano(),
            close_timestamp,
            workflow_timeout,
            task_id: task.get_task_id(),
            memo: info.memo.clone().map(|m| m.into_iter().collect()),
            task_list: info.task_list.clone(),
            is_cron,
            cron_schedule: if is_cron { info.cron_schedule.clone() } else { String::new() },
            num_clusters: num_clusters as i16,
            cluster_attribute: info
                .active_cluster_selection_policy
                .as_ref()
                .and_then(|p| p.cluster_attribute.as_ref())
                .map(|a| ClusterAttribute { scope: a.scope.clone(), name: a.name.clone() }),
            update_timestamp: now.unix_nano(),
            search_attributes: info.search_attributes.clone().unwrap_or_default().into_iter().collect(),
            headers: start_event.headers.clone().unwrap_or_default(),
            shard_id: 0,
            execution_status,
            scheduled_execution_timestamp: scheduled,
            close_status: if closed { info.close_status } else { 0 },
            history_length: if closed { history_length } else { 0 },
            retention_seconds: 0,
        })
    }
}

fn start_event_of(event: &HistoryEvent) -> StartEvent {
    let attrs = event.workflow_execution_started_event_attributes.as_ref();
    StartEvent {
        timestamp: event.timestamp.unwrap_or_default(),
        first_decision_task_backoff_seconds: attrs.and_then(|a| a.first_decision_task_backoff_seconds).unwrap_or(0),
        headers: attrs
            .and_then(|a| a.header.as_ref())
            .and_then(|h| h.fields.clone())
            .map(|f| f.into_iter().collect()),
    }
}

/// Go `getWorkflowExecutionStatus`.
fn close_execution_status(event: &HistoryEvent) -> WorkflowExecutionStatus {
    match event.event_type {
        Some(EventType::WorkflowExecutionCompleted) => WorkflowExecutionStatus::Completed,
        Some(EventType::WorkflowExecutionFailed) => WorkflowExecutionStatus::Failed,
        Some(EventType::WorkflowExecutionCanceled) => WorkflowExecutionStatus::Canceled,
        Some(EventType::WorkflowExecutionTerminated) => WorkflowExecutionStatus::Terminated,
        Some(EventType::WorkflowExecutionTimedOut) => WorkflowExecutionStatus::TimedOut,
        Some(EventType::WorkflowExecutionContinuedAsNew) => {
            let reason = event
                .workflow_execution_continued_as_new_event_attributes
                .as_ref()
                .map(|a| a.failure_reason.clone().unwrap_or_default());
            match reason {
                Some(r) if !r.is_empty() => {
                    let lower = r.to_lowercase();
                    if lower.contains("timeout") || lower.contains("timed out") {
                        WorkflowExecutionStatus::TimedOut
                    } else {
                        WorkflowExecutionStatus::Failed
                    }
                }
                Some(_) => WorkflowExecutionStatus::Completed,
                None => WorkflowExecutionStatus::ContinuedAsNew,
            }
        }
        _ => WorkflowExecutionStatus::Completed,
    }
}

/// `TransferStandbyDependencies` over the shard's execution cache, config and history resender.
pub struct TransferStandbyDepsImpl {
    env: Arc<HistoryEnv>,
    shard: Arc<ShardContext>,
    cache: Arc<EngineExecutionCache>,
    resender: Arc<dyn HistoryResender>,
    domains: Arc<dyn DomainCache>,
}

impl TransferStandbyDepsImpl {
    pub fn new(
        env: Arc<HistoryEnv>,
        shard: Arc<ShardContext>,
        cache: Arc<EngineExecutionCache>,
        resender: Arc<dyn HistoryResender>,
    ) -> Arc<Self> {
        let domains = env.domain_cache.clone() as Arc<dyn DomainCache>;
        Arc::new(Self { env, shard, cache, resender, domains })
    }
}

#[async_trait]
impl TransferStandbyDependencies for TransferStandbyDepsImpl {
    async fn load_workflow(&self, task: &dyn PersistenceTask, event_id: i64) -> TaskResult<Option<Arc<dyn TransferWorkflow>>> {
        let execution = WorkflowExecution {
            workflow_id: task.get_workflow_id().to_string(),
            run_id: task.get_run_id().to_string(),
        };
        let (context, release) = self.cache.get_or_create(task.get_domain_id(), &execution).await.map_err(he)?;
        match load_state_for_task(&context, task, event_id).await {
            Ok(Some(state)) => Ok(Some(Arc::new(EngineTransferWorkflow {
                state,
                release,
                shard_clusters: 0,
                domains: self.domains.clone(),
            }))),
            Ok(None) => {
                release(None);
                Ok(None)
            }
            Err(e) => {
                release(Some(&HistoryError::internal(e.to_string())));
                Err(e)
            }
        }
    }
    fn enable_transfer_queue_v2(&self, shard_id: i32) -> bool {
        (self.env.config.enable_transfer_queue_v2)(shard_id)
    }
    fn current_time(&self, cluster: &str) -> Time {
        Time(self.shard.get_current_time(cluster))
    }
    fn resend_delay(&self) -> Duration {
        (self.env.config.standby_task_missing_events_resend_delay)()
    }
    fn discard_delay(&self) -> Duration {
        (self.env.config.standby_task_missing_events_discard_delay)()
    }
    fn advanced_visibility_writing_enabled(&self) -> bool {
        // TODO(link): advanced visibility writing mode is not in the Rust config; SQL visibility only.
        false
    }
    async fn send_single_workflow_history(&self, cluster: &str, task: &dyn PersistenceTask, id: i64, version: i64) -> TaskResult<()> {
        self.resender
            .send_single_workflow_history(
                cluster,
                task.get_domain_id(),
                task.get_workflow_id(),
                task.get_run_id(),
                Some(id),
                Some(version),
                None,
                None,
            )
            .await
            .map_err(|e| TaskError::InternalService(e.to_string()))
    }
    fn dlq_writer(&self) -> Arc<dyn TaskDLQWriter> {
        Arc::new(SqlTaskDlqWriter)
    }
    fn history_task_dlq_mode(&self) -> StringPropertyFnWithDomainFilter {
        let f = self.env.config.history_task_dlq_mode.clone();
        Arc::new(move |domain: &str| f(domain))
    }
}

/// `TimerStandbyDependencies` over the shard's config, clock and history resender.
pub struct TimerStandbyDepsImpl {
    env: Arc<HistoryEnv>,
    shard: Arc<ShardContext>,
    resender: Arc<dyn HistoryResender>,
}

impl TimerStandbyDepsImpl {
    pub fn new(env: Arc<HistoryEnv>, shard: Arc<ShardContext>, resender: Arc<dyn HistoryResender>) -> Arc<Self> {
        Arc::new(Self { env, shard, resender })
    }
}

#[async_trait]
impl TimerStandbyDependencies for TimerStandbyDepsImpl {
    async fn load_workflow(&self, _task: &dyn PersistenceTask, _event_id: i64) -> TaskResult<Option<Arc<dyn StandbyTimerWorkflow>>> {
        // TODO(link): EngineMutableState has no user timer infos, timer sequence or update_as_passive yet.
        Err(TaskError::Other("timer standby workflow is not supported yet: timer state is not ported to EngineMutableState".into()))
    }
    fn standby_cluster_delay(&self) -> Duration {
        (self.env.config.standby_cluster_delay)()
    }
    fn current_time(&self, cluster: &str) -> Time {
        Time(self.shard.get_current_time(cluster))
    }
    fn enable_timer_queue_v2(&self, shard_id: i32) -> bool {
        (self.env.config.enable_timer_queue_v2)(shard_id)
    }
    fn resend_delay(&self) -> Duration {
        (self.env.config.standby_task_missing_events_resend_delay)()
    }
    fn discard_delay(&self) -> Duration {
        (self.env.config.standby_task_missing_events_discard_delay)()
    }
    fn delete_history_event_context_timeout(&self) -> Duration {
        Duration::from_secs((self.env.config.delete_history_event_context_timeout)().max(0) as u64)
    }
    fn dlq_writer(&self) -> Arc<dyn TaskDLQWriter> {
        Arc::new(SqlTaskDlqWriter)
    }
    fn history_task_dlq_mode(&self) -> StringPropertyFnWithDomainFilter {
        let f = self.env.config.history_task_dlq_mode.clone();
        Arc::new(move |domain: &str| f(domain))
    }
    async fn send_single_workflow_history(&self, cluster: &str, task: &dyn PersistenceTask, last_event_id: i64, last_event_version: i64) -> TaskResult<()> {
        self.resender
            .send_single_workflow_history(
                cluster,
                task.get_domain_id(),
                task.get_workflow_id(),
                task.get_run_id(),
                Some(last_event_id),
                Some(last_event_version),
                None,
                None,
            )
            .await
            .map_err(|e| TaskError::InternalService(e.to_string()))
    }
}
