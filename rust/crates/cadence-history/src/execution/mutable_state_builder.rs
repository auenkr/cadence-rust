//! Typed, Go-shaped mutable state from `service/history/execution/mutable_state_builder.go`.
//!
//! The legacy `crate::mutable_state::MutableStateBuilder` is a JSON-record facade.
//! This type keeps the same maps as Go's `mutableStateBuilder` with concrete
//! value types so state rebuilder, repairer, integrity and execution context
//! can be ported against it.

use std::collections::{BTreeMap, BTreeSet};

use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::{VersionHistories, VersionHistoryItem};
use cadence_persistence::history_contract::{
    HistoryTaskCategory, Task, WorkflowExecutionInfo, WorkflowRequest,
};
use cadence_persistence::version_history::{new_item, VersionHistoriesExt, VersionHistoryExt};

use super::cancellation::RequestCancelInfo;
use super::child_workflow::ChildExecutionInfo;
use super::signal::SignalInfo;
use super::timer::TimerInfo;
use crate::mutable_state_activity::ActivityInfo;
use crate::mutable_state_builder_ext as ext;

const WORKFLOW_STATE_CREATED: i32 = ext::WORKFLOW_STATE_CREATED as i32;
const WORKFLOW_STATE_RUNNING: i32 = ext::WORKFLOW_STATE_RUNNING as i32;
const WORKFLOW_STATE_COMPLETED: i32 = ext::WORKFLOW_STATE_COMPLETED as i32;
const WORKFLOW_STATE_ZOMBIE: i32 = ext::WORKFLOW_STATE_ZOMBIE as i32;
const WORKFLOW_STATE_VOID: i32 = ext::WORKFLOW_STATE_VOID as i32;
const WORKFLOW_STATE_CORRUPTED: i32 = ext::WORKFLOW_STATE_CORRUPTED as i32;

pub const FIRST_EVENT_ID: i64 = 1;
pub const EMPTY_EVENT_ID: i64 = -23;
pub const EMPTY_VERSION: i64 = -24;
pub const BUFFERED_EVENT_ID: i64 = -123;
pub const EMPTY_EVENT_TASK_ID: i64 = -1234;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypedMutableStateError {
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    BadRequest(String),
}

type Result<T> = std::result::Result<T, TypedMutableStateError>;

fn internal<T>(message: impl Into<String>) -> Result<T> {
    Err(TypedMutableStateError::Internal(message.into()))
}

/// Go `persistence.WorkflowMutation` with concrete types.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypedWorkflowMutation {
    pub execution_info: WorkflowExecutionInfo,
    pub version_histories: Option<VersionHistories>,
    pub start_version: i64,
    pub last_write_version: i64,
    pub upsert_activity_infos: Vec<ActivityInfo>,
    pub delete_activity_infos: Vec<i64>,
    pub rewrite_activity_infos: Vec<ActivityInfo>,
    pub upsert_timer_infos: Vec<TimerInfo>,
    pub delete_timer_infos: Vec<String>,
    pub rewrite_timer_infos: Vec<TimerInfo>,
    pub upsert_child_execution_infos: Vec<ChildExecutionInfo>,
    pub delete_child_execution_infos: Vec<i64>,
    pub upsert_request_cancel_infos: Vec<RequestCancelInfo>,
    pub delete_request_cancel_infos: Vec<i64>,
    pub upsert_signal_infos: Vec<SignalInfo>,
    pub delete_signal_infos: Vec<i64>,
    pub upsert_signal_requested_ids: Vec<String>,
    pub delete_signal_requested_ids: Vec<String>,
    pub upsert_semaphore_infos: Vec<super::semaphore::SemaphoreInfo>,
    pub delete_semaphore_infos: Vec<i64>,
    pub new_buffered_events: Vec<HistoryEvent>,
    pub clear_buffered_events: bool,
    pub tasks_by_category: BTreeMap<HistoryTaskCategory, Vec<Task>>,
    pub workflow_requests: Vec<WorkflowRequest>,
    pub condition: i64,
}

/// Go `persistence.WorkflowSnapshot` with concrete types.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypedWorkflowSnapshot {
    pub execution_info: WorkflowExecutionInfo,
    pub version_histories: Option<VersionHistories>,
    pub start_version: i64,
    pub last_write_version: i64,
    pub activity_infos: Vec<ActivityInfo>,
    pub timer_infos: Vec<TimerInfo>,
    pub child_execution_infos: Vec<ChildExecutionInfo>,
    pub request_cancel_infos: Vec<RequestCancelInfo>,
    pub signal_infos: Vec<SignalInfo>,
    pub signal_requested_ids: Vec<String>,
    pub semaphore_infos: Vec<super::semaphore::SemaphoreInfo>,
    pub tasks_by_category: BTreeMap<HistoryTaskCategory, Vec<Task>>,
    pub workflow_requests: Vec<WorkflowRequest>,
    pub condition: i64,
}

/// Emits Go `metrics.DataInconsistentCounter` on `WorkflowContextScope` (installed by the
/// engine wiring; absent in unit tests).
#[derive(Clone, Default)]
pub struct DataInconsistencyHook(pub Option<std::sync::Arc<dyn Fn() + Send + Sync>>);

impl std::fmt::Debug for DataInconsistencyHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataInconsistencyHook")
    }
}

impl DataInconsistencyHook {
    pub fn emit(&self) {
        if let Some(f) = &self.0 {
            f();
        }
    }
}

/// Go `mutableStateBuilder`. Fields mirror the Go struct.
#[derive(Debug, Clone, Default)]
pub struct TypedMutableState {
    /// Go `metricsClient.Scope(WorkflowContextScope).IncCounter(DataInconsistentCounter)`.
    pub data_inconsistency: DataInconsistencyHook,
    pub pending_activity_info_ids: BTreeMap<i64, ActivityInfo>,
    pub pending_activity_id_to_event_id: BTreeMap<String, i64>,
    pub update_activity_infos: BTreeMap<i64, ActivityInfo>,
    pub delete_activity_infos: BTreeSet<i64>,
    pub sync_activity_tasks: BTreeSet<i64>,

    pub pending_timer_info_ids: BTreeMap<String, TimerInfo>,
    pub pending_timer_event_id_to_id: BTreeMap<i64, String>,
    pub update_timer_infos: BTreeMap<String, TimerInfo>,
    pub delete_timer_infos: BTreeSet<String>,

    pub pending_child_execution_info_ids: BTreeMap<i64, ChildExecutionInfo>,
    pub update_child_execution_infos: BTreeMap<i64, ChildExecutionInfo>,
    pub delete_child_execution_info_ids: BTreeSet<i64>,

    pub pending_request_cancel_info_ids: BTreeMap<i64, RequestCancelInfo>,
    pub update_request_cancel_infos: BTreeMap<i64, RequestCancelInfo>,
    pub delete_request_cancel_info_ids: BTreeSet<i64>,

    pub pending_signal_info_ids: BTreeMap<i64, SignalInfo>,
    pub update_signal_infos: BTreeMap<i64, SignalInfo>,
    pub delete_signal_info_ids: BTreeSet<i64>,

    /// Go `pendingSemaphoreInfoIDs` / `updateSemaphoreInfos` / `deleteSemaphoreInfoIDs`.
    pub semaphore_state: super::semaphore::SemaphoreState,
    pub pending_signal_requested_ids: BTreeSet<String>,
    pub update_signal_requested_ids: BTreeSet<String>,
    pub delete_signal_requested_ids: BTreeSet<String>,

    pub buffered_events: Vec<HistoryEvent>,
    pub update_buffered_events: Vec<HistoryEvent>,
    pub clear_buffered_events: bool,
    pub has_buffered_events_in_db: bool,

    pub execution_info: WorkflowExecutionInfo,
    pub version_histories: Option<VersionHistories>,
    pub current_version: i64,
    pub next_event_id_in_db: i64,
    pub state_in_db: i32,
    pub history_size: i64,
    pub tasks: BTreeMap<HistoryTaskCategory, Vec<Task>>,
    pub workflow_requests: Vec<WorkflowRequest>,
    pub insert_workflow_requests: Vec<WorkflowRequest>,
    pub condition: i64,
    /// Go `hBuilder` events produced in the current transaction.
    pub history_events: Vec<HistoryEvent>,
    /// Go `insertTransferTasks`/`insertTimerTasks` (AddTransferTasks/AddTimerTasks
    /// target); persistence blob encoding happens at the persistence layer.
    pub transfer_tasks: Vec<super::mutable_state_task_generator::GeneratedTask>,
    pub timer_tasks: Vec<super::mutable_state_task_generator::GeneratedTask>,
    /// Go `taskGenerator` environment (domain cache, config, rand).
    pub task_gen_env: super::mutable_state_methods_common::TaskGenEnv,
    /// Suppress active queue generation while applying passive replicated history.
    pub suppress_task_generation: bool,
    /// Go `clusterMetadata` + `canReplicateEvents` (None until the load path installs it).
    pub failover: Option<super::mutable_state_failover::FailoverCluster>,
}

impl TypedMutableState {
    /// Go `ReplicateUpsertWorkflowSearchAttributesEvent`.
    pub fn replicate_upsert_workflow_search_attributes_event(&mut self, event: &HistoryEvent) -> Result<()> {
        if let Some(fields) = event.upsert_workflow_search_attributes_event_attributes.as_ref()
            .and_then(|a| a.search_attributes.as_ref())
            .and_then(|a| a.indexed_fields.as_ref()) {
            self.execution_info.search_attributes.extend(fields.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        if !self.suppress_task_generation {
            self.run_task_generator(0, |g| g.generate_workflow_search_attr_tasks())?;
        }
        Ok(())
    }

    /// Go `newMutableStateBuilder`.
    pub fn new() -> Self {
        Self {
            current_version: EMPTY_VERSION,
            next_event_id_in_db: FIRST_EVENT_ID,
            state_in_db: WORKFLOW_STATE_VOID,
            execution_info: WorkflowExecutionInfo {
                next_event_id: FIRST_EVENT_ID,
                last_processed_event: EMPTY_EVENT_ID,
                last_first_event_id: EMPTY_EVENT_ID,
                state: WORKFLOW_STATE_CREATED,
                decision_schedule_id: EMPTY_EVENT_ID,
                decision_started_id: EMPTY_EVENT_ID,
                decision_version: EMPTY_VERSION,
                ..WorkflowExecutionInfo::default()
            },
            ..Self::default()
        }
    }

    /// Go `Load` semaphore map (`pendingSemaphoreInfoIDs`), installed after `load`.
    pub fn load_semaphores(&mut self, semaphores: Vec<super::semaphore::SemaphoreInfo>) {
        self.semaphore_state = super::semaphore::SemaphoreState {
            pending: semaphores.into_iter().map(|s| (s.initiated_id, s)).collect(),
            ..Default::default()
        };
    }

    /// Go `Load`: install persisted state and rebuild the derived indexes.
    #[allow(clippy::too_many_arguments)]
    pub fn load(
        &mut self,
        execution_info: WorkflowExecutionInfo,
        activities: Vec<ActivityInfo>,
        timers: Vec<TimerInfo>,
        children: Vec<ChildExecutionInfo>,
        request_cancels: Vec<RequestCancelInfo>,
        signals: Vec<SignalInfo>,
        signal_requested_ids: Vec<String>,
        buffered_events: Vec<HistoryEvent>,
        version_histories: Option<VersionHistories>,
        history_size: i64,
    ) {
        self.pending_activity_info_ids = activities.into_iter().map(|a| (a.schedule_id, a)).collect();
        self.pending_activity_id_to_event_id = self
            .pending_activity_info_ids
            .values()
            .map(|a| (a.activity_id.clone(), a.schedule_id))
            .collect();
        self.pending_timer_info_ids = timers.into_iter().map(|t| (t.timer_id.clone(), t)).collect();
        self.pending_timer_event_id_to_id = self
            .pending_timer_info_ids
            .values()
            .map(|t| (t.started_id, t.timer_id.clone()))
            .collect();
        self.pending_child_execution_info_ids =
            children.into_iter().map(|c| (c.initiated_id, c)).collect();
        self.pending_request_cancel_info_ids =
            request_cancels.into_iter().map(|c| (c.initiated_id, c)).collect();
        self.pending_signal_info_ids = signals.into_iter().map(|s| (s.initiated_id, s)).collect();
        self.pending_signal_requested_ids = signal_requested_ids.into_iter().collect();
        self.has_buffered_events_in_db = !buffered_events.is_empty();
        self.buffered_events = buffered_events;
        self.state_in_db = execution_info.state;
        self.next_event_id_in_db = execution_info.next_event_id;
        self.execution_info = execution_info;
        self.version_histories = version_histories;
        self.history_size = history_size;
        self.reset_update_tracking();
    }

    fn reset_update_tracking(&mut self) {
        self.update_activity_infos.clear();
        self.delete_activity_infos.clear();
        self.sync_activity_tasks.clear();
        self.update_timer_infos.clear();
        self.delete_timer_infos.clear();
        self.update_child_execution_infos.clear();
        self.delete_child_execution_info_ids.clear();
        self.update_request_cancel_infos.clear();
        self.delete_request_cancel_info_ids.clear();
        self.update_signal_infos.clear();
        self.delete_signal_info_ids.clear();
        self.update_signal_requested_ids.clear();
        self.delete_signal_requested_ids.clear();
    }

    // ---- versions (Go UpdateCurrentVersion / GetCurrentVersion / ...) ----

    pub fn update_current_version(&mut self, version: i64, force_update: bool) -> Result<()> {
        if self.execution_info.state == WORKFLOW_STATE_COMPLETED {
            // Go: always set current version to last write version when workflow is completed.
            self.current_version = self.get_last_write_version()?;
            return Ok(());
        }
        if let Some(histories) = &self.version_histories {
            let history = histories
                .current_history()
                .map_err(|e| TypedMutableStateError::Internal(e.message))?;
            if !history.items.is_empty() {
                // Go: this makes sure current version >= last write version.
                let last = history.last_item().map_err(|e| TypedMutableStateError::Internal(e.message))?;
                self.current_version = last.version;
            }
            if version > self.current_version || force_update {
                self.current_version = version;
            }
            return Ok(());
        }
        self.current_version = EMPTY_VERSION;
        Ok(())
    }

    pub fn get_current_version(&self) -> i64 {
        if self.version_histories.is_some() {
            return self.current_version;
        }
        EMPTY_VERSION
    }

    pub fn get_last_write_version(&self) -> Result<i64> {
        match &self.version_histories {
            Some(histories) => {
                let history = histories
                    .current_history()
                    .map_err(|e| TypedMutableStateError::Internal(e.message))?;
                match history.last_item() {
                    Ok(item) => Ok(item.version),
                    Err(_) => Ok(EMPTY_VERSION),
                }
            }
            None => Ok(EMPTY_VERSION),
        }
    }

    pub fn get_start_version(&self) -> Result<i64> {
        match &self.version_histories {
            Some(histories) => {
                let history = histories
                    .current_history()
                    .map_err(|e| TypedMutableStateError::Internal(e.message))?;
                match history.first_item() {
                    Ok(item) => Ok(item.version),
                    Err(_) => Ok(EMPTY_VERSION),
                }
            }
            None => Ok(EMPTY_VERSION),
        }
    }

    // ---- state predicates ----

    pub fn is_workflow_execution_running(&self) -> bool {
        match self.execution_info.state {
            WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING => true,
            WORKFLOW_STATE_COMPLETED | WORKFLOW_STATE_ZOMBIE | WORKFLOW_STATE_VOID
            | WORKFLOW_STATE_CORRUPTED => false,
            _ => false,
        }
    }

    pub fn is_current_workflow_guaranteed(&self) -> bool {
        // Go: stateInDB (not the in-transaction state) is the bloom filter.
        matches!(self.state_in_db, WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING)
    }

    pub fn has_in_flight_decision(&self) -> bool {
        self.execution_info.decision_started_id > EMPTY_EVENT_ID
    }

    pub fn has_pending_decision(&self) -> bool {
        self.execution_info.decision_schedule_id != EMPTY_EVENT_ID
    }

    pub fn has_buffered_events(&self) -> bool {
        !self.buffered_events.is_empty() || !self.update_buffered_events.is_empty()
    }

    pub fn get_next_event_id(&self) -> i64 {
        self.execution_info.next_event_id
    }

    pub fn get_pre_service_buffered_events(&self) -> &[HistoryEvent] {
        &self.buffered_events
    }

    // ---- event creation / buffering ----

    /// Go `shouldBufferEvent`.
    pub fn should_buffer_event(&self, event_type: &EventType) -> Result<bool> {
        use EventType::*;
        match event_type {
            // Workflow state events and decision events are never buffered.
            WorkflowExecutionStarted
            | WorkflowExecutionCompleted
            | WorkflowExecutionFailed
            | WorkflowExecutionTimedOut
            | WorkflowExecutionTerminated
            | WorkflowExecutionContinuedAsNew
            | WorkflowExecutionCanceled
            | DecisionTaskScheduled
            | DecisionTaskStarted
            | DecisionTaskCompleted
            | DecisionTaskFailed
            | DecisionTaskTimedOut => Ok(false),
            // Events generated directly by a decision are not buffered, but a
            // decision must be in flight when they are produced.
            ActivityTaskScheduled
            | ActivityTaskCancelRequested
            | TimerStarted
            | TimerCanceled
            | CancelTimerFailed
            | RequestCancelExternalWorkflowExecutionInitiated
            | MarkerRecorded
            | StartChildWorkflowExecutionInitiated
            | SignalExternalWorkflowExecutionInitiated
            | UpsertWorkflowSearchAttributes
            | SemaphoreAcquireInitiated
            | SemaphoreReleased => {
                if self.has_in_flight_decision() {
                    return Ok(false);
                }
                internal(format!(
                    "cannot create {event_type} event without an in-flight decision"
                ))
            }
            // Everything else is buffered while a decision is in flight.
            _ => Ok(self.has_in_flight_decision()),
        }
    }

    /// Go `CreateNewHistoryEventWithTimestamp` (+ buffering). Returns the
    /// event; buffered events are appended to `update_buffered_events`,
    /// others to `history_events`.
    pub fn create_new_history_event(
        &mut self,
        event_type: EventType,
        timestamp_unix_nanos: i64,
    ) -> Result<HistoryEvent> {
        let should_buffer = self.should_buffer_event(&event_type)?;
        let event_id = if should_buffer {
            BUFFERED_EVENT_ID
        } else {
            let id = self.execution_info.next_event_id;
            self.execution_info.next_event_id += 1;
            id
        };
        let event = HistoryEvent {
            event_id,
            timestamp: Some(timestamp_unix_nanos),
            event_type: Some(event_type),
            version: self.get_current_version(),
            task_id: EMPTY_EVENT_TASK_ID,
            ..HistoryEvent::default()
        };
        Ok(event)
    }

    /// Go `appendEvent` tail of `hBuilder.addEvent`: route into the buffered
    /// or committed event list.
    pub fn append_event(&mut self, event: HistoryEvent) {
        if event.event_id == BUFFERED_EVENT_ID {
            self.update_buffered_events.push(event);
        } else {
            self.history_events.push(event);
        }
    }

    /// Go `FlushBufferedEvents`.
    pub fn flush_buffered_events(&mut self) -> Result<()> {
        if self.has_in_flight_decision() {
            // Go: do not flush while a decision is in flight.
            return Ok(());
        }
        let mut new_committed = std::mem::take(&mut self.history_events);
        let mut candidates = std::mem::take(&mut self.buffered_events);
        candidates.extend(std::mem::take(&mut self.update_buffered_events));

        // Go: reorderAndFilterDuplicateEvents on the old + new buffered bucket.
        let (reordered, _) = reorder_and_filter_duplicate_events(&new_committed, candidates);
        // Assign event ids and patch started ids.
        let mut next_event_id = self.execution_info.next_event_id;
        let mut scheduled_to_started: BTreeMap<i64, i64> = BTreeMap::new();
        for event in new_committed.iter() {
            // Go collects started events produced earlier in this transaction.
            if let (Some(EventType::ActivityTaskStarted), Some(a)) =
                (&event.event_type, &event.activity_task_started_event_attributes)
            {
                scheduled_to_started.insert(a.scheduled_event_id, event.event_id);
            }
        }
        for mut event in reordered {
            event.event_id = next_event_id;
            next_event_id += 1;
            patch_started_event_id(&mut event, &scheduled_to_started);
            new_committed.push(event);
        }
        self.execution_info.next_event_id = next_event_id;
        self.history_events = new_committed;
        if self.has_buffered_events_in_db {
            self.clear_buffered_events = true;
        }
        self.buffered_events = Vec::new();
        Ok(())
    }

    /// Go `trimEventsAfterWorkflowClose`: drop events after the close event.
    pub fn trim_events_after_workflow_close(events: &[HistoryEvent]) -> Result<Vec<HistoryEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let close_index = events.iter().position(|e| {
            matches!(
                e.event_type,
                Some(EventType::WorkflowExecutionCompleted)
                    | Some(EventType::WorkflowExecutionFailed)
                    | Some(EventType::WorkflowExecutionTimedOut)
                    | Some(EventType::WorkflowExecutionTerminated)
                    | Some(EventType::WorkflowExecutionContinuedAsNew)
                    | Some(EventType::WorkflowExecutionCanceled)
            )
        });
        match close_index {
            Some(index) => Ok(events[..=index].to_vec()),
            None => Ok(events.to_vec()),
        }
    }

    // ---- version history helpers ----

    /// Go `updateVersionHistory` portion of `CloseTransaction`: advance the
    /// current version history to the last committed event.
    pub fn advance_version_history(&mut self, events: &[HistoryEvent]) -> Result<()> {
        let Some(last) = events.last() else { return Ok(()) };
        if let Some(histories) = self.version_histories.as_mut() {
            let index = histories.current_version_history_index as usize;
            let history = histories
                .histories
                .get_mut(index)
                .ok_or_else(|| TypedMutableStateError::Internal("version history index out of range".into()))?;
            history
                .add_or_update(&new_item(last.event_id, last.version))
                .map_err(|e| TypedMutableStateError::Internal(e.message))?;
        }
        Ok(())
    }

    pub fn current_version_history_last_item(&self) -> Option<VersionHistoryItem> {
        self.version_histories
            .as_ref()
            .and_then(|h| h.current_history().ok())
            .and_then(|h| h.last_item().ok())
    }

    // ---- close transaction ----

    /// Prepare an immutable mutation from a transaction copy. The source keeps
    /// all deltas until `cleanup_transaction` is called after durable commit.
    pub fn prepare_close_mutation(
        &self,
        now_nanos: i64,
        policy: super::workflow::TransactionPolicy,
    ) -> Result<(TypedWorkflowMutation, Vec<HistoryEvent>)> {
        let mut working = self.clone();
        working.prepare_close_policy(now_nanos, policy)?;
        let (mutation, events) = working.close_mutation_prepared(policy == super::workflow::TransactionPolicy::Passive)?;
        Ok((mutation, events))
    }

    /// Prepare a snapshot without consuming the source transaction.
    pub fn prepare_close_snapshot(
        &self,
        now_nanos: i64,
        policy: super::workflow::TransactionPolicy,
    ) -> Result<(TypedWorkflowSnapshot, Vec<HistoryEvent>)> {
        let mut working = self.clone();
        working.prepare_close_policy(now_nanos, policy)?;
        let (snapshot, events) = working.close_snapshot_prepared(policy == super::workflow::TransactionPolicy::Passive)?;
        Ok((snapshot, events))
    }

    fn prepare_close_policy(
        &mut self,
        now_nanos: i64,
        policy: super::workflow::TransactionPolicy,
    ) -> Result<()> {
        if policy == super::workflow::TransactionPolicy::Active {
            self.flush_buffered_events()?;
            self.close_transaction_handle_activity_user_timer_tasks_at(now_nanos)?;
        }
        Ok(())
    }

    fn close_transaction_handle_activity_user_timer_tasks_at(&mut self, now_nanos: i64) -> Result<()> {
        if !self.is_workflow_execution_running() { return Ok(()); }
        self.run_task_generator(now_nanos, |g| g.generate_activity_timer_tasks())?;
        self.run_task_generator(now_nanos, |g| g.generate_user_timer_tasks())
    }

    /// Go `cleanupTransaction`.
    pub fn cleanup_transaction(&mut self) {
        self.reset_update_tracking();
        self.semaphore_state.updates.clear();
        self.semaphore_state.deletes.clear();
        if !self.update_buffered_events.is_empty() {
            let moved = std::mem::take(&mut self.update_buffered_events);
            self.buffered_events.extend(moved);
        }
        self.has_buffered_events_in_db = !self.buffered_events.is_empty();
        self.clear_buffered_events = false;
        self.state_in_db = self.execution_info.state;
        self.next_event_id_in_db = self.execution_info.next_event_id;
        self.tasks.clear();
        self.transfer_tasks.clear();
        self.timer_tasks.clear();
        self.workflow_requests.clear();
        self.insert_workflow_requests.clear();
        self.history_events.clear();
    }

    /// Go `DeleteTransferTasks`.
    pub fn delete_transfer_tasks(&mut self) {
        self.transfer_tasks.clear();
    }

    /// Go `DeleteTimerTasks`.
    pub fn delete_timer_tasks(&mut self) {
        self.timer_tasks.clear();
    }

    /// Go `logDataInconsistency`: `DataInconsistentCounter` metric plus error log with the workflow identity.
    pub fn log_data_inconsistency(&self) {
        self.data_inconsistency.emit();
        let info = &self.execution_info;
        tracing::error!(
            domain_id = %info.domain_id,
            workflow_id = %info.workflow_id,
            run_id = %info.run_id,
            "encounter mutable state data inconsistency"
        );
    }

    /// Go `closeTransactionHandleActivityUserTimerTasks` (must run last in
    /// prepareCloseTransaction).
    pub fn close_transaction_handle_activity_user_timer_tasks(&mut self) -> Result<()> {
        if !self.is_workflow_execution_running() {
            return Ok(());
        }
        self.run_task_generator(0, |g| g.generate_activity_timer_tasks())?;
        self.run_task_generator(0, |g| g.generate_user_timer_tasks())
    }

    /// Drains generated transfer/timer tasks into the persistence task map
    /// (Go `insertTransferTasks`/`insertTimerTasks` in `TasksByCategory`).
    fn drain_encoded_tasks(&mut self) -> Result<BTreeMap<HistoryTaskCategory, Vec<Task>>> {
        let transfer = std::mem::take(&mut self.transfer_tasks);
        let timer = std::mem::take(&mut self.timer_tasks);
        let mut by_category = self.tasks.clone();
        for (category, tasks) in super::generated_task_encoding::encode_generated_tasks(&transfer, &timer)? {
            by_category.entry(category).or_default().extend(tasks);
        }
        Ok(by_category)
    }

    /// Go `CloseTransactionAsMutation`.
    pub fn close_transaction_as_mutation(&mut self) -> Result<(TypedWorkflowMutation, Vec<HistoryEvent>)> {
        self.close_transaction_handle_activity_user_timer_tasks()?;
        self.close_mutation_prepared(false)
    }

    fn close_mutation_prepared(&mut self, passive: bool) -> Result<(TypedWorkflowMutation, Vec<HistoryEvent>)> {
        let tasks_by_category = self.drain_encoded_tasks()?;
        let events = std::mem::take(&mut self.history_events);
        // Go updateWithLastWriteEvent: Passive is "already handled in state builder".
        if !passive {
            self.advance_version_history(&events)?;
        }

        let rewrite_activity = !self.delete_activity_infos.is_empty();
        let rewrite_timer = !self.delete_timer_infos.is_empty();

        let workflow_requests = self.prepared_workflow_requests()?;
        let mutation = TypedWorkflowMutation {
            execution_info: self.execution_info.clone(),
            version_histories: self.version_histories.clone(),
            start_version: self.get_start_version()?,
            last_write_version: self.get_last_write_version()?,
            upsert_activity_infos: self.update_activity_infos.values().cloned().collect(),
            delete_activity_infos: self.delete_activity_infos.iter().copied().collect(),
            rewrite_activity_infos: if rewrite_activity {
                self.pending_activity_info_ids.values().cloned().collect()
            } else {
                Vec::new()
            },
            upsert_timer_infos: self.update_timer_infos.values().cloned().collect(),
            delete_timer_infos: self.delete_timer_infos.iter().cloned().collect(),
            rewrite_timer_infos: if rewrite_timer {
                self.pending_timer_info_ids.values().cloned().collect()
            } else {
                Vec::new()
            },
            upsert_child_execution_infos: self.update_child_execution_infos.values().cloned().collect(),
            delete_child_execution_infos: self.delete_child_execution_info_ids.iter().copied().collect(),
            upsert_request_cancel_infos: self.update_request_cancel_infos.values().cloned().collect(),
            delete_request_cancel_infos: self.delete_request_cancel_info_ids.iter().copied().collect(),
            upsert_signal_infos: self.update_signal_infos.values().cloned().collect(),
            delete_signal_infos: self.delete_signal_info_ids.iter().copied().collect(),
            upsert_signal_requested_ids: self.update_signal_requested_ids.iter().cloned().collect(),
            delete_signal_requested_ids: self.delete_signal_requested_ids.iter().cloned().collect(),
            upsert_semaphore_infos: self.semaphore_state.updates.values().cloned().collect(),
            delete_semaphore_infos: self.semaphore_state.deletes.iter().copied().collect(),
            new_buffered_events: self.update_buffered_events.clone(),
            clear_buffered_events: self.clear_buffered_events,
            tasks_by_category,
            workflow_requests,
            condition: self.next_event_id_in_db,
        };
        self.cleanup_transaction();
        Ok((mutation, events))
    }

    /// Go `CloseTransactionAsSnapshot`.
    pub fn close_transaction_as_snapshot(&mut self) -> Result<(TypedWorkflowSnapshot, Vec<HistoryEvent>)> {
        if !self.update_buffered_events.is_empty() || !self.buffered_events.is_empty() {
            return internal("cannot close transaction as snapshot with buffered events");
        }
        self.close_transaction_handle_activity_user_timer_tasks()?;
        self.close_snapshot_prepared(false)
    }

    fn close_snapshot_prepared(&mut self, passive: bool) -> Result<(TypedWorkflowSnapshot, Vec<HistoryEvent>)> {
        if !self.update_buffered_events.is_empty() || !self.buffered_events.is_empty() {
            return internal("cannot close transaction as snapshot with buffered events");
        }
        let tasks_by_category = self.drain_encoded_tasks()?;
        let events = std::mem::take(&mut self.history_events);
        // Go updateWithLastWriteEvent: Passive is "already handled in state builder".
        if !passive {
            self.advance_version_history(&events)?;
        }
        let workflow_requests = self.prepared_workflow_requests()?;
        let snapshot = TypedWorkflowSnapshot {
            execution_info: self.execution_info.clone(),
            version_histories: self.version_histories.clone(),
            start_version: self.get_start_version()?,
            last_write_version: self.get_last_write_version()?,
            activity_infos: self.pending_activity_info_ids.values().cloned().collect(),
            timer_infos: self.pending_timer_info_ids.values().cloned().collect(),
            child_execution_infos: self.pending_child_execution_info_ids.values().cloned().collect(),
            request_cancel_infos: self.pending_request_cancel_info_ids.values().cloned().collect(),
            signal_infos: self.pending_signal_info_ids.values().cloned().collect(),
            signal_requested_ids: self.pending_signal_requested_ids.iter().cloned().collect(),
            semaphore_infos: self.semaphore_state.pending.values().cloned().collect(),
            tasks_by_category,
            workflow_requests,
            condition: self.next_event_id_in_db,
        };
        self.cleanup_transaction();
        Ok((snapshot, events))
    }

    fn prepared_workflow_requests(&self) -> Result<Vec<WorkflowRequest>> {
        let mut requests = self.workflow_requests.clone();
        for inserted in &self.insert_workflow_requests {
            if inserted.data.data.is_empty() || inserted.data.encoding.is_empty() {
                return internal(format!("workflow request {} has no encoded deduplication data", inserted.request_id));
            }
            if let Some(existing) = requests.iter_mut().find(|request| request.request_id == inserted.request_id && request.version == inserted.version && request.request_type == inserted.request_type) {
                if existing.data.data.is_empty() || existing.data.encoding.is_empty() { existing.data = inserted.data.clone(); }
            } else {
                requests.push(inserted.clone());
            }
        }
        if requests.iter().any(|request| request.data.data.is_empty() || request.data.encoding.is_empty()) {
            return internal("workflow request is missing encoded deduplication data");
        }
        Ok(requests)
    }

    // ---- pending-map mutation helpers (Go Update*/Delete*) ----

    pub fn update_activity(&mut self, info: ActivityInfo) {
        self.pending_activity_id_to_event_id.insert(info.activity_id.clone(), info.schedule_id);
        self.pending_activity_info_ids.insert(info.schedule_id, info.clone());
        self.update_activity_infos.insert(info.schedule_id, info);
    }

    pub fn delete_activity(&mut self, schedule_id: i64) -> Result<()> {
        // Go DeleteActivity: a missing activity is logged as a data inconsistency, never an error.
        if let Some(info) = self.pending_activity_info_ids.remove(&schedule_id) {
            if self.pending_activity_id_to_event_id.remove(&info.activity_id).is_none() {
                tracing::error!("unable to find activity ID: {} in mutable state", info.activity_id);
                self.log_data_inconsistency();
            }
        } else {
            tracing::error!("unable to find activity event id: {schedule_id} in mutable state");
            self.log_data_inconsistency();
        }
        self.update_activity_infos.remove(&schedule_id);
        self.delete_activity_infos.insert(schedule_id);
        Ok(())
    }

    pub fn update_user_timer(&mut self, info: TimerInfo) {
        self.pending_timer_event_id_to_id.insert(info.started_id, info.timer_id.clone());
        self.pending_timer_info_ids.insert(info.timer_id.clone(), info.clone());
        self.update_timer_infos.insert(info.timer_id.clone(), info);
    }

    pub fn delete_user_timer(&mut self, timer_id: &str) -> Result<()> {
        // Go DeleteUserTimer: a missing timer is logged as a data
        // inconsistency, never returned as an error.
        if let Some(info) = self.pending_timer_info_ids.remove(timer_id) {
            if self.pending_timer_event_id_to_id.remove(&info.started_id).is_none() {
                tracing::error!("unable to find timer event ID: {timer_id} in mutable state");
                self.log_data_inconsistency();
            }
        } else {
            tracing::error!("unable to find timer ID: {timer_id} in mutable state");
            self.log_data_inconsistency();
        }
        self.update_timer_infos.remove(timer_id);
        self.delete_timer_infos.insert(timer_id.to_string());
        Ok(())
    }

    pub fn update_child_execution(&mut self, info: ChildExecutionInfo) {
        self.pending_child_execution_info_ids.insert(info.initiated_id, info.clone());
        self.update_child_execution_infos.insert(info.initiated_id, info);
    }

    pub fn delete_child_execution(&mut self, initiated_id: i64) -> Result<()> {
        if self.pending_child_execution_info_ids.remove(&initiated_id).is_none() {
            tracing::error!("unable to find child workflow event ID: {initiated_id} in mutable state");
            self.log_data_inconsistency();
        }
        self.delete_child_execution_info_ids.insert(initiated_id);
        self.update_child_execution_infos.remove(&initiated_id);
        Ok(())
    }

    pub fn update_request_cancel(&mut self, info: RequestCancelInfo) {
        self.pending_request_cancel_info_ids.insert(info.initiated_id, info.clone());
        self.update_request_cancel_infos.insert(info.initiated_id, info);
    }

    pub fn delete_request_cancel(&mut self, initiated_id: i64) -> Result<()> {
        if self.pending_request_cancel_info_ids.remove(&initiated_id).is_none() {
            tracing::error!("unable to find request cancel external workflow event ID: {initiated_id} in mutable state");
            self.log_data_inconsistency();
        }
        self.delete_request_cancel_info_ids.insert(initiated_id);
        self.update_request_cancel_infos.remove(&initiated_id);
        Ok(())
    }

    pub fn update_signal(&mut self, info: SignalInfo) {
        self.pending_signal_info_ids.insert(info.initiated_id, info.clone());
        self.update_signal_infos.insert(info.initiated_id, info);
    }

    pub fn delete_signal(&mut self, initiated_id: i64) -> Result<()> {
        if self.pending_signal_info_ids.remove(&initiated_id).is_none() {
            tracing::error!("unable to find signal external workflow event ID: {initiated_id} in mutable state");
            self.log_data_inconsistency();
        }
        self.delete_signal_info_ids.insert(initiated_id);
        self.update_signal_infos.remove(&initiated_id);
        Ok(())
    }

    pub fn add_signal_requested(&mut self, request_id: &str) {
        self.pending_signal_requested_ids.insert(request_id.to_string());
        self.update_signal_requested_ids.insert(request_id.to_string());
    }

    pub fn delete_signal_requested(&mut self, request_id: &str) {
        self.pending_signal_requested_ids.remove(request_id);
        self.delete_signal_requested_ids.insert(request_id.to_string());
    }

    pub fn add_tasks(&mut self, category: HistoryTaskCategory, tasks: Vec<Task>) {
        self.tasks.entry(category).or_default().extend(tasks);
    }
}

/// Go `reorderAndFilterDuplicateEvents`: activity/child completions go to the
/// tail; duplicates (same type, scheduled id, attempt, started id) are dropped.
/// Returns `(events, duplicate_count)`.
pub fn reorder_and_filter_duplicate_events(
    committed: &[HistoryEvent],
    buffered: Vec<HistoryEvent>,
) -> (Vec<HistoryEvent>, usize) {
    let mut seen: BTreeSet<(i32, i64, i64, i64)> = BTreeSet::new();
    for event in committed {
        if let Some(key) = completion_key(event) {
            seen.insert(key);
        }
    }
    let mut head = Vec::new();
    let mut tail = Vec::new();
    let mut duplicates = 0;
    for event in buffered {
        match completion_key(&event) {
            Some(key) => {
                if !seen.insert(key) {
                    duplicates += 1;
                    continue;
                }
                tail.push(event);
            }
            None => head.push(event),
        }
    }
    head.extend(tail);
    (head, duplicates)
}

fn completion_key(event: &HistoryEvent) -> Option<(i32, i64, i64, i64)> {
    let event_type = event.event_type.as_ref()?;
    let (scheduled, started) = match event_type {
        EventType::ActivityTaskCompleted => event
            .activity_task_completed_event_attributes
            .as_ref()
            .map(|a| (a.scheduled_event_id, a.started_event_id))?,
        EventType::ActivityTaskFailed => event
            .activity_task_failed_event_attributes
            .as_ref()
            .map(|a| (a.scheduled_event_id, a.started_event_id))?,
        EventType::ActivityTaskTimedOut => event
            .activity_task_timed_out_event_attributes
            .as_ref()
            .map(|a| (a.scheduled_event_id, a.started_event_id))?,
        EventType::ActivityTaskCanceled => event
            .activity_task_canceled_event_attributes
            .as_ref()
            .map(|a| (a.scheduled_event_id, a.started_event_id))?,
        EventType::ChildWorkflowExecutionCompleted => event
            .child_workflow_execution_completed_event_attributes
            .as_ref()
            .map(|a| (a.initiated_event_id, a.started_event_id))?,
        EventType::ChildWorkflowExecutionFailed => event
            .child_workflow_execution_failed_event_attributes
            .as_ref()
            .map(|a| (a.initiated_event_id, a.started_event_id))?,
        EventType::ChildWorkflowExecutionCanceled => event
            .child_workflow_execution_canceled_event_attributes
            .as_ref()
            .map(|a| (a.initiated_event_id, a.started_event_id))?,
        EventType::ChildWorkflowExecutionTimedOut => event
            .child_workflow_execution_timed_out_event_attributes
            .as_ref()
            .map(|a| (a.initiated_event_id, a.started_event_id))?,
        EventType::ChildWorkflowExecutionTerminated => event
            .child_workflow_execution_terminated_event_attributes
            .as_ref()
            .map(|a| (a.initiated_event_id, a.started_event_id))?,
        _ => return None,
    };
    Some((event_type.value(), scheduled, 0, started))
}

/// Go `assignEventIDToBufferedEvents` started-id patching for completions that
/// referenced a not-yet-assigned (buffered) started event.
fn patch_started_event_id(event: &mut HistoryEvent, scheduled_to_started: &BTreeMap<i64, i64>) {
    if let Some(a) = event.activity_task_completed_event_attributes.as_mut() {
        if let Some(id) = scheduled_to_started.get(&a.scheduled_event_id) {
            a.started_event_id = *id;
        }
    }
    if let Some(a) = event.activity_task_failed_event_attributes.as_mut() {
        if let Some(id) = scheduled_to_started.get(&a.scheduled_event_id) {
            a.started_event_id = *id;
        }
    }
    if let Some(a) = event.activity_task_timed_out_event_attributes.as_mut() {
        if let Some(id) = scheduled_to_started.get(&a.scheduled_event_id) {
            a.started_event_id = *id;
        }
    }
    if let Some(a) = event.activity_task_canceled_event_attributes.as_mut() {
        if let Some(id) = scheduled_to_started.get(&a.scheduled_event_id) {
            a.started_event_id = *id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_matches_go_defaults() {
        let state = TypedMutableState::new();
        assert_eq!(state.execution_info.next_event_id, FIRST_EVENT_ID);
        assert_eq!(state.next_event_id_in_db, FIRST_EVENT_ID);
        assert_eq!(state.state_in_db, WORKFLOW_STATE_VOID);
        assert_eq!(state.get_current_version(), EMPTY_VERSION);
    }

    #[test]
    fn current_workflow_guaranteed_matches_go() {
        let mut state = TypedMutableState::new();
        for (s, want) in [
            (WORKFLOW_STATE_CREATED, true),
            (WORKFLOW_STATE_RUNNING, true),
            (WORKFLOW_STATE_COMPLETED, false),
            (WORKFLOW_STATE_ZOMBIE, false),
            (WORKFLOW_STATE_VOID, false),
            (WORKFLOW_STATE_CORRUPTED, false),
        ] {
            state.state_in_db = s;
            assert_eq!(state.is_current_workflow_guaranteed(), want);
            // the in-transaction state (e.g. replayed terminate) must not matter
            state.execution_info.state = WORKFLOW_STATE_COMPLETED;
            assert_eq!(state.is_current_workflow_guaranteed(), want);
        }
    }

    #[test]
    fn buffering_rules() {
        let mut state = TypedMutableState::new();
        assert!(!state.should_buffer_event(&EventType::WorkflowExecutionStarted).unwrap());
        assert!(!state.should_buffer_event(&EventType::WorkflowExecutionSignaled).unwrap());
        assert!(state.should_buffer_event(&EventType::ActivityTaskScheduled).is_err());
        state.execution_info.decision_started_id = 5;
        assert!(state.should_buffer_event(&EventType::WorkflowExecutionSignaled).unwrap());
        assert!(!state.should_buffer_event(&EventType::TimerStarted).unwrap());
    }

    #[test]
    fn create_event_assigns_ids() {
        let mut state = TypedMutableState::new();
        let e = state.create_new_history_event(EventType::WorkflowExecutionSignaled, 10).unwrap();
        assert_eq!(e.event_id, 1);
        assert_eq!(e.task_id, EMPTY_EVENT_TASK_ID);
        assert_eq!(state.execution_info.next_event_id, 2);
        state.execution_info.decision_started_id = 3;
        let b = state.create_new_history_event(EventType::WorkflowExecutionSignaled, 11).unwrap();
        assert_eq!(b.event_id, BUFFERED_EVENT_ID);
        assert_eq!(state.execution_info.next_event_id, 2);
    }

    #[test]
    fn mutation_tracks_updates_and_deletes_then_resets() {
        let mut state = TypedMutableState::new();
        state.execution_info.domain_id = "11111111-1111-1111-1111-111111111111".into();
        state.execution_info.run_id = "22222222-2222-2222-2222-222222222222".into();
        state.update_activity(ActivityInfo { schedule_id: 5, activity_id: "a".into(), ..ActivityInfo::default() });
        state.update_user_timer(TimerInfo { timer_id: "t".into(), started_id: 7, ..TimerInfo::default() });
        let (m, _) = state.close_transaction_as_mutation().unwrap();
        assert_eq!(m.upsert_activity_infos.len(), 1);
        assert_eq!(m.upsert_timer_infos.len(), 1);
        assert!(m.rewrite_activity_infos.is_empty());
        state.delete_activity(5).unwrap();
        let (m, _) = state.close_transaction_as_mutation().unwrap();
        assert_eq!(m.delete_activity_infos, vec![5]);
        assert!(state.update_activity_infos.is_empty());
        // Go DeleteActivity: a missing activity is a logged data inconsistency (counter), not an error.
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = count.clone();
        state.data_inconsistency = DataInconsistencyHook(Some(std::sync::Arc::new(move || {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })));
        assert!(state.delete_activity(5).is_ok());
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn update_current_version_matches_go() {
        let mut state = TypedMutableState::new();
        state.version_histories = Some(VersionHistories {
            current_version_history_index: 0,
            histories: vec![cadence_api::types::VersionHistory { branch_token: vec![1], items: vec![new_item(5, 10)] }],
        });
        state.execution_info.state = WORKFLOW_STATE_RUNNING;
        // lower incoming version never lowers below last write version
        state.update_current_version(4, false).unwrap();
        assert_eq!(state.current_version, 10);
        state.update_current_version(12, false).unwrap();
        assert_eq!(state.current_version, 12);
        // next call restarts from the last item unless forced
        state.update_current_version(3, false).unwrap();
        assert_eq!(state.current_version, 10);
        state.update_current_version(3, true).unwrap();
        assert_eq!(state.current_version, 3);
        // completed workflow always uses last write version
        state.execution_info.state = WORKFLOW_STATE_COMPLETED;
        state.update_current_version(50, true).unwrap();
        assert_eq!(state.current_version, 10);
    }

    #[test]
    fn snapshot_rejects_buffered_events() {
        let mut state = TypedMutableState::new();
        state.update_buffered_events.push(HistoryEvent::default());
        assert!(state.close_transaction_as_snapshot().is_err());
    }

    #[test]
    fn prepared_passive_close_preserves_buffered_events_source_and_cas() {
        let mut state = TypedMutableState::new();
        state.next_event_id_in_db = 12;
        state.history_events.push(HistoryEvent { event_id: 1, ..Default::default() });
        state.update_buffered_events.push(HistoryEvent { event_id: BUFFERED_EVENT_ID, ..Default::default() });
        let (mutation, events) = state.prepare_close_mutation(99, super::super::workflow::TransactionPolicy::Passive).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(mutation.new_buffered_events.len(), 1);
        assert_eq!(mutation.condition, 12);
        assert_eq!(state.history_events.len(), 1);
        assert_eq!(state.update_buffered_events.len(), 1);
        assert_eq!(state.next_event_id_in_db, 12);
    }

    #[test]
    fn failed_preparation_keeps_events_tasks_and_cas_recoverable() {
        let mut state = TypedMutableState::new();
        state.next_event_id_in_db = 12;
        state.history_events.push(HistoryEvent { event_id: 1, ..Default::default() });
        state.insert_workflow_requests.push(WorkflowRequest { request_id: "req".into(), version: 0, request_type: 0, data: Default::default() });
        assert!(state.prepare_close_mutation(99, super::super::workflow::TransactionPolicy::Passive).is_err());
        assert_eq!(state.history_events.len(), 1);
        assert_eq!(state.next_event_id_in_db, 12);
        assert_eq!(state.insert_workflow_requests.len(), 1);
    }

    #[test]
    fn delete_tasks_clear_pending_inserts() {
        let mut s = TypedMutableState::new();
        s.delete_transfer_tasks();
        s.delete_timer_tasks();
        s.log_data_inconsistency();
        assert!(s.transfer_tasks.is_empty() && s.timer_tasks.is_empty());
    }

    #[test]
    fn trim_after_close() {
        let mk = |t| HistoryEvent { event_type: Some(t), ..HistoryEvent::default() };
        let events = vec![
            mk(EventType::DecisionTaskCompleted),
            mk(EventType::WorkflowExecutionCompleted),
            mk(EventType::WorkflowExecutionSignaled),
        ];
        assert_eq!(TypedMutableState::trim_events_after_workflow_close(&events).unwrap().len(), 2);
    }
}
