//! Port of Go `service/history/ndc` (n-data-center replication).
//!
//! Go's `shard.Context`, `execution.Context`/`Workflow`/`MutableState`, caches and
//! persistence managers are modelled as small traits defined here. The modules
//! (`engine`, `shard`, ...) that implement them are being ported in parallel; they
//! will provide adapters for these traits. All algorithms follow the Go files 1:1.
//!
//! Time values are Unix nanoseconds (`i64`), matching Go `time.Unix(0, n)`.
#![allow(clippy::result_large_err)]

pub mod activity_replicator;
pub mod branch_manager;
pub mod conflict_resolver;
pub mod events_reapplier;
pub mod existing_workflow_transaction_manager;
pub mod history_replicator;
pub mod new_workflow_transaction_manager;
pub mod replication_task;
pub mod transaction_manager;
pub mod workflow_resetter;
#[cfg(test)]
pub(crate) mod test_support;

use cadence_api::errors::{BadRequestError, InternalServiceError, RetryTaskV2Error};
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::{VersionHistories, WorkflowExecution};
use cadence_persistence::history_contract::{
    ConflictResolveWorkflowMode, CreateWorkflowMode, UpdateWorkflowMode, WorkflowRequestMode,
    WorkflowSnapshot,
};

use crate::execution::context::{PersistedBlob, WorkflowEvents};
use crate::execution::workflow::{TransactionPolicy, WorkflowState, WorkflowVectorClock};

/// Go `constants.EmptyEventID`.
pub const EMPTY_EVENT_ID: i64 = -23;

/// Go `execution.ErrMissingVersionHistories`.
pub const ERR_MISSING_VERSION_HISTORIES: &str = "version histories does not exist";

/// Go `errPanic` (`errors.NewInternalFailureError("encounter panic")`).
pub const ERR_PANIC_MESSAGE: &str = "encounter panic";

/// Error type covering the Go error classes the ndc package distinguishes by type switch.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum NdcError {
    #[error("{0}")]
    RetryTaskV2(RetryTaskV2Error),
    #[error("{0}")]
    BadRequest(BadRequestError),
    #[error("{0}")]
    Internal(InternalServiceError),
    /// Go `types.EntityNotExistsError`.
    #[error("{0}")]
    EntityNotExists(String),
    /// Go `persistence.WorkflowExecutionAlreadyStartedError`.
    #[error("{0}")]
    WorkflowExecutionAlreadyStarted(String),
    #[error("{0}")]
    Other(String),
}

impl From<BadRequestError> for NdcError {
    fn from(e: BadRequestError) -> Self {
        NdcError::BadRequest(e)
    }
}
impl From<RetryTaskV2Error> for NdcError {
    fn from(e: RetryTaskV2Error) -> Self {
        NdcError::RetryTaskV2(e)
    }
}

impl NdcError {
    pub fn internal(message: impl Into<String>) -> Self {
        NdcError::Internal(InternalServiceError::new(message))
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        NdcError::BadRequest(BadRequestError::new(message))
    }
    pub fn missing_version_histories() -> Self {
        NdcError::Other(ERR_MISSING_VERSION_HISTORIES.to_string())
    }
    /// Go `errPanic`.
    pub fn panic_error() -> Self {
        NdcError::Other(ERR_PANIC_MESSAGE.to_string())
    }
}

pub type NdcResult<T> = Result<T, NdcError>;

/// Go `newNDCRetryTaskErrorWithHint`.
#[allow(clippy::too_many_arguments)]
pub fn new_ndc_retry_task_error_with_hint(
    message: &str,
    domain_id: &str,
    workflow_id: &str,
    run_id: &str,
    start_event_id: Option<i64>,
    start_event_version: Option<i64>,
    end_event_id: Option<i64>,
    end_event_version: Option<i64>,
) -> NdcError {
    NdcError::RetryTaskV2(RetryTaskV2Error {
        message: message.to_string(),
        domain_id: domain_id.to_string(),
        workflow_id: workflow_id.to_string(),
        run_id: run_id.to_string(),
        start_event_id,
        start_event_version,
        end_event_id,
        end_event_version,
    })
}

/// Subset of Go `persistence.WorkflowExecutionInfo` used by ndc.
#[derive(Debug, Clone, Default)]
pub struct NdcExecutionInfo {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub start_timestamp_nanos: i64,
}

/// Subset of Go `persistence.ActivityInfo` used by the activity replicator.
#[derive(Debug, Clone, Default)]
pub struct NdcActivityInfo {
    pub version: i64,
    pub attempt: i32,
    pub last_heartbeat_updated_time_nanos: i64,
    pub cancel_requested: bool,
}

/// Go `types.SyncActivityRequest` (not yet in cadence-api; local definition).
#[derive(Debug, Clone, Default)]
pub struct SyncActivityRequest {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub version: i64,
    pub scheduled_id: i64,
    pub scheduled_time: i64,
    pub started_id: i64,
    pub started_time: i64,
    pub last_heartbeat_time: i64,
    pub attempt: i32,
    pub version_history: Option<cadence_api::types::VersionHistory>,
    pub details: Vec<u8>,
    pub last_failure_reason: Option<String>,
    pub last_worker_identity: String,
    pub last_failure_details: Vec<u8>,
    pub last_failure_options: Option<cadence_api::types::FailureOptions>,
}

/// Go `types.ReplicateEventsV2Request` (not yet in cadence-api; local definition).
#[derive(Debug, Clone, Default)]
pub struct ReplicateEventsV2Request {
    pub domain_uuid: String,
    pub workflow_execution: Option<WorkflowExecution>,
    pub version_history_items: Vec<cadence_api::types::VersionHistoryItem>,
    pub events: Option<cadence_api::types::DataBlob>,
    pub new_run_events: Option<cadence_api::types::DataBlob>,
}

/// Go `cluster.Metadata` subset.
pub trait NdcClusterMetadata {
    fn get_current_cluster_name(&self) -> String;
    fn cluster_name_for_failover_version(&self, version: i64) -> NdcResult<String>;
    fn is_version_from_same_cluster(&self, v1: i64, v2: i64) -> bool;
}

/// Go `execution.MutableState` subset used by ndc.
pub trait NdcMutableState {
    /// Concrete-state access for adapters (e.g. the typed state builder). `None` for mocks.
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        None
    }
    fn get_execution_info(&self) -> NdcExecutionInfo;
    fn get_version_histories(&self) -> Option<&VersionHistories>;
    fn get_version_histories_mut(&mut self) -> Option<&mut VersionHistories>;
    fn set_version_histories(&mut self, v: VersionHistories) -> NdcResult<()>;
    fn get_current_version(&self) -> i64;
    fn get_start_version(&self) -> NdcResult<i64>;
    fn get_last_write_version(&self) -> NdcResult<i64>;
    fn get_update_condition(&self) -> i64;
    fn set_update_condition(&mut self, c: i64);
    fn has_buffered_events(&self) -> bool;
    fn is_workflow_execution_running(&self) -> bool;
    fn is_current_workflow_guaranteed(&self) -> bool;
    fn get_workflow_state(&self) -> WorkflowState;
    fn get_previous_started_event_id(&self) -> i64;
    fn get_next_event_id(&self) -> i64;
    fn get_current_branch_token(&self) -> NdcResult<Vec<u8>>;
    /// Go `IsResourceDuplicated(definition.NewEventReappliedID(runID, eventID, version))`.
    fn is_event_reapplied(&self, run_id: &str, event_id: i64, version: i64) -> NdcResult<bool>;
    /// Go `UpdateDuplicatedResource(definition.NewEventReappliedID(...))`.
    fn update_event_reapplied(&mut self, run_id: &str, event_id: i64, version: i64) -> NdcResult<()>;
    fn add_workflow_execution_signaled(
        &mut self,
        signal_name: &str,
        input: &[u8],
        identity: &str,
        request_id: &str,
    ) -> NdcResult<()>;
    fn get_activity_info(&self, schedule_id: i64) -> NdcResult<Option<NdcActivityInfo>>;
    fn replicate_activity_info(
        &mut self,
        request: &SyncActivityRequest,
        reset_activity_timer_task_status: bool,
    ) -> NdcResult<()>;
    /// Go `execution.NewTimerSequence(ms).CreateNextActivityTimer()`.
    fn create_next_activity_timer(&mut self) -> NdcResult<bool>;
    /// Go `execution.NewMutableStateTaskGenerator(...).GenerateActivityRetryTasks(id)`.
    fn generate_activity_retry_tasks(&mut self, schedule_id: i64) -> NdcResult<()>;
    // ---- workflow policy operations (execution/workflow.rs). Defaults are
    // explicit errors so an implementor can never silently skip them.
    /// `(active cluster selection policy, last event task id)` for the vector clock.
    fn vector_clock_inputs(
        &self,
    ) -> NdcResult<(Option<cadence_api::types::ActiveClusterSelectionPolicy>, i64)> {
        Err(NdcError::internal("vector_clock_inputs is not supported by this mutable state"))
    }
    fn has_processed_or_pending_decision(&self) -> NdcResult<bool> {
        Err(NdcError::internal("has_processed_or_pending_decision is not supported by this mutable state"))
    }
    fn has_in_flight_decision(&self) -> NdcResult<bool> {
        Err(NdcError::internal("has_in_flight_decision is not supported by this mutable state"))
    }
    fn update_workflow_state(&mut self, _state: WorkflowState) -> NdcResult<()> {
        Err(NdcError::internal("update_workflow_state is not supported by this mutable state"))
    }
    fn update_current_version(&mut self, _version: i64, _force: bool) -> NdcResult<()> {
        Err(NdcError::internal("update_current_version is not supported by this mutable state"))
    }
    /// Go `startTransactionHandleDecisionFailover`; true means a decision was failed and
    /// rescheduled so the caller must flush. Mocks without cluster metadata return false.
    fn start_transaction_handle_decision_failover(&mut self, _incoming_task_version: i64) -> NdcResult<bool> {
        Ok(false)
    }
    fn fail_in_flight_decision_for_failover(&mut self) -> NdcResult<()> {
        Err(NdcError::internal("fail_in_flight_decision_for_failover is not supported by this mutable state"))
    }
    fn schedule_decision(&mut self) -> NdcResult<()> {
        Err(NdcError::internal("schedule_decision is not supported by this mutable state"))
    }
    fn terminate_workflow_execution(
        &mut self,
        _batch_first_event_id: i64,
        _reason: &str,
        _details: Vec<u8>,
        _identity: &str,
    ) -> NdcResult<()> {
        Err(NdcError::internal("terminate_workflow_execution is not supported by this mutable state"))
    }
    /// Go `CloseTransactionAsSnapshot`.
    fn close_transaction_as_snapshot(
        &mut self,
        now_nanos: i64,
        policy: TransactionPolicy,
    ) -> NdcResult<(WorkflowSnapshot, Vec<WorkflowEvents>)>;
    /// Go `CloseTransactionAsMutation`. Explicit error unless supported.
    fn close_transaction_as_mutation(
        &mut self,
        _now_nanos: i64,
        _policy: TransactionPolicy,
    ) -> NdcResult<(cadence_persistence::history_contract::WorkflowMutation, Vec<WorkflowEvents>)> {
        Err(NdcError::internal("close_transaction_as_mutation is not supported by this mutable state"))
    }
    /// Called only after the prepared image has been durably committed.
    fn transaction_committed(&mut self) {}
    /// `(LastFirstEventID, workflow state, close status)` for the history
    /// notification (Go `events.NewNotification`). Explicit error unless supported.
    fn history_notification_inputs(&self) -> NdcResult<(i64, i32, i32)> {
        Err(NdcError::internal("history_notification_inputs is not supported by this mutable state"))
    }
}

/// A workflow's state lent to a persistence write (a placeholder stays behind).
pub struct NdcLentState {
    pub domain_id: String,
    pub execution: WorkflowExecution,
    pub ms: Box<dyn NdcMutableState>,
    pub history_size: i64,
}

/// Go `execution.Workflow` joined with its `execution.Context`, mutable state and
/// release function (Go passes these around as one `Workflow`).
pub trait NdcWorkflow {
    fn mutable_state(&self) -> &dyn NdcMutableState;
    fn mutable_state_mut(&mut self) -> &mut dyn NdcMutableState;
    /// Replaces the mutable state (used after a conflict-resolver rebuild).
    fn replace_mutable_state(&mut self, ms: Box<dyn NdcMutableState>);
    fn clear(&mut self);
    fn set_history_size(&mut self, size: i64);
    /// Lend state to a two-run write (the other run of a continued-as-new /
    /// conflict-resolve). Explicit error unless supported.
    fn lend_state(&mut self) -> NdcResult<NdcLentState> {
        Err(NdcError::internal("lend_state is not supported by this workflow"))
    }
    /// Takes the committed state back after a successful write.
    fn restore_state(&mut self, _ms: Box<dyn NdcMutableState>, _history_size: i64) {}
    /// Go `GetReleaseFn()(err)`.
    fn release(&mut self, err: Option<&NdcError>);
    /// The engine execution context behind this workflow (type-erased), when
    /// there is one; used by the reset collaborator to run the engine resetter.
    fn engine_context(&self) -> Option<std::sync::Arc<dyn std::any::Any + Send + Sync>> {
        None
    }

    fn get_vector_clock(&self) -> NdcResult<WorkflowVectorClock>;
    fn happens_after(&self, other: &dyn NdcWorkflow) -> NdcResult<bool>;
    fn suppress_by(&mut self, other: &dyn NdcWorkflow) -> NdcResult<TransactionPolicy>;
    fn revive(&mut self) -> NdcResult<()>;
    fn flush_buffered_events(&mut self) -> NdcResult<()>;

    fn update_workflow_execution_as_active(&mut self, now_nanos: i64) -> NdcResult<()>;
    fn update_workflow_execution_as_passive(&mut self, now_nanos: i64) -> NdcResult<()>;
    fn update_workflow_execution_with_new_as_passive(
        &mut self,
        now_nanos: i64,
        new_workflow: &mut dyn NdcWorkflow,
    ) -> NdcResult<()>;
    #[allow(clippy::too_many_arguments)]
    fn update_workflow_execution_with_new(
        &mut self,
        now_nanos: i64,
        mode: UpdateWorkflowMode,
        new_workflow: Option<&mut dyn NdcWorkflow>,
        current_policy: TransactionPolicy,
        new_policy: Option<TransactionPolicy>,
        request_mode: WorkflowRequestMode,
    ) -> NdcResult<()>;
    #[allow(clippy::too_many_arguments)]
    fn conflict_resolve_workflow_execution(
        &mut self,
        now_nanos: i64,
        mode: ConflictResolveWorkflowMode,
        new_workflow: Option<&mut dyn NdcWorkflow>,
        current_workflow: Option<&mut dyn NdcWorkflow>,
        current_policy: Option<TransactionPolicy>,
    ) -> NdcResult<()>;
    fn persist_start_workflow_batch_events(&mut self, events: &WorkflowEvents)
        -> NdcResult<PersistedBlob>;
    fn persist_non_start_workflow_batch_events(
        &mut self,
        events: &WorkflowEvents,
    ) -> NdcResult<PersistedBlob>;
    #[allow(clippy::too_many_arguments)]
    fn create_workflow_execution(
        &mut self,
        snapshot: WorkflowSnapshot,
        history_blob: PersistedBlob,
        mode: CreateWorkflowMode,
        prev_run_id: &str,
        prev_last_write_version: i64,
        request_mode: WorkflowRequestMode,
    ) -> NdcResult<()>;
    fn reapply_events(&mut self, events: &[WorkflowEvents]) -> NdcResult<()>;
    /// Go `context.LoadWorkflowExecution` / `LoadWorkflowExecutionWithTaskVersion`.
    /// `EntityNotExists` is returned when the mutable state is missing.
    fn load_workflow_execution(&mut self, task_version: Option<i64>) -> NdcResult<()>;
}

/// Go `execution.Cache.GetOrCreateWorkflowExecution` (the returned workflow is
/// not yet loaded; releasing it returns it to the cache).
pub trait NdcExecutionCache {
    fn get_or_create_workflow_execution(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> NdcResult<Box<dyn NdcWorkflow>>;
    /// Go `execution.NewContext` + `NewWorkflow(..., NoopReleaseFn)` for a new run.
    fn new_workflow(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
        mutable_state: Box<dyn NdcMutableState>,
        release_noop: bool,
    ) -> NdcResult<Box<dyn NdcWorkflow>>;
}

/// Go `defer func(){ if rec := recover() ... }()` part 1: runs `f`, capturing a panic.
pub(crate) fn guarded<T>(
    f: impl FnOnce() -> NdcResult<T>,
) -> Result<NdcResult<T>, Box<dyn std::any::Any + Send>> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
}

/// Part 2: calls `cleanup` with the outcome (`errPanic` on panic, then re-panics).
pub(crate) fn finish_guarded<T>(
    outcome: Result<NdcResult<T>, Box<dyn std::any::Any + Send>>,
    cleanup: impl FnOnce(Option<&NdcError>),
) -> NdcResult<T> {
    match outcome {
        Ok(r) => {
            cleanup(r.as_ref().err());
            r
        }
        Err(p) => {
            cleanup(Some(&NdcError::panic_error()));
            std::panic::resume_unwind(p)
        }
    }
}

/// Shared helper: events of type signaled that are not yet reapplied (Go `getReapplicableEvents`).
pub fn get_reapplicable_events(
    ms: &dyn NdcMutableState,
    events: &[HistoryEvent],
    run_id: &str,
) -> NdcResult<Vec<HistoryEvent>> {
    use cadence_api::enums::EventType;
    let mut result = Vec::new();
    for event in events.iter().filter(|e| e.event_type == Some(EventType::WorkflowExecutionSignaled)) {
        if !ms.is_event_reapplied(run_id, event.event_id, event.version)? {
            result.push(event.clone());
        }
    }
    Ok(result)
}
