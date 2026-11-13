//! group: assembly/engine_decision
//!
//! Decision task manager over `EngineMutableState` (Go
//! `mutableStateDecisionTaskManagerImpl`) and the concrete `DecisionHandler`
//! (`decision/handler.go`: `HandleDecisionTaskScheduled`,
//! `HandleDecisionTaskStarted`, `HandleDecisionTaskFailed`).
//!
//! `HandleDecisionTaskCompleted` drives `decision::task_handler::TaskHandler`
//! over `engine_decision_ops::EngineOps`. Unported decision kinds return explicit
//! errors from the ops (see memory/history-dtcompleted-20261111.md).

use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::enums::{DecisionTaskFailedCause, EventType, TimeoutType};
use cadence_api::errors::{EventAlreadyStartedError, WorkflowExecutionAlreadyCompletedError};
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::{
    DecisionTaskCompletedEventAttributes, DecisionTaskFailedEventAttributes, DecisionTaskScheduledEventAttributes,
    DecisionTaskStartedEventAttributes, DecisionTaskTimedOutEventAttributes, RespondDecisionTaskCompletedRequest,
};
use crate::config::Config;
use crate::decision::task_handler::{ErrorKind, OperationError, TaskHandler};
use crate::wiring::assembly::engine_decision_ops::EngineOps;
use cadence_api::tasktoken::JsonTaskTokenSerializer;
use cadence_api::types::{TaskList, WorkflowExecution};

use crate::engine::deps::{HResult, HistoryError, EMPTY_EVENT_ID};
use crate::engine::deps_ms::MutableState;
use crate::engine::runtime::DecisionHandler;
use crate::engine::types::{
    HistoryRespondDecisionTaskCompletedRequest, HistoryRespondDecisionTaskCompletedResponse,
    HistoryRespondDecisionTaskFailedRequest, RecordDecisionTaskStartedRequest, RecordDecisionTaskStartedResponse,
    ScheduleDecisionTaskRequest, TransientDecisionInfo,
};
use crate::wiring::assembly::engine_cache::{EngineCore, EngineExecutionCache, EngineExecutionContext};
use crate::wiring::assembly::engine_state::{EngineMutableState, Inner, EMPTY_VERSION};

/// Go `emptyUuid`.
const EMPTY_UUID: &str = "00000000-0000-0000-0000-000000000000";
const CONDITIONAL_RETRY_COUNT: usize = 5;

/// Go `DecisionInfo`.
#[derive(Debug, Clone)]
pub struct DecisionSnapshot {
    pub version: i64,
    pub schedule_id: i64,
    pub started_id: i64,
    pub request_id: String,
    pub timeout_nanos: i64,
    pub attempt: i64,
    pub scheduled_nanos: i64,
    pub started_nanos: i64,
    pub task_list: String,
}

/// Go workflow.ErrDeserializingToken (workflow/errors.go:43).
fn err_deserializing_token() -> HistoryError {
    HistoryError::bad_request("error deserializing task token")
}

/// Go workflow.ErrAlreadyCompleted (workflow/errors.go:39): WorkflowExecutionAlreadyCompletedError.
fn already_completed_err() -> HistoryError {
    WorkflowExecutionAlreadyCompletedError { message: "workflow execution already completed".into() }.into()
}

#[cfg(test)]
mod respond_parity_tests {
    use super::*;

    #[test]
    fn go_error_messages_and_types() {
        assert_eq!(err_deserializing_token().to_string(), HistoryError::bad_request("error deserializing task token").to_string());
        assert!(already_completed_err().is_workflow_already_completed());
    }
}

fn invalid_action(what: &str, next_event_id: i64) -> HistoryError {
    HistoryError::internal(format!("invalid history action {what} (next event id {next_event_id})"))
}

impl Inner {
    /// Go `FailDecision`.
    pub(super) fn fail_decision(&mut self, increment_attempt: bool, now_nanos: i64) {
        self.clear_stickyness();
        let attempt = if increment_attempt { self.pc_exec.decision_attempt + 1 } else { 0 };
        self.pc_exec.decision_version = EMPTY_VERSION;
        self.pc_exec.decision_schedule_id = EMPTY_EVENT_ID;
        self.pc_exec.decision_started_id = EMPTY_EVENT_ID;
        self.pc_exec.decision_request_id = EMPTY_UUID.to_string();
        self.pc_exec.decision_timeout_nanos = 0;
        self.pc_exec.decision_started_time_unix_nanos = 0;
        self.pc_exec.decision_scheduled_time_unix_nanos = if increment_attempt { now_nanos } else { 0 };
        self.pc_exec.decision_attempt = attempt;
        self.pc_exec.decision_original_scheduled_time_unix_nanos = 0;
        self.mirror_decision();
        self.dirty = true;
    }
}

impl EngineMutableState {
    /// Go `GetDecisionInfo`.
    pub fn decision_info(&self, schedule_id: i64) -> Option<DecisionSnapshot> {
        let g = self.inner.lock().unwrap();
        if g.pc_exec.decision_schedule_id != schedule_id || schedule_id == EMPTY_EVENT_ID {
            return None;
        }
        let task_list =
            if g.exec.sticky_task_list.is_empty() { g.exec.task_list.clone() } else { g.exec.sticky_task_list.clone() };
        Some(DecisionSnapshot {
            version: g.pc_exec.decision_version,
            schedule_id,
            started_id: g.pc_exec.decision_started_id,
            request_id: g.pc_exec.decision_request_id.clone(),
            timeout_nanos: g.pc_exec.decision_timeout_nanos,
            attempt: g.pc_exec.decision_attempt,
            scheduled_nanos: g.pc_exec.decision_scheduled_time_unix_nanos,
            started_nanos: g.pc_exec.decision_started_time_unix_nanos,
            task_list,
        })
    }

    /// Go `AddDecisionTaskScheduledEvent`.
    pub fn add_decision_task_scheduled(&self, bypass_task_generation: bool) -> HResult<()> {
        let sticky = self.is_sticky_task_list_enabled();
        let now = self.now();
        let params = self.params.clone();
        let mut g = self.inner.lock().unwrap();
        if g.pc_exec.decision_schedule_id != EMPTY_EVENT_ID {
            return Err(invalid_action("DecisionTaskScheduled (pending decision)", g.next_event_id));
        }
        if !sticky {
            g.clear_stickyness();
        }
        let task_list = if sticky { g.exec.sticky_task_list.clone() } else { g.exec.task_list.clone() };
        let timeout_nanos = g.pc_exec.decision_start_to_close_timeout_nanos;
        if g.has_buffered() {
            // Buffered events make the decision a fresh (non transient) one.
            g.pc_exec.decision_attempt = 0;
            g.flush_buffered();
        }
        let attempt = g.pc_exec.decision_attempt;
        let mut schedule_id = g.next_event_id;
        if attempt == 0 {
            let tl = task_list.clone();
            let ev = g.push_event(EventType::DecisionTaskScheduled, now, |e| {
                e.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes {
                    task_list: Some(TaskList { name: tl, ..Default::default() }),
                    start_to_close_timeout_seconds: Some((timeout_nanos / 1_000_000_000) as i32),
                    attempt,
                });
            });
            schedule_id = ev.event_id;
        }
        // ReplicateDecisionTaskScheduledEvent
        let original = if attempt == 0 || g.pc_exec.decision_original_scheduled_time_unix_nanos == 0 {
            now
        } else {
            g.pc_exec.decision_original_scheduled_time_unix_nanos
        };
        g.pc_exec.decision_version = g.current_version;
        g.pc_exec.decision_schedule_id = schedule_id;
        g.pc_exec.decision_started_id = EMPTY_EVENT_ID;
        g.pc_exec.decision_request_id = EMPTY_UUID.to_string();
        g.pc_exec.decision_timeout_nanos = timeout_nanos;
        g.pc_exec.decision_attempt = attempt;
        g.pc_exec.decision_scheduled_time_unix_nanos = now;
        g.pc_exec.decision_started_time_unix_nanos = 0;
        g.pc_exec.decision_original_scheduled_time_unix_nanos = original;
        // Go ReplicateDecisionTaskScheduledEvent: Created -> Running unless Zombie.
        if g.exec.state != crate::engine::deps::WORKFLOW_STATE_ZOMBIE {
            g.exec.state = crate::engine::deps::WORKFLOW_STATE_RUNNING;
            g.exec.close_status = 0;
        }
        g.mirror_decision();
        g.dirty = true;
        if !bypass_task_generation {
            g.gen_decision_schedule_tasks(sticky, &params);
        }
        Ok(())
    }

    /// Go `AddDecisionTaskStartedEvent`.
    pub fn add_decision_task_started(
        &self,
        schedule_id: i64,
        request_id: &str,
        task_list: &str,
        identity: &str,
    ) -> HResult<DecisionSnapshot> {
        let now = self.now();
        {
            let mut g = self.inner.lock().unwrap();
            if !g.exec.is_running()
                || g.pc_exec.decision_schedule_id != schedule_id
                || g.pc_exec.decision_started_id != EMPTY_EVENT_ID
            {
                return Err(invalid_action("DecisionTaskStarted", g.next_event_id));
            }
            let mut sid = schedule_id;
            let mut started_id = sid + 1;
            let mut attempt = g.pc_exec.decision_attempt;
            if attempt > 0 && sid != g.next_event_id {
                // New events arrived after the transient decision was scheduled.
                let timeout = g.pc_exec.decision_timeout_nanos;
                let tl = task_list.to_string();
                let ev = g.push_event(EventType::DecisionTaskScheduled, now, |e| {
                    e.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes {
                        task_list: Some(TaskList { name: tl, ..Default::default() }),
                        start_to_close_timeout_seconds: Some((timeout / 1_000_000_000) as i32),
                        attempt: 0,
                    });
                });
                sid = ev.event_id;
                attempt = 0;
            }
            if attempt == 0 {
                let (rid, ident) = (request_id.to_string(), identity.to_string());
                let ev = g.push_event(EventType::DecisionTaskStarted, now, |e| {
                    e.decision_task_started_event_attributes = Some(DecisionTaskStartedEventAttributes {
                        scheduled_event_id: sid,
                        identity: ident,
                        request_id: rid,
                    });
                });
                started_id = ev.event_id;
            }
            // ReplicateDecisionTaskStartedEvent
            g.pc_exec.decision_version = g.current_version;
            g.pc_exec.decision_schedule_id = sid;
            g.pc_exec.decision_started_id = started_id;
            g.pc_exec.decision_request_id = request_id.to_string();
            g.pc_exec.decision_attempt = attempt;
            g.pc_exec.decision_started_time_unix_nanos = now;
            g.mirror_decision();
            g.dirty = true;
            g.gen_decision_start_tasks(sid);
        }
        let sid = self.inner.lock().unwrap().pc_exec.decision_schedule_id;
        self.decision_info(sid).ok_or_else(|| HistoryError::internal("decision vanished after start"))
    }

    /// `executionInfo.DecisionOriginalScheduledTimestamp`.
    pub fn decision_original_scheduled_nanos(&self) -> i64 {
        self.inner.lock().unwrap().pc_exec.decision_original_scheduled_time_unix_nanos
    }

    pub fn set_decision_original_scheduled_nanos(&self, nanos: i64) {
        self.inner.lock().unwrap().pc_exec.decision_original_scheduled_time_unix_nanos = nanos;
    }

    pub fn decision_schedule_id(&self) -> i64 {
        self.inner.lock().unwrap().pc_exec.decision_schedule_id
    }

    /// Go `executionInfo.ClientLibraryVersion/ClientFeatureVersion/ClientImpl = ...`
    /// (set from the request headers in `handleDecisionTaskCompleted`).
    pub fn set_client_info(&self, library_version: &str, feature_version: &str, client_impl: &str) {
        let mut g = self.inner.lock().unwrap();
        g.exec.client_library_version = library_version.to_string();
        g.exec.client_feature_version = feature_version.to_string();
        g.exec.client_impl = client_impl.to_string();
        g.dirty = true;
    }

    /// Go `ClearStickyness`.
    pub fn clear_sticky(&self) {
        self.inner.lock().unwrap().clear_stickyness();
    }

    /// Go `SetStickyTaskList` + `SetStickyScheduleToStartTimeout`.
    pub fn set_sticky(&self, task_list: &str, schedule_to_start_timeout_seconds: i32) {
        let mut g = self.inner.lock().unwrap();
        g.exec.sticky_task_list = task_list.to_string();
        g.exec.sticky_schedule_to_start_timeout = schedule_to_start_timeout_seconds;
        g.dirty = true;
    }

    /// Go `AddDecisionTaskCompletedEvent` (+ `BeforeAddDecisionTaskCompletedEvent`
    /// for a transient decision, + `AfterAddDecisionTaskCompletedEvent`). Returns
    /// the id of the DecisionTaskCompleted event (binary-checksum reset points via
    /// `add_binary_checksum_if_not_exists`).
    #[allow(clippy::too_many_arguments)]
    pub fn add_decision_task_completed(
        &self,
        schedule_id: i64,
        started_id: i64,
        execution_context: &[u8],
        identity: &str,
        binary_checksum: &str,
        max_reset_points: usize,
        advanced_visibility_writing: bool,
    ) -> HResult<i64> {
        let now = self.now();
        let ms = now / 1_000_000;
        let mut g = self.inner.lock().unwrap();
        if !g.exec.is_running()
            || g.pc_exec.decision_schedule_id != schedule_id
            || g.pc_exec.decision_started_id != started_id
        {
            return Err(invalid_action("DecisionTaskCompleted", g.next_event_id));
        }
        let (mut sid, mut stid) = (schedule_id, started_id);
        if g.pc_exec.decision_attempt > 0 {
            // Transient decision: materialise its scheduled and started events.
            let attempt = g.pc_exec.decision_attempt;
            let timeout = g.pc_exec.decision_timeout_nanos;
            let task_list =
                if g.exec.sticky_task_list.is_empty() { g.exec.task_list.clone() } else { g.exec.sticky_task_list.clone() };
            let scheduled_ns = g.pc_exec.decision_scheduled_time_unix_nanos;
            let started_ns = g.pc_exec.decision_started_time_unix_nanos;
            let ev = g.push_event(EventType::DecisionTaskScheduled, scheduled_ns, |e| {
                e.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes {
                    task_list: Some(TaskList { name: task_list, ..Default::default() }),
                    start_to_close_timeout_seconds: Some((timeout / 1_000_000_000) as i32),
                    attempt,
                });
            });
            sid = ev.event_id;
            let request_id = g.pc_exec.decision_request_id.clone();
            let ident = identity.to_string();
            let ev = g.push_event(EventType::DecisionTaskStarted, started_ns, |e| {
                e.decision_task_started_event_attributes = Some(DecisionTaskStartedEventAttributes {
                    scheduled_event_id: sid,
                    identity: ident,
                    request_id,
                });
            });
            stid = ev.event_id;
        }
        let (ctx, ident, checksum) = (execution_context.to_vec(), identity.to_string(), binary_checksum.to_string());
        let completed = g.push_event(EventType::DecisionTaskCompleted, ms * 1_000_000, |e| {
            e.decision_task_completed_event_attributes = Some(DecisionTaskCompletedEventAttributes {
                execution_context: ctx,
                scheduled_event_id: sid,
                started_event_id: stid,
                identity: ident,
                binary_checksum: checksum,
            });
        });
        // Go `addBinaryCheckSumIfNotExists` (+ GenerateWorkflowSearchAttrTasks when
        // advanced visibility writing is enabled).
        g.add_binary_checksum_if_not_exists(
            binary_checksum,
            completed.event_id,
            now,
            max_reset_points,
            advanced_visibility_writing,
        )?;
        // AfterAddDecisionTaskCompletedEvent + ReplicateDecisionTaskCompletedEvent:
        // DeleteDecision (stickiness is kept).
        g.last_processed_event = stid;
        g.pc_exec.decision_version = EMPTY_VERSION;
        g.pc_exec.decision_schedule_id = EMPTY_EVENT_ID;
        g.pc_exec.decision_started_id = EMPTY_EVENT_ID;
        g.pc_exec.decision_request_id = EMPTY_UUID.to_string();
        g.pc_exec.decision_timeout_nanos = 0;
        g.pc_exec.decision_attempt = 0;
        g.pc_exec.decision_started_time_unix_nanos = 0;
        g.pc_exec.decision_scheduled_time_unix_nanos = 0;
        g.pc_exec.decision_original_scheduled_time_unix_nanos = 0;
        g.mirror_decision();
        g.dirty = true;
        Ok(completed.event_id)
    }

    /// Go `AddDecisionTaskFailedEvent`.
    pub fn add_decision_task_failed(
        &self,
        schedule_id: i64,
        started_id: i64,
        cause: Option<DecisionTaskFailedCause>,
        details: &[u8],
        identity: &str,
        binary_checksum: &str,
    ) -> HResult<()> {
        let now = self.now();
        let mut g = self.inner.lock().unwrap();
        if g.pc_exec.decision_schedule_id != schedule_id || g.pc_exec.decision_started_id != started_id {
            return Err(invalid_action("DecisionTaskFailed", g.next_event_id));
        }
        // Only record the event when the decision is not a transient retry.
        if g.pc_exec.decision_attempt == 0 {
            let (details, identity, checksum) = (details.to_vec(), identity.to_string(), binary_checksum.to_string());
            g.push_event(EventType::DecisionTaskFailed, now, |e| {
                e.decision_task_failed_event_attributes = Some(DecisionTaskFailedEventAttributes {
                    scheduled_event_id: schedule_id,
                    started_event_id: started_id,
                    cause,
                    details,
                    identity,
                    binary_checksum: checksum,
                    ..Default::default()
                });
            });
        }
        g.fail_decision(true, now);
        Ok(())
    }

    /// Go `AddDecisionTaskScheduleToStartTimeoutEvent`.
    pub fn add_decision_task_schedule_to_start_timeout(&self, schedule_id: i64) -> HResult<()> {
        let now = self.now();
        let mut g = self.inner.lock().unwrap();
        if g.pc_exec.decision_schedule_id != schedule_id || g.pc_exec.decision_started_id != EMPTY_EVENT_ID {
            return Err(invalid_action("DecisionTaskTimedOut (schedule to start)", g.next_event_id));
        }
        if g.pc_exec.decision_attempt == 0 {
            g.push_event(EventType::DecisionTaskTimedOut, now, |e| {
                e.decision_task_timed_out_event_attributes = Some(DecisionTaskTimedOutEventAttributes {
                    scheduled_event_id: schedule_id,
                    started_event_id: 0,
                    timeout_type: Some(TimeoutType::ScheduleToStart),
                    ..Default::default()
                });
            });
        }
        // A schedule-to-start timeout does not increment the attempt.
        g.fail_decision(false, now);
        Ok(())
    }

    /// Go `createRecordDecisionTaskStartedResponse`.
    pub fn record_started_response(&self, d: &DecisionSnapshot, identity: &str) -> RecordDecisionTaskStartedResponse {
        let sticky = self.is_sticky_task_list_enabled();
        let g = self.inner.lock().unwrap();
        let mut resp = RecordDecisionTaskStartedResponse {
            workflow_type: Some(cadence_api::types::WorkflowType { name: g.exec.workflow_type_name.clone() }),
            previous_started_event_id: (g.last_processed_event != EMPTY_EVENT_ID).then_some(g.last_processed_event),
            scheduled_event_id: d.schedule_id,
            started_event_id: d.started_id,
            next_event_id: g.next_event_id,
            attempt: d.attempt,
            sticky_execution_enabled: sticky,
            workflow_execution_task_list: Some(TaskList { name: g.exec.task_list.clone(), ..Default::default() }),
            scheduled_timestamp: Some(d.scheduled_nanos),
            started_timestamp: Some(d.started_nanos),
            history_size: g.history_size,
            ..Default::default()
        };
        if d.attempt > 0 {
            // Transient decision: the events are not in the history yet.
            let scheduled = HistoryEvent {
                event_id: d.schedule_id,
                timestamp: Some(d.scheduled_nanos),
                event_type: Some(EventType::DecisionTaskScheduled),
                version: d.version,
                decision_task_scheduled_event_attributes: Some(DecisionTaskScheduledEventAttributes {
                    task_list: Some(TaskList { name: d.task_list.clone(), ..Default::default() }),
                    start_to_close_timeout_seconds: Some((d.timeout_nanos / 1_000_000_000) as i32),
                    attempt: d.attempt,
                }),
                ..Default::default()
            };
            let started = HistoryEvent {
                event_id: d.schedule_id + 1,
                timestamp: Some(d.started_nanos),
                event_type: Some(EventType::DecisionTaskStarted),
                version: d.version,
                decision_task_started_event_attributes: Some(DecisionTaskStartedEventAttributes {
                    scheduled_event_id: d.schedule_id,
                    identity: identity.to_string(),
                    request_id: d.request_id.clone(),
                }),
                ..Default::default()
            };
            resp.decision_info = Some(TransientDecisionInfo { scheduled_event: Some(scheduled), started_event: Some(started) });
        }
        let branch = match &g.version_histories {
            Some(vh) => vh
                .histories
                .get(vh.current_version_history_index as usize)
                .map(|h| h.branch_token.clone())
                .unwrap_or_else(|| g.branch_token.clone()),
            None => g.branch_token.clone(),
        };
        resp.branch_token = branch;
        let registry = &self.queries.0;
        for id in registry.get_buffered_ids() {
            if let Ok(q) = registry.get_query_input(&id) {
                resp.queries.insert(id, q);
            }
        }
        resp
    }
}

/// Go `types.DecisionTaskFailedCause` from the cause names emitted by the
/// decision task handler.
fn failed_cause(name: &str) -> Option<DecisionTaskFailedCause> {
    use DecisionTaskFailedCause as C;
    Some(match name {
        "UNHANDLED_DECISION" => C::UnhandledDecision,
        "BAD_SCHEDULE_ACTIVITY_ATTRIBUTES" => C::BadScheduleActivityAttributes,
        "BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES" => C::BadRequestCancelActivityAttributes,
        "BAD_START_TIMER_ATTRIBUTES" => C::BadStartTimerAttributes,
        "BAD_CANCEL_TIMER_ATTRIBUTES" => C::BadCancelTimerAttributes,
        "BAD_RECORD_MARKER_ATTRIBUTES" => C::BadRecordMarkerAttributes,
        "BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES" => C::BadCompleteWorkflowExecutionAttributes,
        "BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES" => C::BadFailWorkflowExecutionAttributes,
        "BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES" => C::BadCancelWorkflowExecutionAttributes,
        "BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES" => {
            C::BadRequestCancelExternalWorkflowExecutionAttributes
        }
        "BAD_CONTINUE_AS_NEW_ATTRIBUTES" => C::BadContinueAsNewAttributes,
        "START_TIMER_DUPLICATE_ID" => C::StartTimerDuplicateID,
        "BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES" => C::BadSignalWorkflowExecutionAttributes,
        "BAD_START_CHILD_EXECUTION_ATTRIBUTES" => C::BadStartChildExecutionAttributes,
        "SCHEDULE_ACTIVITY_DUPLICATE_ID" => C::ScheduleActivityDuplicateID,
        "BAD_SEARCH_ATTRIBUTES" => C::BadSearchAttributes,
        "BAD_ACQUIRE_SEMAPHORE_ATTRIBUTES" => C::BadAcquireSemaphoreAttributes,
        _ => return None,
    })
}

fn history_error(e: OperationError) -> HistoryError {
    match e.kind {
        ErrorKind::BadRequest => HistoryError::bad_request(e.message),
        ErrorKind::NotFound => HistoryError::entity_not_exists(e.message),
        _ => HistoryError::internal(e.message),
    }
}

/// Client SDK headers of the inbound call (Go `yarpc.CallFromContext(ctx).Header`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientHeaders {
    pub library_version: String,
    pub feature_version: String,
    pub client_impl: String,
}

impl ClientHeaders {
    pub const LIBRARY_VERSION: &'static str = "cadence-client-library-version";
    pub const FEATURE_VERSION: &'static str = "cadence-client-feature-version";
    pub const CLIENT_IMPL: &'static str = "cadence-client-implementation";

    /// Build from a header lookup (missing headers are empty strings, like yarpc).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        Self {
            library_version: get(Self::LIBRARY_VERSION).unwrap_or_default(),
            feature_version: get(Self::FEATURE_VERSION).unwrap_or_default(),
            client_impl: get(Self::CLIENT_IMPL).unwrap_or_default(),
        }
    }
}

tokio::task_local! {
    /// Set by the transports around the RespondDecisionTaskCompleted call.
    pub static CLIENT_HEADERS: ClientHeaders;
}

/// Go `versionChecker.SupportsConsistentQuery`: only uber-go / uber-java at feature
/// version >= 1.5.0 (an unparseable or prerelease version is not supported).
pub fn supports_consistent_query(client_impl: &str, feature_version: &str) -> bool {
    if !matches!(client_impl, "uber-go" | "uber-java") {
        return false;
    }
    let value = feature_version.strip_prefix('v').unwrap_or(feature_version);
    let (numbers, metadata) = value.split_once('+').map_or((value, None), |(n, m)| (n, Some(m)));
    if metadata.is_some_and(|m| {
        m.split('.').any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'~')))
    }) {
        return false;
    }
    let segments = numbers
        .split('.')
        .map(|p| if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) { None } else { p.parse::<u64>().ok() })
        .collect::<Option<Vec<_>>>();
    segments.is_some_and(|s| (s[0], s.get(1).copied().unwrap_or_default()) >= (1, 5))
}

/// Concrete `DecisionHandler` over the engine execution cache.
pub struct EngineDecisionHandler {
    cache: Arc<EngineExecutionCache>,
    core: Arc<EngineCore>,
    config: Arc<Config>,
}

/// What a decision-handler action decided.
struct Outcome<T> {
    noop: bool,
    create_decision: bool,
    value: T,
}

impl EngineDecisionHandler {
    pub fn new(cache: Arc<EngineExecutionCache>, core: Arc<EngineCore>, config: Arc<Config>) -> Self {
        Self { cache, core, config }
    }

    /// Go `handlerImpl.getActiveDomainByWorkflow`.
    async fn ensure_active_domain(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<()> {
        let active = self.core.active_cluster_manager.active_cluster_by_workflow(domain_id, workflow_id, run_id).await?;
        let domain = self.core.domains.get_domain_by_id(domain_id)?;
        let current = self.core.cluster_metadata.get_current_cluster_name();
        if active == current { Ok(()) } else { Err(domain.new_domain_not_active_error(&current, &active)) }
    }

    /// Go `HandleDecisionTaskCompleted` (retry loop of `workflow.UpdateWithAction`
    /// specialised for the failure path that must clear and reload the state).
    async fn completed_run(
        &self,
        context: &Arc<EngineExecutionContext>,
        req: &RespondDecisionTaskCompletedRequest,
        token_schedule_id: i64,
        token_attempt: i64,
    ) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        let force_new = req.force_create_new_decision_task;
        let return_new = req.return_new_decision_task;
        let heartbeat = force_new && req.decisions.is_empty();
        for attempt in 0..CONDITIONAL_RETRY_COUNT {
            let mut state = context.load_state().await?;
            if !state.is_workflow_execution_running() {
                return Err(already_completed_err());
            }
            let decision = state.decision_info(token_schedule_id);
            if decision.is_none() && token_schedule_id >= state.get_next_event_id() {
                context.clear();
                continue;
            }
            let d = decision
                .filter(|d| d.attempt == token_attempt && d.started_id != EMPTY_EVENT_ID)
                .ok_or_else(|| HistoryError::entity_not_exists("Decision task not found."))?;
            let domain_name = state.domain.info.name.clone();
            let original = state.decision_original_scheduled_nanos();
            let timeout = (self.config.decision_heartbeat_timeout)(&domain_name);
            let now = self.core.clock.now();
            let timed_out = heartbeat && original > 0 && now > original.saturating_add(timeout.as_nanos() as i64);
            let headers = CLIENT_HEADERS.try_with(|h| h.clone()).unwrap_or_default();
            state.set_client_info(&headers.library_version, &headers.feature_version, &headers.client_impl);
            let mut has_unhandled: bool;
            let mut local_dispatch: Vec<crate::engine::types::ActivityLocalDispatchInfo> = Vec::new();
            if timed_out {
                state.add_decision_timed_out(d.schedule_id, d.started_id)?;
                state.clear_sticky();
                has_unhandled = true;
            } else {
                let completed_id = state.add_decision_task_completed(
                    d.schedule_id,
                    d.started_id,
                    &req.execution_context,
                    &req.identity,
                    &req.binary_checksum,
                    (self.config.max_auto_reset_points)(&domain_name).max(0) as usize,
                    (self.core.advanced_visibility_writing)(),
                )?;
                match req.sticky_attributes.as_ref().and_then(|s| s.worker_task_list.as_ref()).filter(|t| !t.name.is_empty())
                {
                    Some(tl) => {
                        let timeout = req.sticky_attributes.as_ref().and_then(|s| s.schedule_to_start_timeout_seconds);
                        state.set_sticky(&tl.name, timeout.unwrap_or(0));
                    }
                    None => state.clear_sticky(),
                }
                if req.decisions.iter().any(|d| {
                    matches!(
                        d.decision_type,
                        Some(cadence_api::enums::DecisionType::ContinueAsNewWorkflowExecution)
                            | Some(cadence_api::enums::DecisionType::CompleteWorkflowExecution)
                            | Some(cadence_api::enums::DecisionType::FailWorkflowExecution)
                    )
                }) {
                    // Go `GetStartEvent`; the decision handler is synchronous.
                    state.preload_start_event().await;
                }
                let mut decisions = req.decisions.clone();
                // Go: BadBinaries.Binaries[binChecksum] fails the decision.
                let bad_binary = state.is_bad_binary(&req.binary_checksum);
                let (fail, cause, message, unhandled, not_started_cancelled) = if bad_binary {
                    (
                        true,
                        Some("BAD_BINARY"),
                        Some(format!("binary {} is already marked as bad deployment", req.binary_checksum)),
                        false,
                        false,
                    )
                } else {
                    let semaphore_lookups = if (self.config.enable_distributed_semaphore)(&domain_name) {
                        let sem_domain_id = state.inner.lock().unwrap().exec.domain_id.clone();
                        super::engine_decision_ops::resolve_semaphore_lookups(
                            &*self.core.semaphore_metadata,
                            &sem_domain_id,
                            &decisions,
                        )
                        .await
                    } else {
                        Default::default()
                    };
                    let ops = EngineOps {
                        semaphore_lookups,
                        state: &state,
                        config: self.config.clone(),
                        domains: self.core.domains.clone(),
                        domain_name: domain_name.clone(),
                        identity: req.identity.clone(),
                        completed_id,
                    };
                    let mut handler = TaskHandler::new(ops, req.identity.clone(), completed_id);
                    local_dispatch =
                        handler.handle_decisions(&req.execution_context, &mut decisions).map_err(history_error)?;
                    (
                        handler.fail_decision,
                        handler.fail_decision_cause,
                        handler.fail_message.clone(),
                        handler.has_unhandled_events_before_decisions,
                        handler.activity_not_started_cancelled,
                    )
                };
                has_unhandled = unhandled || not_started_cancelled;
                if fail {
                    // Go: clear the cached state (drops the half-applied
                    // decisions), reload and record DecisionTaskFailed.
                    context.clear();
                    state = context.load_state().await?;
                    local_dispatch.clear();
                    let details = message.unwrap_or_default();
                    state.add_decision_task_failed(
                        d.schedule_id,
                        d.started_id,
                        cause.and_then(failed_cause),
                        details.as_bytes(),
                        &req.identity,
                        &req.binary_checksum,
                    )?;
                    has_unhandled = true;
                }
            }
            let running = state.is_workflow_execution_running();
            let create_new = running && (has_unhandled || force_new);
            if create_new && !state.has_pending_decision() {
                state.add_decision_task_scheduled(return_new)?;
                if heartbeat && !timed_out {
                    state.set_decision_original_scheduled_nanos(original);
                }
            }
            let started = if create_new && return_new {
                let sid = state.decision_schedule_id();
                Some(state.add_decision_task_started(
                    sid,
                    "request-from-RespondDecisionTaskCompleted",
                    "",
                    &req.identity,
                )?)
            } else {
                None
            };
            let updated = match state.take_new_run() {
                Some(new_run) => self.update_with_new_run(context, &new_run).await,
                None => context.update_as_active(self.core.clock.now()).await,
            };
            match updated {
                Err(HistoryError::DuplicateRequest { .. }) => {}
                Err(e) if e.is_conflict_error() => {
                    if attempt != CONDITIONAL_RETRY_COUNT - 1 {
                        context.clear();
                    }
                    continue;
                }
                Err(e) => return Err(e),
                Ok(()) => {}
            }
            // Go handleBufferedQueries (fail-open when the client headers are absent).
            {
                struct Obs {
                    warn: usize,
                    error: usize,
                }
                impl crate::decision::handler::QueryObserver for Obs {
                    fn worker_not_supports_consistent_query(&self, _error: &str) {}
                    fn invalid_query_state(&self, _id: &str, _state: crate::query::TerminationType, _error: &str) {}
                    fn check_blob_size(&self, answer: &[u8]) -> Result<(), String> {
                        let _ = self.warn;
                        if answer.len() > self.error {
                            return Err("Blob data size exceeds limit.".to_string());
                        }
                        Ok(())
                    }
                }
                let obs = Obs {
                    warn: (self.config.blob_size_limit_warn)(&domain_name) as usize,
                    error: (self.config.blob_size_limit_error)(&domain_name) as usize,
                };
                let results = req.query_results.clone().unwrap_or_default();
                let version_error = (!supports_consistent_query(&headers.client_impl, &headers.feature_version))
                    .then_some("client version not supported");
                crate::decision::handler::handle_buffered_queries(
                    &state.queries.0,
                    &obs,
                    &headers.client_impl,
                    &headers.feature_version,
                    version_error,
                    &results,
                    create_new,
                    heartbeat,
                );
            }
            if timed_out {
                return Err(HistoryError::entity_not_exists("decision heartbeat timeout"));
            }
            let mut resp = HistoryRespondDecisionTaskCompletedResponse {
                activities_to_dispatch_locally: local_dispatch
                    .into_iter()
                    .map(|d| (d.activity_id.clone(), d))
                    .collect(),
                ..Default::default()
            };
            if let Some(started) = started {
                resp.started_response = Some(state.record_started_response(&started, &req.identity));
            }
            return Ok(resp);
        }
        Err(HistoryError::MaxAttemptsExceeded)
    }

    /// Go `UpdateWorkflowExecutionWithNewAsActive` for a ContinueAsNew decision:
    /// the closed current run and the new run are written in one update.
    async fn update_with_new_run(
        &self,
        context: &Arc<EngineExecutionContext>,
        new_run: &Arc<EngineMutableState>,
    ) -> HResult<()> {
        context.update_with_new_run(self.core.clock.now(), new_run).await
    }

    /// Go `workflow.UpdateWithAction` (the closure here is synchronous).
    async fn update<T>(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
        mut action: impl FnMut(&Arc<EngineMutableState>) -> HResult<Outcome<T>>,
    ) -> HResult<T> {
        let (context, release) = self.cache.get_or_create(domain_id, execution).await?;
        let result = self.run(&context, &mut action).await;
        release(result.as_ref().err());
        result
    }

    async fn run<T>(
        &self,
        context: &Arc<EngineExecutionContext>,
        action: &mut impl FnMut(&Arc<EngineMutableState>) -> HResult<Outcome<T>>,
    ) -> HResult<T> {
        for attempt in 0..CONDITIONAL_RETRY_COUNT {
            let state = context.load_state().await?;
            let outcome = match action(&state) {
                Ok(o) => o,
                Err(HistoryError::StaleState) => {
                    context.clear();
                    continue;
                }
                Err(e) => return Err(e),
            };
            if outcome.noop {
                return Ok(outcome.value);
            }
            if outcome.create_decision && !state.has_pending_decision() {
                state.add_decision_task_scheduled(false)?;
            }
            match context.update_as_active(self.core.clock.now()).await {
                Err(HistoryError::DuplicateRequest { .. }) => return Ok(outcome.value),
                Err(e) if e.is_conflict_error() => {
                    if attempt != CONDITIONAL_RETRY_COUNT - 1 {
                        context.clear();
                    }
                    continue;
                }
                Err(e) => return Err(e),
                Ok(()) => return Ok(outcome.value),
            }
        }
        Err(HistoryError::MaxAttemptsExceeded)
    }
}

#[async_trait]
impl DecisionHandler for EngineDecisionHandler {
    async fn handle_decision_task_started(
        &self,
        request: &RecordDecisionTaskStartedRequest,
    ) -> HResult<RecordDecisionTaskStartedResponse> {
        let execution = request.workflow_execution.clone().unwrap_or_default();
        self.ensure_active_domain(&request.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        let poll = request.poll_request.clone().unwrap_or_default();
        let identity = poll.identity.clone();
        let task_list = poll.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.update(&request.domain_uuid, &execution, |state| {
            if !state.is_workflow_execution_running() {
                return Err(HistoryError::entity_not_exists("workflow execution already completed"));
            }
            let schedule_id = request.schedule_id;
            let decision = state.decision_info(schedule_id);
            if decision.is_none() && schedule_id >= state.get_next_event_id() {
                // The cached mutable state may be stale: reload and retry.
                return Err(HistoryError::StaleState);
            }
            let d = decision.ok_or_else(|| HistoryError::entity_not_exists("Decision task not found."))?;
            if d.started_id != EMPTY_EVENT_ID {
                if d.request_id == request.request_id {
                    // Duplicate delivery of the same poll: answer without updating.
                    return Ok(Outcome { noop: true, create_decision: false, value: state.record_started_response(&d, &identity) });
                }
                return Err(EventAlreadyStartedError { message: "Decision task already started.".into() }.into());
            }
            let d = state.add_decision_task_started(schedule_id, &request.request_id, &task_list, &identity)?;
            Ok(Outcome { noop: false, create_decision: false, value: state.record_started_response(&d, &identity) })
        })
        .await
    }

    async fn handle_decision_task_completed(
        &self,
        request: &HistoryRespondDecisionTaskCompletedRequest,
    ) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        let req = request
            .complete_request
            .clone()
            .ok_or_else(|| HistoryError::bad_request("Missing complete request."))?;
        let token = JsonTaskTokenSerializer::deserialize(&req.task_token)
            .map_err(|_| err_deserializing_token())?;
        let execution = WorkflowExecution { workflow_id: token.workflow_id.clone(), run_id: token.run_id.clone() };
        self.ensure_active_domain(&request.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        let (context, release) = self.cache.get_or_create(&request.domain_uuid, &execution).await?;
        let result = self.completed_run(&context, &req, token.schedule_id, token.schedule_attempt).await;
        release(result.as_ref().err());
        result
    }

    async fn handle_decision_task_failed(&self, request: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()> {
        let failed = request
            .failed_request
            .clone()
            .ok_or_else(|| HistoryError::bad_request("Missing failed request."))?;
        let token = JsonTaskTokenSerializer::deserialize(&failed.task_token)
            .map_err(|_| err_deserializing_token())?;
        let execution = WorkflowExecution { workflow_id: token.workflow_id.clone(), run_id: token.run_id.clone() };
        self.ensure_active_domain(&request.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        self.update(&request.domain_uuid, &execution, |state| {
            if !state.is_workflow_execution_running() {
                return Err(already_completed_err());
            }
            let d = state
                .decision_info(token.schedule_id)
                .filter(|d| d.attempt == token.schedule_attempt && d.started_id != EMPTY_EVENT_ID)
                .ok_or_else(|| HistoryError::entity_not_exists("Decision task not found."))?;
            state.add_decision_task_failed(
                d.schedule_id,
                d.started_id,
                failed.cause,
                &failed.details,
                &failed.identity,
                &failed.binary_checksum,
            )?;
            // A terminated workflow must not schedule a new decision.
            let running = state.is_workflow_execution_running();
            Ok(Outcome { noop: false, create_decision: running, value: () })
        })
        .await
    }

    async fn handle_decision_task_scheduled(&self, request: &ScheduleDecisionTaskRequest) -> HResult<()> {
        let execution = request.workflow_execution.clone().unwrap_or_default();
        self.ensure_active_domain(&request.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        self.update(&request.domain_uuid, &execution, |state| {
            if !state.is_workflow_execution_running() {
                return Err(HistoryError::entity_not_exists("workflow execution already completed"));
            }
            if state.has_processed_or_pending_decision() {
                return Ok(Outcome { noop: true, create_decision: false, value: () });
            }
            state.add_decision_task_scheduled(false)?;
            Ok(Outcome { noop: false, create_decision: false, value: () })
        })
        .await
    }
}

impl super::engine_state::Inner {
    /// Go `CheckResettable`.
    fn is_resettable(&self) -> bool {
        self.pc_children.is_empty()
            && self.pc_cancels.is_empty()
            && self.pc_signals.is_empty()
            && self.semaphores.pending.is_empty()
    }

    /// Go `addBinaryCheckSumIfNotExists` + `trimBinaryChecksums`.
    pub(super) fn add_binary_checksum_if_not_exists(
        &mut self,
        bin_checksum: &str,
        completed_event_id: i64,
        now_nanos: i64,
        max_reset_points: usize,
        advanced_visibility_writing: bool,
    ) -> HResult<()> {
        use cadence_persistence::sqlblobs::Encoding;
        use cadence_thrift::ToThrift;
        if bin_checksum.is_empty() {
            return Ok(());
        }
        let mut curr = self.exec.auto_reset_points.clone().map(|p| p.points).unwrap_or_default();
        let mut recent: Vec<String> = curr.iter().map(|p| p.binary_checksum.clone()).collect();
        if recent.iter().any(|c| c == bin_checksum) {
            return Ok(());
        }
        if curr.len() >= max_reset_points {
            let start = (curr.len() - max_reset_points + 1).min(curr.len());
            curr.drain(..start);
            recent.drain(..start);
        }
        recent.push(bin_checksum.to_string());
        curr.push(cadence_api::types::ResetPointInfo {
            binary_checksum: bin_checksum.to_string(),
            run_id: self.exec.run_id.clone(),
            first_decision_completed_id: completed_event_id,
            created_time_nano: Some(now_nanos),
            expiring_time_nano: None,
            resettable: self.is_resettable(),
        });
        let points = cadence_api::types::ResetPoints { points: curr };
        let data = Encoding::ThriftRW
            .encode_versioned(&points.to_thrift())
            .map_err(|e| HistoryError::internal(format!("encode auto reset points: {e}")))?;
        self.pc_exec.auto_reset_points =
            Some(cadence_persistence::history_contract::DataBlob { encoding: "thriftrw".to_string(), data });
        self.exec.auto_reset_points = Some(points);
        let bytes = serde_json::to_vec(&recent).map_err(|e| HistoryError::internal(e.to_string()))?;
        self.pc_exec.search_attributes.insert("BinaryChecksums".to_string(), bytes.clone());
        self.exec.search_attributes.get_or_insert_with(Default::default).insert("BinaryChecksums".to_string(), bytes);
        self.dirty = true;
        if advanced_visibility_writing {
            self.gen_search_attr_task();
        }
        Ok(())
    }
}
