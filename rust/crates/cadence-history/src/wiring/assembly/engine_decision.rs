//! group: assembly/engine_decision
//!
//! Decision task manager over `EngineMutableState` (Go
//! `mutableStateDecisionTaskManagerImpl`) and the concrete `DecisionHandler`
//! (`decision/handler.go`: `HandleDecisionTaskScheduled`,
//! `HandleDecisionTaskStarted`, `HandleDecisionTaskFailed`).
//!
//! `HandleDecisionTaskCompleted` (the decision command processor of
//! `decision_task_handler.go`) is not ported here and returns an explicit error.

use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::enums::{DecisionTaskFailedCause, EventType, TimeoutType};
use cadence_api::errors::EventAlreadyStartedError;
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::{
    DecisionTaskFailedEventAttributes, DecisionTaskScheduledEventAttributes, DecisionTaskStartedEventAttributes,
    DecisionTaskTimedOutEventAttributes,
};
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
            let ev = g.push_event(EventType::DecisionTaskScheduled, now / 1_000_000, |e| {
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
                let ev = g.push_event(EventType::DecisionTaskScheduled, now / 1_000_000, |e| {
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
                let ev = g.push_event(EventType::DecisionTaskStarted, now / 1_000_000, |e| {
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
            g.push_event(EventType::DecisionTaskFailed, now / 1_000_000, |e| {
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
            g.push_event(EventType::DecisionTaskTimedOut, now / 1_000_000, |e| {
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
                timestamp: Some(d.scheduled_nanos / 1_000_000),
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
                timestamp: Some(d.started_nanos / 1_000_000),
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

/// Concrete `DecisionHandler` over the engine execution cache.
pub struct EngineDecisionHandler {
    cache: Arc<EngineExecutionCache>,
    core: Arc<EngineCore>,
}

/// What a decision-handler action decided.
struct Outcome<T> {
    noop: bool,
    create_decision: bool,
    value: T,
}

impl EngineDecisionHandler {
    pub fn new(cache: Arc<EngineExecutionCache>, core: Arc<EngineCore>) -> Self {
        Self { cache, core }
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
        let poll = request.poll_request.clone().unwrap_or_default();
        let identity = poll.identity.clone();
        let task_list = poll.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.update(&request.domain_uuid, &execution, |state| {
            if !state.is_workflow_execution_running() {
                return Err(HistoryError::entity_not_exists("Workflow execution already completed."));
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
        _request: &HistoryRespondDecisionTaskCompletedRequest,
    ) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        Err(HistoryError::internal(
            "RespondDecisionTaskCompleted is not ported on the engine mutable state yet (decision command processor)",
        ))
    }

    async fn handle_decision_task_failed(&self, request: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()> {
        let failed = request
            .failed_request
            .clone()
            .ok_or_else(|| HistoryError::bad_request("Missing failed request."))?;
        let token = JsonTaskTokenSerializer::deserialize(&failed.task_token)
            .map_err(|_| HistoryError::bad_request("Error deserializing task token."))?;
        let execution = WorkflowExecution { workflow_id: token.workflow_id.clone(), run_id: token.run_id.clone() };
        self.update(&request.domain_uuid, &execution, |state| {
            if !state.is_workflow_execution_running() {
                return Err(HistoryError::entity_not_exists("workflow execution already completed"));
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
            Ok(Outcome { noop: false, create_decision: true, value: () })
        })
        .await
    }

    async fn handle_decision_task_scheduled(&self, request: &ScheduleDecisionTaskRequest) -> HResult<()> {
        let execution = request.workflow_execution.clone().unwrap_or_default();
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
