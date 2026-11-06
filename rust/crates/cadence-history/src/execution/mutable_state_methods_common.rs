//! Shared helpers used by the `mutable_state_methods_*` ports of Go's
//! `mutable_state_builder_methods_*.go` on [`TypedMutableState`].
//!
//! Go mapping: `checkMutability`, `createInternalServerError`, `createCallerError`,
//! `ClearStickyness`, `UpdateWorkflowStateCloseStatus` and the
//! `hBuilder.addEvent` tail. Task generation (`taskGenerator.*`), the events
//! cache (`writeEventToCache`) and logging are intentionally not wired yet.

use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;

use super::mutable_state_builder::{TypedMutableState, TypedMutableStateError};
use crate::mutable_state_builder_ext as ext;

pub(crate) type Result<T> = std::result::Result<T, TypedMutableStateError>;

pub const WORKFLOW_STATE_CREATED: i32 = ext::WORKFLOW_STATE_CREATED as i32;
pub const WORKFLOW_STATE_RUNNING: i32 = ext::WORKFLOW_STATE_RUNNING as i32;
pub const WORKFLOW_STATE_COMPLETED: i32 = ext::WORKFLOW_STATE_COMPLETED as i32;
pub const WORKFLOW_STATE_ZOMBIE: i32 = ext::WORKFLOW_STATE_ZOMBIE as i32;
pub const WORKFLOW_STATE_VOID: i32 = ext::WORKFLOW_STATE_VOID as i32;

pub const CLOSE_STATUS_NONE: i32 = 0;
pub const CLOSE_STATUS_COMPLETED: i32 = 1;
pub const CLOSE_STATUS_FAILED: i32 = 2;
pub const CLOSE_STATUS_CANCELED: i32 = 3;
pub const CLOSE_STATUS_TERMINATED: i32 = 4;
pub const CLOSE_STATUS_CONTINUED_AS_NEW: i32 = 5;
pub const CLOSE_STATUS_TIMED_OUT: i32 = 6;

/// Go `TimerTaskStatusNone`.
pub const TIMER_TASK_STATUS_NONE: i32 = 0;

/// Go `ErrWorkflowFinished` message.
pub const ERR_WORKFLOW_FINISHED: &str = "invalid mutable state action: mutation after finish";
/// Go `ErrMissingTimerInfo` message.
pub const ERR_MISSING_TIMER_INFO: &str = "unable to get timer info";
/// Go `timerCancellationMsgTimerIDUnknown`.
pub const TIMER_CANCELLATION_MSG_TIMER_ID_UNKNOWN: &str = "TIMER_ID_UNKNOWN";

pub(crate) fn internal_err<T>(message: impl Into<String>) -> Result<T> {
    Err(TypedMutableStateError::Internal(message.into()))
}

impl TypedMutableState {
    /// Go `checkMutability`.
    pub(crate) fn check_mutability(&self) -> Result<()> {
        if !self.is_workflow_execution_running() {
            return internal_err(ERR_WORKFLOW_FINISHED);
        }
        Ok(())
    }

    /// Go `createInternalServerError`.
    pub(crate) fn create_internal_server_error(&self, op: &str) -> TypedMutableStateError {
        TypedMutableStateError::Internal(format!("{op} operation failed"))
    }

    /// Go `createCallerError`.
    pub(crate) fn create_caller_error(&self, op: &str) -> TypedMutableStateError {
        TypedMutableStateError::BadRequest(format!("invalid history builder state for action: {op}"))
    }

    /// Go `ClearStickyness`.
    pub fn clear_stickyness(&mut self) {
        self.execution_info.sticky_task_list = String::new();
        self.execution_info.sticky_schedule_to_start_timeout_nanos = 0;
        self.execution_info.client_library_version = String::new();
        self.execution_info.client_feature_version = String::new();
        self.execution_info.client_impl = String::new();
    }

    /// Go `WorkflowExecutionInfo.UpdateWorkflowStateCloseStatus`.
    pub fn update_workflow_state_close_status(&mut self, state: i32, close_status: i32) -> Result<()> {
        let cur = self.execution_info.state;
        let cur_close = self.execution_info.close_status;
        let bad = || -> Result<()> {
            internal_err(format!(
                "unable to change workflow state from {cur} to {state}, close status {close_status}"
            ))
        };
        let unknown = || -> Result<()> { internal_err(format!("unknown workflow state: {state}")) };
        let none = close_status == CLOSE_STATUS_NONE;
        match cur {
            WORKFLOW_STATE_VOID => {}
            WORKFLOW_STATE_CREATED => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => {
                    if !none {
                        return bad();
                    }
                }
                WORKFLOW_STATE_COMPLETED => {
                    if ![CLOSE_STATUS_TERMINATED, CLOSE_STATUS_TIMED_OUT, CLOSE_STATUS_CONTINUED_AS_NEW]
                        .contains(&close_status)
                    {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            WORKFLOW_STATE_RUNNING => match state {
                WORKFLOW_STATE_CREATED => return bad(),
                WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => {
                    if !none {
                        return bad();
                    }
                }
                WORKFLOW_STATE_COMPLETED => {
                    if none {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            WORKFLOW_STATE_COMPLETED => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => return bad(),
                WORKFLOW_STATE_COMPLETED => {
                    if close_status != cur_close {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            WORKFLOW_STATE_ZOMBIE => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING => {
                    if !none {
                        return bad();
                    }
                }
                WORKFLOW_STATE_COMPLETED | WORKFLOW_STATE_ZOMBIE => {
                    if none {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            _ => return unknown(),
        }
        self.execution_info.state = state;
        self.execution_info.close_status = close_status;
        Ok(())
    }

    /// Go `hBuilder.addEvent` for a freshly created event: create the event,
    /// let `fill` set its attributes, then route it into the transaction.
    pub(crate) fn add_history_event(
        &mut self,
        timestamp_unix_nanos: i64,
        event_type: EventType,
        fill: impl FnOnce(&mut HistoryEvent),
    ) -> Result<HistoryEvent> {
        let mut event = self.create_new_history_event(event_type, timestamp_unix_nanos)?;
        fill(&mut event);
        self.append_event(event.clone());
        Ok(event)
    }

    /// Go `checkAndClearTimerFiredEvent` (method form): searches buffered
    /// events, update-buffered events, then the history builder events.
    pub(crate) fn check_and_clear_timer_fired_event_in_state(&mut self, timer_id: &str) -> Option<HistoryEvent> {
        if let Some(e) = check_and_clear_timer_fired_event(&mut self.buffered_events, timer_id) {
            return Some(e);
        }
        if let Some(e) = check_and_clear_timer_fired_event(&mut self.update_buffered_events, timer_id) {
            return Some(e);
        }
        check_and_clear_timer_fired_event(&mut self.history_events, timer_id)
    }
}

/// Go package-level `checkAndClearTimerFiredEvent`.
pub fn check_and_clear_timer_fired_event(events: &mut Vec<HistoryEvent>, timer_id: &str) -> Option<HistoryEvent> {
    let idx = events.iter().position(|event| {
        matches!(event.event_type, Some(EventType::TimerFired))
            && event
                .timer_fired_event_attributes
                .as_ref()
                .is_some_and(|a| a.timer_id == timer_id)
    })?;
    Some(events.remove(idx))
}
