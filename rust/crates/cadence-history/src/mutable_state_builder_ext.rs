//! Ports of small `mutableStateBuilder` methods from Go
//! `service/history/execution/mutable_state_builder.go`.
use crate::mutable_state::{MutableStateBuilder, Record};

pub const WORKFLOW_STATE_CREATED: i64 = 0;
pub const WORKFLOW_STATE_RUNNING: i64 = 1;
pub const WORKFLOW_STATE_COMPLETED: i64 = 2;
pub const WORKFLOW_STATE_ZOMBIE: i64 = 3;
pub const WORKFLOW_STATE_VOID: i64 = 4;
pub const WORKFLOW_STATE_CORRUPTED: i64 = 5;
pub const CLOSE_STATUS_NONE: i64 = 0;
pub const CLOSE_STATUS_TERMINATED: i64 = 4;
pub const CLOSE_STATUS_CONTINUED_AS_NEW: i64 = 5;
pub const CLOSE_STATUS_TIMED_OUT: i64 = 6;
pub const EMPTY_VERSION: i64 = -24;

fn invalid_transition(cur: i64, state: i64, close: i64) -> String {
    format!("unable to change workflow state from {cur} to {state}, close status {close}")
}

impl MutableStateBuilder {
    fn info_i64(&self, key: &str) -> i64 {
        self.execution_info.get(key).and_then(Record::as_i64).unwrap_or_default()
    }

    /// Go `GetStartVersion`: first item of the current version history.
    pub fn get_start_version(&self) -> Result<i64, String> {
        match &self.version_histories {
            Some(vh) => {
                let h = vh
                    .histories
                    .get(vh.current_version_history_index as usize)
                    .ok_or_else(|| "version histories current index out of range".to_string())?;
                h.items.first().map(|i| i.version).ok_or_else(|| "version history is empty".to_string())
            }
            None => Ok(EMPTY_VERSION),
        }
    }

    /// Go `GetWorkflowStateCloseStatus`.
    pub fn get_workflow_state_close_status(&self) -> (i64, i64) {
        (self.info_i64("state"), self.info_i64("close_status"))
    }

    /// Go `UpdateWorkflowStateCloseStatus` (WorkflowExecutionInfo validation).
    pub fn update_workflow_state_close_status(&mut self, state: i64, close_status: i64) -> Result<(), String> {
        let cur = self.info_i64("state");
        let cur_close = self.info_i64("close_status");
        let bad = || Err(invalid_transition(cur, state, close_status));
        let unknown = || Err(format!("unknown workflow state: {state}"));
        let none = close_status == CLOSE_STATUS_NONE;
        match cur {
            WORKFLOW_STATE_VOID => {}
            WORKFLOW_STATE_CREATED => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => {
                    if !none { return bad(); }
                }
                WORKFLOW_STATE_COMPLETED => {
                    if ![CLOSE_STATUS_TERMINATED, CLOSE_STATUS_TIMED_OUT, CLOSE_STATUS_CONTINUED_AS_NEW].contains(&close_status) { return bad(); }
                }
                _ => return unknown(),
            },
            WORKFLOW_STATE_RUNNING => match state {
                WORKFLOW_STATE_CREATED => return bad(),
                WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => { if !none { return bad(); } }
                WORKFLOW_STATE_COMPLETED => { if none { return bad(); } }
                _ => return unknown(),
            },
            WORKFLOW_STATE_COMPLETED => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => return bad(),
                WORKFLOW_STATE_COMPLETED => { if close_status != cur_close { return bad(); } }
                _ => return unknown(),
            },
            WORKFLOW_STATE_ZOMBIE => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING => { if !none { return bad(); } }
                WORKFLOW_STATE_COMPLETED | WORKFLOW_STATE_ZOMBIE => { if none { return bad(); } }
                _ => return unknown(),
            },
            _ => return unknown(),
        }
        self.execution_info["state"] = state.into();
        self.execution_info["close_status"] = close_status.into();
        Ok(())
    }
}
