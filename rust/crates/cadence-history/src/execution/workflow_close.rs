//! Workflow completion, failure, cancellation, timeout, termination, and
//! continue-as-new state updates from `mutable_state_builder_methods_*`.

use cadence_api::{service_types::HistoryEvent, types::EventType};
use crate::mutable_state::MutableStateBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum WorkflowCloseStatus { Completed = 1, Failed = 2, Canceled = 3, Terminated = 4, ContinuedAsNew = 5, TimedOut = 6 }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("workflow completion event is missing")]
pub struct MissingWorkflowCompletionEvent;

impl MutableStateBuilder {
    pub fn add_workflow_close_event(&mut self, first_event_id: i64, event: HistoryEvent) -> Option<HistoryEvent> {
        let expected = matches!(event.event_type.as_ref(), Some(EventType::WorkflowExecutionCompleted | EventType::WorkflowExecutionFailed | EventType::WorkflowExecutionTimedOut | EventType::WorkflowExecutionCanceled | EventType::WorkflowExecutionTerminated | EventType::WorkflowExecutionContinuedAsNew));
        if !expected { return None; }
        let event = self.add_event(event);
        self.replicate_workflow_close_event(first_event_id, &event)?;
        Some(event)
    }
    pub fn is_workflow_completed(&self) -> bool {
        self.execution_info.get("state").and_then(serde_json::Value::as_i64) == Some(2)
    }

    /// Apply the state and close status for a close event. Go's completion
    /// methods also clear sticky state, persist the event batch ID, and enqueue
    /// close tasks; task generation is exposed separately to history runtime.
    pub fn replicate_workflow_close_event(&mut self, first_event_id: i64, event: &HistoryEvent) -> Option<WorkflowCloseStatus> {
        let status = match event.event_type.clone()? {
            EventType::WorkflowExecutionCompleted => WorkflowCloseStatus::Completed,
            EventType::WorkflowExecutionFailed => WorkflowCloseStatus::Failed,
            EventType::WorkflowExecutionCanceled => WorkflowCloseStatus::Canceled,
            EventType::WorkflowExecutionTerminated => WorkflowCloseStatus::Terminated,
            EventType::WorkflowExecutionContinuedAsNew => WorkflowCloseStatus::ContinuedAsNew,
            EventType::WorkflowExecutionTimedOut => WorkflowCloseStatus::TimedOut,
            _ => return None,
        };
        self.execution_info["state"] = 2.into();
        self.execution_info["close_status"] = (status as i64).into();
        self.execution_info["completion_event_batch_id"] = first_event_id.into();
        self.execution_info["sticky_task_list"] = String::new().into();
        self.execution_info["sticky_schedule_to_start_timeout_seconds"] = 0.into();
        self.execution_info["completion_event"] = serde_json::to_value(event).expect("HistoryEvent is serializable");
        Some(status)
    }

    pub fn get_completion_event(&self) -> Result<HistoryEvent, MissingWorkflowCompletionEvent> {
        if !self.is_workflow_completed() { return Err(MissingWorkflowCompletionEvent); }
        self.execution_info.get("completion_event").cloned().and_then(|v| serde_json::from_value(v).ok()).ok_or(MissingWorkflowCompletionEvent)
    }
}
