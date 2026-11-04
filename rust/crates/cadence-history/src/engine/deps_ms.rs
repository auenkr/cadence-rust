//! Dependency contracts used by the engine port.
//! TODO(link): implement with execution and workflow packages when their ports land.
use async_trait::async_trait;
use cadence_api::{tasks::*, types::*};
use super::{deps::{ActivityInfo, DomainCacheEntry, HResult, WorkflowExecutionInfo}, types::*};

#[async_trait]
pub trait MutableState: Send + Sync {
    fn is_workflow_execution_running(&self) -> bool;
    fn get_next_event_id(&self) -> i64;
    fn get_execution_info(&self) -> WorkflowExecutionInfo;
    fn get_domain_entry(&self) -> DomainCacheEntry;
    fn get_activity_info(&self, schedule_id: i64) -> Option<ActivityInfo>;
    fn get_activity_by_activity_id(&self, activity_id: &str) -> Option<ActivityInfo>;
    fn delete_signal_requested(&self, request_id: &str);
    fn clear_stickyness(&self);
    fn is_cancel_requested(&self) -> (bool, String);
    async fn get_start_event(&self) -> HResult<cadence_api::service_types::HistoryEvent>;
    fn add_activity_task_completed_event(&self, schedule_id: i64, started_id: i64, request: &RespondActivityTaskCompletedRequest) -> HResult<()>;
    fn add_activity_task_failed_event(&self, schedule_id: i64, started_id: i64, request: &RespondActivityTaskFailedRequest) -> HResult<()>;
    fn add_activity_task_canceled_event(&self, schedule_id: i64, started_id: i64, cancel_request_id: i64, details: &[u8], identity: &str) -> HResult<()>;
    fn retry_activity(&self, info: &ActivityInfo, reason: &str, details: &[u8], options: Option<&FailureOptions>) -> HResult<bool>;
    fn update_activity_progress(&self, info: &ActivityInfo, request: &RecordActivityTaskHeartbeatRequest);
    fn add_workflow_execution_cancel_requested_event(&self, cause: &str, request: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()>;
    fn terminate_workflow(&self, first_event_id: i64, reason: &str, details: &[u8], identity: &str) -> HResult<()>;
}
pub trait ExecutionContext: Send + Sync {}
