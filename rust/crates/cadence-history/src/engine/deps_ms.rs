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
    fn has_pending_decision(&self) -> bool;
    fn has_in_flight_decision(&self) -> bool;
    fn get_query_registry(&self) -> std::sync::Arc<dyn super::runtime::QueryRegistry>;
    fn get_child_execution_info(&self, initiated_id: i64) -> Option<ChildExecutionInfo>;
    fn add_child_workflow_execution_completed_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&cadence_api::tasks::WorkflowExecutionCompletedEventAttributes>) -> HResult<()>;
    fn add_child_workflow_execution_failed_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&cadence_api::tasks::WorkflowExecutionFailedEventAttributes>) -> HResult<()>;
    fn add_child_workflow_execution_canceled_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&cadence_api::tasks::WorkflowExecutionCanceledEventAttributes>) -> HResult<()>;
    fn add_child_workflow_execution_terminated_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes>) -> HResult<()>;
    fn add_child_workflow_execution_timed_out_event(&self, initiated_id: i64, execution: &WorkflowExecution, attrs: Option<&cadence_api::tasks::WorkflowExecutionTimedOutEventAttributes>) -> HResult<()>;
    fn get_current_version(&self) -> i64;
    fn get_last_first_event_id(&self) -> i64;
    fn get_previous_started_event_id(&self) -> i64;
    fn get_current_branch_token(&self) -> HResult<Vec<u8>>;
    fn get_version_histories(&self) -> Option<VersionHistories>;
    fn is_sticky_task_list_enabled(&self) -> bool;
    fn get_history_size(&self) -> i64;
    fn get_workflow_type(&self) -> Option<WorkflowType>;
    async fn get_resurrected_activities(&self) -> HResult<std::collections::HashSet<i64>>;
    fn delete_activity(&self, schedule_id: i64) -> HResult<()>;
    async fn get_activity_scheduled_event(&self, schedule_id: i64) -> HResult<cadence_api::service_types::HistoryEvent>;
    fn add_activity_task_started_event(&self, info: &ActivityInfo, schedule_id: i64, request_id: &str, identity: &str) -> HResult<ActivityInfo>;
    fn is_signal_requested(&self, request_id: &str) -> bool;
    fn add_signal_requested(&self, request_id: &str);
    fn has_processed_or_pending_decision(&self) -> bool;
    fn set_workflow_state(&self, state: i32);
    fn copy_to_persistence(&self) -> HResult<serde_json::Value>;
    /// Ordered JSON for DescribeMutableState; production uses Go persistence structs.
    fn copy_to_persistence_json(&self) -> HResult<String> {
        serde_json::to_string(&self.copy_to_persistence()?).map_err(|e| super::deps::HistoryError::internal(e.to_string()))
    }
    fn add_workflow_execution_signaled(&self, name: &str, input: &[u8], identity: &str, request_id: &str) -> HResult<()>;
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
    fn get_last_write_version(&self) -> HResult<i64>;
    fn is_resource_duplicated(&self, dedup_resource_id: &str) -> bool;
    /// Go `UpdateDuplicatedResource`.
    fn update_duplicated_resource(&self, dedup_resource_id: &str);
    fn get_pending_activity_infos(&self) -> Vec<ActivityInfo>;
    fn get_pending_child_execution_infos(&self) -> Vec<PendingChildInfo>;
    fn get_pending_decision(&self) -> Option<crate::mutable_state::DecisionInfo>;
    async fn get_completion_event(&self) -> HResult<cadence_api::service_types::HistoryEvent>;
    fn terminate_workflow(&self, first_event_id: i64, reason: &str, details: &[u8], identity: &str) -> HResult<()>;
    /// `AddWorkflowExecutionStartedEvent` (start path of `startWorkflowHelper`).
    fn add_workflow_execution_started_event(&self, execution: &WorkflowExecution, request: &HistoryStartWorkflowExecutionRequest) -> HResult<cadence_api::service_types::HistoryEvent>;
    /// `AddFirstDecisionTaskScheduled`
    fn add_first_decision_task_scheduled(&self, start_event: &cadence_api::service_types::HistoryEvent) -> HResult<()>;
    /// `AddDecisionTaskScheduledEvent(bypassTaskGeneration)`
    fn add_decision_task_scheduled_event(&self, bypass_task_generation: bool) -> HResult<()>;
    /// Downcast hook for the assembly adapters (default: not downcastable).
    fn as_any_arc(self: std::sync::Arc<Self>) -> Option<std::sync::Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
}
#[async_trait]
pub trait ExecutionContext: Send + Sync {
    fn get_execution(&self) -> WorkflowExecution;
    fn get_workflow_execution(&self) -> Option<super::deps::MutableStateRef>;
    async fn load_workflow_execution(&self) -> HResult<super::deps::MutableStateRef>;
    async fn update_workflow_execution_tasks(&self, now: i64) -> HResult<()>;
    /// `UpdateWorkflowExecutionAsActive`
    async fn update_workflow_execution_as_active(&self, now: i64) -> HResult<()>;
    /// `Clear` (drops the cached mutable state)
    fn clear(&self);
    /// Downcast hook for the assembly adapters (default: not downcastable).
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChildExecutionInfo { pub started_id: i64, pub started_workflow_id: String }

/// `persistence.ChildExecutionInfo` (fields read by DescribeWorkflowExecution).
#[derive(Debug, Clone, Default)]
pub struct PendingChildInfo {
    pub domain_id: String,
    pub domain_name: String,
    pub started_workflow_id: String,
    pub started_run_id: String,
    pub workflow_type_name: String,
    pub initiated_id: i64,
    pub parent_close_policy: Option<cadence_api::enums::ParentClosePolicy>,
}
