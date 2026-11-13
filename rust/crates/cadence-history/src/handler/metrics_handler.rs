//! Go handler `startRequestProfile` / `updateErrorMetric` (go/service/history/handler/handler.go):
//! per-RPC `CadenceRequests`, `CadenceLatency`, `CadenceLatencyHistogram` and error counters.
use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use crate::engine::deps::{HResult, HistoryError};
use crate::handler::Handler;
use crate::task::deps::MetricsScope as _;
use crate::wiring::resources::metrics::{MetricsRegistry, Scope};

pub type ErrorConverter = Arc<dyn Fn(HistoryError) -> HistoryError + Send + Sync>;

pub struct MetricsHandler { pub wrapped: Arc<dyn Handler>, pub registry: Arc<MetricsRegistry>, pub convert_error: Option<ErrorConverter> }

fn snake_to_pascal(s: &str) -> String {
    s.split('_').map(|p| { let mut c = p.chars(); c.next().map(|f| f.to_uppercase().collect::<String>() + c.as_str()).unwrap_or_default() }).collect()
}

impl MetricsHandler {
    async fn profile<T>(&self, op: &str, fut: impl std::future::Future<Output = HResult<T>>) -> HResult<T> {
        let scope = Scope::new(self.registry.clone(), &format!("History{}", snake_to_pascal(op)), vec![]);
        scope.inc_counter("CadenceRequests");
        let start = Instant::now();
        let res = fut.await;
        // Go handler methods return `h.convertError(err)` before `updateErrorMetric`.
        let res = match (res, &self.convert_error) { (Err(e), Some(c)) => Err(c(e)), (r, _) => r };
        let d = start.elapsed();
        scope.record_timer("CadenceLatency", d);
        scope.record_histogram_duration("CadenceLatencyHistogram", d);
        if let Err(e) = &res { Self::update_error_metric(&scope, op, e); }
        res
    }
    /// Go `updateErrorMetric`.
    fn update_error_metric(scope: &Scope, op: &str, e: &HistoryError) {
        if matches!(e, HistoryError::DeadlineExceeded | HistoryError::Canceled) {
            scope.inc_counter("CadenceErrContextTimeoutCounter");
            return;
        }
        let (name, msg) = crate::wiring::server::errors::thrift_exception(e);
        let counter = match name {
            "ShardOwnershipLostError" => "CadenceErrShardOwnershipLostCounter",
            "EventAlreadyStartedError" => "CadenceErrEventAlreadyStartedCounter",
            "BadRequestError" => "CadenceErrBadRequestCounter",
            "DomainNotActiveError" => "CadenceErrDomainNotActiveCounter",
            "WorkflowExecutionAlreadyStartedError" => "CadenceErrExecutionAlreadyStartedCounter",
            "EntityNotExistsError" => "CadenceErrEntityNotExistsCounter",
            "WorkflowExecutionAlreadyCompletedError" => "CadenceErrWorkflowExecutionAlreadyCompletedCounter",
            "CancellationAlreadyRequestedError" => "CadenceErrCancellationAlreadyRequestedCounter",
            "LimitExceededError" => "CadenceErrLimitExceededCounter",
            "RetryTaskV2Error" => "CadenceErrRetryTaskCounter",
            "ServiceBusyError" => "CadenceErrServiceBusyCounter",
            "QueryFailedError" => "CadenceErrQueryFailedCounter",
            _ => {
                scope.inc_counter("CadenceFailures");
                if name == "InternalServiceError" {
                    tracing::error!(operation = op, error = %msg, "Internal service error");
                } else {
                    tracing::error!(operation = op, error = %msg, "Uncategorized error");
                }
                return;
            }
        };
        scope.inc_counter(counter);
    }
}

#[async_trait]
impl Handler for MetricsHandler {
    fn start(&self) { self.wrapped.start(); }
    fn stop(&self) { self.wrapped.stop(); }
    async fn prepare_to_stop(&self, remaining: std::time::Duration) -> std::time::Duration { self.wrapped.prepare_to_stop(remaining).await }
    async fn health(&self) -> HResult<crate::engine::types::HealthStatus> { self.wrapped.health().await }
    async fn close_shard(&self, request: Option<&cadence_api::admin::CloseShardRequest>) -> HResult<()> { self.profile("close_shard", self.wrapped.close_shard(request)).await }
    async fn describe_history_host(&self, request: Option<&cadence_api::admin::DescribeHistoryHostRequest>) -> HResult<cadence_api::admin::DescribeHistoryHostResponse> { self.profile("describe_history_host", self.wrapped.describe_history_host(request)).await }
    async fn describe_mutable_state(&self, request: Option<&crate::engine::types::DescribeMutableStateRequest>) -> HResult<crate::engine::types::DescribeMutableStateResponse> { self.profile("describe_mutable_state", self.wrapped.describe_mutable_state(request)).await }
    async fn describe_queue(&self, request: Option<&cadence_api::admin::DescribeQueueRequest>) -> HResult<cadence_api::admin::DescribeQueueResponse> { self.profile("describe_queue", self.wrapped.describe_queue(request)).await }
    async fn describe_workflow_execution(&self, request: Option<&crate::engine::types::HistoryDescribeWorkflowExecutionRequest>) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse> { self.profile("describe_workflow_execution", self.wrapped.describe_workflow_execution(request)).await }
    async fn get_cross_cluster_tasks(&self, request: Option<&cadence_api::admin::GetCrossClusterTasksRequest>) -> HResult<cadence_api::admin::GetCrossClusterTasksResponse> { self.profile("get_cross_cluster_tasks", self.wrapped.get_cross_cluster_tasks(request)).await }
    async fn count_dlq_messages(&self, request: Option<&cadence_api::admin::CountDLQMessagesRequest>) -> HResult<crate::engine::types::HistoryCountDLQMessagesResponse> { self.profile("count_dlq_messages", self.wrapped.count_dlq_messages(request)).await }
    async fn get_dlq_replication_messages(&self, request: Option<&cadence_api::admin::GetDLQReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetDLQReplicationMessagesResponse> { self.profile("get_dlq_replication_messages", self.wrapped.get_dlq_replication_messages(request)).await }
    async fn get_mutable_state(&self, request: Option<&crate::engine::types::GetMutableStateRequest>) -> HResult<crate::engine::types::GetMutableStateResponse> { self.profile("get_mutable_state", self.wrapped.get_mutable_state(request)).await }
    async fn get_replication_messages(&self, request: Option<&cadence_api::admin::GetReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetReplicationMessagesResponse> { self.profile("get_replication_messages", self.wrapped.get_replication_messages(request)).await }
    async fn merge_dlq_messages(&self, request: Option<&cadence_api::admin::MergeDLQMessagesRequest>) -> HResult<cadence_api::admin::MergeDLQMessagesResponse> { self.profile("merge_dlq_messages", self.wrapped.merge_dlq_messages(request)).await }
    async fn notify_failover_markers(&self, request: Option<&crate::engine::types::NotifyFailoverMarkersRequest>) -> HResult<()> { self.profile("notify_failover_markers", self.wrapped.notify_failover_markers(request)).await }
    async fn poll_mutable_state(&self, request: Option<&crate::engine::types::PollMutableStateRequest>) -> HResult<crate::engine::types::PollMutableStateResponse> { self.profile("poll_mutable_state", self.wrapped.poll_mutable_state(request)).await }
    async fn purge_dlq_messages(&self, request: Option<&cadence_api::admin::PurgeDLQMessagesRequest>) -> HResult<()> { self.profile("purge_dlq_messages", self.wrapped.purge_dlq_messages(request)).await }
    async fn query_workflow(&self, request: Option<&crate::engine::types::HistoryQueryWorkflowRequest>) -> HResult<crate::engine::types::HistoryQueryWorkflowResponse> { self.profile("query_workflow", self.wrapped.query_workflow(request)).await }
    async fn read_dlq_messages(&self, request: Option<&cadence_api::admin::ReadDLQMessagesRequest>) -> HResult<cadence_api::admin::ReadDLQMessagesResponse> { self.profile("read_dlq_messages", self.wrapped.read_dlq_messages(request)).await }
    async fn reapply_events(&self, request: Option<&crate::engine::types::HistoryReapplyEventsRequest>) -> HResult<()> { self.profile("reapply_events", self.wrapped.reapply_events(request)).await }
    async fn record_activity_task_heartbeat(&self, request: Option<&crate::engine::types::HistoryRecordActivityTaskHeartbeatRequest>) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse> { self.profile("record_activity_task_heartbeat", self.wrapped.record_activity_task_heartbeat(request)).await }
    async fn record_activity_task_started(&self, request: Option<&crate::engine::types::RecordActivityTaskStartedRequest>) -> HResult<crate::engine::types::RecordActivityTaskStartedResponse> { self.profile("record_activity_task_started", self.wrapped.record_activity_task_started(request)).await }
    async fn record_child_execution_completed(&self, request: Option<&crate::engine::types::RecordChildExecutionCompletedRequest>) -> HResult<()> { self.profile("record_child_execution_completed", self.wrapped.record_child_execution_completed(request)).await }
    async fn record_decision_task_started(&self, request: Option<&crate::engine::types::RecordDecisionTaskStartedRequest>) -> HResult<crate::engine::types::RecordDecisionTaskStartedResponse> { self.profile("record_decision_task_started", self.wrapped.record_decision_task_started(request)).await }
    async fn refresh_workflow_tasks(&self, request: Option<&crate::engine::types::HistoryRefreshWorkflowTasksRequest>) -> HResult<()> { self.profile("refresh_workflow_tasks", self.wrapped.refresh_workflow_tasks(request)).await }
    async fn remove_signal_mutable_state(&self, request: Option<&crate::engine::types::RemoveSignalMutableStateRequest>) -> HResult<()> { self.profile("remove_signal_mutable_state", self.wrapped.remove_signal_mutable_state(request)).await }
    async fn remove_task(&self, request: Option<&cadence_api::admin::RemoveTaskRequest>) -> HResult<()> { self.profile("remove_task", self.wrapped.remove_task(request)).await }
    async fn replicate_events_v2(&self, request: Option<&crate::engine::types::ReplicateEventsV2Request>) -> HResult<()> { self.profile("replicate_events_v2", self.wrapped.replicate_events_v2(request)).await }
    async fn request_cancel_workflow_execution(&self, request: Option<&crate::engine::types::HistoryRequestCancelWorkflowExecutionRequest>) -> HResult<()> { self.profile("request_cancel_workflow_execution", self.wrapped.request_cancel_workflow_execution(request)).await }
    async fn reset_queue(&self, request: Option<&cadence_api::admin::ResetQueueRequest>) -> HResult<()> { self.profile("reset_queue", self.wrapped.reset_queue(request)).await }
    async fn reset_sticky_task_list(&self, request: Option<&crate::engine::types::HistoryResetStickyTaskListRequest>) -> HResult<crate::engine::types::HistoryResetStickyTaskListResponse> { self.profile("reset_sticky_task_list", self.wrapped.reset_sticky_task_list(request)).await }
    async fn reset_workflow_execution(&self, request: Option<&crate::engine::types::HistoryResetWorkflowExecutionRequest>) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse> { self.profile("reset_workflow_execution", self.wrapped.reset_workflow_execution(request)).await }
    async fn respond_activity_task_canceled(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCanceledRequest>) -> HResult<()> { self.profile("respond_activity_task_canceled", self.wrapped.respond_activity_task_canceled(request)).await }
    async fn respond_activity_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCompletedRequest>) -> HResult<()> { self.profile("respond_activity_task_completed", self.wrapped.respond_activity_task_completed(request)).await }
    async fn respond_activity_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskFailedRequest>) -> HResult<()> { self.profile("respond_activity_task_failed", self.wrapped.respond_activity_task_failed(request)).await }
    async fn respond_cross_cluster_tasks_completed(&self, request: Option<&cadence_api::admin::RespondCrossClusterTasksCompletedRequest>) -> HResult<cadence_api::admin::RespondCrossClusterTasksCompletedResponse> { self.profile("respond_cross_cluster_tasks_completed", self.wrapped.respond_cross_cluster_tasks_completed(request)).await }
    async fn respond_decision_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskCompletedRequest>) -> HResult<crate::engine::types::HistoryRespondDecisionTaskCompletedResponse> { self.profile("respond_decision_task_completed", self.wrapped.respond_decision_task_completed(request)).await }
    async fn respond_decision_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> { self.profile("respond_decision_task_failed", self.wrapped.respond_decision_task_failed(request)).await }
    async fn schedule_decision_task(&self, request: Option<&crate::engine::types::ScheduleDecisionTaskRequest>) -> HResult<()> { self.profile("schedule_decision_task", self.wrapped.schedule_decision_task(request)).await }
    async fn signal_with_start_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWithStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> { self.profile("signal_with_start_workflow_execution", self.wrapped.signal_with_start_workflow_execution(request)).await }
    async fn signal_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWorkflowExecutionRequest>) -> HResult<()> { self.profile("signal_workflow_execution", self.wrapped.signal_workflow_execution(request)).await }
    async fn start_workflow_execution(&self, request: Option<&crate::engine::types::HistoryStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> { self.profile("start_workflow_execution", self.wrapped.start_workflow_execution(request)).await }
    async fn sync_activity(&self, request: Option<&crate::engine::types::SyncActivityRequest>) -> HResult<()> { self.profile("sync_activity", self.wrapped.sync_activity(request)).await }
    async fn sync_shard_status(&self, request: Option<&crate::engine::types::SyncShardStatusRequest>) -> HResult<()> { self.profile("sync_shard_status", self.wrapped.sync_shard_status(request)).await }
    async fn terminate_workflow_execution(&self, request: Option<&crate::engine::types::HistoryTerminateWorkflowExecutionRequest>) -> HResult<()> { self.profile("terminate_workflow_execution", self.wrapped.terminate_workflow_execution(request)).await }
    async fn get_failover_info(&self, request: Option<&crate::engine::types::GetFailoverInfoRequest>) -> HResult<crate::engine::types::GetFailoverInfoResponse> { self.profile("get_failover_info", self.wrapped.get_failover_info(request)).await }
    async fn ratelimit_update(&self, request: Option<&crate::engine::types::RatelimitUpdateRequest>) -> HResult<crate::engine::types::RatelimitUpdateResponse> { self.profile("ratelimit_update", self.wrapped.ratelimit_update(request)).await }
}
