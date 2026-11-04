//! Workflow RPC validation and routing ported from handler/handler.go.
use crate::engine::deps::{HResult, HistoryError};
use super::runtime::HistoryHandler;
impl HistoryHandler {
    pub async fn describe_mutable_state(&self, request: Option<&crate::engine::types::DescribeMutableStateRequest>) -> HResult<crate::engine::types::DescribeMutableStateResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("DescribeMutableState");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("DescribeMutableState", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        let execution = request.execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("DescribeMutableState", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.describe_mutable_state(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn describe_workflow_execution(&self, request: Option<&crate::engine::types::HistoryDescribeWorkflowExecutionRequest>) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("DescribeWorkflowExecution");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("DescribeWorkflowExecution", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.request.clone().and_then(|r| r.execution).unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("DescribeWorkflowExecution", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.describe_workflow_execution(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn get_mutable_state(&self, request: Option<&crate::engine::types::GetMutableStateRequest>) -> HResult<crate::engine::types::GetMutableStateResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("GetMutableState");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("GetMutableState", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("GetMutableState", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.get_mutable_state(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn poll_mutable_state(&self, request: Option<&crate::engine::types::PollMutableStateRequest>) -> HResult<crate::engine::types::PollMutableStateResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("PollMutableState");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("PollMutableState", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("PollMutableState", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.poll_mutable_state(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn query_workflow(&self, request: Option<&crate::engine::types::HistoryQueryWorkflowRequest>) -> HResult<crate::engine::types::HistoryQueryWorkflowResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("QueryWorkflow");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("QueryWorkflow", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.request.clone().and_then(|r| r.execution).unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("QueryWorkflow", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.query_workflow(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn reapply_events(&self, request: Option<&crate::engine::types::HistoryReapplyEventsRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("ReapplyEvents"); self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let inner = request.request.clone().unwrap_or_default();
        let execution = inner.workflow_execution.clone().unwrap_or_default();
        let fail = |error| self.error("ReapplyEvents", &request.domain_uuid, &execution.workflow_id, &execution.run_id, error);
        let engine = self.resources.engine_for_workflow(&execution.workflow_id).map_err(&fail)?;
        // Go forces ThriftRW encoding regardless of the caller's encoding field.
        let history = self.resources.deserialize_thriftrw_history(&inner.events.unwrap_or_default().data).map_err(&fail)?;
        engine.reapply_history_events(&request.domain_uuid, &execution.workflow_id, &execution.run_id, history).await.map_err(fail)
    }

    pub async fn record_activity_task_heartbeat(&self, request: Option<&crate::engine::types::HistoryRecordActivityTaskHeartbeatRequest>) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RecordActivityTaskHeartbeat");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RecordActivityTaskHeartbeat", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let token = self.token(&request.heartbeat_request.clone().unwrap_or_default().task_token).map_err(&fail)?;
        let workflow = token.workflow_id; let run = token.run_id;
        let fail = |error| self.error("RecordActivityTaskHeartbeat", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.record_activity_task_heartbeat(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn record_activity_task_started(&self, request: Option<&crate::engine::types::RecordActivityTaskStartedRequest>) -> HResult<crate::engine::types::RecordActivityTaskStartedResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RecordActivityTaskStarted");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        self.emit_info_or_debug_log(domain, "RecordActivityTaskStarted", &workflow, &run, request.schedule_id);
        let fail = |error| self.error("RecordActivityTaskStarted", domain, &workflow, "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.record_activity_task_started(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn record_child_execution_completed(&self, request: Option<&crate::engine::types::RecordChildExecutionCompletedRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RecordChildExecutionCompleted");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RecordChildExecutionCompleted", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        if request.workflow_execution.is_none() { return Err(fail(HistoryError::bad_request("WorkflowExecution not set on request."))); }
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("RecordChildExecutionCompleted", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.record_child_execution_completed(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn record_decision_task_started(&self, request: Option<&crate::engine::types::RecordDecisionTaskStartedRequest>) -> HResult<crate::engine::types::RecordDecisionTaskStartedResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RecordDecisionTaskStarted");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        self.emit_info_or_debug_log(domain, "RecordDecisionTaskStarted", &workflow, &run, request.schedule_id);
        let fail = |error| self.error("RecordDecisionTaskStarted", domain, &workflow, &run, error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        if request.poll_request.as_ref().and_then(|r| r.task_list.as_ref()).is_none_or(|t| t.name.is_empty()) { return Err(fail(HistoryError::bad_request("Tasklist not set."))); }
        let engine = self.resources.engine_for_workflow(&workflow).map_err(|error| {
            tracing::error!(error = %error, workflow_id = workflow, run_id = run, domain_name = domain,
                schedule_id = request.schedule_id, "RecordDecisionTaskStarted failed.");
            fail(error)
        })?;
        let result = engine.record_decision_task_started(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn refresh_workflow_tasks(&self, request: Option<&crate::engine::types::HistoryRefreshWorkflowTasksRequest>) -> HResult<()> {
        let _profile = self.metrics.start_request_profile("RefreshWorkflowTasks");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let execution = request.request.clone().and_then(|r| r.workflow_execution).unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("RefreshWorkflowTasks", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.refresh_workflow_tasks(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn remove_signal_mutable_state(&self, request: Option<&crate::engine::types::RemoveSignalMutableStateRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RemoveSignalMutableState");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RemoveSignalMutableState", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("RemoveSignalMutableState", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.remove_signal_mutable_state(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn replicate_events_v2(&self, request: Option<&crate::engine::types::ReplicateEventsV2Request>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("ReplicateEventsV2");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("ReplicateEventsV2", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("ReplicateEventsV2", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.replicate_events_v2(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn request_cancel_workflow_execution(&self, request: Option<&crate::engine::types::HistoryRequestCancelWorkflowExecutionRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RequestCancelWorkflowExecution");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RequestCancelWorkflowExecution", domain, "", "", error);
        if domain.is_empty() || request.cancel_request.as_ref().is_none_or(|r| r.domain.is_empty()) { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.cancel_request.clone().and_then(|r| r.workflow_execution).unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("RequestCancelWorkflowExecution", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.request_cancel_workflow_execution(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn reset_sticky_task_list(&self, request: Option<&crate::engine::types::HistoryResetStickyTaskListRequest>) -> HResult<crate::engine::types::HistoryResetStickyTaskListResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("ResetStickyTaskList");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("ResetStickyTaskList", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("ResetStickyTaskList", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.reset_sticky_task_list(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn reset_workflow_execution(&self, request: Option<&crate::engine::types::HistoryResetWorkflowExecutionRequest>) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("ResetWorkflowExecution");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("ResetWorkflowExecution", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.reset_request.clone().and_then(|r| r.workflow_execution).unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("ResetWorkflowExecution", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.reset_workflow_execution(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn respond_activity_task_canceled(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCanceledRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RespondActivityTaskCanceled");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RespondActivityTaskCanceled", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let token = self.token(&request.cancel_request.clone().unwrap_or_default().task_token).map_err(&fail)?;
        let workflow = token.workflow_id; let run = token.run_id;
        let fail = |error| self.error("RespondActivityTaskCanceled", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.respond_activity_task_canceled(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn respond_activity_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCompletedRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RespondActivityTaskCompleted");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RespondActivityTaskCompleted", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let token = self.token(&request.complete_request.clone().unwrap_or_default().task_token).map_err(&fail)?;
        let workflow = token.workflow_id; let run = token.run_id;
        let fail = |error| self.error("RespondActivityTaskCompleted", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.respond_activity_task_completed(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn respond_activity_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskFailedRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RespondActivityTaskFailed");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RespondActivityTaskFailed", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let token = self.token(&request.failed_request.clone().unwrap_or_default().task_token).map_err(&fail)?;
        let workflow = token.workflow_id; let run = token.run_id;
        let fail = |error| self.error("RespondActivityTaskFailed", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.respond_activity_task_failed(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn respond_decision_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskCompletedRequest>) -> HResult<crate::engine::types::HistoryRespondDecisionTaskCompletedResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RespondDecisionTaskCompleted");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RespondDecisionTaskCompleted", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        if request.complete_request.as_ref().is_none_or(|r| r.decisions.is_empty()) { self.metrics.empty_completion_decisions(); }
        let token = self.token(&request.complete_request.clone().unwrap_or_default().task_token).map_err(&fail)?;
        let workflow = token.workflow_id; let run = token.run_id;
        let fail = |error| self.error("RespondDecisionTaskCompleted", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.respond_decision_task_completed(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn respond_decision_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RespondDecisionTaskFailed");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("RespondDecisionTaskFailed", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let failed = request.failed_request.clone().unwrap_or_default();
        let token = self.deserialize_token(&failed.task_token).map_err(&fail)?;
        tracing::debug!(domain_id = token.domain_id, workflow_id = token.workflow_id, run_id = token.run_id,
            schedule_id = token.schedule_id, "RespondDecisionTaskFailed");
        if failed.cause.unwrap_or_default() == cadence_api::enums::DecisionTaskFailedCause::UnhandledDecision {
            tracing::info!(domain_id = token.domain_id, workflow_id = token.workflow_id, run_id = token.run_id, "Non-Deterministic Error");
            self.metrics.non_deterministic(&token.domain_id);
        }
        super::runtime::validate_task_token(&token).map_err(&fail)?;
        let workflow = token.workflow_id; let run = token.run_id;
        let fail = |error| self.error("RespondDecisionTaskFailed", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.respond_decision_task_failed(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn schedule_decision_task(&self, request: Option<&crate::engine::types::ScheduleDecisionTaskRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("ScheduleDecisionTask");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("ScheduleDecisionTask", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        if request.workflow_execution.is_none() { return Err(fail(HistoryError::bad_request("WorkflowExecution not set on request."))); }
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("ScheduleDecisionTask", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.schedule_decision_task(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn signal_with_start_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWithStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("SignalWithStartWorkflowExecution");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("SignalWithStartWorkflowExecution", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let workflow = request.signal_with_start_request.as_ref().map(|r| r.workflow_id.clone()).unwrap_or_default(); let run = String::new();
        let fail = |error| self.error("SignalWithStartWorkflowExecution", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.signal_with_start_workflow_execution(Some(&request)).await;
        let result = match result {
            Err(HistoryError::PersistenceAlreadyStarted(_) | HistoryError::CurrentWorkflowConditionFailed(_)) => engine.signal_with_start_workflow_execution(Some(&request)).await,
            result => result,
        };
        result.map_err(fail)
    }
    pub async fn signal_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWorkflowExecutionRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("SignalWorkflowExecution");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("SignalWorkflowExecution", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.signal_request.clone().and_then(|r| r.workflow_execution).unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("SignalWorkflowExecution", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.signal_workflow_execution(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn start_workflow_execution(&self, request: Option<&crate::engine::types::HistoryStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("StartWorkflowExecution");
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("StartWorkflowExecution", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let workflow = request.start_request.as_ref().map(|r| r.workflow_id.clone()).unwrap_or_default(); let run = String::new();
        let fail = |error| self.error("StartWorkflowExecution", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.start_workflow_execution(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn sync_activity(&self, request: Option<&crate::engine::types::SyncActivityRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("SyncActivity");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_id;
        let fail = |error| self.error("SyncActivity", domain, "", "", error);
        if domain.is_empty() || uuid::Uuid::parse_str(domain).is_err() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        if request.workflow_id.is_empty() { return Err(fail(HistoryError::bad_request("WorkflowId is not set on request."))); }
        if request.run_id.is_empty() || uuid::Uuid::parse_str(&request.run_id).is_err() { return Err(fail(HistoryError::bad_request("RunID is not valid UUID."))); }
        let workflow = request.workflow_id.clone(); let run = request.run_id.clone();
        let fail = |error| self.error("SyncActivity", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.sync_activity(Some(&request)).await;
        result.map_err(fail)
    }
    pub async fn terminate_workflow_execution(&self, request: Option<&crate::engine::types::HistoryTerminateWorkflowExecutionRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("TerminateWorkflowExecution");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let domain = &request.domain_uuid;
        let fail = |error| self.error("TerminateWorkflowExecution", domain, "", "", error);
        if domain.is_empty() { return Err(fail(HistoryError::bad_request("Domain not set on request."))); }
        self.check_rate().map_err(&fail)?;
        let execution = request.terminate_request.clone().and_then(|r| r.workflow_execution).unwrap_or_default();
        let workflow = execution.workflow_id; let run = execution.run_id;
        let fail = |error| self.error("TerminateWorkflowExecution", domain, &workflow, &run, error);
        let engine = self.resources.engine_for_workflow(&workflow).map_err(&fail)?;
        let result = engine.terminate_workflow_execution(Some(&request)).await;
        result.map_err(fail)
    }
}
