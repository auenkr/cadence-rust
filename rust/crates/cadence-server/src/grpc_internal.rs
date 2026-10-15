//! Native internal HistoryAPI adapters over the shared Rust execution state.
//! Unported shard/replication/branch and retry metadata remain explicit gaps.
use crate::grpc::Frontend;
use cadence_proto::{api as p, history as h};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct History {
    pub frontend: Frontend,
}
impl History {
    pub(crate) fn domain_name(&self, id: &str) -> Result<String, Status> {
        if uuid::Uuid::parse_str(id).is_err() {
            return Err(Status::invalid_argument("Domain UUID is not valid."));
        }
        self.frontend
            .state
            .domains
            .describe(&cadence_api::service_types::DescribeDomainRequest {
                name: None,
                uuid: Some(id.to_owned()),
            })
            .map_err(|e| Status::not_found(e.message))?
            .domain_info
            .map(|info| info.name)
            .ok_or_else(|| Status::internal("Domain info is missing."))
    }
    fn check_token(&self, id: &str, bytes: &[u8]) -> Result<(), Status> {
        self.domain_name(id)?;
        let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(bytes)
            .map_err(|_| Status::invalid_argument("Invalid TaskToken."))?;
        if token.domain_id != id {
            return Err(Status::invalid_argument(
                "TaskToken domain does not match request domain.",
            ));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl h::history_api_server::HistoryApi for History {
    async fn reset_sticky_task_list(
        &self,
        request: Request<h::ResetStickyTaskListRequest>,
    ) -> Result<Response<h::ResetStickyTaskListResponse>, Status> {
        let request = request.into_inner();
        let domain = self.domain_name(&request.domain_id)?;
        let mut payload = request
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = domain;
        p::worker_api_server::WorkerApi::reset_sticky_task_list(
            &self.frontend,
            Request::new(payload),
        )
        .await?;
        Ok(Response::new(h::ResetStickyTaskListResponse {}))
    }
    async fn start_workflow_execution(
        &self,
        request: Request<h::StartWorkflowExecutionRequest>,
    ) -> Result<Response<h::StartWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if r.parent_execution_info.is_some()
            || r.attempt != 0
            || r.expiration_time.is_some()
            || r.continue_as_new_initiator != 0
            || r.continued_failure.is_some()
            || r.last_completion_result.is_some()
            || r.first_decision_task_backoff.is_some()
            || !r.partition_config.is_empty()
        {
            return Err(Status::unimplemented(
                "Internal start retry/parent/partition metadata is not ported.",
            ));
        }
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let v = p::workflow_api_server::WorkflowApi::start_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::StartWorkflowExecutionResponse {
            run_id: v.run_id,
        }))
    }
    async fn signal_workflow_execution(
        &self,
        request: Request<h::SignalWorkflowExecutionRequest>,
    ) -> Result<Response<h::SignalWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if r.child_workflow_only || r.external_workflow_execution.is_some() {
            return Err(Status::unimplemented(
                "External child signal validation is not ported.",
            ));
        }
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let _v = p::workflow_api_server::WorkflowApi::signal_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::SignalWorkflowExecutionResponse {}))
    }
    async fn signal_with_start_workflow_execution(
        &self,
        request: Request<h::SignalWithStartWorkflowExecutionRequest>,
    ) -> Result<Response<h::SignalWithStartWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if !r.partition_config.is_empty() {
            return Err(Status::unimplemented(
                "Partition configuration is not ported.",
            ));
        }
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload
            .start_request
            .as_mut()
            .ok_or_else(|| Status::invalid_argument("Start request is not set."))?
            .domain = self.domain_name(&r.domain_id)?;
        let v = p::workflow_api_server::WorkflowApi::signal_with_start_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::SignalWithStartWorkflowExecutionResponse {
            run_id: v.run_id,
        }))
    }
    async fn terminate_workflow_execution(
        &self,
        request: Request<h::TerminateWorkflowExecutionRequest>,
    ) -> Result<Response<h::TerminateWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if r.child_workflow_only || r.external_workflow_execution.is_some() {
            return Err(Status::unimplemented(
                "External child termination validation is not ported.",
            ));
        }
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let _v = p::workflow_api_server::WorkflowApi::terminate_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::TerminateWorkflowExecutionResponse {}))
    }
    async fn reset_workflow_execution(
        &self,
        request: Request<h::ResetWorkflowExecutionRequest>,
    ) -> Result<Response<h::ResetWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let v = p::workflow_api_server::WorkflowApi::reset_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::ResetWorkflowExecutionResponse {
            run_id: v.run_id,
        }))
    }
    async fn refresh_workflow_tasks(
        &self,
        request: Request<h::RefreshWorkflowTasksRequest>,
    ) -> Result<Response<h::RefreshWorkflowTasksResponse>, Status> {
        use crate::grpc_convert as cv;
        let r = request.into_inner();
        let execution = cv::execution(r.workflow_execution.unwrap_or_default());
        let domain = if r.domain.is_empty() {
            self.domain_name(&r.domain_id)?
        } else {
            r.domain.clone()
        };
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| Status::not_found(e.message))?;
        match store.refresh_tasks(&domain, &execution) {
            Ok(()) => Ok(Response::new(h::RefreshWorkflowTasksResponse {})),
            Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => {
                Err(Status::not_found(e.message))
            }
            Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }
    async fn request_cancel_workflow_execution(
        &self,
        request: Request<h::RequestCancelWorkflowExecutionRequest>,
    ) -> Result<Response<h::RequestCancelWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if r.child_workflow_only || r.external_execution_info.is_some() {
            return Err(Status::unimplemented(
                "External child cancellation validation is not ported.",
            ));
        }
        let mut payload = r
            .cancel_request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let _v = p::workflow_api_server::WorkflowApi::request_cancel_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::RequestCancelWorkflowExecutionResponse {}))
    }
    async fn describe_workflow_execution(
        &self,
        request: Request<h::DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<h::DescribeWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();

        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let v = p::workflow_api_server::WorkflowApi::describe_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::DescribeWorkflowExecutionResponse {
            execution_configuration: v.execution_configuration,
            workflow_execution_info: v.workflow_execution_info,
            pending_activities: v.pending_activities,
            pending_children: v.pending_children,
            pending_decision: v.pending_decision,
        }))
    }
    async fn query_workflow(
        &self,
        request: Request<h::QueryWorkflowRequest>,
    ) -> Result<Response<h::QueryWorkflowResponse>, Status> {
        let r = request.into_inner();

        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let v = p::workflow_api_server::WorkflowApi::query_workflow(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::QueryWorkflowResponse {
            query_result: v.query_result,
            query_rejected: v.query_rejected,
        }))
    }
    async fn respond_decision_task_completed(
        &self,
        request: Request<h::RespondDecisionTaskCompletedRequest>,
    ) -> Result<Response<h::RespondDecisionTaskCompletedResponse>, Status> {
        let r = request.into_inner();

        let payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        self.check_token(&r.domain_id, &payload.task_token)?;
        if payload.return_new_decision_task {
            return Err(Status::unimplemented(
                "Internal decision heartbeat response is not ported.",
            ));
        }
        let v = p::worker_api_server::WorkerApi::respond_decision_task_completed(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::RespondDecisionTaskCompletedResponse {
            started_response: None,
            activities_to_dispatch_locally: v.activities_to_dispatch_locally,
        }))
    }
    async fn respond_decision_task_failed(
        &self,
        request: Request<h::RespondDecisionTaskFailedRequest>,
    ) -> Result<Response<h::RespondDecisionTaskFailedResponse>, Status> {
        let r = request.into_inner();

        let payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        self.check_token(&r.domain_id, &payload.task_token)?;
        let _v = p::worker_api_server::WorkerApi::respond_decision_task_failed(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::RespondDecisionTaskFailedResponse {}))
    }
    async fn respond_activity_task_completed(
        &self,
        request: Request<h::RespondActivityTaskCompletedRequest>,
    ) -> Result<Response<h::RespondActivityTaskCompletedResponse>, Status> {
        let r = request.into_inner();

        let payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        self.check_token(&r.domain_id, &payload.task_token)?;
        let _v = p::worker_api_server::WorkerApi::respond_activity_task_completed(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::RespondActivityTaskCompletedResponse {}))
    }
    async fn respond_activity_task_failed(
        &self,
        request: Request<h::RespondActivityTaskFailedRequest>,
    ) -> Result<Response<h::RespondActivityTaskFailedResponse>, Status> {
        let r = request.into_inner();

        let payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        self.check_token(&r.domain_id, &payload.task_token)?;
        let _v = p::worker_api_server::WorkerApi::respond_activity_task_failed(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::RespondActivityTaskFailedResponse {}))
    }
    async fn respond_activity_task_canceled(
        &self,
        request: Request<h::RespondActivityTaskCanceledRequest>,
    ) -> Result<Response<h::RespondActivityTaskCanceledResponse>, Status> {
        let r = request.into_inner();

        let payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        self.check_token(&r.domain_id, &payload.task_token)?;
        let _v = p::worker_api_server::WorkerApi::respond_activity_task_canceled(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::RespondActivityTaskCanceledResponse {}))
    }
    async fn record_activity_task_heartbeat(
        &self,
        request: Request<h::RecordActivityTaskHeartbeatRequest>,
    ) -> Result<Response<h::RecordActivityTaskHeartbeatResponse>, Status> {
        let r = request.into_inner();

        let payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        self.check_token(&r.domain_id, &payload.task_token)?;
        let v = p::worker_api_server::WorkerApi::record_activity_task_heartbeat(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        Ok(Response::new(h::RecordActivityTaskHeartbeatResponse {
            cancel_requested: v.cancel_requested,
        }))
    }
}

pub async fn serve(
    addr: std::net::SocketAddr,
    state: crate::handlers::AppState,
) -> anyhow::Result<()> {
    let limit = state.grpc_max_message_size;
    let service = History {
        frontend: Frontend { state },
    };
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(cadence_proto::FILE_DESCRIPTOR_SET)
        .with_service_name("uber.cadence.history.v1.HistoryAPI")
        .build_v1()?;
    tonic::transport::Server::builder()
        .add_service(reflection)
        .add_service(
            h::history_api_server::HistoryApiServer::new(service).max_decoding_message_size(limit),
        )
        .serve(addr)
        .await?;
    Ok(())
}
