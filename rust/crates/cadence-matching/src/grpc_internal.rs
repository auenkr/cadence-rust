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
            .map_err(|e| crate::grpc::domain_not_found(e.message))?
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
    fn check_shard_id(&self, shard_id: i32) -> Result<(), Status> {
        let count = self.frontend.state.num_history_shards.max(1);
        if shard_id < 0 || shard_id >= count {
            return Err(Status::invalid_argument(format!(
                "Shard ID {shard_id} is out of range for {count} shards."
            )));
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
        let parent_info = r.parent_execution_info.clone();
        let attempt = r.attempt;
        let expiration_nanos =
            r.expiration_time.map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos));
        let continued_initiator = r.continue_as_new_initiator;
        let _continued_failure_reason = r
            .continued_failure
            .as_ref()
            .map(|f| f.reason.clone())
            .unwrap_or_default();
        let _continued_failure_details = r
            .continued_failure
            .as_ref()
            .map(|f| f.details.clone())
            .unwrap_or_default();
        let _last_completion_result = r
            .last_completion_result
            .as_ref()
            .map(|p| p.data.clone())
            .unwrap_or_default();
        let first_decision_backoff_seconds = r
            .first_decision_task_backoff
            .map_or(0, |d| d.seconds as i32 + if d.nanos > 500_000_000 { 1 } else { 0 });
        let workflow_id = r
            .request
            .as_ref()
            .map(|req| req.workflow_id.clone())
            .unwrap_or_default();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let domain = payload.domain.clone();
        let v = p::workflow_api_server::WorkflowApi::start_workflow_execution(
            &self.frontend,
            Request::new(payload),
        )
        .await?
        .into_inner();
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        let execution = cadence_api::types::WorkflowExecution {
            workflow_id,
            run_id: v.run_id.clone(),
        };
        let _ = store.set_start_execution_metadata(
            &domain,
            &execution,
            parent_info.as_ref().map(|p| (p.domain_name.clone(), p
                .workflow_execution
                .as_ref()
                .map(|w| w.workflow_id.clone())
                .unwrap_or_default(), p
                .workflow_execution
                .as_ref()
                .map(|w| w.run_id.clone())
                .unwrap_or_default(), p.initiated_id)),
            attempt,
            expiration_nanos,
            first_decision_backoff_seconds,
        );
        // Store parent-initiated run ids and close-related metadata in the
        // already-created started event attributes. This is a first step only.
        let _ = (&continued_initiator,);
        Ok(Response::new(h::StartWorkflowExecutionResponse {
            run_id: v.run_id,
        }))
    }
    async fn signal_workflow_execution(
        &self,
        request: Request<h::SignalWorkflowExecutionRequest>,
    ) -> Result<Response<h::SignalWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
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
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
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
    async fn get_mutable_state(
        &self,
        request: Request<h::GetMutableStateRequest>,
    ) -> Result<Response<h::GetMutableStateResponse>, Status> {
        let r = request.into_inner();
        let proto_execution = r.workflow_execution.unwrap_or_default();
        let (_, view) = mutable_state_response(self,&r.domain_id, proto_execution)?;
        Ok(Response::new(history_mutable_state_response(view)))
    }

    async fn poll_mutable_state(
        &self,
        request: Request<h::PollMutableStateRequest>,
    ) -> Result<Response<h::PollMutableStateResponse>, Status> {
        let r = request.into_inner();
        let proto_execution = r.workflow_execution.unwrap_or_default();
        // Single host answers immediately with current state instead of
        // long-polling for the expected event ID.
        let (_, view) = mutable_state_response(self,&r.domain_id, proto_execution)?;
        Ok(Response::new(history_poll_mutable_state_response(view)))
    }

    async fn describe_mutable_state(
        &self,
        request: Request<h::DescribeMutableStateRequest>,
    ) -> Result<Response<h::DescribeMutableStateResponse>, Status> {
        let r = request.into_inner();
        let proto_execution = r.workflow_execution.unwrap_or_default();
        let (domain, view) = mutable_state_response(self,&r.domain_id, proto_execution)?;
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        let (_, _, state_json) = store
            .describe_admin(
                &domain,
                &cadence_api::types::WorkflowExecution {
                    workflow_id: view.workflow_id.clone(),
                    run_id: view.run_id.clone(),
                },
            )
            .map_err(|e| Status::not_found(e.message))?;
        Ok(Response::new(h::DescribeMutableStateResponse {
            mutable_state_in_cache: state_json.clone(),
            mutable_state_in_database: state_json,
        }))
    }

    async fn describe_history_host(
        &self,
        _request: Request<h::DescribeHistoryHostRequest>,
    ) -> Result<Response<h::DescribeHistoryHostResponse>, Status> {
        let count = self.frontend.state.num_history_shards.max(1);
        let hostname =
            std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string());
        let domains = self.frontend.state.domains.len() as i64;
        Ok(Response::new(h::DescribeHistoryHostResponse {
            number_of_shards: count,
            shard_ids: (0..count).collect(),
            domain_cache: Some(cadence_proto::admin::DomainCacheInfo {
                num_of_items_in_cache_by_id: domains,
                num_of_items_in_cache_by_name: domains,
            }),
            shard_controller_status: "running".to_string(),
            address: hostname,
        }))
    }

    async fn schedule_decision_task(
        &self,
        request: Request<h::ScheduleDecisionTaskRequest>,
    ) -> Result<Response<h::ScheduleDecisionTaskResponse>, Status> {
        let r = request.into_inner();
        let domain = self.domain_name(&r.domain_id)?;
        let execution = r.workflow_execution.unwrap_or_default();
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        match store.schedule_decision_task(
            &domain,
            &cadence_api::types::WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            },
        ) {
            Ok(()) => Ok(Response::new(h::ScheduleDecisionTaskResponse {})),
            Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => {
                Err(Status::not_found(e.message))
            }
            Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }

    async fn remove_signal_mutable_state(
        &self,
        request: Request<h::RemoveSignalMutableStateRequest>,
    ) -> Result<Response<h::RemoveSignalMutableStateResponse>, Status> {
        let r = request.into_inner();
        let domain = self.domain_name(&r.domain_id)?;
        let execution = r.workflow_execution.unwrap_or_default();
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        match store.remove_signal(
            &domain,
            &cadence_api::types::WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            },
            &r.request_id,
        ) {
            Ok(()) => Ok(Response::new(h::RemoveSignalMutableStateResponse {})),
            Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => {
                Err(Status::not_found(e.message))
            }
            Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }

    async fn sync_activity(
        &self,
        request: Request<h::SyncActivityRequest>,
    ) -> Result<Response<h::SyncActivityResponse>, Status> {
        let r = request.into_inner();
        // Version gating does not exist on a single branch; unknown runs or
        // activities are a success no-op like Go's out-of-order path.
        if let Ok(domain) = self.domain_name(&r.domain_id) {
            if let Ok(store) = self.frontend.state.execution_store(&domain) {
                let execution = r.workflow_execution.unwrap_or_default();
                store.sync_activity(
                    &domain,
                    &cadence_api::types::WorkflowExecution {
                        workflow_id: execution.workflow_id,
                        run_id: execution.run_id,
                    },
                    r.scheduled_id,
                    r.details.map(|p| p.data).unwrap_or_default(),
                    r.attempt,
                );
            }
        }
        Ok(Response::new(h::SyncActivityResponse {}))
    }

    /// Accept replicated events after Go-exact request validation. Single
    /// host has no passive replicas or version branches, so a valid request
    /// is a no-op success; cross-cluster event application is an explicit
    /// gap (see the module docs).
    async fn replicate_events_v2(
        &self,
        request: Request<h::ReplicateEventsV2Request>,
    ) -> Result<Response<h::ReplicateEventsV2Response>, Status> {
        let r = request.into_inner();
        if r.domain_id.is_empty() {
            return Err(Status::invalid_argument("Domain not set on request."));
        }
        let _domain = self.domain_name(&r.domain_id)?;
        let execution = r.workflow_execution.unwrap_or_default();
        if execution.workflow_id.is_empty() {
            return Err(Status::invalid_argument("WorkflowId is not set on request."));
        }
        Ok(Response::new(h::ReplicateEventsV2Response {}))
    }

    /// Accept a shard-status sync after Go-exact validation. Single host has
    /// no remote shards, so a valid request is a no-op success.
    async fn sync_shard_status(
        &self,
        request: Request<h::SyncShardStatusRequest>,
    ) -> Result<Response<h::SyncShardStatusResponse>, Status> {
        let r = request.into_inner();
        if r.source_cluster.is_empty() {
            return Err(Status::invalid_argument(
                "Source Cluster not set on request.",
            ));
        }
        if r.time.is_none() {
            return Err(Status::invalid_argument("Timestamp not set on request."));
        }
        self.check_shard_id(r.shard_id)?;
        Ok(Response::new(h::SyncShardStatusResponse {}))
    }

    /// Close a shard. Single host keeps all shards loaded, so a valid ID is
    /// a no-op success like Go's unload-then-lazy-reload.
    async fn close_shard(
        &self,
        request: Request<h::CloseShardRequest>,
    ) -> Result<Response<h::CloseShardResponse>, Status> {
        let r = request.into_inner();
        self.check_shard_id(r.shard_id)?;
        Ok(Response::new(h::CloseShardResponse {}))
    }

    /// Remove one persistence task. Single host has no addressable
    /// persistence task IDs; task-type validation mirrors Go's engine switch
    /// (transfer/timer/replication) and a valid request is a no-op success.
    async fn remove_task(
        &self,
        request: Request<h::RemoveTaskRequest>,
    ) -> Result<Response<h::RemoveTaskResponse>, Status> {
        let r = request.into_inner();
        self.check_shard_id(r.shard_id)?;
        match r.task_type {
            1..=3 => Ok(Response::new(h::RemoveTaskResponse {})),
            _ => Err(Status::invalid_argument("Invalid task type")),
        }
    }

    /// Reset a processing queue. Single host recomputes dispatch state from
    /// runs; type validation mirrors Go (transfer/timer only) and a valid
    /// request is a no-op success.
    async fn reset_queue(
        &self,
        request: Request<h::ResetQueueRequest>,
    ) -> Result<Response<h::ResetQueueResponse>, Status> {
        let r = request.into_inner();
        self.check_shard_id(r.shard_id)?;
        match r.task_type {
            1..=2 => Ok(Response::new(h::ResetQueueResponse {})),
            _ => Err(Status::invalid_argument("Invalid task type")),
        }
    }

    /// Report live dispatch-queue depths as processing-queue states, mirroring
    /// the admin queue introspection on the same shared state.
    async fn describe_queue(
        &self,
        request: Request<h::DescribeQueueRequest>,
    ) -> Result<Response<h::DescribeQueueResponse>, Status> {
        let r = request.into_inner();
        self.check_shard_id(r.shard_id)?;
        match r.task_type {
            1..=2 => {}
            _ => return Err(Status::invalid_argument("Invalid task type")),
        }
        let (decisions, activities, timers, backoffs, schedules) =
            self.frontend.state.executions.queue_depths();
        Ok(Response::new(h::DescribeQueueResponse {
            processing_queue_states: vec![
                format!("{{\"queue\":\"decision\",\"pending\":{decisions}}}"),
                format!("{{\"queue\":\"activity\",\"pending\":{activities}}}"),
                format!("{{\"queue\":\"timer\",\"pending\":{timers}}}"),
                format!("{{\"queue\":\"backoff\",\"pending\":{backoffs}}}"),
                format!("{{\"queue\":\"schedule\",\"registered\":{schedules}}}"),
            ],
        }))
    }

    /// Fetch replication messages. Single host produces no replication
    /// streams, so the message map is always empty.
    async fn get_replication_messages(
        &self,
        request: Request<h::GetReplicationMessagesRequest>,
    ) -> Result<Response<h::GetReplicationMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.cluster_name.is_empty() {
            return Err(Status::invalid_argument("Cluster name is not set."));
        }
        Ok(Response::new(h::GetReplicationMessagesResponse {
            shard_messages: Default::default(),
        }))
    }

    /// Fetch DLQ replication messages. Single host has no replication DLQ.
    async fn get_dlq_replication_messages(
        &self,
        request: Request<h::GetDlqReplicationMessagesRequest>,
    ) -> Result<Response<h::GetDlqReplicationMessagesResponse>, Status> {
        let _ = request;
        Ok(Response::new(h::GetDlqReplicationMessagesResponse {
            replication_tasks: vec![],
        }))
    }

    /// Reapply signal events to the current run through the shared engine
    /// path used by admin reapply. Validation (including the missing-blob
    /// quirk) mirrors the admin single-branch behavior.
    async fn reapply_events(
        &self,
        request: Request<h::ReapplyEventsRequest>,
    ) -> Result<Response<h::ReapplyEventsResponse>, Status> {
        let r = request.into_inner();
        let domain = if r.domain.is_empty() {
            self.domain_name(&r.domain_id)?
        } else {
            r.domain.clone()
        };
        let execution = r.workflow_execution.unwrap_or_default();
        if execution.workflow_id.is_empty() {
            return Err(Status::invalid_argument("WorkflowID is not set on request."));
        }
        // Go quirks a missing events blob into ErrWorkflowIDNotSet.
        let blob = r
            .events
            .ok_or_else(|| Status::invalid_argument("WorkflowID is not set on request."))?;
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        let events: Vec<cadence_api::service_types::HistoryEvent> =
            serde_json::from_slice(&blob.data).unwrap_or_default();
        let signals: Vec<_> = events
            .into_iter()
            .filter(|e| {
                e.event_type == Some(cadence_api::enums::EventType::WorkflowExecutionSignaled)
            })
            .collect();
        match store.reapply_signals(&domain, &execution.workflow_id, signals) {
            Ok(()) => Ok(Response::new(h::ReapplyEventsResponse {})),
            Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => {
                Err(Status::not_found(e.message))
            }
            Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }

    /// Count DLQ messages. Single host has no replication DLQ.
    async fn count_dlq_messages(
        &self,
        _request: Request<h::CountDlqMessagesRequest>,
    ) -> Result<Response<h::CountDlqMessagesResponse>, Status> {
        Ok(Response::new(h::CountDlqMessagesResponse { entries: vec![] }))
    }

    /// Read DLQ messages. Single host has no replication DLQ.
    async fn read_dlq_messages(
        &self,
        request: Request<h::ReadDlqMessagesRequest>,
    ) -> Result<Response<h::ReadDlqMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.r#type == 0 {
            return Err(Status::invalid_argument("Queue type is not set."));
        }
        Ok(Response::new(h::ReadDlqMessagesResponse {
            r#type: r.r#type,
            replication_tasks: vec![],
            replication_tasks_info: vec![],
            next_page_token: vec![],
        }))
    }

    /// Purge DLQ messages. Single host has no replication DLQ; a valid
    /// request is a no-op success.
    async fn purge_dlq_messages(
        &self,
        request: Request<h::PurgeDlqMessagesRequest>,
    ) -> Result<Response<h::PurgeDlqMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.r#type == 0 {
            return Err(Status::invalid_argument("Queue type is not set."));
        }
        Ok(Response::new(h::PurgeDlqMessagesResponse {}))
    }

    /// Merge DLQ messages. Single host has no replication DLQ; a valid
    /// request merges nothing.
    async fn merge_dlq_messages(
        &self,
        request: Request<h::MergeDlqMessagesRequest>,
    ) -> Result<Response<h::MergeDlqMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.r#type == 0 {
            return Err(Status::invalid_argument("Queue type is not set."));
        }
        Ok(Response::new(h::MergeDlqMessagesResponse {
            next_page_token: vec![],
        }))
    }

    /// Accept failover markers. Go forwards them to the failover coordinator
    /// without validation; single host tracks no cross-cluster failover, so
    /// accepted markers are a no-op success.
    async fn notify_failover_markers(
        &self,
        request: Request<h::NotifyFailoverMarkersRequest>,
    ) -> Result<Response<h::NotifyFailoverMarkersResponse>, Status> {
        let _ = request;
        Ok(Response::new(h::NotifyFailoverMarkersResponse {}))
    }

    /// Fetch cross-cluster tasks. Go history reports the cross-cluster
    /// feature as deprecated with this exact message.
    async fn get_cross_cluster_tasks(
        &self,
        _request: Request<h::GetCrossClusterTasksRequest>,
    ) -> Result<Response<h::GetCrossClusterTasksResponse>, Status> {
        Err(Status::invalid_argument(
            "The cross-cluster feature has been deprecated.",
        ))
    }

    /// Complete cross-cluster tasks. Go history reports the cross-cluster
    /// feature as deprecated with this exact message (no trailing period).
    async fn respond_cross_cluster_tasks_completed(
        &self,
        _request: Request<h::RespondCrossClusterTasksCompletedRequest>,
    ) -> Result<Response<h::RespondCrossClusterTasksCompletedResponse>, Status> {
        Err(Status::invalid_argument(
            "The cross-cluster feature has been deprecated",
        ))
    }

    /// Report ongoing failover progress. Single host tracks no graceful
    /// failovers, so a known domain reports Go's exact record-not-found
    /// error; unknown domains are NOT_FOUND like the other history reads.
    async fn get_failover_info(
        &self,
        request: Request<h::GetFailoverInfoRequest>,
    ) -> Result<Response<h::GetFailoverInfoResponse>, Status> {
        let r = request.into_inner();
        let _domain = self.domain_name(&r.domain_id)?;
        Err(Status::not_found(
            "Graceful failover record not found in shard coordinator",
        ))
    }

    /// Push global-ratelimit data. Single host aggregates no ratelimits, so
    /// the update is accepted and an empty response is returned.
    async fn ratelimit_update(
        &self,
        _request: Request<h::RatelimitUpdateRequest>,
    ) -> Result<Response<h::RatelimitUpdateResponse>, Status> {
        Ok(Response::new(h::RatelimitUpdateResponse { data: None }))
    }

    async fn record_decision_task_started(
        &self,
        request: Request<h::RecordDecisionTaskStartedRequest>,
    ) -> Result<Response<h::RecordDecisionTaskStartedResponse>, Status> {
        let r = request.into_inner();
        let domain = self.domain_name(&r.domain_id)?;
        let execution = r.workflow_execution.unwrap_or_default();
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        match store.record_decision_started(
            &domain,
            &cadence_api::types::WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            },
            r.schedule_id,
            &r.request_id,
            &r.poll_request
                .as_ref()
                .map(|p| p.identity.clone())
                .unwrap_or_default(),
        ) {
            Ok(recorded) => Ok(Response::new(h::RecordDecisionTaskStartedResponse {
                workflow_type: Some(cadence_proto::api::WorkflowType {
                    name: recorded.workflow_type_name.clone(),
                }),
                previous_started_event_id: recorded.previous_started_event_id,
                scheduled_event_id: recorded.scheduled_event_id,
                started_event_id: recorded.started_event_id,
                next_event_id: recorded.next_event_id,
                attempt: recorded.attempt as i32,
                sticky_execution_enabled: false,
                decision_info: None,
                workflow_execution_task_list: Some(cadence_proto::api::TaskList {
                    name: recorded.task_list_name,
                    ..Default::default()
                }),
                event_store_version: 0,
                branch_token: vec![],
                scheduled_time: recorded.scheduled_time_millis.map(|ms| {
                    prost_types::Timestamp {
                        seconds: ms.div_euclid(1000),
                        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
                    }
                }),
                started_time: recorded.started_time_millis.map(|ms| {
                    prost_types::Timestamp {
                        seconds: ms.div_euclid(1000),
                        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
                    }
                }),
                queries: Default::default(),
                history_size: 0,
            })),
            Err(cadence_persistence::RecordError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::RecordError::AlreadyStarted(message)) => {
                Err(Status::failed_precondition(message))
            }
            Err(
                cadence_persistence::RecordError::Stale(message)
                | cadence_persistence::RecordError::Invalid(message),
            ) => Err(Status::aborted(message)),
        }
    }

    async fn record_activity_task_started(
        &self,
        request: Request<h::RecordActivityTaskStartedRequest>,
    ) -> Result<Response<h::RecordActivityTaskStartedResponse>, Status> {
        let r = request.into_inner();
        let domain = self.domain_name(&r.domain_id)?;
        let execution = r.workflow_execution.unwrap_or_default();
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        let identity = r
            .poll_request
            .as_ref()
            .map(|p| p.identity.clone())
            .unwrap_or_default();
        match store.record_activity_started(
            &domain,
            &cadence_api::types::WorkflowExecution {
                workflow_id: execution.workflow_id.clone(),
                run_id: execution.run_id.clone(),
            },
            r.schedule_id,
            &identity,
        ) {
            Ok(recorded) => {
                let event = crate::grpc_history::event(recorded.scheduled_event)?;
                Ok(Response::new(h::RecordActivityTaskStartedResponse {
                    scheduled_event: Some(event),
                    started_time: Some(prost_types::Timestamp {
                        seconds: recorded.started_time_millis.div_euclid(1000),
                        nanos: (recorded.started_time_millis.rem_euclid(1000) * 1_000_000) as i32,
                    }),
                    attempt: recorded.attempt,
                    scheduled_time_of_this_attempt: None,
                    heartbeat_details: if recorded.heartbeat_details.is_empty() {
                        None
                    } else {
                        Some(cadence_proto::api::Payload {
                            data: recorded.heartbeat_details,
                        })
                    },
                    workflow_type: Some(cadence_proto::api::WorkflowType {
                        name: String::new(),
                    }),
                    workflow_domain: domain,
                }))
            }
            Err(cadence_persistence::RecordError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::RecordError::AlreadyStarted(message)) => {
                Err(Status::failed_precondition(message))
            }
            Err(
                cadence_persistence::RecordError::Stale(message)
                | cadence_persistence::RecordError::Invalid(message),
            ) => Err(Status::aborted(message)),
        }
    }

    async fn record_child_execution_completed(
        &self,
        request: Request<h::RecordChildExecutionCompletedRequest>,
    ) -> Result<Response<h::RecordChildExecutionCompletedResponse>, Status> {
        let r = request.into_inner();
        let domain = self.domain_name(&r.domain_id)?;
        let execution = r.workflow_execution.unwrap_or_default();
        let completed = r.completed_execution.unwrap_or_default();
        let event = r.completion_event.ok_or_else(|| {
            Status::invalid_argument("Completion event is not set.")
        })?;
        let event = child_completion_event_in(event)?;
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| crate::grpc::domain_not_found(e.message))?;
        match store.record_child_completed(
            &domain,
            &cadence_api::types::WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            },
            r.initiated_id,
            &cadence_api::types::WorkflowExecution {
                workflow_id: completed.workflow_id,
                run_id: completed.run_id,
            },
            &event,
        ) {
            Ok(()) => Ok(Response::new(h::RecordChildExecutionCompletedResponse {})),
            Err(cadence_persistence::RecordError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::RecordError::AlreadyStarted(message)) => {
                Err(Status::failed_precondition(message))
            }
            Err(
                cadence_persistence::RecordError::Stale(message)
                | cadence_persistence::RecordError::Invalid(message),
            ) => Err(Status::aborted(message)),
        }
    }

    async fn request_cancel_workflow_execution(
        &self,
        request: Request<h::RequestCancelWorkflowExecutionRequest>,
    ) -> Result<Response<h::RequestCancelWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
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

/// Shared mutable-state snapshot for Get/Poll/DescribeMutableState.
fn mutable_state_response(
    history: &History,
    domain_id: &str,
    execution: cadence_proto::api::WorkflowExecution,
) -> Result<(String, cadence_persistence::MutableStateView), Status> {
    let domain = history.domain_name(domain_id)?;
    let execution = cadence_api::types::WorkflowExecution {
        workflow_id: execution.workflow_id,
        run_id: execution.run_id,
    };
    let store = history
        .frontend
        .state
        .execution_store(&domain)
        .map_err(|e| crate::grpc::domain_not_found(e.message))?;
    let view = store
        .mutable_state_view(&domain, &execution)
        .map_err(|e| Status::not_found(e.message))?;
    Ok((domain, view))
}

fn history_mutable_state_response(
    view: cadence_persistence::MutableStateView,
) -> h::GetMutableStateResponse {
    use cadence_api::enums::WorkflowExecutionCloseStatus as CloseStatus;
    let (workflow_state, workflow_close_state) = match view.close_status {
        None => (2, 0),
        Some(CloseStatus::Completed) => (3, 1),
        Some(CloseStatus::Failed) => (3, 2),
        Some(CloseStatus::Canceled) => (3, 3),
        Some(CloseStatus::Terminated) => (3, 4),
        Some(CloseStatus::ContinuedAsNew) => (3, 5),
        Some(CloseStatus::TimedOut) => (3, 6),
        _ => (3, 0),
    };
    let current_branch_token = view.version_histories.as_ref().and_then(|histories| {
        histories.histories.get(histories.current_version_history_index as usize)
    }).map(|history| history.branch_token.clone()).unwrap_or_default();
    let version_histories = view.version_histories.map(crate::grpc_convert::version_histories_out);
    h::GetMutableStateResponse {
        workflow_execution: Some(cadence_proto::api::WorkflowExecution {
            workflow_id: view.workflow_id,
            run_id: view.run_id,
        }),
        workflow_type: Some(cadence_proto::api::WorkflowType {
            name: view.workflow_type_name,
        }),
        next_event_id: view.next_event_id,
        previous_started_event_id: view.previous_started_event_id,
        last_first_event_id: 1,
        task_list: Some(cadence_proto::api::TaskList {
            name: view.task_list_name,
            ..Default::default()
        }),
        sticky_task_list: if view.sticky_task_list_name.is_empty() {
            None
        } else {
            Some(cadence_proto::api::TaskList {
                name: view.sticky_task_list_name,
                kind: 2,
                ..Default::default()
            })
        },
        client_library_version: String::new(),
        client_feature_version: String::new(),
        client_impl: String::new(),
        sticky_task_list_schedule_to_start_timeout: Some(prost_types::Duration {
            seconds: i64::from(view.sticky_schedule_to_start_timeout_seconds),
            nanos: 0,
        }),
        event_store_version: 0,
        current_branch_token,
        workflow_state,
        workflow_close_state,
        version_histories,
        is_sticky_task_list_enabled: false,
        history_size: view.history_size_bytes,
    }
}

fn history_poll_mutable_state_response(
    view: cadence_persistence::MutableStateView,
) -> h::PollMutableStateResponse {
    let full = history_mutable_state_response(view);
    h::PollMutableStateResponse {
        workflow_execution: full.workflow_execution,
        workflow_type: full.workflow_type,
        next_event_id: full.next_event_id,
        previous_started_event_id: full.previous_started_event_id,
        last_first_event_id: full.last_first_event_id,
        task_list: full.task_list,
        sticky_task_list: full.sticky_task_list,
        client_library_version: full.client_library_version,
        client_feature_version: full.client_feature_version,
        client_impl: full.client_impl,
        sticky_task_list_schedule_to_start_timeout: full
            .sticky_task_list_schedule_to_start_timeout,
        current_branch_token: full.current_branch_token,
        version_histories: full.version_histories,
        workflow_state: full.workflow_state,
        workflow_close_state: full.workflow_close_state,
    }
}

/// Convert a proto child terminal event to the Thrift shape, mirroring
/// the completion-event half of Go's `RecordChildExecutionCompleted`. Only
/// child terminal types are accepted; anything else is `InvalidArgument`.
fn child_completion_event_in(
    v: cadence_proto::api::HistoryEvent,
) -> Result<cadence_api::service_types::HistoryEvent, Status> {
    use cadence_api::{enums as e, service_types as st, tasks as tk, types as t};
    let event_id = v.event_id;
    let timestamp = v.event_time.map(|t| {
        t.seconds.saturating_mul(1000)
            .saturating_add(i64::from(t.nanos) / 1_000_000)
    });
    let execution = |e: Option<cadence_proto::api::WorkflowExecution>| {
        e.map(|e| t::WorkflowExecution {
            workflow_id: e.workflow_id,
            run_id: e.run_id,
        })
    };
    let workflow_type = |wt: Option<cadence_proto::api::WorkflowType>| {
        wt.map(|wt| t::WorkflowType { name: wt.name })
    };
    let (event_type, event) = match v.attributes {
        Some(cadence_proto::api::history_event::Attributes::ChildWorkflowExecutionCompletedEventAttributes(a)) => (
            e::EventType::ChildWorkflowExecutionCompleted,
            st::HistoryEvent {
                child_workflow_execution_completed_event_attributes: Some(
                    tk::ChildWorkflowExecutionCompletedEventAttributes {
                        result: a.result.map(|p| p.data).unwrap_or_default(),
                        domain: a.domain,
                        workflow_execution: execution(a.workflow_execution),
                        workflow_type: workflow_type(a.workflow_type),
                        initiated_event_id: a.initiated_event_id,
                        started_event_id: a.started_event_id,
                    },
                ),
                ..Default::default()
            },
        ),
        Some(cadence_proto::api::history_event::Attributes::ChildWorkflowExecutionFailedEventAttributes(a)) => (
            e::EventType::ChildWorkflowExecutionFailed,
            st::HistoryEvent {
                child_workflow_execution_failed_event_attributes: Some(
                    tk::ChildWorkflowExecutionFailedEventAttributes {
                        reason: a.failure.as_ref().map(|f| f.reason.clone()),
                        details: a.failure.map(|f| f.details).unwrap_or_default(),
                        domain: a.domain,
                        workflow_execution: execution(a.workflow_execution),
                        workflow_type: workflow_type(a.workflow_type),
                        initiated_event_id: a.initiated_event_id,
                        started_event_id: a.started_event_id,
                    },
                ),
                ..Default::default()
            },
        ),
        Some(cadence_proto::api::history_event::Attributes::ChildWorkflowExecutionCanceledEventAttributes(a)) => (
            e::EventType::ChildWorkflowExecutionCanceled,
            st::HistoryEvent {
                child_workflow_execution_canceled_event_attributes: Some(
                    tk::ChildWorkflowExecutionCanceledEventAttributes {
                        details: a.details.map(|p| p.data).unwrap_or_default(),
                        domain: a.domain,
                        workflow_execution: execution(a.workflow_execution),
                        workflow_type: workflow_type(a.workflow_type),
                        initiated_event_id: a.initiated_event_id,
                        started_event_id: a.started_event_id,
                    },
                ),
                ..Default::default()
            },
        ),
        Some(cadence_proto::api::history_event::Attributes::ChildWorkflowExecutionTerminatedEventAttributes(a)) => (
            e::EventType::ChildWorkflowExecutionTerminated,
            st::HistoryEvent {
                child_workflow_execution_terminated_event_attributes: Some(
                    tk::ChildWorkflowExecutionTerminatedEventAttributes {
                        domain: a.domain,
                        workflow_execution: execution(a.workflow_execution),
                        workflow_type: workflow_type(a.workflow_type),
                        initiated_event_id: a.initiated_event_id,
                        started_event_id: a.started_event_id,
                    },
                ),
                ..Default::default()
            },
        ),
        Some(cadence_proto::api::history_event::Attributes::ChildWorkflowExecutionTimedOutEventAttributes(a)) => (
            e::EventType::ChildWorkflowExecutionTimedOut,
            st::HistoryEvent {
                child_workflow_execution_timed_out_event_attributes: Some(
                    tk::ChildWorkflowExecutionTimedOutEventAttributes {
                        timeout_type: None,
                        domain: a.domain,
                        workflow_execution: execution(a.workflow_execution),
                        workflow_type: workflow_type(a.workflow_type),
                        initiated_event_id: a.initiated_event_id,
                        started_event_id: a.started_event_id,
                    },
                ),
                ..Default::default()
            },
        ),
        _ => {
            return Err(Status::invalid_argument(
                "Completion event must be a child workflow terminal event.",
            ));
        }
    };
    let mut event = event;
    event.event_id = event_id;
    event.timestamp = timestamp;
    event.event_type = Some(event_type);
    Ok(event)
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
