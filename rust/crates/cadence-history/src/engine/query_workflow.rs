//! Port of engineimpl/query_workflow.go.
use super::{deps::*, runtime::*, types::*};
use cadence_api::{
    enums::{
        QueryConsistencyLevel, QueryRejectCondition, QueryResultType, WorkflowExecutionCloseStatus,
    },
    errors::{QueryFailedError, QueryRejected},
    tasks::*,
};
use std::time::{Duration, Instant};
struct QueryTimer<'a> {
    metrics: &'a dyn EngineMetrics,
    domain: &'a str,
    name: &'static str,
    started: Instant,
}
impl Drop for QueryTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .query_latency(self.domain, self.name, self.started.elapsed());
    }
}
impl HistoryEngine {
    fn query_timer<'a>(&'a self, domain: &'a str, name: &'static str) -> QueryTimer<'a> {
        QueryTimer {
            metrics: &*self.metrics,
            domain,
            name,
            started: Instant::now(),
        }
    }
    pub async fn query_workflow(
        &self,
        request: &HistoryQueryWorkflowRequest,
        deadline: Option<Instant>,
    ) -> HResult<HistoryQueryWorkflowResponse> {
        let mut req = request.request.clone().unwrap_or_default();
        if req.query_consistency_level == Some(QueryConsistencyLevel::Strong)
            && !(self.config.enable_consistent_query()
                && self.config.enable_consistent_query_by_domain(&req.domain))
        {
            return Err(HistoryError::bad_request("cluster or domain does not enable strongly consistent query but strongly consistent query was requested"));
        }
        if req.query_consistency_level == Some(QueryConsistencyLevel::Strong) {
            self.metrics.consistent_query_per_shard();
        }
        let execution = req.execution.clone().unwrap_or_default();
        let mut response = self
            .load_mutable_state(&request.domain_uuid, &execution)
            .await?;
        if !response.is_workflow_running {
            let status = response.workflow_close_state.unwrap_or_default();
            let reject = req.query_reject_condition == Some(QueryRejectCondition::NotOpen)
                || (req.query_reject_condition == Some(QueryRejectCondition::NotCompletedCleanly)
                    && status != WORKFLOW_CLOSE_STATUS_COMPLETED);
            if reject {
                return Ok(HistoryQueryWorkflowResponse {
                    response: Some(QueryWorkflowResponse {
                        query_rejected: Some(QueryRejected {
                            close_status: close_status(status),
                        }),
                        ..Default::default()
                    }),
                });
            }
        }
        let mut wait = Duration::from_secs(1);
        if let Some(deadline) = deadline {
            wait = wait.max(
                deadline
                    .saturating_duration_since(Instant::now())
                    .saturating_sub(Duration::from_secs(1)),
            );
        }
        let first_deadline = Instant::now() + wait;
        while response.previous_started_event_id.unwrap_or_default() <= 0
            && Instant::now() < first_deadline
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
            response = self
                .load_mutable_state(&request.domain_uuid, &execution)
                .await?;
        }
        if response.previous_started_event_id.unwrap_or_default() <= 0 {
            self.metrics
                .query_counter(&req.domain, "QueryBeforeFirstDecisionCount");
            return Err(QueryFailedError {
                message: "workflow must handle at least one decision task before it can be queried"
                    .into(),
            }
            .into());
        }
        let domain = self.domain_cache.get_domain_by_id(&request.domain_uuid)?;
        let (context, release) = self
            .execution_cache
            .get_or_create_workflow_execution(&request.domain_uuid, &execution)
            .await?;
        let result = async {
            let state = context.load_workflow_execution().await?;
            if self.check_for_history_corruptions(&*state).await? {
                return Err(HistoryError::entity_not_exists(
                    "Workflow execution corrupted.",
                ));
            }
            let attribute = state
                .get_execution_info()
                .active_cluster_selection_policy
                .and_then(|p| p.cluster_attribute);
            let active = domain
                .active_clusters
                .iter()
                .find(|(a, _)| a == &attribute)
                .map(|(_, cluster)| cluster == &self.cluster_metadata.get_current_cluster_name())
                .unwrap_or_else(|| {
                    tracing::warn!(
                        "Failed to get active cluster info by cluster attribute, default to active"
                    );
                    true
                });
            let direct = !active
                || !state.is_workflow_execution_running()
                || req
                    .query_consistency_level
                    .unwrap_or(QueryConsistencyLevel::Eventual)
                    == QueryConsistencyLevel::Eventual
                || (!state.has_pending_decision() && !state.has_in_flight_decision());
            if direct {
                release(None);
                let response = self
                    .load_mutable_state(&request.domain_uuid, &execution)
                    .await?;
                req.execution = response.execution.clone();
                return self
                    .query_directly_through_matching(
                        &response,
                        &request.domain_uuid,
                        &req,
                        active,
                        deadline,
                    )
                    .await;
            }
            let _timer = self.query_timer(
                request
                    .request
                    .as_ref()
                    .map(|r| r.domain.as_str())
                    .unwrap_or_default(),
                "DecisionTaskQueryLatency",
            );
            let registry = state.get_query_registry();
            if registry.buffered_count() >= self.config.max_buffered_query_count() {
                self.metrics
                    .query_counter(&req.domain, "QueryBufferExceededCount");
                return Err(HistoryError::internal(
                    "consistent query buffer is full, cannot accept new consistent queries",
                ));
            }
            let (id, channel) = registry.buffer_query(req.query.as_ref());
            release(None);
            let result = async {
                match deadline {
                    Some(deadline) => {
                        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), channel)
                            .await
                            .map_err(|_| {
                                self.metrics
                                    .query_counter(&req.domain, "ConsistentQueryTimeoutCount");
                                HistoryError::DeadlineExceeded
                            })?
                            .map_err(|_| HistoryError::Canceled)?
                    }
                    None => channel.await.map_err(|_| HistoryError::Canceled)?,
                }
                match registry.get_termination_state(&id).inspect_err(|_| {
                    self.metrics
                        .query_counter(&req.domain, "QueryRegistryInvalidStateCount");
                })? {
                    QueryTermination::Completed(result) => {
                        match result.result_type.unwrap_or(QueryResultType::Answered) {
                            QueryResultType::Answered => Ok(HistoryQueryWorkflowResponse {
                                response: Some(QueryWorkflowResponse {
                                    query_result: result.answer,
                                    ..Default::default()
                                }),
                            }),
                            QueryResultType::Failed => Err(QueryFailedError {
                                message: result.error_message,
                            }
                            .into()),
                            _ => {
                                self.metrics
                                    .query_counter(&req.domain, "QueryRegistryInvalidStateCount");
                                Err(invalid_query_state())
                            }
                        }
                    }
                    QueryTermination::Unblocked => {
                        let response = self
                            .load_mutable_state(&request.domain_uuid, &execution)
                            .await?;
                        req.execution = response.execution.clone();
                        self.query_directly_through_matching(
                            &response,
                            &request.domain_uuid,
                            &req,
                            active,
                            deadline,
                        )
                        .await
                    }
                    QueryTermination::Failed(err) => Err(err),
                    QueryTermination::Invalid => {
                        self.metrics
                            .query_counter(&req.domain, "QueryRegistryInvalidStateCount");
                        Err(invalid_query_state())
                    }
                }
            }
            .await;
            registry.remove_query(&id);
            result
        }
        .await;
        release(result.as_ref().err());
        result
    }
    async fn query_directly_through_matching(
        &self,
        response: &GetMutableStateResponse,
        domain_id: &str,
        req: &QueryWorkflowRequest,
        active: bool,
        deadline: Option<Instant>,
    ) -> HResult<HistoryQueryWorkflowResponse> {
        let _timer = self.query_timer(&req.domain, "DirectQueryDispatchLatency");
        let sticky = response
            .sticky_task_list
            .as_ref()
            .is_some_and(|t| !t.name.is_empty());
        if response.is_sticky_task_list_enabled
            && sticky
            && active
            && self.config.enable_sticky_query(&req.domain)
            && self
                .client_checker
                .supports_sticky_query(&response.client_impl, &response.client_feature_version)
        {
            // Sticky uses an independent timeout even when the caller deadline is short.
            let duration = Duration::from_secs(
                response
                    .sticky_task_list_schedule_to_start_timeout
                    .unwrap_or_default()
                    .max(0) as u64,
            );
            let sticky_timer = self.query_timer(&req.domain, "DirectQueryDispatchStickyLatency");
            let result = tokio::time::timeout(
                duration,
                self.raw_matching_client.query_workflow(
                    domain_id,
                    req,
                    response.sticky_task_list.as_ref(),
                ),
            )
            .await;
            drop(sticky_timer);
            match result {
                Ok(Ok(response)) => {
                    self.metrics
                        .query_counter(&req.domain, "DirectQueryDispatchStickySuccessCount");
                    return Ok(HistoryQueryWorkflowResponse {
                        response: Some(response),
                    });
                }
                Ok(Err(
                    HistoryError::StickyWorkerUnavailable
                    | HistoryError::DeadlineExceeded
                    | HistoryError::TransportStatus { code: 4, .. },
                ))
                | Err(_) => {}
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "query directly though matching on sticky failed, will not attempt query on non-sticky");
                    return Err(err);
                }
            }
            if response.is_workflow_running {
                tracing::info!(domain = %req.domain, "query direct through matching failed on sticky, clearing sticky before attempting on non-sticky");
                let reset = HistoryResetStickyTaskListRequest {
                    domain_uuid: domain_id.into(),
                    execution: req.execution.clone(),
                };
                let clear_timer =
                    self.query_timer(&req.domain, "DirectQueryDispatchClearStickinessLatency");
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    self.reset_sticky_task_list(&reset),
                )
                .await
                .map_err(|_| HistoryError::DeadlineExceeded);
                drop(clear_timer);
                if let Err(err) = result? {
                    if !err.is_workflow_already_completed() && !err.is_entity_not_exists() {
                        return Err(err);
                    }
                }
                self.metrics.query_counter(
                    &req.domain,
                    "DirectQueryDispatchClearStickinessSuccessCount",
                );
            }
        }
        if deadline.is_some_and(|d| d <= Instant::now()) {
            tracing::info!(domain = %req.domain, "query context timed out before query on non-sticky task list could be attempted");
            self.metrics.query_counter(
                &req.domain,
                "DirectQueryDispatchTimeoutBeforeNonStickyCount",
            );
            return Err(HistoryError::DeadlineExceeded);
        }
        let non_sticky_timer = self.query_timer(&req.domain, "DirectQueryDispatchNonStickyLatency");
        let call = self
            .matching_client
            .query_workflow(domain_id, req, response.task_list.as_ref());
        let response = match deadline {
            Some(deadline) => {
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), call)
                    .await
                    .map_err(|_| HistoryError::DeadlineExceeded)?
            }
            None => call.await,
        };
        drop(non_sticky_timer);
        let response = response.inspect_err(|err| {
            if err.to_string().contains("unknown queryType") {
                tracing::info!(error = %err, domain = %req.domain, "user calls for unsupported query type");
            } else {
                tracing::error!(error = %err, domain = %req.domain, "query directly though matching on non-sticky failed");
            }
        })?;
        self.metrics
            .query_counter(&req.domain, "DirectQueryDispatchNonStickySuccessCount");
        Ok(HistoryQueryWorkflowResponse {
            response: Some(response),
        })
    }
}
fn invalid_query_state() -> HistoryError {
    HistoryError::bad_request("query entered invalid state, this should be impossible")
}
pub fn close_status(status: i32) -> Option<WorkflowExecutionCloseStatus> {
    match status {
        1 => Some(WorkflowExecutionCloseStatus::Completed),
        2 => Some(WorkflowExecutionCloseStatus::Failed),
        3 => Some(WorkflowExecutionCloseStatus::Canceled),
        4 => Some(WorkflowExecutionCloseStatus::Terminated),
        5 => Some(WorkflowExecutionCloseStatus::ContinuedAsNew),
        6 => Some(WorkflowExecutionCloseStatus::TimedOut),
        _ => None,
    }
}
