//! Port of `go/service/history/handler/handler.go` (`handlerImpl`).
//!
//! Engine-routed RPCs follow the Go order: shutting-down check (where Go has
//! it), domain check, host rate limit, request-specific validation, engine
//! lookup via the shard registry, engine call. Dependencies that the Rust
//! engine does not provide yet are reported explicitly (see
//! `memory/history-gap-handler-20261105.md`).
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::admin as adm;
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskToken};
use cadence_api::types::WorkflowExecution;

use crate::constants;
use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::HistoryEngine;
use crate::engine::types::*;
use crate::handler::Handler;
use crate::wiring::assembly::controller::ShardRegistry;
use crate::wiring::server::limiter::RateLimiter;

/// Go `shardOwnershipTransferDelay`.
const SHARD_OWNERSHIP_TRANSFER_DELAY: Duration = Duration::from_secs(5);

pub struct HandlerImpl {
    registry: Arc<ShardRegistry>,
    rate_limiter: RateLimiter,
    shutting_down: AtomicBool,
    started: AtomicBool,
}

fn wf_id(e: &Option<WorkflowExecution>) -> &str {
    e.as_ref().map(|w| w.workflow_id.as_str()).unwrap_or("")
}

fn nil<T>(r: Option<&T>) -> HResult<&T> {
    r.ok_or_else(|| HistoryError::internal("nil request"))
}

/// Go `validateTaskToken`.
fn validate_task_token(t: &TaskToken) -> HResult<()> {
    if t.workflow_id.is_empty() {
        return Err(constants::err_workflow_id_not_set().into());
    }
    if !t.run_id.is_empty() && uuid::Uuid::parse_str(&t.run_id).is_err() {
        return Err(constants::err_run_id_not_valid().into());
    }
    Ok(())
}

fn deser_token(data: &[u8]) -> HResult<TaskToken> {
    JsonTaskTokenSerializer::deserialize(data).map_err(|e| {
        cadence_api::errors::BadRequestError::new(format!("Error deserializing task token. Error: {e}")).into()
    })
}

fn gap(name: &str) -> HistoryError {
    HistoryError::internal(format!("{name}: engine dependency not ported yet (see memory/history-gap-handler-20261105.md)"))
}

fn signal_with_start_retryable(error: &HistoryError) -> bool {
    matches!(
        error,
        HistoryError::PersistenceAlreadyStarted(_) | HistoryError::CurrentWorkflowConditionFailed(_)
    )
}

impl HandlerImpl {
    pub fn new(registry: Arc<ShardRegistry>, rps: f64) -> Self {
        Self {
            registry,
            rate_limiter: RateLimiter::new(rps),
            shutting_down: AtomicBool::new(false),
            started: AtomicBool::new(false),
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    fn check_shutdown(&self) -> HResult<()> {
        if self.is_shutting_down() {
            return Err(constants::err_shutting_down().into());
        }
        Ok(())
    }

    fn check_domain_and_rate(&self, domain_id: &str) -> HResult<()> {
        if domain_id.is_empty() {
            return Err(constants::err_domain_not_set().into());
        }
        self.check_rate()
    }

    fn check_rate(&self) -> HResult<()> {
        if !self.rate_limiter.allow() {
            return Err(constants::err_history_host_throttle().into());
        }
        Ok(())
    }

    async fn engine(&self, workflow_id: &str) -> HResult<Arc<HistoryEngine>> {
        self.registry.engine_for_workflow(workflow_id).await
    }

    async fn engine_shard(&self, shard_id: i32) -> HResult<Arc<HistoryEngine>> {
        if shard_id < 0 {
            return Err(HistoryError::bad_request(format!("invalid shard id {shard_id}")));
        }
        self.registry.engine_for_shard(shard_id as u32).await
    }

    /// Domain + rate limit + engine for the workflow id.
    async fn prep(&self, shutdown: bool, domain_id: &str, workflow_id: &str) -> HResult<Arc<HistoryEngine>> {
        if shutdown {
            self.check_shutdown()?;
        }
        self.check_domain_and_rate(domain_id)?;
        self.engine(workflow_id).await
    }

    /// Token based RPCs: domain + rate limit + token deserialize/validate + engine.
    async fn prep_token(&self, domain_id: &str, token: &[u8]) -> HResult<Arc<HistoryEngine>> {
        self.check_domain_and_rate(domain_id)?;
        let t = deser_token(token)?;
        validate_task_token(&t)?;
        self.engine(&t.workflow_id).await
    }
}

#[async_trait]
impl Handler for HandlerImpl {
    fn start(&self) {
        // Go `handler.Start`: the failover coordinator runs only with graceful failover.
        let env = self.registry.env();
        if (env.config.enable_graceful_failover)() && tokio::runtime::Handle::try_current().is_ok() {
            crate::failover::Coordinator::start(env.failover_coordinator.clone());
        }
        self.started.store(true, Ordering::SeqCst);
    }

    fn stop(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.registry.close_all();
        crate::failover::Coordinator::stop(&*self.registry.env().failover_coordinator);
    }

    async fn prepare_to_stop(&self, remaining: Duration) -> Duration {
        let sleep = SHARD_OWNERSHIP_TRANSFER_DELAY.min(remaining);
        tokio::time::sleep(sleep).await;
        self.shutting_down.store(true, Ordering::SeqCst);
        remaining - sleep
    }

    async fn health(&self) -> HResult<HealthStatus> {
        Ok(HealthStatus { ok: true, msg: "OK".into() })
    }

    async fn close_shard(&self, request: Option<&adm::CloseShardRequest>) -> HResult<()> {
        let r = nil(request)?;
        if r.shard_id < 0 {
            return Err(HistoryError::bad_request("invalid shard id"));
        }
        self.registry.close_shard(r.shard_id as u32);
        Ok(())
    }

    async fn describe_history_host(&self, _r: Option<&adm::DescribeHistoryHostRequest>) -> HResult<adm::DescribeHistoryHostResponse> {
        let ids = self.registry.owned_shard_ids();
        Ok(adm::DescribeHistoryHostResponse {
            number_of_shards: ids.len() as i32,
            shard_ids: ids.into_iter().map(|i| i as i32).collect(),
            ..Default::default()
        })
    }

    async fn describe_mutable_state(&self, request: Option<&DescribeMutableStateRequest>) -> HResult<DescribeMutableStateResponse> {
        let r = nil(request)?;
        self.check_rate()?;
        self.engine(wf_id(&r.execution)).await?.describe_mutable_state(r).await
    }

    async fn describe_queue(&self, request: Option<&adm::DescribeQueueRequest>) -> HResult<adm::DescribeQueueResponse> {
        let r = nil(request)?;
        let e = self.engine_shard(r.shard_id).await?;
        // Go switch on r.Type: transfer / timer; others are invalid.
        match r.task_type {
            Some(2) => e.describe_transfer_queue(&r.cluster_name).await,
            Some(3) => e.describe_timer_queue(&r.cluster_name).await,
            _ => Err(constants::err_invalid_task_type().into()),
        }
    }

    async fn describe_workflow_execution(&self, request: Option<&HistoryDescribeWorkflowExecutionRequest>) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse> {
        let r = nil(request)?;
        let wf = r.request.as_ref().map(|q| wf_id(&q.execution)).unwrap_or("");
        self.prep(false, &r.domain_uuid, wf).await?.describe_workflow_execution(r).await
    }

    async fn get_cross_cluster_tasks(&self, _r: Option<&adm::GetCrossClusterTasksRequest>) -> HResult<adm::GetCrossClusterTasksResponse> {
        Err(gap("GetCrossClusterTasks (deprecated cross-cluster queue)"))
    }

    async fn count_dlq_messages(&self, request: Option<&adm::CountDLQMessagesRequest>) -> HResult<HistoryCountDLQMessagesResponse> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_rate()?;
        // Sum per source cluster across owned shards.
        let mut totals: std::collections::BTreeMap<String, i64> = Default::default();
        for id in self.registry.owned_shard_ids() {
            let e = self.registry.engine_for_shard(id).await?;
            for (cluster, count) in e.count_dlq_messages(r.force_fetch).await? {
                *totals.entry(cluster).or_default() += count;
            }
        }
        let entries = totals
            .into_iter()
            .map(|(source_cluster, message_count)| adm::HistoryDLQCountEntry { source_cluster, message_count })
            .collect();
        Ok(HistoryCountDLQMessagesResponse { entries })
    }

    async fn get_dlq_replication_messages(&self, request: Option<&adm::GetDLQReplicationMessagesRequest>) -> HResult<adm::GetDLQReplicationMessagesResponse> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_rate()?;
        let Some(first) = r.task_infos.first() else {
            return Ok(adm::GetDLQReplicationMessagesResponse::default());
        };
        let e = self.engine(&first.workflow_id).await?;
        let tasks = e.get_dlq_replication_messages(&r.task_infos).await?;
        Ok(adm::GetDLQReplicationMessagesResponse { replication_tasks: Some(tasks) })
    }

    async fn get_mutable_state(&self, request: Option<&GetMutableStateRequest>) -> HResult<GetMutableStateResponse> {
        let r = nil(request)?;
        let e = self.prep(false, &r.domain_uuid, wf_id(&r.execution)).await?;
        e.get_mutable_state(r, None).await
    }

    async fn get_replication_messages(&self, _r: Option<&adm::GetReplicationMessagesRequest>) -> HResult<adm::GetReplicationMessagesResponse> {
        Err(gap("GetReplicationMessages (per-token shard fan-out, size cap)"))
    }

    async fn merge_dlq_messages(&self, request: Option<&adm::MergeDLQMessagesRequest>) -> HResult<adm::MergeDLQMessagesResponse> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_rate()?;
        self.engine_shard(r.shard_id).await?.merge_dlq_messages(r).await
    }

    async fn notify_failover_markers(&self, request: Option<&NotifyFailoverMarkersRequest>) -> HResult<()> {
        // Go `NotifyFailoverMarkers`: no shutdown/rate checks; hand each token to the coordinator.
        let r = nil(request)?;
        let coordinator = &self.registry.env().failover_coordinator;
        for token in &r.failover_marker_tokens {
            if let Some(marker) = token.failover_marker.clone() {
                crate::failover::Coordinator::receive_failover_markers(&**coordinator, token.shard_ids.clone(), marker);
            }
        }
        Ok(())
    }

    async fn poll_mutable_state(&self, request: Option<&PollMutableStateRequest>) -> HResult<PollMutableStateResponse> {
        let r = nil(request)?;
        let e = self.prep(false, &r.domain_uuid, wf_id(&r.execution)).await?;
        e.poll_mutable_state(r, None).await
    }

    async fn purge_dlq_messages(&self, request: Option<&adm::PurgeDLQMessagesRequest>) -> HResult<()> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_rate()?;
        self.engine_shard(r.shard_id).await?.purge_dlq_messages(r).await
    }

    async fn query_workflow(&self, request: Option<&HistoryQueryWorkflowRequest>) -> HResult<HistoryQueryWorkflowResponse> {
        let r = nil(request)?;
        let wf = r.request.as_ref().map(|q| wf_id(&q.execution)).unwrap_or("");
        self.prep(true, &r.domain_uuid, wf).await?.query_workflow(r, None).await
    }

    async fn read_dlq_messages(&self, request: Option<&adm::ReadDLQMessagesRequest>) -> HResult<adm::ReadDLQMessagesResponse> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_rate()?;
        self.engine_shard(r.shard_id).await?.read_dlq_messages(r).await
    }

    async fn reapply_events(&self, request: Option<&HistoryReapplyEventsRequest>) -> HResult<()> {
        let r = nil(request)?;
        let inner = r.request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        let wf = wf_id(&inner.workflow_execution);
        let run = inner.workflow_execution.as_ref().map(|w| w.run_id.clone()).unwrap_or_default();
        let _e = self.prep(true, &r.domain_uuid, wf).await?;
        let events = inner.events.as_ref().map(|b| b.data.clone()).unwrap_or_default();
        let _ = (run, events);
        Err(gap("ReapplyEvents (DataBlob deserialisation of events)"))
    }

    async fn record_activity_task_heartbeat(&self, request: Option<&HistoryRecordActivityTaskHeartbeatRequest>) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse> {
        let r = nil(request)?;
        let hb = r.heartbeat_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.prep_token(&r.domain_uuid, &hb.task_token).await?.record_activity_task_heartbeat(r).await
    }

    async fn record_activity_task_started(&self, request: Option<&RecordActivityTaskStartedRequest>) -> HResult<RecordActivityTaskStartedResponse> {
        let r = nil(request)?;
        self.prep(false, &r.domain_uuid, wf_id(&r.workflow_execution)).await?.record_activity_task_started(r).await
    }

    async fn record_child_execution_completed(&self, request: Option<&RecordChildExecutionCompletedRequest>) -> HResult<()> {
        let r = nil(request)?;
        self.prep(true, &r.domain_uuid, wf_id(&r.workflow_execution)).await?.record_child_execution_completed(r).await
    }

    async fn record_decision_task_started(&self, request: Option<&RecordDecisionTaskStartedRequest>) -> HResult<RecordDecisionTaskStartedResponse> {
        let r = nil(request)?;
        self.prep(false, &r.domain_uuid, wf_id(&r.workflow_execution)).await?.record_decision_task_started(r).await
    }

    async fn refresh_workflow_tasks(&self, request: Option<&HistoryRefreshWorkflowTasksRequest>) -> HResult<()> {
        let r = nil(request)?;
        let inner = r.request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        let exec = inner.workflow_execution.clone().unwrap_or_default();
        self.prep(true, &r.domain_uuid, &exec.workflow_id).await?.refresh_workflow_tasks(&r.domain_uuid, &exec).await
    }

    async fn remove_signal_mutable_state(&self, request: Option<&RemoveSignalMutableStateRequest>) -> HResult<()> {
        let r = nil(request)?;
        self.prep(true, &r.domain_uuid, wf_id(&r.workflow_execution)).await?.remove_signal_mutable_state(r).await
    }

    async fn remove_task(&self, _r: Option<&adm::RemoveTaskRequest>) -> HResult<()> {
        Err(gap("RemoveTask (shard-context task deletion)"))
    }

    async fn replicate_events_v2(&self, request: Option<&ReplicateEventsV2Request>) -> HResult<()> {
        let r = nil(request)?;
        self.prep(true, &r.domain_uuid, wf_id(&r.workflow_execution)).await?.replicate_events_v2(r).await
    }

    async fn request_cancel_workflow_execution(&self, request: Option<&HistoryRequestCancelWorkflowExecutionRequest>) -> HResult<()> {
        let r = nil(request)?;
        self.check_shutdown()?;
        let c = r.cancel_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        if r.domain_uuid.is_empty() || c.domain.is_empty() {
            return Err(constants::err_domain_not_set().into());
        }
        self.check_rate()?;
        self.engine(wf_id(&c.workflow_execution)).await?.request_cancel_workflow_execution(r).await
    }

    async fn reset_queue(&self, request: Option<&adm::ResetQueueRequest>) -> HResult<()> {
        let r = nil(request)?;
        let e = self.engine_shard(r.shard_id).await?;
        match r.task_type {
            Some(2) => e.reset_transfer_queue(&r.cluster_name).await,
            Some(3) => e.reset_timer_queue(&r.cluster_name).await,
            _ => Err(constants::err_invalid_task_type().into()),
        }
    }

    async fn reset_sticky_task_list(&self, request: Option<&HistoryResetStickyTaskListRequest>) -> HResult<HistoryResetStickyTaskListResponse> {
        let r = nil(request)?;
        self.prep(true, &r.domain_uuid, wf_id(&r.execution)).await?.reset_sticky_task_list(r).await
    }

    async fn reset_workflow_execution(&self, request: Option<&HistoryResetWorkflowExecutionRequest>) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse> {
        let r = nil(request)?;
        let wf = r.reset_request.as_ref().map(|q| wf_id(&q.workflow_execution)).unwrap_or("");
        self.prep(true, &r.domain_uuid, wf).await?.reset_workflow_execution(r).await
    }

    async fn respond_activity_task_canceled(&self, request: Option<&HistoryRespondActivityTaskCanceledRequest>) -> HResult<()> {
        let r = nil(request)?;
        let c = r.cancel_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.prep_token(&r.domain_uuid, &c.task_token).await?.respond_activity_task_canceled(r).await
    }

    async fn respond_activity_task_completed(&self, request: Option<&HistoryRespondActivityTaskCompletedRequest>) -> HResult<()> {
        let r = nil(request)?;
        let c = r.complete_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.prep_token(&r.domain_uuid, &c.task_token).await?.respond_activity_task_completed(r).await
    }

    async fn respond_activity_task_failed(&self, request: Option<&HistoryRespondActivityTaskFailedRequest>) -> HResult<()> {
        let r = nil(request)?;
        let c = r.failed_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.prep_token(&r.domain_uuid, &c.task_token).await?.respond_activity_task_failed(r).await
    }

    async fn respond_cross_cluster_tasks_completed(&self, _r: Option<&adm::RespondCrossClusterTasksCompletedRequest>) -> HResult<adm::RespondCrossClusterTasksCompletedResponse> {
        Err(gap("RespondCrossClusterTasksCompleted (deprecated cross-cluster queue)"))
    }

    async fn respond_decision_task_completed(&self, request: Option<&HistoryRespondDecisionTaskCompletedRequest>) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        let r = nil(request)?;
        let c = r.complete_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.prep_token(&r.domain_uuid, &c.task_token).await?.respond_decision_task_completed(r).await
    }

    async fn respond_decision_task_failed(&self, request: Option<&HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> {
        let r = nil(request)?;
        let c = r.failed_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.prep_token(&r.domain_uuid, &c.task_token).await?.respond_decision_task_failed(r).await
    }

    async fn schedule_decision_task(&self, request: Option<&ScheduleDecisionTaskRequest>) -> HResult<()> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_domain_and_rate(&r.domain_uuid)?;
        let wf = r.workflow_execution.as_ref().ok_or_else(|| HistoryError::from(constants::err_workflow_execution_not_set()))?;
        self.engine(&wf.workflow_id).await?.schedule_decision_task(r).await
    }

    async fn signal_with_start_workflow_execution(&self, request: Option<&HistorySignalWithStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_domain_and_rate(&r.domain_uuid)?;
        let workflow_id = r.signal_with_start_request.as_ref().map(|q| q.workflow_id.as_str()).unwrap_or("");
        let engine = self.engine(workflow_id).await?;
        match engine.signal_with_start_workflow_execution(r).await {
            Err(e) if signal_with_start_retryable(&e) => {
                // Go retries once when concurrent signal-with-start requests race
                // while creating/reusing the workflow execution.
                engine.signal_with_start_workflow_execution(r).await
            }
            result => result,
        }
    }

    async fn signal_workflow_execution(&self, request: Option<&HistorySignalWorkflowExecutionRequest>) -> HResult<()> {
        let r = nil(request)?;
        let wf = r.signal_request.as_ref().map(|q| wf_id(&q.workflow_execution)).unwrap_or("");
        self.prep(true, &r.domain_uuid, wf).await?.signal_workflow_execution(r).await
    }

    async fn start_workflow_execution(&self, request: Option<&HistoryStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        let r = nil(request)?;
        self.check_domain_and_rate(&r.domain_uuid)?;
        let workflow_id = r.start_request.as_ref().map(|q| q.workflow_id.as_str()).unwrap_or("");
        self.engine(workflow_id).await?.start_workflow_execution(r).await
    }

    async fn sync_activity(&self, request: Option<&SyncActivityRequest>) -> HResult<()> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_domain_and_rate(&r.domain_id)?;
        if r.workflow_id.is_empty() {
            return Err(constants::err_workflow_id_not_set().into());
        }
        if r.run_id.is_empty() || uuid::Uuid::parse_str(&r.run_id).is_err() {
            return Err(constants::err_run_id_not_valid().into());
        }
        self.engine(&r.workflow_id).await?.sync_activity(r).await
    }

    async fn sync_shard_status(&self, request: Option<&SyncShardStatusRequest>) -> HResult<()> {
        let r = nil(request)?;
        self.check_shutdown()?;
        self.check_rate()?;
        if r.source_cluster.is_empty() {
            return Err(constants::err_source_cluster_not_set().into());
        }
        if r.timestamp.is_none() {
            return Err(constants::err_timestamp_not_set().into());
        }
        self.engine_shard(r.shard_id as i32).await?.sync_shard_status(r).await
    }

    async fn terminate_workflow_execution(&self, request: Option<&HistoryTerminateWorkflowExecutionRequest>) -> HResult<()> {
        let r = nil(request)?;
        let wf = r.terminate_request.as_ref().map(|q| wf_id(&q.workflow_execution)).unwrap_or("");
        self.prep(true, &r.domain_uuid, wf).await?.terminate_workflow_execution(r).await
    }

    async fn get_failover_info(&self, request: Option<&GetFailoverInfoRequest>) -> HResult<GetFailoverInfoResponse> {
        let r = nil(request)?;
        self.check_shutdown()?;
        match crate::failover::Coordinator::get_failover_info(&*self.registry.env().failover_coordinator, &r.domain_id) {
            Ok(resp) => Ok(GetFailoverInfoResponse {
                completed_shard_count: resp.completed_shard_count,
                pending_shards: resp.pending_shards,
            }),
            Err(e) => Err(HistoryError::entity_not_exists(e.to_string())),
        }
    }

    async fn ratelimit_update(&self, _r: Option<&RatelimitUpdateRequest>) -> HResult<RatelimitUpdateResponse> {
        Err(gap("RatelimitUpdate (ratelimit aggregator not ported)"))
    }
}

#[cfg(test)]
mod start_handler_tests {
    use super::*;
    use crate::engine::deps::WorkflowRequestType;

    #[test]
    fn signal_with_start_retries_only_go_concurrency_conflicts() {
        assert!(signal_with_start_retryable(&HistoryError::PersistenceAlreadyStarted(Box::default())));
        assert!(signal_with_start_retryable(&HistoryError::CurrentWorkflowConditionFailed("race".into())));
        assert!(!signal_with_start_retryable(&HistoryError::DuplicateRequest {
            request_type: WorkflowRequestType::Start,
            run_id: "run".into(),
        }));
        assert!(!signal_with_start_retryable(&HistoryError::internal("unrelated")));
    }
}
