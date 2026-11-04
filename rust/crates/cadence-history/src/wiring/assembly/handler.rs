//! Port of `go/service/history/handler/handler.go` (`handlerImpl`): request
//! validation, shutdown/health semantics and routing to the owning shard engine.
//!
//! The shard controller is abstracted by [`EngineRouter`] (Go `shard.Controller`
//! plus the few host-level facilities the handler uses directly).
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::admin as adm;
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskToken};
use cadence_api::types::WorkflowExecution;

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::HistoryEngine;
use crate::engine::types as et;
use crate::handler::interface::Handler;

/// `shardOwnershipTransferDelay` (handler.go).
pub const SHARD_OWNERSHIP_TRANSFER_DELAY: Duration = Duration::from_secs(5);

/// Go `shard.Controller` as used by the handler, plus host facilities.
#[async_trait]
pub trait EngineRouter: Send + Sync {
    /// `GetEngine(workflowID)`.
    fn engine_for_workflow(&self, workflow_id: &str) -> HResult<Arc<HistoryEngine>>;
    /// `GetEngineForShard(shardID)`.
    fn engine_for_shard(&self, shard_id: i32) -> HResult<Arc<HistoryEngine>>;
    /// `RemoveEngineForShard(shardID)`.
    fn close_shard(&self, shard_id: i32);
    /// `ShardIDs()`.
    fn shard_ids(&self) -> Vec<i32>;
    /// `controller.PrepareToStop()`.
    fn prepare_to_stop(&self);
    /// `DescribeHistoryHost` body (controller status, shards, domain cache size, host address).
    fn describe_host(&self, request: &adm::DescribeHistoryHostRequest) -> HResult<adm::DescribeHistoryHostResponse>;
    /// `RemoveTask`: `executionMgr.CompleteHistoryTask` for the Go task type
    /// (transfer/timer/replication; other types must yield `ErrInvalidTaskType`).
    async fn remove_task(&self, request: &adm::RemoveTaskRequest) -> HResult<()>;
    /// `failoverCoordinator.ReceiveFailoverMarkers` for every token.
    fn receive_failover_markers(&self, tokens: &[et::FailoverMarkerToken]) -> HResult<()>;
    /// `GetFailoverInfo` (failover coordinator / handoff info).
    async fn get_failover_info(&self, request: &et::GetFailoverInfoRequest) -> HResult<et::GetFailoverInfoResponse>;
    /// `RatelimitUpdate` (global ratelimiter aggregator).
    async fn ratelimit_update(&self, request: &et::RatelimitUpdateRequest) -> HResult<et::RatelimitUpdateResponse>;
    /// `config.MaxResponseSize` for GetReplicationMessages.
    fn max_response_size(&self) -> usize;
}

fn err_shutting_down() -> HistoryError {
    HistoryError::internal("Shutting down")
}
fn err_domain_not_set() -> HistoryError {
    HistoryError::bad_request("Domain not set on request.")
}
fn err_workflow_id_not_set() -> HistoryError {
    HistoryError::bad_request("WorkflowId is not set on request.")
}
fn err_run_id_not_valid() -> HistoryError {
    HistoryError::bad_request("RunID is not valid UUID.")
}
fn err_execution_not_set() -> HistoryError {
    HistoryError::bad_request("WorkflowExecution not set on request.")
}
fn err_source_cluster_not_set() -> HistoryError {
    HistoryError::bad_request("Source Cluster not set on request.")
}
fn err_timestamp_not_set() -> HistoryError {
    HistoryError::bad_request("Timestamp not set on request.")
}
fn err_nil_request() -> HistoryError {
    HistoryError::internal("request is nil")
}
/// TODO(D4-remaining): the engine operation is not ported yet.
fn not_ported(op: &str) -> HistoryError {
    HistoryError::internal(format!("{op}: engine operation not ported yet"))
}

fn is_uuid(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok()
}

fn wf_id(e: &Option<WorkflowExecution>) -> &str {
    e.as_ref().map(|e| e.workflow_id.as_str()).unwrap_or("")
}

fn nonnil<T>(r: Option<&T>) -> HResult<&T> {
    r.ok_or_else(err_nil_request)
}

/// Go `validateTaskToken`.
fn validate_task_token(data: &[u8]) -> HResult<TaskToken> {
    let token = JsonTaskTokenSerializer::deserialize(data).map_err(|_| HistoryError::bad_request("Error deserializing task token."))?;
    if token.workflow_id.is_empty() {
        return Err(err_workflow_id_not_set());
    }
    Ok(token)
}

/// `handlerImpl`.
pub struct HistoryHandler {
    router: Arc<dyn EngineRouter>,
    started: tokio::sync::watch::Sender<bool>,
    shutting_down: AtomicBool,
    ownership_transfer_delay: Duration,
}

impl HistoryHandler {
    pub fn new(router: Arc<dyn EngineRouter>) -> Self {
        Self { router, started: tokio::sync::watch::channel(false).0, shutting_down: AtomicBool::new(false), ownership_transfer_delay: SHARD_OWNERSHIP_TRANSFER_DELAY }
    }

    /// Test hook: shorten the drain delay.
    pub fn with_ownership_transfer_delay(mut self, d: Duration) -> Self {
        self.ownership_transfer_delay = d;
        self
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// `startWG.Wait()`.
    async fn wait_started(&self) {
        let mut rx = self.started.subscribe();
        let _ = rx.wait_for(|s| *s).await;
    }

    /// startWG.Wait + shutting-down check shared by most RPCs.
    async fn gate(&self) -> HResult<()> {
        self.wait_started().await;
        if self.is_shutting_down() {
            return Err(err_shutting_down());
        }
        Ok(())
    }

    fn engine(&self, workflow_id: &str) -> HResult<Arc<HistoryEngine>> {
        self.router.engine_for_workflow(workflow_id)
    }
}

/// gate + nil check + domain check + engine lookup, then evaluate `$call`.
macro_rules! route {
    ($self:ident, $req:ident, $r:ident => ($dom:expr, $wid:expr), $e:ident => $call:expr) => {{
        $self.gate().await?;
        let $r = nonnil($req)?;
        let dom: &str = $dom;
        if dom.is_empty() {
            return Err(err_domain_not_set());
        }
        let wid: String = $wid;
        let $e = $self.engine(&wid)?;
        $call.await
    }};
}

#[async_trait]
impl Handler for HistoryHandler {
    fn start(&self) {
        let _ = self.started.send(true);
    }

    fn stop(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    async fn prepare_to_stop(&self, remaining: Duration) -> Duration {
        self.router.prepare_to_stop();
        // common.SleepWithMinDuration(shardOwnershipTransferDelay, remainingTime)
        let sleep = self.ownership_transfer_delay.min(remaining);
        tokio::time::sleep(sleep).await;
        self.shutting_down.store(true, Ordering::SeqCst);
        remaining - sleep
    }

    async fn health(&self) -> HResult<et::HealthStatus> {
        self.wait_started().await;
        Ok(et::HealthStatus { ok: true, msg: "OK".into() })
    }

    async fn close_shard(&self, request: Option<&adm::CloseShardRequest>) -> HResult<()> {
        let r = nonnil(request)?;
        self.router.close_shard(r.shard_id);
        Ok(())
    }

    async fn describe_history_host(&self, request: Option<&adm::DescribeHistoryHostRequest>) -> HResult<adm::DescribeHistoryHostResponse> {
        self.wait_started().await;
        self.router.describe_host(nonnil(request)?)
    }

    async fn describe_mutable_state(&self, request: Option<&et::DescribeMutableStateRequest>) -> HResult<et::DescribeMutableStateResponse> {
        route!(self, request, r => (&r.domain_uuid, wf_id(&r.execution).to_string()), e => e.describe_mutable_state(r))
    }

    async fn describe_queue(&self, request: Option<&adm::DescribeQueueRequest>) -> HResult<adm::DescribeQueueResponse> {
        self.wait_started().await;
        let r = nonnil(request)?;
        let e = self.router.engine_for_shard(r.shard_id)?;
        // Go: switch on the task type (TaskTypeTransfer = 2, TaskTypeTimer = 3).
        match r.task_type {
            Some(2) => e.describe_transfer_queue(&r.cluster_name).await,
            Some(3) => e.describe_timer_queue(&r.cluster_name).await,
            _ => Err(HistoryError::bad_request("Invalid task type")),
        }
    }

    async fn describe_workflow_execution(&self, request: Option<&et::HistoryDescribeWorkflowExecutionRequest>) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        // TODO(D4-remaining): HistoryEngine::describe_workflow_execution is not ported.
        Err(not_ported("DescribeWorkflowExecution"))
    }

    async fn get_cross_cluster_tasks(&self, _request: Option<&adm::GetCrossClusterTasksRequest>) -> HResult<adm::GetCrossClusterTasksResponse> {
        Err(HistoryError::bad_request("The cross-cluster feature has been deprecated."))
    }

    async fn count_dlq_messages(&self, request: Option<&adm::CountDLQMessagesRequest>) -> HResult<et::HistoryCountDLQMessagesResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        let futs = self.router.shard_ids().into_iter().map(|shard| {
            let router = self.router.clone();
            let force = r.force_fetch;
            async move {
                let engine = router.engine_for_shard(shard).map_err(|e| HistoryError::Other(format!("dlq count for shard {shard}: {e}")))?;
                engine.count_dlq_messages(force).await.map_err(|e| HistoryError::Other(format!("dlq count for shard {shard}: {e}")))
            }
        });
        let results = futures_util::future::join_all(futs).await;
        // TODO(D4-remaining): HistoryDLQCountEntry has no shard_id, so counts are summed per source cluster.
        let mut totals: HashMap<String, i64> = HashMap::new();
        let mut first_err = None;
        for res in results {
            match res {
                Ok(counts) => {
                    for (cluster, n) in counts {
                        *totals.entry(cluster).or_default() += n;
                    }
                }
                Err(e) => first_err = first_err.or(Some(e)),
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        let mut entries: Vec<_> = totals.into_iter().map(|(source_cluster, message_count)| adm::HistoryDLQCountEntry { source_cluster, message_count }).collect();
        entries.sort_by(|a, b| a.source_cluster.cmp(&b.source_cluster));
        Ok(et::HistoryCountDLQMessagesResponse { entries })
    }

    async fn get_dlq_replication_messages(&self, request: Option<&adm::GetDLQReplicationMessagesRequest>) -> HResult<adm::GetDLQReplicationMessagesResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        // Batch by (domain, workflow, run) as Go does.
        let mut groups: Vec<((String, String, String), Vec<adm::ReplicationTaskInfo>)> = Vec::new();
        for info in &r.task_infos {
            let key = (info.domain_id.clone(), info.workflow_id.clone(), info.run_id.clone());
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, v)) => v.push(info.clone()),
                None => groups.push((key, vec![info.clone()])),
            }
        }
        let futs = groups.into_iter().map(|((_, wid, _), infos)| {
            let router = self.router.clone();
            async move {
                let engine = match router.engine_for_workflow(&wid) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(error = %e, "History engine not found for workflow");
                        return Vec::new();
                    }
                };
                match engine.get_dlq_replication_messages(&infos).await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to get dlq replication tasks");
                        Vec::new()
                    }
                }
            }
        });
        let tasks: Vec<_> = futures_util::future::join_all(futs).await.into_iter().flatten().collect();
        Ok(adm::GetDLQReplicationMessagesResponse { replication_tasks: Some(tasks) })
    }

    async fn get_mutable_state(&self, request: Option<&et::GetMutableStateRequest>) -> HResult<et::GetMutableStateResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let engine = self.engine(wf_id(&r.execution))?;
        engine.get_mutable_state(r, None).await
    }

    async fn get_replication_messages(&self, request: Option<&adm::GetReplicationMessagesRequest>) -> HResult<adm::GetReplicationMessagesResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        let futs = r.tokens.iter().map(|token| {
            let router = self.router.clone();
            let cluster = r.cluster_name.clone();
            let shard = token.shard_id;
            let last = token.last_retrieved_message_id;
            async move {
                let engine = match router.engine_for_shard(shard) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(error = %e, "History engine not found for shard");
                        return None;
                    }
                };
                match engine.get_replication_messages(&cluster, last).await {
                    Ok(m) => Some((shard, m)),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to get replication tasks for shard");
                        None
                    }
                }
            }
        });
        let mut msgs: Vec<_> = futures_util::future::join_all(futs).await.into_iter().flatten().collect();
        // TODO(D4-remaining): Go sorts by earliest task creation time before applying the size cap.
        msgs.sort_by_key(|(shard, _)| *shard);
        let max = self.router.max_response_size();
        let mut total = 0usize;
        let mut by_shard = HashMap::new();
        for (shard, m) in msgs {
            let size = serde_json::to_vec(&m).map(|v| v.len()).unwrap_or(0);
            if total + size >= max {
                tracing::warn!(shard_id = shard, size, total, max, "Replication messages did not fit in the response (history host)");
                continue;
            }
            total += size;
            by_shard.insert(shard, m);
        }
        Ok(adm::GetReplicationMessagesResponse { messages_by_shard: Some(by_shard) })
    }

    async fn merge_dlq_messages(&self, request: Option<&adm::MergeDLQMessagesRequest>) -> HResult<adm::MergeDLQMessagesResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        self.router.engine_for_shard(r.shard_id)?.merge_dlq_messages(r).await
    }

    async fn notify_failover_markers(&self, request: Option<&et::NotifyFailoverMarkersRequest>) -> HResult<()> {
        self.wait_started().await;
        let r = nonnil(request)?;
        self.router.receive_failover_markers(&r.failover_marker_tokens)
    }

    async fn poll_mutable_state(&self, request: Option<&et::PollMutableStateRequest>) -> HResult<et::PollMutableStateResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let engine = self.engine(wf_id(&r.execution))?;
        engine.poll_mutable_state(r, None).await
    }

    async fn purge_dlq_messages(&self, request: Option<&adm::PurgeDLQMessagesRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        self.router.engine_for_shard(r.shard_id)?.purge_dlq_messages(r).await
    }

    async fn query_workflow(&self, request: Option<&et::HistoryQueryWorkflowRequest>) -> HResult<et::HistoryQueryWorkflowResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let wid = r.request.as_ref().map(|q| wf_id(&q.execution)).unwrap_or("");
        let engine = self.engine(wid)?;
        engine.query_workflow(r, None).await
    }

    async fn read_dlq_messages(&self, request: Option<&adm::ReadDLQMessagesRequest>) -> HResult<adm::ReadDLQMessagesResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        self.router.engine_for_shard(r.shard_id)?.read_dlq_messages(r).await
    }

    async fn reapply_events(&self, request: Option<&et::HistoryReapplyEventsRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        // TODO(D4-remaining): HistoryEngine::reapply_events is not ported.
        Err(not_ported("ReapplyEvents"))
    }

    async fn record_activity_task_heartbeat(&self, request: Option<&et::HistoryRecordActivityTaskHeartbeatRequest>) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let hb = r.heartbeat_request.as_ref().ok_or_else(err_nil_request)?;
        let token = validate_task_token(&hb.task_token)?;
        self.engine(&token.workflow_id)?.record_activity_task_heartbeat(r).await
    }

    async fn record_activity_task_started(&self, request: Option<&et::RecordActivityTaskStartedRequest>) -> HResult<et::RecordActivityTaskStartedResponse> {
        // Go does not check shuttingDown here.
        self.wait_started().await;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        self.engine(wf_id(&r.workflow_execution))?.record_activity_task_started(r).await
    }

    async fn record_child_execution_completed(&self, request: Option<&et::RecordChildExecutionCompletedRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let exec = r.workflow_execution.as_ref().ok_or_else(err_execution_not_set)?;
        if exec.workflow_id.is_empty() {
            return Err(err_workflow_id_not_set());
        }
        self.engine(&exec.workflow_id)?.record_child_execution_completed(r).await
    }

    async fn record_decision_task_started(&self, request: Option<&et::RecordDecisionTaskStartedRequest>) -> HResult<et::RecordDecisionTaskStartedResponse> {
        self.wait_started().await;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        self.engine(wf_id(&r.workflow_execution))?.record_decision_task_started(r).await
    }

    async fn refresh_workflow_tasks(&self, request: Option<&et::HistoryRefreshWorkflowTasksRequest>) -> HResult<()> {
        if self.is_shutting_down() {
            return Err(err_shutting_down());
        }
        let r = nonnil(request)?;
        let execution = r.request.as_ref().and_then(|q| q.workflow_execution.clone()).unwrap_or_default();
        let engine = self.engine(&execution.workflow_id)?;
        engine.refresh_workflow_tasks(&r.domain_uuid, &execution).await
    }

    async fn remove_signal_mutable_state(&self, request: Option<&et::RemoveSignalMutableStateRequest>) -> HResult<()> {
        route!(self, request, r => (&r.domain_uuid, wf_id(&r.workflow_execution).to_string()), e => e.remove_signal_mutable_state(r))
    }

    async fn remove_task(&self, request: Option<&adm::RemoveTaskRequest>) -> HResult<()> {
        self.router.remove_task(nonnil(request)?).await
    }

    async fn replicate_events_v2(&self, request: Option<&et::ReplicateEventsV2Request>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() || !is_uuid(&r.domain_uuid) {
            return Err(err_domain_not_set());
        }
        let exec = r.workflow_execution.as_ref().ok_or_else(err_execution_not_set)?;
        if exec.workflow_id.is_empty() {
            return Err(err_workflow_id_not_set());
        }
        if exec.run_id.is_empty() || !is_uuid(&exec.run_id) {
            return Err(err_run_id_not_valid());
        }
        self.engine(&exec.workflow_id)?.replicate_events_v2(r).await
    }

    async fn request_cancel_workflow_execution(&self, request: Option<&et::HistoryRequestCancelWorkflowExecutionRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let wid = r.cancel_request.as_ref().map(|c| wf_id(&c.workflow_execution)).unwrap_or("");
        self.engine(wid)?.request_cancel_workflow_execution(r).await
    }

    async fn reset_queue(&self, request: Option<&adm::ResetQueueRequest>) -> HResult<()> {
        self.wait_started().await;
        let r = nonnil(request)?;
        let e = self.router.engine_for_shard(r.shard_id)?;
        match r.task_type {
            Some(2) => e.reset_transfer_queue(&r.cluster_name).await,
            Some(3) => e.reset_timer_queue(&r.cluster_name).await,
            _ => Err(HistoryError::bad_request("Invalid task type")),
        }
    }

    async fn reset_sticky_task_list(&self, request: Option<&et::HistoryResetStickyTaskListRequest>) -> HResult<et::HistoryResetStickyTaskListResponse> {
        route!(self, request, r => (&r.domain_uuid, wf_id(&r.execution).to_string()), e => e.reset_sticky_task_list(r))
    }

    async fn reset_workflow_execution(&self, request: Option<&et::HistoryResetWorkflowExecutionRequest>) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let wid = r.reset_request.as_ref().map(|c| wf_id(&c.workflow_execution)).unwrap_or("");
        self.engine(wid)?.reset_workflow_execution(r).await
    }

    async fn respond_activity_task_canceled(&self, request: Option<&et::HistoryRespondActivityTaskCanceledRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let c = r.cancel_request.as_ref().ok_or_else(err_nil_request)?;
        let token = validate_task_token(&c.task_token)?;
        self.engine(&token.workflow_id)?.respond_activity_task_canceled(r).await
    }

    async fn respond_activity_task_completed(&self, request: Option<&et::HistoryRespondActivityTaskCompletedRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let c = r.complete_request.as_ref().ok_or_else(err_nil_request)?;
        let token = validate_task_token(&c.task_token)?;
        self.engine(&token.workflow_id)?.respond_activity_task_completed(r).await
    }

    async fn respond_activity_task_failed(&self, request: Option<&et::HistoryRespondActivityTaskFailedRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let c = r.failed_request.as_ref().ok_or_else(err_nil_request)?;
        let token = validate_task_token(&c.task_token)?;
        self.engine(&token.workflow_id)?.respond_activity_task_failed(r).await
    }

    async fn respond_cross_cluster_tasks_completed(&self, _request: Option<&adm::RespondCrossClusterTasksCompletedRequest>) -> HResult<adm::RespondCrossClusterTasksCompletedResponse> {
        Err(HistoryError::bad_request("The cross-cluster feature has been deprecated"))
    }

    async fn respond_decision_task_completed(&self, request: Option<&et::HistoryRespondDecisionTaskCompletedRequest>) -> HResult<et::HistoryRespondDecisionTaskCompletedResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let c = r.complete_request.as_ref().ok_or_else(err_nil_request)?;
        let token = validate_task_token(&c.task_token)?;
        self.engine(&token.workflow_id)?.respond_decision_task_completed(r).await
    }

    async fn respond_decision_task_failed(&self, request: Option<&et::HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let c = r.failed_request.as_ref().ok_or_else(err_nil_request)?;
        let token = validate_task_token(&c.task_token)?;
        self.engine(&token.workflow_id)?.respond_decision_task_failed(r).await
    }

    async fn schedule_decision_task(&self, request: Option<&et::ScheduleDecisionTaskRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let exec = r.workflow_execution.as_ref().ok_or_else(err_execution_not_set)?;
        self.engine(&exec.workflow_id)?.schedule_decision_task(r).await
    }

    async fn signal_with_start_workflow_execution(&self, request: Option<&et::HistorySignalWithStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let s = r.signal_with_start_request.as_ref().ok_or_else(err_nil_request)?;
        let _engine = self.engine(&s.workflow_id)?;
        // TODO(D4-remaining): HistoryEngine::signal_with_start_workflow_execution is not ported. Once it exists,
        // retry once on PersistenceAlreadyStarted / CurrentWorkflowConditionFailed exactly as handler.go:1140-1157.
        Err(not_ported("SignalWithStartWorkflowExecution"))
    }

    async fn signal_workflow_execution(&self, request: Option<&et::HistorySignalWorkflowExecutionRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let wid = r.signal_request.as_ref().map(|c| wf_id(&c.workflow_execution)).unwrap_or("");
        self.engine(wid)?.signal_workflow_execution(r).await
    }

    async fn start_workflow_execution(&self, request: Option<&et::HistoryStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let s = r.start_request.as_ref().ok_or_else(err_nil_request)?;
        let _engine = self.engine(&s.workflow_id)?;
        // TODO(D4-remaining): HistoryEngine::start_workflow_execution is not ported.
        Err(not_ported("StartWorkflowExecution"))
    }

    async fn sync_activity(&self, request: Option<&et::SyncActivityRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_id.is_empty() || !is_uuid(&r.domain_id) {
            return Err(err_domain_not_set());
        }
        if r.workflow_id.is_empty() {
            return Err(err_workflow_id_not_set());
        }
        if r.run_id.is_empty() || !is_uuid(&r.run_id) {
            return Err(err_run_id_not_valid());
        }
        self.engine(&r.workflow_id)?.sync_activity(r).await
    }

    async fn sync_shard_status(&self, request: Option<&et::SyncShardStatusRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.source_cluster.is_empty() {
            return Err(err_source_cluster_not_set());
        }
        if r.timestamp.is_none() {
            return Err(err_timestamp_not_set());
        }
        self.router.engine_for_shard(r.shard_id as i32)?.sync_shard_status(r).await
    }

    async fn terminate_workflow_execution(&self, request: Option<&et::HistoryTerminateWorkflowExecutionRequest>) -> HResult<()> {
        self.gate().await?;
        let r = nonnil(request)?;
        if r.domain_uuid.is_empty() {
            return Err(err_domain_not_set());
        }
        let wid = r.terminate_request.as_ref().map(|c| wf_id(&c.workflow_execution)).unwrap_or("");
        self.engine(wid)?.terminate_workflow_execution(r).await
    }

    async fn get_failover_info(&self, request: Option<&et::GetFailoverInfoRequest>) -> HResult<et::GetFailoverInfoResponse> {
        self.gate().await?;
        self.router.get_failover_info(nonnil(request)?).await
    }

    async fn ratelimit_update(&self, request: Option<&et::RatelimitUpdateRequest>) -> HResult<et::RatelimitUpdateResponse> {
        self.gate().await?;
        self.router.ratelimit_update(nonnil(request)?).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoRouter;
    #[async_trait]
    impl EngineRouter for NoRouter {
        fn engine_for_workflow(&self, _: &str) -> HResult<Arc<HistoryEngine>> {
            Err(HistoryError::internal("no engine"))
        }
        fn engine_for_shard(&self, _: i32) -> HResult<Arc<HistoryEngine>> {
            Err(HistoryError::internal("no engine"))
        }
        fn close_shard(&self, _: i32) {}
        fn shard_ids(&self) -> Vec<i32> {
            vec![]
        }
        fn prepare_to_stop(&self) {}
        fn describe_host(&self, _: &adm::DescribeHistoryHostRequest) -> HResult<adm::DescribeHistoryHostResponse> {
            Ok(Default::default())
        }
        async fn remove_task(&self, _: &adm::RemoveTaskRequest) -> HResult<()> {
            Ok(())
        }
        fn receive_failover_markers(&self, _: &[et::FailoverMarkerToken]) -> HResult<()> {
            Ok(())
        }
        async fn get_failover_info(&self, _: &et::GetFailoverInfoRequest) -> HResult<et::GetFailoverInfoResponse> {
            Ok(Default::default())
        }
        async fn ratelimit_update(&self, _: &et::RatelimitUpdateRequest) -> HResult<et::RatelimitUpdateResponse> {
            Ok(Default::default())
        }
        fn max_response_size(&self) -> usize {
            1 << 20
        }
    }

    fn handler() -> HistoryHandler {
        let h = HistoryHandler::new(Arc::new(NoRouter)).with_ownership_transfer_delay(Duration::from_millis(1));
        h.start();
        h
    }

    #[tokio::test]
    async fn health_ok() {
        let s = handler().health().await.unwrap();
        assert!(s.ok && s.msg == "OK");
    }

    #[tokio::test]
    async fn nil_request_is_error() {
        assert!(handler().get_mutable_state(None).await.is_err());
    }

    #[tokio::test]
    async fn domain_not_set() {
        let req = et::GetMutableStateRequest::default();
        let e = handler().get_mutable_state(Some(&req)).await.unwrap_err();
        assert!(e.to_string().contains("Domain not set"));
    }

    #[tokio::test]
    async fn sync_shard_status_validation() {
        let h = handler();
        let mut req = et::SyncShardStatusRequest::default();
        assert!(h.sync_shard_status(Some(&req)).await.unwrap_err().to_string().contains("Source Cluster"));
        req.source_cluster = "c".into();
        assert!(h.sync_shard_status(Some(&req)).await.unwrap_err().to_string().contains("Timestamp"));
    }

    #[tokio::test]
    async fn shutting_down_rejects_requests() {
        let h = handler();
        let rem = h.prepare_to_stop(Duration::from_secs(10)).await;
        assert!(rem <= Duration::from_secs(10));
        let req = et::GetMutableStateRequest::default();
        let e = h.get_mutable_state(Some(&req)).await.unwrap_err();
        assert!(e.to_string().contains("Shutting down"));
    }

    #[tokio::test]
    async fn invalid_task_token_rejected() {
        let req = et::HistoryRespondActivityTaskCompletedRequest {
            domain_uuid: "d".into(),
            complete_request: Some(Default::default()),
        };
        let e = handler().respond_activity_task_completed(Some(&req)).await.unwrap_err();
        assert!(e.to_string().contains("task token"));
    }

    #[tokio::test]
    async fn cross_cluster_deprecated() {
        assert!(handler().get_cross_cluster_tasks(None).await.is_err());
    }
}
