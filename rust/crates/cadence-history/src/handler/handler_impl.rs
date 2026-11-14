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
use crate::task::deps::MetricsScope as _;
use crate::wiring::assembly::controller::ShardRegistry;
use crate::wiring::resources::metrics::Scope;
use crate::wiring::server::limiter::RateLimiter;

/// Go `shardOwnershipTransferDelay`.
const SHARD_OWNERSHIP_TRANSFER_DELAY: Duration = Duration::from_secs(5);

pub struct HandlerImpl {
    registry: Arc<ShardRegistry>,
    rate_limiter: RateLimiter,
    shutting_down: AtomicBool,
    started: AtomicBool,
    ratelimit_aggregator: crate::ratelimit_aggregator::RequestWeighted,
}

/// Go `commonconstants.TaskType{Transfer,Timer,Replication}`.
const TASK_TYPE_TRANSFER: i32 = 2;
const TASK_TYPE_TIMER: i32 = 3;
const TASK_TYPE_REPLICATION: i32 = 4;

fn map_persistence_error(e: cadence_persistence::history_contract::PersistenceError) -> HistoryError {
    use cadence_persistence::history_contract::PersistenceError as P;
    match e {
        P::EntityNotExists(m) => HistoryError::entity_not_exists(m),
        P::InvalidArgument(m) | P::InvalidPersistenceRequest(m) => HistoryError::bad_request(m),
        other => HistoryError::internal(format!("{other:?}")),
    }
}

/// Go `replicationShardMessages`.
pub(crate) struct ReplicationShardMessages {
    messages: adm::ReplicationMessages,
    shard_id: i32,
    size: usize,
    earliest_creation_time: Option<i64>,
}

impl ReplicationShardMessages {
    pub(crate) fn new(shard_id: i32, messages: adm::ReplicationMessages) -> Self {
        use prost::Message;
        let size = cadence_service_core::replication_convert::replication_messages_out(messages.clone()).encoded_len();
        // Go `ReplicationMessages.GetEarliestCreationTime`: min creation time over tasks.
        let earliest_creation_time = messages.replication_tasks.iter().filter_map(|t| t.creation_time).min();
        Self { messages, shard_id, size, earliest_creation_time }
    }
}

/// Go `cmpReplicationShardMessages`: nil creation times sort last, then by time, then by size.
fn cmp_replication_shard_messages(a: &ReplicationShardMessages, b: &ReplicationShardMessages) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.earliest_creation_time, b.earliest_creation_time) {
        (None, _) => Greater,
        (_, None) => Less,
        (Some(x), Some(y)) => x.cmp(&y).then(a.size.cmp(&b.size)),
    }
}

/// Go `buildGetReplicationMessagesResponse`.
pub(crate) fn build_get_replication_messages_response(mut msgs: Vec<ReplicationShardMessages>, max_response_size: usize, on_too_large: &dyn Fn(i32)) -> adm::GetReplicationMessagesResponse {
    msgs.sort_by(cmp_replication_shard_messages);
    let mut response_size = 0usize;
    let mut by_shard = std::collections::HashMap::with_capacity(msgs.len());
    for m in msgs {
        if response_size + m.size >= max_response_size {
            on_too_large(m.shard_id);
            tracing::warn!(shard_id = m.shard_id, size = m.size, total = response_size, max = max_response_size,
                "Replication messages did not fit in the response (history host)");
            continue;
        }
        response_size += m.size;
        by_shard.insert(m.shard_id, m.messages);
    }
    adm::GetReplicationMessagesResponse { messages_by_shard: Some(by_shard) }
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

fn signal_with_start_retryable(error: &HistoryError) -> bool {
    matches!(
        error,
        HistoryError::PersistenceAlreadyStarted(_) | HistoryError::CurrentWorkflowConditionFailed(_)
    )
}

impl HandlerImpl {
    pub fn new(registry: Arc<ShardRegistry>, rps: f64) -> Self {
        let cfg = registry.env().config.clone();
        let ratelimit_aggregator = crate::ratelimit_aggregator::RequestWeighted::new(crate::ratelimit_aggregator::Config {
            new_data_weight: cfg.global_ratelimiter_new_data_weight.clone(),
            update_interval: cfg.global_ratelimiter_update_interval.clone(),
            decay_after: cfg.global_ratelimiter_decay_after.clone(),
            gc_after: cfg.global_ratelimiter_gc_after.clone(),
        })
        .expect("invalid ratelimit algorithm config")
        .with_metrics(registry.env().metrics_registry.clone());
        Self {
            ratelimit_aggregator,
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

    fn handler_scope(&self, name: &str) -> Scope {
        Scope::new(self.registry.env().metrics_registry.clone(), name, vec![])
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
        // Go handler.DescribeHistoryHost: total shard count, owned shard ids, domain cache
        // sizes, shard controller status and this host's address.
        let env = self.registry.env();
        let ids = self.registry.owned_shard_ids();
        let (by_id, by_name) = env.matching_domain_cache.get_cache_size();
        let status = if self.shutting_down.load(Ordering::SeqCst) {
            "stopped"
        } else if self.started.load(Ordering::SeqCst) {
            "started"
        } else {
            "initialized"
        };
        let who = env.membership.who_am_i().to_string();
        let address = env.membership.members().into_iter().find(|h| h.identity() == who).map(|h| h.address).unwrap_or_default();
        Ok(adm::DescribeHistoryHostResponse {
            number_of_shards: env.config.number_of_shards as i32,
            shard_ids: ids.into_iter().map(|i| i as i32).collect(),
            domain_cache: Some(adm::DomainCacheInfo { num_of_items_in_cache_by_id: by_id as i64, num_of_items_in_cache_by_name: by_name as i64 }),
            shard_controller_status: status.to_string(),
            address,
        })
    }

    async fn describe_mutable_state(&self, request: Option<&DescribeMutableStateRequest>) -> HResult<DescribeMutableStateResponse> {
        let r = nil(request)?;
        self.check_rate()?;
        let res = self.engine(wf_id(&r.execution)).await?.describe_mutable_state(r).await;
        if let Err(e) = &res {
            tracing::warn!(domain_id = %r.domain_uuid, workflow_id = wf_id(&r.execution), error = %e, "DescribeMutableState failed");
        }
        res
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
        Err(HistoryError::internal("The cross-cluster feature has been deprecated."))
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
        // Go's handler logs an engine conversion/fetch error and omits that
        // workflow's tasks from the successful response.
        let tasks = match e.get_dlq_replication_messages(&r.task_infos).await {
            Ok(tasks) => tasks,
            Err(error) => {
                tracing::error!(error = %error, "Failed to get dlq replication tasks.");
                Vec::new()
            }
        };
        Ok(adm::GetDLQReplicationMessagesResponse { replication_tasks: Some(tasks) })
    }

    async fn get_mutable_state(&self, request: Option<&GetMutableStateRequest>) -> HResult<GetMutableStateResponse> {
        let r = nil(request)?;
        let e = self.prep(false, &r.domain_uuid, wf_id(&r.execution)).await?;
        e.get_mutable_state(r, None).await
    }

    async fn get_replication_messages(&self, request: Option<&adm::GetReplicationMessagesRequest>) -> HResult<adm::GetReplicationMessagesResponse> {
        // Go: no nil check beyond the getters (nil request == empty tokens).
        let default = adm::GetReplicationMessagesRequest::default();
        let r = request.unwrap_or(&default);
        self.check_shutdown()?;
        let futs = r.tokens.iter().map(|token| async move {
            let engine = match self.engine_shard(token.shard_id).await {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(error = %err, "History engine not found for shard");
                    return None;
                }
            };
            match engine.get_replication_messages(&r.cluster_name, token.last_retrieved_message_id).await {
                Ok(msgs) => Some(ReplicationShardMessages::new(token.shard_id, msgs)),
                Err(err) => {
                    tracing::warn!(error = %err, "Failed to get replication tasks for shard");
                    None
                }
            }
        });
        let msgs: Vec<ReplicationShardMessages> = futures_util::future::join_all(futs).await.into_iter().flatten().collect();
        let max = self.registry.env().config.max_response_size.max(0) as usize;
        let reg = self.registry.env().metrics_registry.clone();
        Ok(build_get_replication_messages_response(msgs, max, &|shard| {
            Scope::new(reg.clone(), "HistoryGetReplicationMessages", vec![("shard_id".to_string(), shard.to_string())])
                .inc_counter("ReplicationMessageTooLargePerShard");
        }))
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
            tracing::info!(
                shard_ids = ?token.shard_ids,
                domain_id = ?token.failover_marker.as_ref().and_then(|m| m.domain_id.clone()),
                failover_version = ?token.failover_marker.as_ref().and_then(|m| m.failover_version),
                "failover-marker-recv: NotifyFailoverMarkers applied on this history host"
            );
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
        let engine = self.prep(true, &r.domain_uuid, wf).await?;
        // Go `GetPayloadSerializer().DeserializeBatchEvents` (ThriftRW) before the engine call.
        let blob = cadence_api::types::DataBlob {
            encoding_type: Some(cadence_api::enums::EncodingType::ThriftRW),
            data: inner.events.as_ref().map(|b| b.data.clone()).unwrap_or_default(),
        };
        let events = cadence_thrift::history_blob::decode(&blob)
            .map_err(|e| HistoryError::internal(format!("DeserializeBatchEvents encoding: \"thriftrw\", error: {e}")))?;
        engine.reapply_events(&r.domain_uuid, wf, &run, &events).await
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

    async fn remove_task(&self, request: Option<&adm::RemoveTaskRequest>) -> HResult<()> {
        // Go: no nil check, no shutdown/rate checks; straight to the execution manager.
        let default = adm::RemoveTaskRequest::default();
        let r = request.unwrap_or(&default);
        let (category, ts) = match r.task_type {
            Some(TASK_TYPE_TRANSFER) => ((1, 1), 0),
            Some(TASK_TYPE_TIMER) => ((2, 2), r.visibility_timestamp.unwrap_or(0)),
            Some(TASK_TYPE_REPLICATION) => ((1, 3), 0),
            _ => return Err(constants::err_invalid_task_type().into()),
        };
        self.registry
            .env()
            .persistence
            .executions
            .complete_history_task_batch(
                r.shard_id,
                cadence_persistence::history_contract::HistoryTaskCategory { category_type: category.0, category_id: category.1 },
                vec![(r.task_id, ts)],
            )
            .await
            .map_err(map_persistence_error)
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
        Err(HistoryError::internal("The cross-cluster feature has been deprecated"))
    }

    async fn respond_decision_task_completed(&self, request: Option<&HistoryRespondDecisionTaskCompletedRequest>) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        let r = nil(request)?;
        let c = r.complete_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.check_domain_and_rate(&r.domain_uuid)?;
        if c.decisions.is_empty() {
            self.handler_scope("HistoryRespondDecisionTaskCompleted").inc_counter("EmptyCompletionDecisionsCounter");
        }
        let t = deser_token(&c.task_token)?;
        validate_task_token(&t)?;
        self.engine(&t.workflow_id).await?.respond_decision_task_completed(r).await
    }

    async fn respond_decision_task_failed(&self, request: Option<&HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> {
        let r = nil(request)?;
        let c = r.failed_request.as_ref().ok_or_else(|| HistoryError::internal("nil request"))?;
        self.check_domain_and_rate(&r.domain_uuid)?;
        let t = deser_token(&c.task_token)?;
        if c.cause == Some(cadence_api::enums::DecisionTaskFailedCause::UnhandledDecision) {
            tracing::info!(domain_id = %t.domain_id, workflow_id = %t.workflow_id, run_id = %t.run_id, "Non-Deterministic Error");
            let domain_name = crate::engine::deps::DomainCache::get_domain_name(&*self.registry.env().domain_cache, &t.domain_id);
            let tag = match domain_name {
                Ok(n) => ("domain".to_string(), n),
                Err(_) => ("domain".to_string(), "_unknown_".to_string()),
            };
            Scope::new(self.registry.env().metrics_registry.clone(), "HistoryRespondDecisionTaskFailed", vec![tag])
                .inc_counter("CadenceErrNonDeterministicCounter");
        }
        validate_task_token(&t)?;
        self.engine(&t.workflow_id).await?.respond_decision_task_failed(r).await
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

    async fn ratelimit_update(&self, request: Option<&RatelimitUpdateRequest>) -> HResult<RatelimitUpdateResponse> {
        let r = nil(request)?;
        self.check_shutdown()?;
        let any = r.any.as_ref().ok_or_else(|| HistoryError::internal("failed to map data to args: nil Any"))?;
        let arg = crate::ratelimit_aggregator::any_to_aggregator_update(&any.value_type, &any.value)
            .map_err(|e| HistoryError::internal(format!("failed to map data to args: {e}")))?;
        self.ratelimit_aggregator
            .update(&arg)
            .map_err(|e| HistoryError::internal(format!("failed to update ratelimits: {e}")))?;
        let keys: Vec<String> = arg.load.keys().cloned().collect();
        let weights = self
            .ratelimit_aggregator
            .host_usage(&arg.id, &keys)
            .map_err(|e| HistoryError::internal(format!("failed to retrieve updated weights: {e}")))?;
        Ok(RatelimitUpdateResponse {
            any: Some(crate::engine::types::Any {
                value_type: crate::ratelimit_aggregator::USAGE_QUOTAS_ANY_TYPE.to_string(),
                value: crate::ratelimit_aggregator::aggregator_weights_to_bytes(&weights),
            }),
        })
    }
}

#[cfg(test)]
mod start_handler_tests {
    use super::*;

    fn shard_msgs(shard: i32, creation: Option<i64>, tasks: usize) -> ReplicationShardMessages {
        let t = cadence_api::types::ReplicationTask { creation_time: creation, ..Default::default() };
        ReplicationShardMessages::new(shard, adm::ReplicationMessages { replication_tasks: vec![t; tasks], ..Default::default() })
    }

    #[test]
    fn replication_messages_sorted_oldest_first_nil_last() {
        let mut v = [shard_msgs(1, None, 1), shard_msgs(2, Some(20), 1), shard_msgs(3, Some(10), 1)];
        v.sort_by(cmp_replication_shard_messages);
        assert_eq!(v.iter().map(|m| m.shard_id).collect::<Vec<_>>(), vec![3, 2, 1]);
    }

    #[test]
    fn replication_messages_equal_time_smaller_first() {
        let mut v = [shard_msgs(1, Some(5), 3), shard_msgs(2, Some(5), 1)];
        v.sort_by(cmp_replication_shard_messages);
        assert_eq!(v[0].shard_id, 2);
    }

    #[test]
    fn replication_response_skips_shards_over_max_size() {
        let a = shard_msgs(1, Some(1), 1);
        let b = shard_msgs(2, Some(2), 1);
        let one = a.size;
        // fits exactly one (check is `>=`, so max = size+1 admits the first only)
        let resp = build_get_replication_messages_response(vec![b, a], one + 1, &|_| {});
        let m = resp.messages_by_shard.unwrap();
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&1));
        // `>=`: size == max is rejected
        let resp = build_get_replication_messages_response(vec![shard_msgs(1, Some(1), 1)], one, &|_| {});
        assert!(resp.messages_by_shard.unwrap().is_empty());
    }

    #[test]
    fn task_type_constants_match_go() {
        assert_eq!((TASK_TYPE_TRANSFER, TASK_TYPE_TIMER, TASK_TYPE_REPLICATION), (2, 3, 4));
    }
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
