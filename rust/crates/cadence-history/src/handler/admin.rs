//! Administrative History handler RPCs from handler/handler.go.
use super::{
    replication::{self, ReplicationController},
    runtime::HistoryHandler,
};
use crate::engine::{
    deps::{HResult, HistoryError},
    types::*,
};
use async_trait::async_trait;
use cadence_api::admin::*;
use std::sync::Arc;

/// TODO(link): host resources, failover coordinator and rate-limit RPC mapper.
#[async_trait]
pub trait AdminResources: Send + Sync {
    fn domain_cache_size(&self) -> (i64, i64);
    fn shard_controller_status(&self) -> i32;
    fn number_of_shards(&self) -> usize;
    fn host_address(&self) -> String;
    fn shard_ids(&self) -> Vec<i32>;
    fn remove_engine_for_shard(&self, shard: i32);
    async fn complete_history_task(
        &self,
        shard: i32,
        category: crate::engine::deps::HistoryTaskCategory,
        task: i64,
        timestamp: Option<i64>,
    ) -> HResult<()>;
    fn replication_controller(&self) -> &dyn ReplicationController;
    fn max_response_size(&self) -> usize;
    fn receive_failover_marker(&self, token: &FailoverMarkerToken);
    fn get_failover_info(&self, domain: &str) -> HResult<GetFailoverInfoResponse>;
    fn any_to_aggregator_update(&self, any: Option<&Any>) -> HResult<AggregatorUpdate>;
    fn update_ratelimits(&self, update: &AggregatorUpdate) -> HResult<()>;
    fn host_usage(&self, id: &str, keys: Vec<String>) -> HResult<serde_json::Value>;
    fn aggregator_weights_to_any(&self, weights: serde_json::Value) -> HResult<Any>;
}
pub struct AggregatorUpdate {
    pub id: String,
    pub load: std::collections::HashMap<String, f64>,
}
impl HistoryHandler {
    pub async fn health(&self) -> HResult<HealthStatus> {
        self.wait_started().await;
        tracing::debug!("History health check endpoint reached.");
        Ok(HealthStatus {
            ok: true,
            msg: "OK".into(),
        })
    }
    pub async fn describe_history_host(
        &self,
        _request: Option<&DescribeHistoryHostRequest>,
    ) -> HResult<DescribeHistoryHostResponse> {
        self.wait_started().await;
        let (by_id, by_name) = self.admin.domain_cache_size();
        let status = match self.admin.shard_controller_status() {
            0 => "initialized",
            1 => "started",
            2 => "stopped",
            _ => "",
        };
        Ok(DescribeHistoryHostResponse {
            number_of_shards: self.admin.number_of_shards() as i32,
            shard_ids: self.admin.shard_ids(),
            domain_cache: Some(DomainCacheInfo {
                num_of_items_in_cache_by_id: by_id,
                num_of_items_in_cache_by_name: by_name,
            }),
            shard_controller_status: status.into(),
            address: self.admin.host_address(),
        })
    }
    pub async fn close_shard(&self, request: Option<&CloseShardRequest>) -> HResult<()> {
        self.admin
            .remove_engine_for_shard(request.map(|r| r.shard_id).unwrap_or_default());
        Ok(())
    }
    pub async fn remove_task(&self, request: Option<&RemoveTaskRequest>) -> HResult<()> {
        use crate::engine::deps::HistoryTaskCategory::*;
        let request = request.cloned().unwrap_or_default();
        let category = match request.task_type.unwrap_or_default() {
            2 => Transfer,
            3 => Timer,
            4 => Replication,
            _ => return Err(HistoryError::bad_request("Invalid task type")),
        };
        self.admin
            .complete_history_task(
                request.shard_id,
                category,
                request.task_id,
                (category == Timer).then_some(request.visibility_timestamp.unwrap_or_default()),
            )
            .await
    }
    pub async fn get_replication_messages(
        &self,
        request: Option<&GetReplicationMessagesRequest>,
    ) -> HResult<GetReplicationMessagesResponse> {
        self.wait_started().await;
        tracing::debug!("Received GetReplicationMessages call.");
        let _profile = self.metrics.start_request_profile("GetReplicationMessages");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let messages = replication::get_replication_shard_messages(
            self.admin.replication_controller(),
            &request,
        )
        .await;
        let response = replication::build_get_replication_messages_response(
            self.admin.replication_controller(),
            self.admin.max_response_size(),
            messages,
        );
        tracing::debug!("GetReplicationMessages succeeded.");
        Ok(response)
    }
    pub async fn get_dlq_replication_messages(
        &self,
        request: Option<&GetDLQReplicationMessagesRequest>,
    ) -> HResult<GetDLQReplicationMessagesResponse> {
        self.wait_started().await;
        let _profile = self
            .metrics
            .start_request_profile("GetDLQReplicationMessages");
        self.check_shutdown()?;
        Ok(replication::get_dlq_replication_messages(
            self.admin.replication_controller(),
            &request.cloned().unwrap_or_default(),
        )
        .await)
    }
    pub async fn count_dlq_messages(
        &self,
        request: Option<&CountDLQMessagesRequest>,
    ) -> HResult<HistoryCountDLQMessagesResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("CountDLQMessages");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let results =
            futures_util::future::join_all(self.admin.shard_ids().into_iter().map(|shard| {
                let request = &request;
                async move {
                    let engine = self.resources.engine_for_shard(shard).map_err(|e| {
                        HistoryError::Other(format!("dlq count for shard {shard}: {e}"))
                    })?;
                    engine.count_dlq_messages(Some(request)).await.map_err(|e| {
                        HistoryError::Other(format!("dlq count for shard {shard}: {e}"))
                    })
                }
            }))
            .await;
        let mut entries = Vec::new();
        let mut failure = None;
        for result in results {
            match result {
                Ok(response) => entries.extend(response.entries),
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        if let Some(error) = failure {
            return Err(self.error("CountDLQMessages", "", "", "", error));
        }
        Ok(HistoryCountDLQMessagesResponse { entries })
    }
    pub async fn notify_failover_markers(
        &self,
        request: Option<&NotifyFailoverMarkersRequest>,
    ) -> HResult<()> {
        let _profile = self.metrics.start_request_profile("NotifyFailoverMarkers");
        if let Some(request) = request {
            for token in &request.failover_marker_tokens {
                tracing::debug!("Handling failover maker");
                self.admin.receive_failover_marker(token);
            }
        }
        Ok(())
    }
    pub async fn get_cross_cluster_tasks(
        &self,
        _request: Option<&GetCrossClusterTasksRequest>,
    ) -> HResult<GetCrossClusterTasksResponse> {
        Err(HistoryError::bad_request(
            "The cross-cluster feature has been deprecated.",
        ))
    }
    pub async fn respond_cross_cluster_tasks_completed(
        &self,
        _request: Option<&RespondCrossClusterTasksCompletedRequest>,
    ) -> HResult<RespondCrossClusterTasksCompletedResponse> {
        Err(HistoryError::bad_request(
            "The cross-cluster feature has been deprecated",
        ))
    }
    pub async fn get_failover_info(
        &self,
        request: Option<&GetFailoverInfoRequest>,
    ) -> HResult<GetFailoverInfoResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("GetFailoverInfo");
        self.check_shutdown()?;
        let domain = request.map(|r| r.domain_id.as_str()).unwrap_or_default();
        self.admin
            .get_failover_info(domain)
            .map_err(|e| self.error("GetFailoverInfo", domain, "", "", e))
    }
    pub async fn ratelimit_update(
        &self,
        request: Option<&RatelimitUpdateRequest>,
    ) -> HResult<RatelimitUpdateResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("RatelimitUpdate");
        self.check_shutdown()?;
        let wrap = |prefix: &str, e| {
            self.error(
                "RatelimitUpdate",
                "",
                "",
                "",
                HistoryError::Other(format!("{prefix}: {e}")),
            )
        };
        let arg = self
            .admin
            .any_to_aggregator_update(request.and_then(|r| r.any.as_ref()))
            .map_err(|e| wrap("failed to map data to args", e))?;
        self.admin
            .update_ratelimits(&arg)
            .map_err(|e| wrap("failed to update ratelimits", e))?;
        let weights = self
            .admin
            .host_usage(&arg.id, arg.load.keys().cloned().collect())
            .map_err(|e| wrap("failed to retrieve updated weights", e))?;
        let any = self
            .admin
            .aggregator_weights_to_any(weights)
            .map_err(|e| wrap("failed to Any-package response", e))?;
        Ok(RatelimitUpdateResponse { any: Some(any) })
    }
}
pub type AdminResourcesRef = Arc<dyn AdminResources>;
