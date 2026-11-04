//! Shard RPC routing from handler/handler.go.
use crate::engine::deps::{HResult, HistoryError};
use super::runtime::HistoryHandler;
impl HistoryHandler {
    pub async fn describe_queue(&self, request: Option<&cadence_api::admin::DescribeQueueRequest>) -> HResult<cadence_api::admin::DescribeQueueResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("DescribeQueue");
        let request = request.cloned().unwrap_or_default();
        let fail = |error| self.error("DescribeQueue", "", "", "", error);
        let engine = self.resources.engine_for_shard(request.shard_id).map_err(&fail)?;
        if !matches!(request.task_type.unwrap_or_default(), 2 | 3) { return Err(fail(HistoryError::bad_request("Invalid task type"))); }
        engine.describe_queue(Some(&request)).await.map_err(fail)
    }
    pub async fn merge_dlq_messages(&self, request: Option<&cadence_api::admin::MergeDLQMessagesRequest>) -> HResult<cadence_api::admin::MergeDLQMessagesResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("MergeDLQMessages");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let fail = |error| self.error("MergeDLQMessages", "", "", "", error);
        let engine = self.resources.engine_for_shard(request.shard_id).map_err(&fail)?;
        engine.merge_dlq_messages(Some(&request)).await
    }
    pub async fn purge_dlq_messages(&self, request: Option<&cadence_api::admin::PurgeDLQMessagesRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("PurgeDLQMessages");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let fail = |error| self.error("PurgeDLQMessages", "", "", "", error);
        let engine = self.resources.engine_for_shard(request.shard_id).map_err(&fail)?;
        engine.purge_dlq_messages(Some(&request)).await
    }
    pub async fn read_dlq_messages(&self, request: Option<&cadence_api::admin::ReadDLQMessagesRequest>) -> HResult<cadence_api::admin::ReadDLQMessagesResponse> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("ReadDLQMessages");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let fail = |error| self.error("ReadDLQMessages", "", "", "", error);
        let engine = self.resources.engine_for_shard(request.shard_id).map_err(&fail)?;
        engine.read_dlq_messages(Some(&request)).await
    }
    pub async fn reset_queue(&self, request: Option<&cadence_api::admin::ResetQueueRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("ResetQueue");
        let request = request.cloned().unwrap_or_default();
        let fail = |error| self.error("ResetQueue", "", "", "", error);
        let engine = self.resources.engine_for_shard(request.shard_id).map_err(&fail)?;
        if !matches!(request.task_type.unwrap_or_default(), 2 | 3) { return Err(fail(HistoryError::bad_request("Invalid task type"))); }
        engine.reset_queue(Some(&request)).await.map_err(fail)
    }
    pub async fn sync_shard_status(&self, request: Option<&crate::engine::types::SyncShardStatusRequest>) -> HResult<()> {
        self.wait_started().await;
        let _profile = self.metrics.start_request_profile("SyncShardStatus");
        self.check_shutdown()?;
        let request = request.cloned().unwrap_or_default();
        let fail = |error| self.error("SyncShardStatus", "", "", "", error);
        self.check_rate().map_err(&fail)?;
        if request.source_cluster.is_empty() { return Err(fail(HistoryError::bad_request("Source Cluster not set on request."))); }
        if request.timestamp.is_none() { return Err(fail(HistoryError::bad_request("Timestamp not set on request."))); }
        let engine = self.resources.engine_for_shard(request.shard_id as i32).map_err(&fail)?;
        engine.sync_shard_status(Some(&request)).await.map_err(fail)
    }
}
