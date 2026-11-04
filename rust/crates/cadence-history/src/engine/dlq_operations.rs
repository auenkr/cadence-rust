//! Port of engineimpl/{dlq_operations,get_replication_messages}.go.
use super::{deps::*, runtime::*};
use cadence_api::{admin::*, enums::DLQType, types::ReplicationTask};
use std::collections::HashMap;
impl HistoryEngine {
    pub async fn count_dlq_messages(&self, force_fetch: bool) -> HResult<HashMap<String, i64>> {
        self.replication_dlq_handler
            .get_message_count(force_fetch)
            .await
    }
    pub async fn read_dlq_messages(
        &self,
        request: &ReadDLQMessagesRequest,
    ) -> HResult<ReadDLQMessagesResponse> {
        let (tasks, replication_tasks_info, next_page_token) = self
            .replication_dlq_handler
            .read_messages(
                &request.source_cluster,
                request.inclusive_end_message_id.unwrap_or_default(),
                request.maximum_page_size,
                &request.next_page_token,
            )
            .await?;
        // Unhydrated tasks must not reach the protobuf response marshaler.
        Ok(ReadDLQMessagesResponse {
            dlq_type: Some(request.dlq_type.unwrap_or(DLQType::Invalid)),
            replication_tasks: tasks.into_iter().flatten().collect(),
            replication_tasks_info,
            next_page_token,
        })
    }
    pub async fn purge_dlq_messages(&self, request: &PurgeDLQMessagesRequest) -> HResult<()> {
        self.replication_dlq_handler
            .purge_messages(
                &request.source_cluster,
                request.inclusive_end_message_id.unwrap_or_default(),
            )
            .await
    }
    pub async fn merge_dlq_messages(
        &self,
        request: &MergeDLQMessagesRequest,
    ) -> HResult<MergeDLQMessagesResponse> {
        let next_page_token = self
            .replication_dlq_handler
            .merge_messages(
                &request.source_cluster,
                request.inclusive_end_message_id.unwrap_or_default(),
                request.maximum_page_size,
                &request.next_page_token,
            )
            .await?;
        Ok(MergeDLQMessagesResponse { next_page_token })
    }
    pub async fn get_replication_messages(
        &self,
        cluster: &str,
        last_read_id: i64,
    ) -> HResult<ReplicationMessages> {
        let start = std::time::Instant::now();
        let result = self
            .replication_ack_manager
            .get_tasks(cluster, last_read_id)
            .await;
        self.metrics
            .replication_latency("GetReplicationMessages", start.elapsed());
        let mut messages = result.map_err(|err| {
            tracing::error!(error = %err, "Failed to retrieve replication messages.");
            err
        })?;
        messages.sync_shard_status = Some(SyncShardStatus {
            timestamp: Some(self.time_source.now()),
        });
        tracing::debug!(
            counter = messages.replication_tasks.len(),
            cluster_name = cluster,
            "Successfully fetched replication messages."
        );
        for task in &messages.replication_tasks {
            if tracing::enabled!(tracing::Level::DEBUG) {
                match serde_json::to_string(task) {
                    Ok(data) => tracing::debug!("Replication task: {}", data),
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to marshal replication task.")
                    }
                }
            }
        }
        Ok(messages)
    }
    pub async fn get_dlq_replication_messages(
        &self,
        infos: &[ReplicationTaskInfo],
    ) -> HResult<Vec<ReplicationTask>> {
        let start = std::time::Instant::now();
        let result = async {
            let mut tasks = Vec::with_capacity(infos.len());
            for info in infos {
                let task = convert_to_replication_task(info).map_err(|err| {
                    tracing::error!(error = %err, "Failed to convert replication task.");
                    err
                })?;
                let task = self
                    .replication_hydrator
                    .hydrate(task)
                    .await
                    .map_err(|err| {
                        tracing::error!(error = %err, "Failed to fetch DLQ replication messages.");
                        err
                    })?;
                if let Some(task) = task {
                    tasks.push(task);
                }
            }
            Ok(tasks)
        }
        .await;
        self.metrics
            .replication_latency("GetDLQReplicationMessages", start.elapsed());
        result
    }
}
pub fn convert_to_replication_task(info: &ReplicationTaskInfo) -> HResult<Task> {
    // persistence.ReplicationTaskTypeHistory=0, SyncActivity=1, FailoverMarker=2.
    match info.task_type {
        0 => Ok(Task::History {
            info: info.clone(),
            branch_token: Vec::new(),
            new_run_branch_token: Vec::new(),
        }),
        1 => Ok(Task::SyncActivity(info.clone())),
        2 => Ok(Task::FailoverMarker(info.clone())),
        other => Err(HistoryError::Other(format!(
            "unsupported task type: {other}"
        ))),
    }
}
