//! Replication fan-out and response-budget policy from handler/handler.go.
use std::{cmp::Ordering, collections::HashMap, sync::Arc};
use async_trait::async_trait;
use cadence_api::{admin::*, types::ReplicationTask};
use futures_util::future::join_all;
use crate::engine::deps::HResult;

pub struct ReplicationShardMessages {
    pub messages: ReplicationMessages,
    pub shard_id: i32,
    /// Exact proto.FromReplicationMessages(messages).Size(), not JSON size.
    pub size: usize,
    pub earliest_creation_time: Option<i64>,
}
#[async_trait]
pub trait ReplicationEngine: Send + Sync {
    async fn get_replication_messages(&self, cluster: &str, last_read: i64) -> HResult<ReplicationShardMessages>;
    async fn get_dlq_replication_messages(&self, infos: &[ReplicationTaskInfo]) -> HResult<Vec<ReplicationTask>>;
}
/// TODO(link): shard controller adapters and protobuf payload size mapper.
pub trait ReplicationController: Send + Sync {
    fn engine_for_shard(&self, shard: i32) -> HResult<Arc<dyn ReplicationEngine>>;
    fn engine_for_workflow(&self, workflow: &str) -> HResult<Arc<dyn ReplicationEngine>>;
    fn message_too_large(&self, shard: i32);
}

pub async fn get_replication_shard_messages(controller: &dyn ReplicationController, request: &GetReplicationMessagesRequest) -> Vec<ReplicationShardMessages> {
    let responses = join_all(request.tokens.iter().map(|token| async move {
        let engine = match controller.engine_for_shard(token.shard_id) {
            Ok(engine) => engine,
            Err(error) => { tracing::warn!(error = %error, "History engine not found for shard"); return None; }
        };
        match engine.get_replication_messages(&request.cluster_name, token.last_retrieved_message_id).await {
            Ok(mut messages) => { messages.shard_id = token.shard_id; Some(messages) }
            Err(error) => { tracing::warn!(error = %error, "Failed to get replication tasks for shard"); None }
        }
    })).await;
    responses.into_iter().flatten().collect()
}

pub fn cmp_replication_shard_messages(a: &ReplicationShardMessages, b: &ReplicationShardMessages) -> Ordering {
    // Go's comparator returns +1 even when both times are nil. Preserve the
    // resulting rule that unknown creation times are visited after known ones.
    match (a.earliest_creation_time, b.earliest_creation_time) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a_time), Some(b_time)) => a_time.cmp(&b_time).then_with(|| a.size.cmp(&b.size)),
    }
}
pub fn sort_replication_shard_messages(messages: &mut [ReplicationShardMessages]) {
    messages.sort_by(cmp_replication_shard_messages);
}

pub fn build_get_replication_messages_response(controller: &dyn ReplicationController,
    max_response_size: usize, mut messages: Vec<ReplicationShardMessages>) -> GetReplicationMessagesResponse {
    sort_replication_shard_messages(&mut messages);
    let mut response_size = 0usize;
    let mut by_shard = HashMap::with_capacity(messages.len());
    for message in messages {
        if response_size.saturating_add(message.size) >= max_response_size {
            controller.message_too_large(message.shard_id);
            tracing::warn!(shard_id = message.shard_id, response_size = message.size, response_total_size = response_size,
                response_max_size = max_response_size, "Replication messages did not fit in the response (history host)");
            continue;
        }
        response_size += message.size;
        by_shard.insert(message.shard_id, message.messages);
    }
    GetReplicationMessagesResponse { messages_by_shard: Some(by_shard) }
}

pub async fn get_dlq_replication_messages(controller: &dyn ReplicationController, request: &GetDLQReplicationMessagesRequest) -> GetDLQReplicationMessagesResponse {
    let mut by_execution: HashMap<(&str, &str, &str), Vec<ReplicationTaskInfo>> = HashMap::new();
    for info in &request.task_infos {
        by_execution.entry((&info.domain_id, &info.workflow_id, &info.run_id)).or_default().push(info.clone());
    }
    let results = join_all(by_execution.into_values().map(|infos| async move {
        let Some(first) = infos.first() else { return Vec::new(); };
        let engine = match controller.engine_for_workflow(&first.workflow_id) {
            Ok(engine) => engine,
            Err(error) => { tracing::warn!(error = %error, "History engine not found for workflow ID."); return Vec::new(); }
        };
        match engine.get_dlq_replication_messages(&infos).await {
            Ok(tasks) => tasks,
            Err(error) => { tracing::error!(error = %error, "Failed to get dlq replication tasks."); Vec::new() }
        }
    })).await;
    GetDLQReplicationMessagesResponse { replication_tasks: Some(results.into_iter().flatten().collect()) }
}
