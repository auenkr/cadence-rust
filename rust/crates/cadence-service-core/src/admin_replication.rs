//! History-proxied replication admin APIs. Mirrors the Go admin handler:
//! GetReplicationMessages, GetDLQReplicationMessages and the replication-type
//! branches of ReadDLQMessages / PurgeDLQMessages / MergeDLQMessages.
use cadence_api::admin as api_admin;
use cadence_proto::history as ph;
use cadence_proto::history::history_api_client::HistoryApiClient;

use crate::admin::history_admin_request;
use crate::handlers::AppState;
use crate::replication_convert as convert;

/// Go `constants.ReadDLQMessagesPageSize`.
const READ_DLQ_PAGE_SIZE: i32 = 1_000;
/// Go `constants.InclusiveEndMessageID`.
const INCLUSIVE_END_MESSAGE_ID: i64 = i64::MAX - 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyError {
    BadRequest(String),
    Internal(String),
}

impl ProxyError {
    pub fn message(&self) -> &str {
        match self {
            ProxyError::BadRequest(message) | ProxyError::Internal(message) => message,
        }
    }
}

fn internal(error: tonic::Status) -> ProxyError {
    ProxyError::Internal(error.message().to_string())
}

fn client(state: &AppState) -> Result<HistoryApiClient<tonic::transport::Channel>, ProxyError> {
    let channel = state
        .history_admin_channel
        .clone()
        .ok_or_else(|| ProxyError::Internal("History client is not configured.".to_string()))?;
    Ok(HistoryApiClient::new(channel))
}

pub async fn get_replication_messages(
    state: &AppState,
    request: api_admin::GetReplicationMessagesRequest,
) -> Result<api_admin::GetReplicationMessagesResponse, ProxyError> {
    if request.cluster_name.is_empty() {
        return Err(ProxyError::BadRequest(
            "Cluster name is not set.".to_string(),
        ));
    }
    let mut history = client(state)?;
    let response = history
        .get_replication_messages(history_admin_request(ph::GetReplicationMessagesRequest {
            tokens: request
                .tokens
                .into_iter()
                .map(convert::replication_token_out)
                .collect(),
            cluster_name: request.cluster_name,
        }))
        .await
        .map_err(internal)?
        .into_inner();
    let mut messages_by_shard = std::collections::HashMap::new();
    for (shard, messages) in response.shard_messages {
        messages_by_shard.insert(
            shard,
            convert::replication_messages_in(messages).map_err(ProxyError::Internal)?,
        );
    }
    Ok(api_admin::GetReplicationMessagesResponse {
        messages_by_shard: Some(messages_by_shard),
    })
}

pub async fn get_dlq_replication_messages(
    state: &AppState,
    request: api_admin::GetDLQReplicationMessagesRequest,
) -> Result<api_admin::GetDLQReplicationMessagesResponse, ProxyError> {
    if request.task_infos.is_empty() {
        return Err(ProxyError::BadRequest(
            "Replication task info is not set.".to_string(),
        ));
    }
    let mut history = client(state)?;
    let response = history
        .get_dlq_replication_messages(history_admin_request(
            ph::GetDlqReplicationMessagesRequest {
                task_infos: request
                    .task_infos
                    .into_iter()
                    .map(convert::replication_task_info_out)
                    .collect(),
            },
        ))
        .await
        .map_err(internal)?
        .into_inner();
    Ok(api_admin::GetDLQReplicationMessagesResponse {
        replication_tasks: Some(
            convert::replication_tasks_in(response.replication_tasks)
                .map_err(ProxyError::Internal)?,
        ),
    })
}

pub async fn count_dlq_messages(
    state: &AppState,
    request: api_admin::CountDLQMessagesRequest,
) -> Result<api_admin::CountDLQMessagesResponse, ProxyError> {
    let persistence = state.domain_metadata_persistence.as_ref().ok_or_else(|| {
        ProxyError::Internal(
            "domain replication DLQ is unavailable for this persistence backend".to_string(),
        )
    })?;
    let domain = persistence
        .domain_replication_dlq_size()
        .map_err(ProxyError::Internal)?;
    let mut history = client(state)?;
    let response = history
        .count_dlq_messages(history_admin_request(ph::CountDlqMessagesRequest {
            force_fetch: request.force_fetch,
        }))
        .await
        .map_err(internal)?
        .into_inner();
    Ok(api_admin::CountDLQMessagesResponse {
        history: crate::admin::history_dlq_count_entries(
            response
                .entries
                .into_iter()
                .map(|entry| (entry.source_cluster, entry.count)),
        ),
        domain,
    })
}

pub async fn read_dlq_messages(
    state: &AppState,
    request: api_admin::ReadDLQMessagesRequest,
) -> Result<api_admin::ReadDLQMessagesResponse, ProxyError> {
    let mut history = client(state)?;
    let maximum_page_size = if request.maximum_page_size <= 0 {
        READ_DLQ_PAGE_SIZE
    } else {
        request.maximum_page_size
    };
    let response = history
        .read_dlq_messages(history_admin_request(ph::ReadDlqMessagesRequest {
            r#type: request.dlq_type.map_or(0, convert::dlq_type_out),
            shard_id: request.shard_id,
            source_cluster: request.source_cluster,
            inclusive_end_message_id: Some(
                request
                    .inclusive_end_message_id
                    .unwrap_or(INCLUSIVE_END_MESSAGE_ID),
            ),
            page_size: maximum_page_size,
            next_page_token: request.next_page_token,
        }))
        .await
        .map_err(internal)?
        .into_inner();
    Ok(api_admin::ReadDLQMessagesResponse {
        dlq_type: convert::dlq_type_in(response.r#type).map_err(ProxyError::Internal)?,
        replication_tasks: convert::replication_tasks_in(response.replication_tasks)
            .map_err(ProxyError::Internal)?,
        next_page_token: response.next_page_token,
    })
}

pub async fn purge_dlq_messages(
    state: &AppState,
    request: api_admin::PurgeDLQMessagesRequest,
) -> Result<api_admin::PurgeDLQMessagesResponse, ProxyError> {
    let mut history = client(state)?;
    history
        .purge_dlq_messages(history_admin_request(ph::PurgeDlqMessagesRequest {
            r#type: request.dlq_type.map_or(0, convert::dlq_type_out),
            shard_id: request.shard_id,
            source_cluster: request.source_cluster,
            inclusive_end_message_id: Some(
                request
                    .inclusive_end_message_id
                    .unwrap_or(INCLUSIVE_END_MESSAGE_ID),
            ),
        }))
        .await
        .map_err(internal)?;
    Ok(api_admin::PurgeDLQMessagesResponse {})
}

pub async fn merge_dlq_messages(
    state: &AppState,
    request: api_admin::MergeDLQMessagesRequest,
) -> Result<api_admin::MergeDLQMessagesResponse, ProxyError> {
    let mut history = client(state)?;
    let response = history
        .merge_dlq_messages(history_admin_request(ph::MergeDlqMessagesRequest {
            r#type: request.dlq_type.map_or(0, convert::dlq_type_out),
            shard_id: request.shard_id,
            source_cluster: request.source_cluster,
            inclusive_end_message_id: Some(
                request
                    .inclusive_end_message_id
                    .unwrap_or(INCLUSIVE_END_MESSAGE_ID),
            ),
            page_size: request.maximum_page_size,
            next_page_token: request.next_page_token,
        }))
        .await
        .map_err(internal)?
        .into_inner();
    Ok(api_admin::MergeDLQMessagesResponse {
        next_page_token: response.next_page_token,
    })
}

/// Go `GetCrossClusterTasks`: validate then forward to History. The Rust API
/// model carries no cross-cluster task payloads, so a non-empty
/// `tasksByShard` from History is reported as an Internal error rather than
/// silently dropped; failure causes are forwarded.
pub async fn get_cross_cluster_tasks(
    state: &AppState,
    request: api_admin::GetCrossClusterTasksRequest,
) -> Result<serde_json::Value, ProxyError> {
    if request.target_cluster.is_empty() {
        return Err(ProxyError::BadRequest(
            "Target cluster is not set on request.".to_string(),
        ));
    }
    let mut history = client(state)?;
    let response = history
        .get_cross_cluster_tasks(history_admin_request(ph::GetCrossClusterTasksRequest {
            shard_ids: request.shard_ids,
            target_cluster: request.target_cluster,
        }))
        .await
        .map_err(internal)?
        .into_inner();
    cross_cluster_tasks_json(response)
}

fn cross_cluster_tasks_json(
    response: ph::GetCrossClusterTasksResponse,
) -> Result<serde_json::Value, ProxyError> {
    if response
        .tasks_by_shard
        .values()
        .any(|tasks| !tasks.task_requests.is_empty())
    {
        return Err(ProxyError::Internal(
            "cross-cluster task payload conversion is not supported".to_string(),
        ));
    }
    let causes: std::collections::BTreeMap<String, serde_json::Value> = response
        .failed_cause_by_shard
        .into_iter()
        .map(|(shard, cause)| {
            let name = match cause {
                1 => "SERVICE_BUSY",
                2 => "TIMEOUT",
                3 => "SHARD_OWNERSHIP_LOST",
                _ => "UNCATEGORIZED",
            };
            (shard.to_string(), serde_json::json!(name))
        })
        .collect();
    Ok(serde_json::json!({ "tasksByShard": {}, "failedCauseByShard": causes }))
}

/// Go `RespondCrossClusterTasksCompleted`: validate then forward to History.
pub async fn respond_cross_cluster_tasks_completed(
    state: &AppState,
    request: api_admin::RespondCrossClusterTasksCompletedRequest,
) -> Result<api_admin::RespondCrossClusterTasksCompletedResponse, ProxyError> {
    if request.target_cluster.is_empty() {
        return Err(ProxyError::BadRequest(
            "Target cluster is not set on request.".to_string(),
        ));
    }
    let mut history = client(state)?;
    history
        .respond_cross_cluster_tasks_completed(history_admin_request(
            ph::RespondCrossClusterTasksCompletedRequest {
                shard_id: request.shard_id,
                target_cluster: request.target_cluster,
                task_responses: vec![],
                fetch_new_tasks: request.fetch_new_tasks,
            },
        ))
        .await
        .map_err(internal)?;
    Ok(api_admin::RespondCrossClusterTasksCompletedResponse {})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cross_cluster_requests_validate_then_need_history() {
        let state = AppState::default();
        assert_eq!(
            get_cross_cluster_tasks(&state, Default::default())
                .await
                .unwrap_err(),
            ProxyError::BadRequest("Target cluster is not set on request.".into())
        );
        let req = api_admin::RespondCrossClusterTasksCompletedRequest {
            target_cluster: "c".into(),
            ..Default::default()
        };
        assert_eq!(
            respond_cross_cluster_tasks_completed(&state, req)
                .await
                .unwrap_err(),
            ProxyError::Internal("History client is not configured.".into())
        );
    }

    #[test]
    fn cross_cluster_failed_causes_are_named() {
        let mut response = ph::GetCrossClusterTasksResponse::default();
        response.failed_cause_by_shard.insert(7, 3);
        let value = cross_cluster_tasks_json(response).unwrap();
        assert_eq!(value["failedCauseByShard"]["7"], "SHARD_OWNERSHIP_LOST");
    }

    #[tokio::test]
    async fn requests_are_validated_before_history_is_needed() {
        let state = AppState::default();
        assert_eq!(
            get_replication_messages(&state, Default::default())
                .await
                .unwrap_err(),
            ProxyError::BadRequest("Cluster name is not set.".into())
        );
        assert_eq!(
            get_dlq_replication_messages(&state, Default::default())
                .await
                .unwrap_err(),
            ProxyError::BadRequest("Replication task info is not set.".into())
        );
        // Valid requests reach the (unconfigured) History client.
        assert_eq!(
            purge_dlq_messages(&state, Default::default())
                .await
                .unwrap_err(),
            ProxyError::Internal("History client is not configured.".into())
        );
    }
}
