//! History-proxied replication admin APIs. Mirrors the Go admin handler:
//! GetReplicationMessages, GetDLQReplicationMessages and the replication-type
//! branches of ReadDLQMessages / PurgeDLQMessages / MergeDLQMessages.
use cadence_api::admin as api_admin;
use cadence_proto::history as ph;
use cadence_proto::history::history_api_client::HistoryApiClient;

use crate::admin::history_admin_request;
use crate::handlers::AppState;
use crate::replication_convert as convert;
use cadence_proto::convert::cross_cluster as xc;

/// Go `constants.ReadDLQMessagesPageSize`.
const READ_DLQ_PAGE_SIZE: i32 = 1_000;
/// Go `constants.InclusiveEndMessageID`.
const INCLUSIVE_END_MESSAGE_ID: i64 = i64::MAX - 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyError {
    BadRequest(String),
    Internal(String),
    NotFound(String),
}

impl ProxyError {
    pub fn message(&self) -> &str {
        match self {
            ProxyError::BadRequest(message)
            | ProxyError::Internal(message)
            | ProxyError::NotFound(message) => message,
        }
    }
}

/// Go `ndc.defaultPageSize` / `resendContextTimeout`.
const RESEND_PAGE_SIZE: i32 = 100;
const RESEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Go `HistoryResenderImpl.createReplicationRawRequest`.
pub(crate) fn resend_replication_request(
    domain_id: &str,
    workflow_id: &str,
    run_id: &str,
    batch: cadence_proto::api::DataBlob,
    items: Vec<cadence_proto::admin::VersionHistoryItem>,
) -> ph::ReplicateEventsV2Request {
    ph::ReplicateEventsV2Request {
        domain_id: domain_id.to_owned(),
        workflow_execution: Some(cadence_proto::api::WorkflowExecution {
            workflow_id: workflow_id.to_owned(),
            run_id: run_id.to_owned(),
        }),
        version_history_items: items,
        events: Some(batch),
        new_run_events: None,
    }
}

/// Go proto mapper: a start/end bound exists only when both id and version are set.
fn version_item(
    event_id: Option<i64>,
    version: Option<i64>,
) -> Option<cadence_proto::admin::VersionHistoryItem> {
    Some(cadence_proto::admin::VersionHistoryItem {
        event_id: event_id?,
        version: version?,
    })
}

/// Go admin `ResendReplicationTasks` (`ndc.HistoryResender.SendSingleWorkflowHistory`):
/// page the remote cluster's admin `GetWorkflowExecutionRawHistoryV2` and replay each raw
/// batch into local History through `ReplicateEventsV2`. The remote is reached over gRPC;
/// the resender has no TLS for remotes (Go uses the client bean's transport config).
pub async fn resend_replication_tasks(
    state: &AppState,
    request: api_admin::ResendReplicationTasksRequest,
) -> Result<(), ProxyError> {
    let domain_name = state
        .domains
        .describe(&cadence_api::service_types::DescribeDomainRequest {
            name: None,
            uuid: Some(request.domain_id.clone()),
        })
        .map_err(|error| ProxyError::Internal(format!("getting domain: {}", error.message)))?
        .domain_info
        .map(|info| info.name)
        .unwrap_or_default();
    let remote = state
        .cluster_metadata
        .clusters
        .get(&request.remote_cluster)
        .ok_or_else(|| {
            ProxyError::Internal(format!(
                "history iterator: getting history: unknown remote cluster {:?}",
                request.remote_cluster
            ))
        })?;
    if remote.rpc_transport == "tchannel" {
        return Err(ProxyError::Internal(format!(
            "history iterator: getting history: remote cluster {:?} uses tchannel; only grpc remotes are supported",
            request.remote_cluster
        )));
    }
    let remote_channel = crate::grpc_tls::endpoint_channel(&remote.rpc_address, None)
        .map_err(|error| ProxyError::Internal(format!("getting history: {error}")))?;
    let mut remote_admin =
        cadence_proto::admin::admin_api_client::AdminApiClient::new(remote_channel);
    let mut history = client(state)?;
    let mut token: Vec<u8> = Vec::new();
    loop {
        let mut raw = tonic::Request::new(
            cadence_proto::admin::GetWorkflowExecutionRawHistoryV2Request {
                domain: domain_name.clone(),
                workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                    workflow_id: request.workflow_id.clone(),
                    run_id: request.run_id.clone(),
                }),
                start_event: version_item(request.start_event_id, request.start_version),
                end_event: version_item(request.end_event_id, request.end_version),
                page_size: RESEND_PAGE_SIZE,
                next_page_token: token.clone(),
            },
        );
        raw.set_timeout(RESEND_TIMEOUT);
        for (key, value) in [
            ("rpc-service", remote.rpc_name.as_str()),
            ("rpc-caller", "cadence-frontend"),
            ("rpc-encoding", "proto"),
        ] {
            if let Ok(value) = tonic::metadata::MetadataValue::try_from(value) {
                raw.metadata_mut().insert(key, value);
            }
        }
        let page = remote_admin
            .get_workflow_execution_raw_history_v2(raw)
            .await
            .map_err(|status| {
                ProxyError::Internal(format!(
                    "history iterator: getting history: {}",
                    status.message()
                ))
            })?
            .into_inner();
        let items = page
            .version_history
            .map(|history| history.items)
            .unwrap_or_default();
        for batch in page.history_batches {
            let mut replicate = history_admin_request(resend_replication_request(
                &request.domain_id,
                &request.workflow_id,
                &request.run_id,
                batch,
                items.clone(),
            ));
            replicate.set_timeout(RESEND_TIMEOUT);
            match history.replicate_events_v2(replicate).await {
                Ok(_) => {}
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(ProxyError::NotFound(status.message().to_string()));
                }
                Err(status) => {
                    return Err(ProxyError::Internal(format!(
                        "sending replication request: {}",
                        status.message()
                    )));
                }
            }
        }
        if page.next_page_token.is_empty() {
            return Ok(());
        }
        token = page.next_page_token;
    }
}

#[cfg(test)]
mod resend_tests {
    use super::*;

    #[test]
    fn replication_request_carries_batch_and_version_items() {
        let blob = cadence_proto::api::DataBlob {
            encoding_type: 2,
            data: vec![1, 2, 3],
        };
        let items = vec![cadence_proto::admin::VersionHistoryItem {
            event_id: 5,
            version: 7,
        }];
        let req = resend_replication_request("d", "w", "r", blob.clone(), items.clone());
        assert_eq!(req.domain_id, "d");
        let exec = req.workflow_execution.unwrap();
        assert_eq!(
            (exec.workflow_id.as_str(), exec.run_id.as_str()),
            ("w", "r")
        );
        assert_eq!(req.events, Some(blob));
        assert_eq!(req.version_history_items, items);
        assert!(req.new_run_events.is_none());
    }

    #[test]
    fn bounds_need_both_event_id_and_version() {
        assert!(version_item(Some(1), None).is_none());
        assert!(version_item(None, Some(1)).is_none());
        let item = version_item(Some(4), Some(9)).unwrap();
        assert_eq!((item.event_id, item.version), (4, 9));
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

/// Go `GetCrossClusterTasks`: validate then forward to History; task payloads
/// and failure causes are converted to the API model.
pub async fn get_cross_cluster_tasks(
    state: &AppState,
    request: api_admin::GetCrossClusterTasksRequest,
) -> Result<api_admin::GetCrossClusterTasksResponse, ProxyError> {
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
    cross_cluster_tasks_in(response)
}

fn cross_cluster_tasks_in(
    response: ph::GetCrossClusterTasksResponse,
) -> Result<api_admin::GetCrossClusterTasksResponse, ProxyError> {
    Ok(api_admin::GetCrossClusterTasksResponse {
        tasks_by_shard: xc::tasks_by_shard_in(response.tasks_by_shard).map_err(internal)?,
        failed_cause_by_shard: xc::failed_causes_in(response.failed_cause_by_shard)
            .map_err(internal)?,
    })
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
    let response = history
        .respond_cross_cluster_tasks_completed(history_admin_request(
            ph::RespondCrossClusterTasksCompletedRequest {
                shard_id: request.shard_id,
                target_cluster: request.target_cluster,
                task_responses: request
                    .task_responses
                    .into_iter()
                    .map(xc::task_response_out)
                    .collect(),
                fetch_new_tasks: request.fetch_new_tasks,
            },
        ))
        .await
        .map_err(internal)?
        .into_inner();
    Ok(api_admin::RespondCrossClusterTasksCompletedResponse {
        tasks: xc::task_requests_in(response.tasks).map_err(internal)?,
    })
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
    fn cross_cluster_payloads_are_converted() {
        let mut response = ph::GetCrossClusterTasksResponse::default();
        response.failed_cause_by_shard.insert(7, 3);
        response.tasks_by_shard.insert(
            7,
            cadence_proto::admin::CrossClusterTaskRequests {
                task_requests: vec![cadence_proto::admin::CrossClusterTaskRequest {
                    task_info: Some(cadence_proto::admin::CrossClusterTaskInfo {
                        domain_id: "d".into(),
                        task_id: 5,
                        task_type: 3,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
        );
        let value = cross_cluster_tasks_in(response).unwrap();
        assert_eq!(
            value.failed_cause_by_shard[&7],
            cadence_api::enums::GetTaskFailedCause::ShardOwnershipLost
        );
        let info = value.tasks_by_shard[&7][0].task_info.clone().unwrap();
        assert_eq!(info.task_id, 5);
        assert_eq!(
            info.task_type,
            Some(cadence_api::enums::CrossClusterTaskType::SignalExecution)
        );
        let json = serde_json::to_value(&value).unwrap();
        assert_eq!(json["failedCauseByShard"]["7"], "SHARD_OWNERSHIP_LOST");
        assert_eq!(json["tasksByShard"]["7"][0]["taskInfo"]["taskID"], 5);
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
