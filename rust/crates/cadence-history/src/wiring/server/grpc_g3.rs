//! gRPC handlers for the group-3 History RPCs (replication, failover, DLQ,
//! cross-cluster, host description). Free functions so the tonic trait impl in
//! `grpc.rs` can delegate without this file touching it.
use super::{errors, ServerState};
use crate::engine::types as et;
use cadence_api::enums as e;
use cadence_api::types as t;
use cadence_proto::convert as c;
use cadence_proto::history as pb;
use tonic::Status;

fn gate(state: &ServerState) -> Result<(), Status> {
    if state.limiter.allow() {
        Ok(())
    } else {
        Err(errors::grpc_status(&errors::busy()))
    }
}

fn blob_in(b: Option<cadence_proto::api::DataBlob>) -> Result<Option<t::DataBlob>, Status> {
    b.map(|b| {
        Ok(t::DataBlob {
            encoding_type: c::enum_in(b.encoding_type, e::EncodingType::from_i32)?,
            data: b.data,
        })
    })
    .transpose()
}

fn vh_items_in(v: Vec<cadence_proto::admin::VersionHistoryItem>) -> Vec<t::VersionHistoryItem> {
    v.into_iter().map(|i| t::VersionHistoryItem { event_id: i.event_id, version: i.version }).collect()
}

fn vh_in(v: Option<cadence_proto::admin::VersionHistory>) -> Option<t::VersionHistory> {
    v.map(|h| t::VersionHistory { branch_token: h.branch_token, items: vh_items_in(h.items) })
}

pub fn replicate_events_v2_in(r: pb::ReplicateEventsV2Request) -> Result<et::ReplicateEventsV2Request, Status> {
    Ok(et::ReplicateEventsV2Request {
        domain_uuid: r.domain_id,
        workflow_execution: r.workflow_execution.map(c::execution),
        version_history_items: vh_items_in(r.version_history_items),
        events: blob_in(r.events)?,
        new_run_events: blob_in(r.new_run_events)?,
    })
}

pub fn sync_activity_in(r: pb::SyncActivityRequest) -> Result<et::SyncActivityRequest, Status> {
    let (workflow_id, run_id) = match r.workflow_execution {
        Some(w) => (w.workflow_id, w.run_id),
        None => (String::new(), String::new()),
    };
    let (reason, fdetails, fopts) = c::failure_in(r.last_failure)?;
    Ok(et::SyncActivityRequest {
        domain_id: r.domain_id,
        workflow_id,
        run_id,
        version: r.version,
        scheduled_id: r.scheduled_id,
        scheduled_time: c::nanos(r.scheduled_time),
        started_id: r.started_id,
        started_time: c::nanos(r.started_time),
        last_heartbeat_time: c::nanos(r.last_heartbeat_time),
        details: c::data(r.details),
        attempt: r.attempt,
        last_failure_reason: reason,
        last_worker_identity: r.last_worker_identity,
        last_failure_details: fdetails,
        last_failure_options: fopts,
        version_history: vh_in(r.version_history),
    })
}

pub fn sync_shard_status_in(r: pb::SyncShardStatusRequest) -> et::SyncShardStatusRequest {
    et::SyncShardStatusRequest { source_cluster: r.source_cluster, shard_id: i64::from(r.shard_id), timestamp: c::nanos(r.time) }
}

pub fn notify_failover_markers_in(r: pb::NotifyFailoverMarkersRequest) -> et::NotifyFailoverMarkersRequest {
    et::NotifyFailoverMarkersRequest {
        failover_marker_tokens: r
            .failover_marker_tokens
            .into_iter()
            .map(|tk| et::FailoverMarkerToken {
                shard_ids: tk.shard_ids,
                failover_marker: tk.failover_marker.map(|m| t::FailoverMarkerAttributes {
                    domain_id: Some(m.domain_id),
                    failover_version: Some(m.failover_version),
                    creation_time: c::nanos(m.creation_time),
                }),
            })
            .collect(),
    }
}

fn any_in(a: Option<cadence_proto::shared::Any>) -> Option<et::Any> {
    a.map(|a| et::Any { value_type: a.value_type, value: a.value })
}

fn any_out(a: Option<et::Any>) -> Option<cadence_proto::shared::Any> {
    a.map(|a| cadence_proto::shared::Any { value_type: a.value_type, value: a.value })
}

pub async fn replicate_events_v2(state: &ServerState, req: pb::ReplicateEventsV2Request) -> Result<pb::ReplicateEventsV2Response, Status> {
    gate(state)?;
    let r = replicate_events_v2_in(req)?;
    state.handler.replicate_events_v2(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::ReplicateEventsV2Response {})
}

pub async fn sync_activity(state: &ServerState, req: pb::SyncActivityRequest) -> Result<pb::SyncActivityResponse, Status> {
    gate(state)?;
    let r = sync_activity_in(req)?;
    state.handler.sync_activity(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::SyncActivityResponse {})
}

pub async fn sync_shard_status(state: &ServerState, req: pb::SyncShardStatusRequest) -> Result<pb::SyncShardStatusResponse, Status> {
    gate(state)?;
    let r = sync_shard_status_in(req);
    state.handler.sync_shard_status(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::SyncShardStatusResponse {})
}

pub async fn notify_failover_markers(state: &ServerState, req: pb::NotifyFailoverMarkersRequest) -> Result<pb::NotifyFailoverMarkersResponse, Status> {
    gate(state)?;
    let r = notify_failover_markers_in(req);
    state.handler.notify_failover_markers(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::NotifyFailoverMarkersResponse {})
}

pub async fn get_failover_info(state: &ServerState, req: pb::GetFailoverInfoRequest) -> Result<pb::GetFailoverInfoResponse, Status> {
    gate(state)?;
    let r = et::GetFailoverInfoRequest { domain_id: req.domain_id };
    let resp = state.handler.get_failover_info(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::GetFailoverInfoResponse { completed_shard_count: resp.completed_shard_count, pending_shards: resp.pending_shards })
}

pub async fn ratelimit_update(state: &ServerState, req: pb::RatelimitUpdateRequest) -> Result<pb::RatelimitUpdateResponse, Status> {
    gate(state)?;
    let r = et::RatelimitUpdateRequest { any: any_in(req.data) };
    let resp = state.handler.ratelimit_update(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::RatelimitUpdateResponse { data: any_out(resp.any) })
}

pub async fn describe_history_host(state: &ServerState, _req: pb::DescribeHistoryHostRequest) -> Result<pb::DescribeHistoryHostResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::DescribeHistoryHostRequest::default();
    let resp = state.handler.describe_history_host(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::DescribeHistoryHostResponse {
        number_of_shards: resp.number_of_shards,
        shard_ids: resp.shard_ids,
        domain_cache: resp.domain_cache.map(|d| cadence_proto::admin::DomainCacheInfo {
            num_of_items_in_cache_by_id: d.num_of_items_in_cache_by_id,
            num_of_items_in_cache_by_name: d.num_of_items_in_cache_by_name,
        }),
        shard_controller_status: resp.shard_controller_status,
        address: resp.address,
    })
}

pub async fn count_dlq_messages(state: &ServerState, req: pb::CountDlqMessagesRequest) -> Result<pb::CountDlqMessagesResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::CountDLQMessagesRequest { force_fetch: req.force_fetch };
    let resp = state.handler.count_dlq_messages(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::CountDlqMessagesResponse {
        entries: resp
            .entries
            .into_iter()
            .map(|en| cadence_proto::admin::HistoryDlqCountEntry { count: en.message_count, shard_id: 0, source_cluster: en.source_cluster })
            .collect(),
    })
}

pub async fn get_cross_cluster_tasks(state: &ServerState, req: pb::GetCrossClusterTasksRequest) -> Result<pb::GetCrossClusterTasksResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::GetCrossClusterTasksRequest { shard_ids: req.shard_ids, target_cluster: req.target_cluster };
    let resp = state.handler.get_cross_cluster_tasks(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::GetCrossClusterTasksResponse {
        tasks_by_shard: c::cross_cluster::tasks_by_shard_out(resp.tasks_by_shard)?,
        failed_cause_by_shard: c::cross_cluster::failed_causes_out(resp.failed_cause_by_shard),
    })
}

pub async fn respond_cross_cluster_tasks_completed(
    state: &ServerState,
    req: pb::RespondCrossClusterTasksCompletedRequest,
) -> Result<pb::RespondCrossClusterTasksCompletedResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::RespondCrossClusterTasksCompletedRequest {
        shard_id: req.shard_id,
        target_cluster: req.target_cluster,
        task_responses: req.task_responses.into_iter().map(c::cross_cluster::task_response_in).collect::<Result<_, _>>()?,
        fetch_new_tasks: req.fetch_new_tasks,
    };
    let resp = state.handler.respond_cross_cluster_tasks_completed(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::RespondCrossClusterTasksCompletedResponse { tasks: c::cross_cluster::task_requests_out(resp.tasks)? })
}


use cadence_service_core::replication_convert as rc;

fn dlq_type_in(v: i32) -> Result<Option<e::DLQType>, Status> {
    rc::dlq_type_in(v).map_err(Status::invalid_argument)
}

/// Go `GRPCHandler.GetReplicationMessages`.
pub async fn get_replication_messages(state: &ServerState, req: pb::GetReplicationMessagesRequest) -> Result<pb::GetReplicationMessagesResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::GetReplicationMessagesRequest {
        tokens: req.tokens.into_iter().map(rc::replication_token_in).collect(),
        cluster_name: req.cluster_name,
    };
    let resp = state.handler.get_replication_messages(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::GetReplicationMessagesResponse {
        shard_messages: resp
            .messages_by_shard
            .unwrap_or_default()
            .into_iter()
            .map(|(shard, m)| (shard, rc::replication_messages_out(m)))
            .collect(),
    })
}

/// Go `GRPCHandler.GetDLQReplicationMessages`.
pub async fn get_dlq_replication_messages(state: &ServerState, req: pb::GetDlqReplicationMessagesRequest) -> Result<pb::GetDlqReplicationMessagesResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::GetDLQReplicationMessagesRequest {
        task_infos: req.task_infos.into_iter().map(rc::replication_task_info_in).collect(),
    };
    let resp = state.handler.get_dlq_replication_messages(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::GetDlqReplicationMessagesResponse { replication_tasks: rc::replication_tasks_out(resp.replication_tasks.unwrap_or_default()) })
}

/// Go `GRPCHandler.ReadDLQMessages`.
pub async fn read_dlq_messages(state: &ServerState, req: pb::ReadDlqMessagesRequest) -> Result<pb::ReadDlqMessagesResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::ReadDLQMessagesRequest {
        dlq_type: dlq_type_in(req.r#type)?,
        shard_id: req.shard_id,
        source_cluster: req.source_cluster,
        inclusive_end_message_id: req.inclusive_end_message_id,
        maximum_page_size: req.page_size,
        next_page_token: req.next_page_token,
    };
    let resp = state.handler.read_dlq_messages(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::ReadDlqMessagesResponse {
        r#type: resp.dlq_type.map_or(0, rc::dlq_type_out),
        replication_tasks: rc::replication_tasks_out(resp.replication_tasks),
        replication_tasks_info: resp.replication_tasks_info.into_iter().map(rc::replication_task_info_out).collect(),
        next_page_token: resp.next_page_token,
    })
}

/// Go `GRPCHandler.PurgeDLQMessages`.
pub async fn purge_dlq_messages(state: &ServerState, req: pb::PurgeDlqMessagesRequest) -> Result<pb::PurgeDlqMessagesResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::PurgeDLQMessagesRequest {
        dlq_type: dlq_type_in(req.r#type)?,
        shard_id: req.shard_id,
        source_cluster: req.source_cluster,
        inclusive_end_message_id: req.inclusive_end_message_id,
    };
    state.handler.purge_dlq_messages(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::PurgeDlqMessagesResponse {})
}

/// Go `GRPCHandler.MergeDLQMessages`.
pub async fn merge_dlq_messages(state: &ServerState, req: pb::MergeDlqMessagesRequest) -> Result<pb::MergeDlqMessagesResponse, Status> {
    gate(state)?;
    let r = cadence_api::admin::MergeDLQMessagesRequest {
        dlq_type: dlq_type_in(req.r#type)?,
        shard_id: req.shard_id,
        source_cluster: req.source_cluster,
        inclusive_end_message_id: req.inclusive_end_message_id,
        maximum_page_size: req.page_size,
        next_page_token: req.next_page_token,
    };
    let resp = state.handler.merge_dlq_messages(Some(&r)).await.map_err(|e| errors::grpc_status(&e))?;
    Ok(pb::MergeDlqMessagesResponse { next_page_token: resp.next_page_token })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_shard_status_converts() {
        let r = sync_shard_status_in(pb::SyncShardStatusRequest {
            source_cluster: "a".into(),
            shard_id: 3,
            time: Some(c::timestamp_from_nanos(2_000_000_005)),
        });
        assert_eq!((r.shard_id, r.timestamp), (3, Some(2_000_000_005)));
    }

    #[test]
    fn sync_activity_splits_execution() {
        let r = sync_activity_in(pb::SyncActivityRequest {
            workflow_execution: Some(cadence_proto::api::WorkflowExecution { workflow_id: "w".into(), run_id: "r".into() }),
            attempt: 2,
            ..Default::default()
        })
        .unwrap();
        assert_eq!((r.workflow_id.as_str(), r.run_id.as_str(), r.attempt), ("w", "r", 2));
    }

    #[test]
    fn failover_markers_convert() {
        let r = notify_failover_markers_in(pb::NotifyFailoverMarkersRequest {
            failover_marker_tokens: vec![cadence_proto::admin::FailoverMarkerToken {
                shard_ids: vec![1],
                failover_marker: Some(cadence_proto::admin::FailoverMarkerAttributes {
                    domain_id: "d".into(),
                    failover_version: 4,
                    creation_time: None,
                }),
            }],
        });
        let m = r.failover_marker_tokens[0].failover_marker.as_ref().unwrap();
        assert_eq!(m.failover_version, Some(4));
    }
}
