//! Replication task conversions between the History admin protobuf types and
//! the Cadence API types. Mirrors `go/common/types/mapper/proto/shared.go` and
//! `proto/admin.go` (ReplicationMessages, ReplicationTask and its attributes,
//! ReplicationToken, ReplicationTaskInfo, HistoryDLQCountEntry).
use cadence_api::{admin as api_admin, enums as e, types as t};
use cadence_proto::admin as pa;
use cadence_proto::convert::{enum_in, failure_in, nanos, timestamp_from_nanos};

fn status(error: tonic::Status) -> String {
    error.message().to_string()
}

fn data_blob(blob: cadence_proto::api::DataBlob) -> Option<t::DataBlob> {
    let encoding_type = match blob.encoding_type {
        1 => Some(e::EncodingType::ThriftRW),
        2 => Some(e::EncodingType::JSON),
        _ => None,
    };
    Some(t::DataBlob {
        encoding_type,
        data: blob.data,
    })
}

fn version_history(value: pa::VersionHistory) -> t::VersionHistory {
    t::VersionHistory {
        branch_token: value.branch_token,
        items: value.items.into_iter().map(version_history_item).collect(),
    }
}

fn version_history_item(item: pa::VersionHistoryItem) -> t::VersionHistoryItem {
    t::VersionHistoryItem {
        event_id: item.event_id,
        version: item.version,
    }
}

fn execution_parts(
    execution: Option<cadence_proto::api::WorkflowExecution>,
) -> (Option<String>, Option<String>) {
    match execution {
        Some(execution) => (Some(execution.workflow_id), Some(execution.run_id)),
        None => (None, None),
    }
}

pub fn replication_task_in(task: pa::ReplicationTask) -> Result<t::ReplicationTask, String> {
    use pa::replication_task::Attributes;
    let mut out = t::ReplicationTask {
        task_type: enum_in(task.task_type, e::ReplicationTaskType::from_i32).map_err(status)?,
        source_task_id: Some(task.source_task_id),
        creation_time: nanos(task.creation_time),
        ..Default::default()
    };
    match task.attributes {
        None => {}
        Some(Attributes::DomainTaskAttributes(a)) => {
            // History never emits domain tasks (they come from the domain
            // replication queue), so only the scalar attributes are mapped.
            out.domain_task_attributes = Some(t::DomainTaskAttributes {
                domain_operation: enum_in(a.domain_operation, e::DomainOperation::from_i32)
                    .map_err(status)?,
                id: a.id,
                config_version: a.config_version,
                failover_version: a.failover_version,
                previous_failover_version: a.previous_failover_version,
                ..Default::default()
            });
        }
        Some(Attributes::SyncShardStatusTaskAttributes(a)) => {
            out.sync_shard_status_task_attributes = Some(t::SyncShardStatusTaskAttributes {
                source_cluster: Some(a.source_cluster),
                shard_id: Some(i64::from(a.shard_id)),
                timestamp: nanos(a.timestamp),
            });
        }
        Some(Attributes::SyncActivityTaskAttributes(a)) => {
            let (workflow_id, run_id) = execution_parts(a.workflow_execution);
            let (reason, details, options) = failure_in(a.last_failure).map_err(status)?;
            out.sync_activity_task_attributes = Some(t::SyncActivityTaskAttributes {
                domain_id: Some(a.domain_id),
                workflow_id,
                run_id,
                version: Some(a.version),
                scheduled_id: Some(a.scheduled_id),
                scheduled_time: nanos(a.scheduled_time),
                started_id: Some(a.started_id),
                started_time: nanos(a.started_time),
                last_heartbeat_time: nanos(a.last_heartbeat_time),
                details: a.details.map(|payload| payload.data),
                attempt: Some(a.attempt),
                last_failure_details: reason.as_ref().map(|_| details),
                last_failure_reason: reason,
                last_failure_options: options,
                last_worker_identity: Some(a.last_worker_identity),
                version_history: a.version_history.map(version_history),
            });
        }
        Some(Attributes::HistoryTaskV2Attributes(a)) => {
            let (workflow_id, run_id) = execution_parts(a.workflow_execution);
            out.history_task_v2_attributes = Some(t::HistoryTaskV2Attributes {
                task_id: Some(a.task_id),
                domain_id: Some(a.domain_id),
                workflow_id,
                run_id,
                version_history_items: if a.version_history_items.is_empty() {
                    None
                } else {
                    Some(
                        a.version_history_items
                            .into_iter()
                            .map(version_history_item)
                            .collect(),
                    )
                },
                events: a.events.and_then(data_blob),
                new_run_events: a.new_run_events.and_then(data_blob),
            });
        }
        Some(Attributes::FailoverMarkerAttributes(a)) => {
            out.failover_marker_attributes = Some(failover_marker_in(a));
        }
    }
    Ok(out)
}

pub fn failover_marker_in(a: pa::FailoverMarkerAttributes) -> t::FailoverMarkerAttributes {
    t::FailoverMarkerAttributes {
        domain_id: Some(a.domain_id),
        failover_version: Some(a.failover_version),
        creation_time: nanos(a.creation_time),
    }
}

pub fn replication_tasks_in(
    tasks: Vec<pa::ReplicationTask>,
) -> Result<Vec<t::ReplicationTask>, String> {
    tasks.into_iter().map(replication_task_in).collect()
}

pub fn replication_messages_in(
    messages: pa::ReplicationMessages,
) -> Result<api_admin::ReplicationMessages, String> {
    Ok(api_admin::ReplicationMessages {
        replication_tasks: replication_tasks_in(messages.replication_tasks)?,
        last_retrieved_message_id: messages.last_retrieved_message_id,
        has_more: messages.has_more,
        sync_shard_status: messages
            .sync_shard_status
            .map(|status| api_admin::SyncShardStatus {
                timestamp: nanos(status.timestamp),
            }),
    })
}

pub fn replication_token_out(token: api_admin::ReplicationToken) -> pa::ReplicationToken {
    pa::ReplicationToken {
        shard_id: token.shard_id,
        last_retrieved_message_id: token.last_retrieved_message_id,
        last_processed_message_id: token.last_processed_message_id,
    }
}

pub fn replication_task_info_out(info: api_admin::ReplicationTaskInfo) -> pa::ReplicationTaskInfo {
    pa::ReplicationTaskInfo {
        domain_id: info.domain_id,
        workflow_execution: Some(cadence_proto::api::WorkflowExecution {
            workflow_id: info.workflow_id,
            run_id: info.run_id,
        }),
        task_type: i32::from(info.task_type),
        task_id: info.task_id,
        version: info.version,
        first_event_id: info.first_event_id,
        next_event_id: info.next_event_id,
        scheduled_id: info.scheduled_id,
    }
}

/// Go narrows the protobuf int32 task type to int16 (`int16(t.TaskType)`).
pub fn replication_task_info_in(info: pa::ReplicationTaskInfo) -> api_admin::ReplicationTaskInfo {
    let (workflow_id, run_id) = execution_parts(info.workflow_execution);
    api_admin::ReplicationTaskInfo {
        domain_id: info.domain_id,
        workflow_id: workflow_id.unwrap_or_default(),
        run_id: run_id.unwrap_or_default(),
        task_type: info.task_type as i16,
        task_id: info.task_id,
        version: info.version,
        first_event_id: info.first_event_id,
        next_event_id: info.next_event_id,
        scheduled_id: info.scheduled_id,
    }
}

pub fn dlq_type_out(dlq_type: e::DLQType) -> i32 {
    dlq_type as i32
}

/// Go `FromDLQType`: the protobuf INVALID value maps to nil.
pub fn dlq_type_in(value: i32) -> Result<Option<e::DLQType>, String> {
    if value == 0 {
        return Ok(None);
    }
    e::DLQType::from_i32(value)
        .map(Some)
        .ok_or_else(|| "Unknown DLQ type.".to_string())
}

pub fn sync_shard_timestamp(nanos: i64) -> prost_types::Timestamp {
    timestamp_from_nanos(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_activity_task_converts_like_go() {
        let task = pa::ReplicationTask {
            task_type: 4,
            source_task_id: 9,
            creation_time: Some(prost_types::Timestamp {
                seconds: 2,
                nanos: 5,
            }),
            attributes: Some(pa::replication_task::Attributes::SyncActivityTaskAttributes(
                pa::SyncActivityTaskAttributes {
                    domain_id: "d".into(),
                    workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                        workflow_id: "w".into(),
                        run_id: "r".into(),
                    }),
                    version: 3,
                    attempt: 2,
                    last_worker_identity: "id".into(),
                    version_history: Some(pa::VersionHistory {
                        branch_token: vec![1],
                        items: vec![pa::VersionHistoryItem {
                            event_id: 4,
                            version: 3,
                        }],
                    }),
                    ..Default::default()
                },
            )),
        };
        let out = replication_task_in(task).unwrap();
        assert_eq!(out.task_type, Some(e::ReplicationTaskType::SyncActivity));
        assert_eq!(out.source_task_id, Some(9));
        assert_eq!(out.creation_time, Some(2_000_000_005));
        let a = out.sync_activity_task_attributes.unwrap();
        assert_eq!(a.workflow_id.as_deref(), Some("w"));
        assert_eq!(a.run_id.as_deref(), Some("r"));
        assert_eq!(a.attempt, Some(2));
        assert_eq!(a.version_history.unwrap().items[0].event_id, 4);
        assert!(a.last_failure_reason.is_none());
        assert!(out.history_task_v2_attributes.is_none());
    }

    #[test]
    fn token_and_task_info_round_trip_with_int16_task_type() {
        let token = replication_token_out(api_admin::ReplicationToken {
            shard_id: 3,
            last_retrieved_message_id: 10,
            last_processed_message_id: 8,
        });
        assert_eq!(
            (
                token.shard_id,
                token.last_retrieved_message_id,
                token.last_processed_message_id
            ),
            (3, 10, 8)
        );
        let info = api_admin::ReplicationTaskInfo {
            domain_id: "d".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            task_type: 2,
            task_id: 7,
            version: 1,
            first_event_id: 1,
            next_event_id: 5,
            scheduled_id: 4,
        };
        assert_eq!(
            replication_task_info_in(replication_task_info_out(info.clone())),
            info
        );
        let wide = pa::ReplicationTaskInfo {
            task_type: 0x1_0002,
            ..Default::default()
        };
        assert_eq!(replication_task_info_in(wide).task_type, 2);
    }

    #[test]
    fn history_v2_blob_encoding_maps_thrift_and_json() {
        let blob = |encoding_type| cadence_proto::api::DataBlob {
            encoding_type,
            data: vec![1],
        };
        assert_eq!(
            data_blob(blob(1)).unwrap().encoding_type,
            Some(e::EncodingType::ThriftRW)
        );
        assert_eq!(
            data_blob(blob(2)).unwrap().encoding_type,
            Some(e::EncodingType::JSON)
        );
    }
}
