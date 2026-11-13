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

pub fn data_blob(blob: cadence_proto::api::DataBlob) -> Option<t::DataBlob> {
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

pub fn version_history(value: pa::VersionHistory) -> t::VersionHistory {
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
            Some(Attributes::DomainTaskAttributes(pa::DomainTaskAttributes {
            domain_operation: a.domain_operation.map_or(0, |v| v as i32 + 1),
            id: a.id.clone(),
            domain: Some(domain_out(&a)),
            config_version: a.config_version,
            failover_version: a.failover_version,
            previous_failover_version: a.previous_failover_version,
        }))
    } else if let Some(a) = task.sync_shard_status_task_attributes {
        Some(Attributes::SyncShardStatusTaskAttributes(
            pa::SyncShardStatusTaskAttributes {
                source_cluster: a.source_cluster.unwrap_or_default(),
                shard_id: a.shard_id.unwrap_or_default() as i32,
                timestamp: ts_out(a.timestamp),
            },
        ))
    } else if let Some(a) = task.sync_activity_task_attributes {
        let last_failure = a.last_failure_reason.map(|reason| cadence_proto::api::Failure {
            reason,
            details: a.last_failure_details.unwrap_or_default(),
            options: a.last_failure_options.map(|o| cadence_proto::api::FailureOptions {
                failure_category: o.failure_category.map_or(0, |v| v as i32 + 1),
                next_retry_interval: o
                    .next_retry_interval_seconds
                    .map(|s| cadence_proto::convert::duration(i64::from(s))),
            }),
        });
        Some(Attributes::SyncActivityTaskAttributes(
            pa::SyncActivityTaskAttributes {
                domain_id: a.domain_id.unwrap_or_default(),
                workflow_execution: execution_out(a.workflow_id, a.run_id),
                version: a.version.unwrap_or_default(),
                scheduled_id: a.scheduled_id.unwrap_or_default(),
                scheduled_time: ts_out(a.scheduled_time),
                started_id: a.started_id.unwrap_or_default(),
                started_time: ts_out(a.started_time),
                last_heartbeat_time: ts_out(a.last_heartbeat_time),
                details: a.details.and_then(cadence_proto::convert::payload),
                attempt: a.attempt.unwrap_or_default(),
                last_failure,
                last_worker_identity: a.last_worker_identity.unwrap_or_default(),
                version_history: a.version_history.map(version_history_out),
            },
        ))
    } else if let Some(a) = task.history_task_v2_attributes {
        Some(Attributes::HistoryTaskV2Attributes(pa::HistoryTaskV2Attributes {
            task_id: a.task_id.unwrap_or_default(),
            domain_id: a.domain_id.unwrap_or_default(),
            workflow_execution: execution_out(a.workflow_id, a.run_id),
            version_history_items: a
                .version_history_items
                .unwrap_or_default()
                .into_iter()
                .map(version_history_item_out)
                .collect(),
            events: a.events.map(data_blob_out),
            new_run_events: a.new_run_events.map(data_blob_out),
        }))
    } else {
        task.failover_marker_attributes.map(|a| {
            Attributes::FailoverMarkerAttributes(pa::FailoverMarkerAttributes {
                domain_id: a.domain_id.unwrap_or_default(),
                failover_version: a.failover_version.unwrap_or_default(),
                creation_time: ts_out(a.creation_time),
            })
        })
    };
    pa::ReplicationTask {
        task_type: task.task_type.map_or(0, |v| v as i32 + 1),
        source_task_id: task.source_task_id.unwrap_or_default(),
        creation_time: ts_out(task.creation_time),
        attributes,
    }
}

pub fn replication_tasks_out(tasks: Vec<t::ReplicationTask>) -> Vec<pa::ReplicationTask> {
    tasks.into_iter().map(replication_task_out).collect()
}

pub fn replication_messages_out(messages: api_admin::ReplicationMessages) -> pa::ReplicationMessages {
    pa::ReplicationMessages {
        replication_tasks: replication_tasks_out(messages.replication_tasks),
        last_retrieved_message_id: messages.last_retrieved_message_id,
        has_more: messages.has_more,
        sync_shard_status: messages.sync_shard_status.map(|status| pa::SyncShardStatus {
            timestamp: ts_out(status.timestamp),
        }),
    }
}

pub fn replication_token_in(token: pa::ReplicationToken) -> api_admin::ReplicationToken {
    api_admin::ReplicationToken {
        shard_id: token.shard_id,
        last_retrieved_message_id: token.last_retrieved_message_id,
        last_processed_message_id: token.last_processed_message_id,
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
            attributes: Some(
                pa::replication_task::Attributes::SyncActivityTaskAttributes(
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
                ),
            ),
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
