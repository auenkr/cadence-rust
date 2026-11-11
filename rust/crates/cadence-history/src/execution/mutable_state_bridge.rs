//! Lossless persistence boundary for history mutable state.
//!
//! The envelope retains the source snapshot verbatim while exposing the rows
//! needed by the engine. This deliberately preserves unknown SQL blob fields
//! and records which are not interpreted by this codec.

use std::collections::BTreeMap;

use cadence_api::types::VersionHistories;
use cadence_persistence::{history_contract as pc, sqlblobs};
use cadence_thrift::FromThrift;

use crate::engine::deps::{HResult, HistoryError};

#[derive(Debug, Clone)]
pub struct MutableStateEnvelope {
    /// Original snapshot, including opaque keyed values and persistence-only data.
    pub snapshot: pc::WorkflowSnapshot,
    pub activities: BTreeMap<i64, pc::ActivityInfo>,
    pub timers: BTreeMap<String, pc::TimerInfo>,
    pub children: BTreeMap<i64, pc::ChildExecutionInfo>,
    pub version_histories: Option<VersionHistories>,
    pub current_version: i64,
    pub start_version: i64,
    pub last_write_version: i64,
    pub buffered_events: Vec<cadence_api::service_types::HistoryEvent>,
}

fn decode(blob: &pc::DataBlob) -> HResult<cadence_thrift::value::Struct> {
    sqlblobs::Encoding::parse(&blob.encoding)
        .and_then(|encoding| encoding.decode(&blob.data))
        .map_err(|e| HistoryError::internal(format!("decode mutable state blob: {e}")))
}

fn decode_events(blob: &pc::DataBlob) -> HResult<Vec<cadence_api::service_types::HistoryEvent>> {
    cadence_thrift::history_blob::decode(&cadence_api::types::DataBlob {
        encoding_type: Some(cadence_api::enums::EncodingType::ThriftRW),
        data: blob.data.clone(),
    })
    .map_err(|e| HistoryError::internal(format!("decode buffered events: {e}")))
}

/// Decode snapshot records while retaining the complete original snapshot.
pub fn snapshot_to_envelope(snapshot: pc::WorkflowSnapshot) -> HResult<MutableStateEnvelope> {
    let mut activities = BTreeMap::new();
    for row in &snapshot.activity_infos {
        let value = sqlblobs::activity_info_from_struct(&decode(&row.value)?)
            .map_err(|e| HistoryError::internal(format!("decode activity info: {e}")))?;
        activities.insert(row.key, value);
    }

    let mut timers = BTreeMap::new();
    for row in &snapshot.timer_infos {
        let value = sqlblobs::timer_info_from_struct(&decode(&row.value)?)
            .map_err(|e| HistoryError::internal(format!("decode timer info: {e}")))?;
        timers.insert(row.key.clone(), value);
    }

    let mut children = BTreeMap::new();
    for row in &snapshot.child_execution_infos {
        let value = sqlblobs::child_execution_info_from_struct(&decode(&row.value)?)
            .map_err(|e| HistoryError::internal(format!("decode child execution info: {e}")))?;
        children.insert(row.key, value);
    }

    let version_histories = snapshot.version_histories.as_ref().map(|blob| {
        crate::wiring::server::thrift_types_g2::version_histories_from_thrift(&decode(blob)?)
            .map_err(|e| HistoryError::internal(format!("decode version histories: {e}")))
    }).transpose()?;
    let current_version = version_histories
        .as_ref()
        .and_then(|histories| histories.histories.get(histories.current_version_history_index as usize))
        .and_then(|history| history.items.last())
        .map(|item| item.version)
        .unwrap_or(-24);

    let mut buffered_events = Vec::new();
    for blob in &snapshot.buffered_events {
        buffered_events.extend(decode_events(blob)?);
    }

    let start_version = snapshot.start_version;
    let last_write_version = snapshot.last_write_version;
    Ok(MutableStateEnvelope {
        snapshot,
        activities,
        timers,
        children,
        version_histories,
        current_version,
        start_version,
        last_write_version,
        buffered_events,
    })
}

/// Export an untouched envelope without normalizing or re-encoding any row.
pub fn envelope_to_snapshot(envelope: &MutableStateEnvelope) -> pc::WorkflowSnapshot {
    envelope.snapshot.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(data: Vec<u8>) -> pc::DataBlob {
        pc::DataBlob { encoding: "thriftrw".into(), data }
    }

    #[test]
    fn unchanged_snapshot_round_trips_opaque_and_auxiliary_fields() {
        let versions = VersionHistories {
            current_version_history_index: 1,
            histories: vec![
                cadence_api::types::VersionHistory {
                    branch_token: vec![1, 2],
                    items: vec![cadence_api::types::VersionHistoryItem { event_id: 4, version: 17 }],
                },
                cadence_api::types::VersionHistory {
                    branch_token: vec![3, 4],
                    items: vec![cadence_api::types::VersionHistoryItem { event_id: 9, version: 29 }],
                },
            ],
        };
        let mut snapshot = pc::WorkflowSnapshot::default();
        snapshot.execution_info.workflow_id = "wf".into();
        snapshot.execution_info.run_id = "run".into();
        snapshot.execution_info.last_write_version = 43;
        snapshot.start_version = 31;
        snapshot.last_write_version = 43;
        snapshot.version_histories = Some(blob(
            sqlblobs::Encoding::ThriftRW
                .encode(&crate::wiring::server::thrift_types_g2::version_histories_to_thrift(&versions))
                .unwrap(),
        ));
        snapshot.buffered_events = vec![
            blob(cadence_thrift::history_blob::encode(vec![]).data),
            blob(cadence_thrift::history_blob::encode(vec![]).data),
        ];
        snapshot.activity_heartbeats.insert(
            27,
            pc::ActivityHeartbeat { details: vec![0, 255, 7], ..Default::default() },
        );
        snapshot.workflow_timer_tasks.push(pc::HistoryTaskKey::default());
        snapshot.semaphore_infos.push(pc::DataBlob { encoding: "opaque-v9".into(), data: vec![0, 1, 255] });
        snapshot.checksum = Some(pc::DataBlob { encoding: "opaque-checksum".into(), data: vec![4, 5] });
        snapshot.request_cancel_infos.push(pc::KeyedBlob {
            key: 27,
            value: pc::DataBlob { encoding: "opaque-row".into(), data: vec![9, 0, 8] },
        });
        snapshot.signal_infos.push(pc::KeyedBlob {
            key: 28,
            value: pc::DataBlob { encoding: "opaque-row".into(), data: vec![6, 0, 5] },
        });
        snapshot.signal_requested_ids.push("signal-id".into());

        let envelope = snapshot_to_envelope(snapshot.clone()).unwrap();
        assert_eq!(envelope.current_version, 29);
        assert_eq!(envelope.start_version, 31);
        assert_eq!(envelope.last_write_version, 43);
        assert_eq!(envelope.buffered_events.len(), 0);
        assert_eq!(envelope.snapshot.buffered_events.len(), 2);
        assert_eq!(envelope_to_snapshot(&envelope), snapshot);
    }

    #[test]
    fn malformed_keyed_rows_and_buffered_events_are_errors() {
        let mut bad_activity = pc::WorkflowSnapshot::default();
        bad_activity.activity_infos.push(pc::KeyedBlob {
            key: 55,
            value: pc::DataBlob { encoding: "not-an-encoding".into(), data: vec![1] },
        });
        assert!(snapshot_to_envelope(bad_activity).is_err());

        let mut bad_timer = pc::WorkflowSnapshot::default();
        bad_timer.timer_infos.push(pc::KeyedBlob {
            key: "timer-key".into(),
            value: blob(vec![1, 2, 3]),
        });
        assert!(snapshot_to_envelope(bad_timer).is_err());

        let mut bad_buffer = pc::WorkflowSnapshot::default();
        bad_buffer.buffered_events.push(blob(vec![0, 1]));
        assert!(snapshot_to_envelope(bad_buffer).is_err());
    }
}
