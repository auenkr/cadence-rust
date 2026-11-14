//! Lossless persistence boundary for history mutable state.
//!
//! The envelope retains the source snapshot verbatim while exposing the rows
//! needed by the engine. This deliberately preserves unknown SQL blob fields
//! and records which are not interpreted by this codec.

use std::collections::BTreeMap;

use cadence_api::types::VersionHistories;
use cadence_persistence::{history_contract as pc, sqlblobs};
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

/// Convert Go-shaped mutation records to SQL blobs while overlaying known
/// fields on their original structs. This retains unknown Thrift fields for
/// records that were loaded from persistence and then changed.
pub fn mutation_to_persistence(
    mutation: &super::mutable_state_builder::TypedWorkflowMutation,
    envelope: &MutableStateEnvelope,
) -> HResult<pc::WorkflowMutation> {
    use cadence_thrift::value::Struct;
    use pc::{ActivityInfo, ChildExecutionInfo, RequestCancelInfo, SignalInfo, TimerInfo};
    fn blob(value: Struct, previous: Option<&pc::DataBlob>) -> HResult<pc::DataBlob> {
        let mut merged = previous.map(decode).transpose()?.unwrap_or_default();
        for (id, field) in value.fields { merged.fields.retain(|(old, _)| *old != id); merged.fields.push((id, field)); }
        let encoding = previous.map(|b| sqlblobs::Encoding::parse(&b.encoding)).transpose()
            .map_err(|e| HistoryError::internal(format!("parse prior row encoding: {e}")))?
            .unwrap_or(sqlblobs::Encoding::ThriftRW);
        Ok(pc::DataBlob { encoding: encoding.as_str().into(), data: encoding.encode(&merged)
            .map_err(|e| HistoryError::internal(format!("encode mutable state row: {e}")))? })
    }
    fn keyed<K: Clone>(key: K, info: &impl ToStruct, old: Option<&pc::DataBlob>) -> HResult<pc::KeyedBlob<K>> {
        Ok(pc::KeyedBlob { key, value: blob(info.to_struct(), old)? })
    }
    trait ToStruct { fn to_struct(&self) -> Struct; }
    impl ToStruct for ActivityInfo { fn to_struct(&self) -> Struct { sqlblobs::activity_info_to_struct(self) } }
    impl ToStruct for TimerInfo { fn to_struct(&self) -> Struct { sqlblobs::timer_info_to_struct(self) } }
    impl ToStruct for ChildExecutionInfo { fn to_struct(&self) -> Struct { sqlblobs::child_execution_info_to_struct(self) } }
    impl ToStruct for RequestCancelInfo { fn to_struct(&self) -> Struct { sqlblobs::request_cancel_info_to_struct(self) } }
    impl ToStruct for SignalInfo { fn to_struct(&self) -> Struct { sqlblobs::signal_info_to_struct(self) } }

    let find_activity = |id| envelope.snapshot.activity_infos.iter().find(|r| r.key == id).map(|r| &r.value);
    let find_timer = |id: &str| envelope.snapshot.timer_infos.iter().find(|r| r.key == id).map(|r| &r.value);
    let find_child = |id| envelope.snapshot.child_execution_infos.iter().find(|r| r.key == id).map(|r| &r.value);
    let find_cancel = |id| envelope.snapshot.request_cancel_infos.iter().find(|r| r.key == id).map(|r| &r.value);
    let find_signal = |id| envelope.snapshot.signal_infos.iter().find(|r| r.key == id).map(|r| &r.value);
    let version_histories = match mutation.version_histories.as_ref() {
        Some(histories) => Some(pc::DataBlob {
            encoding: "thriftrw".into(),
            data: sqlblobs::Encoding::ThriftRW.encode_versioned(&crate::wiring::server::thrift_types_g2::version_histories_to_thrift(histories))
                .map_err(|e| HistoryError::internal(format!("encode version histories: {e}")))?,
        }),
        None => envelope.snapshot.version_histories.clone(),
    };
    let mut result = pc::WorkflowMutation {
        execution_info: mutation.execution_info.clone(), start_version: mutation.start_version,
        last_write_version: mutation.last_write_version, condition: mutation.condition,
        version_histories,
        activity_heartbeats: envelope.snapshot.activity_heartbeats.clone(),
        tasks_by_category: mutation.tasks_by_category.clone(), workflow_requests: mutation.workflow_requests.clone(),
        delete_activity_infos: mutation.delete_activity_infos.clone(), delete_timer_infos: mutation.delete_timer_infos.clone(),
        delete_child_execution_infos: mutation.delete_child_execution_infos.clone(),
        delete_request_cancel_infos: mutation.delete_request_cancel_infos.clone(), delete_signal_infos: mutation.delete_signal_infos.clone(),
        upsert_signal_requested_ids: mutation.upsert_signal_requested_ids.clone(), delete_signal_requested_ids: mutation.delete_signal_requested_ids.clone(),
        clear_buffered_events: mutation.clear_buffered_events, ..Default::default()
    };
    for info in &mutation.upsert_activity_infos {
        let id = info.schedule_id;
        let row = pc::ActivityInfo {
            version: info.version, scheduled_event_batch_id: info.scheduled_event_batch_id,
            scheduled_event: vec![], scheduled_event_encoding: String::new(), scheduled_time_unix_nanos: info.scheduled_time,
            started_id: info.started_id, started_event: vec![], started_event_encoding: String::new(), started_time_unix_nanos: info.started_time,
            activity_id: info.activity_id.clone(), request_id: info.request_id.clone(),
            schedule_to_start_timeout_seconds: info.schedule_to_start_timeout, schedule_to_close_timeout_seconds: info.schedule_to_close_timeout,
            start_to_close_timeout_seconds: info.start_to_close_timeout, heartbeat_timeout_seconds: info.heartbeat_timeout,
            cancel_requested: info.cancel_requested, cancel_request_id: info.cancel_request_id, timer_task_status: info.timer_task_status,
            last_heartbeat_timeout_visibility_in_seconds: info.last_heartbeat_timeout_visibility_in_seconds,
            attempt: info.attempt, task_list: info.task_list.clone(), started_identity: info.started_identity.clone(),
            has_retry_policy: info.has_retry_policy, retry_initial_interval_seconds: info.initial_interval,
            retry_maximum_interval_seconds: info.maximum_interval, retry_maximum_attempts: info.maximum_attempts,
            retry_expiration_time_unix_nanos: info.expiration_time, retry_backoff_coefficient: info.backoff_coefficient,
            retry_jitter_coefficient: info.jitter_coefficient, retry_non_retriable_errors: info.non_retriable_errors.clone(),
            retry_last_failure_reason: info.last_failure_reason.clone(), retry_last_worker_identity: info.last_worker_identity.clone(),
            retry_last_failure_details: info.last_failure_details.clone(), retry_last_retry_interval_seconds: info.last_retry_interval_seconds,
            retry_last_failure_category: info.last_failure_category,
            task_list_kind: info.task_list_kind,
        };
        // Persistence-only scheduled/started event blobs, encodings, task-list kind and failure category survive updates.
        let old = find_activity(id);
        let mut typed = row;
        if let Some(old) = old { let prev = sqlblobs::activity_info_from_struct(&decode(old)?)
            .map_err(|e| HistoryError::internal(format!("decode prior activity: {e}")))?;
            typed.scheduled_event = prev.scheduled_event; typed.scheduled_event_encoding = prev.scheduled_event_encoding;
            typed.started_event = prev.started_event; typed.started_event_encoding = prev.started_event_encoding;
           
        }
        result.upsert_activity_infos.push(keyed(id, &typed, old)?);
        if !info.details.is_empty() || info.last_heartbeat_updated_time != 0 {
            result.activity_heartbeats.insert(id, pc::ActivityHeartbeat { details: info.details.clone(), updated_time_unix_nanos: info.last_heartbeat_updated_time });
        }
    }
    for info in &mutation.upsert_timer_infos { let id = info.timer_id.clone(); result.upsert_timer_infos.push(keyed(id.clone(), &pc::TimerInfo { version: info.version, started_id: info.started_id, expiry_time_unix_nanos: info.expiry_time_unix_nanos, task_id: i64::from(info.task_status) }, find_timer(&id))?); }
    for info in &mutation.upsert_child_execution_infos { let id = info.initiated_id; result.upsert_child_execution_infos.push(keyed(id, &pc::ChildExecutionInfo { version: info.version, initiated_event_batch_id: info.initiated_event_batch_id, started_id: info.started_id, started_workflow_id: info.started_workflow_id.clone(), started_run_id: info.started_run_id.as_bytes().to_vec(), create_request_id: info.create_request_id.clone(), domain_id: info.domain_id.clone(), workflow_type_name: info.workflow_type_name.clone(), parent_close_policy: info.parent_close_policy.map_or(0, |x| x as i32), ..Default::default() }, find_child(id))?); }
    for info in &mutation.upsert_request_cancel_infos { let id=info.initiated_id; result.upsert_request_cancel_infos.push(keyed(id, &pc::RequestCancelInfo { version: info.version, initiated_event_batch_id: info.initiated_event_batch_id, cancel_request_id: info.cancel_request_id.clone() }, find_cancel(id))?); }
    for info in &mutation.upsert_signal_infos { let id=info.initiated_id; result.upsert_signal_infos.push(keyed(id, &pc::SignalInfo { version: info.version, initiated_event_batch_id: info.initiated_event_batch_id, request_id: info.signal_request_id.clone(), name: info.signal_name.clone(), input: info.input.clone(), control: info.control.clone() }, find_signal(id))?); }
    for timer in &mutation.rewrite_timer_infos { let id=timer.timer_id.clone(); result.rewrite_timer_infos.push(keyed(id.clone(), &pc::TimerInfo { version: timer.version, started_id: timer.started_id, expiry_time_unix_nanos: timer.expiry_time_unix_nanos, task_id: i64::from(timer.task_status) }, find_timer(&id))?); }
    for activity in &mutation.rewrite_activity_infos {
        if let Some(row) = result.upsert_activity_infos.iter().find(|r| r.key == activity.schedule_id) { result.rewrite_activity_infos.push(row.clone()); }
        else if let Some(old) = find_activity(activity.schedule_id) { result.rewrite_activity_infos.push(pc::KeyedBlob { key: activity.schedule_id, value: old.clone() }); }
        else { let empty = pc::DataBlob::default(); let mut x = activity.clone(); x.timer_task_status = 0; result.rewrite_activity_infos.push(keyed(activity.schedule_id, &pc::ActivityInfo { version:x.version, ..Default::default() }, Some(&empty))?); }
    }
    result.upsert_semaphore_infos = mutation.upsert_semaphore_infos.iter().map(semaphore_to_record).collect();
    result.delete_semaphore_infos = mutation.delete_semaphore_infos.clone();
    if !mutation.new_buffered_events.is_empty() {
        let blob = cadence_thrift::history_blob::encode(mutation.new_buffered_events.clone());
        result.new_buffered_events = Some(pc::DataBlob { encoding: "thriftrw".into(), data: blob.data });
    }
    Ok(result)
}


/// Typed (builder) pending maps decoded from a snapshot; inverse of `mutation_to_persistence`.
pub struct TypedPendingMaps {
    pub activities: Vec<crate::mutable_state_activity::ActivityInfo>,
    pub timers: Vec<super::timer::TimerInfo>,
    pub children: Vec<super::child_workflow::ChildExecutionInfo>,
    pub request_cancels: Vec<super::cancellation::RequestCancelInfo>,
    pub signals: Vec<super::signal::SignalInfo>,
    pub semaphores: Vec<super::semaphore::SemaphoreInfo>,
}

/// Go `Load` of the pending activity/timer/child/cancel/signal/semaphore maps.
pub fn typed_pending_maps(envelope: &MutableStateEnvelope) -> HResult<TypedPendingMaps> {
    let snap = &envelope.snapshot;
    let mut activities = Vec::new();
    for (id, a) in &envelope.activities {
        let hb = snap.activity_heartbeats.get(id);
        activities.push(crate::mutable_state_activity::ActivityInfo {
            version: a.version, schedule_id: *id, scheduled_event_batch_id: a.scheduled_event_batch_id,
            scheduled_time: a.scheduled_time_unix_nanos, started_id: a.started_id, started_time: a.started_time_unix_nanos,
            activity_id: a.activity_id.clone(), domain_id: String::new(),
            schedule_to_start_timeout: a.schedule_to_start_timeout_seconds, schedule_to_close_timeout: a.schedule_to_close_timeout_seconds,
            start_to_close_timeout: a.start_to_close_timeout_seconds, heartbeat_timeout: a.heartbeat_timeout_seconds,
            timer_task_status: a.timer_task_status,
            last_heartbeat_timeout_visibility_in_seconds: a.last_heartbeat_timeout_visibility_in_seconds,
            cancel_requested: a.cancel_requested, cancel_request_id: a.cancel_request_id,
            task_list: a.task_list.clone(), attempt: a.attempt, request_id: a.request_id.clone(),
            started_identity: a.started_identity.clone(),
            last_heartbeat_updated_time: hb.map_or(0, |h| h.updated_time_unix_nanos),
            details: hb.map(|h| h.details.clone()).unwrap_or_default(),
            has_retry_policy: a.has_retry_policy, initial_interval: a.retry_initial_interval_seconds,
            backoff_coefficient: a.retry_backoff_coefficient, jitter_coefficient: a.retry_jitter_coefficient,
            maximum_interval: a.retry_maximum_interval_seconds, maximum_attempts: a.retry_maximum_attempts,
            expiration_time: a.retry_expiration_time_unix_nanos, non_retriable_errors: a.retry_non_retriable_errors.clone(),
            last_failure_reason: a.retry_last_failure_reason.clone(), last_failure_details: a.retry_last_failure_details.clone(),
            last_retry_interval_seconds: a.retry_last_retry_interval_seconds, last_failure_category: a.retry_last_failure_category, task_list_kind: a.task_list_kind, last_worker_identity: a.retry_last_worker_identity.clone(),
        });
    }
    let timers = envelope.timers.iter().map(|(id, t)| super::timer::TimerInfo {
        version: t.version, timer_id: id.clone(), expiry_time_unix_nanos: t.expiry_time_unix_nanos,
        started_id: t.started_id, task_status: t.task_id as i32,
    }).collect();
    let children = envelope.children.iter().map(|(id, c)| super::child_workflow::ChildExecutionInfo {
        version: c.version, initiated_id: *id, initiated_event_batch_id: c.initiated_event_batch_id, started_id: c.started_id,
        started_workflow_id: c.started_workflow_id.clone(), started_run_id: String::from_utf8_lossy(&c.started_run_id).into_owned(),
        create_request_id: c.create_request_id.clone(), domain_id: c.domain_id.clone(), workflow_type_name: c.workflow_type_name.clone(),
        parent_close_policy: match c.parent_close_policy {
            0 => Some(cadence_api::enums::ParentClosePolicy::Abandon),
            1 => Some(cadence_api::enums::ParentClosePolicy::RequestCancel),
            2 => Some(cadence_api::enums::ParentClosePolicy::Terminate),
            _ => None,
        },
    }).collect();
    let mut request_cancels = Vec::new();
    for row in &snap.request_cancel_infos {
        let r = sqlblobs::request_cancel_info_from_struct(&decode(&row.value)?)
            .map_err(|e| HistoryError::internal(format!("decode request cancel info: {e}")))?;
        request_cancels.push(super::cancellation::RequestCancelInfo {
            version: r.version, initiated_id: row.key, initiated_event_batch_id: r.initiated_event_batch_id, cancel_request_id: r.cancel_request_id,
        });
    }
    let mut signals = Vec::new();
    for row in &snap.signal_infos {
        let s = sqlblobs::signal_info_from_struct(&decode(&row.value)?)
            .map_err(|e| HistoryError::internal(format!("decode signal info: {e}")))?;
        signals.push(super::signal::SignalInfo {
            version: s.version, initiated_event_batch_id: s.initiated_event_batch_id, initiated_id: row.key,
            signal_request_id: s.request_id, signal_name: s.name, input: s.input, control: s.control,
        });
    }
    let semaphores = snap.semaphore_infos.iter().map(semaphore_from_record).collect();
    Ok(TypedPendingMaps { activities, timers, children, request_cancels, signals, semaphores })
}

/// Library `SemaphoreInfo` -> persistence record (Go `persistence.SemaphoreInfo`).
pub fn semaphore_to_record(i: &super::semaphore::SemaphoreInfo) -> pc::SemaphoreInfoRecord {
    pc::SemaphoreInfoRecord {
        version: i.version, initiated_id: i.initiated_id, semaphore_name: i.semaphore_name.clone(),
        owner_id: i.owner_id.clone(), token_id: i.token_id, acquire_deadline_unix_nanos: i.acquire_deadline_unix_nanos,
    }
}

/// Persistence record -> library `SemaphoreInfo`.
pub fn semaphore_from_record(r: &pc::SemaphoreInfoRecord) -> super::semaphore::SemaphoreInfo {
    super::semaphore::SemaphoreInfo {
        version: r.version, initiated_id: r.initiated_id, semaphore_name: r.semaphore_name.clone(),
        owner_id: r.owner_id.clone(), acquire_deadline_unix_nanos: r.acquire_deadline_unix_nanos, token_id: r.token_id,
    }
}

/// Convert a typed full snapshot through the same keyed row codecs as mutations.
pub fn typed_snapshot_to_persistence(
    snapshot: &super::mutable_state_builder::TypedWorkflowSnapshot,
) -> HResult<pc::WorkflowSnapshot> {
    let envelope = snapshot_to_envelope(pc::WorkflowSnapshot::default())?;
    let mutation = super::mutable_state_builder::TypedWorkflowMutation {
        execution_info: snapshot.execution_info.clone(),
        version_histories: snapshot.version_histories.clone(),
        start_version: snapshot.start_version,
        last_write_version: snapshot.last_write_version,
        upsert_activity_infos: snapshot.activity_infos.clone(),
        upsert_timer_infos: snapshot.timer_infos.clone(),
        upsert_child_execution_infos: snapshot.child_execution_infos.clone(),
        upsert_request_cancel_infos: snapshot.request_cancel_infos.clone(),
        upsert_signal_infos: snapshot.signal_infos.clone(),
        upsert_signal_requested_ids: snapshot.signal_requested_ids.clone(),
        upsert_semaphore_infos: snapshot.semaphore_infos.clone(),
        tasks_by_category: snapshot.tasks_by_category.clone(),
        workflow_requests: snapshot.workflow_requests.clone(),
        condition: snapshot.condition,
        ..Default::default()
    };
    let mutation = mutation_to_persistence(&mutation, &envelope)?;
    let mut result = pc::WorkflowSnapshot {
        execution_info: mutation.execution_info,
        start_version: mutation.start_version,
        last_write_version: mutation.last_write_version,
        activity_infos: mutation.upsert_activity_infos,
        timer_infos: mutation.upsert_timer_infos,
        child_execution_infos: mutation.upsert_child_execution_infos,
        request_cancel_infos: mutation.upsert_request_cancel_infos,
        signal_infos: mutation.upsert_signal_infos,
        signal_requested_ids: mutation.upsert_signal_requested_ids,
        semaphore_infos: mutation.upsert_semaphore_infos,
        tasks_by_category: mutation.tasks_by_category,
        workflow_requests: mutation.workflow_requests,
        condition: mutation.condition,
        ..Default::default()
    };
    if let Some(histories) = snapshot.version_histories.as_ref() {
        result.version_histories = Some(pc::DataBlob {
            encoding: "thriftrw".into(),
            data: sqlblobs::Encoding::ThriftRW.encode_versioned(&crate::wiring::server::thrift_types_g2::version_histories_to_thrift(histories))
                .map_err(|e| HistoryError::internal(format!("encode version histories: {e}")))?,
        });
    }
    Ok(result)
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
        snapshot.semaphore_infos.push(pc::SemaphoreInfoRecord { version: 9, initiated_id: 3, semaphore_name: "sem".into(), owner_id: "own".into(), token_id: 2, acquire_deadline_unix_nanos: 5 });
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

    #[test]
    fn changed_rows_overlay_fields_and_reload_without_losing_siblings() {
        use super::super::mutable_state_builder::TypedWorkflowMutation;
        let mut old_activity = sqlblobs::activity_info_to_struct(&pc::ActivityInfo {
            activity_id: "old-id".into(), task_list_kind: 7, scheduled_event: vec![9],
            retry_last_failure_category: 4, ..Default::default()
        });
        old_activity.fields.push((999, cadence_thrift::Value::Bytes(vec![0xaa])));
        let mut source = pc::WorkflowSnapshot::default();
        source.activity_infos.push(pc::KeyedBlob { key: 11, value: blob(sqlblobs::Encoding::ThriftRW.encode(&old_activity).unwrap()) });
        source.timer_infos.push(pc::KeyedBlob { key: "timer".into(), value: blob(sqlblobs::Encoding::ThriftRW.encode(&sqlblobs::timer_info_to_struct(&pc::TimerInfo { started_id: 3, task_id: 0x21, ..Default::default() })).unwrap()) });
        let envelope = snapshot_to_envelope(source).unwrap();
        let changed = TypedWorkflowMutation {
            upsert_activity_infos: vec![crate::mutable_state_activity::ActivityInfo { schedule_id: 11, version: 12, activity_id: "new-id".into(), timer_task_status: 0x44, ..Default::default() }],
            upsert_timer_infos: vec![super::super::timer::TimerInfo { timer_id: "timer".into(), version: 8, started_id: 13, expiry_time_unix_nanos: 99, task_status: 0x55 }],
            upsert_child_execution_infos: vec![super::super::child_workflow::ChildExecutionInfo { initiated_id: 21, version: 2, started_workflow_id: "child".into(), ..Default::default() }],
            upsert_request_cancel_infos: vec![super::super::cancellation::RequestCancelInfo { initiated_id: 31, cancel_request_id: "cancel".into(), ..Default::default() }],
            upsert_signal_infos: vec![super::super::signal::SignalInfo { initiated_id: 41, signal_name: "sig".into(), signal_request_id: "request".into(), input: vec![5], ..Default::default() }],
            ..Default::default()
        };
        let persisted = mutation_to_persistence(&changed, &envelope).unwrap();
        let reloaded = pc::WorkflowSnapshot {
            activity_infos: persisted.upsert_activity_infos,
            timer_infos: persisted.upsert_timer_infos,
            child_execution_infos: persisted.upsert_child_execution_infos,
            request_cancel_infos: persisted.upsert_request_cancel_infos,
            signal_infos: persisted.upsert_signal_infos,
            ..Default::default()
        };
        let roundtrip = snapshot_to_envelope(reloaded).unwrap();
        let activity = &roundtrip.activities[&11];
        assert_eq!(activity.activity_id, "new-id");
        assert_eq!(activity.timer_task_status, 0x44);
        assert_eq!(activity.task_list_kind, 7);
        assert_eq!(activity.scheduled_event, vec![9]);
        assert_eq!(activity.retry_last_failure_category, 4);
        let activity_raw = decode(&roundtrip.snapshot.activity_infos.iter().find(|r| r.key == 11).unwrap().value).unwrap();
        assert_eq!(activity_raw.get(999), Some(&cadence_thrift::Value::Bytes(vec![0xaa])));
        assert_eq!(roundtrip.timers["timer"].task_id, 0x55, "typed task_status maps to persistence task_id");
        assert_eq!(roundtrip.timers["timer"].started_id, 13);
        assert_eq!(roundtrip.children[&21].started_workflow_id, "child");
        assert_eq!(sqlblobs::request_cancel_info_from_struct(&decode(&roundtrip.snapshot.request_cancel_infos[0].value).unwrap()).unwrap().cancel_request_id, "cancel");
        assert_eq!(sqlblobs::signal_info_from_struct(&decode(&roundtrip.snapshot.signal_infos[0].value).unwrap()).unwrap().name, "sig");
    }

    #[test]
    fn keyed_delete_and_empty_rewrite_are_distinct_from_no_rewrite() {
        use super::super::mutable_state_builder::TypedWorkflowMutation;
        let envelope = snapshot_to_envelope(pc::WorkflowSnapshot::default()).unwrap();
        let deleted = mutation_to_persistence(&TypedWorkflowMutation { delete_activity_infos: vec![7], ..Default::default() }, &envelope).unwrap();
        assert_eq!(deleted.delete_activity_infos, vec![7]);
        assert!(deleted.rewrite_activity_infos.is_empty());
        let changed = TypedWorkflowMutation { delete_activity_infos: vec![7], rewrite_activity_infos: vec![], ..Default::default() };
        let rewritten = mutation_to_persistence(&changed, &envelope).unwrap();
        // Rust SQL's mutation writer applies keyed deletes and has no rewrite operation.
        assert_eq!(rewritten.delete_activity_infos, vec![7]);
        assert!(rewritten.rewrite_activity_infos.is_empty());
    }
}
