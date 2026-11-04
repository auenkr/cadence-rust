//! Mutable workflow state primitives corresponding to Go's
//! `service/history/execution/mutable_state_builder.go`.
//!
//! Event-specific transitions and task generation are layered on this state
//! container. Values in the pending maps are JSON-compatible persistence
//! records until the history service's persistence contract is fully ported.

use std::collections::{BTreeMap, BTreeSet};

use cadence_api::{service_types::HistoryEvent, types::VersionHistories};
use serde::{Deserialize, Serialize};

pub type Record = serde_json::Value;

/// Local transaction record for the decision task manager. Kept in history
/// until the persistence crate's matching contract is ported.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionInfo {
    pub schedule_id: i64,
    pub started_id: i64,
    pub version: i64,
    pub request_id: String,
    pub task_list: String,
    pub start_to_close_timeout_seconds: i32,
    pub attempt: i64,
    pub schedule_timestamp: i64,
    pub original_schedule_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MutableStateError {
    #[error("event id {actual} does not match next event id {expected}")]
    UnexpectedEventId { expected: i64, actual: i64 },
    #[error("cannot create a snapshot with buffered events")]
    SnapshotWithBufferedEvents,
    #[error("cannot create a snapshot with transient event batches")]
    SnapshotWithTransientEvents,
}

/// A close-transaction persistence delta. The buckets mirror Go's
/// `persistence.WorkflowMutation`; the service-specific task and checksum
/// policies are supplied by later history phases.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowMutation {
    pub execution_info: Record,
    pub version_histories: Option<VersionHistories>,
    pub upsert_activity_infos: Vec<(i64, Record)>,
    pub delete_activity_infos: Vec<i64>,
    pub upsert_timer_infos: Vec<(String, Record)>,
    pub delete_timer_infos: Vec<String>,
    pub upsert_child_execution_infos: Vec<(i64, Record)>,
    pub delete_child_execution_infos: Vec<i64>,
    pub upsert_request_cancel_infos: Vec<(i64, Record)>,
    pub delete_request_cancel_infos: Vec<i64>,
    pub upsert_signal_infos: Vec<(i64, Record)>,
    pub delete_signal_infos: Vec<i64>,
    pub upsert_signal_requested_ids: Vec<String>,
    pub delete_signal_requested_ids: Vec<String>,
    pub new_buffered_events: Vec<HistoryEvent>,
    pub clear_buffered_events: bool,
    pub condition: i64,
    pub checksum: Option<Vec<u8>>,
    pub decision_info: Option<DecisionInfo>,
    pub activity_id_to_event_id: BTreeMap<String, i64>,
}

/// Full pending-state image, corresponding to Go's
/// `persistence.WorkflowSnapshot`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowSnapshot {
    pub execution_info: Record,
    pub version_histories: Option<VersionHistories>,
    pub activity_infos: Vec<(i64, Record)>,
    pub timer_infos: Vec<(String, Record)>,
    pub child_execution_infos: Vec<(i64, Record)>,
    pub request_cancel_infos: Vec<(i64, Record)>,
    pub signal_infos: Vec<(i64, Record)>,
    pub signal_requested_ids: Vec<String>,
    pub buffered_events: Vec<HistoryEvent>,
    pub condition: i64,
    pub checksum: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventBatch {
    pub events: Vec<HistoryEvent>,
}

/// State held by a single history workflow transaction. `execution_info` is
/// deliberately opaque pending a dedicated Rust history persistence record;
/// event payloads use the repository's generated Cadence API types.
#[derive(Debug, Clone, PartialEq)]
pub struct MutableStateBuilder {
    pub execution_info: Record,
    pub pending_activity_infos: BTreeMap<i64, Record>,
    pub update_activity_infos: BTreeMap<i64, Record>,
    pub delete_activity_infos: BTreeSet<i64>,
    pub pending_timer_infos: BTreeMap<String, Record>,
    pub update_timer_infos: BTreeMap<String, Record>,
    pub delete_timer_infos: BTreeSet<String>,
    pub pending_child_execution_infos: BTreeMap<i64, Record>,
    pub update_child_execution_infos: BTreeMap<i64, Record>,
    pub delete_child_execution_infos: BTreeSet<i64>,
    pub pending_request_cancel_infos: BTreeMap<i64, Record>,
    pub update_request_cancel_infos: BTreeMap<i64, Record>,
    pub delete_request_cancel_infos: BTreeSet<i64>,
    pub pending_signal_infos: BTreeMap<i64, Record>,
    pub update_signal_infos: BTreeMap<i64, Record>,
    pub delete_signal_infos: BTreeSet<i64>,
    pub pending_signal_requested_ids: BTreeSet<String>,
    pub update_signal_requested_ids: BTreeSet<String>,
    pub delete_signal_requested_ids: BTreeSet<String>,
    pub buffered_events: Vec<HistoryEvent>,
    pub update_buffered_events: Vec<HistoryEvent>,
    pub clear_buffered_events: bool,
    pub version_histories: Option<VersionHistories>,
    pub history: Vec<HistoryEvent>,
    pub next_event_id: i64,
    pub next_event_id_in_db: i64,
    pub checksum: Option<Vec<u8>>,
}

impl Default for MutableStateBuilder {
    fn default() -> Self {
        Self {
            execution_info: Record::Object(Default::default()),
            pending_activity_infos: BTreeMap::new(), update_activity_infos: BTreeMap::new(), delete_activity_infos: BTreeSet::new(),
            pending_timer_infos: BTreeMap::new(), update_timer_infos: BTreeMap::new(), delete_timer_infos: BTreeSet::new(),
            pending_child_execution_infos: BTreeMap::new(), update_child_execution_infos: BTreeMap::new(), delete_child_execution_infos: BTreeSet::new(),
            pending_request_cancel_infos: BTreeMap::new(), update_request_cancel_infos: BTreeMap::new(), delete_request_cancel_infos: BTreeSet::new(),
            pending_signal_infos: BTreeMap::new(), update_signal_infos: BTreeMap::new(), delete_signal_infos: BTreeSet::new(),
            pending_signal_requested_ids: BTreeSet::new(), update_signal_requested_ids: BTreeSet::new(), delete_signal_requested_ids: BTreeSet::new(),
            buffered_events: Vec::new(), update_buffered_events: Vec::new(), clear_buffered_events: false,
            version_histories: None, history: Vec::new(), next_event_id: 1, next_event_id_in_db: 0, checksum: None,
            decision_info: None, activity_id_to_event_id: BTreeMap::new(),
        }
    }
}

impl MutableStateBuilder {
    /// Append an event created locally. IDs are allocated by the builder, as
    /// in Go's history builder, and `next_event_id` advances after append.
    pub fn add_event(&mut self, mut event: HistoryEvent) -> HistoryEvent {
        event.event_id = self.next_event_id;
        self.next_event_id += 1;
        self.history.push(event.clone());
        event
    }

    /// Apply an event received from replication while enforcing contiguous
    /// event IDs. Duplicate/reordered application is rejected for the caller
    /// to handle according to the replication protocol.
    pub fn replicate_event(&mut self, event: HistoryEvent) -> Result<(), MutableStateError> {
        if event.event_id != self.next_event_id {
            return Err(MutableStateError::UnexpectedEventId { expected: self.next_event_id, actual: event.event_id });
        }
        self.next_event_id += 1;
        self.history.push(event);
        Ok(())
    }

    pub fn upsert_activity(&mut self, id: i64, value: Record) { self.pending_activity_infos.insert(id, value.clone()); self.update_activity_infos.insert(id, value); self.delete_activity_infos.remove(&id); }
    pub fn delete_activity(&mut self, id: i64) { self.pending_activity_infos.remove(&id); self.update_activity_infos.remove(&id); self.delete_activity_infos.insert(id); }
    pub fn upsert_timer(&mut self, id: String, value: Record) { self.pending_timer_infos.insert(id.clone(), value.clone()); self.update_timer_infos.insert(id.clone(), value); self.delete_timer_infos.remove(&id); }
    pub fn delete_timer(&mut self, id: String) { self.pending_timer_infos.remove(&id); self.update_timer_infos.remove(&id); self.delete_timer_infos.insert(id); }

    pub fn get_decision_info(&self, schedule_id: i64) -> Option<&DecisionInfo> {
        self.decision_info.as_ref().filter(|d| d.schedule_id == schedule_id)
    }
    pub fn pending_decision(&self) -> Option<&DecisionInfo> {
        self.decision_info.as_ref().filter(|d| d.started_id == 0)
    }
    pub fn in_flight_decision(&self) -> Option<&DecisionInfo> {
        self.decision_info.as_ref().filter(|d| d.started_id != 0)
    }
    pub fn has_processed_or_pending_decision(&self) -> bool {
        self.execution_info.get("decision_attempt").and_then(serde_json::Value::as_i64).unwrap_or_default() > 0
            || self.decision_info.is_some()
    }
    pub fn update_decision(&mut self, info: DecisionInfo) { self.decision_info = Some(info); }
    pub fn delete_decision(&mut self) { self.decision_info = None; }
    pub fn fail_decision(&mut self, increment_attempt: bool) {
        self.decision_info = None;
        if increment_attempt {
            let attempt = self.execution_info.get("decision_attempt").and_then(serde_json::Value::as_i64).unwrap_or_default() + 1;
            self.execution_info["decision_attempt"] = attempt.into();
        }
    }

    /// Schedule a decision event and retain its mutable manager state.
    pub fn add_decision_scheduled(&mut self, version: i64, task_list: String, timeout: i32, attempt: i64, schedule_timestamp: i64, original_schedule_timestamp: i64) -> (HistoryEvent, DecisionInfo) {
        use cadence_api::{enums::EventType, tasks::{DecisionTaskScheduledEventAttributes, TaskList}};
        let mut event = self.add_event(HistoryEvent { event_type: Some(EventType::DecisionTaskScheduled), ..Default::default() });
        event.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes {
            task_list: Some(TaskList { name: task_list.clone() }), start_to_close_timeout_seconds: Some(timeout), attempt,
        });
        let info = DecisionInfo { schedule_id: event.event_id, version, task_list, start_to_close_timeout_seconds: timeout, attempt, schedule_timestamp, original_schedule_timestamp, ..Default::default() };
        self.decision_info = Some(info.clone()); (event, info)
    }
    pub fn start_decision(&mut self, schedule_id: i64, request_id: String, version: i64) -> Option<(HistoryEvent, DecisionInfo)> {
        use cadence_api::{enums::EventType, tasks::DecisionTaskStartedEventAttributes};
        let mut info = self.decision_info.clone()?;
        if info.schedule_id != schedule_id || info.started_id != 0 { return None; }
        let mut event = self.add_event(HistoryEvent { event_type: Some(EventType::DecisionTaskStarted), ..Default::default() });
        event.decision_task_started_event_attributes = Some(DecisionTaskStartedEventAttributes { scheduled_event_id: schedule_id, request_id: request_id.clone(), ..Default::default() });
        info.started_id = event.event_id; info.request_id = request_id; info.version = version;
        self.decision_info = Some(info.clone()); Some((event, info))
    }

    /// Apply an activity schedule event to the pending activity indexes.
    pub fn track_activity_scheduled(&mut self, event: &HistoryEvent, record: Record) {
        let Some(attrs) = event.activity_task_scheduled_event_attributes.as_ref() else { return };
        let id = event.event_id;
        self.upsert_activity(id, record);
        self.activity_id_to_event_id.insert(attrs.activity_id.clone(), id);
    }
    pub fn activity_event_id(&self, activity_id: &str) -> Option<i64> { self.activity_id_to_event_id.get(activity_id).copied() }
    pub fn remove_activity(&mut self, schedule_id: i64) {
        if let Some(info) = self.pending_activity_infos.remove(&schedule_id) {
            if let Some(id) = info.get("activity_id").and_then(serde_json::Value::as_str) { self.activity_id_to_event_id.remove(id); }
        }
        self.delete_activity(schedule_id);
    }

    pub fn close_as_mutation(&self, checksum: Option<Vec<u8>>) -> WorkflowMutation {
        WorkflowMutation {
            execution_info: self.execution_info.clone(), version_histories: self.version_histories.clone(),
            upsert_activity_infos: self.update_activity_infos.iter().map(|(k,v)| (*k,v.clone())).collect(), delete_activity_infos: self.delete_activity_infos.iter().copied().collect(),
            upsert_timer_infos: self.update_timer_infos.iter().map(|(k,v)| (k.clone(),v.clone())).collect(), delete_timer_infos: self.delete_timer_infos.iter().cloned().collect(),
            upsert_child_execution_infos: self.update_child_execution_infos.iter().map(|(k,v)| (*k,v.clone())).collect(), delete_child_execution_infos: self.delete_child_execution_infos.iter().copied().collect(),
            upsert_request_cancel_infos: self.update_request_cancel_infos.iter().map(|(k,v)| (*k,v.clone())).collect(), delete_request_cancel_infos: self.delete_request_cancel_infos.iter().copied().collect(),
            upsert_signal_infos: self.update_signal_infos.iter().map(|(k,v)| (*k,v.clone())).collect(), delete_signal_infos: self.delete_signal_infos.iter().copied().collect(),
            upsert_signal_requested_ids: self.update_signal_requested_ids.iter().cloned().collect(), delete_signal_requested_ids: self.delete_signal_requested_ids.iter().cloned().collect(),
            new_buffered_events: self.update_buffered_events.clone(), clear_buffered_events: self.clear_buffered_events,
            condition: self.next_event_id_in_db, checksum,
        }
    }

    pub fn close_as_snapshot(&self, transient_event_batches: usize, checksum: Option<Vec<u8>>) -> Result<WorkflowSnapshot, MutableStateError> {
        if transient_event_batches > 1 { return Err(MutableStateError::SnapshotWithTransientEvents); }
        if !self.buffered_events.is_empty() || !self.update_buffered_events.is_empty() { return Err(MutableStateError::SnapshotWithBufferedEvents); }
        Ok(WorkflowSnapshot {
            execution_info: self.execution_info.clone(), version_histories: self.version_histories.clone(),
            activity_infos: self.pending_activity_infos.iter().map(|(k,v)| (*k,v.clone())).collect(),
            timer_infos: self.pending_timer_infos.iter().map(|(k,v)| (k.clone(),v.clone())).collect(),
            child_execution_infos: self.pending_child_execution_infos.iter().map(|(k,v)| (*k,v.clone())).collect(),
            request_cancel_infos: self.pending_request_cancel_infos.iter().map(|(k,v)| (*k,v.clone())).collect(),
            signal_infos: self.pending_signal_infos.iter().map(|(k,v)| (*k,v.clone())).collect(),
            signal_requested_ids: self.pending_signal_requested_ids.iter().cloned().collect(), buffered_events: vec![],
            condition: self.next_event_id_in_db, checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::enums::EventType;

    #[test]
    fn local_and_replicated_events_advance_contiguous_ids() {
        let mut s = MutableStateBuilder::default();
        let local = s.add_event(HistoryEvent { event_type: Some(EventType::WorkflowExecutionStarted), ..Default::default() });
        assert_eq!(local.event_id, 1);
        let replicated = HistoryEvent { event_id: 2, event_type: Some(EventType::DecisionTaskScheduled), ..Default::default() };
        s.replicate_event(replicated).unwrap();
        assert_eq!(s.next_event_id, 3);
        assert_eq!(s.history.len(), 2);
    }

    #[test]
    fn mutation_tracks_upsert_delete_and_snapshot_has_full_maps() {
        let mut s = MutableStateBuilder::default();
        s.upsert_activity(4, serde_json::json!({"activityId":"a"}));
        s.delete_activity(4);
        s.upsert_timer("timer".into(), serde_json::json!({"eventId":3}));
        let mutation = s.close_as_mutation(None);
        assert_eq!(mutation.delete_activity_infos, vec![4]);
        assert!(mutation.upsert_activity_infos.is_empty());
        let snapshot = s.close_as_snapshot(1, None).unwrap();
        assert_eq!(snapshot.timer_infos.len(), 1);
    }

    #[test]
    fn snapshot_rejects_buffered_events_and_multiple_batches() {
        let mut s = MutableStateBuilder::default();
        s.buffered_events.push(HistoryEvent::default());
        assert_eq!(s.close_as_snapshot(1, None), Err(MutableStateError::SnapshotWithBufferedEvents));
        s.buffered_events.clear();
        assert_eq!(s.close_as_snapshot(2, None), Err(MutableStateError::SnapshotWithTransientEvents));
    }

    #[test]
    fn decision_manager_tracks_pending_started_and_failure_attempt() {
        let mut s = MutableStateBuilder::default();
        let (scheduled, info) = s.add_decision_scheduled(9, "workflow".into(), 10, 0, 100, 100);
        assert_eq!(scheduled.event_id, info.schedule_id);
        assert!(s.pending_decision().is_some());
        let (_, started) = s.start_decision(info.schedule_id, "request".into(), 9).unwrap();
        assert_eq!(started.started_id, 2);
        assert!(s.in_flight_decision().is_some());
        s.fail_decision(true);
        assert!(s.decision_info.is_none());
        assert_eq!(s.execution_info["decision_attempt"], 1);
    }

    #[test]
    fn activity_schedule_indexes_activity_id_and_delete_clears_it() {
        let mut s = MutableStateBuilder::default();
        let event = HistoryEvent {
            event_id: 10,
            activity_task_scheduled_event_attributes: Some(cadence_api::tasks::ActivityTaskScheduledEventAttributes { activity_id: "activity".into(), ..Default::default() }),
            ..Default::default()
        };
        s.track_activity_scheduled(&event, serde_json::json!({"activity_id":"activity"}));
        assert_eq!(s.activity_event_id("activity"), Some(10));
        s.remove_activity(10);
        assert_eq!(s.activity_event_id("activity"), None);
    }
}
