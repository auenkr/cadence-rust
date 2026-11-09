//! Activity state transitions corresponding to Go's
//! `mutable_state_builder_methods_activity.go`.
//!
//! The history crate is being migrated incrementally. This module owns the
//! activity persistence record and the replication/state-index operations so
//! event transition work does not conflict with decision transitions.

use cadence_api::service_types::HistoryEvent;
use serde::{Deserialize, Serialize};

use crate::mutable_state::{MutableStateBuilder, Record};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivityTransitionError {
    #[error("activity info for schedule event {0} is missing")]
    MissingActivityInfo(i64),
}

/// Activity fields retained by the Go mutable-state builder. Time values are
/// Unix nanoseconds, matching the generated event timestamp representation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivityInfo {
    pub version: i64,
    pub schedule_id: i64,
    pub scheduled_event_batch_id: i64,
    pub scheduled_time: i64,
    pub started_id: i64,
    pub started_time: i64,
    pub activity_id: String,
    pub domain_id: String,
    pub schedule_to_start_timeout: i32,
    pub schedule_to_close_timeout: i32,
    pub start_to_close_timeout: i32,
    pub heartbeat_timeout: i32,
    /// Go ActivityInfo.TimerTaskStatus bitset, persisted across transactions.
    #[serde(default)]
    pub timer_task_status: i32,
    /// Go LastHeartbeatTimeoutVisibilityInSeconds.
    #[serde(default)]
    pub last_heartbeat_timeout_visibility_in_seconds: i64,
    pub cancel_requested: bool,
    pub cancel_request_id: i64,
    pub task_list: String,
    pub attempt: i32,
    pub request_id: String,
    pub started_identity: String,
    pub last_heartbeat_updated_time: i64,
    pub details: Vec<u8>,
    pub has_retry_policy: bool,
    pub initial_interval: i32,
    pub backoff_coefficient: f64,
    pub jitter_coefficient: f64,
    pub maximum_interval: i32,
    pub maximum_attempts: i32,
    pub expiration_time: i64,
    pub non_retriable_errors: Vec<String>,
    pub last_failure_reason: String,
    pub last_failure_details: Vec<u8>,
    pub last_retry_interval_seconds: i32,
    #[serde(default)]
    pub last_worker_identity: String,
}

impl MutableStateBuilder {
    /// Go GetActivityInfo returns the pending record and presence bit.
    pub fn get_activity_info(&self, schedule_id: i64) -> Option<ActivityInfo> {
        self.pending_activity_infos.get(&schedule_id).and_then(decode)
    }

    /// Go GetActivityByActivityID follows the activity-ID index.
    pub fn get_activity_by_activity_id(&self, activity_id: &str) -> Option<ActivityInfo> {
        self.activity_id_to_event_id.get(activity_id).and_then(|id| self.get_activity_info(*id))
    }

    /// Go UpdateActivity updates both the in-memory lookup and persistence delta.
    pub fn update_activity(&mut self, info: ActivityInfo) -> Result<(), ActivityTransitionError> {
        if !self.pending_activity_infos.contains_key(&info.schedule_id) {
            return Err(ActivityTransitionError::MissingActivityInfo(info.schedule_id));
        }
        self.upsert_activity(info.schedule_id, encode(&info));
        Ok(())
    }

    /// Go UpdateActivityProgress updates heartbeat details and marks the
    /// schedule for persistence. `updated_at` is Unix nanoseconds.
    pub fn update_activity_progress(&mut self, info: &mut ActivityInfo, version: i64, details: Vec<u8>, updated_at: i64) {
        info.version = version;
        info.details = details;
        info.last_heartbeat_updated_time = updated_at;
        self.upsert_activity(info.schedule_id, encode(info));
    }

    /// Go DeleteActivity clears both activity indexes and records a delete.
    /// Missing records are tolerated by Go (with an inconsistency log).
    pub fn delete_activity_info(&mut self, schedule_id: i64) {
        if let Some(info) = self.get_activity_info(schedule_id) {
            self.activity_id_to_event_id.remove(&info.activity_id);
        }
        self.remove_activity(schedule_id);
    }

    /// Apply a scheduled event received from the active cluster. Go derives
    /// all pending attributes from this event and indexes by both IDs.
    pub fn replicate_activity_task_scheduled(&mut self, first_event_id: i64, event: &HistoryEvent, domain_id: String) -> Option<ActivityInfo> {
        let attrs = event.activity_task_scheduled_event_attributes.as_ref()?;
        let info = ActivityInfo {
            version: event.version,
            schedule_id: event.event_id,
            scheduled_event_batch_id: first_event_id,
            scheduled_time: event.timestamp.unwrap_or_default(),
            started_id: 0,
            activity_id: attrs.activity_id.clone(),
            domain_id,
            schedule_to_start_timeout: attrs.schedule_to_start_timeout_seconds.unwrap_or_default(),
            schedule_to_close_timeout: attrs.schedule_to_close_timeout_seconds.unwrap_or_default(),
            start_to_close_timeout: attrs.start_to_close_timeout_seconds.unwrap_or_default(),
            heartbeat_timeout: attrs.heartbeat_timeout_seconds.unwrap_or_default(),
            task_list: attrs.task_list.as_ref().map(|v| v.name.clone()).unwrap_or_default(),
            has_retry_policy: attrs.retry_policy.is_some(),
            ..Default::default()
        };
        self.activity_id_to_event_id.insert(info.activity_id.clone(), info.schedule_id);
        self.upsert_activity(info.schedule_id, encode(&info));
        Some(info)
    }

    /// Apply a started event, preserving schedule attributes and updating the
    /// version, request ID and heartbeat baseline like Go.
    pub fn replicate_activity_task_started(&mut self, event: &HistoryEvent) -> Result<(), ActivityTransitionError> {
        let attrs = event.activity_task_started_event_attributes.as_ref().ok_or(ActivityTransitionError::MissingActivityInfo(0))?;
        let id = attrs.scheduled_event_id;
        let mut info = self.get_activity_info(id).ok_or(ActivityTransitionError::MissingActivityInfo(id))?;
        info.version = event.version;
        info.started_id = event.event_id;
        info.request_id = attrs.request_id.clone();
        info.started_time = event.timestamp.unwrap_or_default();
        info.last_heartbeat_updated_time = info.started_time;
        self.upsert_activity(id, encode(&info));
        Ok(())
    }

    pub fn replicate_activity_task_completed(&mut self, event: &HistoryEvent) {
        if let Some(a) = &event.activity_task_completed_event_attributes { self.delete_activity_info(a.scheduled_event_id); }
    }
    pub fn replicate_activity_task_failed(&mut self, event: &HistoryEvent) {
        if let Some(a) = &event.activity_task_failed_event_attributes { self.delete_activity_info(a.scheduled_event_id); }
    }
    pub fn replicate_activity_task_timed_out(&mut self, event: &HistoryEvent) {
        if let Some(a) = &event.activity_task_timed_out_event_attributes { self.delete_activity_info(a.scheduled_event_id); }
    }
    pub fn replicate_activity_task_canceled(&mut self, event: &HistoryEvent) {
        if let Some(a) = &event.activity_task_canceled_event_attributes { self.delete_activity_info(a.scheduled_event_id); }
    }

    /// Go tolerates cancel-request events for activities absent on the passive
    /// cluster; otherwise it marks the pending record and persists the event ID.
    pub fn replicate_activity_task_cancel_requested(&mut self, event: &HistoryEvent) {
        let Some(attrs) = &event.activity_task_cancel_requested_event_attributes else { return };
        let Some(mut info) = self.get_activity_by_activity_id(&attrs.activity_id) else { return };
        info.version = event.version;
        info.cancel_requested = true;
        info.cancel_request_id = event.event_id;
        self.upsert_activity(info.schedule_id, encode(&info));
    }
}

fn encode(info: &ActivityInfo) -> Record { serde_json::to_value(info).unwrap_or(Record::Null) }
fn decode(record: &Record) -> Option<ActivityInfo> { serde_json::from_value(record.clone()).ok() }

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::enums::EventType;

    fn scheduled(id: i64, activity_id: &str) -> HistoryEvent {
        HistoryEvent {
            event_id: id,
            event_type: Some(EventType::ActivityTaskScheduled),
            activity_task_scheduled_event_attributes: Some(cadence_api::tasks::ActivityTaskScheduledEventAttributes {
                activity_id: activity_id.into(), ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn state() -> MutableStateBuilder {
        let mut s = MutableStateBuilder::default();
        s.replicate_activity_task_scheduled(1, &scheduled(2, "a"), "domain".into());
        s
    }

    #[test]
    fn get_activity_info_and_by_id_follow_indexes() {
        let s = state();
        assert_eq!(s.get_activity_info(2).unwrap().activity_id, "a");
        assert_eq!(s.get_activity_by_activity_id("a").unwrap().schedule_id, 2);
        assert!(s.get_activity_info(3).is_none());
    }
    #[test]
    fn update_activity_requires_existing_and_updates_persistence_delta() {
        let mut s = state();
        let mut info = s.get_activity_info(2).unwrap();
        info.attempt = 3;
        s.update_activity(info).unwrap();
        assert_eq!(s.get_activity_info(2).unwrap().attempt, 3);
        assert!(matches!(s.update_activity(ActivityInfo { schedule_id: 8, ..Default::default() }), Err(ActivityTransitionError::MissingActivityInfo(8))));
    }
    #[test]
    fn update_progress_records_version_details_and_heartbeat_time() {
        let mut s = state();
        let mut info = s.get_activity_info(2).unwrap();
        s.update_activity_progress(&mut info, 7, vec![1, 2], 99);
        let saved = s.get_activity_info(2).unwrap();
        assert_eq!((saved.version, saved.details, saved.last_heartbeat_updated_time), (7, vec![1, 2], 99));
    }
    #[test]
    fn delete_activity_clears_activity_id_and_sets_delete_delta() {
        let mut s = state();
        s.delete_activity_info(2);
        assert!(s.get_activity_info(2).is_none());
        assert!(s.get_activity_by_activity_id("a").is_none());
        assert!(s.delete_activity_infos.contains(&2));
    }
    #[test]
    fn replicate_scheduled_uses_event_fields_and_updates_indexes() {
        let s = state();
        let info = s.get_activity_info(2).unwrap();
        assert_eq!((info.scheduled_event_batch_id, info.activity_id.as_str()), (1, "a"));
        assert_eq!(s.activity_id_to_event_id.get("a"), Some(&2));
    }
    #[test]
    fn replicate_started_updates_started_metadata() {
        let mut s = state();
        let event = HistoryEvent { event_id: 4, version: 8, timestamp: Some(123), activity_task_started_event_attributes: Some(cadence_api::tasks::ActivityTaskStartedEventAttributes { scheduled_event_id: 2, request_id: "req".into(), ..Default::default() }), ..Default::default() };
        s.replicate_activity_task_started(&event).unwrap();
        let info = s.get_activity_info(2).unwrap();
        assert_eq!((info.version, info.started_id, info.request_id.as_str(), info.started_time, info.last_heartbeat_updated_time), (8, 4, "req", 123, 123));
    }
    #[test]
    fn replicate_cancel_requested_updates_pending_record() {
        let mut s = state();
        let event = HistoryEvent { event_id: 5, version: 8, activity_task_cancel_requested_event_attributes: Some(cadence_api::tasks::ActivityTaskCancelRequestedEventAttributes { activity_id: "a".into(), ..Default::default() }), ..Default::default() };
        s.replicate_activity_task_cancel_requested(&event);
        let info = s.get_activity_info(2).unwrap();
        assert!(info.cancel_requested);
        assert_eq!((info.version, info.cancel_request_id), (8, 5));
    }
    #[test]
    fn replicate_terminal_activity_events_delete_pending_records() {
        let event = scheduled(2, "a");
        let mut s = state();
        s.replicate_activity_task_completed(&HistoryEvent { activity_task_completed_event_attributes: Some(cadence_api::tasks::ActivityTaskCompletedEventAttributes { scheduled_event_id: event.event_id, ..Default::default() }), ..Default::default() });
        assert!(s.get_activity_info(2).is_none());
        for terminal in [
            HistoryEvent { activity_task_failed_event_attributes: Some(cadence_api::tasks::ActivityTaskFailedEventAttributes { scheduled_event_id: 2, ..Default::default() }), ..Default::default() },
            HistoryEvent { activity_task_timed_out_event_attributes: Some(cadence_api::tasks::ActivityTaskTimedOutEventAttributes { scheduled_event_id: 2, ..Default::default() }), ..Default::default() },
            HistoryEvent { activity_task_canceled_event_attributes: Some(cadence_api::tasks::ActivityTaskCanceledEventAttributes { scheduled_event_id: 2, ..Default::default() }), ..Default::default() },
        ] {
            let mut s = state();
            match terminal.event_type {
                _ if terminal.activity_task_failed_event_attributes.is_some() => s.replicate_activity_task_failed(&terminal),
                _ if terminal.activity_task_timed_out_event_attributes.is_some() => s.replicate_activity_task_timed_out(&terminal),
                _ => s.replicate_activity_task_canceled(&terminal),
            }
            assert!(s.get_activity_info(2).is_none());
        }
    }
    #[test]
    fn missing_activity_update_returns_mutable_state_error() {
        let mut s = MutableStateBuilder::default();
        assert_eq!(s.replicate_activity_task_started(&HistoryEvent::default()), Err(ActivityTransitionError::MissingActivityInfo(0)));
    }
}
