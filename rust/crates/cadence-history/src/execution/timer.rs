//! User timer transitions from `mutable_state_builder_methods_timer.go`.

use cadence_api::{service_types::HistoryEvent, tasks::{CancelTimerFailedEventAttributes, TimerCanceledEventAttributes, TimerFiredEventAttributes, TimerStartedEventAttributes}, types::EventType};
use serde::{Deserialize, Serialize};

use crate::mutable_state::{MutableStateBuilder, MutableStateError, Record};

/// Persistence representation of Go's `persistence.TimerInfo` fields used by
/// timer processing. `expiry_time_unix_nanos` is based on the event timestamp,
/// so replay produces the same deadline on every replica.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerInfo {
    pub version: i64,
    pub timer_id: String,
    pub expiry_time_unix_nanos: i64,
    pub started_id: i64,
    pub task_status: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TimerError {
    #[error("timer {0:?} is already pending")]
    AlreadyStarted(String),
    #[error("timer {0:?} is not pending")]
    NotFound(String),
    #[error(transparent)]
    State(#[from] MutableStateError),
    #[error("invalid timer event: {0}")]
    InvalidEvent(&'static str),
}

impl MutableStateBuilder {
    /// Read a pending user timer, matching Go's `GetUserTimerInfo` lookup.
    pub fn get_user_timer_info(&self, timer_id: &str) -> Option<TimerInfo> {
        self.pending_timer_infos.get(timer_id).and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Create and apply a local TimerStarted event. Duplicate timer IDs are
    /// rejected before allocating an event ID.
    pub fn add_timer_started_event(
        &mut self,
        decision_completed_event_id: i64,
        timer_id: impl Into<String>,
        timeout_seconds: i64,
        timestamp_unix_nanos: i64,
        version: i64,
    ) -> Result<(HistoryEvent, TimerInfo), TimerError> {
        let timer_id = timer_id.into();
        if self.get_user_timer_info(&timer_id).is_some() {
            return Err(TimerError::AlreadyStarted(timer_id));
        }
        let event = self.add_event(HistoryEvent {
            event_type: Some(EventType::TimerStarted),
            timestamp: Some(timestamp_unix_nanos),
            version,
            timer_started_event_attributes: Some(TimerStartedEventAttributes {
                timer_id,
                start_to_fire_timeout_seconds: Some(timeout_seconds),
                decision_task_completed_event_id: decision_completed_event_id,
            }),
            ..Default::default()
        });
        let info = self.replicate_timer_started_event(&event)?;
        Ok((event, info))
    }

    /// Apply a TimerStarted event from local history or replication.
    pub fn replicate_timer_started_event(&mut self, event: &HistoryEvent) -> Result<TimerInfo, TimerError> {
        let attrs = event.timer_started_event_attributes.as_ref().ok_or(TimerError::InvalidEvent("missing TimerStarted attributes"))?;
        if attrs.timer_id.is_empty() {
            return Err(TimerError::InvalidEvent("empty timer ID"));
        }
        let timeout = attrs.start_to_fire_timeout_seconds.unwrap_or_default();
        let info = TimerInfo {
            version: event.version,
            timer_id: attrs.timer_id.clone(),
            expiry_time_unix_nanos: event.timestamp.unwrap_or_default().saturating_add(timeout.saturating_mul(1_000_000_000)),
            started_id: event.event_id,
            task_status: 0,
        };
        let value = serde_json::to_value(&info).expect("TimerInfo is serializable");
        self.upsert_timer(info.timer_id.clone(), value);
        Ok(info)
    }

    /// Build and apply TimerFired, then remove the timer from pending state.
    pub fn add_timer_fired_event(&mut self, timer_id: &str, timestamp_unix_nanos: i64, version: i64) -> Result<HistoryEvent, TimerError> {
        let info = self.get_user_timer_info(timer_id).ok_or_else(|| TimerError::NotFound(timer_id.to_owned()))?;
        let event = self.add_event(HistoryEvent {
            event_type: Some(EventType::TimerFired),
            timestamp: Some(timestamp_unix_nanos),
            version,
            timer_fired_event_attributes: Some(TimerFiredEventAttributes { timer_id: timer_id.to_owned(), started_event_id: info.started_id }),
            ..Default::default()
        });
        self.replicate_timer_fired_event(&event)?;
        Ok(event)
    }

    pub fn replicate_timer_fired_event(&mut self, event: &HistoryEvent) -> Result<(), TimerError> {
        let attrs = event.timer_fired_event_attributes.as_ref().ok_or(TimerError::InvalidEvent("missing TimerFired attributes"))?;
        self.delete_user_timer(&attrs.timer_id);
        Ok(())
    }

    /// Build and apply TimerCanceled. If `timer_id` is absent, return the same
    /// not-found result used by the timer decision handler's failure path.
    pub fn add_timer_canceled_event(&mut self, decision_completed_event_id: i64, timer_id: &str, identity: &str, timestamp_unix_nanos: i64, version: i64) -> Result<HistoryEvent, TimerError> {
        let info = self.get_user_timer_info(timer_id).ok_or_else(|| TimerError::NotFound(timer_id.to_owned()))?;
        let event = self.add_event(HistoryEvent {
            event_type: Some(EventType::TimerCanceled),
            timestamp: Some(timestamp_unix_nanos),
            version,
            timer_canceled_event_attributes: Some(TimerCanceledEventAttributes {
                timer_id: timer_id.to_owned(), started_event_id: info.started_id,
                decision_task_completed_event_id, identity: identity.to_owned(),
            }),
            ..Default::default()
        });
        self.replicate_timer_canceled_event(&event)?;
        Ok(event)
    }

    /// Emit the no-op failure event used when a cancel decision names an
    /// unknown timer. This event does not mutate timer state.
    pub fn add_cancel_timer_failed_event(&mut self, decision_completed_event_id: i64, timer_id: &str, cause: &str, identity: &str, timestamp_unix_nanos: i64, version: i64) -> HistoryEvent {
        self.add_event(HistoryEvent {
            event_type: Some(EventType::CancelTimerFailed), timestamp: Some(timestamp_unix_nanos), version,
            cancel_timer_failed_event_attributes: Some(CancelTimerFailedEventAttributes {
                timer_id: timer_id.to_owned(), cause: cause.to_owned(), decision_task_completed_event_id: decision_completed_event_id, identity: identity.to_owned(),
            }), ..Default::default()
        })
    }

    pub fn replicate_timer_canceled_event(&mut self, event: &HistoryEvent) -> Result<(), TimerError> {
        let attrs = event.timer_canceled_event_attributes.as_ref().ok_or(TimerError::InvalidEvent("missing TimerCanceled attributes"))?;
        self.delete_user_timer(&attrs.timer_id);
        Ok(())
    }

    /// Refresh the user timer's start time after a reset/replay operation.
    pub fn update_user_timer(&mut self, info: TimerInfo) -> Result<(), TimerError> {
        let Some(existing_id) = self.pending_timer_infos.iter().find_map(|(id, value)| {
            serde_json::from_value::<TimerInfo>(value.clone()).ok().filter(|old| old.started_id == info.started_id).map(|_| id.clone())
        }) else { return Err(TimerError::NotFound(info.timer_id)); };
        self.pending_timer_infos.remove(&existing_id);
        self.update_timer_infos.remove(&existing_id);
        let id = info.timer_id.clone();
        self.upsert_timer(id, serde_json::to_value(info).expect("TimerInfo is serializable"));
        Ok(())
    }

    pub fn delete_user_timer(&mut self, timer_id: &str) {
        self.pending_timer_infos.remove(timer_id);
        self.update_timer_infos.remove(timer_id);
        self.delete_timer_infos.insert(timer_id.to_owned());
    }
}

/// Convert a timer state to the generic persistence record shape used by the
/// mutable-state delta contract.
pub fn timer_record(info: TimerInfo) -> Record { serde_json::to_value(info).expect("TimerInfo is serializable") }

/// Remove and return the first matching TimerFired event, as Go's buffered
/// history scan does while reconciling an unknown timer cancellation.
pub fn check_and_clear_timer_fired_event(events: &mut Vec<HistoryEvent>, timer_id: &str) -> Option<HistoryEvent> {
    let index = events.iter().position(|event| event.timer_fired_event_attributes.as_ref().is_some_and(|attrs| attrs.timer_id == timer_id))?;
    Some(events.remove(index))
}

