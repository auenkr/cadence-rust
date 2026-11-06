//! `mutable_state_builder_methods_timer.go` on [`TypedMutableState`].

use std::collections::BTreeMap;

use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::{
    CancelTimerDecisionAttributes, CancelTimerFailedEventAttributes, StartTimerDecisionAttributes,
    TimerCanceledEventAttributes, TimerFiredEventAttributes, TimerStartedEventAttributes,
};

use super::mutable_state_builder::TypedMutableState;
use super::mutable_state_methods_common::{
    internal_err, Result, ERR_MISSING_TIMER_INFO, TIMER_CANCELLATION_MSG_TIMER_ID_UNKNOWN,
    TIMER_TASK_STATUS_NONE,
};
use super::timer::TimerInfo;

impl TypedMutableState {
    /// Go `GetPendingTimerInfos`.
    pub fn get_pending_timer_infos(&self) -> &BTreeMap<String, TimerInfo> {
        &self.pending_timer_info_ids
    }

    /// Go `GetUserTimerInfo`.
    pub fn get_user_timer_info(&self, timer_id: &str) -> Option<&TimerInfo> {
        self.pending_timer_info_ids.get(timer_id)
    }

    /// Go `AddTimerStartedEvent`.
    pub fn add_timer_started_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        request: &StartTimerDecisionAttributes,
    ) -> Result<(HistoryEvent, TimerInfo)> {
        let op = "add-timer-started-event";
        self.check_mutability()?;
        let timer_id = request.timer_id.clone();
        if self.get_user_timer_info(&timer_id).is_some() {
            return Err(self.create_caller_error(op));
        }
        let event = self.add_history_event(timestamp_unix_nanos, EventType::TimerStarted, |e| {
            e.timer_started_event_attributes = Some(TimerStartedEventAttributes {
                timer_id: request.timer_id.clone(),
                start_to_fire_timeout_seconds: request.start_to_fire_timeout_seconds,
                decision_task_completed_event_id: decision_completed_event_id,
            });
        })?;
        let ti = self.replicate_timer_started_event(&event)?;
        Ok((event, ti))
    }

    /// Go `ReplicateTimerStartedEvent`.
    pub fn replicate_timer_started_event(&mut self, event: &HistoryEvent) -> Result<TimerInfo> {
        let Some(attributes) = event.timer_started_event_attributes.as_ref() else {
            return internal_err("TimerStarted event is missing attributes");
        };
        let start_to_fire_timeout = attributes.start_to_fire_timeout_seconds.unwrap_or_default();
        let fire_timeout_nanos = start_to_fire_timeout.saturating_mul(1_000_000_000);
        // Go: time.Unix(0, event.GetTimestamp()).Add(fireTimeout) — the event time, not now.
        let expiry = event.timestamp.unwrap_or_default().saturating_add(fire_timeout_nanos);
        let ti = TimerInfo {
            version: event.version,
            timer_id: attributes.timer_id.clone(),
            expiry_time_unix_nanos: expiry,
            started_id: event.event_id,
            task_status: TIMER_TASK_STATUS_NONE,
        };
        self.update_user_timer_unchecked(ti.clone());
        Ok(ti)
    }

    /// Go `AddTimerFiredEvent`.
    pub fn add_timer_fired_event(&mut self, timestamp_unix_nanos: i64, timer_id: &str) -> Result<HistoryEvent> {
        let op = "add-timer-fired-event";
        self.check_mutability()?;
        let Some(timer_info) = self.get_user_timer_info(timer_id).cloned() else {
            return Err(self.create_internal_server_error(op));
        };
        let event = self.add_history_event(timestamp_unix_nanos, EventType::TimerFired, |e| {
            e.timer_fired_event_attributes = Some(TimerFiredEventAttributes {
                timer_id: timer_id.to_string(),
                started_event_id: timer_info.started_id,
            });
        })?;
        self.replicate_timer_fired_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateTimerFiredEvent`.
    pub fn replicate_timer_fired_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(attributes) = event.timer_fired_event_attributes.as_ref() else {
            return internal_err("TimerFired event is missing attributes");
        };
        let timer_id = attributes.timer_id.clone();
        self.delete_user_timer(&timer_id)
    }

    /// Go `AddTimerCanceledEvent`.
    pub fn add_timer_canceled_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        attributes: &CancelTimerDecisionAttributes,
        identity: &str,
    ) -> Result<HistoryEvent> {
        let op = "add-timer-canceled-event";
        self.check_mutability()?;
        let timer_id = attributes.timer_id.clone();
        let existing = self.get_user_timer_info(&timer_id).cloned();
        let timer_started_id = match &existing {
            None => {
                // If the timer is not running, check whether it has fired in the
                // mutable state (buffered events and history builder) and clear it.
                let Some(fired) = self.check_and_clear_timer_fired_event_in_state(&timer_id) else {
                    return Err(self.create_caller_error(op));
                };
                fired
                    .timer_fired_event_attributes
                    .as_ref()
                    .map(|a| a.started_event_id)
                    .unwrap_or_default()
            }
            Some(ti) => ti.started_id,
        };
        let event = self.add_history_event(timestamp_unix_nanos, EventType::TimerCanceled, |e| {
            e.timer_canceled_event_attributes = Some(TimerCanceledEventAttributes {
                timer_id: timer_id.clone(),
                started_event_id: timer_started_id,
                decision_task_completed_event_id: decision_completed_event_id,
                identity: identity.to_string(),
            });
        })?;
        if existing.is_some() {
            self.replicate_timer_canceled_event(&event)?;
        }
        Ok(event)
    }

    /// Go `ReplicateTimerCanceledEvent`.
    pub fn replicate_timer_canceled_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(attributes) = event.timer_canceled_event_attributes.as_ref() else {
            return internal_err("TimerCanceled event is missing attributes");
        };
        let timer_id = attributes.timer_id.clone();
        self.delete_user_timer(&timer_id)
    }

    /// Go `AddCancelTimerFailedEvent`.
    pub fn add_cancel_timer_failed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        attributes: &CancelTimerDecisionAttributes,
        identity: &str,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        // No operation: we could not cancel it, probably TIMER_ID_UNKNOWN.
        self.add_history_event(timestamp_unix_nanos, EventType::CancelTimerFailed, |e| {
            e.cancel_timer_failed_event_attributes = Some(CancelTimerFailedEventAttributes {
                timer_id: attributes.timer_id.clone(),
                cause: TIMER_CANCELLATION_MSG_TIMER_ID_UNKNOWN.to_string(),
                decision_task_completed_event_id: decision_completed_event_id,
                identity: identity.to_string(),
            });
        })
    }

    /// Go `UpdateUserTimer` (checked: the timer must already be pending).
    pub fn update_user_timer_checked(&mut self, ti: TimerInfo) -> Result<()> {
        let Some(timer_id) = self.pending_timer_event_id_to_id.get(&ti.started_id).cloned() else {
            return internal_err(ERR_MISSING_TIMER_INFO);
        };
        if !self.pending_timer_info_ids.contains_key(&timer_id) {
            return internal_err(ERR_MISSING_TIMER_INFO);
        }
        self.pending_timer_info_ids.insert(ti.timer_id.clone(), ti.clone());
        self.update_timer_infos.insert(ti.timer_id.clone(), ti);
        Ok(())
    }

    /// Insert into pending + update maps and the event-id index (Go
    /// `ReplicateTimerStartedEvent` map writes).
    fn update_user_timer_unchecked(&mut self, ti: TimerInfo) {
        self.update_user_timer(ti);
    }
}
