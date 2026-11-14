//! `mutable_state_builder_methods_activity.go` on [`TypedMutableState`].
//!
//! Time values are Unix nanoseconds (0 == Go zero `time.Time`). Domain-cache
//! lookup, task generation, events cache, config limits, logging and the clock
//! are not wired in `TypedMutableState`, so the callers pass the resolved values
//! (`target_domain_id`, `now_unix_nanos`, jitter sample, ...).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cadence_api::enums::{EventType, TimeoutType};
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::{
    ActivityTaskCancelRequestedEventAttributes, ActivityTaskCanceledEventAttributes,
    ActivityTaskCompletedEventAttributes, ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes, ActivityTaskStartedEventAttributes,
    ActivityTaskTimedOutEventAttributes, RecordActivityTaskHeartbeatRequest,
    RequestCancelActivityTaskFailedEventAttributes, RespondActivityTaskCompletedRequest,
    RespondActivityTaskFailedRequest, ScheduleActivityTaskDecisionAttributes,
};
use cadence_api::types::FailureOptions;

use super::mutable_state_builder::{TypedMutableState, EMPTY_EVENT_ID};
use super::mutable_state_methods_common::{internal_err, Result, TIMER_TASK_STATUS_NONE};
use super::retry::{backoff_interval, should_retry};
use crate::mutable_state_activity::ActivityInfo;

/// Go `common.TransientEventID`.
pub const TRANSIENT_EVENT_ID: i64 = -124;
/// Go `ErrMissingActivityInfo` message.
pub const ERR_MISSING_ACTIVITY_INFO: &str = "unable to get activity info";

fn to_system_time(nanos: i64) -> SystemTime {
    if nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(nanos as u64)
    } else {
        UNIX_EPOCH - Duration::from_nanos(nanos.unsigned_abs())
    }
}

impl TypedMutableState {
    /// Go `GetActivityInfo`.
    pub fn get_activity_info(&self, schedule_event_id: i64) -> Option<&ActivityInfo> {
        self.pending_activity_info_ids.get(&schedule_event_id)
    }

    /// Go `GetPendingActivityInfos`.
    pub fn get_pending_activity_infos(&self) -> &std::collections::BTreeMap<i64, ActivityInfo> {
        &self.pending_activity_info_ids
    }

    /// Go `GetActivityByActivityID`.
    pub fn get_activity_by_activity_id(&self, activity_id: &str) -> Option<&ActivityInfo> {
        let id = self.pending_activity_id_to_event_id.get(activity_id)?;
        self.get_activity_info(*id)
    }

    /// Go `UpdateActivityProgress`.
    pub fn update_activity_progress(
        &mut self,
        schedule_id: i64,
        request: &RecordActivityTaskHeartbeatRequest,
        now_unix_nanos: i64,
    ) {
        let version = self.get_current_version();
        let Some(ai) = self.pending_activity_info_ids.get_mut(&schedule_id) else {
            return;
        };
        ai.version = version;
        ai.details = request.details.clone();
        ai.last_heartbeat_updated_time = now_unix_nanos;
        let ai = ai.clone();
        self.update_activity_infos.insert(schedule_id, ai);
        self.sync_activity_tasks.insert(schedule_id);
    }

    /// Go `ReplicateActivityInfo`. `request` is the sync-activity request.
    pub fn replicate_activity_info(
        &mut self,
        request: &crate::engine::types::SyncActivityRequest,
        reset_activity_timer_task_status: bool,
    ) -> Result<()> {
        let Some(ai) = self.pending_activity_info_ids.get_mut(&request.scheduled_id) else {
            return internal_err(ERR_MISSING_ACTIVITY_INFO);
        };
        ai.version = request.version;
        ai.scheduled_time = request.scheduled_time.unwrap_or_default();
        ai.started_id = request.started_id;
        ai.last_heartbeat_updated_time = request.last_heartbeat_time.unwrap_or_default();
        ai.started_time = if ai.started_id == EMPTY_EVENT_ID {
            0
        } else {
            request.started_time.unwrap_or_default()
        };
        ai.details = request.details.clone();
        ai.attempt = request.attempt;
        ai.last_failure_reason = request.last_failure_reason.clone().unwrap_or_default();
        ai.last_worker_identity = request.last_worker_identity.clone();
        ai.last_failure_details = request.last_failure_details.clone();
        ai.last_retry_interval_seconds = request
            .last_failure_options
            .as_ref()
            .and_then(|o| o.next_retry_interval_seconds)
            .unwrap_or_default();
        if reset_activity_timer_task_status {
            ai.timer_task_status = TIMER_TASK_STATUS_NONE;
        }
        let ai = ai.clone();
        self.update_activity_infos.insert(ai.schedule_id, ai);
        Ok(())
    }

    /// Go `UpdateActivity` (checked).
    pub fn update_activity_checked(&mut self, ai: ActivityInfo) -> Result<()> {
        if !self.pending_activity_info_ids.contains_key(&ai.schedule_id) {
            return internal_err(ERR_MISSING_ACTIVITY_INFO);
        }
        self.pending_activity_info_ids.insert(ai.schedule_id, ai.clone());
        self.update_activity_infos.insert(ai.schedule_id, ai);
        Ok(())
    }

    /// Go `DeleteActivity`: missing entries only log a data inconsistency.
    pub fn delete_activity_tolerant(&mut self, schedule_event_id: i64) -> Result<()> {
        if let Some(info) = self.pending_activity_info_ids.remove(&schedule_event_id) {
            // Go logs a data inconsistency when the id index is missing.
            self.pending_activity_id_to_event_id.remove(&info.activity_id);
        }
        self.update_activity_infos.remove(&schedule_event_id);
        self.delete_activity_infos.insert(schedule_event_id);
        Ok(())
    }

    /// Go `AddActivityTaskScheduledEvent` (pending-count limits and events cache
    /// are not wired here; production local dispatch lives in `engine_decision_ops`).
    pub fn add_activity_task_scheduled_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        attributes: &ScheduleActivityTaskDecisionAttributes,
        target_domain_id: &str,
    ) -> Result<(HistoryEvent, ActivityInfo)> {
        let op = "add-activitytask-scheduled-event";
        self.check_mutability()?;
        if self.get_activity_by_activity_id(&attributes.activity_id).is_some() {
            return Err(self.create_caller_error(op));
        }
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ActivityTaskScheduled, |e| {
            e.activity_task_scheduled_event_attributes = Some(ActivityTaskScheduledEventAttributes {
                activity_id: attributes.activity_id.clone(),
                activity_type: attributes.activity_type.clone(),
                domain: if attributes.domain.is_empty() { None } else { Some(attributes.domain.clone()) },
                task_list: attributes.task_list.clone(),
                input: attributes.input.clone(),
                schedule_to_close_timeout_seconds: attributes.schedule_to_close_timeout_seconds,
                schedule_to_start_timeout_seconds: attributes.schedule_to_start_timeout_seconds,
                start_to_close_timeout_seconds: attributes.start_to_close_timeout_seconds,
                heartbeat_timeout_seconds: attributes.heartbeat_timeout_seconds,
                decision_task_completed_event_id: decision_completed_event_id,
                retry_policy: attributes.retry_policy.clone(),
                header: attributes.header.clone(),
            });
        })?;
        // Go: ReplicateActivityTaskScheduledEvent(.., true) skips task generation, then
        // GenerateActivityTransferTasks (the production decision path skips it for local dispatch).
        let ai = self.replicate_activity_task_scheduled_event(
            decision_completed_event_id,
            &event,
            target_domain_id,
        )?;
        self.run_task_generator(0, |g| g.generate_activity_transfer_tasks(&event))?;
        Ok((event, ai))
    }

    /// Go `ReplicateActivityTaskScheduledEvent` (task generation is the caller's job).
    pub fn replicate_activity_task_scheduled_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
        target_domain_id: &str,
    ) -> Result<ActivityInfo> {
        let Some(attributes) = event.activity_task_scheduled_event_attributes.as_ref() else {
            return internal_err("ActivityTaskScheduled event is missing attributes");
        };
        let schedule_event_id = event.event_id;
        let scheduled_time = event.timestamp.unwrap_or_default();
        let mut ai = ActivityInfo {
            version: event.version,
            schedule_id: schedule_event_id,
            scheduled_event_batch_id: first_event_id,
            scheduled_time,
            started_id: EMPTY_EVENT_ID,
            started_time: 0,
            activity_id: attributes.activity_id.clone(),
            domain_id: target_domain_id.to_string(),
            schedule_to_start_timeout: attributes.schedule_to_start_timeout_seconds.unwrap_or_default(),
            schedule_to_close_timeout: attributes.schedule_to_close_timeout_seconds.unwrap_or_default(),
            start_to_close_timeout: attributes.start_to_close_timeout_seconds.unwrap_or_default(),
            heartbeat_timeout: attributes.heartbeat_timeout_seconds.unwrap_or_default(),
            cancel_requested: false,
            cancel_request_id: EMPTY_EVENT_ID,
            last_heartbeat_updated_time: 0,
            task_list: attributes.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
            has_retry_policy: attributes.retry_policy.is_some(),
            ..ActivityInfo::default()
        };
        if let Some(rp) = attributes.retry_policy.as_ref() {
            ai.initial_interval = rp.initial_interval_in_seconds;
            ai.backoff_coefficient = rp.backoff_coefficient;
            ai.jitter_coefficient = rp.jitter_coefficient;
            ai.maximum_interval = rp.maximum_interval_in_seconds;
            ai.maximum_attempts = rp.maximum_attempts;
            ai.non_retriable_errors = rp.non_retriable_error_reasons.clone();
            if rp.expiration_interval_in_seconds != 0 {
                ai.expiration_time = scheduled_time
                    .saturating_add((rp.expiration_interval_in_seconds as i64).saturating_mul(1_000_000_000));
            }
        }
        self.pending_activity_info_ids.insert(schedule_event_id, ai.clone());
        self.pending_activity_id_to_event_id.insert(ai.activity_id.clone(), schedule_event_id);
        self.update_activity_infos.insert(ai.schedule_id, ai.clone());
        Ok(ai)
    }

    /// Go `addTransientActivityStartedEvent`.
    fn add_transient_activity_started_event(
        &mut self,
        timestamp_unix_nanos: i64,
        schedule_event_id: i64,
    ) -> Result<()> {
        let Some(ai) = self.get_activity_info(schedule_event_id).cloned() else {
            return Ok(());
        };
        if ai.started_id != TRANSIENT_EVENT_ID {
            return Ok(());
        }
        let ts = if ai.started_time != 0 { ai.started_time } else { timestamp_unix_nanos };
        let event = self.add_history_event(ts, EventType::ActivityTaskStarted, |e| {
            e.activity_task_started_event_attributes = Some(ActivityTaskStartedEventAttributes {
                scheduled_event_id: schedule_event_id,
                identity: ai.started_identity.clone(),
                request_id: ai.request_id.clone(),
                attempt: ai.attempt,
                last_failure_reason: Some(ai.last_failure_reason.clone()),
                last_failure_details: ai.last_failure_details.clone(),
                last_failure_options: Some(FailureOptions {
                    failure_category: None,
                    next_retry_interval_seconds: Some(ai.last_retry_interval_seconds),
                }),
            });
        })?;
        self.replicate_activity_task_started_event(&event)
    }

    /// Go `AddActivityTaskStartedEvent`.
    pub fn add_activity_task_started_event(
        &mut self,
        timestamp_unix_nanos: i64,
        schedule_event_id: i64,
        request_id: &str,
        identity: &str,
    ) -> Result<Option<HistoryEvent>> {
        self.check_mutability()?;
        let Some(ai) = self.get_activity_info(schedule_event_id).cloned() else {
            return internal_err(ERR_MISSING_ACTIVITY_INFO);
        };
        if !ai.has_retry_policy {
            let event = self.add_history_event(timestamp_unix_nanos, EventType::ActivityTaskStarted, |e| {
                e.activity_task_started_event_attributes = Some(ActivityTaskStartedEventAttributes {
                    scheduled_event_id: schedule_event_id,
                    identity: identity.to_string(),
                    request_id: request_id.to_string(),
                    attempt: ai.attempt,
                    last_failure_reason: Some(ai.last_failure_reason.clone()),
                    last_failure_details: ai.last_failure_details.clone(),
                    last_failure_options: Some(FailureOptions {
                        failure_category: None,
                        next_retry_interval_seconds: Some(ai.last_retry_interval_seconds),
                    }),
                });
            })?;
            self.replicate_activity_task_started_event(&event)?;
            return Ok(Some(event));
        }
        // May retry: do not append the started event yet (transient).
        let mut ai = ai;
        ai.version = self.get_current_version();
        ai.started_id = TRANSIENT_EVENT_ID;
        ai.request_id = request_id.to_string();
        ai.started_time = timestamp_unix_nanos;
        ai.last_heartbeat_updated_time = ai.started_time;
        ai.started_identity = identity.to_string();
        let sid = ai.schedule_id;
        self.update_activity_checked(ai)?;
        self.sync_activity_tasks.insert(sid);
        Ok(None)
    }

    /// Go `ReplicateActivityTaskStartedEvent`.
    pub fn replicate_activity_task_started_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(attributes) = event.activity_task_started_event_attributes.as_ref() else {
            return internal_err("ActivityTaskStarted event is missing attributes");
        };
        let Some(ai) = self.pending_activity_info_ids.get_mut(&attributes.scheduled_event_id) else {
            return internal_err(ERR_MISSING_ACTIVITY_INFO);
        };
        ai.version = event.version;
        ai.started_id = event.event_id;
        ai.request_id = attributes.request_id.clone();
        ai.started_time = event.timestamp.unwrap_or_default();
        ai.last_heartbeat_updated_time = ai.started_time;
        let ai = ai.clone();
        self.update_activity_infos.insert(ai.schedule_id, ai);
        Ok(())
    }

    /// Go `AddActivityTaskCompletedEvent`.
    pub fn add_activity_task_completed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        schedule_event_id: i64,
        started_event_id: i64,
        request: &RespondActivityTaskCompletedRequest,
    ) -> Result<HistoryEvent> {
        let op = "add-activitytask-completed-event";
        self.check_mutability()?;
        match self.get_activity_info(schedule_event_id) {
            Some(ai) if ai.started_id == started_event_id => {}
            _ => return Err(self.create_internal_server_error(op)),
        }
        self.add_transient_activity_started_event(timestamp_unix_nanos, schedule_event_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ActivityTaskCompleted, |e| {
            e.activity_task_completed_event_attributes = Some(ActivityTaskCompletedEventAttributes {
                result: request.result.clone(),
                scheduled_event_id: schedule_event_id,
                started_event_id,
                identity: request.identity.clone(),
            });
        })?;
        self.replicate_activity_task_completed_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateActivityTaskCompletedEvent`.
    pub fn replicate_activity_task_completed_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.activity_task_completed_event_attributes.as_ref() else {
            return internal_err("ActivityTaskCompleted event is missing attributes");
        };
        self.delete_activity_tolerant(a.scheduled_event_id)
    }

    /// Go `AddActivityTaskFailedEvent`.
    pub fn add_activity_task_failed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        schedule_event_id: i64,
        started_event_id: i64,
        request: &RespondActivityTaskFailedRequest,
    ) -> Result<HistoryEvent> {
        let op = "add-activitytask-failed-event";
        self.check_mutability()?;
        match self.get_activity_info(schedule_event_id) {
            Some(ai) if ai.started_id == started_event_id => {}
            _ => return Err(self.create_internal_server_error(op)),
        }
        self.add_transient_activity_started_event(timestamp_unix_nanos, schedule_event_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ActivityTaskFailed, |e| {
            e.activity_task_failed_event_attributes = Some(ActivityTaskFailedEventAttributes {
                reason: request.reason.clone(),
                details: request.details.clone(),
                failure_options: None,
                scheduled_event_id: schedule_event_id,
                started_event_id,
                identity: request.identity.clone(),
            });
        })?;
        self.replicate_activity_task_failed_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateActivityTaskFailedEvent`.
    pub fn replicate_activity_task_failed_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.activity_task_failed_event_attributes.as_ref() else {
            return internal_err("ActivityTaskFailed event is missing attributes");
        };
        self.delete_activity_tolerant(a.scheduled_event_id)
    }

    /// Go `AddActivityTaskTimedOutEvent`.
    pub fn add_activity_task_timed_out_event(
        &mut self,
        timestamp_unix_nanos: i64,
        schedule_event_id: i64,
        started_event_id: i64,
        timeout_type: TimeoutType,
        last_heartbeat_details: Vec<u8>,
    ) -> Result<HistoryEvent> {
        let op = "add-activitytask-timed-event";
        self.check_mutability()?;
        let ai = match self.get_activity_info(schedule_event_id) {
            Some(ai)
                if ai.started_id == started_event_id
                    && !((timeout_type == TimeoutType::StartToClose || timeout_type == TimeoutType::Heartbeat)
                        && ai.started_id == EMPTY_EVENT_ID) =>
            {
                ai.clone()
            }
            _ => return Err(self.create_internal_server_error(op)),
        };
        self.add_transient_activity_started_event(timestamp_unix_nanos, schedule_event_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ActivityTaskTimedOut, |e| {
            e.activity_task_timed_out_event_attributes = Some(ActivityTaskTimedOutEventAttributes {
                details: last_heartbeat_details,
                scheduled_event_id: schedule_event_id,
                started_event_id,
                timeout_type: Some(timeout_type),
                last_failure_reason: Some(ai.last_failure_reason.clone()),
                last_failure_details: ai.last_failure_details.clone(),
                last_failure_options: Some(FailureOptions {
                    failure_category: None,
                    next_retry_interval_seconds: Some(ai.last_retry_interval_seconds),
                }),
            });
        })?;
        self.replicate_activity_task_timed_out_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateActivityTaskTimedOutEvent`.
    pub fn replicate_activity_task_timed_out_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.activity_task_timed_out_event_attributes.as_ref() else {
            return internal_err("ActivityTaskTimedOut event is missing attributes");
        };
        self.delete_activity_tolerant(a.scheduled_event_id)
    }

    /// Go `AddActivityTaskCancelRequestedEvent`.
    pub fn add_activity_task_cancel_requested_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        activity_id: &str,
    ) -> Result<(HistoryEvent, ActivityInfo)> {
        let op = "add-activitytask-cancel-requested-event";
        self.check_mutability()?;
        // The event is added even when the activity is not pending (Go).
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ActivityTaskCancelRequested, |e| {
            e.activity_task_cancel_requested_event_attributes = Some(ActivityTaskCancelRequestedEventAttributes {
                activity_id: activity_id.to_string(),
                decision_task_completed_event_id: decision_completed_event_id,
            });
        })?;
        match self.get_activity_by_activity_id(activity_id) {
            Some(ai) if !ai.cancel_requested => {}
            _ => return Err(self.create_caller_error(op)),
        }
        self.replicate_activity_task_cancel_requested_event(&event)?;
        let ai = self.get_activity_by_activity_id(activity_id).cloned().unwrap_or_default();
        Ok((event, ai))
    }

    /// Go `ReplicateActivityTaskCancelRequestedEvent`.
    pub fn replicate_activity_task_cancel_requested_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.activity_task_cancel_requested_event_attributes.as_ref() else {
            return internal_err("ActivityTaskCancelRequested event is missing attributes");
        };
        let Some(id) = self.pending_activity_id_to_event_id.get(&a.activity_id).copied() else {
            return Ok(());
        };
        let Some(ai) = self.pending_activity_info_ids.get_mut(&id) else {
            return Ok(());
        };
        ai.version = event.version;
        ai.cancel_requested = true;
        ai.cancel_request_id = event.event_id;
        let ai = ai.clone();
        self.update_activity_infos.insert(ai.schedule_id, ai);
        Ok(())
    }

    /// Go `AddRequestCancelActivityTaskFailedEvent`.
    pub fn add_request_cancel_activity_task_failed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        activity_id: &str,
        cause: &str,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        self.add_history_event(timestamp_unix_nanos, EventType::RequestCancelActivityTaskFailed, |e| {
            e.request_cancel_activity_task_failed_event_attributes =
                Some(RequestCancelActivityTaskFailedEventAttributes {
                    activity_id: activity_id.to_string(),
                    cause: cause.to_string(),
                    decision_task_completed_event_id: decision_completed_event_id,
                });
        })
    }

    /// Go `AddActivityTaskCanceledEvent`.
    pub fn add_activity_task_canceled_event(
        &mut self,
        timestamp_unix_nanos: i64,
        schedule_event_id: i64,
        started_event_id: i64,
        latest_cancel_requested_event_id: i64,
        details: Vec<u8>,
        identity: &str,
    ) -> Result<HistoryEvent> {
        let op = "add-activitytask-canceled-event";
        self.check_mutability()?;
        match self.get_activity_info(schedule_event_id) {
            Some(ai) if ai.started_id == started_event_id && ai.cancel_requested => {}
            _ => return Err(self.create_internal_server_error(op)),
        }
        self.add_transient_activity_started_event(timestamp_unix_nanos, schedule_event_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ActivityTaskCanceled, |e| {
            e.activity_task_canceled_event_attributes = Some(ActivityTaskCanceledEventAttributes {
                details,
                latest_cancel_requested_event_id,
                scheduled_event_id: schedule_event_id,
                started_event_id,
                identity: identity.to_string(),
            });
        })?;
        self.replicate_activity_task_canceled_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateActivityTaskCanceledEvent`.
    pub fn replicate_activity_task_canceled_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.activity_task_canceled_event_attributes.as_ref() else {
            return internal_err("ActivityTaskCanceled event is missing attributes");
        };
        self.delete_activity_tolerant(a.scheduled_event_id)
    }

    /// Go `RetryActivity`. `fatal` is `failureOptions.FailureCategory == Fatal`;
    /// `jitter_sample` is the `rand.Float64()` value; `GenerateActivityRetryTasks`
    /// is wired in.
    #[allow(clippy::too_many_arguments)]
    pub fn retry_activity(
        &mut self,
        schedule_id: i64,
        now_unix_nanos: i64,
        failure_reason: &str,
        failure_details: Vec<u8>,
        failure_options: Option<&FailureOptions>,
        fatal: bool,
        jitter_sample: f64,
    ) -> Result<bool> {
        self.check_mutability()?;
        let Some(ai) = self.get_activity_info(schedule_id).cloned() else {
            return internal_err(ERR_MISSING_ACTIVITY_INFO);
        };
        if !ai.has_retry_policy || ai.cancel_requested {
            return Ok(false);
        }
        let next_interval_seconds = failure_options
            .and_then(|o| o.next_retry_interval_seconds)
            .unwrap_or_default();
        let interval = if next_interval_seconds > 0 {
            Duration::from_secs(next_interval_seconds as u64)
        } else {
            match backoff_interval(
                ai.attempt,
                ai.initial_interval,
                ai.maximum_interval,
                ai.backoff_coefficient,
                ai.jitter_coefficient,
                jitter_sample,
            ) {
                Some(d) => d,
                None => return Ok(false), // backoff.NoBackoff
            }
        };
        let next_scheduled = now_unix_nanos.saturating_add(interval.as_nanos().min(i64::MAX as u128) as i64);
        let expiration = if ai.expiration_time == 0 { None } else { Some(to_system_time(ai.expiration_time)) };
        if !should_retry(
            to_system_time(next_scheduled),
            ai.attempt,
            ai.maximum_attempts,
            expiration,
            failure_reason,
            &ai.non_retriable_errors,
            fatal,
        ) {
            return Ok(false);
        }
        let version = self.get_current_version();
        let mut ai = ai;
        ai.version = version;
        ai.attempt += 1;
        ai.scheduled_time = next_scheduled;
        ai.started_id = EMPTY_EVENT_ID;
        ai.request_id = String::new();
        ai.started_time = 0;
        ai.timer_task_status = TIMER_TASK_STATUS_NONE;
        ai.last_heartbeat_timeout_visibility_in_seconds = 0;
        ai.last_failure_reason = failure_reason.to_string();
        ai.last_worker_identity = ai.started_identity.clone();
        ai.last_failure_details = failure_details;
        ai.last_retry_interval_seconds = next_interval_seconds;
        self.pending_activity_info_ids.insert(schedule_id, ai.clone());
        self.run_task_generator(0, |g| g.generate_activity_retry_tasks(schedule_id))?;
        self.update_activity_infos.insert(schedule_id, ai);
        self.sync_activity_tasks.insert(schedule_id);
        Ok(true)
    }
}

#[cfg(test)]
mod activity_timer_retry_tests {
    use super::*;

    #[test]
    fn retry_clears_activity_timer_status_and_heartbeat_visibility() {
        let mut state = TypedMutableState::new();
        state.current_version = 1;
        let activity = ActivityInfo {
            schedule_id: 7,
            activity_id: "a".into(),
            has_retry_policy: true,
            initial_interval: 1,
            backoff_coefficient: 2.0,
            maximum_interval: 10,
            maximum_attempts: 5,
            attempt: 1,
            timer_task_status: 15,
            last_heartbeat_timeout_visibility_in_seconds: 123,
            ..Default::default()
        };
        state.update_activity(activity);

        assert!(state.retry_activity(7, 10_000_000_000, "failure", vec![], None, false, 0.0).unwrap());
        let retried = state.get_activity_info(7).unwrap();
        assert_eq!(retried.timer_task_status, TIMER_TASK_STATUS_NONE);
        assert_eq!(retried.last_heartbeat_timeout_visibility_in_seconds, 0);
    }
}
