//! Decision lifecycle policy from Go `mutable_state_decision_task_manager.go`.
//! Event attribute construction is provided by the execution history builder.
use cadence_api::{enums::{DecisionTaskFailedCause, DecisionTaskTimedOutCause, TimeoutType}, service_types::HistoryEvent, tasks::DecisionTaskFailedEventAttributes};
use crate::mutable_state::DecisionInfo;

pub trait DecisionManagerState {
    type Error;
    fn decision(&self) -> DecisionInfo;
    fn update_decision(&mut self, decision: DecisionInfo);
    fn now_nanos(&self) -> i64;
    fn next_event_id(&self) -> i64;
    fn current_version(&self) -> i64;
    fn last_write_version(&self) -> Result<i64, Self::Error>;
    fn zombie(&self) -> bool;
    fn set_running(&mut self) -> Result<(), Self::Error>;
    fn task_list(&self) -> String;
    fn sticky_task_list(&self) -> String;
    fn sticky_enabled(&self) -> bool;
    fn clear_stickiness(&mut self);
    fn decision_timeout_seconds(&self) -> i32;
    fn sticky_schedule_to_start_timeout_nanos(&self) -> i64;
    fn normal_schedule_to_start_timeout_nanos(&self) -> i64;
    fn normal_schedule_to_start_max_attempts(&self) -> i64;
    fn previous_started_event_id(&self) -> i64;
    fn has_buffered_events(&self) -> bool;
    fn has_buffered_events_in_db(&self) -> bool;
    fn flush_buffered_events(&mut self) -> Result<(), Self::Error>;
    fn schedule_tasks(&mut self, id: i64) -> Result<(), Self::Error>;
    fn start_tasks(&mut self, id: i64) -> Result<(), Self::Error>;
    fn delayed_decision_tasks(&mut self, event: &HistoryEvent) -> Result<(), Self::Error>;
    fn add_scheduled_event(&mut self, task_list: String, timeout: i32, attempt: i64, transient_timestamp: Option<i64>) -> HistoryEvent;
    fn add_started_event(&mut self, scheduled: i64, request: String, identity: String, transient_timestamp: Option<i64>) -> HistoryEvent;
    fn add_completed_event(&mut self, scheduled: i64, started: i64, request: &cadence_api::tasks::RespondDecisionTaskCompletedRequest) -> HistoryEvent;
    fn add_failed_event(&mut self, attributes: DecisionTaskFailedEventAttributes) -> HistoryEvent;
    fn add_timeout_event(&mut self, attributes: cadence_api::tasks::DecisionTaskTimedOutEventAttributes) -> HistoryEvent;
    fn add_binary_checksum(&mut self, event: &HistoryEvent, max_reset_points: usize) -> Result<(), Self::Error>;
    fn set_last_processed_event(&mut self, id: i64);
    fn insert_reset_request(&mut self, request_id: String, version: i64);
    fn max_reset_points(&self) -> usize;
    fn invalid_action(&self, operation: &'static str) -> Self::Error;
    fn critical_retry(&mut self, attempt: i64);
}
pub struct DecisionTaskManager<'a, S> { pub state: &'a mut S }
impl<S: DecisionManagerState> DecisionTaskManager<'_, S> {
    pub fn has_pending_decision(&self) -> bool { self.state.decision().schedule_id != 0 }
    pub fn get_pending_decision(&self) -> Option<DecisionInfo> { self.has_pending_decision().then(|| self.state.decision()) }
    pub fn has_in_flight_decision(&self) -> bool { self.state.decision().started_id > 0 }
    pub fn get_in_flight_decision(&self) -> Option<DecisionInfo> { let d = self.state.decision(); (d.schedule_id != 0 && d.started_id != 0).then_some(d) }
    pub fn has_processed_or_pending_decision(&self) -> bool { self.has_pending_decision() || self.state.previous_started_event_id() != 0 }
    pub fn get_decision_info(&self, id: i64) -> Option<DecisionInfo> { let d = self.state.decision(); (d.schedule_id == id).then_some(d) }
    pub fn should_update_last_write_version(&self) -> bool { self.state.last_write_version().is_ok_and(|version| version != self.state.current_version()) }
    pub fn update_decision(&mut self, decision: DecisionInfo) { self.state.update_decision(decision); }
    pub fn delete_decision(&mut self) {
        let original_schedule_timestamp = self.state.decision().original_schedule_timestamp;
        self.update_decision(DecisionInfo { version: -24, original_schedule_timestamp, ..Default::default() });
    }
    pub fn fail_decision(&mut self, increment: bool) {
        self.state.clear_stickiness();
        let mut decision = DecisionInfo { version: -24, ..Default::default() };
        if increment { decision.attempt = self.state.decision().attempt + 1; decision.schedule_timestamp = self.state.now_nanos(); self.state.critical_retry(decision.attempt); }
        self.update_decision(decision);
    }
    pub fn get_decision_schedule_to_start_timeout(&self) -> i64 {
        if !self.state.sticky_task_list().is_empty() { return self.state.sticky_schedule_to_start_timeout_nanos(); }
        if self.state.decision().attempt < self.state.normal_schedule_to_start_max_attempts() { self.state.normal_schedule_to_start_timeout_nanos() } else { 0 }
    }
    pub fn replicate_decision_task_scheduled_event(&mut self, decision: DecisionInfo, bypass: bool) -> Result<DecisionInfo, S::Error> {
        if !self.state.zombie() { self.state.set_running()?; }
        self.update_decision(decision.clone());
        if !bypass { self.state.schedule_tasks(decision.schedule_id)?; }
        Ok(decision)
    }
    pub fn replicate_transient_decision_task_scheduled(&mut self) -> Result<(), S::Error> {
        let attempt = self.state.decision().attempt;
        if self.has_pending_decision() || attempt == 0 { return Ok(()); }
        let d = DecisionInfo { version: self.state.current_version(), schedule_id: self.state.next_event_id(), start_to_close_timeout_seconds: self.state.decision_timeout_seconds(), task_list: self.state.task_list(), attempt, schedule_timestamp: self.state.now_nanos(), ..Default::default() };
        self.update_decision(d.clone()); self.state.schedule_tasks(d.schedule_id)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn replicate_decision_task_started_event(&mut self, decision: Option<DecisionInfo>, version: i64, schedule_id: i64, started_id: i64, request_id: String, timestamp: i64) -> Result<DecisionInfo, S::Error> {
        let mut d = match decision { Some(d) => d, None => { let mut d = self.get_decision_info(schedule_id).ok_or_else(|| self.state.invalid_action("unable to find decision"))?; d.attempt = 0; d } };
        d.version = version; d.schedule_id = schedule_id; d.started_id = started_id; d.request_id = request_id; d.started_timestamp = timestamp;
        self.update_decision(d.clone()); self.state.start_tasks(schedule_id)?; Ok(d)
    }
    pub fn replicate_decision_task_completed_event(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        self.delete_decision(); self.after_completed(event, self.state.max_reset_points())
    }
    fn after_completed(&mut self, event: &HistoryEvent, max_reset_points: usize) -> Result<(), S::Error> {
        let id = event.decision_task_completed_event_attributes.as_ref().map(|a| a.started_event_id).unwrap_or_default();
        self.state.set_last_processed_event(id); self.state.add_binary_checksum(event, max_reset_points)
    }
    pub fn replicate_decision_task_failed_event(&mut self, event: Option<&HistoryEvent>) -> Result<(), S::Error> {
        if let Some(event) = event { if let Some(a) = &event.decision_task_failed_event_attributes { if a.cause == Some(DecisionTaskFailedCause::ResetWorkflow) { self.state.insert_reset_request(a.request_id.clone(), event.version); } } }
        self.fail_decision(true); Ok(())
    }
    pub fn replicate_decision_task_timed_out_event(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let a = event.decision_task_timed_out_event_attributes.as_ref().ok_or_else(|| self.state.invalid_action("DecisionTaskTimedOut"))?;
        let increment = !(a.timeout_type == Some(TimeoutType::ScheduleToStart) && !self.state.sticky_task_list().is_empty());
        if a.cause == Some(DecisionTaskTimedOutCause::Reset) { self.state.insert_reset_request(a.request_id.clone(), event.version); }
        self.fail_decision(increment); Ok(())
    }
    pub fn add_decision_task_scheduled_event_as_heartbeat(&mut self, bypass: bool, original: i64) -> Result<DecisionInfo, S::Error> {
        if self.has_pending_decision() { return Err(self.state.invalid_action("DecisionTaskScheduled")); }
        let task_list = if self.state.sticky_enabled() { self.state.sticky_task_list() } else { self.state.clear_stickiness(); self.state.task_list() };
        if self.state.has_buffered_events() || self.state.has_buffered_events_in_db() {
            let mut d = self.state.decision(); d.attempt = 0; self.update_decision(d); self.state.flush_buffered_events()?;
        }
        let mut schedule_id = self.state.next_event_id(); let mut schedule_timestamp = self.state.now_nanos();
        let mut attempt = self.state.decision().attempt;
        let timeout = self.state.decision_timeout_seconds();
        if attempt == 0 || self.should_update_last_write_version() {
            let event = self.state.add_scheduled_event(task_list.clone(), timeout, attempt, None);
            schedule_id = event.event_id; schedule_timestamp = event.timestamp.unwrap_or_default(); attempt = 0;
        }
        self.replicate_decision_task_scheduled_event(DecisionInfo { version: self.state.current_version(), schedule_id, task_list, start_to_close_timeout_seconds: timeout, attempt, schedule_timestamp, original_schedule_timestamp: original, ..Default::default() }, bypass)
    }
    pub fn add_decision_task_scheduled_event(&mut self, bypass: bool) -> Result<DecisionInfo, S::Error> { self.add_decision_task_scheduled_event_as_heartbeat(bypass, self.state.now_nanos()) }
    pub fn add_first_decision_task_scheduled(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let delay = event.workflow_execution_started_event_attributes.as_ref().and_then(|a| a.first_decision_task_backoff_seconds).unwrap_or_default();
        if delay != 0 { self.state.delayed_decision_tasks(event) } else { self.add_decision_task_scheduled_event(false).map(|_| ()) }
    }
    pub fn add_decision_task_started_event(&mut self, id: i64, request_id: String, task_list: String, identity: String) -> Result<(Option<HistoryEvent>, DecisionInfo), S::Error> {
        let mut d = self.get_decision_info(id).filter(|d| d.started_id == 0).ok_or_else(|| self.state.invalid_action("DecisionTaskStarted"))?;
        let mut scheduled = d.schedule_id; let mut started = scheduled + 1; let mut timestamp = self.state.now_nanos(); let mut event = None;
        if d.attempt > 0 && (d.schedule_id != self.state.next_event_id() || self.should_update_last_write_version()) {
            let scheduled_event = self.state.add_scheduled_event(task_list, d.start_to_close_timeout_seconds, 0, None); scheduled = scheduled_event.event_id; d.attempt = 0;
        }
        if d.attempt == 0 { let e = self.state.add_started_event(scheduled, request_id.clone(), identity, None); started = e.event_id; timestamp = e.timestamp.unwrap_or_default(); event = Some(e); }
        let d = self.replicate_decision_task_started_event(Some(d), self.state.current_version(), scheduled, started, request_id, timestamp)?;
        Ok((event, d))
    }
    pub fn add_decision_task_completed_event(&mut self, scheduled: i64, mut started: i64, request: &cadence_api::tasks::RespondDecisionTaskCompletedRequest, max_reset_points: usize) -> Result<HistoryEvent, S::Error> {
        let d = self.get_decision_info(scheduled).filter(|d| d.started_id == started).ok_or_else(|| self.state.invalid_action("DecisionTaskCompleted"))?;
        self.delete_decision();
        if d.attempt > 0 {
            let event = self.state.add_scheduled_event(self.state.task_list(), d.start_to_close_timeout_seconds, d.attempt, Some(d.schedule_timestamp));
            let event = self.state.add_started_event(event.event_id, d.request_id, request.identity.clone(), Some(d.started_timestamp)); started = event.event_id;
        }
        let event = self.state.add_completed_event(scheduled, started, request); self.after_completed(&event, max_reset_points)?; Ok(event)
    }
    pub fn add_decision_task_failed_event(&mut self, attributes: DecisionTaskFailedEventAttributes) -> Result<Option<HistoryEvent>, S::Error> {
        let d = self.get_decision_info(attributes.scheduled_event_id).filter(|d| d.started_id == attributes.started_event_id).ok_or_else(|| self.state.invalid_action("DecisionTaskFailed"))?;
        let cause = attributes.cause;
        let event = if d.attempt == 0 { Some(self.state.add_failed_event(attributes)) } else { None };
        self.replicate_decision_task_failed_event(event.as_ref())?;
        if matches!(cause, Some(DecisionTaskFailedCause::ResetWorkflow | DecisionTaskFailedCause::FailoverCloseDecision)) { let mut d = self.state.decision(); d.attempt = 0; self.update_decision(d); }
        Ok(event)
    }
    pub fn add_decision_task_timed_out_event(&mut self, scheduled: i64, started: i64) -> Result<HistoryEvent, S::Error> {
        let d = self.get_decision_info(scheduled).filter(|d| d.started_id == started).ok_or_else(|| self.state.invalid_action("DecisionTaskTimedOut"))?;
        let attributes = cadence_api::tasks::DecisionTaskTimedOutEventAttributes { scheduled_event_id: scheduled, started_event_id: started, timeout_type: Some(TimeoutType::StartToClose), cause: Some(DecisionTaskTimedOutCause::Timeout), fork_event_version: -24, ..Default::default() };
        let event = if d.attempt == 0 { self.state.add_timeout_event(attributes) } else { HistoryEvent { decision_task_timed_out_event_attributes: Some(cadence_api::tasks::DecisionTaskTimedOutEventAttributes { timeout_type: Some(TimeoutType::StartToClose), ..Default::default() }), ..Default::default() } };
        self.replicate_decision_task_timed_out_event(&event)?; Ok(event)
    }
    pub fn add_decision_task_schedule_to_start_timeout_event(&mut self, id: i64) -> Result<Option<HistoryEvent>, S::Error> {
        let d = self.state.decision();
        if d.schedule_id != id || d.started_id > 0 { return Err(self.state.invalid_action("DecisionTaskTimedOut")); }
        let attributes = cadence_api::tasks::DecisionTaskTimedOutEventAttributes { scheduled_event_id: id, timeout_type: Some(TimeoutType::ScheduleToStart), cause: Some(DecisionTaskTimedOutCause::Timeout), fork_event_version: -24, ..Default::default() };
        let event = (d.attempt == 0).then(|| self.state.add_timeout_event(attributes));
        let synthetic = HistoryEvent { decision_task_timed_out_event_attributes: Some(cadence_api::tasks::DecisionTaskTimedOutEventAttributes { timeout_type: Some(TimeoutType::ScheduleToStart), ..Default::default() }), ..Default::default() };
        self.replicate_decision_task_timed_out_event(event.as_ref().unwrap_or(&synthetic))?; Ok(event)
    }
    pub fn add_decision_task_reset_timeout_event(&mut self, attributes: cadence_api::tasks::DecisionTaskTimedOutEventAttributes) -> Result<HistoryEvent, S::Error> {
        if self.state.decision().schedule_id != attributes.scheduled_event_id { return Err(self.state.invalid_action("DecisionTaskTimedOut")); }
        let event = self.state.add_timeout_event(cadence_api::tasks::DecisionTaskTimedOutEventAttributes { timeout_type: Some(TimeoutType::ScheduleToStart), cause: Some(DecisionTaskTimedOutCause::Reset), started_event_id: 0, ..attributes });
        self.replicate_decision_task_timed_out_event(&event)?;
        let mut d = self.state.decision(); d.attempt = 0; self.update_decision(d); Ok(event)
    }
}
