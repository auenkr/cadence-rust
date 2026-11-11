//! `mutable_state_builder_methods_decision.go` plus the logic of
//! `mutable_state_decision_task_manager.go` on [`TypedMutableState`].
//!
//! The Go builder methods are thin wrappers (`checkMutability` + delegate to
//! `decisionTaskManager`); here the manager bodies are merged into the builder
//! methods. Differences forced by the typed state not owning a clock, config,
//! domain entry or task generator:
//! * Go `timeSource.Now()` and config lookups are carried in [`DecisionEnv`].
//! * Task generation is wired via `run_task_generator` (tasks land in `transfer_tasks`/`timer_tasks`).
//! * Logging / metrics go through `tracing` and `TaskGenEnv::metrics`.
//! * `insertWorkflowRequest` stores only the request id (type/version are not
//!   representable in the Rust `WorkflowRequest` yet).
//! * Auto reset points are kept in `DecisionEnv::auto_reset_points` because the
//!   `WorkflowExecutionInfo.auto_reset_points` blob codec is not available here.
//!
//! Timeouts stored on `WorkflowExecutionInfo` are in nanoseconds
//! (`decision_timeout_nanos`), the Go `DecisionInfo.DecisionTimeout` is seconds.

use cadence_api::enums::{DecisionTaskFailedCause, DecisionTaskTimedOutCause, EventType, TimeoutType};
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::{
    DecisionTaskCompletedEventAttributes, DecisionTaskFailedEventAttributes,
    DecisionTaskScheduledEventAttributes, DecisionTaskStartedEventAttributes,
    DecisionTaskTimedOutEventAttributes, RespondDecisionTaskCompletedRequest,
};
use cadence_api::types::{ResetPointInfo, ResetPoints, TaskList};
use cadence_persistence::history_contract::WorkflowRequest;

use super::mutable_state_builder::{TypedMutableState, TypedMutableStateError, EMPTY_EVENT_ID, EMPTY_VERSION};
use super::mutable_state_methods_common::{
    internal_err, Result, WORKFLOW_STATE_RUNNING, WORKFLOW_STATE_ZOMBIE, CLOSE_STATUS_NONE,
};
use crate::mutable_state::DecisionInfo;

/// Go `constants.DefaultHistoryMaxAutoResetPoints`.
pub const DEFAULT_HISTORY_MAX_AUTO_RESET_POINTS: usize = 20;
/// Go `constants.EmptyUUID`.
pub const EMPTY_UUID: &str = "emptyUuid";
/// Go `definition.BinaryChecksums`.
pub const BINARY_CHECKSUMS: &str = "BinaryChecksums";

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Inputs Go reads from `timeSource`, `config`, `shard` and the domain entry.
#[derive(Debug, Clone, Default)]
pub struct DecisionEnv {
    /// Go `timeSource.Now().UnixNano()`.
    pub now_nanos: i64,
    /// Go `config.StickyTTL(domain)` in nanoseconds.
    pub sticky_ttl_nanos: i64,
    /// Go `config.NormalDecisionScheduleToStartTimeout(domain)` in nanoseconds.
    pub normal_schedule_to_start_timeout_nanos: i64,
    /// Go `config.NormalDecisionScheduleToStartMaxAttempts(domain)`.
    pub normal_schedule_to_start_max_attempts: i64,
    /// Go `shard.GetConfig().DecisionRetryCriticalAttempts()`.
    pub decision_retry_critical_attempts: i64,
    /// Go `config.MaxAutoResetPoints(domain)`; 0 means use the default.
    pub max_reset_points: usize,
    /// Go `common.IsAdvancedVisibilityWritingEnabled(...)`.
    pub advanced_visibility_writing: bool,
    /// Decoded `executionInfo.AutoResetPoints` (in/out).
    pub auto_reset_points: Option<ResetPoints>,
    /// Set by `add_binary_checksum_if_not_exists` when Go would call
    /// `taskGenerator.GenerateWorkflowSearchAttrTasks`.
    pub needs_search_attr_tasks: bool,
}

fn secs_to_nanos(s: i32) -> i64 {
    i64::from(s) * NANOS_PER_SECOND
}

fn nanos_to_secs(n: i64) -> i32 {
    (n / NANOS_PER_SECOND) as i32
}

impl TypedMutableState {
    // ---- manager: decision info accessors -------------------------------

    /// Go `getDecisionInfo`.
    fn get_decision_info_internal(&self) -> DecisionInfo {
        let info = &self.execution_info;
        let task_list = if info.sticky_task_list.is_empty() {
            info.task_list.clone()
        } else {
            info.sticky_task_list.clone()
        };
        DecisionInfo {
            version: info.decision_version,
            schedule_id: info.decision_schedule_id,
            started_id: info.decision_started_id,
            request_id: info.decision_request_id.clone(),
            start_to_close_timeout_seconds: nanos_to_secs(info.decision_timeout_nanos),
            attempt: info.decision_attempt,
            started_timestamp: info.decision_started_time_unix_nanos,
            schedule_timestamp: info.decision_scheduled_time_unix_nanos,
            task_list,
            original_schedule_timestamp: info.decision_original_scheduled_time_unix_nanos,
        }
    }

    /// Go `GetDecisionInfo`.
    pub fn get_decision_info(&self, schedule_event_id: i64) -> Option<DecisionInfo> {
        let decision = self.get_decision_info_internal();
        (schedule_event_id == decision.schedule_id).then_some(decision)
    }

    /// Go `UpdateDecision`.
    pub fn update_decision(&mut self, decision: &DecisionInfo) {
        let info = &mut self.execution_info;
        info.decision_version = decision.version;
        info.decision_schedule_id = decision.schedule_id;
        info.decision_started_id = decision.started_id;
        info.decision_request_id = decision.request_id.clone();
        info.decision_timeout_nanos = secs_to_nanos(decision.start_to_close_timeout_seconds);
        info.decision_attempt = decision.attempt;
        info.decision_started_time_unix_nanos = decision.started_timestamp;
        info.decision_scheduled_time_unix_nanos = decision.schedule_timestamp;
        info.decision_original_scheduled_time_unix_nanos = decision.original_schedule_timestamp;
        // NOTE: do not update tasklist in execution info
    }

    /// Go `DeleteDecision`.
    pub fn delete_decision(&mut self) {
        let reset = DecisionInfo {
            version: EMPTY_VERSION,
            schedule_id: EMPTY_EVENT_ID,
            started_id: EMPTY_EVENT_ID,
            request_id: EMPTY_UUID.to_string(),
            start_to_close_timeout_seconds: 0,
            attempt: 0,
            started_timestamp: 0,
            schedule_timestamp: 0,
            task_list: String::new(),
            // Keep the last original scheduled timestamp for heartbeat decisions.
            original_schedule_timestamp: self.execution_info.decision_original_scheduled_time_unix_nanos,
        };
        self.update_decision(&reset);
    }

    /// Go `FailDecision`.
    pub fn fail_decision(&mut self, env: &DecisionEnv, increment_attempt: bool) {
        // Clear stickiness whenever decision fails
        self.clear_stickyness();
        let mut fail = DecisionInfo {
            version: EMPTY_VERSION,
            schedule_id: EMPTY_EVENT_ID,
            started_id: EMPTY_EVENT_ID,
            request_id: EMPTY_UUID.to_string(),
            start_to_close_timeout_seconds: 0,
            started_timestamp: 0,
            task_list: String::new(),
            original_schedule_timestamp: 0,
            ..DecisionInfo::default()
        };
        if increment_attempt {
            fail.attempt = self.execution_info.decision_attempt + 1;
            fail.schedule_timestamp = env.now_nanos;
            if fail.attempt >= env.decision_retry_critical_attempts {
                self.emit_decision_attempt_critical(fail.attempt);
            }
        }
        self.update_decision(&fail);
    }

    /// Go `FailDecision` critical-attempt metrics + warn log.
    pub(crate) fn emit_decision_attempt_critical(&self, attempt: i64) {
        let domain_id = self.execution_info.domain_id.as_str();
        if let Some(m) = &self.task_gen_env.metrics {
            m.record_timer("WorkflowContextScope", "DecisionAttemptTimer", domain_id, attempt);
            m.int_exponential_histogram("WorkflowContextScope", "DecisionAttemptHistogram", domain_id, attempt);
        }
        tracing::warn!(
            domain_id = domain_id,
            workflow_id = %self.execution_info.workflow_id,
            run_id = %self.execution_info.run_id,
            "Critical error processing decision task, retrying."
        );
    }

    /// Go `HasProcessedOrPendingDecision`.
    pub fn has_processed_or_pending_decision(&self) -> bool {
        self.has_pending_decision() || self.get_previous_started_event_id() != EMPTY_EVENT_ID
    }

    /// Go `GetPreviousStartedEventID`.
    pub fn get_previous_started_event_id(&self) -> i64 {
        self.execution_info.last_processed_event
    }

    /// Go `GetPendingDecision`.
    pub fn get_pending_decision(&self) -> Option<DecisionInfo> {
        if self.execution_info.decision_schedule_id == EMPTY_EVENT_ID {
            return None;
        }
        Some(self.get_decision_info_internal())
    }

    /// Go `GetInFlightDecision`.
    pub fn get_in_flight_decision(&self) -> Option<DecisionInfo> {
        if self.execution_info.decision_schedule_id == EMPTY_EVENT_ID
            || self.execution_info.decision_started_id == EMPTY_EVENT_ID
        {
            return None;
        }
        Some(self.get_decision_info_internal())
    }

    /// Go `GetDecisionScheduleToStartTimeout` (nanoseconds).
    pub fn get_decision_schedule_to_start_timeout(&self, env: &DecisionEnv) -> i64 {
        if !self.execution_info.sticky_task_list.is_empty() {
            return self.execution_info.sticky_schedule_to_start_timeout_nanos;
        }
        if self.execution_info.decision_attempt < env.normal_schedule_to_start_max_attempts {
            return env.normal_schedule_to_start_timeout_nanos;
        }
        0
    }

    /// Go `IsStickyTaskListEnabled`.
    pub fn is_sticky_task_list_enabled(&self, env: &DecisionEnv) -> bool {
        if self.execution_info.sticky_task_list.is_empty() {
            return false;
        }
        let expiry = self
            .execution_info
            .last_updated_time_unix_nanos
            .saturating_add(env.sticky_ttl_nanos);
        env.now_nanos <= expiry
    }

    /// Go `CreateTransientDecisionEvents`.
    pub fn create_transient_decision_events(
        &self,
        decision: &DecisionInfo,
        identity: &str,
    ) -> (HistoryEvent, HistoryEvent) {
        use super::history_builder::{
            new_decision_task_scheduled_event_with_info, new_decision_task_started_event_with_info,
        };
        let scheduled = new_decision_task_scheduled_event_with_info(
            decision.schedule_id,
            decision.schedule_timestamp,
            self.execution_info.task_list.clone(),
            decision.start_to_close_timeout_seconds,
            decision.attempt,
        );
        let started = new_decision_task_started_event_with_info(
            decision.started_id,
            decision.started_timestamp,
            decision.schedule_id,
            decision.request_id.clone(),
            identity.to_string(),
        );
        (scheduled, started)
    }

    /// Go `shouldUpdateLastWriteVersion`.
    fn should_update_last_write_version(&self) -> bool {
        match self.get_last_write_version() {
            // The error is version history has no item (first batch of a workflow).
            Err(_) => false,
            Ok(last) => self.get_current_version() != last,
        }
    }

    /// Go `insertWorkflowRequest` for reset requests (only the id is kept).
    fn insert_reset_workflow_request(&mut self, request_id: &str, _version: i64) {
        self.insert_workflow_requests.push(WorkflowRequest {
            request_id: request_id.to_string(),
            data: Default::default(),
        });
    }

    // ---- builder wrappers + manager bodies --------------------------------

    /// Go `AddFirstDecisionTaskScheduled`. A non-zero first-decision backoff
    /// generates the delayed-decision (`WorkflowBackoff`) timer task via
    /// `GenerateDelayedDecisionTasks` and returns `Ok(false)`; otherwise the
    /// decision is scheduled and `Ok(true)` is returned.
    pub fn add_first_decision_task_scheduled(
        &mut self,
        env: &DecisionEnv,
        start_event: &HistoryEvent,
    ) -> Result<bool> {
        self.check_mutability()?;
        let backoff = start_event
            .workflow_execution_started_event_attributes
            .as_ref()
            .and_then(|a| a.first_decision_task_backoff_seconds)
            .unwrap_or_default();
        if backoff != 0 {
            self.run_task_generator(0, |g| g.generate_delayed_decision_tasks(start_event))?;
            return Ok(false);
        }
        self.add_decision_task_scheduled_event(env, false)?;
        Ok(true)
    }

    /// Go `AddDecisionTaskScheduledEvent`.
    pub fn add_decision_task_scheduled_event(
        &mut self,
        env: &DecisionEnv,
        bypass_task_generation: bool,
    ) -> Result<DecisionInfo> {
        self.check_mutability()?;
        self.add_decision_task_scheduled_event_as_heartbeat(env, bypass_task_generation, env.now_nanos)
    }

    /// Go `AddDecisionTaskScheduledEventAsHeartbeat`.
    pub fn add_decision_task_scheduled_event_as_heartbeat(
        &mut self,
        env: &DecisionEnv,
        bypass_task_generation: bool,
        original_scheduled_timestamp: i64,
    ) -> Result<DecisionInfo> {
        let op = "add-decision-task-scheduled-event";
        self.check_mutability()?;
        self.task_gen_env.normal_schedule_to_start_timeout_nanos = env.normal_schedule_to_start_timeout_nanos;
        self.task_gen_env.normal_schedule_to_start_max_attempts = env.normal_schedule_to_start_max_attempts;
        if self.has_pending_decision() {
            return Err(self.create_internal_server_error(op));
        }

        // Tasklist and decision timeout should already be set from workflow execution started event
        let mut task_list = self.execution_info.task_list.clone();
        if self.is_sticky_task_list_enabled(env) {
            task_list = self.execution_info.sticky_task_list.clone();
        } else {
            self.clear_stickyness();
        }
        let timeout_secs = nanos_to_secs(self.execution_info.decision_start_to_close_timeout_nanos);

        // Flush any buffered events before creating the decision.
        if self.has_buffered_events() || self.has_buffered_events_in_db {
            self.execution_info.decision_attempt = 0;
            self.flush_buffered_events()?;
        }

        let mut schedule_id = self.get_next_event_id();
        let mut schedule_time = env.now_nanos;
        let use_non_transient = self.should_update_last_write_version();

        if self.execution_info.decision_attempt == 0 || use_non_transient {
            let attempt = self.execution_info.decision_attempt;
            let tl = task_list.clone();
            let event = self.add_history_event(env.now_nanos, EventType::DecisionTaskScheduled, |e| {
                e.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes {
                    task_list: Some(TaskList { name: tl, ..Default::default() }),
                    start_to_close_timeout_seconds: Some(timeout_secs),
                    attempt,
                });
            })?;
            schedule_id = event.event_id;
            schedule_time = event.timestamp.unwrap_or_default();
            self.execution_info.decision_attempt = 0;
        }

        let attempt = self.execution_info.decision_attempt;
        let version = self.get_current_version();
        self.replicate_decision_task_scheduled_event(
            version,
            schedule_id,
            &task_list,
            timeout_secs,
            attempt,
            schedule_time,
            original_scheduled_timestamp,
            bypass_task_generation,
        )
    }

    /// Go `ReplicateDecisionTaskScheduledEvent` (incl. `GenerateDecisionScheduleTasks`
    /// unless bypassed; schedule-to-start config comes from `task_gen_env`).
    #[allow(clippy::too_many_arguments)]
    pub fn replicate_decision_task_scheduled_event(
        &mut self,
        version: i64,
        schedule_id: i64,
        task_list: &str,
        start_to_close_timeout_seconds: i32,
        attempt: i64,
        schedule_timestamp: i64,
        original_scheduled_timestamp: i64,
        bypass_task_generation: bool,
    ) -> Result<DecisionInfo> {
        // NOTE: for zombie workflow, should not change the state
        if self.execution_info.state != WORKFLOW_STATE_ZOMBIE {
            self.update_workflow_state_close_status(WORKFLOW_STATE_RUNNING, CLOSE_STATUS_NONE)?;
        }
        let decision = DecisionInfo {
            version,
            schedule_id,
            started_id: EMPTY_EVENT_ID,
            request_id: EMPTY_UUID.to_string(),
            start_to_close_timeout_seconds,
            task_list: task_list.to_string(),
            attempt,
            schedule_timestamp,
            started_timestamp: 0,
            original_schedule_timestamp: original_scheduled_timestamp,
        };
        self.update_decision(&decision);
        if !bypass_task_generation && !self.suppress_task_generation {
            let timeout = self.env_decision_schedule_to_start_timeout_nanos();
            self.run_task_generator(timeout, |g| g.generate_decision_schedule_tasks(decision.schedule_id))?;
        }
        Ok(decision)
    }

    /// Go `ReplicateTransientDecisionTaskScheduled`.
    pub fn replicate_transient_decision_task_scheduled(&mut self, env: &DecisionEnv) -> Result<()> {
        if self.has_pending_decision() || self.execution_info.decision_attempt == 0 {
            return Ok(());
        }
        // The schedule ID for this decision is guaranteed to be wrong; see Go comment.
        let decision = DecisionInfo {
            version: self.get_current_version(),
            schedule_id: self.get_next_event_id(),
            started_id: EMPTY_EVENT_ID,
            request_id: EMPTY_UUID.to_string(),
            start_to_close_timeout_seconds: nanos_to_secs(self.execution_info.decision_start_to_close_timeout_nanos),
            task_list: self.execution_info.task_list.clone(),
            attempt: self.execution_info.decision_attempt,
            schedule_timestamp: env.now_nanos,
            started_timestamp: 0,
            ..DecisionInfo::default()
        };
        self.update_decision(&decision);
        if self.suppress_task_generation { return Ok(()); }
        let timeout = self.env_decision_schedule_to_start_timeout_nanos();
        self.run_task_generator(timeout, |g| g.generate_decision_schedule_tasks(decision.schedule_id))
    }

    /// Go `AddDecisionTaskStartedEvent`; `task_list_name`/`identity` are the
    /// `PollForDecisionTaskRequest` TaskList name and identity.
    pub fn add_decision_task_started_event(
        &mut self,
        env: &DecisionEnv,
        schedule_event_id: i64,
        request_id: &str,
        task_list_name: &str,
        identity: &str,
    ) -> Result<(Option<HistoryEvent>, DecisionInfo)> {
        let op = "add-decision-task-started-event";
        self.check_mutability()?;
        let mut decision = match self.get_decision_info(schedule_event_id) {
            Some(d) if d.started_id == EMPTY_EVENT_ID => d,
            _ => return Err(self.create_internal_server_error(op)),
        };

        let mut event = None;
        let mut schedule_id = decision.schedule_id;
        let mut started_id = schedule_id + 1;
        let mut start_time = env.now_nanos;
        let use_non_transient = self.should_update_last_write_version();

        // First check to see if new events came since transient decision was scheduled
        if decision.attempt > 0 && (decision.schedule_id != self.get_next_event_id() || use_non_transient) {
            let timeout = decision.start_to_close_timeout_seconds;
            let tl = task_list_name.to_string();
            let schedule_event = self.add_history_event(env.now_nanos, EventType::DecisionTaskScheduled, |e| {
                e.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes {
                    task_list: Some(TaskList { name: tl, ..Default::default() }),
                    start_to_close_timeout_seconds: Some(timeout),
                    attempt: 0,
                });
            })?;
            schedule_id = schedule_event.event_id;
            decision.attempt = 0;
        }

        // Avoid creating new history events when decisions are continuously failing
        if decision.attempt == 0 {
            let sid = schedule_id;
            let rid = request_id.to_string();
            let ident = identity.to_string();
            let started = self.add_history_event(env.now_nanos, EventType::DecisionTaskStarted, |e| {
                e.decision_task_started_event_attributes = Some(DecisionTaskStartedEventAttributes {
                    scheduled_event_id: sid,
                    identity: ident,
                    request_id: rid,
                });
            })?;
            started_id = started.event_id;
            start_time = started.timestamp.unwrap_or_default();
            event = Some(started);
        }

        let version = self.get_current_version();
        let decision = self.replicate_decision_task_started_event(
            Some(decision),
            version,
            schedule_id,
            started_id,
            request_id,
            start_time,
        )?;
        Ok((event, decision))
    }

    /// Go `ReplicateDecisionTaskStartedEvent` (incl. `GenerateDecisionStartTasks`).
    pub fn replicate_decision_task_started_event(
        &mut self,
        decision: Option<DecisionInfo>,
        version: i64,
        schedule_id: i64,
        started_id: i64,
        request_id: &str,
        timestamp: i64,
    ) -> Result<DecisionInfo> {
        // Replicator calls it with None; it is safe to always lookup the decision then.
        let decision = match decision {
            Some(d) => d,
            None => {
                let Some(mut d) = self.get_decision_info(schedule_id) else {
                    return internal_err(format!("unable to find decision: {schedule_id}"));
                };
                // setting decision attempt to 0 for decision task replication
                d.attempt = 0;
                d
            }
        };
        let updated = DecisionInfo {
            version,
            schedule_id,
            started_id,
            request_id: request_id.to_string(),
            start_to_close_timeout_seconds: decision.start_to_close_timeout_seconds,
            attempt: decision.attempt,
            started_timestamp: timestamp,
            schedule_timestamp: decision.schedule_timestamp,
            task_list: decision.task_list,
            original_schedule_timestamp: decision.original_schedule_timestamp,
        };
        self.update_decision(&updated);
        if !self.suppress_task_generation {
            self.run_task_generator(0, |g| g.generate_decision_start_tasks(schedule_id))?;
        }
        Ok(updated)
    }

    /// Go `CheckResettable` (pending semaphore holds are not modelled yet).
    pub fn check_resettable(&self) -> Result<()> {
        if !self.pending_child_execution_info_ids.is_empty() {
            return Err(TypedMutableStateError::BadRequest(
                "it is not allowed resetting to a point that workflow has pending child types.".into(),
            ));
        }
        if !self.pending_request_cancel_info_ids.is_empty() {
            return Err(TypedMutableStateError::BadRequest(
                "it is not allowed resetting to a point that workflow has pending request cancel.".into(),
            ));
        }
        if !self.pending_signal_info_ids.is_empty() {
            return Err(TypedMutableStateError::BadRequest(
                "it is not allowed resetting to a point that workflow has pending signals to send.".into(),
            ));
        }
        Ok(())
    }

    /// Go `AddDecisionTaskCompletedEvent`.
    pub fn add_decision_task_completed_event(
        &mut self,
        env: &mut DecisionEnv,
        schedule_event_id: i64,
        mut started_event_id: i64,
        request: &RespondDecisionTaskCompletedRequest,
        max_reset_points: usize,
    ) -> Result<HistoryEvent> {
        let op = "add-decision-task-completed-event";
        self.check_mutability()?;
        let decision = match self.get_decision_info(schedule_event_id) {
            Some(d) if d.started_id == started_event_id => d,
            _ => return Err(self.create_internal_server_error(op)),
        };

        self.delete_decision(); // beforeAddDecisionTaskCompletedEvent
        if decision.attempt > 0 {
            // Create corresponding DecisionTaskSchedule and DecisionTaskStarted events
            // for decisions we have been retrying
            let task_list = self.execution_info.task_list.clone();
            let timeout = decision.start_to_close_timeout_seconds;
            let attempt = decision.attempt;
            let scheduled = self.add_history_event(decision.schedule_timestamp, EventType::DecisionTaskScheduled, |e| {
                e.decision_task_scheduled_event_attributes = Some(DecisionTaskScheduledEventAttributes {
                    task_list: Some(TaskList { name: task_list, ..Default::default() }),
                    start_to_close_timeout_seconds: Some(timeout),
                    attempt,
                });
            })?;
            let sid = scheduled.event_id;
            let rid = decision.request_id.clone();
            let ident = request.identity.clone();
            let started = self.add_history_event(decision.started_timestamp, EventType::DecisionTaskStarted, |e| {
                e.decision_task_started_event_attributes = Some(DecisionTaskStartedEventAttributes {
                    scheduled_event_id: sid,
                    identity: ident,
                    request_id: rid,
                });
            })?;
            started_event_id = started.event_id;
        }

        // Now write the completed event
        let started_id = started_event_id;
        let event = self.add_history_event(env.now_nanos, EventType::DecisionTaskCompleted, |e| {
            e.decision_task_completed_event_attributes = Some(DecisionTaskCompletedEventAttributes {
                execution_context: request.execution_context.clone(),
                scheduled_event_id: schedule_event_id,
                started_event_id: started_id,
                identity: request.identity.clone(),
                binary_checksum: request.binary_checksum.clone(),
            });
        })?;
        self.after_add_decision_task_completed_event(env, &event, max_reset_points)?;
        Ok(event)
    }

    /// Go `ReplicateDecisionTaskCompletedEvent`.
    pub fn replicate_decision_task_completed_event(
        &mut self,
        env: &mut DecisionEnv,
        event: &HistoryEvent,
    ) -> Result<()> {
        self.delete_decision(); // beforeAddDecisionTaskCompletedEvent
        let max = if env.max_reset_points == 0 {
            DEFAULT_HISTORY_MAX_AUTO_RESET_POINTS
        } else {
            env.max_reset_points
        };
        self.after_add_decision_task_completed_event(env, event, max)
    }

    /// Go `afterAddDecisionTaskCompletedEvent`.
    fn after_add_decision_task_completed_event(
        &mut self,
        env: &mut DecisionEnv,
        event: &HistoryEvent,
        max_reset_points: usize,
    ) -> Result<()> {
        self.execution_info.last_processed_event = event
            .decision_task_completed_event_attributes
            .as_ref()
            .map(|a| a.started_event_id)
            .unwrap_or_default();
        self.add_binary_checksum_if_not_exists(env, event, max_reset_points)
    }

    /// Go `AddDecisionTaskTimedOutEvent`.
    pub fn add_decision_task_timed_out_event(
        &mut self,
        env: &DecisionEnv,
        schedule_event_id: i64,
        started_event_id: i64,
    ) -> Result<HistoryEvent> {
        let op = "add-decision-task-timed-out-event";
        self.check_mutability()?;
        let dt = match self.get_decision_info(schedule_event_id) {
            Some(d) if d.started_id == started_event_id => d,
            _ => return Err(self.create_internal_server_error(op)),
        };
        let mut event = HistoryEvent {
            decision_task_timed_out_event_attributes: Some(DecisionTaskTimedOutEventAttributes {
                timeout_type: Some(TimeoutType::StartToClose),
                ..Default::default()
            }),
            ..Default::default()
        };
        // Avoid creating new history events when decisions are continuously timing out
        if dt.attempt == 0 {
            event = self.add_decision_timed_out_history_event(
                env.now_nanos,
                schedule_event_id,
                started_event_id,
                TimeoutType::StartToClose,
                "",
                "",
                EMPTY_VERSION,
                "",
                DecisionTaskTimedOutCause::Timeout,
                "",
            )?;
        }
        self.replicate_decision_task_timed_out_event(env, &event)?;
        Ok(event)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_decision_timed_out_history_event(
        &mut self,
        timestamp: i64,
        schedule_event_id: i64,
        started_event_id: i64,
        timeout_type: TimeoutType,
        base_run_id: &str,
        new_run_id: &str,
        fork_event_version: i64,
        reason: &str,
        cause: DecisionTaskTimedOutCause,
        reset_request_id: &str,
    ) -> Result<HistoryEvent> {
        self.add_history_event(timestamp, EventType::DecisionTaskTimedOut, |e| {
            e.decision_task_timed_out_event_attributes = Some(DecisionTaskTimedOutEventAttributes {
                scheduled_event_id: schedule_event_id,
                started_event_id,
                timeout_type: Some(timeout_type),
                base_run_id: base_run_id.to_string(),
                new_run_id: new_run_id.to_string(),
                fork_event_version,
                reason: reason.to_string(),
                cause: Some(cause),
                request_id: reset_request_id.to_string(),
            });
        })
    }

    /// Go `ReplicateDecisionTaskTimedOutEvent`.
    pub fn replicate_decision_task_timed_out_event(&mut self, env: &DecisionEnv, event: &HistoryEvent) -> Result<()> {
        let Some(attrs) = event.decision_task_timed_out_event_attributes.as_ref() else {
            return internal_err("DecisionTaskTimedOut event is missing attributes");
        };
        // Do not increment decision attempt in the case of sticky scheduleToStart timeout
        // to prevent creating next decision as transient (best effort, see Go comment).
        let mut increment_attempt = true;
        if attrs.timeout_type == Some(TimeoutType::ScheduleToStart)
            && !self.execution_info.sticky_task_list.is_empty()
        {
            increment_attempt = false;
        }
        if attrs.cause == Some(DecisionTaskTimedOutCause::Reset) {
            self.insert_reset_workflow_request(&attrs.request_id, event.version);
        }
        self.fail_decision(env, increment_attempt);
        Ok(())
    }

    /// Go `AddDecisionTaskScheduleToStartTimeoutEvent`.
    pub fn add_decision_task_schedule_to_start_timeout_event(
        &mut self,
        env: &DecisionEnv,
        schedule_event_id: i64,
    ) -> Result<Option<HistoryEvent>> {
        let op = "add-decision-task-timed-out-event";
        self.check_mutability()?;
        if self.execution_info.decision_schedule_id != schedule_event_id
            || self.execution_info.decision_started_id > 0
        {
            return Err(self.create_internal_server_error(op));
        }
        // stickyness will be cleared in ReplicateDecisionTaskTimedOutEvent
        let mut event = None;
        if self.execution_info.decision_attempt == 0 {
            let e = self.add_decision_timed_out_history_event(
                env.now_nanos,
                schedule_event_id,
                0,
                TimeoutType::ScheduleToStart,
                "",
                "",
                EMPTY_VERSION,
                "",
                DecisionTaskTimedOutCause::Timeout,
                "",
            )?;
            self.replicate_decision_task_timed_out_event(env, &e)?;
            event = Some(e);
        } else {
            let synthetic = HistoryEvent {
                decision_task_timed_out_event_attributes: Some(DecisionTaskTimedOutEventAttributes {
                    timeout_type: Some(TimeoutType::ScheduleToStart),
                    ..Default::default()
                }),
                ..Default::default()
            };
            self.replicate_decision_task_timed_out_event(env, &synthetic)?;
        }
        Ok(event)
    }

    /// Go `AddDecisionTaskResetTimeoutEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_decision_task_reset_timeout_event(
        &mut self,
        env: &DecisionEnv,
        schedule_event_id: i64,
        base_run_id: &str,
        new_run_id: &str,
        fork_event_version: i64,
        reason: &str,
        reset_request_id: &str,
    ) -> Result<HistoryEvent> {
        let op = "add-decision-task-timed-out-event";
        self.check_mutability()?;
        if self.execution_info.decision_schedule_id != schedule_event_id {
            return Err(self.create_internal_server_error(op));
        }
        let event = self.add_decision_timed_out_history_event(
            env.now_nanos,
            schedule_event_id,
            0,
            TimeoutType::ScheduleToStart,
            base_run_id,
            new_run_id,
            fork_event_version,
            reason,
            DecisionTaskTimedOutCause::Reset,
            reset_request_id,
        )?;
        self.replicate_decision_task_timed_out_event(env, &event)?;
        // always clear decision attempt for reset
        self.execution_info.decision_attempt = 0;
        Ok(event)
    }

    /// Go `AddDecisionTaskFailedEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_decision_task_failed_event(
        &mut self,
        env: &DecisionEnv,
        schedule_event_id: i64,
        started_event_id: i64,
        cause: DecisionTaskFailedCause,
        details: Vec<u8>,
        identity: &str,
        reason: &str,
        bin_checksum: &str,
        base_run_id: &str,
        new_run_id: &str,
        fork_event_version: i64,
        reset_request_id: &str,
    ) -> Result<Option<HistoryEvent>> {
        let op = "add-decision-task-failed-event";
        self.check_mutability()?;
        let attr = DecisionTaskFailedEventAttributes {
            scheduled_event_id: schedule_event_id,
            started_event_id,
            cause: Some(cause),
            details,
            identity: identity.to_string(),
            reason: Some(reason.to_string()),
            binary_checksum: bin_checksum.to_string(),
            base_run_id: base_run_id.to_string(),
            new_run_id: new_run_id.to_string(),
            fork_event_version,
            request_id: reset_request_id.to_string(),
        };
        let dt = match self.get_decision_info(schedule_event_id) {
            Some(d) if d.started_id == started_event_id => d,
            _ => return Err(self.create_internal_server_error(op)),
        };
        // Only emit DecisionTaskFailedEvent for the very first time
        let mut event = None;
        if dt.attempt == 0 {
            event = Some(self.add_history_event(env.now_nanos, EventType::DecisionTaskFailed, |e| {
                e.decision_task_failed_event_attributes = Some(attr);
            })?);
        }
        self.replicate_decision_task_failed_event(env, event.as_ref())?;
        // always clear decision attempt for reset
        if cause == DecisionTaskFailedCause::ResetWorkflow
            || cause == DecisionTaskFailedCause::FailoverCloseDecision
        {
            self.execution_info.decision_attempt = 0;
        }
        Ok(event)
    }

    /// Go `ReplicateDecisionTaskFailedEvent`.
    pub fn replicate_decision_task_failed_event(
        &mut self,
        env: &DecisionEnv,
        event: Option<&HistoryEvent>,
    ) -> Result<()> {
        if let Some(event) = event {
            if let Some(a) = &event.decision_task_failed_event_attributes {
                if a.cause == Some(DecisionTaskFailedCause::ResetWorkflow) {
                    self.insert_reset_workflow_request(&a.request_id, event.version);
                }
            }
        }
        self.fail_decision(env, true);
        Ok(())
    }

    /// Go `addBinaryCheckSumIfNotExists`: add BinaryCheckSum for the first
    /// decisionTaskCompletedID for auto-reset. Updates `env.auto_reset_points`
    /// and the BinaryChecksums search attribute; sets
    /// `env.needs_search_attr_tasks` instead of calling the task generator.
    pub fn add_binary_checksum_if_not_exists(
        &mut self,
        env: &mut DecisionEnv,
        event: &HistoryEvent,
        max_reset_points: usize,
    ) -> Result<()> {
        let bin_checksum = event
            .decision_task_completed_event_attributes
            .as_ref()
            .map(|a| a.binary_checksum.clone())
            .unwrap_or_default();
        if bin_checksum.is_empty() {
            return Ok(());
        }
        let mut curr: Vec<ResetPointInfo> = env
            .auto_reset_points
            .as_ref()
            .map(|p| p.points.clone())
            .unwrap_or_default();

        // List of all recent binary checksums associated with the workflow.
        let mut recent: Vec<String> = Vec::new();
        for rp in &curr {
            recent.push(rp.binary_checksum.clone());
            if rp.binary_checksum == bin_checksum {
                // this checksum already exists
                return Ok(());
            }
        }

        // Go trimBinaryChecksums
        let num = curr.len();
        if num >= max_reset_points {
            let start = num - max_reset_points + 1;
            curr.drain(..start);
            recent.drain(..start);
        }

        // Adding current version of the binary checksum.
        recent.push(bin_checksum.clone());

        let resettable = self.check_resettable().is_ok();
        curr.push(ResetPointInfo {
            binary_checksum: bin_checksum,
            run_id: self.execution_info.run_id.clone(),
            first_decision_completed_id: event.event_id,
            created_time_nano: Some(env.now_nanos),
            expiring_time_nano: None,
            resettable,
        });
        env.auto_reset_points = Some(ResetPoints { points: curr });

        let bytes = serde_json::to_vec(&recent)
            .map_err(|e| TypedMutableStateError::Internal(e.to_string()))?;
        self.execution_info
            .search_attributes
            .insert(BINARY_CHECKSUMS.to_string(), bytes);
        if env.advanced_visibility_writing && !self.suppress_task_generation {
            self.run_task_generator(0, |g| g.generate_workflow_search_attr_tasks())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod decision_tests {
    use super::*;

    fn env() -> DecisionEnv {
        DecisionEnv { now_nanos: 1_000_000_000, ..Default::default() }
    }

    fn scheduled() -> (TypedMutableState, DecisionEnv, DecisionInfo) {
        let mut ms = TypedMutableState::new();
        let env = env();
        let di = ms.add_decision_task_scheduled_event(&env, true).expect("schedule");
        (ms, env, di)
    }

    fn started() -> (TypedMutableState, DecisionEnv, i64, i64) {
        let (mut ms, env, di) = scheduled();
        let (ev, info) = ms
            .add_decision_task_started_event(&env, di.schedule_id, "req-1", "tl", "worker")
            .expect("start");
        assert!(ev.is_some());
        (ms, env, di.schedule_id, info.started_id)
    }

    #[derive(Default)]
    struct Rec(std::sync::Mutex<Vec<String>>);
    impl super::super::mutable_state_methods_common::MutableStateMetrics for Rec {
        fn record_timer(&self, scope: &str, metric: &str, d: &str, v: i64) {
            self.0.lock().unwrap().push(format!("timer {scope} {metric} {d} {v}"));
        }
        fn int_exponential_histogram(&self, scope: &str, metric: &str, d: &str, v: i64) {
            self.0.lock().unwrap().push(format!("hist {scope} {metric} {d} {v}"));
        }
        fn inc_counter(&self, scope: &str, metric: &str, d: &str, _t: Option<(&str, &str)>) {
            self.0.lock().unwrap().push(format!("count {scope} {metric} {d}"));
        }
    }

    #[test]
    fn fail_decision_emits_critical_attempt_metrics_at_threshold() {
        let rec = std::sync::Arc::new(Rec::default());
        let mut ms = TypedMutableState::new();
        ms.task_gen_env.metrics = Some(rec.clone());
        ms.execution_info.domain_id = "d1".into();
        ms.execution_info.decision_attempt = 1;
        let mut env = env();
        env.decision_retry_critical_attempts = 2;
        ms.fail_decision(&env, true); // attempt 2 >= 2
        let got = rec.0.lock().unwrap().clone();
        assert_eq!(got, vec![
            "timer WorkflowContextScope DecisionAttemptTimer d1 2".to_string(),
            "hist WorkflowContextScope DecisionAttemptHistogram d1 2".to_string(),
        ]);
    }

    #[test]
    fn fail_decision_below_threshold_emits_nothing() {
        let rec = std::sync::Arc::new(Rec::default());
        let mut ms = TypedMutableState::new();
        ms.task_gen_env.metrics = Some(rec.clone());
        let mut env = env();
        env.decision_retry_critical_attempts = 10;
        ms.fail_decision(&env, true);
        assert!(rec.0.lock().unwrap().is_empty());
    }

    #[test]
    fn schedule_happy_path() {
        let (ms, _env, di) = scheduled();
        assert_eq!(di.schedule_id, 1);
        assert_eq!(di.started_id, EMPTY_EVENT_ID);
        assert_eq!(ms.execution_info.decision_schedule_id, 1);
        assert!(ms.has_pending_decision());
        assert!(!ms.has_in_flight_decision());
    }

    #[test]
    fn schedule_twice_fails() {
        let (mut ms, env, _) = scheduled();
        assert!(ms.add_decision_task_scheduled_event(&env, true).is_err());
    }

    #[test]
    fn start_happy_path() {
        let (ms, _env, schedule_id, started_id) = started();
        assert_eq!(started_id, schedule_id + 1);
        assert_eq!(ms.execution_info.decision_started_id, started_id);
        assert!(ms.has_in_flight_decision());
    }

    #[test]
    fn start_unknown_schedule_id_fails() {
        let (mut ms, env, _) = scheduled();
        assert!(ms.add_decision_task_started_event(&env, 99, "r", "tl", "w").is_err());
    }

    #[test]
    fn complete_happy_path() {
        let (mut ms, mut env, schedule_id, started_id) = started();
        let req = RespondDecisionTaskCompletedRequest::default();
        let ev = ms
            .add_decision_task_completed_event(&mut env, schedule_id, started_id, &req, 20)
            .expect("complete");
        assert_eq!(ev.event_type, Some(EventType::DecisionTaskCompleted));
        assert_eq!(ms.execution_info.decision_schedule_id, EMPTY_EVENT_ID);
        assert_eq!(ms.execution_info.decision_started_id, EMPTY_EVENT_ID);
        assert!(!ms.has_pending_decision());
    }

    #[test]
    fn complete_wrong_started_id_fails() {
        let (mut ms, mut env, schedule_id, started_id) = started();
        let req = RespondDecisionTaskCompletedRequest::default();
        assert!(ms
            .add_decision_task_completed_event(&mut env, schedule_id, started_id + 5, &req, 20)
            .is_err());
    }

    #[test]
    fn timeout_happy_path() {
        let (mut ms, env, schedule_id, started_id) = started();
        let ev = ms.add_decision_task_timed_out_event(&env, schedule_id, started_id).expect("timeout");
        assert_eq!(ev.event_type, Some(EventType::DecisionTaskTimedOut));
        assert_eq!(ms.execution_info.decision_started_id, EMPTY_EVENT_ID);
        assert_eq!(ms.execution_info.decision_schedule_id, EMPTY_EVENT_ID);
    }

    #[test]
    fn fail_happy_path() {
        let (mut ms, env, schedule_id, started_id) = started();
        ms.add_decision_task_failed_event(
            &env,
            schedule_id,
            started_id,
            DecisionTaskFailedCause::UnhandledDecision,
            vec![],
            "worker",
            "reason",
            "",
            "",
            "",
            0,
            "",
        )
        .expect("fail");
        assert_eq!(ms.execution_info.decision_started_id, EMPTY_EVENT_ID);
        assert_eq!(ms.execution_info.decision_schedule_id, EMPTY_EVENT_ID);
        assert!(!ms.has_in_flight_decision());
    }

    #[test]
    fn schedule_to_start_timeout_happy_path() {
        let (mut ms, env, di) = scheduled();
        let ev = ms
            .add_decision_task_schedule_to_start_timeout_event(&env, di.schedule_id)
            .expect("s2s timeout");
        assert!(ev.is_some());
        assert_eq!(ms.execution_info.decision_schedule_id, EMPTY_EVENT_ID);
    }
}

#[cfg(test)]
mod first_decision_tests {
    use super::*;
    use cadence_api::enums::ContinueAsNewInitiator;
    use cadence_api::service_types::WorkflowExecutionStartedEventAttributes;
    use super::super::mutable_state_task_generator::TaskKind;

    fn start(backoff: i32, initiator: Option<ContinueAsNewInitiator>) -> HistoryEvent {
        HistoryEvent {
            timestamp: Some(5_000_000_000),
            workflow_execution_started_event_attributes: Some(WorkflowExecutionStartedEventAttributes {
                first_decision_task_backoff_seconds: Some(backoff),
                initiator,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn backoff_type(ms: &TypedMutableState) -> Vec<(i32, Option<i64>)> {
        ms.timer_tasks
            .iter()
            .filter_map(|t| match t.kind {
                TaskKind::WorkflowBackoff { timeout_type } => Some((timeout_type, t.visibility_timestamp_unix_nanos)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn zero_backoff_schedules_decision() {
        let mut ms = TypedMutableState::new();
        let env = DecisionEnv { now_nanos: 1_000_000_000, ..Default::default() };
        assert!(ms.add_first_decision_task_scheduled(&env, &start(0, None)).unwrap());
        assert!(ms.has_pending_decision());
        assert!(backoff_type(&ms).is_empty());
    }

    #[test]
    fn backoff_generates_timer_task() {
        for (init, ty) in [
            (None, 1),
            (Some(ContinueAsNewInitiator::CronSchedule), 1),
            (Some(ContinueAsNewInitiator::RetryPolicy), 0),
        ] {
            let mut ms = TypedMutableState::new();
            let env = DecisionEnv { now_nanos: 1_000_000_000, ..Default::default() };
            assert!(!ms.add_first_decision_task_scheduled(&env, &start(10, init)).unwrap());
            assert!(!ms.has_pending_decision());
            assert_eq!(backoff_type(&ms), vec![(ty, Some(15_000_000_000))]);
        }
    }

    #[test]
    fn backoff_with_decider_initiator_errors() {
        let mut ms = TypedMutableState::new();
        let env = DecisionEnv::default();
        let ev = start(10, Some(ContinueAsNewInitiator::Decider));
        assert!(ms.add_first_decision_task_scheduled(&env, &ev).is_err());
    }
}
