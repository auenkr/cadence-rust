//! group: assembly/engine_decision_ops
//!
//! `decision::task_handler::Operations` over `EngineMutableState` (the mutable
//! state calls made by Go `decision_task_handler.go`). Decisions whose state
//! machinery is not ported on the engine mutable state yet return an explicit
//! error naming the decision type; nothing succeeds silently.

use std::sync::Arc;
use std::time::Duration;

use cadence_api::enums::{DecisionType, EventType};
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::*;
use cadence_persistence::history_contract as pc;

use crate::config::Config;
use crate::decision::checker::{self, ValidationContext};
use crate::decision::task_handler::{Activity, ErrorKind, LocalDispatch, OperationError, Operations, Result, WorkflowIdentity};
use crate::decision::{
    DecisionError, DomainReplication, DomainReplicationLookup, ParentDecisionDefaults, SearchAttributePolicy,
};
use crate::engine::types::ActivityLocalDispatchInfo;
use crate::engine::deps::{
    DomainCache, EMPTY_EVENT_ID, WORKFLOW_CLOSE_STATUS_CANCELED, WORKFLOW_CLOSE_STATUS_COMPLETED,
    WORKFLOW_CLOSE_STATUS_FAILED, WORKFLOW_STATE_COMPLETED,
};
use crate::wiring::assembly::engine_state::EngineMutableState;

const NANOS: i64 = 1_000_000_000;

impl LocalDispatch for ActivityLocalDispatchInfo {
    fn set_task_token(&mut self, token: Vec<u8>) {
        self.task_token = token;
    }
    fn set_timestamps(&mut self, scheduled: i64, scheduled_this_attempt: i64, started: i64) {
        self.scheduled_timestamp = Some(scheduled);
        self.scheduled_timestamp_of_this_attempt = Some(scheduled_this_attempt);
        self.started_timestamp = Some(started);
    }
}

fn bad(msg: impl Into<String>) -> OperationError {
    OperationError { kind: ErrorKind::BadRequest, message: msg.into() }
}

fn internal(msg: impl Into<String>) -> OperationError {
    OperationError { kind: ErrorKind::Internal, message: msg.into() }
}

fn secs(d: Duration) -> i32 {
    d.as_secs().min(i32::MAX as u64) as i32
}

pub struct EngineOps<'a> {
    pub state: &'a EngineMutableState,
    pub config: Arc<Config>,
    pub domains: Arc<dyn DomainCache>,
    pub domain_name: String,
    pub identity: String,
    /// Go `workflowSizeChecker.completedID`: the DecisionTaskCompleted event id.
    pub completed_id: i64,
    /// Go `semaphoreMetadataManager.GetSemaphore` results, resolved asynchronously before the
    /// synchronous decision handler runs (see [`resolve_semaphore_lookups`]).
    pub semaphore_lookups: std::collections::HashMap<String, SemaphoreLookup>,
}

/// Result of a `GetSemaphore` metadata lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemaphoreLookup {
    Found,
    /// `EntityNotExistsError`.
    NotFound(String),
    /// Any other persistence error (e.g. Go's "not supported on SQL").
    Failed(String),
}

/// Looks up the metadata of every semaphore named by an `AcquireSemaphore` decision
/// (Go calls `GetSemaphore` when it reaches that decision; the lookup is read-only, so
/// resolving it up front is equivalent).
pub async fn resolve_semaphore_lookups(
    store: &dyn cadence_persistence::SemaphorePersistence,
    domain_id: &str,
    decisions: &[Decision],
) -> std::collections::HashMap<String, SemaphoreLookup> {
    let mut out = std::collections::HashMap::new();
    for d in decisions {
        if d.decision_type != Some(DecisionType::AcquireSemaphore) {
            continue;
        }
        let Some(name) = d.acquire_semaphore_decision_attributes.as_ref().map(|a| a.semaphore_name.clone()) else {
            continue;
        };
        if out.contains_key(&name) {
            continue;
        }
        let r = match store.get_semaphore_metadata(domain_id, &name).await {
            Ok(_) => SemaphoreLookup::Found,
            Err(cadence_persistence::semaphores::AcquireError::NotFound(e)) => SemaphoreLookup::NotFound(e.to_string()),
            Err(e) => SemaphoreLookup::Failed(e.to_string()),
        };
        out.insert(name, r);
    }
    out
}

impl EngineOps<'_> {
    /// Target domain id of an external/child decision; an empty name means the
    /// execution's own domain.
    fn target_domain_id(&self, domain: &str) -> Result<String> {
        if domain.is_empty() {
            return Ok(self.state.inner.lock().unwrap().exec.domain_id.clone());
        }
        self.domains.get_domain(domain).map(|d| d.info.id.clone()).map_err(|e| internal(format!("{e:?}")))
    }

    /// Attributes of this run's start event (Go `GetStartEvent`), preloaded by the
    /// decision handler.
    fn start_attributes(&self) -> Result<cadence_api::service_types::WorkflowExecutionStartedEventAttributes> {
        self.state
            .inner
            .lock()
            .unwrap()
            .prev_start_event
            .clone()
            .and_then(|e| e.workflow_execution_started_event_attributes)
            .ok_or_else(|| internal("workflow start event is missing"))
    }

    /// Close-event common path (Go `AddCompletedWorkflowEvent` & co.).
    fn close_workflow(
        &self,
        ty: EventType,
        status: i32,
        completed_id: i64,
        what: &str,
        fill: impl FnOnce(&mut HistoryEvent),
    ) -> Result<()> {
        let ms = self.state.now_millis();
        let params = self.state.params.clone();
        let mut g = self.state.inner.lock().unwrap();
        if !g.exec.is_running() {
            return Err(internal(format!(
                "invalid history action {what} (next event id {})",
                g.next_event_id
            )));
        }
        let ev = g.push_event(ty, ms * 1_000_000, fill);
        g.exec.state = WORKFLOW_STATE_COMPLETED;
        g.exec.close_status = status;
        g.pc_exec.state = WORKFLOW_STATE_COMPLETED;
        g.pc_exec.close_status = status;
        g.pc_exec.completion_event_batch_id = g.hist.iter().find(|e| e.event_id > 0).map(|e| e.event_id).unwrap_or(completed_id);
        g.clear_stickyness();
        g.gen_workflow_close_tasks(&params, ms * 1_000_000, ev.version);
        Ok(())
    }
}

impl Operations for EngineOps<'_> {
    type Dispatch = ActivityLocalDispatchInfo;

    fn workflow_identity(&self) -> WorkflowIdentity {
        let g = self.state.inner.lock().unwrap();
        WorkflowIdentity {
            domain_id: g.exec.domain_id.clone(),
            workflow_id: g.exec.workflow_id.clone(),
            workflow_type: g.exec.workflow_type_name.clone(),
            run_id: g.exec.run_id.clone(),
        }
    }

    fn count_decision(&mut self, _kind: DecisionType) {}

    fn has_buffered_events(&self) -> bool {
        self.state.inner.lock().unwrap().has_buffered()
    }

    fn workflow_running(&self) -> bool {
        self.state.inner.lock().unwrap().exec.is_running()
    }

    fn cancel_requested(&self) -> bool {
        self.state.inner.lock().unwrap().pc_exec.cancel_requested
    }

    fn multiple_completion(&mut self, _kind: DecisionType) {}

    fn check_workflow_size(&mut self) -> Result<bool> {
        let (size, count) = {
            let g = self.state.inner.lock().unwrap();
            (g.history_size, g.next_event_id - 1)
        };
        let d = &self.domain_name;
        if size > (self.config.history_size_limit_error)(d) || count > (self.config.history_count_limit_error)(d) {
            let completed = self.completed_id;
            self.fail_workflow(completed, crate::decision::FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT, b"Workflow history size / count exceeds limit.")?;
            return Ok(true);
        }
        Ok(false)
    }

    fn check_blob_size(&mut self, _kind: DecisionType, blob: &[u8], message: &str) -> Result<bool> {
        if blob.len() as i64 > (self.config.blob_size_limit_error)(&self.domain_name) {
            let completed = self.completed_id;
            self.fail_workflow(completed, "DECISION_BLOB_SIZE_EXCEEDS_LIMIT", message.as_bytes())?;
            return Ok(true);
        }
        Ok(false)
    }

    fn set_execution_context(&mut self, context: &[u8]) {
        let mut g = self.state.inner.lock().unwrap();
        g.pc_exec.execution_context = context.to_vec();
        g.dirty = true;
    }

    fn validate(&mut self, decision: &mut Decision) -> Result<()> {
        let mut ctx = Validation::new(self);
        // Go: target domain name per decision type; empty means the own domain.
        let target_name = if let Some(a) = decision.request_cancel_external_workflow_execution_decision_attributes.as_ref() {
            a.domain.clone()
        } else if let Some(a) = decision.signal_external_workflow_execution_decision_attributes.as_ref() {
            a.domain.clone()
        } else if let Some(a) = decision.start_child_workflow_execution_decision_attributes.as_ref() {
            a.domain.clone()
        } else {
            String::new()
        };
        if !target_name.is_empty() {
            ctx.target_domain_id = self.target_domain_id(&target_name)?;
        }
        let name = self.domain_name.clone();
        checker::validate(&ctx, decision, &name).map_err(Into::into)
    }

    fn resolve_target_domain(&mut self, name: &str) -> Result<()> {
        // Same-domain calls only; cross-domain resolution goes through the
        // domain cache and then the cross-cluster validation in `validate`.
        self.domains.get_domain(name).map(|_| ()).map_err(|e| internal(format!("{e:?}")))
    }

    fn schedule_activity(&mut self, completed_id: i64, decision: &Decision) -> Result<(Activity, Option<ActivityLocalDispatchInfo>)> {
        let a = decision
            .schedule_activity_task_decision_attributes
            .clone()
            .ok_or_else(|| bad("ScheduleActivityTaskDecisionAttributes is not set on decision."))?;
        let ms = self.state.now_millis();
        let now_nanos = ms * 1_000_000;
        let limit_enabled = (self.config.pending_activity_validation_enabled)();
        let limit = (self.config.pending_activities_count_limit_error)(&self.domain_name);
        let mut g = self.state.inner.lock().unwrap();
        if g.pc_acts.values().any(|x| x.activity_id == a.activity_id) {
            return Err(bad(format!("activity id {} is already pending", a.activity_id)));
        }
        if limit_enabled && g.pc_acts.len() as i64 >= limit {
            // Go execution.ErrTooManyPendingActivities (no trailing dot; task_handler compares the text).
            return Err(internal(crate::decision::ERR_TOO_MANY_PENDING_ACTIVITIES));
        }
        let ev_attrs = ActivityTaskScheduledEventAttributes {
            activity_id: a.activity_id.clone(),
            activity_type: a.activity_type.clone(),
            domain: Some(a.domain.clone()).filter(|d| !d.is_empty()),
            task_list: a.task_list.clone(),
            input: a.input.clone(),
            schedule_to_close_timeout_seconds: a.schedule_to_close_timeout_seconds,
            schedule_to_start_timeout_seconds: a.schedule_to_start_timeout_seconds,
            start_to_close_timeout_seconds: a.start_to_close_timeout_seconds,
            heartbeat_timeout_seconds: a.heartbeat_timeout_seconds,
            decision_task_completed_event_id: completed_id,
            retry_policy: a.retry_policy.clone(),
            header: a.header.clone(),
        };
        let ev = g.push_event(EventType::ActivityTaskScheduled, ms * 1_000_000, |e| {
            e.activity_task_scheduled_event_attributes = Some(ev_attrs);
        });
        let sid = ev.event_id;
        let mut info = pc::ActivityInfo {
            version: ev.version,
            // Go ReplicateActivityTaskScheduledEvent(firstEventID, ..): the first event of
            // the history batch (a transient decision's scheduled event precedes the completion).
            scheduled_event_batch_id: g
                .hist
                .iter()
                .find(|e| e.event_id > 0)
                .map(|e| e.event_id)
                .unwrap_or(completed_id),
            scheduled_time_unix_nanos: now_nanos,
            started_id: EMPTY_EVENT_ID,
            activity_id: a.activity_id.clone(),
            schedule_to_start_timeout_seconds: a.schedule_to_start_timeout_seconds.unwrap_or(0),
            schedule_to_close_timeout_seconds: a.schedule_to_close_timeout_seconds.unwrap_or(0),
            start_to_close_timeout_seconds: a.start_to_close_timeout_seconds.unwrap_or(0),
            heartbeat_timeout_seconds: a.heartbeat_timeout_seconds.unwrap_or(0),
            cancel_request_id: EMPTY_EVENT_ID,
            task_list: a.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
            task_list_kind: a.task_list.as_ref().and_then(|t| t.kind).map(|k| k as i32).unwrap_or(0),
            ..Default::default()
        };
        if let Some(p) = a.retry_policy.as_ref() {
            info.has_retry_policy = true;
            info.retry_initial_interval_seconds = p.initial_interval_in_seconds;
            info.retry_maximum_interval_seconds = p.maximum_interval_in_seconds;
            info.retry_maximum_attempts = p.maximum_attempts;
            info.retry_backoff_coefficient = p.backoff_coefficient;
            info.retry_non_retriable_errors = p.non_retriable_error_reasons.clone();
            if p.expiration_interval_in_seconds > 0 {
                info.retry_expiration_time_unix_nanos = now_nanos + p.expiration_interval_in_seconds as i64 * NANOS;
            }
        }
        g.pc_acts.insert(sid, info);
        g.refresh_activity(sid);
        // Go: local dispatch skips the matching transfer task; the worker runs the
        // activity directly and history records ActivityTaskStarted below.
        let local_dispatch = (self.config.enable_activity_local_dispatch_by_domain)(&self.domain_name)
            && a.request_local_dispatch;
        if !local_dispatch {
            g.gen_activity_transfer_task(sid);
        }
        let activity = Activity {
            schedule_id: sid,
            started_id: EMPTY_EVENT_ID,
            cancel_event_id: EMPTY_EVENT_ID,
            activity_id: a.activity_id,
            scheduled_time_unix_nanos: now_nanos,
            started_time_unix_nanos: 0,
        };
        let dispatch = local_dispatch.then(|| ActivityLocalDispatchInfo {
            activity_id: activity.activity_id.clone(),
            scheduled_timestamp: None,
            started_timestamp: None,
            scheduled_timestamp_of_this_attempt: None,
            task_token: Vec::new(),
        });
        Ok((activity, dispatch))
    }

    /// Go `AddActivityTaskStartedEvent(ai, scheduleEventID, uuid, identity)` for a locally dispatched activity.
    fn start_local_activity(&mut self, activity: &mut Activity, request_id: &str, identity: &str) -> Result<()> {
        let info = {
            let g = self.state.inner.lock().unwrap();
            g.activities
                .get(&activity.schedule_id)
                .cloned()
                .ok_or_else(|| internal("pending activity not found for local dispatch"))?
        };
        let started = self
            .state
            .ev_activity_started(&info, activity.schedule_id, request_id, identity)
            .map_err(|e| internal(format!("{e:?}")))?;
        activity.started_id = started.started_id;
        activity.started_time_unix_nanos = self
            .state
            .inner
            .lock()
            .unwrap()
            .pc_acts
            .get(&activity.schedule_id)
            .map(|a| a.started_time_unix_nanos)
            .unwrap_or_default();
        Ok(())
    }

    fn request_cancel_activity(&mut self, completed_id: i64, id: &str, _identity: &str) -> Result<Activity> {
        let ms = self.state.now_millis();
        let mut g = self.state.inner.lock().unwrap();
        let Some((sid, info)) = g.pc_acts.iter().find(|(_, a)| a.activity_id == id).map(|(s, a)| (*s, a.clone())) else {
            return Err(bad(format!("unable to find activity ID: {id}")));
        };
        if info.cancel_requested {
            return Err(bad(format!("activity {id} cancellation already requested")));
        }
        let activity_id = id.to_string();
        let ev = g.push_event(EventType::ActivityTaskCancelRequested, ms * 1_000_000, |e| {
            e.activity_task_cancel_requested_event_attributes = Some(ActivityTaskCancelRequestedEventAttributes {
                activity_id,
                decision_task_completed_event_id: completed_id,
            });
        });
        if let Some(a) = g.pc_acts.get_mut(&sid) {
            a.cancel_requested = true;
            a.cancel_request_id = ev.event_id;
        }
        g.refresh_activity(sid);
        Ok(Activity {
            schedule_id: sid,
            started_id: info.started_id,
            cancel_event_id: ev.event_id,
            activity_id: info.activity_id,
            scheduled_time_unix_nanos: info.scheduled_time_unix_nanos,
            started_time_unix_nanos: info.started_time_unix_nanos,
        })
    }

    fn cancel_activity(&mut self, activity: &Activity, details: &[u8], identity: &str) -> Result<()> {
        self.state
            .ev_activity_canceled(activity.schedule_id, activity.started_id, activity.cancel_event_id, details, identity)
            .map_err(|e| internal(format!("{e:?}")))
    }

    fn request_cancel_activity_failed(&mut self, completed_id: i64, id: &str, cause: &str) -> Result<()> {
        let ms = self.state.now_millis();
        let (activity_id, cause) = (id.to_string(), cause.to_string());
        let mut g = self.state.inner.lock().unwrap();
        g.push_event(EventType::RequestCancelActivityTaskFailed, ms * 1_000_000, |e| {
            e.request_cancel_activity_task_failed_event_attributes = Some(RequestCancelActivityTaskFailedEventAttributes {
                activity_id,
                cause,
                decision_task_completed_event_id: completed_id,
            });
        });
        Ok(())
    }

    fn start_timer(&mut self, completed_id: i64, decision: &Decision) -> Result<()> {
        let a = decision
            .start_timer_decision_attributes
            .clone()
            .ok_or_else(|| bad("StartTimerDecisionAttributes is not set on decision."))?;
        let ms = self.state.now_millis();
        let mut g = self.state.inner.lock().unwrap();
        if g.timers.contains_key(&a.timer_id) {
            return Err(bad(format!("timer id {} is already pending", a.timer_id)));
        }
        let fire = a.start_to_fire_timeout_seconds;
        let timer_id = a.timer_id.clone();
        let ev = g.push_event(EventType::TimerStarted, ms * 1_000_000, |e| {
            e.timer_started_event_attributes = Some(TimerStartedEventAttributes {
                timer_id,
                start_to_fire_timeout_seconds: fire,
                decision_task_completed_event_id: completed_id,
            });
        });
        let info = pc::TimerInfo {
            version: ev.version,
            started_id: ev.event_id,
            expiry_time_unix_nanos: ms * 1_000_000 + fire.unwrap_or(0) * NANOS,
            task_id: 0,
        };
        g.delete_timers.remove(&a.timer_id);
        g.upsert_timers.insert(a.timer_id.clone());
        g.timers.insert(a.timer_id, info);
        g.dirty = true;
        Ok(())
    }

    fn cancel_timer(&mut self, completed_id: i64, decision: &Decision, identity: &str) -> Result<()> {
        let a = decision
            .cancel_timer_decision_attributes
            .clone()
            .ok_or_else(|| bad("CancelTimerDecisionAttributes is not set on decision."))?;
        let ms = self.state.now_millis();
        let identity = identity.to_string();
        let mut g = self.state.inner.lock().unwrap();
        let running = g.timers.get(&a.timer_id).cloned();
        let started_id = match &running {
            Some(info) => info.started_id,
            None => {
                // Go checkAndClearTimerFiredEvent: the timer may already have fired into the
                // persisted buffered events, the update-buffered events or the new history.
                match g.check_and_clear_timer_fired_event(&a.timer_id) {
                    Some(fired) => fired.timer_fired_event_attributes.map(|t| t.started_event_id).unwrap_or_default(),
                    None => return Err(bad(format!("unable to find timer: {}", a.timer_id))),
                }
            }
        };
        let timer_id = a.timer_id.clone();
        g.push_event(EventType::TimerCanceled, ms * 1_000_000, |e| {
            e.timer_canceled_event_attributes = Some(TimerCanceledEventAttributes {
                timer_id,
                started_event_id: started_id,
                decision_task_completed_event_id: completed_id,
                identity,
            });
        });
        if running.is_some() {
            g.timers.remove(&a.timer_id);
            g.upsert_timers.remove(&a.timer_id);
            g.delete_timers.insert(a.timer_id);
        }
        g.dirty = true;
        Ok(())
    }

    fn cancel_timer_failed(&mut self, completed_id: i64, decision: &Decision, identity: &str) -> Result<()> {
        let timer_id = decision.cancel_timer_decision_attributes.as_ref().map(|a| a.timer_id.clone()).unwrap_or_default();
        let ms = self.state.now_millis();
        let identity = identity.to_string();
        let mut g = self.state.inner.lock().unwrap();
        g.push_event(EventType::CancelTimerFailed, ms * 1_000_000, |e| {
            e.cancel_timer_failed_event_attributes = Some(CancelTimerFailedEventAttributes {
                timer_id,
                cause: "TIMER_ID_UNKNOWN".to_string(),
                decision_task_completed_event_id: completed_id,
                identity,
            });
        });
        Ok(())
    }

    fn complete_workflow(&mut self, completed_id: i64, decision: &Decision) -> Result<()> {
        let result = decision
            .complete_workflow_execution_decision_attributes
            .as_ref()
            .map(|a| a.result.clone())
            .unwrap_or_default();
        self.close_workflow(
            EventType::WorkflowExecutionCompleted,
            WORKFLOW_CLOSE_STATUS_COMPLETED,
            completed_id,
            "WorkflowExecutionCompleted",
            |e| {
                e.workflow_execution_completed_event_attributes = Some(WorkflowExecutionCompletedEventAttributes {
                    result,
                    decision_task_completed_event_id: completed_id,
                });
            },
        )
    }

    fn fail_workflow(&mut self, completed_id: i64, reason: &str, details: &[u8]) -> Result<()> {
        let (reason, details) = (reason.to_string(), details.to_vec());
        self.close_workflow(
            EventType::WorkflowExecutionFailed,
            WORKFLOW_CLOSE_STATUS_FAILED,
            completed_id,
            "WorkflowExecutionFailed",
            |e| {
                e.workflow_execution_failed_event_attributes = Some(WorkflowExecutionFailedEventAttributes {
                    reason: Some(reason),
                    details,
                    decision_task_completed_event_id: completed_id,
                });
            },
        )
    }

    fn cancel_workflow(&mut self, completed_id: i64, details: &[u8]) -> Result<()> {
        let details = details.to_vec();
        self.close_workflow(
            EventType::WorkflowExecutionCanceled,
            WORKFLOW_CLOSE_STATUS_CANCELED,
            completed_id,
            "WorkflowExecutionCanceled",
            |e| {
                e.workflow_execution_canceled_event_attributes = Some(WorkflowExecutionCanceledEventAttributes {
                    decision_task_completed_event_id: completed_id,
                    details,
                });
            },
        )
    }

    fn cron_backoff_seconds(&mut self) -> Result<Option<i32>> {
        self.state.workflow_cron_backoff().map(|d| d.map(secs)).map_err(|e| internal(format!("{e:?}")))
    }

    fn retry_backoff_seconds(&self, reason: &str) -> Option<i32> {
        self.state.workflow_retry_backoff(reason).map(secs)
    }

    fn retry_cron_continue_as_new(
        &mut self,
        completed_id: i64,
        seconds: i32,
        retry: bool,
        reason: Option<&str>,
        details: &[u8],
        completion_result: Option<&[u8]>,
    ) -> Result<()> {
        let start = self.start_attributes()?;
        let initiator = if retry {
            cadence_api::enums::ContinueAsNewInitiator::RetryPolicy
        } else {
            cadence_api::enums::ContinueAsNewInitiator::CronSchedule
        };
        let attrs = crate::decision::task_handler::retry_cron_continue_as_new_attributes(
            &start,
            seconds,
            initiator,
            reason.map(str::to_string),
            details.to_vec(),
            completion_result.map(<[u8]>::to_vec).unwrap_or_default(),
            start.active_cluster_selection_policy.clone(),
        );
        let parent_domain = start.parent_workflow_domain.clone().unwrap_or_default();
        self.state
            .add_continue_as_new_event(completed_id, completed_id, &parent_domain, &attrs)
            .map_err(|e| internal(format!("{e:?}")))
    }

    fn start_last_completion_result(&mut self) -> Result<Vec<u8>> {
        Ok(self.start_attributes()?.last_completion_result)
    }

    fn parent_domain_name(&mut self) -> Result<String> {
        let (parent_wf, parent_domain) = {
            let g = self.state.inner.lock().unwrap();
            (g.pc_exec.parent_workflow_id.clone(), g.pc_exec.parent_domain_id.clone())
        };
        if parent_wf.is_empty() {
            return Ok(String::new());
        }
        self.domains.get_domain_name(&parent_domain).map_err(|e| internal(format!("{e:?}")))
    }

    fn execution_domain_name(&mut self) -> Result<String> {
        Ok(self.state.domain.info.name.clone())
    }

    fn continue_as_new(&mut self, completed_id: i64, decision: &Decision, parent_domain_name: &str) -> Result<()> {
        let a = decision
            .continue_as_new_workflow_execution_decision_attributes
            .clone()
            .ok_or_else(|| bad("ContinueAsNewWorkflowExecutionDecisionAttributes is not set on decision."))?;
        self.state
            .add_continue_as_new_event(completed_id, completed_id, parent_domain_name, &a)
            .map_err(|e| internal(format!("{e:?}")))
    }

    fn max_semaphore_name_length(&self) -> usize {
        (self.config.semaphore_name_max_length)(&self.domain_name).max(0) as usize
    }

    fn record_marker(&mut self, completed_id: i64, decision: &Decision) -> Result<()> {
        let a = decision
            .record_marker_decision_attributes
            .clone()
            .ok_or_else(|| bad("RecordMarkerDecisionAttributes is not set on decision."))?;
        let ms = self.state.now_millis();
        let mut g = self.state.inner.lock().unwrap();
        g.push_event(EventType::MarkerRecorded, ms * 1_000_000, |e| {
            e.marker_recorded_event_attributes = Some(MarkerRecordedEventAttributes {
                marker_name: a.marker_name,
                details: a.details,
                decision_task_completed_event_id: completed_id,
                header: a.header,
            });
        });
        Ok(())
    }

    fn initiate_cancel_external(&mut self, completed_id: i64, request_id: &str, decision: &Decision) -> Result<()> {
        let a = decision
            .request_cancel_external_workflow_execution_decision_attributes
            .clone()
            .ok_or_else(|| bad("RequestCancelExternalWorkflowExecutionDecisionAttributes is not set on decision."))?;
        let target_domain_id = self.target_domain_id(&a.domain)?;
        let ms = self.state.now_millis();
        let mut g = self.state.inner.lock().unwrap();
        let attrs = a.clone();
        let ev = g.push_event(EventType::RequestCancelExternalWorkflowExecutionInitiated, ms * 1_000_000, |e| {
            e.request_cancel_external_workflow_execution_initiated_event_attributes =
                Some(RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                    decision_task_completed_event_id: completed_id,
                    domain: attrs.domain,
                    workflow_execution: Some(cadence_api::types::WorkflowExecution {
                        workflow_id: attrs.workflow_id,
                        run_id: attrs.run_id,
                    }),
                    control: attrs.control,
                    child_workflow_only: attrs.child_workflow_only,
                });
        });
        let batch_id = g.hist.iter().find(|e| e.event_id > 0).map(|e| e.event_id).unwrap_or(completed_id);
        g.pc_cancels.insert(
            ev.event_id,
            pc::RequestCancelInfo {
                version: ev.version,
                initiated_event_batch_id: batch_id,
                cancel_request_id: request_id.to_string(),
            },
        );
        g.upsert_cancels.insert(ev.event_id);
        g.gen_external_task(true, ev.event_id, ev.version, &target_domain_id, &a.workflow_id, &a.run_id, a.child_workflow_only);
        Ok(())
    }

    fn initiate_signal_external(&mut self, completed_id: i64, request_id: &str, decision: &Decision) -> Result<()> {
        let a = decision
            .signal_external_workflow_execution_decision_attributes
            .clone()
            .ok_or_else(|| bad("SignalExternalWorkflowExecutionDecisionAttributes is not set on decision."))?;
        let target_domain_id = self.target_domain_id(&a.domain)?;
        let exec = a.execution.clone().unwrap_or_default();
        let ms = self.state.now_millis();
        let mut g = self.state.inner.lock().unwrap();
        let attrs = a.clone();
        let ev = g.push_event(EventType::SignalExternalWorkflowExecutionInitiated, ms * 1_000_000, |e| {
            e.signal_external_workflow_execution_initiated_event_attributes =
                Some(SignalExternalWorkflowExecutionInitiatedEventAttributes {
                    decision_task_completed_event_id: completed_id,
                    domain: attrs.domain,
                    workflow_execution: attrs.execution,
                    signal_name: attrs.signal_name,
                    input: attrs.input,
                    control: attrs.control,
                });
        });
        let batch_id = g.hist.iter().find(|e| e.event_id > 0).map(|e| e.event_id).unwrap_or(completed_id);
        g.pc_signals.insert(
            ev.event_id,
            pc::SignalInfo {
                version: ev.version,
                initiated_event_batch_id: batch_id,
                request_id: request_id.to_string(),
                name: a.signal_name.clone(),
                input: a.input.clone(),
                control: a.control.clone(),
            },
        );
        g.upsert_signals.insert(ev.event_id);
        g.gen_external_task(false, ev.event_id, ev.version, &target_domain_id, &exec.workflow_id, &exec.run_id, a.child_workflow_only);
        Ok(())
    }

    fn initiate_child(&mut self, completed_id: i64, request_id: &str, decision: &Decision) -> Result<()> {
        let a = decision
            .start_child_workflow_execution_decision_attributes
            .clone()
            .ok_or_else(|| bad("StartChildWorkflowExecutionDecisionAttributes is not set on decision."))?;
        let target_domain_id = self.target_domain_id(&a.domain)?;
        let ms = self.state.now_millis();
        let mut g = self.state.inner.lock().unwrap();
        let attrs = a.clone();
        let ev = g.push_event(EventType::StartChildWorkflowExecutionInitiated, ms * 1_000_000, |e| {
            e.start_child_workflow_execution_initiated_event_attributes =
                Some(StartChildWorkflowExecutionInitiatedEventAttributes {
                    domain: attrs.domain,
                    workflow_id: attrs.workflow_id,
                    workflow_type: attrs.workflow_type,
                    task_list: attrs.task_list,
                    input: attrs.input,
                    execution_start_to_close_timeout_seconds: attrs.execution_start_to_close_timeout_seconds,
                    task_start_to_close_timeout_seconds: attrs.task_start_to_close_timeout_seconds,
                    parent_close_policy: attrs.parent_close_policy,
                    control: attrs.control,
                    decision_task_completed_event_id: completed_id,
                    workflow_id_reuse_policy: attrs.workflow_id_reuse_policy,
                    header: attrs.header,
                    retry_policy: attrs.retry_policy,
                    cron_schedule: attrs.cron_schedule,
                    memo: attrs.memo,
                    search_attributes: attrs.search_attributes,
                    cron_overlap_policy: attrs.cron_overlap_policy,
                    ..Default::default()
                });
        });
        let blob = {
            use cadence_thrift::ToThrift;
            cadence_persistence::sqlblobs::Encoding::ThriftRW
                .encode_versioned(&ev.to_thrift())
                .map_err(|e| internal(format!("encode child initiated event: {e}")))?
        };
        let batch_id = g.hist.iter().find(|e| e.event_id > 0).map(|e| e.event_id).unwrap_or(completed_id);
        g.pc_children.insert(
            ev.event_id,
            pc::ChildExecutionInfo {
                version: ev.version,
                initiated_event_batch_id: batch_id,
                started_id: EMPTY_EVENT_ID,
                initiated_event: blob,
                initiated_event_encoding: "thriftrw".to_string(),
                started_workflow_id: a.workflow_id.clone(),
                create_request_id: request_id.to_string(),
                domain_id: target_domain_id.clone(),
                workflow_type_name: a.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
                parent_close_policy: a.parent_close_policy.map(|p| p as i32).unwrap_or(0),
                ..Default::default()
            },
        );
        g.children.insert(
            ev.event_id,
            crate::engine::deps_ms::ChildExecutionInfo {
                started_id: EMPTY_EVENT_ID,
                started_workflow_id: a.workflow_id.clone(),
            },
        );
        g.upsert_children.insert(ev.event_id);
        g.gen_child_workflow_task(ev.event_id, &target_domain_id);
        Ok(())
    }

    fn parent_close_policy_enabled(&self) -> bool {
        (self.config.enable_parent_close_policy)(&self.domain_name)
    }

    fn upsert_search_attributes(&mut self, completed_id: i64, decision: &Decision) -> Result<()> {
        let a = decision
            .upsert_workflow_search_attributes_decision_attributes
            .clone()
            .ok_or_else(|| bad("UpsertWorkflowSearchAttributesDecisionAttributes is not set on decision."))?;
        let fields = a.search_attributes.as_ref().and_then(|s| s.indexed_fields.clone()).unwrap_or_default();
        let ms = self.state.now_millis();
        let mut g = self.state.inner.lock().unwrap();
        g.push_event(EventType::UpsertWorkflowSearchAttributes, ms * 1_000_000, |e| {
            e.upsert_workflow_search_attributes_event_attributes = Some(UpsertWorkflowSearchAttributesEventAttributes {
                decision_task_completed_event_id: completed_id,
                search_attributes: a.search_attributes,
            });
        });
        // Go ReplicateUpsertWorkflowSearchAttributesEvent: merge into the execution info.
        for (k, v) in &fields {
            g.pc_exec.search_attributes.insert(k.clone(), v.clone());
        }
        let merged = g.exec.search_attributes.get_or_insert_with(Default::default);
        for (k, v) in fields {
            merged.insert(k, v);
        }
        g.gen_search_attr_task();
        Ok(())
    }

    fn domain_name(&self) -> &str {
        &self.domain_name
    }

    fn semaphore_enabled(&self) -> bool {
        (self.config.enable_distributed_semaphore)(&self.domain_name)
    }

    fn semaphore_exists(&mut self, name: &str) -> Result<()> {
        match self.semaphore_lookups.get(name) {
            Some(SemaphoreLookup::Found) => Ok(()),
            Some(SemaphoreLookup::NotFound(m)) => {
                Err(OperationError { kind: ErrorKind::NotFound, message: m.clone() })
            }
            Some(SemaphoreLookup::Failed(m)) => Err(internal(m.clone())),
            None => Err(internal(format!("semaphore {name:?} metadata was not resolved"))),
        }
    }

    fn semaphore_wait_timeout_seconds(&self, supplied: Option<i32>) -> i32 {
        supplied.unwrap_or_else(|| secs((self.config.semaphore_acquire_default_wait_timeout)(&self.domain_name)))
    }

    fn initiate_semaphore(&mut self, completed_id: i64, name: &str, wait_seconds: i32) -> Result<()> {
        use crate::wiring::assembly::engine_semaphore::SemaphoreOpError;
        match self.state.add_semaphore_acquire_initiated(completed_id, name, wait_seconds) {
            Ok(_) => Ok(()),
            Err(SemaphoreOpError::Caller(m)) => Err(bad(m)),
            Err(SemaphoreOpError::Internal(m)) => Err(internal(m)),
        }
    }
}

/// Snapshot of what the decision checker needs (Go `attrValidator`).
struct Validation {
    domain_id: String,
    domain_name: String,
    config: Arc<Config>,
    domains: Arc<dyn DomainCache>,
    defaults: ParentDecisionDefaults,
    /// Go `targetDomainID`: resolved per decision from the decision's domain name.
    target_domain_id: String,
}

impl Validation {
    fn new(ops: &EngineOps<'_>) -> Self {
        let g = ops.state.inner.lock().unwrap();
        Self {
            domain_id: g.exec.domain_id.clone(),
            domain_name: ops.domain_name.clone(),
            config: ops.config.clone(),
            domains: ops.domains.clone(),
            target_domain_id: g.exec.domain_id.clone(),
            defaults: ParentDecisionDefaults {
                workflow_type_name: g.exec.workflow_type_name.clone(),
                task_list_name: g.exec.task_list.clone(),
                task_list_kind: g.exec.task_list_kind,
                workflow_timeout_seconds: g.exec.workflow_timeout,
                decision_timeout_seconds: g.exec.decision_start_to_close_timeout,
                active_cluster_selection_policy: g.exec.active_cluster_selection_policy.clone(),
            },
        }
    }
}

impl DomainReplicationLookup for Validation {
    fn get_domain_by_id(&self, domain_id: &str) -> std::result::Result<DomainReplication, DecisionError> {
        if domain_id == self.domain_id {
            return Ok(DomainReplication {
                domain_id: self.domain_id.clone(),
                name: self.domain_name.clone(),
                clusters: Vec::new(),
            });
        }
        let e = self.domains.get_domain_by_id(domain_id).map_err(|e| DecisionError::internal(format!("{e:?}")))?;
        Ok(DomainReplication { domain_id: e.info.id.clone(), name: e.info.name.clone(), clusters: e.clusters.clone() })
    }

    fn cross_cluster_operations_enabled(&self, _source_domain_name: &str) -> bool {
        false
    }
}

impl ValidationContext for Validation {
    fn source_domain_id(&self) -> &str {
        &self.domain_id
    }

    fn target_domain_id(&self) -> &str {
        &self.target_domain_id
    }

    fn defaults(&self) -> ParentDecisionDefaults {
        self.defaults.clone()
    }

    fn max_length(&self, field: &str, domain: &str) -> usize {
        let c = &self.config;
        let d = if domain.is_empty() { self.domain_name.as_str() } else { domain };
        let v = match field {
            "TaskList" => (c.task_list_name_max_length)(d),
            "ActivityID" => (c.activity_id_max_length)(d),
            "ActivityType" => (c.activity_type_max_length)(d),
            "Domain" => (c.domain_name_max_length)(d),
            "TimerID" => (c.timer_id_max_length)(d),
            "MarkerName" => (c.marker_name_max_length)(d),
            "WorkflowID" => (c.workflow_id_max_length)(d),
            "WorkflowType" => (c.workflow_type_max_length)(d),
            "SemaphoreName" => (c.semaphore_name_max_length)(d),
            _ => i64::MAX,
        };
        v.max(0) as usize
    }

    fn max_schedule_to_start_retry_seconds(&self) -> i32 {
        secs((self.config.activity_max_schedule_to_start_timeout_for_retry)(&self.domain_name))
    }

    fn reserved_task_list_prefix(&self) -> &str {
        "/__cadence_sys/"
    }

    fn search_policy(&self, domain: &str) -> std::result::Result<SearchAttributePolicy, DecisionError> {
        // Go `searchAttributesValidator`: dynamic config driven key whitelist + limits.
        use cadence_api::enums::IndexedValueType as T;
        let c = &self.config;
        let mut valid_types = std::collections::HashMap::new();
        for (k, v) in (c.valid_search_attributes)() {
            let t = match &v {
                serde_json::Value::Number(n) => n.as_f64().and_then(|f| T::from_i32(f as i32)),
                serde_json::Value::String(s) => T::parse(s),
                _ => None,
            };
            let t = t.ok_or_else(|| DecisionError::internal(format!("unknown index value type for key {k}")))?;
            valid_types.insert(k, t);
        }
        Ok(SearchAttributePolicy {
            enabled: (c.enable_query_attribute_validation)(),
            valid_types,
            number_of_keys_limit: (c.search_attributes_number_of_keys_limit)(domain).max(0) as usize,
            value_size_limit: (c.search_attributes_size_of_value_limit)(domain).max(0) as usize,
            total_size_limit: (c.search_attributes_total_size_limit)(domain).max(0) as usize,
        })
    }

    fn validate_cron(&self, schedule: &str) -> std::result::Result<(), DecisionError> {
        if schedule.is_empty() {
            return Ok(());
        }
        // Go `backoff.ValidateSchedule`: invalid schedules are BadRequestError.
        let now = cadence_common::cron::naive_from_nanos(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        );
        cadence_common::cron::validate_schedule(schedule, now).map(|_| ()).map_err(DecisionError::new)
    }

    fn id_length_observation(&self, _field: &str, _domain: &str, _value: &str) {}

    fn domain_name_by_id(&self, domain_id: &str) -> std::result::Result<String, DecisionError> {
        self.domains.get_domain_name(domain_id).map_err(|e| DecisionError::internal(format!("{e:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::task_handler::TaskHandler;
    use crate::engine::deps_ms::MutableState;
    use crate::engine::deps::{DomainCacheEntry, DomainInfo, HResult};
    use cadence_api::types::{ActivityType, TaskList};

    struct Domains;

    impl DomainCache for Domains {
        fn get_domain_by_id(&self, _id: &str) -> HResult<Arc<DomainCacheEntry>> {
            Ok(Arc::new(DomainCacheEntry::default()))
        }
        fn get_domain(&self, _name: &str) -> HResult<Arc<DomainCacheEntry>> {
            Ok(Arc::new(DomainCacheEntry::default()))
        }
        fn get_domain_name(&self, _id: &str) -> HResult<String> {
            Ok("test-domain".to_string())
        }
        fn unregister_domain_change_callback(&self, _shard_name: &str) {}
    }

    fn state() -> EngineMutableState {
        let domain = DomainCacheEntry {
            info: DomainInfo { id: "domain-id".into(), name: "test-domain".into(), status: 0 },
            ..Default::default()
        };
        let mut snapshot = pc::WorkflowSnapshot::default();
        snapshot.execution_info.domain_id = "domain-id".into();
        snapshot.execution_info.workflow_id = "wf".into();
        snapshot.execution_info.run_id = "run".into();
        snapshot.execution_info.task_list = "wf-tl".into();
        snapshot.execution_info.next_event_id = 5;
        let s = EngineMutableState::from_snapshot(snapshot, domain, 0, Arc::new(|| 1_700_000_000_000_000_000)).unwrap();
        s.inner.lock().unwrap().exec.workflow_timeout = 100;
        s
    }

    fn handler(state: &EngineMutableState) -> TaskHandler<EngineOps<'_>> {
        let ops = EngineOps {
            state,
            config: Arc::new(Config::new_for_test()),
            domains: Arc::new(Domains),
            domain_name: "test-domain".into(),
            identity: "id".into(),
            completed_id: 4,
            semaphore_lookups: Default::default(),
        };
        TaskHandler::new(ops, "id".into(), 4)
    }

    fn event_types(state: &EngineMutableState) -> Vec<EventType> {
        state.inner.lock().unwrap().hist.iter().filter_map(|e| e.event_type).collect()
    }

    #[test]
    fn history_limit_failure_uses_go_reason() {
        let s = state();
        let mut config = Config::new_for_test();
        config.history_count_limit_error = Arc::new(|_| 1);
        let ops = EngineOps {
            state: &s,
            config: Arc::new(config),
            domains: Arc::new(Domains),
            domain_name: "test-domain".into(),
            identity: "id".into(),
            completed_id: 4,
            semaphore_lookups: Default::default(),
        };
        let mut h = TaskHandler::new(ops, "id".into(), 4);
        let mut d = vec![];
        h.handle_decisions(b"", &mut d).unwrap();
        let g = s.inner.lock().unwrap();
        let reason = g
            .hist
            .iter()
            .filter_map(|e| e.workflow_execution_failed_event_attributes.as_ref())
            .filter_map(|a| a.reason.clone())
            .next();
        assert_eq!(reason.as_deref(), Some("HISTORY_EXCEEDS_LIMIT"));
    }

    #[test]
    fn cancel_timer_retracts_buffered_timer_fired() {
        let s = state();
        s.inner.lock().unwrap().buffered.push(HistoryEvent {
            event_type: Some(EventType::TimerFired),
            timer_fired_event_attributes: Some(TimerFiredEventAttributes { timer_id: "t1".into(), started_event_id: 7 }),
            ..Default::default()
        });
        let mut h = handler(&s);
        let mut d = vec![Decision {
            decision_type: Some(DecisionType::CancelTimer),
            cancel_timer_decision_attributes: Some(CancelTimerDecisionAttributes { timer_id: "t1".into() }),
            ..Default::default()
        }];
        h.handle_decisions(b"", &mut d).unwrap();
        assert!(!h.fail_decision, "{:?}", h.fail_message);
        assert_eq!(event_types(&s), vec![EventType::TimerCanceled]);
        let g = s.inner.lock().unwrap();
        assert!(g.buffered.is_empty(), "TimerFired removed from buffered events");
        assert_eq!(g.hist[0].timer_canceled_event_attributes.as_ref().unwrap().started_event_id, 7);
    }

    #[test]
    fn complete_workflow_closes_execution() {
        let s = state();
        let mut h = handler(&s);
        let mut d = vec![Decision {
            decision_type: Some(DecisionType::CompleteWorkflowExecution),
            complete_workflow_execution_decision_attributes: Some(CompleteWorkflowExecutionDecisionAttributes {
                result: b"ok".to_vec(),
            }),
            ..Default::default()
        }];
        h.handle_decisions(b"", &mut d).unwrap();
        assert!(!h.fail_decision);
        assert!(!s.is_workflow_execution_running());
        assert_eq!(event_types(&s), vec![EventType::WorkflowExecutionCompleted]);
        assert!(!s.inner.lock().unwrap().transfer_tasks.is_empty());
    }

    #[test]
    fn schedule_activity_adds_event_state_and_transfer_task() {
        let s = state();
        let mut h = handler(&s);
        let mut d = vec![Decision {
            decision_type: Some(DecisionType::ScheduleActivityTask),
            schedule_activity_task_decision_attributes: Some(ScheduleActivityTaskDecisionAttributes {
                activity_id: "a1".into(),
                activity_type: Some(ActivityType { name: "act".into() }),
                task_list: Some(TaskList { name: "tl".into(), ..Default::default() }),
                schedule_to_close_timeout_seconds: Some(10),
                schedule_to_start_timeout_seconds: Some(5),
                start_to_close_timeout_seconds: Some(5),
                ..Default::default()
            }),
            ..Default::default()
        }];
        h.handle_decisions(b"", &mut d).unwrap();
        assert!(!h.fail_decision, "{:?}", h.fail_message);
        assert_eq!(event_types(&s), vec![EventType::ActivityTaskScheduled]);
        let g = s.inner.lock().unwrap();
        assert_eq!(g.pc_acts.len(), 1);
        assert_eq!(g.pc_acts.values().next().unwrap().activity_id, "a1");
        assert_eq!(g.transfer_tasks.len(), 1);
    }

    #[test]
    fn start_timer_adds_event_and_pending_timer() {
        let s = state();
        let mut h = handler(&s);
        let mut d = vec![Decision {
            decision_type: Some(DecisionType::StartTimer),
            start_timer_decision_attributes: Some(StartTimerDecisionAttributes {
                timer_id: "t1".into(),
                start_to_fire_timeout_seconds: Some(5),
            }),
            ..Default::default()
        }];
        h.handle_decisions(b"", &mut d).unwrap();
        assert!(!h.fail_decision, "{:?}", h.fail_message);
        assert_eq!(event_types(&s), vec![EventType::TimerStarted]);
        let g = s.inner.lock().unwrap();
        assert!(g.timers.contains_key("t1"));
        assert!(g.upsert_timers.contains("t1"));
    }

    #[test]
    fn invalid_schedule_activity_attributes_fail_the_decision() {
        let s = state();
        let mut h = handler(&s);
        let mut d = vec![Decision {
            decision_type: Some(DecisionType::ScheduleActivityTask),
            schedule_activity_task_decision_attributes: Some(ScheduleActivityTaskDecisionAttributes::default()),
            ..Default::default()
        }];
        h.handle_decisions(b"", &mut d).unwrap();
        assert!(h.fail_decision);
        assert_eq!(h.fail_decision_cause, Some("BAD_SCHEDULE_ACTIVITY_ATTRIBUTES"));
    }

    fn schedule_activity_decision(request_local_dispatch: bool) -> Vec<Decision> {
        vec![Decision {
            decision_type: Some(DecisionType::ScheduleActivityTask),
            schedule_activity_task_decision_attributes: Some(ScheduleActivityTaskDecisionAttributes {
                activity_id: "a1".into(),
                activity_type: Some(ActivityType { name: "act".into() }),
                task_list: Some(TaskList { name: "tl".into(), ..Default::default() }),
                schedule_to_close_timeout_seconds: Some(10),
                schedule_to_start_timeout_seconds: Some(5),
                start_to_close_timeout_seconds: Some(5),
                request_local_dispatch,
                ..Default::default()
            }),
            ..Default::default()
        }]
    }

    #[test]
    fn too_many_pending_activities_fails_workflow_with_go_reason() {
        let s = state();
        let mut config = Config::new_for_test();
        config.pending_activity_validation_enabled = Arc::new(|| true);
        config.pending_activities_count_limit_error = Arc::new(|_| 0);
        let ops = EngineOps {
            state: &s,
            config: Arc::new(config),
            domains: Arc::new(Domains),
            domain_name: "test-domain".into(),
            identity: "id".into(),
            completed_id: 4,
            semaphore_lookups: Default::default(),
        };
        let mut h = TaskHandler::new(ops, "id".into(), 4);
        let mut d = schedule_activity_decision(false);
        h.handle_decisions(b"", &mut d).unwrap();
        assert!(!h.fail_decision);
        assert_eq!(event_types(&s), vec![EventType::WorkflowExecutionFailed]);
        assert_eq!(crate::decision::ERR_TOO_MANY_PENDING_ACTIVITIES, "Too many pending activities");
    }

    #[test]
    fn local_dispatch_starts_activity_and_skips_transfer_task() {
        let s = state();
        let mut h = handler(&s);
        let mut d = schedule_activity_decision(true);
        let dispatch = h.handle_decisions(b"", &mut d).unwrap();
        assert!(!h.fail_decision, "{:?}", h.fail_message);
        assert_eq!(dispatch.len(), 1);
        assert_eq!(dispatch[0].activity_id, "a1");
        assert!(!dispatch[0].task_token.is_empty());
        assert!(dispatch[0].scheduled_timestamp.is_some());
        assert!(dispatch[0].started_timestamp.is_some_and(|t| t > 0));
        assert_eq!(event_types(&s), vec![EventType::ActivityTaskScheduled, EventType::ActivityTaskStarted]);
        let g = s.inner.lock().unwrap();
        assert!(g.transfer_tasks.is_empty());
        assert_ne!(g.pc_acts.values().next().unwrap().started_id, EMPTY_EVENT_ID);
    }

    #[test]
    fn no_local_dispatch_without_request_flag() {
        let s = state();
        let mut h = handler(&s);
        let mut d = schedule_activity_decision(false);
        let dispatch = h.handle_decisions(b"", &mut d).unwrap();
        assert!(dispatch.is_empty());
        assert_eq!(event_types(&s), vec![EventType::ActivityTaskScheduled]);
        assert!(!s.inner.lock().unwrap().transfer_tasks.is_empty());
    }

    #[test]
    fn new_binary_checksum_generates_search_attr_task_only_with_advanced_visibility() {
        let s = state();
        let mut g = s.inner.lock().unwrap();
        g.add_binary_checksum_if_not_exists("bin-1", 4, 1, 10, false).unwrap();
        assert!(g.transfer_tasks.is_empty());
        g.add_binary_checksum_if_not_exists("bin-2", 5, 2, 10, true).unwrap();
        assert_eq!(g.transfer_tasks.len(), 1);
        // an already-recorded checksum returns early without another task
        g.add_binary_checksum_if_not_exists("bin-2", 6, 3, 10, true).unwrap();
        assert_eq!(g.transfer_tasks.len(), 1);
    }

    #[test]
    fn set_client_info_stores_versions_on_execution_info() {
        let s = state();
        s.set_client_info("1.2.3", "1.5.0", "uber-go");
        let g = s.inner.lock().unwrap();
        assert_eq!(g.exec.client_library_version, "1.2.3");
        assert_eq!(g.exec.client_feature_version, "1.5.0");
        assert_eq!(g.exec.client_impl, "uber-go");
    }

    #[test]
    fn consistent_query_support_matches_go_version_checker() {
        use super::super::engine_decision::supports_consistent_query as f;
        assert!(f("uber-go", "1.5.0"));
        assert!(f("uber-java", "v1.6"));
        assert!(!f("uber-go", "1.4.9"));
        assert!(!f("uber-go", ""));
        assert!(!f("unknown", "2.0.0"));
        assert!(!f("uber-go", "1.5.0-rc1"));
    }
}
