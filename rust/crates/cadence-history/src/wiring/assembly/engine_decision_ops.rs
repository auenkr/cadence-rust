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
use crate::engine::deps::{
    DomainCache, EMPTY_EVENT_ID, WORKFLOW_CLOSE_STATUS_CANCELED, WORKFLOW_CLOSE_STATUS_COMPLETED,
    WORKFLOW_CLOSE_STATUS_FAILED, WORKFLOW_STATE_COMPLETED,
};
use crate::wiring::assembly::engine_state::EngineMutableState;

const NANOS: i64 = 1_000_000_000;

/// Local activity dispatch is not used by the engine (activities are always
/// dispatched through matching), so `schedule_activity` never returns one.
pub struct NoDispatch;

impl LocalDispatch for NoDispatch {
    fn set_task_token(&mut self, _token: Vec<u8>) {}
    fn set_timestamps(&mut self, _scheduled: i64, _scheduled_this_attempt: i64, _started: i64) {}
}

fn bad(msg: impl Into<String>) -> OperationError {
    OperationError { kind: ErrorKind::BadRequest, message: msg.into() }
}

fn internal(msg: impl Into<String>) -> OperationError {
    OperationError { kind: ErrorKind::Internal, message: msg.into() }
}

fn not_ported(what: &str) -> OperationError {
    internal(format!("{what} decision is not ported on the engine mutable state yet"))
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
}

impl EngineOps<'_> {
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
        let ev = g.push_event(ty, ms, fill);
        g.exec.state = WORKFLOW_STATE_COMPLETED;
        g.exec.close_status = status;
        g.pc_exec.state = WORKFLOW_STATE_COMPLETED;
        g.pc_exec.close_status = status;
        g.pc_exec.completion_event_batch_id = completed_id;
        g.clear_stickyness();
        g.gen_workflow_close_tasks(&params, ms * 1_000_000, ev.version);
        Ok(())
    }
}

impl Operations for EngineOps<'_> {
    type Dispatch = NoDispatch;

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
            let completed = self.state.inner.lock().unwrap().next_event_id - 1;
            self.fail_workflow(completed, "FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT", b"Workflow history size / count exceeds limit.")?;
            return Ok(true);
        }
        Ok(false)
    }

    fn check_blob_size(&mut self, _kind: DecisionType, blob: &[u8], message: &str) -> Result<bool> {
        if blob.len() as i64 > (self.config.blob_size_limit_error)(&self.domain_name) {
            let completed = self.state.inner.lock().unwrap().next_event_id - 1;
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
        let ctx = Validation::new(self);
        let name = self.domain_name.clone();
        checker::validate(&ctx, decision, &name).map_err(Into::into)
    }

    fn resolve_target_domain(&mut self, name: &str) -> Result<()> {
        // Same-domain calls only; cross-domain resolution goes through the
        // domain cache and then the cross-cluster validation in `validate`.
        self.domains.get_domain(name).map(|_| ()).map_err(|e| internal(format!("{e:?}")))
    }

    fn schedule_activity(&mut self, completed_id: i64, decision: &Decision) -> Result<(Activity, Option<NoDispatch>)> {
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
            return Err(internal("Too many pending activities."));
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
        let ev = g.push_event(EventType::ActivityTaskScheduled, ms, |e| {
            e.activity_task_scheduled_event_attributes = Some(ev_attrs);
        });
        let sid = ev.event_id;
        let mut info = pc::ActivityInfo {
            version: ev.version,
            scheduled_event_batch_id: completed_id,
            scheduled_time_unix_nanos: now_nanos,
            started_id: EMPTY_EVENT_ID,
            activity_id: a.activity_id.clone(),
            schedule_to_start_timeout_seconds: a.schedule_to_start_timeout_seconds.unwrap_or(0),
            schedule_to_close_timeout_seconds: a.schedule_to_close_timeout_seconds.unwrap_or(0),
            start_to_close_timeout_seconds: a.start_to_close_timeout_seconds.unwrap_or(0),
            heartbeat_timeout_seconds: a.heartbeat_timeout_seconds.unwrap_or(0),
            cancel_request_id: EMPTY_EVENT_ID,
            task_list: a.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
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
        g.gen_activity_transfer_task(sid);
        let activity = Activity {
            schedule_id: sid,
            started_id: EMPTY_EVENT_ID,
            cancel_event_id: EMPTY_EVENT_ID,
            activity_id: a.activity_id,
            scheduled_time_unix_nanos: now_nanos,
            started_time_unix_nanos: 0,
        };
        Ok((activity, None))
    }

    fn start_local_activity(&mut self, _activity: &mut Activity, _request_id: &str, _identity: &str) -> Result<()> {
        Err(not_ported("local ScheduleActivityTask dispatch"))
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
        let ev = g.push_event(EventType::ActivityTaskCancelRequested, ms, |e| {
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
        g.push_event(EventType::RequestCancelActivityTaskFailed, ms, |e| {
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
        let ev = g.push_event(EventType::TimerStarted, ms, |e| {
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
        let Some(info) = g.timers.get(&a.timer_id).cloned() else {
            return Err(bad(format!("unable to find timer: {}", a.timer_id)));
        };
        let timer_id = a.timer_id.clone();
        g.push_event(EventType::TimerCanceled, ms, |e| {
            e.timer_canceled_event_attributes = Some(TimerCanceledEventAttributes {
                timer_id,
                started_event_id: info.started_id,
                decision_task_completed_event_id: completed_id,
                identity,
            });
        });
        g.timers.remove(&a.timer_id);
        g.upsert_timers.remove(&a.timer_id);
        g.delete_timers.insert(a.timer_id);
        g.dirty = true;
        Ok(())
    }

    fn cancel_timer_failed(&mut self, completed_id: i64, decision: &Decision, identity: &str) -> Result<()> {
        let timer_id = decision.cancel_timer_decision_attributes.as_ref().map(|a| a.timer_id.clone()).unwrap_or_default();
        let ms = self.state.now_millis();
        let identity = identity.to_string();
        let mut g = self.state.inner.lock().unwrap();
        g.push_event(EventType::CancelTimerFailed, ms, |e| {
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
        _completed_id: i64,
        _seconds: i32,
        _retry: bool,
        _reason: Option<&str>,
        _details: &[u8],
        _completion_result: Option<&[u8]>,
    ) -> Result<()> {
        Err(not_ported("retry/cron ContinueAsNew"))
    }

    fn start_last_completion_result(&mut self) -> Result<Vec<u8>> {
        Err(not_ported("cron last completion result"))
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

    fn continue_as_new(&mut self, _completed_id: i64, _decision: &Decision, _parent_domain_name: &str) -> Result<()> {
        Err(not_ported("ContinueAsNewWorkflowExecution"))
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
        g.push_event(EventType::MarkerRecorded, ms, |e| {
            e.marker_recorded_event_attributes = Some(MarkerRecordedEventAttributes {
                marker_name: a.marker_name,
                details: a.details,
                decision_task_completed_event_id: completed_id,
                header: a.header,
            });
        });
        Ok(())
    }

    fn initiate_cancel_external(&mut self, _completed_id: i64, _request_id: &str, _decision: &Decision) -> Result<()> {
        Err(not_ported("RequestCancelExternalWorkflowExecution"))
    }

    fn initiate_signal_external(&mut self, _completed_id: i64, _request_id: &str, _decision: &Decision) -> Result<()> {
        Err(not_ported("SignalExternalWorkflowExecution"))
    }

    fn initiate_child(&mut self, _completed_id: i64, _request_id: &str, _decision: &Decision) -> Result<()> {
        Err(not_ported("StartChildWorkflowExecution"))
    }

    fn parent_close_policy_enabled(&self) -> bool {
        (self.config.enable_parent_close_policy)(&self.domain_name)
    }

    fn upsert_search_attributes(&mut self, _completed_id: i64, _decision: &Decision) -> Result<()> {
        Err(not_ported("UpsertWorkflowSearchAttributes"))
    }

    fn domain_name(&self) -> &str {
        &self.domain_name
    }

    fn semaphore_enabled(&self) -> bool {
        (self.config.enable_distributed_semaphore)(&self.domain_name)
    }

    fn semaphore_exists(&mut self, _name: &str) -> Result<()> {
        Err(not_ported("AcquireSemaphore"))
    }

    fn semaphore_wait_timeout_seconds(&self, supplied: Option<i32>) -> i32 {
        supplied.unwrap_or_else(|| secs((self.config.semaphore_acquire_default_wait_timeout)(&self.domain_name)))
    }

    fn initiate_semaphore(&mut self, _completed_id: i64, _name: &str, _wait_seconds: i32) -> Result<()> {
        Err(not_ported("AcquireSemaphore"))
    }
}

/// Snapshot of what the decision checker needs (Go `attrValidator`).
struct Validation {
    domain_id: String,
    domain_name: String,
    config: Arc<Config>,
    domains: Arc<dyn DomainCache>,
    defaults: ParentDecisionDefaults,
}

impl Validation {
    fn new(ops: &EngineOps<'_>) -> Self {
        let g = ops.state.inner.lock().unwrap();
        Self {
            domain_id: g.exec.domain_id.clone(),
            domain_name: ops.domain_name.clone(),
            config: ops.config.clone(),
            domains: ops.domains.clone(),
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
        Err(DecisionError::internal("cross-domain decision validation is not ported on the engine mutable state yet"))
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
        &self.domain_id
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

    fn search_policy(&self, _domain: &str) -> std::result::Result<SearchAttributePolicy, DecisionError> {
        Err(DecisionError::internal("search attribute validation is not ported on the engine mutable state yet"))
    }

    fn validate_cron(&self, schedule: &str) -> std::result::Result<(), DecisionError> {
        if schedule.is_empty() {
            return Ok(());
        }
        Err(DecisionError::internal("cron schedule validation is not ported on the engine mutable state yet"))
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
        EngineMutableState::from_snapshot(snapshot, domain, 0, Arc::new(|| 1_700_000_000_000_000_000)).unwrap()
    }

    fn handler(state: &EngineMutableState) -> TaskHandler<EngineOps<'_>> {
        let ops = EngineOps {
            state,
            config: Arc::new(Config::new_for_test()),
            domains: Arc::new(Domains),
            domain_name: "test-domain".into(),
            identity: "id".into(),
        };
        TaskHandler::new(ops, "id".into(), 4)
    }

    fn event_types(state: &EngineMutableState) -> Vec<EventType> {
        state.inner.lock().unwrap().hist.iter().filter_map(|e| e.event_type).collect()
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
}
