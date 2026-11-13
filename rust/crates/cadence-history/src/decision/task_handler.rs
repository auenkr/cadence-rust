//! Go task_handler.go control flow over primitive mutable-state operations.
//! TODO(link): bind Operations to execution::MutableState after that package lands.
use super::{DecisionError, DecisionErrorKind, AcquireSemaphoreDecisionAttributes};
use cadence_api::{enums::{DecisionType, ParentClosePolicy}, tasks::Decision};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind { BadRequest, Internal, NotFound, EventAlreadyStarted, Other }
#[derive(Debug, Clone)]
pub struct OperationError { pub kind: ErrorKind, pub message: String }
pub type Result<T> = std::result::Result<T, OperationError>;
impl From<DecisionError> for OperationError {
    fn from(e: DecisionError) -> Self {
        let kind = match e.kind {
            DecisionErrorKind::BadRequest => ErrorKind::BadRequest,
            DecisionErrorKind::Internal => ErrorKind::Internal,
            DecisionErrorKind::EntityNotExists => ErrorKind::NotFound,
            DecisionErrorKind::EventAlreadyStarted => ErrorKind::EventAlreadyStarted,
            DecisionErrorKind::AlreadyCompleted | DecisionErrorKind::Other => ErrorKind::Other,
        };
        Self { kind, message: e.message }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Activity { pub schedule_id: i64, pub started_id: i64, pub cancel_event_id: i64, pub activity_id: String, pub scheduled_time_unix_nanos: i64, pub started_time_unix_nanos: i64 }
#[derive(Debug,Clone,Default)]
pub struct WorkflowIdentity { pub domain_id:String,pub workflow_id:String,pub workflow_type:String,pub run_id:String }
pub trait LocalDispatch { fn set_task_token(&mut self, token:Vec<u8>); fn set_timestamps(&mut self, scheduled:i64, scheduled_this_attempt:i64, started:i64); }

/// All methods represent one dependency call, rather than a whole decision handler.
pub trait Operations {
    type Dispatch: LocalDispatch;
    fn workflow_identity(&self) -> WorkflowIdentity;
    fn count_decision(&mut self, kind: DecisionType);
    fn has_buffered_events(&self) -> bool;
    fn workflow_running(&self) -> bool;
    fn cancel_requested(&self) -> bool;
    fn multiple_completion(&mut self, kind: DecisionType);
    fn check_workflow_size(&mut self) -> Result<bool>;
    fn check_blob_size(&mut self, kind: DecisionType, blob: &[u8], message: &str) -> Result<bool>;
    fn set_execution_context(&mut self, context: &[u8]);
    fn validate(&mut self, decision: &mut Decision) -> Result<()>;
    fn resolve_target_domain(&mut self, name: &str) -> Result<()>;
    fn schedule_activity(&mut self, completed_id: i64, decision: &Decision) -> Result<(Activity, Option<Self::Dispatch>)>;
    fn start_local_activity(&mut self, activity: &mut Activity, request_id: &str, identity: &str) -> Result<()>;
    fn populate_dispatch_token_and_timestamps(&mut self, activity: &Activity, decision: &Decision, dispatch: &mut Self::Dispatch) -> Result<()> {
        let workflow = self.workflow_identity();
        let activity_type = decision.schedule_activity_task_decision_attributes.as_ref().and_then(|a|a.activity_type.as_ref()).map_or("",|a|a.name.as_str());
        let token = cadence_api::tasktoken::TaskToken { domain_id:workflow.domain_id,workflow_id:workflow.workflow_id,workflow_type:workflow.workflow_type,run_id:workflow.run_id,schedule_id:activity.schedule_id,schedule_attempt:0,activity_id:activity.activity_id.clone(),activity_type:activity_type.into() };
        dispatch.set_task_token(cadence_api::tasktoken::JsonTaskTokenSerializer::serialize(&token).map_err(|_|OperationError{kind:ErrorKind::BadRequest,message:"error serializing task token".into()})?);
        dispatch.set_timestamps(activity.scheduled_time_unix_nanos,activity.scheduled_time_unix_nanos,activity.started_time_unix_nanos);
        Ok(())
    }
    fn request_cancel_activity(&mut self, completed_id: i64, id: &str, identity: &str) -> Result<Activity>;
    fn cancel_activity(&mut self, activity: &Activity, details: &[u8], identity: &str) -> Result<()>;
    fn request_cancel_activity_failed(&mut self, completed_id: i64, id: &str, cause: &str) -> Result<()>;
    fn start_timer(&mut self, completed_id: i64, decision: &Decision) -> Result<()>;
    fn cancel_timer(&mut self, completed_id: i64, decision: &Decision, identity: &str) -> Result<()>;
    fn cancel_timer_failed(&mut self, completed_id: i64, decision: &Decision, identity: &str) -> Result<()>;
    fn complete_workflow(&mut self, completed_id: i64, decision: &Decision) -> Result<()>;
    fn fail_workflow(&mut self, completed_id: i64, reason: &str, details: &[u8]) -> Result<()>;
    fn cancel_workflow(&mut self, completed_id: i64, details: &[u8]) -> Result<()>;
    fn cron_backoff_seconds(&mut self) -> Result<Option<i32>>;
    fn retry_backoff_seconds(&self, reason: &str) -> Option<i32>;
    /// Go retryCronContinueAsNew: the implementation loads the start event, builds the attributes
    /// with `retry_cron_continue_as_new_attributes` and passes `attr.GetParentWorkflowDomain()`.
    fn retry_cron_continue_as_new(&mut self, completed_id: i64, seconds: i32, retry: bool, reason: Option<&str>, details: &[u8], completion_result: Option<&[u8]>) -> Result<()>;
    /// Go `GetStartEvent(ctx)` then `startAttributes.LastCompletionResult` (cron-after-failure path).
    fn start_last_completion_result(&mut self) -> Result<Vec<u8>>;
    /// Go: empty unless `HasParentExecution()`, then `domainCache.GetDomainName(executionInfo.ParentDomainID)`.
    fn parent_domain_name(&mut self) -> Result<String>;
    /// Go `domainCache.GetDomainName(executionInfo.DomainID)` used by upsert-search-attributes.
    fn execution_domain_name(&mut self) -> Result<String>;
    fn continue_as_new(&mut self, completed_id: i64, decision: &Decision, parent_domain_name: &str) -> Result<()>;
    fn max_semaphore_name_length(&self) -> usize;
    fn record_marker(&mut self, completed_id: i64, decision: &Decision) -> Result<()>;
    fn initiate_cancel_external(&mut self, completed_id: i64, request_id: &str, decision: &Decision) -> Result<()>;
    fn initiate_signal_external(&mut self, completed_id: i64, request_id: &str, decision: &Decision) -> Result<()>;
    fn initiate_child(&mut self, completed_id: i64, request_id: &str, decision: &Decision) -> Result<()>;
    fn parent_close_policy_enabled(&self) -> bool;
    fn upsert_search_attributes(&mut self, completed_id: i64, decision: &Decision) -> Result<()>;
    fn domain_name(&self) -> &str;
    fn semaphore_enabled(&self) -> bool;
    fn semaphore_exists(&mut self, name: &str) -> Result<()>;
    fn semaphore_wait_timeout_seconds(&self, supplied: Option<i32>) -> i32;
    fn initiate_semaphore(&mut self, completed_id: i64, name: &str, wait_seconds: i32) -> Result<()>;
}
pub struct TaskHandler<O: Operations> {
    pub operations: O,
    pub identity: String,
    pub completed_id: i64,
    pub has_unhandled_events_before_decisions: bool,
    pub fail_decision: bool,
    pub fail_decision_cause: Option<&'static str>,
    pub fail_message: Option<String>,
    pub activity_not_started_cancelled: bool,
    pub stop_processing: bool,
}
impl<O: Operations> TaskHandler<O> {
    pub fn new(operations: O, identity: String, completed_id: i64) -> Self {
        let buffered = operations.has_buffered_events();
        Self { operations, identity, completed_id, has_unhandled_events_before_decisions: buffered, fail_decision: false, fail_decision_cause: None, fail_message: None, activity_not_started_cancelled: false, stop_processing: false }
    }
    pub fn handler_fail_decision(&mut self, cause: &'static str, message: impl Into<String>) {
        self.fail_decision = true; self.fail_decision_cause = Some(cause); self.fail_message = Some(message.into()); self.stop_processing = true;
    }
    pub fn handle_decisions(&mut self, context: &[u8], decisions: &mut [Decision]) -> Result<Vec<O::Dispatch>> {
        if self.operations.check_workflow_size()? { return Ok(Vec::new()); }
        let mut results = Vec::new();
        for decision in decisions {
            let result = self.handle_decision_with_result(decision)?;
            if self.stop_processing { return Ok(Vec::new()); }
            if let Some(result) = result { results.push(result); }
        }
        self.operations.set_execution_context(context);
        Ok(results)
    }
    fn validate_decision_attr(&mut self, decision: &mut Decision, cause: &'static str) -> Result<bool> {
        match self.operations.validate(decision) {
            Ok(()) => Ok(true),
            Err(e) if e.kind == ErrorKind::BadRequest => { self.handler_fail_decision(cause, e.message); Ok(false) },
            Err(e) => Err(e),
        }
    }
    fn blob(&mut self, kind: DecisionType, bytes: &[u8], message: &str) -> Result<bool> {
        match self.operations.check_blob_size(kind, bytes, message) {
            Ok(false) => Ok(true),
            Ok(true) => { self.stop_processing = true; Ok(false) },
            Err(e) => { self.stop_processing = true; Err(e) },
        }
    }
    pub fn handle_fail_workflow_error(&mut self, reason: &str, details: &str) -> Result<()> {
        self.stop_processing = true;
        self.operations.fail_workflow(self.completed_id, reason, details.as_bytes())
    }
    pub fn handle_decision_with_result(&mut self, decision: &mut Decision) -> Result<Option<O::Dispatch>> {
        use DecisionType as D;
        let kind = decision.decision_type.unwrap_or(D::ScheduleActivityTask);
        if kind == D::AcquireSemaphore {
            let max_name = self.operations.max_semaphore_name_length();
            self.handle_decision_acquire_semaphore(decision.acquire_semaphore_decision_attributes.as_ref(), max_name)?;
            return Ok(None);
        }
        self.operations.count_decision(kind);
        let (domain, error_prefix) = match kind {
            D::ScheduleActivityTask => (decision.schedule_activity_task_decision_attributes.as_ref().map(|a| a.domain.as_str()), "Unable to schedule activity across domain "),
            D::RequestCancelExternalWorkflowExecution => (decision.request_cancel_external_workflow_execution_decision_attributes.as_ref().map(|a| a.domain.as_str()), "Unable to cancel workflow across domain: "),
            D::SignalExternalWorkflowExecution => (decision.signal_external_workflow_execution_decision_attributes.as_ref().map(|a| a.domain.as_str()), "Unable to signal workflow across domain: "),
            D::StartChildWorkflowExecution => (decision.start_child_workflow_execution_decision_attributes.as_ref().map(|a| a.domain.as_str()), "Unable to schedule child execution across domain "),
            _ => (None, ""),
        };
        if let Some(name) = domain.filter(|name| !name.is_empty()) {
            if self.operations.resolve_target_domain(name).is_err() { return Err(OperationError { kind: ErrorKind::Internal, message: format!("{error_prefix}{name}.") }); }
        }
        let closes = matches!(kind, D::CompleteWorkflowExecution | D::FailWorkflowExecution | D::ContinueAsNewWorkflowExecution | D::CancelWorkflowExecution);
        if closes && self.has_unhandled_events_before_decisions {
            self.handler_fail_decision("UNHANDLED_DECISION", if kind == D::CancelWorkflowExecution { "cannot process cancellation, new pending decisions were scheduled while this decision was processing" } else { "cannot complete workflow, new pending decisions were scheduled while this decision was processing" });
            return Ok(None);
        }
        let cause = match kind {
            D::ScheduleActivityTask => "BAD_SCHEDULE_ACTIVITY_ATTRIBUTES",
            D::StartTimer => "BAD_START_TIMER_ATTRIBUTES",
            D::RequestCancelActivityTask => "BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES",
            D::CancelTimer => "BAD_CANCEL_TIMER_ATTRIBUTES",
            D::CompleteWorkflowExecution => "BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES",
            D::FailWorkflowExecution => "BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES",
            D::CancelWorkflowExecution => "BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES",
            D::RequestCancelExternalWorkflowExecution => "BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES",
            D::SignalExternalWorkflowExecution => "BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES",
            D::RecordMarker => "BAD_RECORD_MARKER_ATTRIBUTES",
            D::ContinueAsNewWorkflowExecution => "BAD_CONTINUE_AS_NEW_ATTRIBUTES",
            D::StartChildWorkflowExecution => "BAD_START_CHILD_EXECUTION_ATTRIBUTES",
            D::UpsertWorkflowSearchAttributes => "BAD_SEARCH_ATTRIBUTES",
            _ => return Err(OperationError { kind: ErrorKind::BadRequest, message: format!("Unknown decision type: {kind}") }),
        };
        if kind == D::UpsertWorkflowSearchAttributes && self.operations.execution_domain_name().is_err() {
            return Err(OperationError { kind: ErrorKind::Internal, message: format!("Unable to get domain for domainID: {}.", self.operations.workflow_identity().domain_id) });
        }
        if !self.validate_decision_attr(decision, cause)? { return Ok(None); }
        let blob = match kind {
            D::ScheduleActivityTask => decision.schedule_activity_task_decision_attributes.as_ref().map(|a| (a.input.clone(), "ScheduleActivityTaskDecisionAttributes.Input exceeds size limit.")),
            D::CompleteWorkflowExecution => decision.complete_workflow_execution_decision_attributes.as_ref().map(|a| (a.result.clone(), "CompleteWorkflowExecutionDecisionAttributes.Result exceeds size limit.")),
            D::FailWorkflowExecution => decision.fail_workflow_execution_decision_attributes.as_ref().map(|a| (a.details.clone(), "FailWorkflowExecutionDecisionAttributes.Details exceeds size limit.")),
            D::RecordMarker => decision.record_marker_decision_attributes.as_ref().map(|a| (a.details.clone(), "RecordMarkerDecisionAttributes.Details exceeds size limit.")),
            D::ContinueAsNewWorkflowExecution => decision.continue_as_new_workflow_execution_decision_attributes.as_ref().map(|a| (a.input.clone(), "ContinueAsNewWorkflowExecutionDecisionAttributes. Input exceeds size limit.")),
            D::StartChildWorkflowExecution => decision.start_child_workflow_execution_decision_attributes.as_ref().map(|a| (a.input.clone(), "StartChildWorkflowExecutionDecisionAttributes.Input exceeds size limit.")),
            D::SignalExternalWorkflowExecution => decision.signal_external_workflow_execution_decision_attributes.as_ref().map(|a| (a.input.clone(), "SignalExternalWorkflowExecutionDecisionAttributes.Input exceeds size limit.")),
            D::UpsertWorkflowSearchAttributes => decision.upsert_workflow_search_attributes_decision_attributes.as_ref().map(|a| (a.search_attributes.as_ref().and_then(|s| s.indexed_fields.as_ref()).map(|fields| fields.iter().flat_map(|(k,v)| k.as_bytes().iter().chain(v).copied()).collect()).unwrap_or_default(), "UpsertWorkflowSearchAttributesDecisionAttributes exceeds size limit.")),
            _ => None,
        };
        if let Some((bytes, message)) = blob { if !self.blob(kind, &bytes, message)? { return Ok(None); } }
        if closes && !self.operations.workflow_running() { self.operations.multiple_completion(kind); return Ok(None); }
        match kind {
            D::ScheduleActivityTask => match self.operations.schedule_activity(self.completed_id, decision) {
                Ok((mut activity, Some(mut dispatch))) => {
                    self.operations.start_local_activity(&mut activity, &uuid::Uuid::new_v4().to_string(), &self.identity)?;
                    self.operations.populate_dispatch_token_and_timestamps(&activity, decision, &mut dispatch).map_err(|_| OperationError { kind: ErrorKind::BadRequest, message: "error serializing task token".into() })?;
                    return Ok(Some(dispatch));
                },
                Ok(_) => {},
                Err(e) if e.kind == ErrorKind::BadRequest => self.handler_fail_decision("SCHEDULE_ACTIVITY_DUPLICATE_ID", ""),
                Err(e) if e.kind == ErrorKind::Internal && e.message == super::ERR_TOO_MANY_PENDING_ACTIVITIES => self.handle_fail_workflow_error("PENDING_ACTIVITY_EXCEEDS_LIMIT", &e.message)?,
                Err(e) => return Err(e),
            },
            D::RequestCancelActivityTask => {
                let id = &decision.request_cancel_activity_task_decision_attributes.as_ref().unwrap().activity_id;
                match self.operations.request_cancel_activity(self.completed_id, id, &self.identity) {
                    Ok(activity) if activity.started_id == -23 => { self.operations.cancel_activity(&activity, super::ACTIVITY_CANCELLATION_MSG_ACTIVITY_NOT_STARTED.as_bytes(), &self.identity)?; self.activity_not_started_cancelled = true; },
                    Ok(_) => {},
                    Err(e) if e.kind == ErrorKind::BadRequest => self.operations.request_cancel_activity_failed(self.completed_id, id, super::ACTIVITY_CANCELLATION_MSG_ACTIVITY_ID_UNKNOWN)?,
                    Err(e) => return Err(e),
                }
            },
            D::StartTimer => match self.operations.start_timer(self.completed_id, decision) {
                Ok(()) => {}, Err(e) if e.kind == ErrorKind::BadRequest => self.handler_fail_decision("START_TIMER_DUPLICATE_ID", ""), Err(e) => return Err(e),
            },
            D::CancelTimer => match self.operations.cancel_timer(self.completed_id, decision, &self.identity) {
                Ok(()) => self.has_unhandled_events_before_decisions = self.operations.has_buffered_events(),
                Err(e) if e.kind == ErrorKind::BadRequest => self.operations.cancel_timer_failed(self.completed_id, decision, &self.identity)?, Err(e) => return Err(e),
            },
            D::CompleteWorkflowExecution => {
                let result = &decision.complete_workflow_execution_decision_attributes.as_ref().unwrap().result;
                let backoff = self.cron_backoff()?;
                match (self.operations.cancel_requested(), backoff) {
                    (false, Some(seconds)) => self.operations.retry_cron_continue_as_new(self.completed_id, seconds, false, None, &[], Some(result))?,
                    _ => self.operations.complete_workflow(self.completed_id, decision).map_err(|_| OperationError { kind: ErrorKind::Internal, message: "Unable to add complete workflow event.".into() })?,
                }
            },
            D::FailWorkflowExecution => { let a = decision.fail_workflow_execution_decision_attributes.as_ref().unwrap(); self.handle_workflow_failure(a.reason.as_deref(), &a.details)?; },
            D::CancelWorkflowExecution => self.operations.cancel_workflow(self.completed_id, &decision.cancel_workflow_execution_decision_attributes.as_ref().unwrap().details)?,
            D::ContinueAsNewWorkflowExecution => { if self.operations.cancel_requested() { self.operations.cancel_workflow(self.completed_id, &[])?; } else { let parent_domain_name = self.operations.parent_domain_name()?; self.operations.continue_as_new(self.completed_id, decision, &parent_domain_name)?; } },
            D::RecordMarker => self.operations.record_marker(self.completed_id, decision)?,
            D::RequestCancelExternalWorkflowExecution => self.operations.initiate_cancel_external(self.completed_id, &uuid::Uuid::new_v4().to_string(), decision)?,
            D::SignalExternalWorkflowExecution => self.operations.initiate_signal_external(self.completed_id, &uuid::Uuid::new_v4().to_string(), decision)?,
            D::StartChildWorkflowExecution => {
                let enabled = self.operations.parent_close_policy_enabled();
                let a = decision.start_child_workflow_execution_decision_attributes.as_mut().unwrap();
                if !enabled { a.parent_close_policy = Some(ParentClosePolicy::Abandon); } else if a.parent_close_policy.is_none() { a.parent_close_policy = Some(ParentClosePolicy::Terminate); }
                self.operations.initiate_child(self.completed_id, &uuid::Uuid::new_v4().to_string(), decision)?;
            },
            D::UpsertWorkflowSearchAttributes => self.operations.upsert_search_attributes(self.completed_id, decision)?,
            _ => unreachable!(),
        }
        Ok(None)
    }
    fn cron_backoff(&mut self) -> Result<Option<i32>> { self.operations.cron_backoff_seconds().inspect_err(|_| self.stop_processing = true) }
    pub fn handle_workflow_failure(&mut self, reason: Option<&str>, details: &[u8]) -> Result<()> {
        if !self.operations.workflow_running() { self.operations.multiple_completion(DecisionType::FailWorkflowExecution); return Ok(()); }
        if self.operations.cancel_requested() { return self.operations.cancel_workflow(self.completed_id, details); }
        let retry = self.operations.retry_backoff_seconds(reason.unwrap_or_default());
        let (backoff, is_retry) = if retry.is_some() { (retry, true) } else { (self.cron_backoff()?, false) };
        match backoff {
            None => self.operations.fail_workflow(self.completed_id, reason.unwrap_or_default(), details),
            Some(seconds) => {
                let last_completion_result = self.operations.start_last_completion_result()?;
                self.operations.retry_cron_continue_as_new(self.completed_id, seconds, is_retry, reason, details, Some(&last_completion_result))
            },
        }
    }
    pub fn handle_decision_acquire_semaphore(&mut self, attr: Option<&AcquireSemaphoreDecisionAttributes>, max_name: usize) -> Result<()> {
        self.operations.count_decision(DecisionType::AcquireSemaphore);
        if !self.operations.semaphore_enabled() { return Err(OperationError { kind: ErrorKind::BadRequest, message: format!("Semaphores are not enabled for domain {:?}. Set dynamic config system.enableDistributedSemaphore=true for this domain to enable them.", self.operations.domain_name()) }); }
        if let Err(e) = super::validate_acquire_semaphore_attributes(attr, max_name) { self.handler_fail_decision("BAD_ACQUIRE_SEMAPHORE_ATTRIBUTES", e.message); return Ok(()); }
        let attr = attr.unwrap();
        match self.operations.semaphore_exists(&attr.semaphore_name) {
            Err(e) if e.kind == ErrorKind::NotFound => {
                if self.has_unhandled_events_before_decisions { self.handler_fail_decision("UNHANDLED_DECISION", "cannot fail workflow for a missing semaphore, new events arrived while this decision was processing"); return Ok(()); }
                self.stop_processing = true;
                return self.handle_workflow_failure(Some(super::FAILURE_REASON_SEMAPHORE_NOT_FOUND), super::fail_workflow_semaphore_not_found_details(&attr.semaphore_name, self.operations.domain_name()).as_bytes());
            },
            Err(e) => return Err(e), Ok(()) => {},
        }
        let wait = self.operations.semaphore_wait_timeout_seconds(attr.wait_timeout_seconds);
        match self.operations.initiate_semaphore(self.completed_id, &attr.semaphore_name, wait) {
            Err(e) if e.kind == ErrorKind::BadRequest => { self.handler_fail_decision("BAD_ACQUIRE_SEMAPHORE_ATTRIBUTES", format!("Run already holds or is waiting for semaphore {:?}.", attr.semaphore_name)); Ok(()) },
            result => result,
        }
    }
}

/// Go retryCronContinueAsNew's attribute propagation. The parent domain accompanies
/// the AddContinueAsNewEvent call rather than being a decision attribute.
pub fn retry_cron_continue_as_new_attributes(
    start: &cadence_api::service_types::WorkflowExecutionStartedEventAttributes,
    seconds: i32,
    initiator: cadence_api::enums::ContinueAsNewInitiator,
    reason: Option<String>,
    details: Vec<u8>,
    completion_result: Vec<u8>,
    active_cluster_selection_policy: Option<cadence_api::types::ActiveClusterSelectionPolicy>,
) -> cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes {
    cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes {
        workflow_type: start.workflow_type.clone(), task_list: start.task_list.clone(),
        retry_policy: start.retry_policy.clone(), input: start.input.clone(),
        execution_start_to_close_timeout_seconds: start.execution_start_to_close_timeout_seconds,
        task_start_to_close_timeout_seconds: start.task_start_to_close_timeout_seconds,
        cron_schedule: start.cron_schedule.clone(), backoff_start_interval_in_seconds: Some(seconds),
        initiator: Some(initiator), failure_reason: reason, failure_details: details,
        last_completion_result: completion_result, header: start.header.clone(), memo: start.memo.clone(),
        search_attributes: start.search_attributes.clone(), jitter_start_seconds: start.jitter_start_seconds,
        cron_overlap_policy: start.cron_overlap_policy,
        active_cluster_selection_policy,
    }
}
pub fn acquire_wait_timeout_seconds(supplied: Option<i32>, configured: std::time::Duration, built_in: std::time::Duration, mut warn: impl FnMut()) -> i32 {
    if let Some(seconds) = supplied.filter(|s| *s > 0) { return seconds; }
    let timeout = if configured < std::time::Duration::from_secs(1) { warn(); built_in } else { configured };
    timeout.as_secs().min(i32::MAX as u64) as i32
}
