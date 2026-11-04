//! Domain-aware validation from checker.go, with configuration/cache dependency seams.
use super::*;
use super::task_handler::PortDecision;
use cadence_api::enums::DecisionType;
/// TODO(link): implement using history Config and DomainCache. Each callback is one dependency operation.
pub trait ValidationContext: DomainReplicationLookup {
    fn source_domain_id(&self) -> &str;
    fn target_domain_id(&self) -> &str;
    fn defaults(&self) -> ParentDecisionDefaults;
    fn max_length(&self, field: &str, domain: &str) -> usize;
    fn max_schedule_to_start_retry_seconds(&self) -> i32;
    fn reserved_task_list_prefix(&self) -> &str;
    fn search_policy(&self, domain: &str) -> Result<SearchAttributePolicy, DecisionError>;
    fn validate_cron(&self, schedule: &str) -> Result<(), DecisionError>;
    fn id_length_observation(&self, field: &str, domain: &str, value: &str);
}
fn length(context: &impl ValidationContext, field: &str, domain: &str, value: &str, message: &str) -> Result<(), DecisionError> {
    context.id_length_observation(field, domain, value);
    if value.len() > context.max_length(field, domain) { Err(DecisionError::new(message)) } else { Ok(()) }
}
pub fn validate(context: &impl ValidationContext, decision: &mut PortDecision, domain_name: &str) -> Result<(), DecisionError> {
    use DecisionType as D;
    let defaults = context.defaults();
    match decision.decision_type.unwrap_or(D::ScheduleActivityTask) {
        D::ScheduleActivityTask => {
            validate_cross_domain_call(context, context.source_domain_id(), context.target_domain_id())?;
            let a = decision.schedule_activity_task_decision_attributes.as_mut().ok_or_else(|| DecisionError::new("ScheduleActivityTaskDecisionAttributes is not set on decision."))?;
            // Go normalizes the task list before checking activity ID/type/retry policy.
            let (name, kind) = a.task_list.as_ref().map_or(("", None), |t| (t.name.as_str(), t.kind));
            let (name, kind) = validated_task_list(name, kind, &defaults.task_list_name, defaults.task_list_kind, context.max_length("TaskList", &a.domain), context.reserved_task_list_prefix())?;
            a.task_list = Some(cadence_api::types::TaskList { name, kind, base_name: String::new() });
            if a.activity_id.is_empty() { return Err(DecisionError::new("ActivityId is not set on decision.")); }
            let ty = a.activity_type.as_ref().filter(|t| !t.name.is_empty()).ok_or_else(|| DecisionError::new("ActivityType is not set on decision."))?;
            validate_retry_policy(a.retry_policy.as_ref())?;
            length(context, "ActivityID", &a.domain, &a.activity_id, "ActivityID exceeds length limit.")?;
            length(context, "ActivityType", &a.domain, &ty.name, "ActivityType exceeds length limit.")?;
            length(context, "Domain", &a.domain, &a.domain, "Domain exceeds length limit.")?;
            let mut timeouts = ActivityTimeouts { schedule_to_close_seconds: a.schedule_to_close_timeout_seconds.unwrap_or(0), schedule_to_start_seconds: a.schedule_to_start_timeout_seconds.unwrap_or(0), start_to_close_seconds: a.start_to_close_timeout_seconds.unwrap_or(0), heartbeat_seconds: a.heartbeat_timeout_seconds.unwrap_or(0) };
            normalize_activity_timeouts(&mut timeouts, defaults.workflow_timeout_seconds)?;
            if let Some(p) = &a.retry_policy {
                let retryable = !p.non_retriable_error_reasons.iter().any(|r| r == "cadenceInternal:Timeout SCHEDULE_TO_START");
                extend_activity_timeouts_for_retry(&mut timeouts, defaults.workflow_timeout_seconds, p.expiration_interval_in_seconds, context.max_schedule_to_start_retry_seconds(), retryable);
            }
            a.schedule_to_close_timeout_seconds = Some(timeouts.schedule_to_close_seconds);
            a.schedule_to_start_timeout_seconds = Some(timeouts.schedule_to_start_seconds);
            a.start_to_close_timeout_seconds = Some(timeouts.start_to_close_seconds);
            a.heartbeat_timeout_seconds = Some(timeouts.heartbeat_seconds);
            Ok(())
        },
        D::StartTimer => {
            let a = decision.start_timer_decision_attributes.as_ref().ok_or_else(|| DecisionError::new("StartTimerDecisionAttributes is not set on decision."))?;
            if a.timer_id.is_empty() { return Err(DecisionError::new("TimerId is not set on decision.")); }
            length(context, "TimerID", domain_name, &a.timer_id, "TimerId exceeds length limit.")?;
            validate_timer_timeout(&a.timer_id, a.start_to_fire_timeout_seconds.unwrap_or(0), context.max_length("TimerID", domain_name))
        },
        D::RequestCancelActivityTask => validate_activity_cancel_attributes(decision.request_cancel_activity_task_decision_attributes.as_ref(), context.max_length("ActivityID", domain_name)),
        D::CancelTimer => validate_timer_cancel_attributes(decision.cancel_timer_decision_attributes.as_ref(), context.max_length("TimerID", domain_name)),
        D::RecordMarker => validate_record_marker_attributes(decision.record_marker_decision_attributes.as_ref(), context.max_length("MarkerName", domain_name)),
        D::CompleteWorkflowExecution => validate_complete_workflow_execution_attributes(decision.complete_workflow_execution_decision_attributes.as_ref()),
        D::FailWorkflowExecution => validate_fail_workflow_execution_attributes(decision.fail_workflow_execution_decision_attributes.as_ref()),
        D::CancelWorkflowExecution => validate_cancel_workflow_execution_attributes(decision.cancel_workflow_execution_decision_attributes.as_ref()),
        D::RequestCancelExternalWorkflowExecution => {
            let domain = decision.request_cancel_external_workflow_execution_decision_attributes.as_ref().map_or("", |a| a.domain.as_str());
            validate_cancel_external_workflow_execution_attributes(context, context.source_domain_id(), context.target_domain_id(), decision.request_cancel_external_workflow_execution_decision_attributes.as_ref(), context.max_length("Domain", domain), context.max_length("WorkflowID", domain))
        },
        D::SignalExternalWorkflowExecution => {
            let domain = decision.signal_external_workflow_execution_decision_attributes.as_ref().map_or("", |a| a.domain.as_str());
            validate_signal_external_workflow_execution_attributes(context, context.source_domain_id(), context.target_domain_id(), decision.signal_external_workflow_execution_decision_attributes.as_ref(), context.max_length("Domain", domain), context.max_length("WorkflowID", domain))
        },
        D::StartChildWorkflowExecution => {
            validate_cross_domain_call(context, context.source_domain_id(), context.target_domain_id())?;
            let a = decision.start_child_workflow_execution_decision_attributes.as_mut().ok_or_else(|| DecisionError::new("StartChildWorkflowExecutionDecisionAttributes is not set on decision."))?;
            if a.workflow_id.is_empty() { return Err(DecisionError::new("Required field WorkflowID is not set on decision.")); }
            if a.workflow_type.as_ref().is_none_or(|t| t.name.is_empty()) { return Err(DecisionError::new("Required field WorkflowType is not set on decision.")); }
            length(context, "Domain", &a.domain, &a.domain, "Domain exceeds length limit.")?;
            let max_id = context.max_length("WorkflowID", &a.domain);
            let max_type = context.max_length("WorkflowType", &a.domain);
            let max_task_list = context.max_length("TaskList", &a.domain);
            validate_start_child_execution_attributes(Some(a), &defaults, max_id, max_type, max_task_list, context.reserved_task_list_prefix(), |s| context.validate_cron(s))
        },
        D::ContinueAsNewWorkflowExecution => {
            let policy = context.search_policy(domain_name)?;
            validate_continue_as_new_workflow_execution_attributes(decision.continue_as_new_workflow_execution_decision_attributes.as_mut(), &defaults, context.max_length("WorkflowType", domain_name), context.max_length("TaskList", domain_name), context.reserved_task_list_prefix(), &policy)
        },
        D::UpsertWorkflowSearchAttributes => validate_upsert_workflow_search_attributes(decision.upsert_workflow_search_attributes_decision_attributes.as_ref(), &context.search_policy(domain_name)?),
        _ => Err(DecisionError::new("Unknown decision type")),
    }
}

pub struct AttrValidator<C: ValidationContext> { pub context: C }
pub fn new_attr_validator<C: ValidationContext>(context: C) -> AttrValidator<C> { AttrValidator { context } }
impl<C: ValidationContext> AttrValidator<C> {
    pub fn validate(&self, decision: &mut PortDecision, domain: &str) -> Result<(), DecisionError> { validate(&self.context, decision, domain) }
}
pub struct WorkflowSizeChecker<'a, S, O> {
    pub mutable_state: &'a mut S, pub observer: &'a O, pub limits: WorkflowSizeLimits, pub completed_id: i64,
}
pub fn new_workflow_size_checker<'a, S: WorkflowSizeMutableState, O: WorkflowSizeObserver>(mutable_state: &'a mut S, observer: &'a O, limits: WorkflowSizeLimits, completed_id: i64) -> WorkflowSizeChecker<'a, S, O> { WorkflowSizeChecker { mutable_state, observer, limits, completed_id } }
impl<S: WorkflowSizeMutableState, O: WorkflowSizeObserver> WorkflowSizeChecker<'_, S, O> {
    pub fn fail_workflow_size_exceeds_limit(&mut self) -> Result<bool, DecisionError> { fail_workflow_size_exceeds_limit(self.mutable_state, self.observer, self.limits, self.completed_id) }
    pub fn fail_workflow_if_blob_size_exceeds_limit(&mut self, decision_type: DecisionType, blob: &[u8], message: &str) -> Result<bool, DecisionError> {
        self.observer.record_blob_size(decision_type, blob.len());
        let warn = self.limits.blob_size_warn.min(self.limits.blob_size_error);
        if self.limits.blob_size_error < self.limits.blob_size_warn { self.observer.misconfigured_blob_limits(); }
        if blob.len() <= warn { return Ok(false); }
        if blob.len() <= self.limits.blob_size_error { self.observer.warn_blob_limit(decision_type, blob.len()); return Ok(false); }
        self.observer.error_blob_limit(decision_type, blob.len());
        self.mutable_state.add_fail_workflow_event(self.completed_id, FAILURE_REASON_DECISION_BLOB_SIZE_EXCEEDS_LIMIT, message.as_bytes())?;
        Ok(true)
    }
}

/// TODO(link): shared Cadence API types currently omit this Go field.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterAttribute { pub scope: String, pub name: String }
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveClusterSelectionPolicy { pub cluster_attribute: Option<ClusterAttribute> }
#[derive(Debug, Clone, Default)]
pub struct ContinueAsNewPortAttributes {
    pub attributes: cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes,
    pub active_cluster_selection_policy: Option<ActiveClusterSelectionPolicy>,
}
pub fn inherit_active_cluster_selection_policy(attributes: &mut ContinueAsNewPortAttributes, previous: &Option<ActiveClusterSelectionPolicy>) {
    if attributes.active_cluster_selection_policy.is_none() { attributes.active_cluster_selection_policy.clone_from(previous); }
}
