//! Decision task validation and dispatch logic ported from Go `service/history/decision`.
//! TODO(link): execution-facing callbacks become `execution::MutableState` methods once that port lands.
pub mod task_handler;
pub mod handler;
pub mod checker;
use cadence_api::enums::DecisionType;
pub use checker::AttrValidator as AttributeValidator;
pub use handler::RpcHandler as Handler;
pub use task_handler::TaskHandler as DecisionTaskHandler;

pub const ACTIVITY_CANCELLATION_MSG_ACTIVITY_ID_UNKNOWN: &str = "ACTIVITY_ID_UNKNOWN";
pub const ACTIVITY_CANCELLATION_MSG_ACTIVITY_NOT_STARTED: &str = "ACTIVITY_ID_NOT_STARTED";
pub const FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT: &str = "HISTORY_EXCEEDS_LIMIT";
pub const FAILURE_REASON_DECISION_BLOB_SIZE_EXCEEDS_LIMIT: &str = "DECISION_BLOB_SIZE_EXCEEDS_LIMIT";
pub const FAILURE_REASON_SEMAPHORE_NOT_FOUND: &str = "SEMAPHORE_NOT_FOUND";
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionError {
    pub kind: DecisionErrorKind,
    pub message: String,
}
/// Go typed errors returned by the decision package (`types.BadRequestError`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionErrorKind {
    BadRequest,
    Internal,
    EntityNotExists,
    EventAlreadyStarted,
}
impl DecisionError {
    /// Go `&types.BadRequestError{Message: m}`.
    pub fn new(m: impl Into<String>) -> Self {
        Self { kind: DecisionErrorKind::BadRequest, message: m.into() }
    }
    pub fn internal(m: impl Into<String>) -> Self {
        Self { kind: DecisionErrorKind::Internal, message: m.into() }
    }
    pub fn entity_not_exists(m: impl Into<String>) -> Self {
        Self { kind: DecisionErrorKind::EntityNotExists, message: m.into() }
    }
    pub fn event_already_started(m: impl Into<String>) -> Self {
        Self { kind: DecisionErrorKind::EventAlreadyStarted, message: m.into() }
    }
}
impl std::fmt::Display for DecisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for DecisionError {}

#[derive(Debug, Clone, Copy, Default)]
pub struct WorkflowSizeLimits {
    pub history_size_warn: usize,
    pub history_size_error: usize,
    pub history_count_warn: usize,
    pub history_count_error: usize,
    pub blob_size_warn: usize,
    pub blob_size_error: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeAction {
    Continue,
    Warn,
    Fail,
}
pub fn check_workflow_size(
    history_size: usize,
    history_count: usize,
    limits: WorkflowSizeLimits,
) -> SizeAction {
    if history_size > limits.history_size_error || history_count > limits.history_count_error {
        SizeAction::Fail
    } else if history_size > limits.history_size_warn || history_count > limits.history_count_warn {
        SizeAction::Warn
    } else {
        SizeAction::Continue
    }
}
pub fn check_blob_size(blob_size: usize, limits: WorkflowSizeLimits) -> SizeAction {
    if blob_size > limits.blob_size_error {
        SizeAction::Fail
    } else if blob_size > limits.blob_size_warn.min(limits.blob_size_error) {
        SizeAction::Warn
    } else {
        SizeAction::Continue
    }
}
pub fn convert_search_attributes_to_byte_array(
    fields: &std::collections::BTreeMap<String, Vec<u8>>,
) -> Vec<u8> {
    fields.iter().flat_map(|(key, value)| key.as_bytes().iter().chain(value).copied()).collect()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActivityTimeouts {
    pub schedule_to_close_seconds: i32,
    pub schedule_to_start_seconds: i32,
    pub start_to_close_seconds: i32,
    pub heartbeat_seconds: i32,
}
/// Mirrors Go timeout normalization: reject negatives, cap to workflow timeout,
/// then infer schedule-to-close or its component values using the same precedence.
pub fn normalize_activity_timeouts(
    t: &mut ActivityTimeouts,
    workflow_timeout: i32,
) -> Result<(), DecisionError> {
    if t.schedule_to_close_seconds < 0
        || t.schedule_to_start_seconds < 0
        || t.start_to_close_seconds < 0
        || t.heartbeat_seconds < 0
    {
        return Err(DecisionError::new("A valid timeout may not be negative."));
    }
    if t.schedule_to_close_seconds > wf_timeout(workflow_timeout) {
        t.schedule_to_close_seconds = workflow_timeout
    }
    if t.schedule_to_start_seconds > workflow_timeout {
        t.schedule_to_start_seconds = workflow_timeout
    }
    if t.start_to_close_seconds > workflow_timeout {
        t.start_to_close_seconds = workflow_timeout
    }
    if t.heartbeat_seconds > workflow_timeout {
        t.heartbeat_seconds = workflow_timeout
    }
    let close = t.schedule_to_close_seconds > 0;
    let start = t.schedule_to_start_seconds > 0;
    let run = t.start_to_close_seconds > 0;
    if close {
        if !start {
            t.schedule_to_start_seconds = t.schedule_to_close_seconds
        }
        if !run {
            t.start_to_close_seconds = t.schedule_to_close_seconds
        }
    } else if start && run {
        t.schedule_to_close_seconds = t
            .schedule_to_start_seconds
            .wrapping_add(t.start_to_close_seconds)
            .min(workflow_timeout)
    } else {
        return Err(DecisionError::new(
            "A valid ScheduleToCloseTimeout is not set on decision.",
        ));
    }
    Ok(())
}
fn wf_timeout(v: i32) -> i32 {
    v
}
pub fn validate_timer_timeout(
    timer_id: &str,
    start_to_fire_timeout_seconds: i64,
    max_id_length: usize,
) -> Result<(), DecisionError> {
    if timer_id.is_empty() {
        return Err(DecisionError::new("TimerId is not set on decision."));
    }
    if timer_id.len() > max_id_length {
        return Err(DecisionError::new("TimerId exceeds length limit."));
    }
    if start_to_fire_timeout_seconds <= 0 {
        return Err(DecisionError::new(format!(
            "Invalid StartToFireTimeoutSeconds: {start_to_fire_timeout_seconds}"
        )));
    }
    Ok(())
}
pub fn validated_task_list(
    name: &str,
    kind: Option<cadence_api::enums::TaskListKind>,
    default_name: &str,
    default_kind: Option<cadence_api::enums::TaskListKind>,
    max_name_length: usize,
    reserved_prefix: &str,
) -> Result<(String, Option<cadence_api::enums::TaskListKind>), DecisionError> {
    use cadence_api::enums::TaskListKind;
    let mut name = name.to_owned();
    let mut kind = kind;
    if name.is_empty() {
        if default_name.is_empty() {
            return Err(DecisionError::new("missing task list name"));
        }
        return Ok((default_name.to_owned(), default_kind));
    }
    if kind.unwrap_or(TaskListKind::Normal) == TaskListKind::Normal
        && default_kind == Some(TaskListKind::Ephemeral)
    {
        kind = default_kind
    }
    if name.len() > max_name_length {
        return Err(DecisionError::new(format!(
            "task list name exceeds length limit of {max_name_length}"
        )));
    }
    if name.starts_with(reserved_prefix) {
        return Err(DecisionError::new(format!(
            "task list name cannot start with reserved prefix {reserved_prefix}"
        )));
    }
    Ok((std::mem::take(&mut name), kind))
}
/// Extends retryable activity timers to the retry-policy expiration bound.
pub fn extend_activity_timeouts_for_retry(
    t: &mut ActivityTimeouts,
    workflow_timeout: i32,
    expiration_seconds: i32,
    max_schedule_to_start_retry_seconds: i32,
    schedule_to_start_retryable: bool,
) {
    let expiration = if expiration_seconds == 0 || expiration_seconds > workflow_timeout {
        workflow_timeout
    } else {
        expiration_seconds
    };
    if schedule_to_start_retryable {
        t.schedule_to_start_seconds = t
            .schedule_to_start_seconds
            .max(expiration.min(max_schedule_to_start_retry_seconds))
    }
    t.schedule_to_close_seconds = t.schedule_to_close_seconds.max(expiration)
}

/// Go `common.ValidateRetryPolicy`, including its exact client error messages.
pub fn validate_retry_policy(
    policy: Option<&cadence_api::types::RetryPolicy>,
) -> Result<(), DecisionError> {
    let Some(p) = policy else { return Ok(()) };
    if p.initial_interval_in_seconds <= 0 {
        return Err(DecisionError::new(
            "InitialIntervalInSeconds must be greater than 0 on retry policy.",
        ));
    }
    if p.backoff_coefficient < 1.0 {
        return Err(DecisionError::new(
            "BackoffCoefficient cannot be less than 1 on retry policy.",
        ));
    }
    if p.maximum_interval_in_seconds < 0 {
        return Err(DecisionError::new(
            "MaximumIntervalInSeconds cannot be less than 0 on retry policy.",
        ));
    }
    if p.maximum_interval_in_seconds > 0
        && p.maximum_interval_in_seconds < p.initial_interval_in_seconds
    {
        return Err(DecisionError::new("MaximumIntervalInSeconds cannot be less than InitialIntervalInSeconds on retry policy."));
    }
    if p.maximum_attempts < 0 {
        return Err(DecisionError::new(
            "MaximumAttempts cannot be less than 0 on retry policy.",
        ));
    }
    if p.expiration_interval_in_seconds < 0 {
        return Err(DecisionError::new(
            "ExpirationIntervalInSeconds cannot be less than 0 on retry policy.",
        ));
    }
    if p.jitter_coefficient.is_nan() || !(0.0..=1.0).contains(&p.jitter_coefficient) {
        return Err(DecisionError::new(
            "JitterCoefficient must be between 0 and 1 on retry policy.",
        ));
    }
    if p.maximum_attempts == 0 && p.expiration_interval_in_seconds == 0 {
        return Err(DecisionError::new("MaximumAttempts and ExpirationIntervalInSeconds are both 0. At least one of them must be specified."));
    }
    Ok(())
}
/// Go checker equivalent for activity inputs with workflow timeout and inherited task list supplied by execution.
#[allow(clippy::too_many_arguments)]
pub fn validate_activity_schedule_attributes(
    a: Option<&mut cadence_api::tasks::ScheduleActivityTaskDecisionAttributes>,
    default_task_list: (&str, Option<cadence_api::enums::TaskListKind>),
    workflow_timeout: i32,
    max_activity_id: usize,
    max_activity_type: usize,
    max_task_list: usize,
    max_schedule_to_start_retry_seconds: i32,
    reserved_prefix: &str,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new("ScheduleActivityTaskDecisionAttributes is not set on decision.")
    })?;
    if a.activity_id.is_empty() {
        return Err(DecisionError::new("ActivityId is not set on decision."));
    }
    if a.activity_id.len() > max_activity_id {
        return Err(DecisionError::new("ActivityID exceeds length limit."));
    }
    let activity_type = a
        .activity_type
        .as_ref()
        .ok_or_else(|| DecisionError::new("ActivityType is not set on decision."))?;
    if activity_type.name.is_empty() {
        return Err(DecisionError::new("ActivityType is not set on decision."));
    }
    if activity_type.name.len() > max_activity_type {
        return Err(DecisionError::new("ActivityType exceeds length limit."));
    }
    validate_retry_policy(a.retry_policy.as_ref())?;
    let (task_name, task_kind) = a
        .task_list
        .take()
        .map_or((String::new(), None), |x| (x.name, x.kind));
    let (name, kind) = validated_task_list(
        &task_name,
        task_kind,
        default_task_list.0,
        default_task_list.1,
        max_task_list,
        reserved_prefix,
    )?;
    a.task_list = Some(cadence_api::types::TaskList {
        name,
        kind,
        base_name: String::new(),
    });
    let mut timeouts = ActivityTimeouts {
        schedule_to_close_seconds: a.schedule_to_close_timeout_seconds.unwrap_or(0),
        schedule_to_start_seconds: a.schedule_to_start_timeout_seconds.unwrap_or(0),
        start_to_close_seconds: a.start_to_close_timeout_seconds.unwrap_or(0),
        heartbeat_seconds: a.heartbeat_timeout_seconds.unwrap_or(0),
    };
    normalize_activity_timeouts(&mut timeouts, workflow_timeout)?;
    a.schedule_to_close_timeout_seconds = Some(timeouts.schedule_to_close_seconds);
    a.schedule_to_start_timeout_seconds = Some(timeouts.schedule_to_start_seconds);
    a.start_to_close_timeout_seconds = Some(timeouts.start_to_close_seconds);
    a.heartbeat_timeout_seconds = Some(timeouts.heartbeat_seconds);
    if let Some(policy) = a.retry_policy.as_ref() {
        let retryable = !policy
            .non_retriable_error_reasons
            .iter()
            .any(|reason| reason == "cadenceInternal:Timeout SCHEDULE_TO_START");
        extend_activity_timeouts_for_retry(
            &mut timeouts,
            workflow_timeout,
            policy.expiration_interval_in_seconds,
            max_schedule_to_start_retry_seconds,
            retryable,
        );
        a.schedule_to_close_timeout_seconds = Some(timeouts.schedule_to_close_seconds);
        a.schedule_to_start_timeout_seconds = Some(timeouts.schedule_to_start_seconds)
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct DomainReplication {
    pub domain_id: String,
    pub name: String,
    pub clusters: Vec<String>,
}
pub trait DomainReplicationLookup {
    fn get_domain_by_id(&self, domain_id: &str) -> Result<DomainReplication, DecisionError>;
    fn cross_cluster_operations_enabled(&self, source_domain_name: &str) -> bool;
}
/// Exact Go cross-domain rule: local domains must share one cluster; global
/// domains require matching cluster-set lengths, source-domain enablement, and
/// every source cluster to appear in the target set.
pub fn validate_cross_domain_call(
    lookup: &impl DomainReplicationLookup,
    source_domain_id: &str,
    target_domain_id: &str,
) -> Result<(), DecisionError> {
    if source_domain_id == target_domain_id {
        return Ok(());
    }
    let source = lookup.get_domain_by_id(source_domain_id)?;
    let target = lookup.get_domain_by_id(target_domain_id)?;
    let invalid = || {
        DecisionError::new(format!(
            "cannot make cross domain call between {} and {}",
            source.name, target.name
        ))
    };
    if source.clusters.len() == 1 && target.clusters.len() == 1 {
        return if source.clusters[0] == target.clusters[0] {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    if source.clusters.len() == target.clusters.len()
        && lookup.cross_cluster_operations_enabled(&source.name)
    {
        return if source
            .clusters
            .iter()
            .all(|c| target.clusters.iter().any(|other| other == c))
        {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    Err(invalid())
}

pub fn validate_complete_workflow_execution_attributes(
    a: Option<&cadence_api::tasks::CompleteWorkflowExecutionDecisionAttributes>,
) -> Result<(), DecisionError> {
    a.ok_or_else(|| {
        DecisionError::new("CompleteWorkflowExecutionDecisionAttributes is not set on decision.")
    })
    .map(|_| ())
}
pub fn validate_fail_workflow_execution_attributes(
    a: Option<&cadence_api::tasks::FailWorkflowExecutionDecisionAttributes>,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new("FailWorkflowExecutionDecisionAttributes is not set on decision.")
    })?;
    if a.reason.is_none() {
        return Err(DecisionError::new("Reason is not set on decision."));
    }
    Ok(())
}
pub fn validate_cancel_workflow_execution_attributes(
    a: Option<&cadence_api::tasks::CancelWorkflowExecutionDecisionAttributes>,
) -> Result<(), DecisionError> {
    a.ok_or_else(|| {
        DecisionError::new("CancelWorkflowExecutionDecisionAttributes is not set on decision.")
    })
    .map(|_| ())
}
pub fn validate_activity_cancel_attributes(
    a: Option<&cadence_api::tasks::RequestCancelActivityTaskDecisionAttributes>,
    max_id_length: usize,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new("RequestCancelActivityTaskDecisionAttributes is not set on decision.")
    })?;
    if a.activity_id.is_empty() {
        return Err(DecisionError::new("ActivityId is not set on decision."));
    }
    if a.activity_id.len() > max_id_length {
        return Err(DecisionError::new("ActivityId exceeds length limit."));
    }
    Ok(())
}
pub fn validate_timer_cancel_attributes(
    a: Option<&cadence_api::tasks::CancelTimerDecisionAttributes>,
    max_id_length: usize,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new("CancelTimerDecisionAttributes is not set on decision.")
    })?;
    if a.timer_id.is_empty() {
        return Err(DecisionError::new("TimerId is not set on decision."));
    }
    if a.timer_id.len() > max_id_length {
        return Err(DecisionError::new("TimerId exceeds length limit."));
    }
    Ok(())
}
pub fn validate_record_marker_attributes(
    a: Option<&cadence_api::tasks::RecordMarkerDecisionAttributes>,
    max_name_length: usize,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new("RecordMarkerDecisionAttributes is not set on decision.")
    })?;
    if a.marker_name.is_empty() {
        return Err(DecisionError::new("MarkerName is not set on decision."));
    }
    if a.marker_name.len() > max_name_length {
        return Err(DecisionError::new("MarkerName exceeds length limit."));
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct SearchAttributePolicy {
    pub enabled: bool,
    pub valid_types: std::collections::HashMap<String, cadence_api::enums::IndexedValueType>,
    pub number_of_keys_limit: usize,
    pub value_size_limit: usize,
    pub total_size_limit: usize,
}
pub fn validate_upsert_workflow_search_attributes(
    a: Option<&cadence_api::tasks::UpsertWorkflowSearchAttributesDecisionAttributes>,
    policy: &SearchAttributePolicy,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new(
            "UpsertWorkflowSearchAttributesDecisionAttributes is not set on decision.",
        )
    })?;
    let attrs = a
        .search_attributes
        .as_ref()
        .ok_or_else(|| DecisionError::new("SearchAttributes is not set on decision."))?;
    let fields = attrs
        .indexed_fields
        .as_ref()
        .ok_or_else(|| DecisionError::new("IndexedFields is empty on decision."))?;
    if fields.is_empty() {
        return Err(DecisionError::new("IndexedFields is empty on decision."));
    }
    if fields.len() > policy.number_of_keys_limit {
        return Err(DecisionError::new(format!(
            "number of keys {} exceed limit",
            fields.len()
        )));
    }
    let mut total = 0usize;
    for (key, value) in fields {
        if policy.enabled {
            let Some(kind) = policy.valid_types.get(key) else {
                return Err(DecisionError::new(format!(
                    "{key} is not a valid search attribute key"
                )));
            };
            if !search_value_matches(value, *kind) {
                return Err(DecisionError::new(format!(
                    "{} is not a valid search attribute value for key {key}",
                    String::from_utf8_lossy(value)
                )));
            }
        }
        if cadence_api::search_attributes::is_system_indexed_key(key) {
            return Err(DecisionError::new(format!(
                "{key} is read-only Cadence reserved attribute"
            )));
        }
        if value.len() > policy.value_size_limit {
            return Err(DecisionError::new(format!(
                "size limit exceed for key {key}"
            )));
        }
        total = total.saturating_add(key.len()).saturating_add(value.len())
    }
    if total > policy.total_size_limit {
        return Err(DecisionError::new(format!(
            "total size {total} exceed limit"
        )));
    }
    Ok(())
}
fn search_value_matches(bytes: &[u8], kind: cadence_api::enums::IndexedValueType) -> bool {
    use cadence_api::enums::IndexedValueType as T;
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    fn scalar(v: &serde_json::Value, k: cadence_api::enums::IndexedValueType) -> bool {
        use cadence_api::enums::IndexedValueType as T;
        if v.is_null() { return true; }
        match k {
            T::String | T::Keyword => v.is_string(),
            T::Int => v.as_i64().is_some(),
            T::Double => v.as_f64().is_some(),
            T::Bool => v.is_boolean(),
            T::Datetime => v.as_str().is_some_and(valid_rfc3339),
            _ => false,
        }
    }
    match v {
        serde_json::Value::Array(a) => a.iter().all(|x| scalar(x, kind)),
        other => {
            let _ = T::String;
            scalar(&other, kind)
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ParentDecisionDefaults {
    pub workflow_type_name: String,
    pub task_list_name: String,
    pub task_list_kind: Option<cadence_api::enums::TaskListKind>,
    pub workflow_timeout_seconds: i32,
    pub decision_timeout_seconds: i32,
    /// Previous run's `executionInfo.ActiveClusterSelectionPolicy` (ContinueAsNew inheritance).
    pub active_cluster_selection_policy: Option<cadence_api::types::ActiveClusterSelectionPolicy>,
}
#[allow(clippy::too_many_arguments)]
pub fn validate_start_child_execution_attributes(
    a: Option<&mut cadence_api::tasks::StartChildWorkflowExecutionDecisionAttributes>,
    defaults: &ParentDecisionDefaults,
    max_workflow_id: usize,
    max_workflow_type: usize,
    max_task_list: usize,
    reserved_task_list_prefix: &str,
    validate_cron: impl Fn(&str) -> Result<(), DecisionError>,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new("StartChildWorkflowExecutionDecisionAttributes is not set on decision.")
    })?;
    if a.workflow_id.is_empty() {
        return Err(DecisionError::new(
            "Required field WorkflowID is not set on decision.",
        ));
    }
    let workflow_type = a
        .workflow_type
        .as_ref()
        .ok_or_else(|| DecisionError::new("Required field WorkflowType is not set on decision."))?;
    if workflow_type.name.is_empty() {
        return Err(DecisionError::new(
            "Required field WorkflowType is not set on decision.",
        ));
    }
    if a.workflow_id.len() > max_workflow_id {
        return Err(DecisionError::new("WorkflowId exceeds length limit."));
    }
    if workflow_type.name.len() > max_workflow_type {
        return Err(DecisionError::new("WorkflowType exceeds length limit."));
    }
    validate_retry_policy(a.retry_policy.as_ref())?;
    if !a.cron_schedule.is_empty() {
        validate_cron(&a.cron_schedule)?
    }
    let (task_name, task_kind) = a
        .task_list
        .take()
        .map_or((String::new(), None), |t| (t.name, t.kind));
    let (name, kind) = validated_task_list(
        &task_name,
        task_kind,
        &defaults.task_list_name,
        defaults.task_list_kind,
        max_task_list,
        reserved_task_list_prefix,
    )?;
    a.task_list = Some(cadence_api::types::TaskList {
        name,
        kind,
        base_name: String::new(),
    });
    if a.execution_start_to_close_timeout_seconds.unwrap_or(0) <= 0 {
        a.execution_start_to_close_timeout_seconds = Some(defaults.workflow_timeout_seconds)
    }
    if a.task_start_to_close_timeout_seconds.unwrap_or(0) <= 0 {
        a.task_start_to_close_timeout_seconds = Some(defaults.decision_timeout_seconds)
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
pub fn validate_continue_as_new_workflow_execution_attributes(
    a: Option<&mut cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes>,
    defaults: &ParentDecisionDefaults,
    max_workflow_type: usize,
    max_task_list: usize,
    reserved_task_list_prefix: &str,
    domain_name: impl FnOnce() -> Result<String, DecisionError>,
    search_policy: impl FnOnce(&str) -> Result<SearchAttributePolicy, DecisionError>,
) -> Result<(), DecisionError> {
    let a = a.ok_or_else(|| {
        DecisionError::new(
            "ContinueAsNewWorkflowExecutionDecisionAttributes is not set on decision.",
        )
    })?;
    if a.workflow_type.as_ref().is_none_or(|t| t.name.is_empty()) {
        a.workflow_type = Some(cadence_api::types::WorkflowType {
            name: defaults.workflow_type_name.clone(),
        })
    }
    let wt = a.workflow_type.as_ref().unwrap();
    if wt.name.len() > max_workflow_type {
        return Err(DecisionError::new("WorkflowType exceeds length limit."));
    }
    let (task_name, task_kind) = a
        .task_list
        .take()
        .map_or((String::new(), None), |t| (t.name, t.kind));
    let (name, kind) = validated_task_list(
        &task_name,
        task_kind,
        &defaults.task_list_name,
        defaults.task_list_kind,
        max_task_list,
        reserved_task_list_prefix,
    )?;
    a.task_list = Some(cadence_api::types::TaskList {
        name,
        kind,
        base_name: String::new(),
    });
    if a.execution_start_to_close_timeout_seconds.unwrap_or(0) <= 0 {
        a.execution_start_to_close_timeout_seconds = Some(defaults.workflow_timeout_seconds)
    }
    if a.task_start_to_close_timeout_seconds.unwrap_or(0) <= 0 {
        a.task_start_to_close_timeout_seconds = Some(defaults.decision_timeout_seconds)
    }
    // Go: inherit the previous run's policy when the decision has none.
    if a.active_cluster_selection_policy.is_none() {
        a.active_cluster_selection_policy.clone_from(&defaults.active_cluster_selection_policy);
    }
    if a.backoff_start_interval_in_seconds.unwrap_or(0) < 0 {
        return Err(DecisionError::new("BackoffStartInterval is less than 0."));
    }
    // Go resolves the domain name from executionInfo.DomainID before validating search attributes.
    let domain_name = domain_name()?;
    if let Some(sa) = a.search_attributes.as_ref().filter(|sa| sa.indexed_fields.as_ref().is_some_and(|f| !f.is_empty())) {
        let attrs = cadence_api::tasks::UpsertWorkflowSearchAttributesDecisionAttributes {
            search_attributes: Some(sa.clone()),
        };
        validate_upsert_workflow_search_attributes(Some(&attrs), &search_policy(&domain_name)?)?
    }
    Ok(())
}

pub use cadence_api::tasks::AcquireSemaphoreDecisionAttributes;
pub fn validate_acquire_semaphore_attributes(a:Option<&AcquireSemaphoreDecisionAttributes>,max_name_length:usize)->Result<(),DecisionError>{let a=a.ok_or_else(||DecisionError::new("AcquireSemaphoreDecisionAttributes is not set on decision."))?;if a.semaphore_name.is_empty(){return Err(DecisionError::new("SemaphoreName is not set on decision."))}
if a.semaphore_name.len()>max_name_length{return Err(DecisionError::new("SemaphoreName exceeds length limit."))}
if a.wait_timeout_seconds.unwrap_or(0)<0{return Err(DecisionError::new(format!("Invalid WaitTimeoutSeconds: {}",a.wait_timeout_seconds.unwrap())))}Ok(())}
pub fn fail_workflow_semaphore_not_found_details(name:&str,domain_name:&str)->String{format!("semaphore \"{name}\" does not exist in domain \"{domain_name}\"")}

#[allow(clippy::too_many_arguments)]pub fn validate_cancel_external_workflow_execution_attributes(lookup:&impl DomainReplicationLookup,source_domain_id:&str,target_domain_id:&str,a:Option<&cadence_api::tasks::RequestCancelExternalWorkflowExecutionDecisionAttributes>,max_domain_length:usize,max_workflow_id_length:usize)->Result<(),DecisionError>{validate_cross_domain_call(lookup,source_domain_id,target_domain_id)?;let a=a.ok_or_else(||DecisionError::new("RequestCancelExternalWorkflowExecutionDecisionAttributes is not set on decision."))?;if a.workflow_id.is_empty(){return Err(DecisionError::new("WorkflowId is not set on decision."))}
if a.domain.len()>max_domain_length{return Err(DecisionError::new("Domain exceeds length limit."))}
if a.workflow_id.len()>max_workflow_id_length{return Err(DecisionError::new("WorkflowId exceeds length limit."))}
if !a.run_id.is_empty()&&uuid::Uuid::parse_str(&a.run_id).is_err(){return Err(DecisionError::new("Invalid RunId set on decision."))}Ok(())}
#[allow(clippy::too_many_arguments)]pub fn validate_signal_external_workflow_execution_attributes(lookup:&impl DomainReplicationLookup,source_domain_id:&str,target_domain_id:&str,a:Option<&cadence_api::tasks::SignalExternalWorkflowExecutionDecisionAttributes>,max_domain_length:usize,max_workflow_id_length:usize)->Result<(),DecisionError>{validate_cross_domain_call(lookup,source_domain_id,target_domain_id)?;let a=a.ok_or_else(||DecisionError::new("SignalExternalWorkflowExecutionDecisionAttributes is not set on decision."))?;let execution=a.execution.as_ref().ok_or_else(||DecisionError::new("Execution is nil on decision."))?;if execution.workflow_id.is_empty(){return Err(DecisionError::new("WorkflowId is not set on decision."))}
if a.domain.len()>max_domain_length{return Err(DecisionError::new("Domain exceeds length limit."))}
if execution.workflow_id.len()>max_workflow_id_length{return Err(DecisionError::new("WorkflowId exceeds length limit."))}
if !execution.run_id.is_empty()&&uuid::Uuid::parse_str(&execution.run_id).is_err(){return Err(DecisionError::new("Invalid RunId set on decision."))}
if a.signal_name.is_empty(){return Err(DecisionError::new("SignalName is not set on decision."))}Ok(())}

pub trait WorkflowSizeMutableState{fn next_event_id(&self)->i64;fn history_size(&self)->i64;fn add_fail_workflow_event(&mut self,completed_event_id:i64,reason:&str,details:&[u8])->Result<(),DecisionError>;}
pub trait WorkflowSizeObserver{fn record_blob_size(&self,_decision_type:DecisionType,_size:usize){}fn misconfigured_blob_limits(&self){}fn warn_blob_limit(&self,_decision_type:DecisionType,_size:usize){}fn error_blob_limit(&self,_decision_type:DecisionType,_size:usize){}fn record_history_size(&self,_size:i64){}fn record_history_count(&self,_count:i64){}fn warn_history_limit(&self,_size:i64,_count:i64){}fn error_history_limit(&self,_size:i64,_count:i64){}}
pub fn fail_workflow_size_exceeds_limit(state:&mut impl WorkflowSizeMutableState,observer:&impl WorkflowSizeObserver,limits:WorkflowSizeLimits,completed_event_id:i64)->Result<bool,DecisionError>{let history_count=state.next_event_id()-1;let history_size=state.history_size();observer.record_history_size(history_size);observer.record_history_count(history_count);if history_size>limits.history_size_error as i64||history_count>limits.history_count_error as i64{observer.error_history_limit(history_size,history_count);state.add_fail_workflow_event(completed_event_id,FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT,b"Workflow history size / count exceeds limit.")?;return Ok(true)}
if history_size>limits.history_size_warn as i64||history_count>limits.history_count_warn as i64{observer.warn_history_limit(history_size,history_count)}Ok(false)}
pub fn fail_workflow_if_blob_size_exceeds_limit(state:&mut impl WorkflowSizeMutableState,blob_size:usize,limits:WorkflowSizeLimits,completed_event_id:i64,message:&str)->Result<bool,DecisionError>{if blob_size>limits.blob_size_error{state.add_fail_workflow_event(completed_event_id,FAILURE_REASON_DECISION_BLOB_SIZE_EXCEEDS_LIMIT,message.as_bytes())?;return Ok(true)}Ok(false)}

// Go time.Time JSON parsing requires RFC3339 with an explicit zone.
fn valid_rfc3339(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' { return false; }
    fn number(b: &[u8]) -> Option<u32> { if !b.iter().all(u8::is_ascii_digit) { return None; } Some(b.iter().fold(0, |n,d| n * 10 + (d-b'0') as u32)) }
    let Some(year) = number(&b[..4]) else { return false; };
    let Some(month) = number(&b[5..7]) else { return false; };
    let Some(day) = number(&b[8..10]) else { return false; };
    let Some(hour) = number(&b[11..13]) else { return false; };
    let Some(minute) = number(&b[14..16]) else { return false; };
    let Some(second) = number(&b[17..19]) else { return false; };
    let days = match month { 1|3|5|7|8|10|12 => 31, 4|6|9|11 => 30, 2 => if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {29} else {28}, _ => return false };
    if day == 0 || day > days || hour > 23 || minute > 59 || second > 59 { return false; }
    let mut i = 19;
    if b.get(i) == Some(&b'.') || b.get(i) == Some(&b',') {
        i += 1; let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) { i += 1; }
        if start == i { return false; }
    }
    if b.get(i) == Some(&b'Z') { return i + 1 == b.len(); }
    if i+6 != b.len() || !matches!(b.get(i), Some(b'+')|Some(b'-')) || b[i+3] != b':' { return false; }
    number(&b[i+1..i+3]).is_some_and(|n| n <= 24) && number(&b[i+4..i+6]).is_some_and(|n| n <= 60)
}
