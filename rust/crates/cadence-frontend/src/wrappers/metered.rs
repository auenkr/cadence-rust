//! Frontend error metering and normalization (Go: wrappers/metered).

#![allow(clippy::result_large_err)]

use cadence_api::errors::{CadenceError, InternalServiceError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Counter {
    Failures,
    BadRequest,
    ServiceBusy,
    EntityNotExists,
    ExecutionAlreadyStarted,
    WorkflowAlreadyCompleted,
    DomainAlreadyExists,
    CancellationAlreadyRequested,
    QueryFailed,
    LimitExceeded,
    ClientVersionNotSupported,
    ContextTimeout,
    ConnectionClosing,
    ServiceUnavailable,
    AuthorizationFailed,
    Unauthorized,
}

impl Counter {
    /// Metric identifier names used by the Go Frontend metered wrapper.
    pub const fn metric_name(self) -> &'static str {
        match self {
            Self::Failures => "cadence_errors",
            Self::BadRequest => "cadence_errors_bad_request",
            Self::ServiceBusy => "cadence_errors_service_busy",
            Self::EntityNotExists => "cadence_errors_entity_not_exists",
            Self::ExecutionAlreadyStarted => "cadence_errors_execution_already_started",
            Self::WorkflowAlreadyCompleted => "cadence_errors_workflow_execution_already_completed",
            Self::DomainAlreadyExists => "cadence_errors_domain_already_exists",
            Self::CancellationAlreadyRequested => "cadence_errors_cancellation_already_requested",
            Self::QueryFailed => "cadence_errors_query_failed",
            Self::LimitExceeded => "cadence_errors_limit_exceeded",
            Self::ClientVersionNotSupported => "cadence_errors_client_version_not_supported",
            Self::ContextTimeout => "cadence_errors_context_timeout",
            Self::ConnectionClosing => "cadence_errors_grpc_connection_closing",
            Self::ServiceUnavailable => "cadence_errors_service_unavailable",
            Self::AuthorizationFailed => "cadence_errors_authorize_failed",
            Self::Unauthorized => "cadence_errors_unauthorized",
        }
    }
}

pub trait Metrics {
    fn increment(&self, counter: Counter);
    fn increment_requests(&self, _operation: &str) {}
    fn record_operation(&self, _operation: &str, _elapsed: std::time::Duration, _tags: &[MetricTag]) {}
    fn record_latency_histogram(&self, _operation: &str, _elapsed: std::time::Duration) {}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricTag {
    pub key: &'static str,
    pub value: String,
}

/// Tags emitted by Go's per-operation metered wrapper helpers. Methods without
/// a helper intentionally produce an empty list.
pub fn request_tags(operation: &str, request: &serde_json::Value) -> Vec<MetricTag> {
    let request = if matches!(
        operation,
        "StartWorkflowExecutionAsync" | "SignalWithStartWorkflowExecutionAsync"
    ) {
        request.get("request").unwrap_or(request)
    } else {
        request
    };
    let mut tags = Vec::new();
    let mut add = |key, value: String| tags.push(MetricTag { key, value });
    let domain = || string_field(request, "domain");
    match operation {
        "CountWorkflowExecutions"
        | "GetTaskListsByDomain"
        | "ListArchivedWorkflowExecutions"
        | "ListClosedWorkflowExecutions"
        | "ListOpenWorkflowExecutions"
        | "ListWorkflowExecutions"
        | "ScanWorkflowExecutions" => add("wf-domain-name", domain()),
        "DescribeTaskList" => {
            add("wf-domain-name", domain());
            let task_list = request.get("taskList").unwrap_or(&serde_json::Value::Null);
            add("wf-task-list-name", string_field(task_list, "name"));
            add("wf-task-list-type", number_field(request, "taskListType"));
            add("wf-task-list-kind", number_field(task_list, "kind"));
        }
        "DescribeWorkflowExecution"
        | "DiagnoseWorkflowExecution"
        | "RequestCancelWorkflowExecution"
        | "ResetWorkflowExecution"
        | "RestartWorkflowExecution"
        | "SignalWorkflowExecution"
        | "TerminateWorkflowExecution" => {
            add("wf-domain-name", domain());
            let execution = request
                .get("workflowExecution")
                .or_else(|| request.get("execution"))
                .unwrap_or(&serde_json::Value::Null);
            add("wf-id", string_field(execution, "workflowId"));
            add("wf-run-id", string_field(execution, "runId"));
            if operation == "SignalWorkflowExecution" {
                add("wf-signal-name", string_field(request, "signalName"));
            }
        }
        "QueryWorkflow" => {
            add("wf-domain-name", domain());
            let execution = request.get("execution").unwrap_or(&serde_json::Value::Null);
            add("wf-id", string_field(execution, "workflowId"));
            add("wf-run-id", string_field(execution, "runId"));
        }
        "GetWorkflowExecutionHistory" => {
            add("wf-domain-name", domain());
            let execution = request
                .get("workflowExecution")
                .unwrap_or(&serde_json::Value::Null);
            add("wf-id", string_field(execution, "workflowId"));
            add("wf-run-id", string_field(execution, "runId"));
        }
        "RefreshWorkflowTasks" | "ResetStickyTaskList" => {
            add("wf-domain-name", domain());
            let execution = request.get("execution").unwrap_or(&serde_json::Value::Null);
            add("wf-id", string_field(execution, "workflowId"));
            add("wf-run-id", string_field(execution, "runId"));
        }
        "RecordActivityTaskHeartbeatByID"
        | "RespondActivityTaskCanceledByID"
        | "RespondActivityTaskCompletedByID"
        | "RespondActivityTaskFailedByID" => {
            add("wf-domain-name", domain());
            add("wf-id", string_field(request, "workflowId"));
            add("wf-run-id", string_field(request, "runId"));
        }
        "StartWorkflowExecution" | "StartWorkflowExecutionAsync" => {
            add("wf-domain-name", domain());
            add("wf-id", string_field(request, "workflowId"));
            add("wf-type", nested_string(request, "workflowType", "name"));
            add("wf-cron-schedule", string_field(request, "cronSchedule"));
        }
        "SignalWithStartWorkflowExecution" | "SignalWithStartWorkflowExecutionAsync" => {
            add("wf-domain-name", domain());
            add("wf-id", string_field(request, "workflowId"));
            add("wf-type", nested_string(request, "workflowType", "name"));
            add("wf-signal-name", string_field(request, "signalName"));
        }
        "ListTaskListPartitions" | "PollForActivityTask" | "PollForDecisionTask" => {
            add("wf-domain-name", domain());
            let task_list = request.get("taskList").unwrap_or(&serde_json::Value::Null);
            add("wf-task-list-name", string_field(task_list, "name"));
            add("wf-task-list-kind", number_field(task_list, "kind"));
        }
        _ => {}
    }
    tags
}

fn string_field(value: &serde_json::Value, key: &str) -> String {
    value.get(key).and_then(serde_json::Value::as_str).unwrap_or_default().to_owned()
}

fn number_field(value: &serde_json::Value, key: &str) -> String {
    value.get(key).map_or_else(String::new, |value| match value {
        serde_json::Value::Number(number) => number.to_string(),
        _ => String::new(),
    })
}

fn nested_string(value: &serde_json::Value, parent: &str, child: &str) -> String {
    value
        .get(parent)
        .map_or_else(String::new, |value| string_field(value, child))
}

/// Go forwards known typed API errors unchanged. Internal and uncategorized
/// failures become ordinary internal errors so Thrift clients do not count
/// them as user errors.
pub fn normalize_error(error: CadenceError) -> (CadenceError, Counter) {
    match error {
        CadenceError::BadRequest(_)
        | CadenceError::DomainNotActive(_) => (error, Counter::BadRequest),
        CadenceError::ServiceBusy(_) => (error, Counter::ServiceBusy),
        CadenceError::EntityNotExists(_) => (error, Counter::EntityNotExists),
        CadenceError::WorkflowAlreadyStarted(_) => (error, Counter::ExecutionAlreadyStarted),
        CadenceError::WorkflowAlreadyCompleted(_) => (error, Counter::WorkflowAlreadyCompleted),
        CadenceError::DomainAlreadyExists(_) => (error, Counter::DomainAlreadyExists),
        CadenceError::CancellationAlreadyRequested(_) => {
            (error, Counter::CancellationAlreadyRequested)
        }
        CadenceError::QueryFailed(_) => (error, Counter::QueryFailed),
        CadenceError::ClientVersionNotSupported(_) => {
            (error, Counter::ClientVersionNotSupported)
        }
        CadenceError::LimitExceeded(_) => (error, Counter::LimitExceeded),
        CadenceError::Internal(inner) => (
            CadenceError::Internal(InternalServiceError::new(format!(
                "cadence internal error, msg: {}",
                inner.message
            ))),
            Counter::Failures,
        ),
        CadenceError::AccessDenied(inner) => (
            CadenceError::Internal(InternalServiceError::new(format!(
                "cadence internal uncategorized error, msg: {inner}"
            ))),
            Counter::Failures,
        ),
        CadenceError::Unknown(message) => (
            CadenceError::Internal(InternalServiceError::new(format!(
                "cadence internal uncategorized error, msg: {message}"
            ))),
            Counter::Failures,
        ),
        other => (
            CadenceError::Internal(InternalServiceError::new(format!(
                "cadence internal uncategorized error, msg: {other}"
            ))),
            Counter::Failures,
        ),
    }
}

pub fn handle_error(error: CadenceError, metrics: &dyn Metrics) -> CadenceError {
    let (normalized, counter) = normalize_error(error);
    metrics.increment(counter);
    normalized
}

/// Run one API operation, preserving its result while emitting request timing
/// and applying Go's typed error counter/normalization behavior.
pub fn measure<T>(
    operation: &str,
    tags: &[MetricTag],
    metrics: &dyn Metrics,
    call: impl FnOnce() -> Result<T, CadenceError>,
) -> Result<T, CadenceError> {
    metrics.increment_requests(operation);
    let start = std::time::Instant::now();
    let result = call();
    let elapsed = start.elapsed();
    metrics.record_operation(operation, elapsed, tags);
    metrics.record_latency_histogram(operation, elapsed);
    result.map_err(|error| handle_error(error, metrics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Default)]
    struct RecordingMetrics(RefCell<Vec<Counter>>);
    impl Metrics for RecordingMetrics {
        fn increment(&self, counter: Counter) {
            self.0.borrow_mut().push(counter);
        }
    }

    #[test]
    fn known_service_errors_keep_type_and_use_go_metric() {
        let metrics = RecordingMetrics::default();
        let error = handle_error(
            CadenceError::ServiceBusy(cadence_api::errors::ServiceBusyError::new("busy")),
            &metrics,
        );
        assert_eq!(error.to_string(), "busy");
        assert_eq!(&*metrics.0.borrow(), &[Counter::ServiceBusy]);
        assert_eq!(Counter::ServiceBusy.metric_name(), "cadence_errors_service_busy");
    }

    #[test]
    fn internal_error_is_normalized_with_go_message_prefix() {
        let error = normalize_error(CadenceError::Internal(InternalServiceError::new("broken")));
        assert_eq!(error.0.to_string(), "cadence internal error, msg: broken");
        assert_eq!(error.1, Counter::Failures);
    }

    #[test]
    fn uncategorized_error_uses_go_fallback() {
        let (error, counter) = normalize_error(CadenceError::Unknown("oops".into()));
        assert_eq!(error.to_string(), "cadence internal uncategorized error, msg: oops");
        assert_eq!(counter, Counter::Failures);
    }

    #[test]
    fn errors_outside_go_typed_error_switch_are_normalized() {
        use cadence_api::errors::{CadenceError, ShardOwnershipLostError};
        let (normalized, counter) = normalize_error(CadenceError::ShardOwnershipLost(
            ShardOwnershipLostError::new("ownership lost"),
        ));
        assert_eq!(counter, Counter::Failures);
        assert!(matches!(normalized, CadenceError::Internal(_)));
        assert_eq!(
            normalized.to_string(),
            "cadence internal uncategorized error, msg: ownership lost"
        );
    }

    #[test]
    fn request_tags_match_go_tag_names_and_fields() {
        let tags = request_tags(
            "DescribeWorkflowExecution",
            &serde_json::json!({
                "domain": "d",
                "execution": {"workflowId": "w", "runId": "r"}
            }),
        );
        assert_eq!(
            tags,
            vec![
                MetricTag { key: "wf-domain-name", value: "d".into() },
                MetricTag { key: "wf-id", value: "w".into() },
                MetricTag { key: "wf-run-id", value: "r".into() },
            ]
        );

        let async_tags = request_tags(
            "StartWorkflowExecutionAsync",
            &serde_json::json!({
                "request": {
                    "domain": "async-domain",
                    "workflowId": "async-workflow",
                    "workflowType": {"name": "async-type"},
                    "cronSchedule": "@daily"
                }
            }),
        );
        assert_eq!(
            async_tags,
            vec![
                MetricTag { key: "wf-domain-name", value: "async-domain".into() },
                MetricTag { key: "wf-id", value: "async-workflow".into() },
                MetricTag { key: "wf-type", value: "async-type".into() },
                MetricTag { key: "wf-cron-schedule", value: "@daily".into() },
            ]
        );
    }
}
