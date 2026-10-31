//! Workflow API dispatch boundary and operation catalogue.
//!
//! Go's WorkflowHandler owns request validation, wrapper invocation and calls
//! into History, Matching, visibility and domain services. The `WorkflowBackend`
//! seam represents those outbound calls; its production implementation is
//! supplied by the frontend client/persistence integration.

#![allow(clippy::result_large_err)]

use cadence_api::errors::{BadRequestError, CadenceError};
use cadence_api::{service_types, tasks};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum WorkflowOperation {
    Health,
    DiagnoseWorkflowExecution,
    PollForActivityTask,
    PollForDecisionTask,
    RecordActivityTaskHeartbeat,
    RecordActivityTaskHeartbeatByID,
    RespondActivityTaskCompleted,
    RespondActivityTaskCompletedByID,
    RespondActivityTaskFailed,
    RespondActivityTaskFailedByID,
    RespondActivityTaskCanceled,
    RespondActivityTaskCanceledByID,
    RespondDecisionTaskCompleted,
    RespondDecisionTaskFailed,
    RespondQueryTaskCompleted,
    StartWorkflowExecution,
    StartWorkflowExecutionAsync,
    GetWorkflowExecutionHistory,
    SignalWorkflowExecution,
    SignalWithStartWorkflowExecution,
    SignalWithStartWorkflowExecutionAsync,
    TerminateWorkflowExecution,
    ResetWorkflowExecution,
    RequestCancelWorkflowExecution,
    RestartWorkflowExecution,
    GetSearchAttributes,
    QueryWorkflow,
    DescribeWorkflowExecution,
    CountWorkflowExecutions,
    ScanWorkflowExecutions,
    ListOpenWorkflowExecutions,
    ListClosedWorkflowExecutions,
    ListArchivedWorkflowExecutions,
    ListWorkflowExecutions,
    DescribeTaskList,
    ListTaskListPartitions,
    GetTaskListsByDomain,
    ResetStickyTaskList,
    RefreshWorkflowTasks,
    GetClusterInfo,
}

impl WorkflowOperation {
    pub const ALL: &'static [Self] = &[
        Self::Health,
        Self::DiagnoseWorkflowExecution,
        Self::PollForActivityTask,
        Self::PollForDecisionTask,
        Self::RecordActivityTaskHeartbeat,
        Self::RecordActivityTaskHeartbeatByID,
        Self::RespondActivityTaskCompleted,
        Self::RespondActivityTaskCompletedByID,
        Self::RespondActivityTaskFailed,
        Self::RespondActivityTaskFailedByID,
        Self::RespondActivityTaskCanceled,
        Self::RespondActivityTaskCanceledByID,
        Self::RespondDecisionTaskCompleted,
        Self::RespondDecisionTaskFailed,
        Self::RespondQueryTaskCompleted,
        Self::StartWorkflowExecution,
        Self::StartWorkflowExecutionAsync,
        Self::GetWorkflowExecutionHistory,
        Self::SignalWorkflowExecution,
        Self::SignalWithStartWorkflowExecution,
        Self::SignalWithStartWorkflowExecutionAsync,
        Self::TerminateWorkflowExecution,
        Self::ResetWorkflowExecution,
        Self::RequestCancelWorkflowExecution,
        Self::RestartWorkflowExecution,
        Self::GetSearchAttributes,
        Self::QueryWorkflow,
        Self::DescribeWorkflowExecution,
        Self::CountWorkflowExecutions,
        Self::ScanWorkflowExecutions,
        Self::ListOpenWorkflowExecutions,
        Self::ListClosedWorkflowExecutions,
        Self::ListArchivedWorkflowExecutions,
        Self::ListWorkflowExecutions,
        Self::DescribeTaskList,
        Self::ListTaskListPartitions,
        Self::GetTaskListsByDomain,
        Self::ResetStickyTaskList,
        Self::RefreshWorkflowTasks,
        Self::GetClusterInfo,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Health => "Health",
            Self::DiagnoseWorkflowExecution => "DiagnoseWorkflowExecution",
            Self::PollForActivityTask => "PollForActivityTask",
            Self::PollForDecisionTask => "PollForDecisionTask",
            Self::RecordActivityTaskHeartbeat => "RecordActivityTaskHeartbeat",
            Self::RecordActivityTaskHeartbeatByID => "RecordActivityTaskHeartbeatByID",
            Self::RespondActivityTaskCompleted => "RespondActivityTaskCompleted",
            Self::RespondActivityTaskCompletedByID => "RespondActivityTaskCompletedByID",
            Self::RespondActivityTaskFailed => "RespondActivityTaskFailed",
            Self::RespondActivityTaskFailedByID => "RespondActivityTaskFailedByID",
            Self::RespondActivityTaskCanceled => "RespondActivityTaskCanceled",
            Self::RespondActivityTaskCanceledByID => "RespondActivityTaskCanceledByID",
            Self::RespondDecisionTaskCompleted => "RespondDecisionTaskCompleted",
            Self::RespondDecisionTaskFailed => "RespondDecisionTaskFailed",
            Self::RespondQueryTaskCompleted => "RespondQueryTaskCompleted",
            Self::StartWorkflowExecution => "StartWorkflowExecution",
            Self::StartWorkflowExecutionAsync => "StartWorkflowExecutionAsync",
            Self::GetWorkflowExecutionHistory => "GetWorkflowExecutionHistory",
            Self::SignalWorkflowExecution => "SignalWorkflowExecution",
            Self::SignalWithStartWorkflowExecution => "SignalWithStartWorkflowExecution",
            Self::SignalWithStartWorkflowExecutionAsync => "SignalWithStartWorkflowExecutionAsync",
            Self::TerminateWorkflowExecution => "TerminateWorkflowExecution",
            Self::ResetWorkflowExecution => "ResetWorkflowExecution",
            Self::RequestCancelWorkflowExecution => "RequestCancelWorkflowExecution",
            Self::RestartWorkflowExecution => "RestartWorkflowExecution",
            Self::GetSearchAttributes => "GetSearchAttributes",
            Self::QueryWorkflow => "QueryWorkflow",
            Self::DescribeWorkflowExecution => "DescribeWorkflowExecution",
            Self::CountWorkflowExecutions => "CountWorkflowExecutions",
            Self::ScanWorkflowExecutions => "ScanWorkflowExecutions",
            Self::ListOpenWorkflowExecutions => "ListOpenWorkflowExecutions",
            Self::ListClosedWorkflowExecutions => "ListClosedWorkflowExecutions",
            Self::ListArchivedWorkflowExecutions => "ListArchivedWorkflowExecutions",
            Self::ListWorkflowExecutions => "ListWorkflowExecutions",
            Self::DescribeTaskList => "DescribeTaskList",
            Self::ListTaskListPartitions => "ListTaskListPartitions",
            Self::GetTaskListsByDomain => "GetTaskListsByDomain",
            Self::ResetStickyTaskList => "ResetStickyTaskList",
            Self::RefreshWorkflowTasks => "RefreshWorkflowTasks",
            Self::GetClusterInfo => "GetClusterInfo",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|operation| operation.name() == name)
    }
}

/// The client layer must implement Go's existing service boundaries, including
/// mapping History/Matching errors without changing their Cadence error types.
pub trait WorkflowBackend: Send + Sync {
    fn invoke<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, CadenceError>> + Send + 'a>>;
}

/// Temporary compatibility constructor retained for the independent domain,
/// schedule and admin skeletons. Workflow API operations use `WorkflowBackend`
/// and do not call this stub path.
pub fn unimplemented_operation(operation: &str) -> cadence_api::errors::InternalServiceError {
    cadence_api::errors::InternalServiceError::new(format!(
        "Frontend operation {operation} is not implemented"
    ))
}

pub struct Handler<B> {
    backend: B,
    validator: crate::request_validator::RequestValidator,
    metrics: Arc<dyn crate::wrappers::metered::Metrics + Send + Sync>,
}

#[derive(Default)]
struct NoopWorkflowMetrics;

impl crate::wrappers::metered::Metrics for NoopWorkflowMetrics {
    fn increment(&self, _: crate::wrappers::metered::Counter) {}
}

impl<B: WorkflowBackend> Handler<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            validator: crate::request_validator::RequestValidator::default(),
            metrics: Arc::new(NoopWorkflowMetrics),
        }
    }

    pub fn with_validator(
        backend: B,
        validator: crate::request_validator::RequestValidator,
    ) -> Self {
        Self {
            backend,
            validator,
            metrics: Arc::new(NoopWorkflowMetrics),
        }
    }

    /// Construct a handler with the Frontend metered-wrapper sink. The
    /// middleware is outside validation and backend dispatch, matching Go's
    /// generated wrapper order: validation/backend errors are normalized and
    /// counted at the API boundary.
    pub fn with_metrics(
        backend: B,
        validator: crate::request_validator::RequestValidator,
        metrics: Arc<dyn crate::wrappers::metered::Metrics + Send + Sync>,
    ) -> Self {
        Self {
            backend,
            validator,
            metrics,
        }
    }

    pub async fn dispatch(&self, operation: &str, request: Value) -> Result<Value, CadenceError> {
        let operation = WorkflowOperation::from_name(operation).ok_or_else(|| {
            CadenceError::BadRequest(BadRequestError::new(format!(
                "unknown workflow operation: {operation}"
            )))
        })?;
        let start = std::time::Instant::now();
        self.metrics.increment_requests(operation.name());
        let tags = crate::wrappers::metered::request_tags(operation.name(), &request);
        let result = self.dispatch_operation(operation, request).await;
        let elapsed = start.elapsed();
        self.metrics
            .record_operation(operation.name(), elapsed, &tags);
        self.metrics
            .record_latency_histogram(operation.name(), elapsed);
        match result {
            Ok(response) => Ok(response),
            Err(error) => {
                let (error, counter) = crate::wrappers::metered::normalize_error(error);
                self.metrics.increment(counter);
                Err(error)
            }
        }
    }

    async fn dispatch_operation(
        &self,
        operation: WorkflowOperation,
        request: Value,
    ) -> Result<Value, CadenceError> {
        validate_ratelimit_wrapper_request(operation, &request)?;
        let request = self.validate_request(operation, request)?;
        self.backend.invoke(operation, request).await
    }

    fn validate_request(
        &self,
        operation: WorkflowOperation,
        value: Value,
    ) -> Result<Value, CadenceError> {
        if matches!(
            operation,
            WorkflowOperation::StartWorkflowExecution
                | WorkflowOperation::StartWorkflowExecutionAsync
                | WorkflowOperation::SignalWithStartWorkflowExecution
                | WorkflowOperation::SignalWithStartWorkflowExecutionAsync
        ) {
            let request_value = if matches!(
                operation,
                WorkflowOperation::StartWorkflowExecutionAsync
                    | WorkflowOperation::SignalWithStartWorkflowExecutionAsync
            ) {
                value.get("request").cloned().unwrap_or(Value::Null)
            } else {
                value.clone()
            };
            if request_value.is_null() {
                return Err(cadence_api::validation::err_request_not_set().into());
            }
            let request: cadence_api::types::StartWorkflowExecutionRequest =
                serde_json::from_value(request_value.clone()).map_err(|error| {
                    CadenceError::BadRequest(BadRequestError::new(error.to_string()))
                })?;
            cadence_api::validation::validate_start_workflow_execution_request(
                Some(&request),
                &cadence_api::validation::Limits::default(),
            )?;
            if matches!(
                operation,
                WorkflowOperation::SignalWithStartWorkflowExecution
                    | WorkflowOperation::SignalWithStartWorkflowExecutionAsync
            ) && request_value
                .get("signalName")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(cadence_api::validation::err_signal_name_not_set().into());
            }
            return Ok(value);
        }
        if operation == WorkflowOperation::GetWorkflowExecutionHistory {
            if value.is_null() {
                return Err(cadence_api::validation::err_request_not_set().into());
            }
            let request: cadence_api::service_types::GetWorkflowExecutionHistoryRequest =
                serde_json::from_value(value.clone()).map_err(|error| {
                    CadenceError::BadRequest(BadRequestError::new(error.to_string()))
                })?;
            if request.domain.is_empty() {
                return Err(CadenceError::BadRequest(BadRequestError::new(
                    "Domain not set on request.",
                )));
            }
            cadence_api::validation::check_execution(request.execution.as_ref())?;
            if !request.next_page_token.is_empty() {
                let token = crate::handler_workflow_history::deserialize_history_token(
                    &request.next_page_token,
                )
                .map_err(|_| {
                    CadenceError::BadRequest(BadRequestError::new(
                        "Invalid NextPageToken.",
                    ))
                })?;
                crate::handler_workflow_history::validate_history_run_id(
                    request
                        .execution
                        .as_ref()
                        .map_or("", |execution| execution.run_id.as_str()),
                    &token,
                )?;
            }
            return Ok(value);
        }
        if matches!(
            operation,
            WorkflowOperation::QueryWorkflow
                | WorkflowOperation::DiagnoseWorkflowExecution
                | WorkflowOperation::DescribeWorkflowExecution
                | WorkflowOperation::TerminateWorkflowExecution
                | WorkflowOperation::ResetWorkflowExecution
                | WorkflowOperation::RequestCancelWorkflowExecution
                | WorkflowOperation::RestartWorkflowExecution
                | WorkflowOperation::SignalWorkflowExecution
        ) {
            validate_domain(&value)?;
            let field = if operation == WorkflowOperation::QueryWorkflow {
                "execution"
            } else {
                "workflowExecution"
            };
            if operation == WorkflowOperation::DiagnoseWorkflowExecution {
                let execution = value.get(field);
                let workflow_id = execution
                    .and_then(|execution| execution.get("workflowId"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let run_id = execution
                    .and_then(|execution| execution.get("runId"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if workflow_id.is_empty() || run_id.is_empty() {
                    return Err(CadenceError::BadRequest(BadRequestError::new(
                        "Execution is not set on request.",
                    )));
                }
            } else {
                validate_execution_field(&value, field)?;
            }
            if operation == WorkflowOperation::QueryWorkflow {
                let query = value.get("query").filter(|query| !query.is_null());
                let Some(query) = query else {
                    return Err(CadenceError::BadRequest(BadRequestError::new(
                        "WorkflowQuery is not set on request.",
                    )));
                };
                if query
                    .get("queryType")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .is_empty()
                {
                    return Err(CadenceError::BadRequest(BadRequestError::new(
                        "QueryType is not set on request.",
                    )));
                }
            }
            if operation == WorkflowOperation::SignalWorkflowExecution
                && value
                    .get("signalName")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .is_empty()
            {
                return Err(cadence_api::validation::err_signal_name_not_set().into());
            }
        }
        if operation == WorkflowOperation::RecordActivityTaskHeartbeatByID {
            if value.is_null() {
                return Err(cadence_api::validation::err_request_not_set().into());
            }
            validate_domain(&value)?;
            for (field, message) in [
                ("workflowId", "WorkflowId is not set on request."),
                ("activityId", "ActivityID is not set on request."),
            ] {
                if value
                    .get(field)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .is_empty()
                {
                    return Err(CadenceError::BadRequest(BadRequestError::new(message)));
                }
            }
        }
        if operation == WorkflowOperation::RecordActivityTaskHeartbeat {
            if value.is_null() {
                return Err(cadence_api::validation::err_request_not_set().into());
            }
            if value.get("taskToken").is_none_or(Value::is_null) {
                return Err(CadenceError::BadRequest(BadRequestError::new(
                    "Task token not set on request.",
                )));
            }
        }
        match operation {
            WorkflowOperation::CountWorkflowExecutions => {
                let request: service_types::CountWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator.validate_count_workflow_executions_request(Some(&request))?;
            }
            WorkflowOperation::ScanWorkflowExecutions | WorkflowOperation::ListWorkflowExecutions => {
                let mut request: service_types::ListWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator.validate_list_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "pageSize", request.page_size);
            }
            WorkflowOperation::ListOpenWorkflowExecutions => {
                let mut request: service_types::ListOpenWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator.validate_list_open_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "maximumPageSize", request.maximum_page_size);
            }
            WorkflowOperation::ListClosedWorkflowExecutions => {
                let mut request: service_types::ListClosedWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator.validate_list_closed_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "maximumPageSize", request.maximum_page_size);
            }
            WorkflowOperation::ListArchivedWorkflowExecutions => {
                let mut request: service_types::ListArchivedWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator.validate_list_archived_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "pageSize", request.page_size);
            }
            WorkflowOperation::DescribeTaskList => {
                let request: tasks::DescribeTaskListRequest = parse_request(value.clone())?;
                self.validator.validate_describe_task_list_request(Some(&request))?;
            }
            WorkflowOperation::ListTaskListPartitions => {
                let request: tasks::ListTaskListPartitionsRequest = parse_request(value.clone())?;
                self.validator.validate_list_task_list_partitions_request(Some(&request))?;
            }
            WorkflowOperation::GetTaskListsByDomain => {
                let request: tasks::GetTaskListsByDomainRequest = parse_request(value.clone())?;
                self.validator.validate_get_task_lists_by_domain_request(Some(&request))?;
            }
            WorkflowOperation::ResetStickyTaskList => {
                let request: service_types::ResetStickyTaskListRequest = parse_request(value.clone())?;
                self.validator.validate_reset_sticky_task_list_request(Some(&request))?;
            }
            WorkflowOperation::RefreshWorkflowTasks => {
                let request: cadence_api::types::RefreshWorkflowTasksRequest = parse_request(value.clone())?;
                self.validator.validate_refresh_workflow_tasks_request(Some(&request))?;
            }
            _ => {}
        }
        Ok(value)
    }
}

fn parse_request<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, CadenceError> {
    serde_json::from_value(value)
        .map_err(|error| CadenceError::BadRequest(BadRequestError::new(error.to_string())))
}

fn set_i32_field(value: &mut Value, field: &str, number: i32) {
    if let Some(object) = value.as_object_mut() {
        object.insert(field.to_owned(), Value::from(number));
    }
}

/// Match the generated Go rate-limit wrapper's preflight checks. The quota
/// policy itself is injected by the production decorator; these request
/// checks run before that policy and before the workflow handler.
fn validate_ratelimit_wrapper_request(
    operation: WorkflowOperation,
    request: &Value,
) -> Result<(), CadenceError> {
    let Some(rule) = crate::wrappers::ratelimited::operation_rule(operation.name()) else {
        return Ok(());
    };
    if request.is_null() {
        return Err(cadence_api::validation::err_request_not_set().into());
    }
    use crate::wrappers::ratelimited::RequestClass;
    if rule.class != RequestClass::Worker {
        let domain = if matches!(
            operation,
            WorkflowOperation::StartWorkflowExecutionAsync
                | WorkflowOperation::SignalWithStartWorkflowExecutionAsync
        ) {
            request.get("request").unwrap_or(request)
        } else {
            request
        };
        if domain
            .get("domain")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
        {
            return Err(cadence_api::validation::err_domain_not_set().into());
        }
        return Ok(());
    }

    match operation {
        WorkflowOperation::ResetStickyTaskList
        | WorkflowOperation::RecordActivityTaskHeartbeatByID
        | WorkflowOperation::RespondActivityTaskCanceledByID
        | WorkflowOperation::RespondActivityTaskCompletedByID
        | WorkflowOperation::RespondActivityTaskFailedByID => {
            if request
                .get("domain")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(cadence_api::validation::err_domain_not_set().into());
            }
        }
        _ if request.get("taskToken").is_none_or(Value::is_null) => {
            return Err(CadenceError::BadRequest(BadRequestError::new(
                "Task token not set on request.",
            )));
        }
        _ => {}
    }
    Ok(())
}

fn validate_domain(request: &Value) -> Result<(), CadenceError> {
    if request.is_null() {
        return Err(cadence_api::validation::err_request_not_set().into());
    }
    if request
        .get("domain")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .is_empty()
    {
        return Err(cadence_api::validation::err_domain_not_set().into());
    }
    Ok(())
}

fn validate_execution_field(request: &Value, field: &str) -> Result<(), CadenceError> {
    let execution = request
        .get(field)
        .cloned()
        .map(serde_json::from_value::<cadence_api::types::WorkflowExecution>)
        .transpose()
        .map_err(|error| CadenceError::BadRequest(BadRequestError::new(error.to_string())))?;
    cadence_api::validation::check_execution(execution.as_ref())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingMetrics {
        requests: Mutex<Vec<String>>,
        counters: Mutex<Vec<crate::wrappers::metered::Counter>>,
        operations: Mutex<Vec<(String, Vec<crate::wrappers::metered::MetricTag>)>>,
        latency_samples: Mutex<Vec<String>>,
    }

    impl crate::wrappers::metered::Metrics for RecordingMetrics {
        fn increment(&self, counter: crate::wrappers::metered::Counter) {
            self.counters.lock().unwrap().push(counter);
        }

        fn increment_requests(&self, operation: &str) {
            self.requests.lock().unwrap().push(operation.to_owned());
        }

        fn record_operation(
            &self,
            operation: &str,
            _: std::time::Duration,
            tags: &[crate::wrappers::metered::MetricTag],
        ) {
            self.operations
                .lock()
                .unwrap()
                .push((operation.to_owned(), tags.to_vec()));
        }

        fn record_latency_histogram(&self, operation: &str, _: std::time::Duration) {
            self.latency_samples.lock().unwrap().push(operation.to_owned());
        }
    }

    #[derive(Default)]
    struct FakeBackend(Mutex<Vec<WorkflowOperation>>);

    struct FailingBackend(&'static str);

    impl WorkflowBackend for FailingBackend {
        fn invoke<'a>(
            &'a self,
            _: WorkflowOperation,
            _: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, CadenceError>> + Send + 'a>> {
            Box::pin(async move { Err(CadenceError::Unknown(self.0.to_owned())) })
        }
    }

    #[derive(serde::Serialize)]
    struct EncodedHistoryToken {
        #[serde(
            rename = "nextPageToken",
            serialize_with = "cadence_api::b64::serialize_bytes"
        )]
        next_page_token: Vec<u8>,
    }

    impl WorkflowBackend for FakeBackend {
        fn invoke<'a>(
            &'a self,
            operation: WorkflowOperation,
            request: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, CadenceError>> + Send + 'a>> {
            Box::pin(async move {
                self.0.lock().unwrap().push(operation);
                Ok(request)
            })
        }
    }

    #[test]
    fn operation_names_round_trip_and_include_list_tasklist_surface() {
        assert_eq!(WorkflowOperation::ALL.len(), 40);
        for operation in WorkflowOperation::ALL {
            assert_eq!(WorkflowOperation::from_name(operation.name()), Some(*operation));
        }
        assert!(WorkflowOperation::from_name("CountWorkflowExecutions").is_some());
        assert!(WorkflowOperation::from_name("ScanWorkflowExecutions").is_some());
        assert!(WorkflowOperation::from_name("GetTaskListsByDomain").is_some());
    }

    #[tokio::test]
    async fn dispatch_preserves_request_and_backend_errors() {
        let backend = FakeBackend::default();
        let handler = Handler::new(backend);
        let request = serde_json::json!({
            "domain": "sample",
            "workflowId": "wf",
            "workflowType": {"name": "type"},
            "taskList": {"name": "tasks"},
            "requestId": "550e8400-e29b-41d4-a716-446655440000",
            "executionStartToCloseTimeoutSeconds": 60,
            "taskStartToCloseTimeoutSeconds": 10,
            "futureSdkField": "preserved"
        });
        assert_eq!(
            handler.dispatch("StartWorkflowExecution", request.clone()).await.unwrap(),
            request
        );
        assert_eq!(
            handler.backend.0.lock().unwrap().as_slice(),
            &[WorkflowOperation::StartWorkflowExecution]
        );
        assert!(handler.dispatch("NotAnApi", Value::Null).await.is_err());
    }

    #[tokio::test]
    async fn dispatch_runs_go_metered_wrapper_around_validation_and_backend() {
        let metrics = Arc::new(RecordingMetrics::default());
        let handler = Handler::with_metrics(
            FakeBackend::default(),
            crate::request_validator::RequestValidator::default(),
            metrics.clone(),
        );
        let error = handler
            .dispatch("DescribeWorkflowExecution", serde_json::json!({}))
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "Domain not set on request.");
        assert_eq!(*metrics.requests.lock().unwrap(), ["DescribeWorkflowExecution"]);
        assert_eq!(
            *metrics.counters.lock().unwrap(),
            [crate::wrappers::metered::Counter::BadRequest]
        );
        assert_eq!(
            metrics.operations.lock().unwrap()[0].0,
            "DescribeWorkflowExecution"
        );
        assert_eq!(
            metrics.operations.lock().unwrap()[0].1,
            crate::wrappers::metered::request_tags(
                "DescribeWorkflowExecution",
                &serde_json::json!({})
            )
        );
        assert_eq!(
            *metrics.latency_samples.lock().unwrap(),
            ["DescribeWorkflowExecution"]
        );
    }

    #[tokio::test]
    async fn metered_wrapper_normalizes_uncategorized_backend_errors() {
        let metrics = Arc::new(RecordingMetrics::default());
        let handler = Handler::with_metrics(
            FailingBackend("backend broke"),
            crate::request_validator::RequestValidator::default(),
            metrics.clone(),
        );
        let error = handler
            .dispatch("Health", Value::Null)
            .await
            .unwrap_err();

        assert!(matches!(error, CadenceError::Internal(_)));
        assert_eq!(
            error.to_string(),
            "cadence internal uncategorized error, msg: backend broke"
        );
        assert_eq!(
            *metrics.counters.lock().unwrap(),
            [crate::wrappers::metered::Counter::Failures]
        );
    }

    #[tokio::test]
    async fn workflow_start_validates_before_calling_backend() {
        let backend = FakeBackend::default();
        let handler = Handler::new(backend);
        let error = handler
            .dispatch("StartWorkflowExecution", serde_json::json!({"domain": "d"}))
            .await
            .unwrap_err();
        assert!(matches!(error, CadenceError::BadRequest(_)));
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn asynchronous_start_validates_embedded_request_before_backend() {
        let backend = FakeBackend::default();
        let handler = Handler::new(backend);
        let error = handler
            .dispatch(
                "StartWorkflowExecutionAsync",
                serde_json::json!({"request": {"domain": "d"}}),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CadenceError::BadRequest(_)));
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn signal_and_describe_validate_in_go_order_before_backend() {
        let handler = Handler::new(FakeBackend::default());
        let missing_domain = handler
            .dispatch("DescribeWorkflowExecution", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(missing_domain.to_string(), "Domain not set on request.");

        let missing_signal = handler
            .dispatch(
                "SignalWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {"workflowId": "wf"}
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(missing_signal.to_string(), "SignalName is not set on request.");
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn query_requires_query_and_query_type_after_execution_validation() {
        let handler = Handler::new(FakeBackend::default());
        let request = serde_json::json!({
            "domain": "shop",
            "execution": {"workflowId": "checkout"}
        });
        assert_eq!(
            handler.dispatch("QueryWorkflow", request.clone()).await.unwrap_err().to_string(),
            "WorkflowQuery is not set on request."
        );
        let request = serde_json::json!({
            "domain": "shop",
            "execution": {"workflowId": "checkout"},
            "query": {"queryArgs": ""}
        });
        assert_eq!(
            handler.dispatch("QueryWorkflow", request).await.unwrap_err().to_string(),
            "QueryType is not set on request."
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn diagnose_requires_complete_execution_before_backend() {
        let handler = Handler::new(FakeBackend::default());
        for execution in [
            serde_json::json!({"workflowId": "checkout"}),
            serde_json::json!({"runId": "run-1"}),
        ] {
            let error = handler
                .dispatch(
                    "DiagnoseWorkflowExecution",
                    serde_json::json!({"domain": "shop", "workflowExecution": execution}),
                )
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), "Execution is not set on request.");
        }
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn generated_ratelimit_wrapper_checks_domain_and_worker_token_first() {
        let handler = Handler::new(FakeBackend::default());
        assert_eq!(
            handler
                .dispatch("GetTaskListsByDomain", serde_json::json!({}))
                .await
                .unwrap_err()
                .to_string(),
            "Domain not set on request."
        );
        assert_eq!(
            handler
                .dispatch("RespondActivityTaskCompleted", serde_json::json!({}))
                .await
                .unwrap_err()
                .to_string(),
            "Task token not set on request."
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn visibility_validator_defaults_page_size_without_dropping_unknown_fields() {
        let handler = Handler::new(FakeBackend::default());
        let response = handler
            .dispatch(
                "ListWorkflowExecutions",
                serde_json::json!({
                    "domain": "shop",
                    "pageSize": 0,
                    "nextPageToken": "",
                    "query": "WorkflowType='order'",
                    "futureSdkField": "preserved"
                }),
            )
            .await
            .unwrap();
        assert_eq!(response["pageSize"], 1000);
        assert_eq!(response["futureSdkField"], "preserved");
    }

    #[tokio::test]
    async fn visibility_and_task_list_validation_fail_before_backend() {
        let handler = Handler::new(FakeBackend::default());
        let filter_error = handler
            .dispatch(
                "ListOpenWorkflowExecutions",
                serde_json::json!({"domain": "shop"}),
            )
            .await
            .unwrap_err();
        assert_eq!(filter_error.to_string(), "StartTimeFilter is required");

        let task_list_error = handler
            .dispatch(
                "DescribeTaskList",
                serde_json::json!({"domain": "shop", "taskListType": 0}),
            )
            .await
            .unwrap_err();
        assert_eq!(task_list_error.to_string(), "TaskList is not set on request.");
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn history_token_errors_match_go_before_backend() {
        let handler = Handler::new(FakeBackend::default());
        let invalid = handler
            .dispatch(
                "GetWorkflowExecutionHistory",
                serde_json::json!({
                    "domain": "shop",
                    "execution": {"workflowId": "checkout"},
                    "nextPageToken": "e30="
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(invalid.to_string(), "Invalid NextPageToken.");

        let token = crate::handler_workflow_history::HistoryContinuationToken {
            run_id: "token-run".into(),
            ..Default::default()
        };
        let bytes = crate::handler_workflow_history::serialize_history_token(Some(&token))
            .unwrap()
            .unwrap();
        let encoded = serde_json::to_value(EncodedHistoryToken {
            next_page_token: bytes,
        })
        .unwrap()["nextPageToken"]
            .as_str()
            .unwrap()
            .to_owned();
        let mismatch = handler
            .dispatch(
                "GetWorkflowExecutionHistory",
                serde_json::json!({
                    "domain": "shop",
                    "execution": {"workflowId": "checkout", "runId": "request-run"},
                    "nextPageToken": encoded
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(
            mismatch.to_string(),
            "RunID in the request does not match the NextPageToken."
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }
}
