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
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum HealthStatus {
    Ok = 1,
    WarmingUp = 2,
    ShuttingDown = 3,
}

impl HealthStatus {
    const fn message(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::WarmingUp => "WarmingUp",
            Self::ShuttingDown => "ShuttingDown",
        }
    }

    fn from_byte(value: u8) -> Self {
        match value {
            1 => Self::Ok,
            3 => Self::ShuttingDown,
            _ => Self::WarmingUp,
        }
    }
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
        Self::ALL
            .iter()
            .copied()
            .find(|operation| operation.name() == name)
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

    /// Forward a call to a selected Frontend cluster. Backends that do not
    /// support cross-cluster calls retain local behavior; production clients
    /// should attach Go's autoforwarding cluster header here.
    fn invoke_on_cluster<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: Value,
        _target_cluster: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, CadenceError>> + Send + 'a>> {
        self.invoke(operation, request)
    }
}

/// Synchronous name-to-UUID lookup supplied by Frontend's domain cache. Go's
/// cache is local and synchronous from the handler's point of view; keeping
/// this seam separate lets the RPC and persistence adapters share it.
pub trait WorkflowDomainLookup: Send + Sync {
    fn domain_id(&self, domain: &str) -> Result<String, CadenceError>;
}

pub type WorkflowFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, CadenceError>> + Send + 'a>>;
pub type WorkflowNext<'a> = Box<dyn FnMut(Option<String>) -> WorkflowFuture<'a> + Send + 'a>;

/// Outer cluster-redirection decorator. It can invoke `next` on the selected
/// cluster and retry once on the active cluster after DomainNotActive, while
/// keeping each attempt inside metering/rate/version handling.
pub trait RequestRedirector: Send + Sync {
    fn invoke<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: &'a Value,
        next: WorkflowNext<'a>,
    ) -> WorkflowFuture<'a>;
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
    history_max_page_size: i32,
    history_page_size_policy: Option<Arc<dyn HistoryPageSizePolicy>>,
    metrics: Arc<dyn crate::wrappers::metered::Metrics + Send + Sync>,
    access_control: Option<Arc<dyn RequestAuthorization>>,
    redirector: Option<Arc<dyn RequestRedirector>>,
    version_check: Option<VersionCheck>,
    rate_limit: Option<Arc<dyn RequestRateLimit>>,
    visibility_query_validator: Option<Arc<dyn VisibilityQueryValidator>>,
    visibility_filter_policy: Option<Arc<dyn VisibilityFilterPolicy>>,
    workflow_query_policy: Option<Arc<dyn WorkflowQueryPolicy>>,
    isolation_group_state: Option<Arc<dyn IsolationGroupState>>,
    async_publisher: Option<Arc<dyn AsyncWorkflowPublisher>>,
    domain_lookup: Option<Arc<dyn WorkflowDomainLookup>>,
    search_attributes: Option<Arc<dyn SearchAttributeProvider>>,
    shutting_down: Arc<AtomicBool>,
    health_status: Arc<AtomicU8>,
}

/// Caller metadata carried by the transport into the same wrapper layers that
/// Go reads from its request context. The plain `dispatch` entry point uses
/// empty metadata for internal callers; transports should call
/// `dispatch_with_metadata` with the received identity and version headers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CallerMetadata {
    pub actor: String,
    pub client_impl: String,
    pub feature_version: String,
    /// Isolation group carried by Cadence's request context.
    pub isolation_group: String,
    /// Original transport headers copied into Go's async queue message.
    pub original_headers: HashMap<String, String>,
}

/// Go checks task-list isolation health for selected workflow operations.
/// The implementation owns both the dynamic enable flag and the state lookup.
pub trait IsolationGroupState: Send + Sync {
    fn is_drained(&self, domain: &str, isolation_group: &str) -> Result<bool, String>;
}

/// Runtime queue integration for the two Go asynchronous workflow methods.
/// It selects the producer by domain and publishes the already encoded
/// ThriftRW envelope and original request headers.
pub trait AsyncWorkflowPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        message: crate::handler_workflow_async::AsyncWorkflowMessage,
    ) -> WorkflowFuture<'a>;
}

#[derive(serde::Serialize)]
struct DiagnosticsStarterWorkflowInput<'a> {
    #[serde(rename = "Domain")]
    domain: &'a str,
    #[serde(rename = "Identity")]
    identity: &'a str,
    #[serde(rename = "WorkflowID")]
    workflow_id: &'a str,
    #[serde(rename = "RunID")]
    run_id: &'a str,
}

pub trait RequestAuthorization: Send + Sync {
    fn authorize(&self, operation: &str, request: &Value, actor: &str) -> Result<(), CadenceError>;
}

pub trait RequestRateLimit: Send + Sync {
    fn allow(&self, operation: &str, request: &Value) -> Result<(), CadenceError>;
}

/// Go's visibility query validator is independent of request validation: it
/// needs the configured visibility dialect and registered search attributes.
pub trait VisibilityQueryValidator: Send + Sync {
    fn validate_query(&self, query: &str) -> Result<String, CadenceError>;
}

/// Go's `DisableListVisibilityByFilter` dynamic property is evaluated for
/// every open/closed visibility request that selects a workflow, type, or
/// status filter. The policy is injected because it depends on live config.
pub trait VisibilityFilterPolicy: Send + Sync {
    fn filters_disabled(&self, domain: &str) -> bool;
}

/// Go resolves `HistoryMaxPageSize` from dynamic config for each request's
/// domain. The static builder value remains a fallback for tests and bootstrap
/// callers that have not installed the live Frontend config provider.
pub trait HistoryPageSizePolicy: Send + Sync {
    fn maximum_page_size(&self, domain: &str) -> i32;
}

/// Dynamic per-domain `DisallowQuery` check performed by Go before it checks
/// the Query payload itself.
pub trait WorkflowQueryPolicy: Send + Sync {
    fn query_disallowed(&self, domain: &str) -> bool;
}

/// Live `frontend.validSearchAttributes` property used by Go's local
/// GetSearchAttributes handler and visibility validators.
pub trait SearchAttributeProvider: Send + Sync {
    fn valid_search_attributes(&self) -> HashMap<String, cadence_api::enums::IndexedValueType>;
}

#[derive(Clone)]
struct VersionCheck {
    enabled: bool,
    checker: Arc<dyn crate::wrappers::versioncheck::VersionChecker + Send + Sync>,
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
            history_max_page_size: 1000,
            history_page_size_policy: None,
            metrics: Arc::new(NoopWorkflowMetrics),
            access_control: None,
            redirector: None,
            version_check: None,
            rate_limit: None,
            visibility_query_validator: None,
            visibility_filter_policy: None,
            workflow_query_policy: None,
            isolation_group_state: None,
            async_publisher: None,
            domain_lookup: None,
            search_attributes: None,
            shutting_down: Arc::new(AtomicBool::new(false)),
            health_status: Arc::new(AtomicU8::new(HealthStatus::WarmingUp as u8)),
        }
    }

    pub fn with_validator(
        backend: B,
        validator: crate::request_validator::RequestValidator,
    ) -> Self {
        Self {
            backend,
            validator,
            history_max_page_size: 1000,
            history_page_size_policy: None,
            metrics: Arc::new(NoopWorkflowMetrics),
            access_control: None,
            redirector: None,
            version_check: None,
            rate_limit: None,
            visibility_query_validator: None,
            visibility_filter_policy: None,
            workflow_query_policy: None,
            isolation_group_state: None,
            async_publisher: None,
            domain_lookup: None,
            search_attributes: None,
            shutting_down: Arc::new(AtomicBool::new(false)),
            health_status: Arc::new(AtomicU8::new(HealthStatus::WarmingUp as u8)),
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
            history_max_page_size: 1000,
            history_page_size_policy: None,
            metrics,
            access_control: None,
            redirector: None,
            version_check: None,
            rate_limit: None,
            visibility_query_validator: None,
            visibility_filter_policy: None,
            workflow_query_policy: None,
            isolation_group_state: None,
            async_publisher: None,
            domain_lookup: None,
            search_attributes: None,
            shutting_down: Arc::new(AtomicBool::new(false)),
            health_status: Arc::new(AtomicU8::new(HealthStatus::WarmingUp as u8)),
        }
    }

    /// Install the Go access-control decorator. This check intentionally runs
    /// outside metering, matching `accesscontrolled.NewAPIHandler` being the
    /// outermost Go handler decorator.
    pub fn with_access_control(mut self, authorizer: Arc<dyn RequestAuthorization>) -> Self {
        self.access_control = Some(authorizer);
        self
    }

    /// Install Go's cluster redirection decorator outside metering. The
    /// redirector may retry the same operation after a DomainNotActive error;
    /// each selected attempt remains metered independently.
    pub fn with_cluster_redirection(mut self, redirector: Arc<dyn RequestRedirector>) -> Self {
        self.redirector = Some(redirector);
        self
    }

    /// Install the Go version-check decorator. It runs after rate-limit
    /// request preflight and before the API handler, inside metering.
    pub fn with_version_check(
        mut self,
        enabled: bool,
        checker: Arc<dyn crate::wrappers::versioncheck::VersionChecker + Send + Sync>,
    ) -> Self {
        self.version_check = Some(VersionCheck { enabled, checker });
        self
    }

    /// Install the quota policy used by the generated rate-limited wrapper.
    /// The request-shape checks still run first, as they do in Go.
    pub fn with_rate_limit(mut self, rate_limit: Arc<dyn RequestRateLimit>) -> Self {
        self.rate_limit = Some(rate_limit);
        self
    }

    /// Attach the generated Go quota implementation and its domain/token
    /// dimension resolver as one middleware component.
    pub fn with_rate_limit_policies(
        self,
        rate_limit: crate::wrappers::ratelimited::HandlerRateLimit,
    ) -> Self {
        self.with_rate_limit(Arc::new(rate_limit))
    }

    /// Set the domain-filtered Go `frontend.historyMaxPageSize` value used
    /// when GetWorkflowExecutionHistory omits a positive page size.
    pub fn with_history_max_page_size(mut self, maximum: i32) -> Self {
        self.history_max_page_size = maximum;
        self
    }

    pub fn with_history_page_size_policy(mut self, policy: Arc<dyn HistoryPageSizePolicy>) -> Self {
        self.history_page_size_policy = Some(policy);
        self
    }

    pub fn with_visibility_query_validator(
        mut self,
        validator: Arc<dyn VisibilityQueryValidator>,
    ) -> Self {
        self.visibility_query_validator = Some(validator);
        self
    }

    pub fn with_visibility_filter_policy(
        mut self,
        policy: Arc<dyn VisibilityFilterPolicy>,
    ) -> Self {
        self.visibility_filter_policy = Some(policy);
        self
    }

    pub fn with_workflow_query_policy(mut self, policy: Arc<dyn WorkflowQueryPolicy>) -> Self {
        self.workflow_query_policy = Some(policy);
        self
    }

    pub fn with_isolation_group_state(mut self, state: Arc<dyn IsolationGroupState>) -> Self {
        self.isolation_group_state = Some(state);
        self
    }

    pub fn with_async_publisher(mut self, publisher: Arc<dyn AsyncWorkflowPublisher>) -> Self {
        self.async_publisher = Some(publisher);
        self
    }

    /// Install the domain-cache lookup used to build Go's internal History,
    /// Matching and visibility requests. Public requests continue to carry
    /// domain names; only the outbound service request receives the UUID.
    pub fn with_domain_lookup(mut self, lookup: Arc<dyn WorkflowDomainLookup>) -> Self {
        self.domain_lookup = Some(lookup);
        self
    }

    pub fn with_search_attribute_provider(
        mut self,
        provider: Arc<dyn SearchAttributeProvider>,
    ) -> Self {
        self.search_attributes = Some(provider);
        self
    }

    /// Match `WorkflowHandler.Stop`: subsequent workflow operations return
    /// the shared Frontend shutting-down error. Health remains available so
    /// peers can observe service state during the drain window.
    pub fn stop(&self) {
        self.shutting_down.store(true, Ordering::Release);
    }

    /// Start the API health warmup timer. Like Go, the timer only promotes a
    /// handler that is still in its initial warming state.
    pub fn start(&self, warmup_duration: std::time::Duration) {
        let health_status = Arc::clone(&self.health_status);
        tokio::spawn(async move {
            tokio::time::sleep(warmup_duration).await;
            let _ = health_status.compare_exchange(
                HealthStatus::WarmingUp as u8,
                HealthStatus::Ok as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        });
    }

    pub fn update_health_status(&self, status: HealthStatus) {
        self.health_status.store(status as u8, Ordering::Release);
    }

    pub async fn dispatch(&self, operation: &str, request: Value) -> Result<Value, CadenceError> {
        self.dispatch_with_metadata(operation, request, &CallerMetadata::default())
            .await
    }

    pub async fn dispatch_with_metadata<'a>(
        &'a self,
        operation: &str,
        request: Value,
        caller: &'a CallerMetadata,
    ) -> Result<Value, CadenceError> {
        let operation = WorkflowOperation::from_name(operation).ok_or_else(|| {
            CadenceError::BadRequest(BadRequestError::new(format!(
                "unknown workflow operation: {operation}"
            )))
        })?;
        if let Some(access_control) = &self.access_control {
            access_control.authorize(operation.name(), &request, &caller.actor)?;
        }
        if let Some(redirector) = &self.redirector {
            let redirect_request = request.clone();
            let next = |target_cluster| {
                self.dispatch_metered(operation, request.clone(), caller, target_cluster)
            };
            return redirector
                .invoke(operation, &redirect_request, Box::new(next))
                .await;
        }
        self.dispatch_metered(operation, request, caller, None)
            .await
    }

    fn dispatch_metered<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: Value,
        caller: &'a CallerMetadata,
        target_cluster: Option<String>,
    ) -> WorkflowFuture<'a> {
        Box::pin(async move {
            let start = std::time::Instant::now();
            self.metrics.increment_requests(operation.name());
            let tags = crate::wrappers::metered::request_tags(operation.name(), &request);
            let result = self
                .dispatch_operation(operation, request, caller, target_cluster)
                .await;
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
        })
    }

    async fn dispatch_operation(
        &self,
        operation: WorkflowOperation,
        request: Value,
        caller: &CallerMetadata,
        target_cluster: Option<String>,
    ) -> Result<Value, CadenceError> {
        validate_ratelimit_wrapper_request(operation, &request)?;
        if let Some(rate_limit) = &self.rate_limit {
            rate_limit.allow(operation.name(), &request)?;
        }
        if let Some(version_check) = &self.version_check {
            crate::wrappers::versioncheck::check(
                operation.name(),
                version_check.enabled,
                &caller.client_impl,
                &caller.feature_version,
                version_check.checker.as_ref(),
            )?;
        }
        if !matches!(
            operation,
            WorkflowOperation::Health | WorkflowOperation::GetClusterInfo
        ) && self.shutting_down.load(Ordering::Acquire)
        {
            return Err(CadenceError::Internal(
                cadence_api::errors::InternalServiceError::new("Shutting down"),
            ));
        }
        if operation == WorkflowOperation::Health {
            let status = HealthStatus::from_byte(self.health_status.load(Ordering::Acquire));
            return Ok(serde_json::json!({
                "ok": status == HealthStatus::Ok,
                "msg": status.message()
            }));
        }
        // Go's GetClusterInfo is a local constant response and remains
        // available while the handler drains; it does not call History.
        if operation == WorkflowOperation::GetClusterInfo {
            return Ok(serde_json::json!({
                "supportedClientVersions": {
                    "goSdk": "1.7.0",
                    "javaSdk": "1.5.0"
                }
            }));
        }
        if operation == WorkflowOperation::GetSearchAttributes {
            if let Some(provider) = &self.search_attributes {
                let response = tasks::GetSearchAttributesResponse {
                    keys: Some(provider.valid_search_attributes()),
                };
                return serde_json::to_value(response)
                    .map_err(|error| CadenceError::Unknown(error.to_string()));
            }
        }
        let request = self.validate_request(operation, request)?;
        if matches!(
            operation,
            WorkflowOperation::StartWorkflowExecution
                | WorkflowOperation::StartWorkflowExecutionAsync
                | WorkflowOperation::SignalWorkflowExecution
                | WorkflowOperation::SignalWithStartWorkflowExecution
                | WorkflowOperation::SignalWithStartWorkflowExecutionAsync
                | WorkflowOperation::RestartWorkflowExecution
        ) {
            if let (Some(state), Some(domain)) = (
                &self.isolation_group_state,
                request
                    .get("domain")
                    .or_else(|| request.get("request").and_then(|nested| nested.get("domain")))
                    .and_then(Value::as_str),
            ) {
                // Go logs lookup errors and treats the group as healthy.
                if state
                    .is_drained(domain, &caller.isolation_group)
                    .unwrap_or(false)
                {
                    return Err(CadenceError::BadRequest(BadRequestError::new(format!(
                        "Domain {domain} is drained from isolation group {}.",
                        caller.isolation_group
                    ))));
                }
            }
        }
        if operation == WorkflowOperation::DiagnoseWorkflowExecution {
            return self
                .diagnose_workflow_execution(request, caller, target_cluster)
                .await;
        }
        if matches!(
            operation,
            WorkflowOperation::StartWorkflowExecutionAsync
                | WorkflowOperation::SignalWithStartWorkflowExecutionAsync
        ) {
            if let Some(publisher) = &self.async_publisher {
                let message = crate::handler_workflow_async::encode_async_workflow_request(
                    operation.name(),
                    &request,
                    &caller.original_headers,
                )
                .map_err(|error| {
                    CadenceError::Internal(cadence_api::errors::InternalServiceError::new(
                        format!("failed to encode {}Request: {error}", operation.name()),
                    ))
                })?;
                publisher.publish(message).await?;
                return Ok(serde_json::json!({}));
            }
        }
        let request = self.prepare_backend_request(operation, request)?;
        if operation == WorkflowOperation::RestartWorkflowExecution {
            return self.restart_workflow_execution(request, target_cluster).await;
        }
        self.backend
            .invoke_on_cluster(operation, request, target_cluster)
            .await
    }

    async fn restart_workflow_execution(
        &self,
        request: Value,
        target_cluster: Option<String>,
    ) -> Result<Value, CadenceError> {
        let domain = request
            .get("domain")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let execution = request
            .get("workflowExecution")
            .cloned()
            .unwrap_or(Value::Null);
        let Some(domains) = &self.domain_lookup else {
            return self
                .backend
                .invoke_on_cluster(
                    WorkflowOperation::RestartWorkflowExecution,
                    request,
                    target_cluster,
                )
                .await;
        };
        let domain_id = domains.domain_id(domain)?;

        let history = self
            .backend
            .invoke_on_cluster(
                WorkflowOperation::GetWorkflowExecutionHistory,
                serde_json::json!({
                    "domain": domain,
                    "execution": execution,
                    "skipArchival": true
                }),
                target_cluster.clone(),
            )
            .await
            .map_err(|_| {
                CadenceError::BadRequest(BadRequestError::new(
                    "Requested workflow history not found, may have passed retention period.",
                ))
            })?;

        let started = history
            .get("history")
            .and_then(|value| value.get("events"))
            .and_then(Value::as_array)
            .and_then(|events| events.first())
            .and_then(|event| event.get("workflowExecutionStartedEventAttributes"))
            .filter(|attributes| !attributes.is_null())
            .ok_or_else(|| {
                CadenceError::BadRequest(BadRequestError::new(
                    "Requested workflow history not found, may have passed retention period.",
                ))
            })?;
        let workflow_id = execution
            .get("workflowId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut start_request = serde_json::json!({
            "requestId": uuid::Uuid::new_v4().to_string(),
            "domain": domain,
            "workflowId": workflow_id,
            "workflowType": {
                "name": started.get("workflowType").and_then(|v| v.get("name")).cloned().unwrap_or(Value::Null)
            },
            "taskList": {
                "name": started.get("taskList").and_then(|v| v.get("name")).cloned().unwrap_or(Value::Null),
                "kind": started.get("taskList").and_then(|v| v.get("kind")).cloned().unwrap_or(Value::from(0))
            },
            "input": started.get("input").cloned().unwrap_or(Value::Null),
            "executionStartToCloseTimeoutSeconds": started.get("executionStartToCloseTimeoutSeconds").cloned().unwrap_or(Value::Null),
            "taskStartToCloseTimeoutSeconds": started.get("taskStartToCloseTimeoutSeconds").cloned().unwrap_or(Value::Null),
            "identity": request.get("identity").cloned().unwrap_or(Value::Null),
            "workflowIdReusePolicy": 3,
            "delayStartSeconds": 0
        });
        for field in [
            "cronSchedule",
            "retryPolicy",
            "jitterStartSeconds",
            "header",
            "memo",
            "searchAttributes",
            "cronOverlapPolicy",
            "activeClusterSelectionPolicy",
        ] {
            if let Some(value) = started.get(field) {
                start_request[field] = value.clone();
            }
        }

        let response = self
            .backend
            .invoke_on_cluster(
                WorkflowOperation::StartWorkflowExecution,
                serde_json::json!({
                    "domainUUID": domain_id,
                    "startRequest": start_request
                }),
                target_cluster,
            )
            .await?;
        Ok(serde_json::json!({
            "runID": response.get("runID").cloned().unwrap_or(Value::Null)
        }))
    }

    async fn diagnose_workflow_execution(
        &self,
        request: Value,
        caller: &CallerMetadata,
        target_cluster: Option<String>,
    ) -> Result<Value, CadenceError> {
        let domain = request
            .get("domain")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let execution = request
            .get("workflowExecution")
            .cloned()
            .unwrap_or(Value::Null);
        let workflow_id = execution
            .get("workflowId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let run_id = execution
            .get("runId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let diagnostic_domain = "cadence-system";
        let diagnostic_workflow_id = format!("{domain}-{run_id}");
        let identity = request
            .get("identity")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let input = serde_json::to_vec(&DiagnosticsStarterWorkflowInput {
            domain,
            identity,
            workflow_id,
            run_id,
        })
        .map_err(|error| CadenceError::Unknown(error.to_string()))?;
        let start_request = serde_json::json!({
            "requestId": uuid::Uuid::new_v4().to_string(),
            "domain": diagnostic_domain,
            "workflowId": diagnostic_workflow_id,
            "workflowType": {"name": "diagnostics-starter-workflow"},
            "taskList": {"name": "diagnostics-wf-tasklist"},
            "input": cadence_api::b64::encode_bytes(&input),
            "executionStartToCloseTimeoutSeconds": 86400,
            "taskStartToCloseTimeoutSeconds": 300,
            "identity": identity,
            "workflowIdReusePolicy": 1
        });
        let start_request = self.validate_request(
            WorkflowOperation::StartWorkflowExecution,
            start_request,
        )?;
        if let Some(state) = &self.isolation_group_state {
            if state
                .is_drained(diagnostic_domain, &caller.isolation_group)
                .unwrap_or(false)
            {
                return Err(CadenceError::BadRequest(BadRequestError::new(format!(
                    "Domain {diagnostic_domain} is drained from isolation group {}.",
                    caller.isolation_group
                ))));
            }
        }
        let start_request = self.prepare_backend_request(
            WorkflowOperation::StartWorkflowExecution,
            start_request,
        )?;
        let run_id = match self
            .backend
            .invoke_on_cluster(
                WorkflowOperation::StartWorkflowExecution,
                start_request,
                target_cluster,
            )
            .await
        {
            Ok(response) => response.get("runID").cloned().unwrap_or(Value::Null),
            Err(CadenceError::WorkflowAlreadyStarted(error)) => {
                Value::String(error.run_id)
            }
            Err(error) => return Err(error),
        };
        Ok(serde_json::json!({
            "domain": diagnostic_domain,
            "diagnosticWorkflowExecution": {
                "workflowId": diagnostic_workflow_id,
                "runId": run_id
            }
        }))
    }

    fn prepare_backend_request(
        &self,
        operation: WorkflowOperation,
        request: Value,
    ) -> Result<Value, CadenceError> {
        if matches!(
            operation,
            WorkflowOperation::RecordActivityTaskHeartbeat
                | WorkflowOperation::RespondActivityTaskCompleted
                | WorkflowOperation::RespondActivityTaskFailed
                | WorkflowOperation::RespondActivityTaskCanceled
                | WorkflowOperation::RespondDecisionTaskCompleted
                | WorkflowOperation::RespondDecisionTaskFailed
                | WorkflowOperation::RespondQueryTaskCompleted
        ) {
            return prepare_task_token_request(operation, request);
        }
        let Some(domains) = &self.domain_lookup else {
            // The bootstrap and test backends can still exercise validation
            // before the production domain-cache adapter is installed.
            return Ok(request);
        };
        let domain = request
            .get("domain")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let domain_id = || domains.domain_id(&domain);
        if matches!(
            operation,
            WorkflowOperation::RecordActivityTaskHeartbeatByID
                | WorkflowOperation::RespondActivityTaskCompletedByID
                | WorkflowOperation::RespondActivityTaskFailedByID
                | WorkflowOperation::RespondActivityTaskCanceledByID
        ) {
            return prepare_by_id_task_request(operation, request, domain_id()?);
        }
        let prepared = match operation {
            WorkflowOperation::DescribeTaskList => serde_json::json!({
                "domainUUID": domain_id()?,
                "descRequest": request,
            }),
            WorkflowOperation::ListTaskListPartitions => serde_json::json!({
                "domain": request.get("domain").cloned().unwrap_or(Value::Null),
                "taskList": request.get("taskList").cloned().unwrap_or(Value::Null),
            }),
            WorkflowOperation::GetTaskListsByDomain => serde_json::json!({
                "domain": request.get("domain").cloned().unwrap_or(Value::Null),
            }),
            WorkflowOperation::ResetStickyTaskList => serde_json::json!({
                "domainUUID": domain_id()?,
                "execution": request.get("execution").cloned().unwrap_or(Value::Null),
            }),
            // These are the History service request envelopes constructed by
            // Go's WorkflowHandler. The domain name remains in the public
            // request, while the internal History call receives its UUID.
            WorkflowOperation::StartWorkflowExecution => serde_json::json!({
                "domainUUID": domain_id()?,
                "startRequest": request,
            }),
            WorkflowOperation::SignalWithStartWorkflowExecution => serde_json::json!({
                "domainUUID": domain_id()?,
                "signalWithStartRequest": request,
            }),
            WorkflowOperation::SignalWorkflowExecution => serde_json::json!({
                "domainUUID": domain_id()?,
                "signalRequest": request,
            }),
            WorkflowOperation::TerminateWorkflowExecution => serde_json::json!({
                "domainUUID": domain_id()?,
                "terminateRequest": request,
            }),
            WorkflowOperation::ResetWorkflowExecution => serde_json::json!({
                "domainUUID": domain_id()?,
                "resetRequest": request,
            }),
            WorkflowOperation::RequestCancelWorkflowExecution => serde_json::json!({
                "domainUUID": domain_id()?,
                "cancelRequest": request,
            }),
            WorkflowOperation::QueryWorkflow => serde_json::json!({
                "domainUUID": domain_id()?,
                "request": request,
            }),
            WorkflowOperation::DescribeWorkflowExecution => serde_json::json!({
                "domainUUID": domain_id()?,
                "request": request,
            }),
            WorkflowOperation::CountWorkflowExecutions => serde_json::json!({
                "domainUUID": domain_id()?,
                "domain": domain.clone(),
                "query": request.get("query").cloned().unwrap_or(Value::Null),
            }),
            WorkflowOperation::ScanWorkflowExecutions
            | WorkflowOperation::ListWorkflowExecutions => serde_json::json!({
                "domainUUID": domain_id()?,
                "domain": domain.clone(),
                "pageSize": request.get("pageSize").cloned().unwrap_or(Value::Null),
                "nextPageToken": request.get("nextPageToken").cloned().unwrap_or(Value::Null),
                "query": request.get("query").cloned().unwrap_or(Value::Null),
            }),
            WorkflowOperation::ListOpenWorkflowExecutions
            | WorkflowOperation::ListClosedWorkflowExecutions => {
                let time_filter = request
                    .get("StartTimeFilter")
                    .or_else(|| request.get("startTimeFilter"));
                let mut internal = serde_json::json!({
                    "domainUUID": domain_id()?,
                    "domain": domain.clone(),
                    "pageSize": request.get("maximumPageSize").cloned().unwrap_or(Value::Null),
                    "nextPageToken": request.get("nextPageToken").cloned().unwrap_or(Value::Null),
                    "earliestTime": time_filter.and_then(|v| v.get("earliestTime")).cloned().unwrap_or(Value::Null),
                    "latestTime": time_filter.and_then(|v| v.get("latestTime")).cloned().unwrap_or(Value::Null),
                });
                if let Some(filter) = request.get("executionFilter") {
                    internal["workflowId"] =
                        filter.get("workflowId").cloned().unwrap_or(Value::Null);
                } else if let Some(filter) = request.get("typeFilter") {
                    internal["workflowTypeName"] =
                        filter.get("name").cloned().unwrap_or(Value::Null);
                }
                if operation == WorkflowOperation::ListClosedWorkflowExecutions {
                    if let Some(status) = request.get("statusFilter") {
                        internal["status"] = status.clone();
                    }
                }
                internal
            }
            WorkflowOperation::RefreshWorkflowTasks => serde_json::json!({
                // Preserve the legacy Go field spelling in its internal
                // History request (DomainUIID is intentionally not UUID).
                "domainUIID": domain_id()?,
                "request": request,
            }),
            _ => request,
        };
        Ok(prepared)
    }

    fn validate_request(
        &self,
        operation: WorkflowOperation,
        mut value: Value,
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
            let mut request: cadence_api::service_types::GetWorkflowExecutionHistoryRequest =
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
                    CadenceError::BadRequest(BadRequestError::new("Invalid NextPageToken."))
                })?;
                crate::handler_workflow_history::validate_history_run_id(
                    request
                        .execution
                        .as_ref()
                        .map_or("", |execution| execution.run_id.as_str()),
                    &token,
                )?;
            }
            if request.maximum_page_size <= 0 {
                let domain = request.domain.clone();
                request.maximum_page_size = self
                    .history_page_size_policy
                    .as_ref()
                    .map_or(self.history_max_page_size, |policy| {
                        policy.maximum_page_size(&domain)
                    });
            }
            request.maximum_page_size = request.maximum_page_size.min(1000);
            set_i32_field(&mut value, "maximumPageSize", request.maximum_page_size);
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
            let field = if matches!(
                operation,
                WorkflowOperation::QueryWorkflow | WorkflowOperation::DescribeWorkflowExecution
            ) {
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
                if self.workflow_query_policy.as_ref().is_some_and(|policy| {
                    policy.query_disallowed(
                        value
                            .get("domain")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                }) {
                    return Err(CadenceError::BadRequest(BadRequestError::new(
                        "Domain is not allowed to query, please contact cadence team to re-enable queries.",
                    )));
                }
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
        if matches!(
            operation,
            WorkflowOperation::RecordActivityTaskHeartbeatByID
                | WorkflowOperation::RespondActivityTaskCanceledByID
                | WorkflowOperation::RespondActivityTaskCompletedByID
                | WorkflowOperation::RespondActivityTaskFailedByID
        ) {
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
            WorkflowOperation::PollForActivityTask | WorkflowOperation::PollForDecisionTask => {
                let request: tasks::ListTaskListPartitionsRequest =
                    parse_request(serde_json::json!({
                        "domain": value.get("domain").cloned().unwrap_or(Value::Null),
                        "taskList": value.get("taskList").cloned().unwrap_or(Value::Null),
                    }))?;
                self.validator
                    .validate_list_task_list_partitions_request(Some(&request))?;
            }
            WorkflowOperation::CountWorkflowExecutions => {
                let request: service_types::CountWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator
                    .validate_count_workflow_executions_request(Some(&request))?;
            }
            WorkflowOperation::ScanWorkflowExecutions
            | WorkflowOperation::ListWorkflowExecutions => {
                let mut request: service_types::ListWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator
                    .validate_list_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "pageSize", request.page_size);
            }
            WorkflowOperation::ListOpenWorkflowExecutions => {
                let mut request: service_types::ListOpenWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator
                    .validate_list_open_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "maximumPageSize", request.maximum_page_size);
            }
            WorkflowOperation::ListClosedWorkflowExecutions => {
                let mut request: service_types::ListClosedWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator
                    .validate_list_closed_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "maximumPageSize", request.maximum_page_size);
            }
            WorkflowOperation::ListArchivedWorkflowExecutions => {
                let mut request: service_types::ListArchivedWorkflowExecutionsRequest =
                    parse_request(value.clone())?;
                self.validator
                    .validate_list_archived_workflow_executions_request(Some(&mut request))?;
                set_i32_field(&mut value, "pageSize", request.page_size);
            }
            WorkflowOperation::DescribeTaskList => {
                let request: tasks::DescribeTaskListRequest = parse_request(value.clone())?;
                self.validator
                    .validate_describe_task_list_request(Some(&request))?;
            }
            WorkflowOperation::ListTaskListPartitions => {
                let request: tasks::ListTaskListPartitionsRequest = parse_request(value.clone())?;
                self.validator
                    .validate_list_task_list_partitions_request(Some(&request))?;
            }
            WorkflowOperation::GetTaskListsByDomain => {
                let request: tasks::GetTaskListsByDomainRequest = parse_request(value.clone())?;
                self.validator
                    .validate_get_task_lists_by_domain_request(Some(&request))?;
            }
            WorkflowOperation::ResetStickyTaskList => {
                let request: service_types::ResetStickyTaskListRequest =
                    parse_request(value.clone())?;
                self.validator
                    .validate_reset_sticky_task_list_request(Some(&request))?;
            }
            WorkflowOperation::RefreshWorkflowTasks => {
                let request: cadence_api::types::RefreshWorkflowTasksRequest =
                    parse_request(value.clone())?;
                self.validator
                    .validate_refresh_workflow_tasks_request(Some(&request))?;
            }
            _ => {}
        }
        if matches!(
            operation,
            WorkflowOperation::CountWorkflowExecutions
                | WorkflowOperation::ScanWorkflowExecutions
                | WorkflowOperation::ListWorkflowExecutions
        ) {
            if let Some(validator) = &self.visibility_query_validator {
                let query = value
                    .get("query")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let validated_query = validator.validate_query(query)?;
                if let Some(object) = value.as_object_mut() {
                    object.insert("query".to_owned(), Value::String(validated_query));
                }
            }
        }
        if matches!(
            operation,
            WorkflowOperation::ListOpenWorkflowExecutions
                | WorkflowOperation::ListClosedWorkflowExecutions
        ) {
            let filter_fields: &[&str] =
                if operation == WorkflowOperation::ListClosedWorkflowExecutions {
                    &["executionFilter", "typeFilter", "statusFilter"]
                } else {
                    &["executionFilter", "typeFilter"]
                };
            let has_filter = filter_fields
                .iter()
                .any(|key| value.get(*key).is_some_and(|filter| !filter.is_null()));
            if has_filter
                && self
                    .visibility_filter_policy
                    .as_ref()
                    .is_some_and(|policy| {
                        policy.filters_disabled(
                            value
                                .get("domain")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                        )
                    })
            {
                return Err(CadenceError::BadRequest(BadRequestError::new(
                    "No permission to do this operation.",
                )));
            }
        }
        Ok(value)
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct EncodedTaskToken {
    #[serde(default, deserialize_with = "cadence_api::b64::deserialize_bytes")]
    task_token: Vec<u8>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct EncodedTaskTokenRequest<'a> {
    #[serde(serialize_with = "cadence_api::b64::serialize_bytes")]
    task_token: &'a Vec<u8>,
}

fn encoded_task_token(request: &Value) -> Result<Vec<u8>, CadenceError> {
    serde_json::from_value::<EncodedTaskToken>(request.clone())
        .map(|token| token.task_token)
        .map_err(|error| CadenceError::BadRequest(BadRequestError::new(error.to_string())))
}

fn decode_workflow_task_token(
    request: &Value,
) -> Result<cadence_api::tasktoken::TaskToken, CadenceError> {
    let encoded = encoded_task_token(request)?;
    cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(&encoded)
        .map_err(|error| CadenceError::BadRequest(BadRequestError::new(error.to_string())))
}

fn decode_query_task_token(
    request: &Value,
) -> Result<cadence_api::tasktoken::QueryTaskToken, CadenceError> {
    let encoded = encoded_task_token(request)?;
    cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(&encoded)
        .map_err(|error| CadenceError::BadRequest(BadRequestError::new(error.to_string())))
}

fn prepare_task_token_request(
    operation: WorkflowOperation,
    request: Value,
) -> Result<Value, CadenceError> {
    if operation == WorkflowOperation::RespondQueryTaskCompleted {
        let token = decode_query_task_token(&request)?;
        if token.domain_id.is_empty() || token.task_list.is_empty() || token.task_id.is_empty() {
            return Err(CadenceError::BadRequest(BadRequestError::new(
                "Invalid TaskToken.",
            )));
        }
        return Ok(serde_json::json!({
            "domainUUID": token.domain_id,
            "taskList": {"name": token.task_list},
            "taskID": token.task_id,
            "completedRequest": request,
        }));
    }
    let token = decode_workflow_task_token(&request)?;
    if token.domain_id.is_empty() {
        return Err(cadence_api::validation::err_domain_not_set().into());
    }
    let mut internal = serde_json::json!({"domainUUID": token.domain_id});
    let field = match operation {
        WorkflowOperation::RecordActivityTaskHeartbeat
        | WorkflowOperation::RecordActivityTaskHeartbeatByID => "heartbeatRequest",
        WorkflowOperation::RespondActivityTaskCompleted
        | WorkflowOperation::RespondActivityTaskCompletedByID
        | WorkflowOperation::RespondDecisionTaskCompleted => "completeRequest",
        WorkflowOperation::RespondActivityTaskFailed
        | WorkflowOperation::RespondActivityTaskFailedByID
        | WorkflowOperation::RespondDecisionTaskFailed => "failedRequest",
        WorkflowOperation::RespondActivityTaskCanceled
        | WorkflowOperation::RespondActivityTaskCanceledByID => "cancelRequest",
        _ => unreachable!("matched task-token operation"),
    };
    internal[field] = request;
    Ok(internal)
}

fn prepare_by_id_task_request(
    operation: WorkflowOperation,
    request: Value,
    domain_id: String,
) -> Result<Value, CadenceError> {
    let token = cadence_api::tasktoken::TaskToken {
        domain_id,
        workflow_id: request_string(&request, "workflowId"),
        run_id: request_string(&request, "runId"),
        activity_id: request_string(&request, "activityId"),
        ..Default::default()
    };
    let token_bytes = cadence_api::tasktoken::JsonTaskTokenSerializer::serialize(&token)
        .map_err(|error| CadenceError::Unknown(error.to_string()))?;
    let mut token_request = serde_json::to_value(EncodedTaskTokenRequest {
        task_token: &token_bytes,
    })
    .map_err(|error| CadenceError::Unknown(error.to_string()))?;
    let fields: &[&str] = match operation {
        WorkflowOperation::RecordActivityTaskHeartbeatByID => &["details", "identity"],
        WorkflowOperation::RespondActivityTaskCompletedByID => &["result", "identity"],
        WorkflowOperation::RespondActivityTaskFailedByID => &[
            "reason",
            "details",
            "identity",
            "failureOptions",
            "heartbeatDetails",
        ],
        WorkflowOperation::RespondActivityTaskCanceledByID => &["details", "identity"],
        _ => unreachable!("matched activity-by-ID operation"),
    };
    for field in fields {
        if let Some(value) = request.get(field) {
            token_request[field] = value.clone();
        }
    }
    prepare_task_token_request(operation, token_request)
}

fn request_string(request: &Value, field: &str) -> String {
    request
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
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
            self.latency_samples
                .lock()
                .unwrap()
                .push(operation.to_owned());
        }
    }

    #[derive(Default)]
    struct FakeBackend(Mutex<Vec<WorkflowOperation>>);

    #[derive(Default)]
    struct CapturingBackend(Mutex<Vec<(WorkflowOperation, Value)>>);

    #[derive(Default)]
    struct RestartBackend(Mutex<Vec<(WorkflowOperation, Value)>>);

    struct AlreadyStartedBackend;

    #[derive(Default)]
    struct RecordingAsyncPublisher(Mutex<Vec<crate::handler_workflow_async::AsyncWorkflowMessage>>);

    impl AsyncWorkflowPublisher for RecordingAsyncPublisher {
        fn publish<'a>(
            &'a self,
            message: crate::handler_workflow_async::AsyncWorkflowMessage,
        ) -> WorkflowFuture<'a> {
            self.0.lock().unwrap().push(message);
            Box::pin(async { Ok(Value::Null) })
        }
    }

    impl WorkflowBackend for CapturingBackend {
        fn invoke<'a>(
            &'a self,
            operation: WorkflowOperation,
            request: Value,
        ) -> WorkflowFuture<'a> {
            Box::pin(async move {
                self.0.lock().unwrap().push((operation, request));
                Ok(Value::Null)
            })
        }
    }

    impl WorkflowBackend for AlreadyStartedBackend {
        fn invoke<'a>(&'a self, _: WorkflowOperation, _: Value) -> WorkflowFuture<'a> {
            Box::pin(async {
                Err(CadenceError::WorkflowAlreadyStarted(
                    cadence_api::errors::WorkflowExecutionAlreadyStartedError {
                        run_id: "diagnostic-run-existing".into(),
                        ..Default::default()
                    },
                ))
            })
        }
    }

    impl WorkflowBackend for RestartBackend {
        fn invoke<'a>(&'a self, operation: WorkflowOperation, request: Value) -> WorkflowFuture<'a> {
            Box::pin(async move {
                self.0.lock().unwrap().push((operation, request));
                match operation {
                    WorkflowOperation::GetWorkflowExecutionHistory => Ok(serde_json::json!({
                        "history": {"events": [{
                            "workflowExecutionStartedEventAttributes": {
                                "workflowType": {"name": "Order"},
                                "taskList": {"name": "orders", "kind": 0},
                                "input": "AQI=",
                                "executionStartToCloseTimeoutSeconds": 3600,
                                "taskStartToCloseTimeoutSeconds": 10,
                                "cronSchedule": "0 * * * *",
                                "retryPolicy": {"initialIntervalInSeconds": 5},
                                "jitterStartSeconds": 7,
                                "header": {"fields": {"trace": "AQ=="}},
                                "memo": {"fields": {"key": "dmFsdWU="}},
                                "searchAttributes": {"indexedFields": {"Keyword": "dmFsdWU="}},
                                "cronOverlapPolicy": 1,
                                "activeClusterSelectionPolicy": 0
                            }
                        }]}
                    })),
                    WorkflowOperation::StartWorkflowExecution => {
                        Ok(serde_json::json!({"runID": "new-run"}))
                    }
                    _ => Ok(Value::Null),
                }
            })
        }
    }

    struct StaticDomainLookup;

    struct DrainedIsolationGroup;

    impl IsolationGroupState for DrainedIsolationGroup {
        fn is_drained(&self, _: &str, _: &str) -> Result<bool, String> {
            Ok(true)
        }
    }

    impl WorkflowDomainLookup for StaticDomainLookup {
        fn domain_id(&self, domain: &str) -> Result<String, CadenceError> {
            Ok(format!("uuid:{domain}"))
        }
    }

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

    #[derive(Default)]
    struct RedirectBackend(Mutex<Vec<Option<String>>>);

    impl WorkflowBackend for RedirectBackend {
        fn invoke<'a>(&'a self, _: WorkflowOperation, _: Value) -> WorkflowFuture<'a> {
            Box::pin(async { Ok(Value::Null) })
        }

        fn invoke_on_cluster<'a>(
            &'a self,
            _: WorkflowOperation,
            _: Value,
            target_cluster: Option<String>,
        ) -> WorkflowFuture<'a> {
            Box::pin(async move {
                self.0.lock().unwrap().push(target_cluster);
                Ok(Value::Null)
            })
        }
    }

    struct ForwardToCluster(&'static str);

    impl RequestRedirector for ForwardToCluster {
        fn invoke<'a>(
            &'a self,
            _: WorkflowOperation,
            _: &'a Value,
            mut next: WorkflowNext<'a>,
        ) -> WorkflowFuture<'a> {
            let target = self.0.to_owned();
            Box::pin(async move { next(Some(target)).await })
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

    #[derive(Default)]
    struct RecordingAuthorization(Mutex<Vec<String>>);

    impl RequestAuthorization for RecordingAuthorization {
        fn authorize(&self, operation: &str, _: &Value, actor: &str) -> Result<(), CadenceError> {
            self.0.lock().unwrap().push(format!("{operation}:{actor}"));
            Ok(())
        }
    }

    #[derive(Default)]
    struct DenyRateLimit(Mutex<Vec<String>>);

    impl RequestRateLimit for DenyRateLimit {
        fn allow(&self, operation: &str, _: &Value) -> Result<(), CadenceError> {
            self.0.lock().unwrap().push(operation.to_owned());
            Err(CadenceError::ServiceBusy(
                cadence_api::errors::ServiceBusyError::new("limited"),
            ))
        }
    }

    #[derive(Default)]
    struct RecordingVersionCheck(Mutex<Vec<(String, String)>>);

    impl crate::wrappers::versioncheck::VersionChecker for RecordingVersionCheck {
        fn client_supported(
            &self,
            _: bool,
            client_impl: &str,
            feature_version: &str,
        ) -> Result<(), cadence_api::errors::ClientVersionNotSupportedError> {
            self.0
                .lock()
                .unwrap()
                .push((client_impl.to_owned(), feature_version.to_owned()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingVisibilityQuery(Mutex<Vec<String>>);

    impl VisibilityQueryValidator for RecordingVisibilityQuery {
        fn validate_query(&self, query: &str) -> Result<String, CadenceError> {
            self.0.lock().unwrap().push(query.to_owned());
            Ok(format!("validated:{query}"))
        }
    }

    struct DisabledVisibilityFilters(&'static str);

    impl VisibilityFilterPolicy for DisabledVisibilityFilters {
        fn filters_disabled(&self, domain: &str) -> bool {
            domain == self.0
        }
    }

    struct DomainHistoryPageSize;

    impl HistoryPageSizePolicy for DomainHistoryPageSize {
        fn maximum_page_size(&self, domain: &str) -> i32 {
            if domain == "shop" {
                350
            } else {
                125
            }
        }
    }

    struct DisabledWorkflowQueries(&'static str);

    impl WorkflowQueryPolicy for DisabledWorkflowQueries {
        fn query_disallowed(&self, domain: &str) -> bool {
            domain == self.0
        }
    }

    struct StaticSearchAttributes;

    impl SearchAttributeProvider for StaticSearchAttributes {
        fn valid_search_attributes(&self) -> HashMap<String, cadence_api::enums::IndexedValueType> {
            HashMap::from([(
                "CustomKeywordField".to_owned(),
                cadence_api::enums::IndexedValueType::Keyword,
            )])
        }
    }

    #[test]
    fn operation_names_round_trip_and_include_list_tasklist_surface() {
        assert_eq!(WorkflowOperation::ALL.len(), 40);
        for operation in WorkflowOperation::ALL {
            assert_eq!(
                WorkflowOperation::from_name(operation.name()),
                Some(*operation)
            );
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
            handler
                .dispatch("StartWorkflowExecution", request.clone())
                .await
                .unwrap(),
            request
        );
        assert_eq!(
            handler.backend.0.lock().unwrap().as_slice(),
            &[WorkflowOperation::StartWorkflowExecution]
        );
        assert!(handler.dispatch("NotAnApi", Value::Null).await.is_err());
    }

    #[tokio::test]
    async fn stop_rejects_workflow_calls_but_keeps_health_available() {
        let handler = Handler::new(FakeBackend::default());
        handler.stop();

        let error = handler
            .dispatch(
                "StartWorkflowExecution",
                serde_json::json!({"domain":"shop"}),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "cadence internal error, msg: Shutting down"
        );

        assert!(
            handler
                .dispatch("Health", serde_json::Value::Null)
                .await
                .unwrap()["ok"]
                .as_bool()
                == Some(false)
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cluster_info_matches_go_constants_and_remains_available_while_stopping() {
        let handler = Handler::new(FakeBackend::default());
        handler.stop();
        assert_eq!(
            handler
                .dispatch("GetClusterInfo", Value::Null)
                .await
                .unwrap(),
            serde_json::json!({
                "supportedClientVersions": {
                    "goSdk": "1.7.0",
                    "javaSdk": "1.5.0"
                }
            })
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn search_attributes_uses_local_dynamic_config_provider() {
        let handler = Handler::new(FakeBackend::default())
            .with_search_attribute_provider(Arc::new(StaticSearchAttributes));
        assert_eq!(
            handler
                .dispatch("GetSearchAttributes", Value::Null)
                .await
                .unwrap(),
            serde_json::json!({"keys": {"CustomKeywordField": 1}})
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn health_warmup_promotes_only_if_status_stays_warming() {
        let handler = Handler::new(FakeBackend::default());
        handler.start(std::time::Duration::from_millis(1));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let health = handler.dispatch("Health", Value::Null).await.unwrap();
        assert_eq!(health["ok"], true);
        assert_eq!(health["msg"], "OK");

        handler.update_health_status(HealthStatus::ShuttingDown);
        handler.start(std::time::Duration::ZERO);
        tokio::task::yield_now().await;
        let health = handler.dispatch("Health", Value::Null).await.unwrap();
        assert_eq!(health["ok"], false);
        assert_eq!(health["msg"], "ShuttingDown");
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
        assert_eq!(
            *metrics.requests.lock().unwrap(),
            ["DescribeWorkflowExecution"]
        );
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
            .dispatch("GetSearchAttributes", Value::Null)
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
    async fn wrapper_order_is_access_then_metered_rate_limit_then_version() {
        let authorization = Arc::new(RecordingAuthorization::default());
        let rate_limit = Arc::new(DenyRateLimit::default());
        let version = Arc::new(RecordingVersionCheck::default());
        let metrics = Arc::new(RecordingMetrics::default());
        let handler = Handler::with_metrics(
            FakeBackend::default(),
            crate::request_validator::RequestValidator::default(),
            metrics.clone(),
        )
        .with_access_control(authorization.clone())
        .with_rate_limit(rate_limit.clone())
        .with_version_check(true, version.clone());
        let error = handler
            .dispatch_with_metadata(
                "DescribeWorkflowExecution",
                serde_json::json!({"domain": "shop", "workflowExecution": {"workflowId": "wf"}}),
                &CallerMetadata {
                    actor: "sdk-worker".to_owned(),
                    client_impl: "go-sdk".to_owned(),
                    feature_version: "1.2".to_owned(),
                    isolation_group: String::new(),
                    original_headers: HashMap::new(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "limited");
        assert_eq!(
            *authorization.0.lock().unwrap(),
            ["DescribeWorkflowExecution:sdk-worker"]
        );
        assert_eq!(*rate_limit.0.lock().unwrap(), ["DescribeWorkflowExecution"]);
        assert!(version.0.lock().unwrap().is_empty());
        assert!(handler.backend.0.lock().unwrap().is_empty());
        assert_eq!(
            *metrics.requests.lock().unwrap(),
            ["DescribeWorkflowExecution"]
        );
    }

    #[tokio::test]
    async fn access_control_denial_is_outside_metering() {
        struct Deny;
        impl RequestAuthorization for Deny {
            fn authorize(&self, _: &str, _: &Value, _: &str) -> Result<(), CadenceError> {
                Err(CadenceError::AccessDenied(
                    cadence_api::errors::AccessDeniedError::new("Request unauthorized."),
                ))
            }
        }

        let metrics = Arc::new(RecordingMetrics::default());
        let handler = Handler::with_metrics(
            FakeBackend::default(),
            crate::request_validator::RequestValidator::default(),
            metrics.clone(),
        )
        .with_access_control(Arc::new(Deny));
        let error = handler
            .dispatch(
                "StartWorkflowExecution",
                serde_json::json!({"domain": "shop"}),
            )
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "Request unauthorized.");
        assert!(metrics.requests.lock().unwrap().is_empty());
        assert!(metrics.counters.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cluster_redirection_wraps_and_selects_the_backend_cluster() {
        let backend = RedirectBackend::default();
        let metrics = Arc::new(RecordingMetrics::default());
        let handler = Handler::with_metrics(
            backend,
            crate::request_validator::RequestValidator::default(),
            metrics.clone(),
        )
        .with_cluster_redirection(Arc::new(ForwardToCluster("cluster-b")));

        handler
            .dispatch("GetSearchAttributes", Value::Null)
            .await
            .unwrap();

        assert_eq!(
            *handler.backend.0.lock().unwrap(),
            [Some("cluster-b".to_owned())]
        );
        assert_eq!(*metrics.requests.lock().unwrap(), ["GetSearchAttributes"]);
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
        assert_eq!(
            missing_signal.to_string(),
            "SignalName is not set on request."
        );
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
            handler
                .dispatch("QueryWorkflow", request.clone())
                .await
                .unwrap_err()
                .to_string(),
            "WorkflowQuery is not set on request."
        );
        let request = serde_json::json!({
            "domain": "shop",
            "execution": {"workflowId": "checkout"},
            "query": {"queryArgs": ""}
        });
        assert_eq!(
            handler
                .dispatch("QueryWorkflow", request)
                .await
                .unwrap_err()
                .to_string(),
            "QueryType is not set on request."
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn query_domain_policy_runs_after_execution_and_before_query_payload() {
        let handler = Handler::new(CapturingBackend::default())
            .with_workflow_query_policy(Arc::new(DisabledWorkflowQueries("shop")));
        let error = handler
            .dispatch(
                "QueryWorkflow",
                serde_json::json!({
                    "domain": "shop",
                    "execution": {"workflowId": "wf"}
                }),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Domain is not allowed to query, please contact cadence team to re-enable queries."
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
        assert_eq!(
            task_list_error.to_string(),
            "TaskList is not set on request."
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn describe_task_list_builds_go_matching_request_after_domain_lookup() {
        let backend = CapturingBackend::default();
        let handler = Handler::new(backend).with_domain_lookup(Arc::new(StaticDomainLookup));
        handler
            .dispatch(
                "DescribeTaskList",
                serde_json::json!({
                    "domain": "shop",
                    "taskList": {"name": "orders"},
                    "taskListType": 0,
                    "includeTaskListStatus": true
                }),
            )
            .await
            .unwrap();
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, WorkflowOperation::DescribeTaskList);
        assert_eq!(
            calls[0].1,
            serde_json::json!({
                "domainUUID": "uuid:shop",
                "descRequest": {
                    "domain": "shop",
                    "taskList": {"name": "orders"},
                    "taskListType": 0,
                    "includeTaskListStatus": true
                }
            })
        );
    }

    #[tokio::test]
    async fn describe_workflow_uses_public_execution_field_and_history_envelope() {
        let backend = CapturingBackend::default();
        let handler = Handler::new(backend).with_domain_lookup(Arc::new(StaticDomainLookup));
        handler
            .dispatch(
                "DescribeWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "execution": {
                        "workflowId": "wf",
                        "runId": "550e8400-e29b-41d4-a716-446655440000"
                    }
                }),
            )
            .await
            .unwrap();
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].1,
            serde_json::json!({
                "domainUUID": "uuid:shop",
                "request": {
                    "domain": "shop",
                    "execution": {
                        "workflowId": "wf",
                        "runId": "550e8400-e29b-41d4-a716-446655440000"
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn workflow_history_operations_build_go_request_envelopes() {
        let backend = CapturingBackend::default();
        let handler = Handler::new(backend).with_domain_lookup(Arc::new(StaticDomainLookup));
        let cases = [
            (
                "SignalWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {"workflowId": "wf"},
                    "signalName": "wake"
                }),
                "signalRequest",
            ),
            (
                "TerminateWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {"workflowId": "wf"}
                }),
                "terminateRequest",
            ),
            (
                "ResetWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {"workflowId": "wf"}
                }),
                "resetRequest",
            ),
            (
                "RequestCancelWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {"workflowId": "wf"}
                }),
                "cancelRequest",
            ),
            (
                "QueryWorkflow",
                serde_json::json!({
                    "domain": "shop",
                    "execution": {"workflowId": "wf"},
                    "query": {"queryType": "status"}
                }),
                "request",
            ),
        ];
        for (operation, request, envelope_field) in cases {
            handler.dispatch(operation, request.clone()).await.unwrap();
            let mut expected = serde_json::json!({"domainUUID": "uuid:shop"});
            expected[envelope_field] = request;
            let calls = handler.backend.0.lock().unwrap();
            assert_eq!(calls.last().unwrap().1, expected, "{operation}");
        }
    }

    #[tokio::test]
    async fn start_workflow_builds_history_request_with_domain_uuid() {
        let backend = CapturingBackend::default();
        let handler = Handler::new(backend).with_domain_lookup(Arc::new(StaticDomainLookup));
        let request = serde_json::json!({
            "domain": "shop",
            "workflowId": "order-1",
            "workflowType": {"name": "Order"},
            "taskList": {"name": "orders"},
            "requestId": "550e8400-e29b-41d4-a716-446655440000",
            "executionStartToCloseTimeoutSeconds": 60,
            "taskStartToCloseTimeoutSeconds": 10
        });
        handler
            .dispatch("StartWorkflowExecution", request.clone())
            .await
            .unwrap();
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].1,
            serde_json::json!({"domainUUID": "uuid:shop", "startRequest": request})
        );
    }

    #[tokio::test]
    async fn signal_with_start_builds_history_request_with_domain_uuid() {
        let backend = CapturingBackend::default();
        let handler = Handler::new(backend).with_domain_lookup(Arc::new(StaticDomainLookup));
        let request = serde_json::json!({
            "domain": "shop",
            "workflowId": "order-1",
            "workflowType": {"name": "Order"},
            "taskList": {"name": "orders"},
            "requestId": "550e8400-e29b-41d4-a716-446655440000",
            "executionStartToCloseTimeoutSeconds": 60,
            "taskStartToCloseTimeoutSeconds": 10,
            "signalName": "confirmed"
        });
        handler
            .dispatch("SignalWithStartWorkflowExecution", request.clone())
            .await
            .unwrap();
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].1,
            serde_json::json!({
                "domainUUID": "uuid:shop",
                "signalWithStartRequest": request
            })
        );
    }

    #[tokio::test]
    async fn restart_reads_start_event_and_starts_a_new_run_with_go_fields() {
        let handler = Handler::new(RestartBackend::default())
            .with_domain_lookup(Arc::new(StaticDomainLookup));
        let response = handler
            .dispatch(
                "RestartWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {
                        "workflowId": "order-1",
                        "runId": "550e8400-e29b-41d4-a716-446655440000"
                    },
                    "identity": "operator"
                }),
            )
            .await
            .unwrap();

        assert_eq!(response, serde_json::json!({"runID": "new-run"}));
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, WorkflowOperation::GetWorkflowExecutionHistory);
        assert_eq!(calls[0].1["skipArchival"], true);
        assert_eq!(calls[1].0, WorkflowOperation::StartWorkflowExecution);
        assert_eq!(calls[1].1["domainUUID"], "uuid:shop");
        let start = &calls[1].1["startRequest"];
        assert_eq!(start["workflowIdReusePolicy"], 3);
        assert_eq!(start["domain"], "shop");
        assert_eq!(start["workflowId"], "order-1");
        assert_eq!(start["workflowType"]["name"], "Order");
        assert_eq!(start["identity"], "operator");
        assert_eq!(start["delayStartSeconds"], 0);
        assert_eq!(start["cronSchedule"], "0 * * * *");
        assert_eq!(start["jitterStartSeconds"], 7);
        assert!(uuid::Uuid::parse_str(start["requestId"].as_str().unwrap()).is_ok());
    }

    #[tokio::test]
    async fn restart_rejects_a_drained_isolation_group_before_history_lookup() {
        let backend = RestartBackend::default();
        let handler = Handler::new(backend)
            .with_domain_lookup(Arc::new(StaticDomainLookup))
            .with_isolation_group_state(Arc::new(DrainedIsolationGroup));
        let error = handler
            .dispatch_with_metadata(
                "RestartWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {
                        "workflowId": "order-1",
                        "runId": "550e8400-e29b-41d4-a716-446655440000"
                    }
                }),
                &CallerMetadata {
                    isolation_group: "dca1".into(),
                    ..CallerMetadata::default()
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Domain shop is drained from isolation group dca1."
        );
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn async_start_publishes_go_thriftrw_message_and_skips_history_backend() {
        let publisher = Arc::new(RecordingAsyncPublisher::default());
        let handler = Handler::new(FakeBackend::default()).with_async_publisher(publisher.clone());
        handler
            .dispatch_with_metadata(
                "StartWorkflowExecutionAsync",
                serde_json::json!({
                    "request": {
                        "requestId": "550e8400-e29b-41d4-a716-446655440001",
                        "domain": "shop",
                        "workflowId": "order-19",
                        "workflowType": {"name": "Order"},
                        "taskList": {"name": "orders"},
                        "executionStartToCloseTimeoutSeconds": 100,
                        "taskStartToCloseTimeoutSeconds": 10,
                        "input": "AQI="
                    }
                }),
                &CallerMetadata {
                    original_headers: HashMap::from([("x-cadence-trace".into(), "abc".into())]),
                    ..CallerMetadata::default()
                },
            )
            .await
            .unwrap();

        let messages = publisher.0.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].domain, "shop");
        assert_eq!(messages[0].partition_key, "order-19");
        assert_eq!(messages[0].headers["x-cadence-trace"], b"abc");
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn diagnose_starts_system_workflow_with_go_input_and_request_policy() {
        let handler = Handler::new(CapturingBackend::default())
            .with_domain_lookup(Arc::new(StaticDomainLookup));
        let response = handler
            .dispatch(
                "DiagnoseWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {
                        "workflowId": "order-20",
                        "runId": "550e8400-e29b-41d4-a716-446655440000"
                    },
                    "identity": "operator"
                }),
            )
            .await
            .unwrap();

        assert_eq!(response["domain"], "cadence-system");
        assert_eq!(
            response["diagnosticWorkflowExecution"]["workflowId"],
            "shop-550e8400-e29b-41d4-a716-446655440000"
        );
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, WorkflowOperation::StartWorkflowExecution);
        assert_eq!(calls[0].1["domainUUID"], "uuid:cadence-system");
        let start: cadence_api::types::StartWorkflowExecutionRequest =
            serde_json::from_value(calls[0].1["startRequest"].clone()).unwrap();
        assert_eq!(start.workflow_id, "shop-550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(
            start.workflow_id_reuse_policy,
            Some(cadence_api::enums::WorkflowIDReusePolicy::AllowDuplicate)
        );
        assert_eq!(
            start.input,
            br#"{"Domain":"shop","Identity":"operator","WorkflowID":"order-20","RunID":"550e8400-e29b-41d4-a716-446655440000"}"#
        );
    }

    #[tokio::test]
    async fn diagnose_treats_already_started_as_success_with_existing_run_id() {
        let handler = Handler::new(AlreadyStartedBackend);
        let response = handler
            .dispatch(
                "DiagnoseWorkflowExecution",
                serde_json::json!({
                    "domain": "shop",
                    "workflowExecution": {
                        "workflowId": "order-21",
                        "runId": "550e8400-e29b-41d4-a716-446655440000"
                    }
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            response["diagnosticWorkflowExecution"]["runId"],
            "diagnostic-run-existing"
        );
    }

    #[tokio::test]
    async fn count_visibility_builds_go_persistence_request_with_validated_query() {
        let backend = CapturingBackend::default();
        let handler = Handler::new(backend)
            .with_domain_lookup(Arc::new(StaticDomainLookup))
            .with_visibility_query_validator(Arc::new(RecordingVisibilityQuery::default()));
        handler
            .dispatch(
                "CountWorkflowExecutions",
                serde_json::json!({"domain": "shop", "query": "WorkflowType='order'"}),
            )
            .await
            .unwrap();
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, WorkflowOperation::CountWorkflowExecutions);
        assert_eq!(
            calls[0].1,
            serde_json::json!({
                "domainUUID": "uuid:shop",
                "domain": "shop",
                "query": "validated:WorkflowType='order'"
            })
        );
    }

    #[tokio::test]
    async fn activity_heartbeat_decodes_token_and_builds_go_history_envelope() {
        let token = cadence_api::tasktoken::TaskToken {
            domain_id: "domain-uuid".into(),
            workflow_id: "wf".into(),
            run_id: "run".into(),
            schedule_id: 7,
            activity_id: "activity".into(),
            ..Default::default()
        };
        let api_request = tasks::RecordActivityTaskHeartbeatRequest {
            task_token: cadence_api::tasktoken::JsonTaskTokenSerializer::serialize(&token).unwrap(),
            details: vec![1, 2, 3],
            identity: "worker".into(),
        };
        let expected_request = serde_json::to_value(&api_request).unwrap();
        let handler = Handler::new(CapturingBackend::default());
        handler
            .dispatch("RecordActivityTaskHeartbeat", expected_request.clone())
            .await
            .unwrap();
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, WorkflowOperation::RecordActivityTaskHeartbeat);
        assert_eq!(
            calls[0].1,
            serde_json::json!({
                "domainUUID": "domain-uuid",
                "heartbeatRequest": expected_request
            })
        );
    }

    #[tokio::test]
    async fn activity_heartbeat_by_id_builds_go_task_token_and_history_envelope() {
        let handler = Handler::new(CapturingBackend::default())
            .with_domain_lookup(Arc::new(StaticDomainLookup));
        let request = serde_json::json!({
            "domain": "shop",
            "workflowId": "wf",
            "runId": "run",
            "activityId": "activity",
            "details": "AQID",
            "identity": "worker"
        });
        handler
            .dispatch("RecordActivityTaskHeartbeatByID", request)
            .await
            .unwrap();
        let token = cadence_api::tasktoken::TaskToken {
            domain_id: "uuid:shop".into(),
            workflow_id: "wf".into(),
            run_id: "run".into(),
            schedule_id: 0,
            activity_id: "activity".into(),
            ..Default::default()
        };
        let synthesized_request = serde_json::to_value(EncodedTaskTokenRequest {
            task_token: &cadence_api::tasktoken::JsonTaskTokenSerializer::serialize(&token)
                .unwrap(),
        })
        .unwrap();
        let mut heartbeat_request = synthesized_request;
        heartbeat_request["details"] = serde_json::json!("AQID");
        heartbeat_request["identity"] = serde_json::json!("worker");
        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].1,
            serde_json::json!({
                "domainUUID": "uuid:shop",
                "heartbeatRequest": heartbeat_request
            })
        );
    }

    #[tokio::test]
    async fn poll_methods_validate_task_list_before_matching_call() {
        let handler = Handler::new(FakeBackend::default());
        for operation in ["PollForActivityTask", "PollForDecisionTask"] {
            let error = handler
                .dispatch(
                    operation,
                    serde_json::json!({"domain": "shop", "taskList": {"name": ""}}),
                )
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), "TaskList is not set on request.");
        }
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn by_id_activity_methods_validate_workflow_and_activity_ids() {
        let handler = Handler::new(FakeBackend::default());
        for operation in [
            "RecordActivityTaskHeartbeatByID",
            "RespondActivityTaskCanceledByID",
            "RespondActivityTaskCompletedByID",
            "RespondActivityTaskFailedByID",
        ] {
            let base = serde_json::json!({"domain": "shop"});
            assert_eq!(
                handler
                    .dispatch(operation, base.clone())
                    .await
                    .unwrap_err()
                    .to_string(),
                "WorkflowId is not set on request."
            );
            let workflow = serde_json::json!({"domain": "shop", "workflowId": "wf"});
            assert_eq!(
                handler
                    .dispatch(operation, workflow)
                    .await
                    .unwrap_err()
                    .to_string(),
                "ActivityID is not set on request."
            );
        }
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
                    "execution": {
                        "workflowId": "checkout",
                        "runId": "550e8400-e29b-41d4-a716-446655440000"
                    },
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

    #[tokio::test]
    async fn history_page_size_uses_domain_default_and_go_hard_cap() {
        let handler = Handler::new(FakeBackend::default()).with_history_max_page_size(1200);
        let request = serde_json::json!({
            "domain": "shop",
            "execution": {"workflowId": "wf"},
            "maximumPageSize": 0,
            "futureSdkField": true
        });
        let response = handler
            .dispatch("GetWorkflowExecutionHistory", request)
            .await
            .unwrap();
        assert_eq!(response["maximumPageSize"], 1000);
        assert_eq!(response["futureSdkField"], true);

        let response = handler
            .dispatch(
                "GetWorkflowExecutionHistory",
                serde_json::json!({
                    "domain": "shop",
                    "execution": {"workflowId": "wf"},
                    "maximumPageSize": 1500
                }),
            )
            .await
            .unwrap();
        assert_eq!(response["maximumPageSize"], 1000);
    }

    #[tokio::test]
    async fn history_page_size_policy_resolves_dynamic_default_by_domain() {
        let handler = Handler::new(FakeBackend::default())
            .with_history_page_size_policy(Arc::new(DomainHistoryPageSize));
        let response = handler
            .dispatch(
                "GetWorkflowExecutionHistory",
                serde_json::json!({
                    "domain": "shop",
                    "execution": {"workflowId": "wf"},
                    "maximumPageSize": 0
                }),
            )
            .await
            .unwrap();

        assert_eq!(response["maximumPageSize"], 350);
    }

    #[tokio::test]
    async fn count_visibility_query_uses_injected_go_validator() {
        let query_validator = Arc::new(RecordingVisibilityQuery::default());
        let handler = Handler::new(FakeBackend::default())
            .with_visibility_query_validator(query_validator.clone());
        let response = handler
            .dispatch(
                "CountWorkflowExecutions",
                serde_json::json!({
                    "domain": "shop",
                    "query": "WorkflowType = 'checkout'",
                    "futureSdkField": "kept"
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            *query_validator.0.lock().unwrap(),
            ["WorkflowType = 'checkout'"]
        );
        assert_eq!(response["query"], "validated:WorkflowType = 'checkout'");
        assert_eq!(response["futureSdkField"], "kept");
    }

    #[tokio::test]
    async fn filtered_visibility_list_obeys_domain_dynamic_filter_gate() {
        let handler = Handler::new(CapturingBackend::default())
            .with_visibility_filter_policy(Arc::new(DisabledVisibilityFilters("shop")));
        let error = handler
            .dispatch(
                "ListOpenWorkflowExecutions",
                serde_json::json!({
                    "domain": "shop",
                    "StartTimeFilter": {"earliestTime": 1, "latestTime": 2},
                    "executionFilter": {"workflowId": "wf"}
                }),
            )
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "No permission to do this operation.");
        assert!(handler.backend.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unfiltered_visibility_list_is_not_blocked_by_filter_gate() {
        let handler = Handler::new(CapturingBackend::default())
            .with_domain_lookup(Arc::new(StaticDomainLookup))
            .with_visibility_filter_policy(Arc::new(DisabledVisibilityFilters("shop")));
        handler
            .dispatch(
                "ListOpenWorkflowExecutions",
                serde_json::json!({
                    "domain": "shop",
                    "StartTimeFilter": {"earliestTime": 1, "latestTime": 2}
                }),
            )
            .await
            .unwrap();

        let calls = handler.backend.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1["domainUUID"], "uuid:shop");
    }
}
