//! Native Cadence protobuf gRPC frontend. All implemented adapters use the
//! same application state and request handlers as the HTTP/Thrift frontend.
//! RPCs whose engine behavior has not been ported return UNIMPLEMENTED.
use crate::grpc_convert as cv;
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request},
};
use cadence_api::{enums, service_types as st, types as t};
use cadence_proto::api as p;
use prost::Message;
use serde::{de::DeserializeOwned, Serialize};
use tonic::{Response, Status};
use tower::ServiceExt;

// google.rpc.Status, used by Cadence/YARPC for typed error details.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}
pub(crate) fn detailed_status<M: Message>(
    code: tonic::Code,
    message: impl Into<String>,
    name: &str,
    detail: M,
) -> Status {
    detailed_status_type(
        code,
        message,
        format!("type.googleapis.com/uber.cadence.api.v1.{name}"),
        detail,
    )
}
/// Go `types/mapper/proto.fromEntityNotExistsError` packs a domain lookup miss
/// (`Domain <id or name> does not exist.`) as NotFound with an
/// `EntityNotExistsError` detail. The domain store sets no cluster fields, so
/// the detail is empty.
pub(crate) fn domain_not_found(message: impl Into<String>) -> Status {
    detailed_status(
        tonic::Code::NotFound,
        message,
        "EntityNotExistsError",
        p::EntityNotExistsError::default(),
    )
}
/// Go `types/mapper/proto.FromError` packs shared-v1 errors (for example
/// `RemoteSyncMatchedError`, mapped to `CodeUnavailable`) with their own
/// proto package URL instead of the api.v1 prefix above.
pub(crate) fn detailed_status_type<M: Message>(
    code: tonic::Code,
    message: impl Into<String>,
    type_url: String,
    detail: M,
) -> Status {
    let message = message.into();
    let status = RpcStatus {
        code: code as i32,
        message: message.clone(),
        details: vec![prost_types::Any {
            type_url,
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details(code, message, status.encode_to_vec().into())
}
fn query(path: &str, pairs: &[(&str, &str)]) -> String {
    fn escape(v: &str) -> String {
        v.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                    (b as char).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect()
    }
    format!(
        "{path}?{}",
        pairs
            .iter()
            .map(|(k, v)| format!("{}={}", escape(k), escape(v)))
            .collect::<Vec<_>>()
            .join("&")
    )
}

fn status_from_error_type(kind: &str, message: &str) -> Status {
    match kind {
        "AccessDeniedError" => Status::permission_denied(message),
        "DomainNotActiveError" | "ClientVersionNotSupportedError" => {
            Status::failed_precondition(message)
        }
        "LimitExceededError" => Status::resource_exhausted(message),
        "CancellationAlreadyRequestedError" => Status::already_exists(message),
        "BadRequestError" => Status::invalid_argument(message),
        "EntityNotExistsError" => detailed_status(
            tonic::Code::NotFound,
            message,
            "EntityNotExistsError",
            p::EntityNotExistsError::default(),
        ),
        "WorkflowExecutionAlreadyCompletedError" => detailed_status(
            tonic::Code::NotFound,
            message,
            "WorkflowExecutionAlreadyCompletedError",
            p::WorkflowExecutionAlreadyCompletedError {},
        ),
        "DomainAlreadyExistsError" => detailed_status(
            tonic::Code::AlreadyExists,
            message,
            "DomainAlreadyExistsError",
            p::DomainAlreadyExistsError {},
        ),
        "WorkflowExecutionAlreadyStartedError" => Status::already_exists(message),
        "ServiceBusyError" => detailed_status(
            tonic::Code::ResourceExhausted,
            message,
            "ServiceBusyError",
            p::ServiceBusyError {
                reason: String::new(),
            },
        ),
        "QueryFailedError" => detailed_status(
            tonic::Code::InvalidArgument,
            message,
            "QueryFailedError",
            p::QueryFailedError {},
        ),
        _ => Status::internal(message),
    }
}

tokio::task_local! {
    /// Inbound gRPC request headers and deadline for the current request.
    pub(crate) static GRPC_CALLER: (std::collections::HashMap<String, String>, Option<std::time::Instant>);
}

fn parse_grpc_timeout(value: &str) -> Option<std::time::Duration> {
    let (digits, unit) = value.split_at(value.len().checked_sub(1)?);
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => std::time::Duration::from_secs(n.saturating_mul(3600)),
        "M" => std::time::Duration::from_secs(n.saturating_mul(60)),
        "S" => std::time::Duration::from_secs(n),
        "m" => std::time::Duration::from_millis(n),
        "u" => std::time::Duration::from_micros(n),
        "n" => std::time::Duration::from_nanos(n),
        _ => return None,
    })
}

/// Tower layer that records caller headers and the `grpc-timeout` deadline
/// for the Frontend hook (Go: request headers and context deadline).
#[derive(Clone)]
pub(crate) struct CallerScope<S>(pub(crate) S);

impl<S, B> tower::Service<axum::http::Request<B>> for CallerScope<S>
where
    S: tower::Service<axum::http::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: axum::http::Request<B>) -> Self::Future {
        let mut headers = std::collections::HashMap::new();
        for (name, value) in request.headers() {
            if let Ok(value) = value.to_str() {
                headers.insert(name.as_str().to_ascii_lowercase(), value.to_owned());
            }
        }
        let deadline = headers
            .get("grpc-timeout")
            .and_then(|value| parse_grpc_timeout(value))
            .map(|timeout| std::time::Instant::now() + timeout);
        let clone = self.0.clone();
        let mut inner = std::mem::replace(&mut self.0, clone);
        Box::pin(GRPC_CALLER.scope(
            (headers, deadline),
            async move { inner.call(request).await },
        ))
    }
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Map a legacy in-process route to the Frontend hook operation name and, for
/// GET routes, the request JSON built from the query string.
fn schedule_operation(method: &Method, path: &str) -> Option<&'static str> {
    if method != Method::POST {
        return None;
    }
    Some(match path {
        "/api/v1/schedules/create" => "CreateSchedule",
        "/api/v1/schedules/describe" => "DescribeSchedule",
        "/api/v1/schedules/list" => "ListSchedules",
        "/api/v1/schedules/delete" => "DeleteSchedule",
        "/api/v1/schedules/pause" => "PauseSchedule",
        "/api/v1/schedules/unpause" => "UnpauseSchedule",
        "/api/v1/schedules/backfill" => "BackfillSchedule",
        "/api/v1/schedules/update" => "UpdateSchedule",
        _ => return None,
    })
}

fn hook_operation(
    method: &Method,
    path: &str,
) -> Option<(&'static str, Option<serde_json::Value>)> {
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    let param = |name: &str| {
        query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(key, _)| *key == name)
            .map(|(_, value)| percent_decode(value))
            .unwrap_or_default()
    };
    let op = match (method.as_str(), route) {
        ("POST", "/api/v1/workflows/start") => "StartWorkflowExecution",
        ("POST", "/api/v1/workflows/signal-with-start") => "SignalWithStartWorkflowExecution",
        ("POST", "/api/v1/workflows/history") => "GetWorkflowExecutionHistory",
        ("POST", "/api/v1/workflows/signal") => "SignalWorkflowExecution",
        ("POST", "/api/v1/workflows/query") => "QueryWorkflow",
        ("POST", "/api/v1/workflows/list-open") => "ListOpenWorkflowExecutions",
        ("POST", "/api/v1/workflows/list-closed") => "ListClosedWorkflowExecutions",
        ("POST", "/api/v1/workflows/archived") => "ListArchivedWorkflowExecutions",
        ("POST", "/api/v1/workflows/terminate") => "TerminateWorkflowExecution",
        ("POST", "/api/v1/workflows/restart") => "RestartWorkflowExecution",
        ("POST", "/api/v1/workflows/reset") => "ResetWorkflowExecution",
        ("POST", "/api/v1/workflows/refresh-tasks") => "RefreshWorkflowTasks",
        ("POST", "/api/v1/workflows/diagnose") => "DiagnoseWorkflowExecution",
        ("POST", "/api/v1/workflows/request-cancel") => "RequestCancelWorkflowExecution",
        ("POST", "/api/v1/tasklists/describe") => "DescribeTaskList",
        ("POST", "/api/v1/tasklists/by-domain") => "GetTaskListsByDomain",
        ("POST", "/api/v1/tasklists/partitions") => "ListTaskListPartitions",
        ("POST", "/api/v1/tasks/reset-sticky") => "ResetStickyTaskList",
        ("POST", "/api/v1/tasks/poll-decision") => "PollForDecisionTask",
        ("POST", "/api/v1/tasks/poll-activity") => "PollForActivityTask",
        ("POST", "/api/v1/tasks/respond-decision-complete") => "RespondDecisionTaskCompleted",
        ("POST", "/api/v1/tasks/respond-decision-failed") => "RespondDecisionTaskFailed",
        ("POST", "/api/v1/tasks/respond-query-completed") => "RespondQueryTaskCompleted",
        ("POST", "/api/v1/tasks/respond-activity-complete") => "RespondActivityTaskCompleted",
        ("POST", "/api/v1/tasks/respond-activity-failed") => "RespondActivityTaskFailed",
        ("POST", "/api/v1/tasks/respond-activity-canceled") => "RespondActivityTaskCanceled",
        ("POST", "/api/v1/tasks/record-activity-heartbeat") => "RecordActivityTaskHeartbeat",
        ("GET", "/api/v1/workflows/describe") => {
            return Some((
                "DescribeWorkflowExecution",
                Some(serde_json::json!({
                    "domain": param("domain"),
                    "execution": {"workflowId": param("workflowId"), "runId": param("runId")},
                })),
            ))
        }
        ("GET", "/api/v1/search-attributes") => {
            return Some(("GetSearchAttributes", Some(serde_json::json!({}))))
        }
        _ => return None,
    };
    Some((op, None))
}

#[derive(Clone)]
pub struct Frontend {
    pub state: crate::handlers::AppState,
}

impl Frontend {
    /// Go `accesscontrolled.apiHandler` check for the domain APIs; runs
    /// before request validation like Go's generated wrapper.
    fn authorize_domain_api<T>(
        &self,
        api: &str,
        domain: &str,
        request: &tonic::Request<T>,
    ) -> Result<(), Status> {
        use cadence_service_core::workflow_api::AdminAuthError;
        let Some(auth) = self
            .state
            .workflow_api
            .as_ref()
            .and_then(|a| a.admin_authorizer())
        else {
            return Ok(());
        };
        let token = request
            .metadata()
            .get("cadence-authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        match auth.authorize_domain_api(api, domain, token) {
            Ok(()) => Ok(()),
            Err(AdminAuthError::Denied(m)) => Err(Status::permission_denied(m)),
            Err(AdminAuthError::Failed(m)) => Err(Status::unknown(m)),
        }
    }
    fn activity_token(
        &self,
        domain: &str,
        execution: Option<p::WorkflowExecution>,
        id: &str,
    ) -> Result<Vec<u8>, Status> {
        let executions = self.domain(domain)?;
        let execution = execution
            .map(cv::execution)
            .ok_or_else(|| Status::invalid_argument("Execution is not set on request."))?;
        cadence_api::validation::check_execution(Some(&execution))
            .map_err(|e| Status::invalid_argument(e.message))?;
        if id.is_empty() {
            return Err(Status::invalid_argument(
                "ActivityID is not set on request.",
            ));
        }
        executions
            .activity_token_by_id(domain, &execution, id)
            .map_err(|e| match e {
                cadence_persistence::TaskError::NotFound(e) => detailed_status(
                    tonic::Code::NotFound,
                    e.message,
                    "EntityNotExistsError",
                    p::EntityNotExistsError::default(),
                ),
                cadence_persistence::TaskError::AlreadyCompleted(e) => detailed_status(
                    tonic::Code::NotFound,
                    e.message,
                    "WorkflowExecutionAlreadyCompletedError",
                    p::WorkflowExecutionAlreadyCompletedError {},
                ),
                cadence_persistence::TaskError::InvalidToken(e)
                | cadence_persistence::TaskError::BadDecision(e) => {
                    Status::invalid_argument(e.message)
                }
            })
    }
    fn visible(
        &self,
        domain: &str,
        open: Option<bool>,
    ) -> Result<Vec<st::WorkflowExecutionInfo>, Status> {
        let executions = self.domain(domain)?;
        let mut all = match open {
            Some(value) => executions.list(domain, value),
            None => {
                let mut v = executions.list(domain, true);
                v.extend(executions.list(domain, false));
                v
            }
        };
        all.sort_by(|a, b| {
            b.start_time.cmp(&a.start_time).then_with(|| {
                a.execution
                    .as_ref()
                    .map(|e| &e.run_id)
                    .cmp(&b.execution.as_ref().map(|e| &e.run_id))
            })
        });
        Ok(all)
    }
    /// Dispatch in process: there is no dependency on another server or Go.
    pub(crate) async fn call<T: Serialize, R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        input: &T,
    ) -> Result<R, Status> {
        self.call_with_headers(method, path, input, &[]).await
    }

    pub(crate) async fn call_with_headers<T: Serialize, R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        input: &T,
        headers: &[(&str, &str)],
    ) -> Result<R, Status> {
        // Request-level tracing (gate diagnostics): begin/end with elapsed time.
        let trace_start = std::time::Instant::now();
        eprintln!("FE-TRACE begin {method} {path}");
        let result = self
            .call_with_headers_inner(method.clone(), path, input, headers)
            .await;
        match &result {
            Ok(_) => eprintln!(
                "FE-TRACE end ok {method} {path} {}ms",
                trace_start.elapsed().as_millis()
            ),
            Err(status) => eprintln!(
                "FE-TRACE end err {method} {path} {}ms code={:?} msg={}",
                trace_start.elapsed().as_millis(),
                status.code(),
                status.message()
            ),
        }
        result
    }

    async fn call_with_headers_inner<T: Serialize, R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        input: &T,
        headers: &[(&str, &str)],
    ) -> Result<R, Status> {
        if let Some(api) = self.state.workflow_api.clone() {
            if let Some(operation) = schedule_operation(&method, path) {
                // Go clusterRedirectionHandler wraps schedule APIs with Redirect.
                let request =
                    serde_json::to_value(input).map_err(|e| Status::internal(e.to_string()))?;
                let (mut caller_headers, deadline) = GRPC_CALLER
                    .try_with(|(headers, deadline)| (headers.clone(), *deadline))
                    .unwrap_or_default();
                for (name, value) in headers {
                    caller_headers.insert(name.to_ascii_lowercase(), (*value).to_owned());
                }
                let ctx = cadence_service_core::workflow_api::CallerContext {
                    deadline,
                    headers: caller_headers,
                };
                let forwarded = api
                    .redirect(operation, &request, &ctx)
                    .await
                    .map_err(|error| {
                        let (name, _, message) =
                            cadence_service_core::workflow_api::error_to_thrift(&error);
                        status_from_error_type(name, &message)
                    })?;
                if let Some(value) = forwarded {
                    return serde_json::from_value(value)
                        .map_err(|e| Status::internal(e.to_string()));
                }
            }
            if let Some((operation, override_request)) = hook_operation(&method, path) {
                if api.serves(operation) {
                    let request = match override_request {
                        Some(value) => value,
                        None => serde_json::to_value(input)
                            .map_err(|e| Status::internal(e.to_string()))?,
                    };
                    let (mut caller_headers, deadline) = GRPC_CALLER
                        .try_with(|(headers, deadline)| (headers.clone(), *deadline))
                        .unwrap_or_default();
                    for (name, value) in headers {
                        caller_headers.insert(name.to_ascii_lowercase(), (*value).to_owned());
                    }
                    let ctx = cadence_service_core::workflow_api::CallerContext {
                        deadline,
                        headers: caller_headers,
                    };
                    let value = api.call(operation, request, ctx).await.map_err(|error| {
                        let (name, _, message) =
                            cadence_service_core::workflow_api::error_to_thrift(&error);
                        status_from_error_type(name, &message)
                    })?;
                    return serde_json::from_value(value)
                        .map_err(|e| Status::internal(e.to_string()));
                }
            }
        }
        let body = if method == Method::GET {
            Vec::new()
        } else {
            serde_json::to_vec(input).map_err(|e| Status::internal(e.to_string()))?
        };
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder
            .body(Body::from(body))
            .map_err(|e| Status::internal(e.to_string()))?;
        let response = crate::handlers::router(self.state.clone())
            .layer(axum::extract::DefaultBodyLimit::disable())
            .oneshot(request)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if !status.is_success() {
            let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
            let message = error["message"]
                .as_str()
                .unwrap_or("Cadence request failed");
            return Err(status_from_error_type(
                error["type"].as_str().unwrap_or(""),
                message,
            ));
        }
        serde_json::from_slice(if bytes.is_empty() { b"null" } else { &bytes })
            .map_err(|e| Status::internal(e.to_string()))
    }

    fn domain(&self, name: &str) -> Result<cadence_persistence::InMemoryExecutionStore, Status> {
        if name.is_empty() {
            return Err(Status::invalid_argument("Domain is not set on request."));
        }
        self.state
            .execution_store(name)
            .map_err(|e| Status::not_found(e.message))
    }
}

/// Go `ContextWithIsolationGroup`: the matching poll's isolation group rides
/// to the frontend poll as request metadata, like the worker header does.
pub(crate) fn with_isolation_group<T>(payload: T, isolation_group: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(payload);
    if let Ok(value) = tonic::metadata::AsciiMetadataValue::try_from(isolation_group) {
        request
            .metadata_mut()
            .insert(crate::handlers::ISOLATION_GROUP_HEADER, value);
    }
    request
}

fn isolation_group_metadata<T>(request: &tonic::Request<T>) -> String {
    request
        .metadata()
        .get(crate::handlers::ISOLATION_GROUP_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn page(
    all: Vec<st::WorkflowExecutionInfo>,
    size: i32,
    token: Vec<u8>,
) -> Result<(Vec<st::WorkflowExecutionInfo>, Vec<u8>), Status> {
    let page = cadence_persistence::visibility::page(all, size, &token)
        .map_err(|e| Status::invalid_argument(e.message))?;
    Ok((page.executions, page.next_page_token))
}

fn time_filter(
    all: &mut Vec<st::WorkflowExecutionInfo>,
    filter: Option<p::StartTimeFilter>,
) -> Result<(), Status> {
    fn nanos(v: prost_types::Timestamp) -> Result<i64, Status> {
        v.seconds
            .checked_mul(1_000_000_000)
            .and_then(|s| s.checked_add(i64::from(v.nanos)))
            .ok_or_else(|| {
                Status::invalid_argument("Timestamp exceeds the Cadence nanosecond range.")
            })
    }
    let filter = filter
        .map(|f| {
            Ok::<_, Status>(st::StartTimeFilter {
                earliest_time: f.earliest_time.map(nanos).transpose()?,
                latest_time: f.latest_time.map(nanos).transpose()?,
            })
        })
        .transpose()?;
    cadence_persistence::visibility::filter_start_time(all, filter.as_ref())
        .map_err(|e| Status::invalid_argument(e.message))
}

fn proto_time_filter(f: Option<p::StartTimeFilter>) -> Result<Option<st::StartTimeFilter>, Status> {
    fn nanos(v: prost_types::Timestamp) -> Result<i64, Status> {
        v.seconds
            .checked_mul(1_000_000_000)
            .and_then(|s| s.checked_add(i64::from(v.nanos)))
            .ok_or_else(|| {
                Status::invalid_argument("Timestamp exceeds the Cadence nanosecond range.")
            })
    }
    f.map(|f| {
        Ok(st::StartTimeFilter {
            earliest_time: f.earliest_time.map(nanos).transpose()?,
            latest_time: f.latest_time.map(nanos).transpose()?,
        })
    })
    .transpose()
}

fn visibility_query(all: &mut Vec<st::WorkflowExecutionInfo>, query: &str) -> Result<(), Status> {
    cadence_persistence::visibility::apply(all, query)
        .map_err(|e| Status::invalid_argument(e.message))
}

fn duration(seconds: i64) -> prost_types::Duration {
    prost_types::Duration { seconds, nanos: 0 }
}
fn domain_response(v: st::DescribeDomainResponse) -> p::Domain {
    let mut result = p::Domain {
        failover_version: v.failover_version,
        is_global_domain: v.is_global_domain,
        failover_info: v.failover_info.map(|i| p::FailoverInfo {
            failover_version: i.failover_version,
            failover_start_timestamp: Some(cv::timestamp_nanos(i.failover_start_timestamp)),
            failover_expire_timestamp: Some(cv::timestamp_nanos(i.failover_expire_timestamp)),
            completed_shard_count: i.completed_shard_count,
            pending_shards: i.pending_shards,
        }),
        ..Default::default()
    };
    if let Some(info) = v.domain_info {
        result.id = info.uuid;
        result.name = info.name;
        result.status = info.status.map_or(0, |s| s as i32 + 1);
        result.description = info.description;
        result.owner_email = info.owner_email;
        result.data = info.data.unwrap_or_default();
    }
    if let Some(config) = v.configuration {
        result.workflow_execution_retention_period = Some(duration(
            i64::from(config.workflow_execution_retention_period_in_days) * 86400,
        ));
        result.history_archival_status = config.history_archival_status.map_or(0, |s| s as i32 + 1);
        result.history_archival_uri = config.history_archival_uri;
        result.visibility_archival_status = config
            .visibility_archival_status
            .map_or(0, |s| s as i32 + 1);
        result.visibility_archival_uri = config.visibility_archival_uri;
        result.bad_binaries = config.bad_binaries.map(|v| p::BadBinaries {
            binaries: v
                .binaries
                .unwrap_or_default()
                .into_iter()
                .map(|(k, b)| {
                    let b = b.unwrap_or_default();
                    (
                        k,
                        p::BadBinaryInfo {
                            reason: b.reason,
                            operator: b.operator,
                            created_time: b.created_time_nano.map(cv::timestamp_nanos),
                        },
                    )
                })
                .collect(),
        });
    }
    if let Some(rep) = v.replication_configuration {
        result.active_cluster_name = rep.active_cluster_name;
        result.clusters = rep
            .clusters
            .into_iter()
            .map(|c| p::ClusterReplicationConfiguration {
                cluster_name: c.cluster_name,
            })
            .collect();
        result.active_clusters = rep.active_clusters.map(cv::active_clusters_out);
    }
    result
}

#[tonic::async_trait]
impl p::meta_api_server::MetaApi for Frontend {
    async fn health(
        &self,
        _: tonic::Request<p::HealthRequest>,
    ) -> Result<Response<p::HealthResponse>, Status> {
        Ok(Response::new(p::HealthResponse {
            ok: true,
            message: "OK".to_string(),
        }))
    }
}

#[tonic::async_trait]
impl p::domain_api_server::DomainApi for Frontend {
    async fn update_domain(
        &self,
        request: tonic::Request<p::UpdateDomainRequest>,
    ) -> Result<Response<p::UpdateDomainResponse>, Status> {
        self.authorize_domain_api("UpdateDomain", "", &request)?;
        let r = request.into_inner();
        if r.name.is_empty() {
            return Err(Status::invalid_argument("Domain is not set on request."));
        }
        let selected = |key: &str| {
            r.update_mask
                .as_ref()
                .is_none_or(|m| m.paths.iter().any(|p| p == key))
        };
        let bad_binaries = if selected("bad_binaries") {
            r.bad_binaries.clone().map(|b| t::BadBinaries {
                binaries: Some(
                    b.binaries
                        .into_iter()
                        .map(|(k, b)| {
                            (
                                k,
                                Some(t::BadBinaryInfo {
                                    reason: b.reason,
                                    operator: b.operator,
                                    created_time_nano: b.created_time.and_then(|t| {
                                        t.seconds
                                            .checked_mul(1_000_000_000)
                                            .and_then(|s| s.checked_add(i64::from(t.nanos)))
                                    }),
                                }),
                            )
                        })
                        .collect(),
                ),
            })
        } else {
            None
        };
        let input = st::UpdateDomainRequest {
            name: r.name.clone(),
            security_token: r.security_token.clone(),
            updated_info: Some(st::UpdateDomainInfo {
                description: selected("description").then(|| r.description.clone()),
                owner_email: selected("owner_email").then(|| r.owner_email.clone()),
                data: (selected("data") && !r.data.is_empty()).then(|| r.data.clone()),
            }),
            configuration: Some(st::DomainConfigurationUpdate {
                workflow_execution_retention_period_in_days: if selected(
                    "workflow_execution_retention_period",
                ) {
                    r.workflow_execution_retention_period
                        .map(|d| (d.seconds / 86400) as i32)
                } else {
                    None
                },
                bad_binaries,
                history_archival_status: if selected("history_archival_status") {
                    cv::enum_in(r.history_archival_status, enums::ArchivalStatus::from_i32)?
                } else {
                    None
                },
                history_archival_uri: selected("history_archival_uri")
                    .then(|| r.history_archival_uri.clone()),
                visibility_archival_status: if selected("visibility_archival_status") {
                    cv::enum_in(
                        r.visibility_archival_status,
                        enums::ArchivalStatus::from_i32,
                    )?
                } else {
                    None
                },
                visibility_archival_uri: selected("visibility_archival_uri")
                    .then(|| r.visibility_archival_uri.clone()),
                ..Default::default()
            }),
            replication_configuration: Some(st::DomainReplicationConfigurationUpdate {
                active_cluster_name: selected("active_cluster_name")
                    .then(|| r.active_cluster_name.clone()),
                clusters: (selected("clusters") && !r.clusters.is_empty()).then(|| {
                    r.clusters
                        .iter()
                        .map(|c| t::ClusterReplicationConfiguration {
                            cluster_name: c.cluster_name.clone(),
                        })
                        .collect()
                }),
                active_clusters: if selected("active_clusters") {
                    r.active_clusters.clone().map(cv::active_clusters_in)
                } else {
                    None
                },
            }),
            delete_bad_binary: selected("delete_bad_binary").then(|| r.delete_bad_binary.clone()),
            failover_timeout_in_seconds: if selected("failover_timeout") {
                r.failover_timeout.map(|d| d.seconds as i32)
            } else {
                None
            },
        };
        let v: st::UpdateDomainResponse = self
            .call(Method::PUT, "/api/v1/domains/update", &input)
            .await?;
        Ok(Response::new(p::UpdateDomainResponse {
            domain: Some(domain_response(st::DescribeDomainResponse {
                domain_info: v.domain_info,
                configuration: v.configuration,
                replication_configuration: v.replication_configuration,
                failover_version: v.failover_version,
                is_global_domain: v.is_global_domain,
                failover_info: None,
            })),
        }))
    }

    async fn register_domain(
        &self,
        request: tonic::Request<p::RegisterDomainRequest>,
    ) -> Result<Response<p::RegisterDomainResponse>, Status> {
        self.authorize_domain_api("RegisterDomain", "", &request)?;
        let r = request.into_inner();
        let days = r
            .workflow_execution_retention_period
            .map_or(3, |d| (d.seconds / 86400) as i32);
        let input = t::RegisterDomainRequest {
            name: r.name,
            description: r.description,
            owner_email: r.owner_email,
            workflow_execution_retention_period_in_days: days,
            emit_metric: Some(true),
            clusters: r
                .clusters
                .into_iter()
                .map(|c| t::ClusterReplicationConfiguration {
                    cluster_name: c.cluster_name,
                })
                .collect(),
            active_cluster_name: r.active_cluster_name,
            active_clusters: r.active_clusters.map(cv::active_clusters_in),
            data: Some(r.data),
            is_global_domain: r.is_global_domain,
            history_archival_status: match r.history_archival_status {
                1 => Some(enums::ArchivalStatus::Disabled),
                2 => Some(enums::ArchivalStatus::Enabled),
                _ => None,
            },
            history_archival_uri: r.history_archival_uri,
            visibility_archival_status: match r.visibility_archival_status {
                1 => Some(enums::ArchivalStatus::Disabled),
                2 => Some(enums::ArchivalStatus::Enabled),
                _ => None,
            },
            visibility_archival_uri: r.visibility_archival_uri,
            security_token: r.security_token,
        };
        let _: st::DescribeDomainResponse = self
            .call(Method::POST, "/api/v1/domains/register", &input)
            .await?;
        Ok(Response::new(p::RegisterDomainResponse {}))
    }
    async fn describe_domain(
        &self,
        request: tonic::Request<p::DescribeDomainRequest>,
    ) -> Result<Response<p::DescribeDomainResponse>, Status> {
        let domain = match &request.get_ref().describe_by {
            Some(p::describe_domain_request::DescribeBy::Name(name)) => name.clone(),
            _ => String::new(),
        };
        self.authorize_domain_api("DescribeDomain", &domain, &request)?;
        let input = match request.into_inner().describe_by {
            Some(p::describe_domain_request::DescribeBy::Name(name)) => st::DescribeDomainRequest {
                name: Some(name),
                uuid: None,
            },
            Some(p::describe_domain_request::DescribeBy::Id(id)) => st::DescribeDomainRequest {
                name: None,
                uuid: Some(id),
            },
            None => return Err(Status::invalid_argument("Domain is not set on request.")),
        };
        let result = self
            .state
            .domain_manager
            .describe(&input)
            .map_err(|e| Status::not_found(e.message))?;
        Ok(Response::new(p::DescribeDomainResponse {
            domain: Some(domain_response(result)),
        }))
    }
    async fn list_domains(
        &self,
        request: tonic::Request<p::ListDomainsRequest>,
    ) -> Result<Response<p::ListDomainsResponse>, Status> {
        let r = request.into_inner();
        let v = self
            .state
            .domain_manager
            .list_page(&st::ListDomainsRequest {
                page_size: r.page_size,
                next_page_token: r.next_page_token,
            })
            .map_err(|e| Status::invalid_argument(e.message))?;
        Ok(Response::new(p::ListDomainsResponse {
            domains: v.domains.into_iter().map(domain_response).collect(),
            next_page_token: v.next_page_token,
        }))
    }
    async fn failover_domain(
        &self,
        request: tonic::Request<p::FailoverDomainRequest>,
    ) -> Result<Response<p::FailoverDomainResponse>, Status> {
        self.authorize_domain_api("FailoverDomain", &request.get_ref().domain_name, &request)?;
        let r = request.into_inner();
        let input = cv::failover_in(r);
        cadence_api::validation::validate_failover_domain_request(Some(&input))
            .map_err(|e| Status::invalid_argument(e.message))?;
        let v: cadence_api::types::FailoverDomainResponse = self
            .call(Method::POST, "/api/v1/domains/failover", &input)
            .await?;
        Ok(Response::new(p::FailoverDomainResponse {
            domain: Some(domain_response(st::DescribeDomainResponse {
                domain_info: v.domain_info,
                configuration: v.configuration,
                replication_configuration: v.replication_configuration,
                failover_version: v.failover_version,
                is_global_domain: v.is_global_domain,
                failover_info: None,
            })),
        }))
    }
    async fn list_failover_history(
        &self,
        request: tonic::Request<p::ListFailoverHistoryRequest>,
    ) -> Result<Response<p::ListFailoverHistoryResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::types::ListFailoverHistoryRequest {
            filters: r
                .filters
                .map(|f| cadence_api::types::ListFailoverHistoryRequestFilters {
                    domain_id: f.domain_id,
                }),
            pagination: r.pagination.map(|p| cadence_api::types::PaginationOptions {
                page_size: Some(p.page_size),
                next_page_token: p.next_page_token,
            }),
        };
        if input
            .filters
            .as_ref()
            .is_none_or(|f| f.domain_id.is_empty())
        {
            return Err(Status::invalid_argument("DomainID is required in filters."));
        }
        let v: cadence_api::types::ListFailoverHistoryResponse = self
            .call(Method::POST, "/api/v1/domains/failover-history", &input)
            .await?;
        Ok(Response::new(p::ListFailoverHistoryResponse {
            failover_events: v
                .failover_events
                .into_iter()
                .map(cv::failover_event_out)
                .collect(),
            next_page_token: v.next_page_token,
        }))
    }
    async fn deprecate_domain(
        &self,
        request: tonic::Request<p::DeprecateDomainRequest>,
    ) -> Result<Response<p::DeprecateDomainResponse>, Status> {
        self.authorize_domain_api("DeprecateDomain", "", &request)?;
        let r = request.into_inner();
        let _: serde_json::Value = self
            .call(
                Method::POST,
                "/api/v1/domains/deprecate",
                &st::DeprecateDomainRequest {
                    name: r.name,
                    security_token: r.security_token,
                },
            )
            .await?;
        Ok(Response::new(p::DeprecateDomainResponse {}))
    }
    async fn delete_domain(
        &self,
        request: tonic::Request<p::DeleteDomainRequest>,
    ) -> Result<Response<p::DeleteDomainResponse>, Status> {
        self.authorize_domain_api("DeleteDomain", "", &request)?;
        let r = request.into_inner();
        let _: serde_json::Value = self
            .call(
                Method::POST,
                "/api/v1/domains/delete",
                &st::DeleteDomainRequest {
                    name: r.name,
                    security_token: r.security_token,
                },
            )
            .await?;
        Ok(Response::new(p::DeleteDomainResponse {}))
    }
}

#[tonic::async_trait]
impl p::workflow_api_server::WorkflowApi for Frontend {
    async fn start_workflow_execution(
        &self,
        request: tonic::Request<p::StartWorkflowExecutionRequest>,
    ) -> Result<Response<p::StartWorkflowExecutionResponse>, Status> {
        let input = cv::start(request.into_inner())?;
        if self
            .state
            .workflow_api
            .as_ref()
            .is_some_and(|api| api.serves("StartWorkflowExecution"))
        {
            let result: t::StartWorkflowExecutionResponse = self
                .call_with_headers(Method::POST, "/api/v1/workflows/start", &input, &[])
                .await?;
            return Ok(Response::new(p::StartWorkflowExecutionResponse {
                run_id: result.run_id,
            }));
        }
        let executions = self.domain(&input.domain)?;
        cadence_api::validation::validate_start_workflow_execution_request(
            Some(&input),
            &self.state.limits,
        )
        .map_err(|e| Status::invalid_argument(e.message))?;
        let result = executions.start(&input).map_err(|e| match e {
            cadence_persistence::StartError::AlreadyStarted(e) => detailed_status(
                tonic::Code::AlreadyExists,
                e.message,
                "WorkflowExecutionAlreadyStartedError",
                p::WorkflowExecutionAlreadyStartedError {
                    start_request_id: e.start_request_id,
                    run_id: e.run_id,
                },
            ),
            cadence_persistence::StartError::InvalidArgument(message) => {
                Status::invalid_argument(message)
            }
        })?;
        Ok(Response::new(p::StartWorkflowExecutionResponse {
            run_id: result.run_id,
        }))
    }
    async fn signal_workflow_execution(
        &self,
        request: tonic::Request<p::SignalWorkflowExecutionRequest>,
    ) -> Result<Response<p::SignalWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        let input = t::SignalWorkflowExecutionRequest {
            domain: r.domain,
            workflow_execution: r.workflow_execution.map(cv::execution),
            signal_name: r.signal_name,
            input: cv::data(r.signal_input),
            identity: r.identity,
            request_id: r.request_id,
            control: r.control,
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/workflows/signal", &input)
            .await?;
        Ok(Response::new(p::SignalWorkflowExecutionResponse {}))
    }
    async fn start_workflow_execution_async(
        &self,
        request: tonic::Request<p::StartWorkflowExecutionAsyncRequest>,
    ) -> Result<Response<p::StartWorkflowExecutionAsyncResponse>, Status> {
        let r = request.into_inner();
        let inner = r
            .request
            .map(cv::start)
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        let executions = self.domain(&inner.domain)?;
        cadence_api::validation::validate_start_workflow_execution_request(
            Some(&inner),
            &self.state.limits,
        )
        .map_err(|e| Status::invalid_argument(e.message))?;
        executions.enqueue_async_start(inner);
        Ok(Response::new(p::StartWorkflowExecutionAsyncResponse {}))
    }
    async fn signal_with_start_workflow_execution_async(
        &self,
        request: tonic::Request<p::SignalWithStartWorkflowExecutionAsyncRequest>,
    ) -> Result<Response<p::SignalWithStartWorkflowExecutionAsyncResponse>, Status> {
        let r = request.into_inner();
        // Rebuild the Thrift signal-with-start shape from the proto wrapper,
        // mirroring the sync adapter below.
        let inner_proto = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        let start = cv::start(
            inner_proto
                .start_request
                .ok_or_else(|| Status::invalid_argument("Start request is not set."))?,
        )?;
        let input = st::SignalWithStartWorkflowExecutionRequest {
            domain: start.domain,
            workflow_id: start.workflow_id,
            workflow_type: start.workflow_type,
            task_list: start.task_list,
            input: start.input,
            execution_start_to_close_timeout_seconds: start
                .execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: start.task_start_to_close_timeout_seconds,
            identity: start.identity,
            request_id: start.request_id,
            workflow_id_reuse_policy: start.workflow_id_reuse_policy,
            retry_policy: start.retry_policy,
            cron_schedule: start.cron_schedule,
            memo: start.memo,
            search_attributes: start.search_attributes,
            header: start.header,
            delay_start_seconds: start.delay_start_seconds,
            jitter_start_seconds: start.jitter_start_seconds,
            first_run_at_timestamp: start.first_run_at_timestamp,
            cron_overlap_policy: start.cron_overlap_policy,
            signal_name: inner_proto.signal_name,
            signal_input: cv::data(inner_proto.signal_input),
            control: inner_proto.control,
        };
        let executions = self.domain(&input.domain)?;
        cadence_api::validation::validate_signal_with_start_request(
            Some(&input),
            &self.state.limits,
        )
        .map_err(|e| Status::invalid_argument(e.message))?;
        executions.enqueue_async_signal_with_start(input);
        Ok(Response::new(
            p::SignalWithStartWorkflowExecutionAsyncResponse {},
        ))
    }
    async fn signal_with_start_workflow_execution(
        &self,
        request: tonic::Request<p::SignalWithStartWorkflowExecutionRequest>,
    ) -> Result<Response<p::SignalWithStartWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        let start = cv::start(
            r.start_request
                .ok_or_else(|| Status::invalid_argument("Start request is not set."))?,
        )?;
        let input = st::SignalWithStartWorkflowExecutionRequest {
            domain: start.domain,
            workflow_id: start.workflow_id,
            workflow_type: start.workflow_type,
            task_list: start.task_list,
            input: start.input,
            execution_start_to_close_timeout_seconds: start
                .execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: start.task_start_to_close_timeout_seconds,
            identity: start.identity,
            request_id: start.request_id,
            workflow_id_reuse_policy: start.workflow_id_reuse_policy,
            retry_policy: start.retry_policy,
            cron_schedule: start.cron_schedule,
            memo: start.memo,
            search_attributes: start.search_attributes,
            header: start.header,
            delay_start_seconds: start.delay_start_seconds,
            jitter_start_seconds: start.jitter_start_seconds,
            first_run_at_timestamp: start.first_run_at_timestamp,
            cron_overlap_policy: start.cron_overlap_policy,
            signal_name: r.signal_name,
            signal_input: cv::data(r.signal_input),
            control: r.control,
        };
        if self
            .state
            .workflow_api
            .as_ref()
            .is_some_and(|api| api.serves("SignalWithStartWorkflowExecution"))
        {
            let result: t::StartWorkflowExecutionResponse = self
                .call_with_headers(
                    Method::POST,
                    "/api/v1/workflows/signal-with-start",
                    &input,
                    &[],
                )
                .await?;
            return Ok(Response::new(p::SignalWithStartWorkflowExecutionResponse {
                run_id: result.run_id,
            }));
        }
        let executions = self.domain(&input.domain)?;
        cadence_api::validation::validate_signal_with_start_request(
            Some(&input),
            &self.state.limits,
        )
        .map_err(|e| Status::invalid_argument(e.message))?;
        let v = executions.signal_with_start(&input).map_err(|e| match e {
            cadence_persistence::StartError::AlreadyStarted(e) => detailed_status(
                tonic::Code::AlreadyExists,
                e.message,
                "WorkflowExecutionAlreadyStartedError",
                p::WorkflowExecutionAlreadyStartedError {
                    start_request_id: e.start_request_id,
                    run_id: e.run_id,
                },
            ),
            cadence_persistence::StartError::InvalidArgument(message) => {
                Status::invalid_argument(message)
            }
        })?;
        Ok(Response::new(p::SignalWithStartWorkflowExecutionResponse {
            run_id: v.run_id,
        }))
    }
    async fn query_workflow(
        &self,
        request: tonic::Request<p::QueryWorkflowRequest>,
    ) -> Result<Response<p::QueryWorkflowResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::tasks::QueryWorkflowRequest {
            domain: r.domain,
            execution: r.workflow_execution.map(cv::execution),
            query: r.query.map(|q| cadence_api::tasks::WorkflowQuery {
                query_type: q.query_type,
                query_args: cv::data(q.query_args),
            }),
            query_reject_condition: cv::enum_in(
                r.query_reject_condition,
                enums::QueryRejectCondition::from_i32,
            )?,
            query_consistency_level: cv::enum_in(
                r.query_consistency_level,
                enums::QueryConsistencyLevel::from_i32,
            )?,
        };
        let v: cadence_api::tasks::QueryWorkflowResponse = self
            .call(Method::POST, "/api/v1/workflows/query", &input)
            .await?;
        Ok(Response::new(p::QueryWorkflowResponse {
            query_result: cv::payload(v.query_result),
            query_rejected: v.query_rejected.map(|q| p::QueryRejected {
                close_status: q.close_status.map_or(0, |s| s as i32 + 1),
            }),
        }))
    }
    async fn terminate_workflow_execution(
        &self,
        request: tonic::Request<p::TerminateWorkflowExecutionRequest>,
    ) -> Result<Response<p::TerminateWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        let input = t::TerminateWorkflowExecutionRequest {
            domain: r.domain,
            workflow_execution: r.workflow_execution.map(cv::execution),
            reason: r.reason,
            details: cv::data(r.details),
            identity: r.identity,
            first_execution_run_id: r.first_execution_run_id,
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/workflows/terminate", &input)
            .await?;
        Ok(Response::new(p::TerminateWorkflowExecutionResponse {}))
    }
    async fn restart_workflow_execution(
        &self,
        request: tonic::Request<p::RestartWorkflowExecutionRequest>,
    ) -> Result<Response<p::RestartWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::types::RestartWorkflowExecutionRequest {
            domain: r.domain,
            workflow_execution: r.workflow_execution.map(cv::execution),
            reason: r.reason,
            identity: r.identity,
        };
        if input
            .workflow_execution
            .as_ref()
            .is_none_or(|e| e.workflow_id.is_empty() || e.run_id.is_empty())
        {
            return Err(Status::invalid_argument("Execution is not set on request."));
        }
        let v: cadence_api::types::RestartWorkflowExecutionResponse = self
            .call(Method::POST, "/api/v1/workflows/restart", &input)
            .await?;
        Ok(Response::new(p::RestartWorkflowExecutionResponse {
            run_id: v.run_id,
        }))
    }
    async fn reset_workflow_execution(
        &self,
        request: tonic::Request<p::ResetWorkflowExecutionRequest>,
    ) -> Result<Response<p::ResetWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::types::ResetWorkflowExecutionRequest {
            domain: r.domain,
            workflow_execution: r.workflow_execution.map(cv::execution),
            reason: r.reason,
            decision_finish_event_id: r.decision_finish_event_id,
            request_id: r.request_id,
            skip_signal_reapply: r.skip_signal_reapply,
        };
        if input
            .workflow_execution
            .as_ref()
            .is_none_or(|e| e.workflow_id.is_empty() || e.run_id.is_empty())
        {
            return Err(Status::invalid_argument("Execution is not set on request."));
        }
        let v: cadence_api::types::ResetWorkflowExecutionResponse = self
            .call(Method::POST, "/api/v1/workflows/reset", &input)
            .await?;
        Ok(Response::new(p::ResetWorkflowExecutionResponse {
            run_id: v.run_id,
        }))
    }
    async fn refresh_workflow_tasks(
        &self,
        request: tonic::Request<p::RefreshWorkflowTasksRequest>,
    ) -> Result<Response<p::RefreshWorkflowTasksResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::types::RefreshWorkflowTasksRequest {
            domain: r.domain,
            execution: r.workflow_execution.map(cv::execution),
        };
        if input
            .execution
            .as_ref()
            .is_none_or(|e| e.workflow_id.is_empty())
        {
            return Err(Status::invalid_argument("Execution is not set on request."));
        }
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/workflows/refresh-tasks", &input)
            .await?;
        Ok(Response::new(p::RefreshWorkflowTasksResponse {}))
    }
    async fn diagnose_workflow_execution(
        &self,
        request: tonic::Request<p::DiagnoseWorkflowExecutionRequest>,
    ) -> Result<Response<p::DiagnoseWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::types::DiagnoseWorkflowExecutionRequest {
            domain: r.domain,
            workflow_execution: r.workflow_execution.map(cv::execution),
            identity: r.identity,
        };
        if input
            .workflow_execution
            .as_ref()
            .is_none_or(|e| e.workflow_id.is_empty() || e.run_id.is_empty())
        {
            return Err(Status::invalid_argument("Execution is not set on request."));
        }
        let v: cadence_api::types::DiagnoseWorkflowExecutionResponse = self
            .call(Method::POST, "/api/v1/workflows/diagnose", &input)
            .await?;
        Ok(Response::new(p::DiagnoseWorkflowExecutionResponse {
            domain: v.domain,
            diagnostic_workflow_execution: v.diagnostic_workflow_execution.map(cv::execution_out),
        }))
    }
    async fn request_cancel_workflow_execution(
        &self,
        request: tonic::Request<p::RequestCancelWorkflowExecutionRequest>,
    ) -> Result<Response<p::RequestCancelWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        let input = st::RequestCancelWorkflowExecutionRequest {
            domain: r.domain,
            workflow_execution: r.workflow_execution.map(cv::execution),
            identity: r.identity,
            request_id: r.request_id,
            cause: r.cause,
            first_execution_run_id: r.first_execution_run_id,
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/workflows/request-cancel", &input)
            .await?;
        Ok(Response::new(p::RequestCancelWorkflowExecutionResponse {}))
    }
    async fn describe_workflow_execution(
        &self,
        request: tonic::Request<p::DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<p::DescribeWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        let execution = r
            .workflow_execution
            .ok_or_else(|| Status::invalid_argument("Execution is not set on request."))?;
        let path = query(
            "/api/v1/workflows/describe",
            &[
                ("domain", &r.domain),
                ("workflowId", &execution.workflow_id),
                ("runId", &execution.run_id),
            ],
        );
        let result: st::DescribeWorkflowExecutionResponse =
            self.call(Method::GET, &path, &()).await?;
        Ok(Response::new(cv::describe(result)))
    }
    async fn get_cluster_info(
        &self,
        _: tonic::Request<p::GetClusterInfoRequest>,
    ) -> Result<Response<p::GetClusterInfoResponse>, Status> {
        Ok(Response::new(p::GetClusterInfoResponse {
            supported_client_versions: Some(p::SupportedClientVersions::default()),
        }))
    }
    async fn get_workflow_execution_history(
        &self,
        request: tonic::Request<p::GetWorkflowExecutionHistoryRequest>,
    ) -> Result<Response<p::GetWorkflowExecutionHistoryResponse>, Status> {
        if self
            .state
            .workflow_api
            .as_ref()
            .is_some_and(|api| api.serves("GetWorkflowExecutionHistory"))
        {
            let r = request.get_ref();
            let input = st::GetWorkflowExecutionHistoryRequest {
                domain: r.domain.clone(),
                execution: r.workflow_execution.clone().map(cv::execution),
                maximum_page_size: r.page_size,
                next_page_token: r.next_page_token.clone(),
                wait_for_new_event: r.wait_for_new_event,
                history_event_filter_type: cv::enum_in(
                    r.history_event_filter_type,
                    enums::HistoryEventFilterType::from_i32,
                )?,
                skip_archival: r.skip_archival,
                query_consistency_level: cv::enum_in(
                    r.query_consistency_level,
                    enums::QueryConsistencyLevel::from_i32,
                )?,
            };
            let v: st::GetWorkflowExecutionHistoryResponse = self
                .call_with_headers(Method::POST, "/api/v1/workflows/history", &input, &[])
                .await?;
            return Ok(Response::new(p::GetWorkflowExecutionHistoryResponse {
                history: v.history.map(cv::history).transpose()?,
                raw_history: v
                    .raw_history
                    .into_iter()
                    .map(|b| p::DataBlob {
                        encoding_type: match b.encoding_type {
                            Some(enums::EncodingType::ThriftRW) => 1,
                            Some(enums::EncodingType::JSON) => 2,
                            _ => 0,
                        },
                        data: b.data,
                    })
                    .collect(),
                next_page_token: v.next_page_token,
                archived: v.archived,
            }));
        }
        let r = request.into_inner();
        let executions = self.domain(&r.domain)?;
        let execution = r
            .workflow_execution
            .map(cv::execution)
            .ok_or_else(|| Status::invalid_argument("Execution is not set on request."))?;
        cadence_api::validation::check_execution(Some(&execution))
            .map_err(|e| Status::invalid_argument(e.message))?;
        let filter = cv::enum_in(
            r.history_event_filter_type,
            enums::HistoryEventFilterType::from_i32,
        )?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let v = executions
                .history_page(
                    &r.domain,
                    &execution,
                    r.page_size,
                    &r.next_page_token,
                    filter,
                )
                .map_err(|e| match e {
                    cadence_persistence::HistoryPageError::NotFound(e) => detailed_status(
                        tonic::Code::NotFound,
                        e.message,
                        "EntityNotExistsError",
                        p::EntityNotExistsError::default(),
                    ),
                    cadence_persistence::HistoryPageError::InvalidToken(e) => {
                        Status::invalid_argument(e.message)
                    }
                })?;
            if !r.wait_for_new_event
                || v.history.as_ref().is_some_and(|h| !h.events.is_empty())
                || !r.next_page_token.is_empty()
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(Response::new(p::GetWorkflowExecutionHistoryResponse {
                    history: v.history.map(cv::history).transpose()?,
                    raw_history: v
                        .raw_history
                        .into_iter()
                        .map(|b| p::DataBlob {
                            encoding_type: match b.encoding_type {
                                Some(enums::EncodingType::ThriftRW) => 1,
                                Some(enums::EncodingType::JSON) => 2,
                                _ => 0,
                            },
                            data: b.data,
                        })
                        .collect(),
                    next_page_token: v.next_page_token,
                    archived: v.archived,
                }));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    async fn describe_task_list(
        &self,
        request: tonic::Request<p::DescribeTaskListRequest>,
    ) -> Result<Response<p::DescribeTaskListResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::tasks::DescribeTaskListRequest {
            domain: r.domain,
            task_list: r.task_list.clone().map(cv::task_list).transpose()?,
            task_list_type: cv::enum_in(r.task_list_type, enums::TaskListType::from_i32)?,
            include_task_list_status: r.include_task_list_status,
        };
        let v: cadence_api::tasks::DescribeTaskListResponse = self
            .call(Method::POST, "/api/v1/tasklists/describe", &input)
            .await?;
        let mut out = cv::describe_task_list_out(v);
        out.task_list = r.task_list;
        Ok(Response::new(out))
    }
    async fn get_task_lists_by_domain(
        &self,
        request: tonic::Request<p::GetTaskListsByDomainRequest>,
    ) -> Result<Response<p::GetTaskListsByDomainResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::tasks::GetTaskListsByDomainRequest { domain: r.domain };
        let v: cadence_api::tasks::GetTaskListsByDomainResponse = self
            .call(Method::POST, "/api/v1/tasklists/by-domain", &input)
            .await?;
        let map = |m: Option<
            std::collections::HashMap<String, cadence_api::tasks::DescribeTaskListResponse>,
        >| {
            m.unwrap_or_default()
                .into_iter()
                .map(|(k, desc)| {
                    let task_list = desc.task_list.clone().map(|t| p::TaskList {
                        name: t.name,
                        ..Default::default()
                    });
                    let mut out = cv::describe_task_list_out(desc);
                    out.task_list = task_list;
                    (k, out)
                })
                .collect()
        };
        Ok(Response::new(p::GetTaskListsByDomainResponse {
            decision_task_list_map: map(v.decision_task_list_map),
            activity_task_list_map: map(v.activity_task_list_map),
        }))
    }
    async fn list_task_list_partitions(
        &self,
        request: tonic::Request<p::ListTaskListPartitionsRequest>,
    ) -> Result<Response<p::ListTaskListPartitionsResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::tasks::ListTaskListPartitionsRequest {
            domain: r.domain,
            task_list: r.task_list.clone().map(cv::task_list).transpose()?,
        };
        if input.task_list.as_ref().is_none_or(|t| t.name.is_empty()) {
            return Err(Status::invalid_argument("TaskList is not set."));
        }
        let v: cadence_api::tasks::ListTaskListPartitionsResponse = self
            .call(Method::POST, "/api/v1/tasklists/partitions", &input)
            .await?;
        let partitions = |items: Vec<cadence_api::tasks::TaskListPartitionMetadata>| {
            items
                .into_iter()
                .map(|m| p::TaskListPartitionMetadata {
                    key: m.key,
                    owner_host_name: m.owner_host_name,
                })
                .collect()
        };
        Ok(Response::new(p::ListTaskListPartitionsResponse {
            activity_task_list_partitions: partitions(v.activity_task_list_partitions),
            decision_task_list_partitions: partitions(v.decision_task_list_partitions),
        }))
    }
}
#[tonic::async_trait]
impl p::schedule_api_server::ScheduleApi for Frontend {
    async fn create_schedule(
        &self,
        request: tonic::Request<p::CreateScheduleRequest>,
    ) -> Result<Response<p::CreateScheduleResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::CreateScheduleRequest {
            domain: r.domain,
            schedule_id: r.schedule_id,
            spec: r.spec.map(cv::schedule_spec_in),
            action: r.action.map(cv::schedule_action_in).transpose()?,
            policies: r.policies.map(cv::schedule_policies_in).transpose()?,
            memo: r.memo.map(|m| t::Memo {
                fields: Some(m.fields.into_iter().map(|(k, v)| (k, v.data)).collect()),
            }),
            search_attributes: r.search_attributes.map(|m| t::SearchAttributes {
                indexed_fields: Some(
                    m.indexed_fields
                        .into_iter()
                        .map(|(k, v)| (k, v.data))
                        .collect(),
                ),
            }),
            state: r.state.map(|s| sch::ScheduleState {
                paused: s.paused,
                pause_info: s.pause_info.map(|i| sch::SchedulePauseInfo {
                    reason: i.reason,
                    paused_time_nano: cv::nanos(i.paused_at),
                    paused_by: i.paused_by,
                }),
            }),
        };
        let v: sch::CreateScheduleResponse = self
            .call(Method::POST, "/api/v1/schedules/create", &input)
            .await?;
        Ok(Response::new(p::CreateScheduleResponse {
            schedule_id: v.schedule_id,
        }))
    }
    async fn describe_schedule(
        &self,
        request: tonic::Request<p::DescribeScheduleRequest>,
    ) -> Result<Response<p::DescribeScheduleResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::DescribeScheduleRequest {
            domain: r.domain,
            schedule_id: r.schedule_id,
        };
        let v: sch::DescribeScheduleResponse = self
            .call(Method::POST, "/api/v1/schedules/describe", &input)
            .await?;
        Ok(Response::new(p::DescribeScheduleResponse {
            spec: v.spec.map(cv::schedule_spec_out),
            action: v.action.map(cv::schedule_action_out),
            policies: v.policies.map(cv::schedule_policies_out),
            state: v.state.map(cv::schedule_state_out),
            info: v.info.map(cv::schedule_info_out),
            memo: v.memo.map(|m| p::Memo {
                fields: m
                    .fields
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| (k, p::Payload { data: v }))
                    .collect(),
            }),
            search_attributes: v.search_attributes.map(|m| p::SearchAttributes {
                indexed_fields: m
                    .indexed_fields
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| (k, p::Payload { data: v }))
                    .collect(),
            }),
        }))
    }
    async fn list_schedules(
        &self,
        request: tonic::Request<p::ListSchedulesRequest>,
    ) -> Result<Response<p::ListSchedulesResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::ListSchedulesRequest {
            domain: r.domain,
            page_size: r.page_size,
            next_page_token: r.next_page_token,
        };
        let v: sch::ListSchedulesResponse = self
            .call(Method::POST, "/api/v1/schedules/list", &input)
            .await?;
        Ok(Response::new(p::ListSchedulesResponse {
            schedules: v
                .schedules
                .into_iter()
                .map(cv::schedule_entry_out)
                .collect(),
            next_page_token: v.next_page_token,
        }))
    }
    async fn delete_schedule(
        &self,
        request: tonic::Request<p::DeleteScheduleRequest>,
    ) -> Result<Response<p::DeleteScheduleResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::DeleteScheduleRequest {
            domain: r.domain,
            schedule_id: r.schedule_id,
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/schedules/delete", &input)
            .await?;
        Ok(Response::new(p::DeleteScheduleResponse {}))
    }
    async fn pause_schedule(
        &self,
        request: tonic::Request<p::PauseScheduleRequest>,
    ) -> Result<Response<p::PauseScheduleResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::PauseScheduleRequest {
            domain: r.domain,
            schedule_id: r.schedule_id,
            reason: r.reason,
            identity: r.identity,
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/schedules/pause", &input)
            .await?;
        Ok(Response::new(p::PauseScheduleResponse {}))
    }
    async fn unpause_schedule(
        &self,
        request: tonic::Request<p::UnpauseScheduleRequest>,
    ) -> Result<Response<p::UnpauseScheduleResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::UnpauseScheduleRequest {
            domain: r.domain,
            schedule_id: r.schedule_id,
            reason: r.reason,
            catch_up_policy: cv::enum_in(
                r.catch_up_policy,
                enums::ScheduleCatchUpPolicy::from_i32,
            )?,
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/schedules/unpause", &input)
            .await?;
        Ok(Response::new(p::UnpauseScheduleResponse {}))
    }
    async fn backfill_schedule(
        &self,
        request: tonic::Request<p::BackfillScheduleRequest>,
    ) -> Result<Response<p::BackfillScheduleResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::BackfillScheduleRequest {
            domain: r.domain,
            schedule_id: r.schedule_id,
            start_time_nano: cv::nanos(r.start_time),
            end_time_nano: cv::nanos(r.end_time),
            overlap_policy: cv::enum_in(r.overlap_policy, enums::ScheduleOverlapPolicy::from_i32)?,
            backfill_id: r.backfill_id,
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/schedules/backfill", &input)
            .await?;
        Ok(Response::new(p::BackfillScheduleResponse {}))
    }
    async fn update_schedule(
        &self,
        request: tonic::Request<p::UpdateScheduleRequest>,
    ) -> Result<Response<p::UpdateScheduleResponse>, Status> {
        use cadence_api::schedule as sch;
        let r = request.into_inner();
        let input = sch::UpdateScheduleRequest {
            domain: r.domain,
            schedule_id: r.schedule_id,
            spec: r.spec.map(cv::schedule_spec_in),
            action: r.action.map(cv::schedule_action_in).transpose()?,
            policies: r.policies.map(cv::schedule_policies_in).transpose()?,
            search_attributes: r.search_attributes.map(|m| t::SearchAttributes {
                indexed_fields: Some(
                    m.indexed_fields
                        .into_iter()
                        .map(|(k, v)| (k, v.data))
                        .collect(),
                ),
            }),
        };
        let _: serde_json::Value = self
            .call(Method::POST, "/api/v1/schedules/update", &input)
            .await?;
        Ok(Response::new(p::UpdateScheduleResponse {}))
    }
}
#[tonic::async_trait]
impl p::semaphore_api_server::SemaphoreApi for Frontend {
    async fn create_semaphore(
        &self,
        request: tonic::Request<p::CreateSemaphoreRequest>,
    ) -> Result<Response<p::CreateSemaphoreResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::types::CreateSemaphoreRequest {
            domain: r.domain,
            semaphore_name: r.semaphore_name,
            capacity: r.capacity,
            bucket_capacity: r.bucket_capacity,
        };
        cadence_api::validation::validate_create_semaphore_request(Some(&input))
            .map_err(|e| Status::invalid_argument(e.message))?;
        let v: cadence_api::types::CreateSemaphoreResponse = self
            .call(Method::POST, "/api/v1/semaphores/create", &input)
            .await?;
        Ok(Response::new(p::CreateSemaphoreResponse {
            semaphore: v.semaphore.map(|m| p::Semaphore {
                semaphore_name: m.semaphore_name,
                capacity: m.capacity,
                bucket_capacity: m.bucket_capacity,
            }),
        }))
    }
}
#[tonic::async_trait]
impl p::visibility_api_server::VisibilityApi for Frontend {
    async fn list_workflow_executions(
        &self,
        request: tonic::Request<p::ListWorkflowExecutionsRequest>,
    ) -> Result<Response<p::ListWorkflowExecutionsResponse>, Status> {
        let r = request.into_inner();
        let mut all = self.visible(&r.domain, None)?;
        visibility_query(&mut all, &r.query)?;
        let (items, token) = page(all, r.page_size, r.next_page_token)?;
        Ok(Response::new(p::ListWorkflowExecutionsResponse {
            executions: items.into_iter().map(cv::info).collect(),
            next_page_token: token,
        }))
    }
    async fn list_open_workflow_executions(
        &self,
        request: tonic::Request<p::ListOpenWorkflowExecutionsRequest>,
    ) -> Result<Response<p::ListOpenWorkflowExecutionsResponse>, Status> {
        if self
            .state
            .workflow_api
            .as_ref()
            .is_some_and(|api| api.serves("ListOpenWorkflowExecutions"))
        {
            let r = request.get_ref();
            let input = st::ListOpenWorkflowExecutionsRequest {
                domain: r.domain.clone(),
                maximum_page_size: r.page_size,
                next_page_token: r.next_page_token.clone(),
                start_time_filter: proto_time_filter(r.start_time_filter)?,
                execution_filter: match &r.filters {
                    Some(p::list_open_workflow_executions_request::Filters::ExecutionFilter(f)) => {
                        Some(st::WorkflowExecutionFilter {
                            workflow_id: f.workflow_id.clone(),
                            run_id: f.run_id.clone(),
                        })
                    }
                    _ => None,
                },
                type_filter: match &r.filters {
                    Some(p::list_open_workflow_executions_request::Filters::TypeFilter(f)) => {
                        Some(st::WorkflowTypeFilter {
                            name: f.name.clone(),
                        })
                    }
                    _ => None,
                },
            };
            let v: st::ListOpenWorkflowExecutionsResponse = self
                .call_with_headers(Method::POST, "/api/v1/workflows/list-open", &input, &[])
                .await?;
            return Ok(Response::new(p::ListOpenWorkflowExecutionsResponse {
                executions: v.executions.into_iter().map(cv::info_nanos).collect(),
                next_page_token: v.next_page_token,
            }));
        }
        let r = request.into_inner();
        let mut all = self.visible(&r.domain, Some(true))?;
        time_filter(&mut all, r.start_time_filter)?;
        match r.filters {
            Some(p::list_open_workflow_executions_request::Filters::ExecutionFilter(f)) => all
                .retain(|e| {
                    e.execution
                        .as_ref()
                        .is_some_and(|e| e.workflow_id == f.workflow_id)
                }),
            Some(p::list_open_workflow_executions_request::Filters::TypeFilter(f)) => {
                all.retain(|e| e.workflow_type.as_ref().is_some_and(|e| e.name == f.name))
            }
            None => {}
        }
        let (items, token) = page(all, r.page_size, r.next_page_token)?;
        Ok(Response::new(p::ListOpenWorkflowExecutionsResponse {
            executions: items.into_iter().map(cv::info).collect(),
            next_page_token: token,
        }))
    }
    async fn list_closed_workflow_executions(
        &self,
        request: tonic::Request<p::ListClosedWorkflowExecutionsRequest>,
    ) -> Result<Response<p::ListClosedWorkflowExecutionsResponse>, Status> {
        if self
            .state
            .workflow_api
            .as_ref()
            .is_some_and(|api| api.serves("ListClosedWorkflowExecutions"))
        {
            let r = request.get_ref();
            let input = st::ListClosedWorkflowExecutionsRequest {
                domain: r.domain.clone(),
                maximum_page_size: r.page_size,
                next_page_token: r.next_page_token.clone(),
                start_time_filter: proto_time_filter(r.start_time_filter)?,
                execution_filter: match &r.filters {
                    Some(p::list_closed_workflow_executions_request::Filters::ExecutionFilter(
                        f,
                    )) => Some(st::WorkflowExecutionFilter {
                        workflow_id: f.workflow_id.clone(),
                        run_id: f.run_id.clone(),
                    }),
                    _ => None,
                },
                type_filter: match &r.filters {
                    Some(p::list_closed_workflow_executions_request::Filters::TypeFilter(f)) => {
                        Some(st::WorkflowTypeFilter {
                            name: f.name.clone(),
                        })
                    }
                    _ => None,
                },
                status_filter: match &r.filters {
                    Some(p::list_closed_workflow_executions_request::Filters::StatusFilter(f)) => {
                        cv::enum_in(f.status, enums::WorkflowExecutionCloseStatus::from_i32)?
                    }
                    _ => None,
                },
            };
            let v: st::ListClosedWorkflowExecutionsResponse = self
                .call_with_headers(Method::POST, "/api/v1/workflows/list-closed", &input, &[])
                .await?;
            return Ok(Response::new(p::ListClosedWorkflowExecutionsResponse {
                executions: v.executions.into_iter().map(cv::info_nanos).collect(),
                next_page_token: v.next_page_token,
            }));
        }
        let r = request.into_inner();
        let mut all = self.visible(&r.domain, Some(false))?;
        time_filter(&mut all, r.start_time_filter)?;
        match r.filters {
            Some(p::list_closed_workflow_executions_request::Filters::ExecutionFilter(f)) => all
                .retain(|e| {
                    e.execution
                        .as_ref()
                        .is_some_and(|e| e.workflow_id == f.workflow_id)
                }),
            Some(p::list_closed_workflow_executions_request::Filters::TypeFilter(f)) => {
                all.retain(|e| e.workflow_type.as_ref().is_some_and(|e| e.name == f.name))
            }
            Some(p::list_closed_workflow_executions_request::Filters::StatusFilter(f)) => {
                all.retain(|e| e.close_status.is_some_and(|s| s as i32 + 1 == f.status))
            }
            None => {}
        }
        let (items, token) = page(all, r.page_size, r.next_page_token)?;
        Ok(Response::new(p::ListClosedWorkflowExecutionsResponse {
            executions: items.into_iter().map(cv::info).collect(),
            next_page_token: token,
        }))
    }
    async fn list_archived_workflow_executions(
        &self,
        request: tonic::Request<p::ListArchivedWorkflowExecutionsRequest>,
    ) -> Result<Response<p::ListArchivedWorkflowExecutionsResponse>, Status> {
        let r = request.into_inner();
        let input = st::ListArchivedWorkflowExecutionsRequest {
            domain: r.domain,
            page_size: r.page_size,
            next_page_token: r.next_page_token,
            query: r.query,
        };
        if self
            .state
            .workflow_api
            .as_ref()
            .is_some_and(|api| api.serves("ListArchivedWorkflowExecutions"))
        {
            let result: st::ListArchivedWorkflowExecutionsResponse = self
                .call_with_headers(Method::POST, "/api/v1/workflows/archived", &input, &[])
                .await?;
            return Ok(Response::new(p::ListArchivedWorkflowExecutionsResponse {
                executions: result.executions.into_iter().map(cv::info_nanos).collect(),
                next_page_token: result.next_page_token,
            }));
        }
        cadence_api::validation::validate_list_archived_request(Some(&input))
            .map_err(|e| Status::invalid_argument(e.message))?;
        // Legacy in-process path (no WorkflowApi installed); mirror Go's
        // cluster-gating error.
        Err(Status::invalid_argument(
            "Cluster is not configured for visibility archival",
        ))
    }
    async fn scan_workflow_executions(
        &self,
        request: tonic::Request<p::ScanWorkflowExecutionsRequest>,
    ) -> Result<Response<p::ScanWorkflowExecutionsResponse>, Status> {
        let r = request.into_inner();
        let mut all = self.visible(&r.domain, None)?;
        visibility_query(&mut all, &r.query)?;
        let (items, token) = page(all, r.page_size, r.next_page_token)?;
        Ok(Response::new(p::ScanWorkflowExecutionsResponse {
            executions: items.into_iter().map(cv::info).collect(),
            next_page_token: token,
        }))
    }
    async fn count_workflow_executions(
        &self,
        request: tonic::Request<p::CountWorkflowExecutionsRequest>,
    ) -> Result<Response<p::CountWorkflowExecutionsResponse>, Status> {
        let r = request.into_inner();
        let mut all = self.visible(&r.domain, None)?;
        visibility_query(&mut all, &r.query)?;
        Ok(Response::new(p::CountWorkflowExecutionsResponse {
            count: all.len() as i64,
        }))
    }
    async fn get_search_attributes(
        &self,
        _: tonic::Request<p::GetSearchAttributesRequest>,
    ) -> Result<Response<p::GetSearchAttributesResponse>, Status> {
        let v: cadence_api::tasks::GetSearchAttributesResponse = self
            .call(Method::GET, "/api/v1/search-attributes", &())
            .await?;
        Ok(Response::new(p::GetSearchAttributesResponse {
            keys: v
                .keys
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, v as i32 + 1))
                .collect(),
        }))
    }
}
#[tonic::async_trait]
impl p::worker_api_server::WorkerApi for Frontend {
    async fn reset_sticky_task_list(
        &self,
        request: tonic::Request<p::ResetStickyTaskListRequest>,
    ) -> Result<Response<p::ResetStickyTaskListResponse>, Status> {
        let request = request.into_inner();
        let input = st::ResetStickyTaskListRequest {
            domain: request.domain,
            execution: request.workflow_execution.map(cv::execution),
        };
        let _: st::ResetStickyTaskListResponse = self
            .call(Method::POST, "/api/v1/tasks/reset-sticky", &input)
            .await?;
        Ok(Response::new(p::ResetStickyTaskListResponse {}))
    }
    async fn respond_activity_task_completed_by_id(
        &self,
        request: tonic::Request<p::RespondActivityTaskCompletedByIdRequest>,
    ) -> Result<Response<p::RespondActivityTaskCompletedByIdResponse>, Status> {
        let r = request.into_inner();
        let task_token = self.activity_token(&r.domain, r.workflow_execution, &r.activity_id)?;
        p::worker_api_server::WorkerApi::respond_activity_task_completed(
            self,
            tonic::Request::new(p::RespondActivityTaskCompletedRequest {
                task_token,
                result: r.result,
                identity: r.identity,
            }),
        )
        .await?;
        Ok(Response::new(
            p::RespondActivityTaskCompletedByIdResponse {},
        ))
    }
    async fn respond_activity_task_failed_by_id(
        &self,
        request: tonic::Request<p::RespondActivityTaskFailedByIdRequest>,
    ) -> Result<Response<p::RespondActivityTaskFailedByIdResponse>, Status> {
        let r = request.into_inner();
        let task_token = self.activity_token(&r.domain, r.workflow_execution, &r.activity_id)?;
        p::worker_api_server::WorkerApi::respond_activity_task_failed(
            self,
            tonic::Request::new(p::RespondActivityTaskFailedRequest {
                task_token,
                failure: r.failure,
                identity: r.identity,
                heartbeat_details: r.heartbeat_details,
            }),
        )
        .await?;
        Ok(Response::new(p::RespondActivityTaskFailedByIdResponse {}))
    }
    async fn respond_activity_task_canceled_by_id(
        &self,
        request: tonic::Request<p::RespondActivityTaskCanceledByIdRequest>,
    ) -> Result<Response<p::RespondActivityTaskCanceledByIdResponse>, Status> {
        let r = request.into_inner();
        let task_token = self.activity_token(&r.domain, r.workflow_execution, &r.activity_id)?;
        p::worker_api_server::WorkerApi::respond_activity_task_canceled(
            self,
            tonic::Request::new(p::RespondActivityTaskCanceledRequest {
                task_token,
                details: r.details,
                identity: r.identity,
            }),
        )
        .await?;
        Ok(Response::new(p::RespondActivityTaskCanceledByIdResponse {}))
    }
    async fn record_activity_task_heartbeat_by_id(
        &self,
        request: tonic::Request<p::RecordActivityTaskHeartbeatByIdRequest>,
    ) -> Result<Response<p::RecordActivityTaskHeartbeatByIdResponse>, Status> {
        let r = request.into_inner();
        let task_token = self.activity_token(&r.domain, r.workflow_execution, &r.activity_id)?;
        let v = p::worker_api_server::WorkerApi::record_activity_task_heartbeat(
            self,
            tonic::Request::new(p::RecordActivityTaskHeartbeatRequest {
                task_token,
                details: r.details,
                identity: r.identity,
            }),
        )
        .await?;
        Ok(Response::new(p::RecordActivityTaskHeartbeatByIdResponse {
            cancel_requested: v.into_inner().cancel_requested,
        }))
    }
    async fn respond_decision_task_completed(
        &self,
        request: tonic::Request<p::RespondDecisionTaskCompletedRequest>,
    ) -> Result<Response<p::RespondDecisionTaskCompletedResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::tasks::RespondDecisionTaskCompletedRequest {
            task_token: r.task_token,
            decisions: r
                .decisions
                .into_iter()
                .map(crate::grpc_decisions::decision)
                .collect::<Result<_, _>>()?,
            execution_context: r.execution_context,
            identity: r.identity,
            binary_checksum: r.binary_checksum,
            sticky_attributes: r
                .sticky_attributes
                .map(|s| {
                    Ok::<_, Status>(t::StickyExecutionAttributes {
                        worker_task_list: s.worker_task_list.map(cv::task_list).transpose()?,
                        schedule_to_start_timeout_seconds: cv::seconds(
                            s.schedule_to_start_timeout,
                        )?,
                    })
                })
                .transpose()?,
            return_new_decision_task: r.return_new_decision_task,
            force_create_new_decision_task: r.force_create_new_decision_task,
            query_results: Some(
                r.query_results
                    .into_iter()
                    .map(|(k, v)| {
                        Ok::<_, Status>((
                            k,
                            cadence_api::tasks::WorkflowQueryResult {
                                result_type: cv::enum_in(
                                    v.result_type,
                                    enums::QueryResultType::from_i32,
                                )?,
                                answer: cv::data(v.answer),
                                error_message: v.error_message,
                            },
                        ))
                    })
                    .collect::<Result<_, _>>()?,
            ),
        };
        let v: cadence_api::tasks::RespondDecisionTaskCompletedResponse = self
            .call(
                Method::POST,
                "/api/v1/tasks/respond-decision-complete",
                &input,
            )
            .await?;
        Ok(Response::new(p::RespondDecisionTaskCompletedResponse {
            decision_task: v.decision_task.map(cv::decision_task).transpose()?,
            activities_to_dispatch_locally: Default::default(),
        }))
    }
    async fn respond_activity_task_failed(
        &self,
        request: tonic::Request<p::RespondActivityTaskFailedRequest>,
    ) -> Result<Response<p::RespondActivityTaskFailedResponse>, Status> {
        let r = request.into_inner();
        let (reason, details, failure_options) = cv::failure_in(r.failure)?;
        let input = cadence_api::tasks::RespondActivityTaskFailedRequest {
            task_token: r.task_token,
            reason,
            details,
            failure_options,
            identity: r.identity,
            heartbeat_details: cv::data(r.heartbeat_details),
        };
        let _: serde_json::Value = self
            .call(
                Method::POST,
                "/api/v1/tasks/respond-activity-failed",
                &input,
            )
            .await?;
        Ok(Response::new(p::RespondActivityTaskFailedResponse {}))
    }
    async fn poll_for_decision_task(
        &self,
        request: tonic::Request<p::PollForDecisionTaskRequest>,
    ) -> Result<Response<p::PollForDecisionTaskResponse>, Status> {
        let isolation_group = isolation_group_metadata(&request);
        let r = request.into_inner();
        let input = cadence_api::tasks::PollForDecisionTaskRequest {
            domain: r.domain,
            task_list: r.task_list.map(cv::task_list).transpose()?,
            identity: r.identity,
            binary_checksum: r.binary_checksum,
        };
        let v: Option<cadence_api::tasks::PollForDecisionTaskResponse> = self
            .call_with_headers(
                Method::POST,
                "/api/v1/tasks/poll-decision",
                &input,
                &[(
                    crate::handlers::ISOLATION_GROUP_HEADER,
                    isolation_group.as_str(),
                )],
            )
            .await?;
        Ok(Response::new(
            v.map(cv::decision_task).transpose()?.unwrap_or_default(),
        ))
    }
    async fn poll_for_activity_task(
        &self,
        request: tonic::Request<p::PollForActivityTaskRequest>,
    ) -> Result<Response<p::PollForActivityTaskResponse>, Status> {
        let isolation_group = isolation_group_metadata(&request);
        let r = request.into_inner();
        let input = cadence_api::tasks::PollForActivityTaskRequest {
            domain: r.domain,
            task_list: r.task_list.map(cv::task_list).transpose()?,
            identity: r.identity,
            task_list_metadata: r.task_list_metadata.map(|m| t::TaskListMetadata {
                max_tasks_per_second: m.max_tasks_per_second,
            }),
        };
        let v: Option<cadence_api::tasks::PollForActivityTaskResponse> = self
            .call_with_headers(
                Method::POST,
                "/api/v1/tasks/poll-activity",
                &input,
                &[(
                    crate::handlers::ISOLATION_GROUP_HEADER,
                    isolation_group.as_str(),
                )],
            )
            .await?;
        Ok(Response::new(v.map(cv::activity_task).unwrap_or_default()))
    }
    async fn respond_activity_task_completed(
        &self,
        request: tonic::Request<p::RespondActivityTaskCompletedRequest>,
    ) -> Result<Response<p::RespondActivityTaskCompletedResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::tasks::RespondActivityTaskCompletedRequest {
            task_token: r.task_token,
            result: cv::data(r.result),
            identity: r.identity,
        };
        let _: serde_json::Value = self
            .call(
                Method::POST,
                "/api/v1/tasks/respond-activity-complete",
                &input,
            )
            .await?;
        Ok(Response::new(p::RespondActivityTaskCompletedResponse {}))
    }
    async fn respond_activity_task_canceled(
        &self,
        request: tonic::Request<p::RespondActivityTaskCanceledRequest>,
    ) -> Result<Response<p::RespondActivityTaskCanceledResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::tasks::RespondActivityTaskCanceledRequest {
            task_token: r.task_token,
            details: cv::data(r.details),
            identity: r.identity,
        };
        let _: serde_json::Value = self
            .call(
                Method::POST,
                "/api/v1/tasks/respond-activity-canceled",
                &input,
            )
            .await?;
        Ok(Response::new(p::RespondActivityTaskCanceledResponse {}))
    }
    async fn record_activity_task_heartbeat(
        &self,
        request: tonic::Request<p::RecordActivityTaskHeartbeatRequest>,
    ) -> Result<Response<p::RecordActivityTaskHeartbeatResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::tasks::RecordActivityTaskHeartbeatRequest {
            task_token: r.task_token,
            details: cv::data(r.details),
            identity: r.identity,
        };
        let v: cadence_api::tasks::RecordActivityTaskHeartbeatResponse = self
            .call(
                Method::POST,
                "/api/v1/tasks/record-activity-heartbeat",
                &input,
            )
            .await?;
        Ok(Response::new(p::RecordActivityTaskHeartbeatResponse {
            cancel_requested: v.cancel_requested,
        }))
    }
    async fn respond_decision_task_failed(
        &self,
        request: tonic::Request<p::RespondDecisionTaskFailedRequest>,
    ) -> Result<Response<p::RespondDecisionTaskFailedResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::tasks::RespondDecisionTaskFailedRequest {
            task_token: r.task_token,
            cause: cv::enum_in(r.cause, enums::DecisionTaskFailedCause::from_i32)?,
            details: cv::data(r.details),
            identity: r.identity,
            binary_checksum: r.binary_checksum,
        };
        let _: serde_json::Value = self
            .call(
                Method::POST,
                "/api/v1/tasks/respond-decision-failed",
                &input,
            )
            .await?;
        Ok(Response::new(p::RespondDecisionTaskFailedResponse {}))
    }
    async fn respond_query_task_completed(
        &self,
        request: tonic::Request<p::RespondQueryTaskCompletedRequest>,
    ) -> Result<Response<p::RespondQueryTaskCompletedResponse>, Status> {
        let input = cv::query_completed(request.into_inner())?;
        if input.task_token.is_empty() {
            return Err(Status::invalid_argument("Task token is not set."));
        }
        let _: serde_json::Value = self
            .call(
                Method::POST,
                "/api/v1/tasks/respond-query-completed",
                &input,
            )
            .await?;
        Ok(Response::new(p::RespondQueryTaskCompletedResponse {}))
    }
}

/// Go `accesscontrolled.NewAdminHandler` for the gRPC Admin API: runs the
/// Frontend-installed admin authorizer before the inner service.
#[derive(Clone)]
struct AdminAuth<S> {
    inner: S,
    api: Option<cadence_service_core::workflow_api::SharedWorkflowApi>,
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for AdminAuth<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> tower::Service<axum::http::Request<B>> for AdminAuth<S>
where
    S: tower::Service<
            axum::http::Request<B>,
            Response = axum::http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: axum::http::Request<B>) -> Self::Future {
        use cadence_service_core::workflow_api::AdminAuthError;
        let denial = self
            .api
            .as_ref()
            .and_then(|api| api.admin_authorizer())
            .and_then(|auth| {
                let method = request.uri().path().rsplit('/').next().unwrap_or_default();
                let token = request
                    .headers()
                    .get("cadence-authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default();
                auth.authorize_admin(method, token).err()
            });
        if let Some(error) = denial {
            let status = match error {
                AdminAuthError::Denied(m) => tonic::Status::permission_denied(m),
                AdminAuthError::Failed(m) => tonic::Status::unknown(m),
            };
            let mut response = axum::http::Response::new(tonic::body::empty_body());
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/grpc"),
            );
            status.add_header(response.headers_mut()).ok();
            return Box::pin(async move { Ok(response) });
        }
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move { inner.call(request).await })
    }
}

pub async fn serve(
    addr: std::net::SocketAddr,
    state: crate::handlers::AppState,
) -> anyhow::Result<()> {
    let limit = state.grpc_max_message_size;
    let service = Frontend { state };
    let admission = || frontend_admission_interceptor(service.state.frontend_accepting.clone());
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(cadence_proto::FILE_DESCRIPTOR_SET)
        .with_service_name("uber.cadence.api.v1.MetaAPI")
        .with_service_name("uber.cadence.api.v1.DomainAPI")
        .with_service_name("uber.cadence.api.v1.WorkflowAPI")
        .with_service_name("uber.cadence.api.v1.VisibilityAPI")
        .with_service_name("uber.cadence.api.v1.WorkerAPI")
        .with_service_name("uber.cadence.api.v1.ScheduleAPI")
        .with_service_name("uber.cadence.api.v1.SemaphoreAPI")
        .with_service_name("uber.cadence.admin.v1.AdminAPI")
        .with_service_name("uber.cadence.sharddistributor.v1.ShardDistributorAPI")
        .with_service_name("uber.cadence.sharddistributor.v1.ShardDistributorExecutorAPI")
        .with_service_name("uber.cadence.sharddistributor.v1.ShardDistributorExecutorCanaryAPI")
        .build_v1()?;
    tonic::transport::Server::builder()
        .layer(tower::layer::layer_fn(CallerScope))
        .add_service(reflection)
        .add_service(
            tonic::service::interceptor::InterceptedService::new(
                p::meta_api_server::MetaApiServer::new(service.clone())
                    .max_decoding_message_size(limit),
                admission(),
            ),
        )
        .add_service(
            tonic::service::interceptor::InterceptedService::new(
                p::domain_api_server::DomainApiServer::new(service.clone())
                    .max_decoding_message_size(limit),
                admission(),
            ),
        )
        .add_service(
            tonic::service::interceptor::InterceptedService::new(
                p::workflow_api_server::WorkflowApiServer::new(service.clone())
                    .max_decoding_message_size(limit),
                admission(),
            ),
        )
        .add_service(
            tonic::service::interceptor::InterceptedService::new(
                p::visibility_api_server::VisibilityApiServer::new(service.clone())
                    .max_decoding_message_size(limit),
                admission(),
            ),
        )
        .add_service(
            tonic::service::interceptor::InterceptedService::new(
                p::worker_api_server::WorkerApiServer::new(service.clone())
                    .max_decoding_message_size(limit),
                admission(),
            ),
        )
        .add_service(
            tonic::service::interceptor::InterceptedService::new(
                p::schedule_api_server::ScheduleApiServer::new(service.clone())
                    .max_decoding_message_size(limit),
                admission(),
            ),
        )
        .add_service(
            tonic::service::interceptor::InterceptedService::new(
                p::semaphore_api_server::SemaphoreApiServer::new(service.clone())
                    .max_decoding_message_size(limit),
                admission(),
            ),
        )
        .add_service(AdminAuth {
            inner: tonic::service::interceptor::InterceptedService::new(
                cadence_proto::admin::admin_api_server::AdminApiServer::new(
                    crate::grpc_admin::Admin { frontend: service.clone() },
                )
                .max_decoding_message_size(limit),
                admission(),
            ),
            api: service.state.workflow_api.clone(),
        })
        .add_service(
            cadence_proto::sharddistributor::shard_distributor_api_server::ShardDistributorApiServer::new(
                crate::grpc_shard::ShardDistributor {
                    state: service.state.clone(),
                },
            )
            .max_decoding_message_size(limit),
        )
        .add_service(
            cadence_proto::sharddistributor::shard_distributor_executor_api_server::ShardDistributorExecutorApiServer::new(
                crate::grpc_shard::ShardExecutor {
                    state: service.state.clone(),
                },
            )
            .max_decoding_message_size(limit),
        )
        .add_service(
            cadence_proto::sharddistributor::shard_distributor_executor_canary_api_server::ShardDistributorExecutorCanaryApiServer::new(
                crate::grpc_shard::ShardCanary {
                    state: service.state.clone(),
                },
            )
            .max_decoding_message_size(limit),
        )
        .serve(addr)
        .await?;
    Ok(())
}

fn frontend_admission_interceptor(
    accepting: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> impl FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Clone {
    move |request| {
        if accepting.load(std::sync::atomic::Ordering::Acquire) {
            Ok(request)
        } else {
            Err(tonic::Status::unavailable("Frontend is shutting down."))
        }
    }
}

#[cfg(test)]
mod workflow_hook_tests {
    use super::*;
    use cadence_proto::api::workflow_api_server::WorkflowApi as _;
    use cadence_service_core::workflow_api::{ApiFuture, CallerContext, WorkflowApi};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct RecordingApi {
        calls: Mutex<Vec<(String, serde_json::Value, CallerContext)>>,
        result: serde_json::Value,
        error: bool,
    }

    impl WorkflowApi for RecordingApi {
        fn serves(&self, operation: &str) -> bool {
            matches!(
                operation,
                "StartWorkflowExecution"
                    | "SignalWithStartWorkflowExecution"
                    | "GetWorkflowExecutionHistory"
            )
        }

        fn call<'a>(
            &'a self,
            operation: &'a str,
            request: serde_json::Value,
            ctx: CallerContext,
        ) -> ApiFuture<'a> {
            self.calls
                .lock()
                .unwrap()
                .push((operation.to_owned(), request, ctx));
            Box::pin(async move {
                if self.error {
                    Err(cadence_api::errors::CadenceError::Unknown(
                        "hook failure".to_owned(),
                    ))
                } else {
                    Ok(self.result.clone())
                }
            })
        }
    }

    fn frontend(api: Arc<RecordingApi>) -> Frontend {
        let state = crate::handlers::AppState {
            workflow_api: Some(api),
            ..Default::default()
        };
        Frontend { state }
    }

    fn valid_start_request() -> p::StartWorkflowExecutionRequest {
        p::StartWorkflowExecutionRequest {
            domain: "sample-domain".into(),
            workflow_id: "sample-workflow".into(),
            workflow_type: Some(p::WorkflowType {
                name: "sample-type".into(),
            }),
            task_list: Some(p::TaskList {
                name: "sample-task-list".into(),
                ..Default::default()
            }),
            execution_start_to_close_timeout: Some(prost_types::Duration {
                seconds: 60,
                nanos: 0,
            }),
            task_start_to_close_timeout: Some(prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
            request_id: "request-id".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn synchronous_start_operations_use_workflow_hook_context_and_run_id() {
        let api = Arc::new(RecordingApi {
            calls: Mutex::new(Vec::new()),
            result: serde_json::json!({"runId": "hook-run"}),
            error: false,
        });
        let frontend = frontend(api.clone());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(45);
        let headers = std::collections::HashMap::from([(
            "x-cadence-caller".to_owned(),
            "sdk-worker".to_owned(),
        )]);
        let start = frontend.start_workflow_execution(tonic::Request::new(valid_start_request()));
        let start_result = GRPC_CALLER
            .scope((headers.clone(), Some(deadline)), start)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(start_result.run_id, "hook-run");

        let signal_start = frontend.signal_with_start_workflow_execution(tonic::Request::new(
            p::SignalWithStartWorkflowExecutionRequest {
                start_request: Some(valid_start_request()),
                ..Default::default()
            },
        ));
        let signal_result = GRPC_CALLER
            .scope((headers, Some(deadline)), signal_start)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(signal_result.run_id, "hook-run");

        let calls = api.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "StartWorkflowExecution");
        assert_eq!(calls[1].0, "SignalWithStartWorkflowExecution");
        for (_, request, context) in calls.iter() {
            assert!(request.is_object());
            assert_eq!(context.headers["x-cadence-caller"], "sdk-worker");
            assert_eq!(context.deadline, Some(deadline));
        }
    }

    #[tokio::test]
    async fn synchronous_start_hook_errors_become_grpc_errors() {
        let api = Arc::new(RecordingApi {
            calls: Mutex::new(Vec::new()),
            result: serde_json::Value::Null,
            error: true,
        });
        let frontend = frontend(api.clone());
        let result = frontend
            .start_workflow_execution(tonic::Request::new(valid_start_request()))
            .await;
        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error.message().contains("hook failure"));
        assert_eq!(api.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn history_hook_preserves_binary_fields_and_caller_context() {
        let api = Arc::new(RecordingApi {
            calls: Mutex::new(Vec::new()),
            result: serde_json::json!({
                "history": {"events": []},
                "rawHistory": [{"EncodingType": 1, "Data": "AAE="}],
                "nextPageToken": "AgM=",
                "archived": true
            }),
            error: false,
        });
        let frontend = frontend(api.clone());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let headers = std::collections::HashMap::from([(
            "x-cadence-caller".to_owned(),
            "sdk-worker".to_owned(),
        )]);
        let request = p::GetWorkflowExecutionHistoryRequest {
            domain: "sample-domain".into(),
            workflow_execution: Some(p::WorkflowExecution {
                workflow_id: "sample-workflow".into(),
                run_id: "sample-run".into(),
            }),
            page_size: 23,
            next_page_token: vec![4, 5],
            skip_archival: true,
            ..Default::default()
        };
        let future = frontend.get_workflow_execution_history(tonic::Request::new(request));
        let response = GRPC_CALLER
            .scope((headers, Some(deadline)), future)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.raw_history.len(), 1);
        assert_eq!(response.raw_history[0].encoding_type, 2);
        assert_eq!(response.raw_history[0].data, vec![0, 1]);
        assert_eq!(response.next_page_token, vec![2, 3]);
        assert!(response.archived);
        assert!(response.history.is_some());
        let calls = api.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "GetWorkflowExecutionHistory");
        assert_eq!(calls[0].1["domain"], "sample-domain");
        assert_eq!(calls[0].1["maximumPageSize"], 23);
        assert_eq!(calls[0].1["nextPageToken"], "BAU=");
        assert_eq!(calls[0].2.headers["x-cadence-caller"], "sdk-worker");
        assert_eq!(calls[0].2.deadline, Some(deadline));
    }

    #[tokio::test]
    async fn history_hook_converts_frontend_errors_to_grpc_status() {
        let api = Arc::new(RecordingApi {
            calls: Mutex::new(Vec::new()),
            result: serde_json::Value::Null,
            error: true,
        });
        let frontend = frontend(api.clone());
        let error = frontend
            .get_workflow_execution_history(tonic::Request::new(
                p::GetWorkflowExecutionHistoryRequest {
                    domain: "sample-domain".into(),
                    workflow_execution: Some(p::WorkflowExecution {
                        workflow_id: "sample-workflow".into(),
                        run_id: "sample-run".into(),
                    }),
                    ..Default::default()
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error.message().contains("hook failure"));
        assert_eq!(api.calls.lock().unwrap().len(), 1);
    }
}
