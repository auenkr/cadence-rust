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

#[derive(Clone)]
pub struct Frontend {
    pub state: crate::handlers::AppState,
}

impl Frontend {
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
            return Err(match error["type"].as_str().unwrap_or("") {
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
                "ServiceBusyError" => detailed_status(tonic::Code::ResourceExhausted, message, "ServiceBusyError", p::ServiceBusyError { reason: String::new() }),
                "QueryFailedError" => detailed_status(
                    tonic::Code::InvalidArgument,
                    message,
                    "QueryFailedError",
                    p::QueryFailedError {},
                ),
                _ => Status::internal(message),
            });
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
            })),
        }))
    }

    async fn register_domain(
        &self,
        request: tonic::Request<p::RegisterDomainRequest>,
    ) -> Result<Response<p::RegisterDomainResponse>, Status> {
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
            .domains
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
            .domains
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
            })),
        }))
    }
    async fn list_failover_history(
        &self,
        request: tonic::Request<p::ListFailoverHistoryRequest>,
    ) -> Result<Response<p::ListFailoverHistoryResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::types::ListFailoverHistoryRequest {
            filters: r.filters.map(|f| {
                cadence_api::types::ListFailoverHistoryRequestFilters {
                    domain_id: f.domain_id,
                }
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
            failover_events: v.failover_events.into_iter().map(cv::failover_event_out).collect(),
            next_page_token: v.next_page_token,
        }))
    }
    async fn deprecate_domain(
        &self,
        request: tonic::Request<p::DeprecateDomainRequest>,
    ) -> Result<Response<p::DeprecateDomainResponse>, Status> {
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
        let inner_proto = r.request.ok_or_else(|| Status::invalid_argument("Request is not set."))?;
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
        if input.workflow_execution.as_ref().is_none_or(|e| {
            e.workflow_id.is_empty() || e.run_id.is_empty()
        }) {
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
        if input.workflow_execution.as_ref().is_none_or(|e| {
            e.workflow_id.is_empty() || e.run_id.is_empty()
        }) {
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
        if input.workflow_execution.as_ref().is_none_or(|e| {
            e.workflow_id.is_empty() || e.run_id.is_empty()
        }) {
            return Err(Status::invalid_argument("Execution is not set on request."));
        }
        let v: cadence_api::types::DiagnoseWorkflowExecutionResponse = self
            .call(Method::POST, "/api/v1/workflows/diagnose", &input)
            .await?;
        Ok(Response::new(p::DiagnoseWorkflowExecutionResponse {
            domain: v.domain,
            diagnostic_workflow_execution: v
                .diagnostic_workflow_execution
                .map(cv::execution_out),
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
                    raw_history: Vec::new(),
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
        let input = cadence_api::tasks::GetTaskListsByDomainRequest {
            domain: r.domain,
        };
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
                fields: m.fields.unwrap_or_default().into_iter().map(|(k, v)| (k, p::Payload { data: v })).collect(),
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
            schedules: v.schedules.into_iter().map(cv::schedule_entry_out).collect(),
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
            overlap_policy: cv::enum_in(
                r.overlap_policy,
                enums::ScheduleOverlapPolicy::from_i32,
            )?,
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
        cadence_api::validation::validate_list_archived_request(Some(&input))
            .map_err(|e| Status::invalid_argument(e.message))?;
        // Visibility archival has no backend in this port; mirror Go's
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
                &[(crate::handlers::ISOLATION_GROUP_HEADER, isolation_group.as_str())],
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
                &[(crate::handlers::ISOLATION_GROUP_HEADER, isolation_group.as_str())],
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

pub async fn serve(
    addr: std::net::SocketAddr,
    state: crate::handlers::AppState,
) -> anyhow::Result<()> {
    let limit = state.grpc_max_message_size;
    let service = Frontend { state };
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
        .add_service(reflection)
        .add_service(
            p::meta_api_server::MetaApiServer::new(service.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            p::domain_api_server::DomainApiServer::new(service.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            p::workflow_api_server::WorkflowApiServer::new(service.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            p::visibility_api_server::VisibilityApiServer::new(service.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            p::worker_api_server::WorkerApiServer::new(service.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            p::schedule_api_server::ScheduleApiServer::new(service.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            p::semaphore_api_server::SemaphoreApiServer::new(service.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            cadence_proto::admin::admin_api_server::AdminApiServer::new(
                crate::grpc_admin::Admin { frontend: service.clone() },
            )
            .max_decoding_message_size(limit),
        )
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
