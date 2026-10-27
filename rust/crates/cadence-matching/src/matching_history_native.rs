//! Raw-byte (Go string) task-start calls from Matching to History. Additive to
//! the legacy UTF-8 API in the parent module: it reuses its retry, deadline,
//! redirect and metrics machinery, but never converts identifiers through
//! `String`. Declared as a child module of `matching_history` so it can use the
//! client's private connection state.
//!
//! With `with_tchannel_outbound` the calls go through the byte-preserving
//! Thrift mapping in `matching_history_tchannel_native.rs`; no path converts
//! identifiers through UTF-8.
use super::{HistoryConnection, MatchingHistoryClient};
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskTokenBytes};
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::AllocatedTaskBytes;
use cadence_proto::native as n;
use tonic::{codegen::http::uri::PathAndQuery, transport::Channel, Request, Status};
use uuid::Uuid;

const RECORD_DECISION_TASK_STARTED: &str =
    "/uber.cadence.history.v1.HistoryAPI/RecordDecisionTaskStarted";
const RECORD_ACTIVITY_TASK_STARTED: &str =
    "/uber.cadence.history.v1.HistoryAPI/RecordActivityTaskStarted";

/// One unary call over the configured channel with the native prost codec.
async fn native_unary<Req, Resp>(
    channel: Channel,
    max_message_size: usize,
    path: &'static str,
    request: Request<Req>,
) -> Result<Resp, Status>
where
    Req: prost::Message + Send + 'static,
    Resp: prost::Message + Default + Send + 'static,
{
    let mut grpc = tonic::client::Grpc::new(channel)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);
    grpc.ready().await.map_err(|error| {
        let error: tonic::codegen::StdError = error.into();
        Status::unknown(format!("Service was not ready: {error}"))
    })?;
    grpc.unary(
        request,
        PathAndQuery::from_static(path),
        tonic::codec::ProstCodec::default(),
    )
    .await
    .map(tonic::Response::into_inner)
}

fn execution(task: &AllocatedTaskBytes) -> Result<n::api::WorkflowExecution, Status> {
    let run_id = match task.data.run_id.as_deref() {
        _ if task.data.raw_run_id.is_some() => GoString::from(task.data.run_id_bytes()),
        None | Some([]) => GoString::default(),
        Some(bytes) => Uuid::from_slice(bytes)
            .map_err(|_| Status::internal("Invalid run ID in persisted matching task."))?
            .to_string()
            .into(),
    };
    Ok(n::api::WorkflowExecution {
        workflow_id: task.data.workflow_id.clone(),
        run_id,
    })
}

pub(super) fn decision_response(
    domain_id: &GoString,
    execution: n::api::WorkflowExecution,
    started: n::history::RecordDecisionTaskStartedResponse,
) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
    let token = TaskTokenBytes {
        domain_id: domain_id.clone(),
        workflow_id: execution.workflow_id.clone(),
        run_id: execution.run_id.clone(),
        schedule_id: started.scheduled_event_id,
        schedule_attempt: i64::from(started.attempt),
        ..Default::default()
    };
    Ok(n::matching::PollForDecisionTaskResponse {
        task_token: JsonTaskTokenSerializer::serialize_bytes(&token),
        workflow_execution: Some(execution),
        workflow_type: started.workflow_type,
        previous_started_event_id: started
            .previous_started_event_id
            .filter(|event| *event != -23),
        started_event_id: started.started_event_id,
        attempt: started.attempt,
        next_event_id: started.next_event_id,
        sticky_execution_enabled: started.sticky_execution_enabled,
        decision_info: started.decision_info,
        workflow_execution_task_list: started.workflow_execution_task_list,
        branch_token: started.branch_token,
        scheduled_time: started.scheduled_time,
        started_time: started.started_time,
        queries: started.queries,
        total_history_bytes: started.history_size,
        ..Default::default()
    })
}

pub(super) fn activity_response(
    domain_id: &GoString,
    schedule_id: i64,
    execution: n::api::WorkflowExecution,
    started: n::history::RecordActivityTaskStartedResponse,
) -> Result<n::matching::PollForActivityTaskResponse, Status> {
    let event = started.scheduled_event.ok_or_else(|| {
        Status::internal("Activity scheduled event is missing from History response.")
    })?;
    let Some(n::api::history_event::Attributes::ActivityTaskScheduledEventAttributes(attributes)) =
        event.attributes
    else {
        return Err(Status::internal(
            "ActivityTaskScheduledEventAttributes is not set.",
        ));
    };
    // Deref-coerces whether the generated variant is boxed or not.
    let attributes: &n::api::ActivityTaskScheduledEventAttributes = &attributes;
    if attributes.activity_id.is_empty() {
        return Err(Status::internal(
            "ActivityTaskScheduledEventAttributes.ActivityID is not set.",
        ));
    }
    let name = |value: &Option<n::api::ActivityType>| {
        value
            .as_ref()
            .map(|value| value.name.clone())
            .unwrap_or_default()
    };
    let token = TaskTokenBytes {
        domain_id: domain_id.clone(),
        workflow_id: execution.workflow_id.clone(),
        run_id: execution.run_id.clone(),
        workflow_type: started
            .workflow_type
            .as_ref()
            .map(|value| value.name.clone())
            .unwrap_or_default(),
        schedule_id,
        schedule_attempt: i64::from(started.attempt),
        activity_id: attributes.activity_id.clone(),
        activity_type: name(&attributes.activity_type),
    };
    Ok(n::matching::PollForActivityTaskResponse {
        task_token: JsonTaskTokenSerializer::serialize_bytes(&token),
        workflow_execution: Some(execution),
        activity_id: attributes.activity_id.clone(),
        activity_type: attributes.activity_type.clone(),
        input: attributes.input.clone(),
        scheduled_time: event.event_time,
        started_time: started.started_time,
        schedule_to_close_timeout: attributes.schedule_to_close_timeout,
        start_to_close_timeout: attributes.start_to_close_timeout,
        heartbeat_timeout: attributes.heartbeat_timeout,
        attempt: started.attempt,
        scheduled_time_of_this_attempt: started.scheduled_time_of_this_attempt,
        heartbeat_details: started.heartbeat_details,
        workflow_type: started.workflow_type,
        workflow_domain: started.workflow_domain,
        header: attributes.header.clone(),
        ..Default::default()
    })
}

impl MatchingHistoryClient {
    /// Byte-aware providers can resolve raw domains without aliasing invalid
    /// bytes to a valid domain containing a replacement rune.
    fn record_started_operation_timeout_bytes(
        &self,
        domain_id: &GoString,
    ) -> Option<std::time::Duration> {
        let timeout = if let Some(provider) = &self.record_started_timeout_bytes {
            provider(domain_id.as_bytes())
        } else if let Ok(domain) = domain_id.as_utf8() {
            return self.record_started_operation_timeout(domain);
        } else {
            std::time::Duration::from_nanos(super::DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS as u64)
        };
        (!timeout.is_zero()).then_some(timeout)
    }

    pub async fn start_decision_native(
        &self,
        domain_id: &GoString,
        task: &AllocatedTaskBytes,
        poll: n::api::PollForDecisionTaskRequest,
    ) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
        self.start_decision_native_with_deadline(domain_id, task, poll, None)
            .await
    }

    pub async fn start_decision_native_with_deadline(
        &self,
        domain_id: &GoString,
        task: &AllocatedTaskBytes,
        poll: n::api::PollForDecisionTaskRequest,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
        let execution = execution(task)?;
        let payload = n::history::RecordDecisionTaskStartedRequest {
            domain_id: domain_id.clone(),
            workflow_execution: Some(execution.clone()),
            schedule_id: task.data.schedule_id,
            task_id: task.task_id,
            request_id: Uuid::new_v4().to_string().into(),
            poll_request: Some(poll),
        };
        let max = self.max_message_size;
        let started = self
            .instrument_history(
                "RecordDecisionTaskStarted",
                self.retry_record_started(
                    payload,
                    self.record_started_operation_timeout_bytes(domain_id),
                    caller_deadline,
                    execution.workflow_id.as_bytes(),
                    move |connection, request| async move {
                        match connection {
                            HistoryConnection::Grpc(_, channel) => {
                                native_unary::<_, n::history::RecordDecisionTaskStartedResponse>(
                                    channel,
                                    max,
                                    RECORD_DECISION_TASK_STARTED,
                                    request,
                                )
                                .await
                            }
                            HistoryConnection::Tchannel(client, timeout) => {
                                client
                                    .record_decision_task_started_native(request.get_ref(), timeout)
                                    .await
                            }
                        }
                    },
                ),
            )
            .await?;
        decision_response(domain_id, execution, started)
    }

    pub async fn start_activity_native(
        &self,
        domain_id: &GoString,
        task: &AllocatedTaskBytes,
        poll: n::api::PollForActivityTaskRequest,
    ) -> Result<n::matching::PollForActivityTaskResponse, Status> {
        self.start_activity_native_with_deadline(domain_id, task, poll, None)
            .await
    }

    pub async fn start_activity_native_with_deadline(
        &self,
        domain_id: &GoString,
        task: &AllocatedTaskBytes,
        poll: n::api::PollForActivityTaskRequest,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<n::matching::PollForActivityTaskResponse, Status> {
        let execution = execution(task)?;
        let payload = n::history::RecordActivityTaskStartedRequest {
            domain_id: domain_id.clone(),
            workflow_execution: Some(execution.clone()),
            schedule_id: task.data.schedule_id,
            task_id: task.task_id,
            request_id: Uuid::new_v4().to_string().into(),
            poll_request: Some(poll),
        };
        let max = self.max_message_size;
        let started = self
            .instrument_history(
                "RecordActivityTaskStarted",
                self.retry_record_started(
                    payload,
                    self.record_started_operation_timeout_bytes(domain_id),
                    caller_deadline,
                    execution.workflow_id.as_bytes(),
                    move |connection, request| async move {
                        match connection {
                            HistoryConnection::Grpc(_, channel) => {
                                native_unary::<_, n::history::RecordActivityTaskStartedResponse>(
                                    channel,
                                    max,
                                    RECORD_ACTIVITY_TASK_STARTED,
                                    request,
                                )
                                .await
                            }
                            HistoryConnection::Tchannel(client, timeout) => {
                                client
                                    .record_activity_task_started_native(request.get_ref(), timeout)
                                    .await
                            }
                        }
                    },
                ),
            )
            .await?;
        activity_response(domain_id, task.data.schedule_id, execution, started)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_history_peer_resolver::{
        HistoryMembership, MatchingHistoryPeerResolver, NamedPort,
    };
    use cadence_common::membership::Host;
    use cadence_persistence::matching_blob::TaskDataBytes;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn raw(bytes: &[u8]) -> GoString {
        GoString::from(bytes)
    }

    fn task() -> AllocatedTaskBytes {
        AllocatedTaskBytes {
            task_id: 77,
            data: TaskDataBytes {
                workflow_id: raw(b"wf-\xff\xfe"),
                raw_run_id: Some(raw(b"run-\xfe\xff")),
                schedule_id: 9,
                ..Default::default()
            },
        }
    }

    #[test]
    fn malformed_persisted_uuid_errors_but_raw_request_bytes_take_precedence() {
        let mut task = task();
        task.data.raw_run_id = None;
        task.data.run_id = Some(vec![1, 2]);
        let error = execution(&task).unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(
            error.message(),
            "Invalid run ID in persisted matching task."
        );
        task.data.raw_run_id = Some(raw(b"\xff\xfe"));
        assert_eq!(execution(&task).unwrap().run_id.as_bytes(), &[0xff, 0xfe]);
    }

    #[tokio::test]
    async fn raw_timeout_provider_never_aliases_invalid_domain_bytes() {
        let client = MatchingHistoryClient::new(
            Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            1024,
        )
        .with_record_started_timeout(Arc::new(|domain| {
            assert_eq!(domain, "�");
            Duration::from_secs(7)
        }));
        assert_eq!(
            client.record_started_operation_timeout_bytes(&raw("�".as_bytes())),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            client.record_started_operation_timeout_bytes(&raw(b"\xff")),
            Some(Duration::from_secs(1))
        );
        let client = client.with_record_started_timeout_bytes(Arc::new(|domain| {
            assert_eq!(domain, b"\xff");
            Duration::from_secs(3)
        }));
        assert_eq!(
            client.record_started_operation_timeout_bytes(&raw(b"\xff")),
            Some(Duration::from_secs(3))
        );
    }

    #[test]
    fn execution_preserves_malformed_run_and_workflow_bytes() {
        let execution = execution(&task()).unwrap();
        assert_eq!(execution.workflow_id.as_bytes(), b"wf-\xff\xfe");
        assert_eq!(execution.run_id.as_bytes(), b"run-\xfe\xff");
    }

    #[test]
    fn decision_response_token_and_fields_keep_raw_bytes() {
        let domain = raw(b"dom-\xff");
        let execution = execution(&task()).unwrap();
        let response = decision_response(
            &domain,
            execution.clone(),
            n::history::RecordDecisionTaskStartedResponse {
                scheduled_event_id: 9,
                attempt: 2,
                previous_started_event_id: Some(-23),
                workflow_type: Some(n::api::WorkflowType {
                    name: raw(b"t\xfe"),
                }),
                started_event_id: 11,
                ..Default::default()
            },
        )
        .unwrap();
        let expected = JsonTaskTokenSerializer::serialize_bytes(&TaskTokenBytes {
            domain_id: domain,
            workflow_id: execution.workflow_id.clone(),
            run_id: execution.run_id.clone(),
            schedule_id: 9,
            schedule_attempt: 2,
            ..Default::default()
        });
        assert_eq!(response.task_token, expected);
        assert_eq!(response.previous_started_event_id, None);
        assert_eq!(response.workflow_execution, Some(execution));
        assert_eq!(response.workflow_type.unwrap().name.as_bytes(), b"t\xfe");
    }

    #[test]
    fn activity_response_requires_attributes_and_keeps_raw_ids() {
        let domain = raw(b"d\xff");
        let execution = execution(&task()).unwrap();
        let missing = activity_response(
            &domain,
            9,
            execution.clone(),
            n::history::RecordActivityTaskStartedResponse::default(),
        )
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Internal);
        let attributes = n::api::ActivityTaskScheduledEventAttributes {
            activity_id: raw(b"a\xff"),
            activity_type: Some(n::api::ActivityType {
                name: raw(b"at\xfe"),
            }),
            ..Default::default()
        };
        let event = n::api::HistoryEvent {
            attributes: Some(
                n::api::history_event::Attributes::ActivityTaskScheduledEventAttributes(attributes),
            ),
            ..Default::default()
        };
        let response = activity_response(
            &domain,
            9,
            execution.clone(),
            n::history::RecordActivityTaskStartedResponse {
                scheduled_event: Some(event),
                attempt: 3,
                workflow_type: Some(n::api::WorkflowType {
                    name: raw(b"wt\xfe"),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let expected = JsonTaskTokenSerializer::serialize_bytes(&TaskTokenBytes {
            domain_id: domain,
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
            workflow_type: raw(b"wt\xfe"),
            schedule_id: 9,
            schedule_attempt: 3,
            activity_id: raw(b"a\xff"),
            activity_type: raw(b"at\xfe"),
        });
        assert_eq!(response.task_token, expected);
        assert_eq!(response.activity_id.as_bytes(), b"a\xff");
    }

    #[derive(Clone)]
    struct Members(Vec<Host>);
    impl HistoryMembership for Members {
        fn hosts(&self) -> Result<Vec<Host>, Status> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn raw_resolver_hashes_exact_bytes_like_str_path() {
        let resolver = MatchingHistoryPeerResolver::new(
            16,
            NamedPort::Grpc,
            Arc::new(Members(vec![Host {
                address: "h:1".into(),
                identity: "h".into(),
                grpc_address: "http://127.0.0.1:1".into(),
            }])),
        )
        .unwrap();
        assert_eq!(
            resolver.workflow("wf-ascii").unwrap(),
            resolver.workflow_bytes(b"wf-ascii").unwrap()
        );
        assert_eq!(
            resolver.domain("dom").unwrap(),
            resolver.domain_bytes(b"dom").unwrap()
        );
        // Malformed bytes route without UTF-8 conversion.
        resolver.workflow_bytes(b"\xff\xfe").unwrap();
    }

    type Captured = Arc<Mutex<Option<n::history::RecordDecisionTaskStartedRequest>>>;

    #[derive(Clone)]
    struct ScriptedHistory(Captured);

    impl tonic::server::NamedService for ScriptedHistory {
        const NAME: &'static str = "uber.cadence.history.v1.HistoryAPI";
    }

    impl tower::Service<tonic::codegen::http::Request<tonic::body::BoxBody>> for ScriptedHistory {
        type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
        >;
        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn call(
            &mut self,
            request: tonic::codegen::http::Request<tonic::body::BoxBody>,
        ) -> Self::Future {
            struct Decision(Captured);
            impl tonic::server::UnaryService<n::history::RecordDecisionTaskStartedRequest> for Decision {
                type Response = n::history::RecordDecisionTaskStartedResponse;
                type Future = std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<tonic::Response<Self::Response>, Status>,
                            > + Send,
                    >,
                >;
                fn call(
                    &mut self,
                    request: Request<n::history::RecordDecisionTaskStartedRequest>,
                ) -> Self::Future {
                    let captured = self.0.clone();
                    Box::pin(async move {
                        *captured.lock().unwrap() = Some(request.into_inner());
                        Ok(tonic::Response::new(
                            n::history::RecordDecisionTaskStartedResponse {
                                scheduled_event_id: 9,
                                attempt: 1,
                                ..Default::default()
                            },
                        ))
                    })
                }
            }
            let captured = self.0.clone();
            let path = request.uri().path().to_owned();
            Box::pin(async move {
                if path != RECORD_DECISION_TASK_STARTED {
                    return Ok(Status::unimplemented("unexpected path").into_http());
                }
                let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::<
                    n::history::RecordDecisionTaskStartedResponse,
                    n::history::RecordDecisionTaskStartedRequest,
                >::default());
                Ok(grpc.unary(Decision(captured), request).await)
            })
        }
    }

    #[tokio::test]
    async fn scripted_peer_receives_original_bytes_and_returns_native_token() {
        let captured: Captured = Arc::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = ScriptedHistory(captured.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let resolver = Arc::new(
            MatchingHistoryPeerResolver::new(
                16,
                NamedPort::Grpc,
                Arc::new(Members(vec![Host {
                    address: "h:1".into(),
                    identity: "h".into(),
                    grpc_address: format!("http://{address}"),
                }])),
            )
            .unwrap(),
        );
        let client = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            1 << 20,
        )
        .with_peer_resolver(resolver);
        let domain = raw(b"dom-\xff");
        let poll = n::api::PollForDecisionTaskRequest {
            domain: raw(b"name-\xfe"),
            identity: raw(b"id-\xff\xfe"),
            ..Default::default()
        };
        let response = client
            .start_decision_native(&domain, &task(), poll.clone())
            .await
            .unwrap();
        let request = captured.lock().unwrap().take().unwrap();
        assert_eq!(request.domain_id, domain);
        let execution = request.workflow_execution.unwrap();
        assert_eq!(execution.workflow_id.as_bytes(), b"wf-\xff\xfe");
        assert_eq!(execution.run_id.as_bytes(), b"run-\xfe\xff");
        assert_eq!(request.poll_request, Some(poll));
        assert_eq!((request.schedule_id, request.task_id), (9, 77));
        let expected = JsonTaskTokenSerializer::serialize_bytes(&TaskTokenBytes {
            domain_id: domain,
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
            schedule_id: 9,
            schedule_attempt: 1,
            ..Default::default()
        });
        assert_eq!(response.task_token, expected);
    }
}
