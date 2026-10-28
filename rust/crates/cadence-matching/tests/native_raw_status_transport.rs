//! UNVERIFIED (not compiled or run locally): real HTTP/2 served-transport
//! regression for `matching_native_status::RawStatusService` wrapped around
//! the native Matching tonic server. A tower wrapper around the real
//! `Channel` records the raw HTTP response headers before tonic's client
//! decodes `grpc-message`.

use cadence_common::go_string::GoString;
use cadence_matching::matching_native_status::{
    encode_go_grpc_message, raw_detailed_status, raw_status, RawStatusService, RAW_MESSAGE_MARKER,
};
use cadence_proto::{
    native::{matching as m, NativeMatchingApi},
    native_service,
};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tonic::codegen::http::{self, HeaderMap};
use tonic::codegen::{Bytes, Service};
use tonic::{Code, Request, Response, Status};

const DETAIL: &[u8] = &[0x0a, 0x02, b'o', b'k'];

#[derive(Clone)]
struct Failing;

fn failure(identity: &[u8]) -> Status {
    match identity {
        b"invalid" => raw_status(
            Code::NotFound,
            &GoString::from(&b"a\xffb\xe2\x82 %\n"[..]),
            Bytes::new(),
        ),
        b"typed" => raw_detailed_status(
            Code::NotFound,
            &GoString::from("valid \u{e9}%"),
            "EntityNotExistsError",
            DETAIL.to_vec(),
        ),
        _ => raw_detailed_status(
            Code::NotFound,
            &GoString::from(&b"bad\xff"[..]),
            "EntityNotExistsError",
            DETAIL.to_vec(),
        ),
    }
}

macro_rules! failing_service {
    ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
        #[tonic::async_trait]
        impl NativeMatchingApi for Failing {
            async fn poll_for_decision_task(&self, request: Request<m::PollForDecisionTaskRequest>) -> Result<Response<m::PollForDecisionTaskResponse>, Status> {
                let identity = request.into_inner().request.unwrap_or_default().identity;
                Err(failure(identity.as_bytes()))
            }
            $(async fn $method(&self, _request: Request<m::$request>) -> Result<Response<m::$response>, Status> {
                Err(Status::unimplemented("unused"))
            })*
        }
    };
}

failing_service! {
    add_decision_task: AddDecisionTaskRequest => AddDecisionTaskResponse,
    poll_for_activity_task: PollForActivityTaskRequest => PollForActivityTaskResponse,
    add_activity_task: AddActivityTaskRequest => AddActivityTaskResponse,
    query_workflow: QueryWorkflowRequest => QueryWorkflowResponse,
    respond_query_task_completed: RespondQueryTaskCompletedRequest => RespondQueryTaskCompletedResponse,
    cancel_outstanding_poll: CancelOutstandingPollRequest => CancelOutstandingPollResponse,
    describe_task_list: DescribeTaskListRequest => DescribeTaskListResponse,
    list_task_list_partitions: ListTaskListPartitionsRequest => ListTaskListPartitionsResponse,
    get_task_lists_by_domain: GetTaskListsByDomainRequest => GetTaskListsByDomainResponse,
    update_task_list_partition_config: UpdateTaskListPartitionConfigRequest => UpdateTaskListPartitionConfigResponse,
    refresh_task_list_partition_config: RefreshTaskListPartitionConfigRequest => RefreshTaskListPartitionConfigResponse,
    acquire_semaphore: AcquireSemaphoreRequest => AcquireSemaphoreResponse,
}

/// Records the response headers of every call before the tonic client
/// decodes them.
#[derive(Clone)]
struct Capture<S> {
    inner: S,
    seen: Arc<Mutex<Vec<HeaderMap>>>,
}

impl<S, B, R> Service<http::Request<B>> for Capture<S>
where
    S: Service<http::Request<B>, Response = http::Response<R>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let fut = self.inner.call(req);
        let seen = self.seen.clone();
        Box::pin(async move {
            let response = fut.await?;
            seen.lock().unwrap().push(response.headers().clone());
            Ok(response)
        })
    }
}

fn poll(identity: &[u8]) -> m::PollForDecisionTaskRequest {
    m::PollForDecisionTaskRequest {
        request: Some(cadence_proto::native::api::PollForDecisionTaskRequest {
            identity: identity.to_vec().into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn served_raw_status_normalizes_message_details_and_marker() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RawStatusService::new(
                    native_service::matching_api_server::MatchingApiServer::new(Failing),
                ))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                ),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut client = native_service::matching_api_client::MatchingApiClient::new(Capture {
            inner: channel,
            seen: seen.clone(),
        });

        for identity in [&b"invalid"[..], b"typed", b"invalid-outer"] {
            let status = client
                .poll_for_decision_task(poll(identity))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::NotFound);
        }
        let headers = seen.lock().unwrap().clone();
        assert_eq!(headers.len(), 3);
        for h in &headers {
            assert!(h.get(RAW_MESSAGE_MARKER).is_none(), "marker leaked: {h:?}");
            assert!(
                h.get("x-cadence-raw-error-details-bin").is_none(),
                "typed marker leaked: {h:?}"
            );
            assert_eq!(h["grpc-status"], "5");
        }

        // Original invalid bytes: Go RuneError-per-byte normalization.
        let expect = encode_go_grpc_message(b"a\xffb\xe2\x82 %\n");
        assert_eq!(expect, "a%EF%BF%BDb%EF%BF%BD%EF%BF%BD %25%0A");
        assert_eq!(headers[0]["grpc-message"], expect.as_str());
        assert!(headers[0].get("grpc-status-details-bin").is_none());

        // Valid message keeps typed details.
        assert_eq!(headers[1]["grpc-message"], "valid %C3%A9%25");
        let details = headers[1]["grpc-status-details-bin"].to_str().unwrap();
        use base64::Engine;
        let raw = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(details.trim_end_matches('='))
            .unwrap();
        assert!(raw.windows(DETAIL.len()).any(|w| w == DETAIL));
        assert!(raw
            .windows(b"uber.cadence.api.v1.EntityNotExistsError".len())
            .any(|w| w == b"uber.cadence.api.v1.EntityNotExistsError"));

        // Invalid outer Status.message: grpc-message sent, details omitted.
        assert_eq!(headers[2]["grpc-message"], "bad%EF%BF%BD");
        assert!(headers[2].get("grpc-status-details-bin").is_none());

        drop(client);
        stop.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("bounded server shutdown")
            .unwrap()
            .unwrap();
    })
    .await
    .expect("test timeout");
}
