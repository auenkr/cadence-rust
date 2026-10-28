//! Adapters implementing the dispatcher's narrow `NativeMatchingApi` over the
//! real byte-preserving native Matching surface: in-process
//! (`cadence_proto::native::NativeMatchingApi`) and remote
//! (`native_service::matching_api_client::MatchingApiClient<Channel>`).
//! Raw requests and replies pass through untouched; the remaining caller
//! deadline becomes the gRPC timeout; remote calls carry the YARPC routing
//! headers; every call runs inside the legacy `inject_client_call` operation.
//!
//! Polls use `matching-poll-for-decision-task`/`matching-poll-for-activity-task`
//! and adds use `matching-add-decision-task`/`matching-add-activity-task`. A parent that is itself a
//! `NativeParent::Dispatcher` must be wrapped separately by the caller so
//! faults are not injected twice.
//!
//! Additive and unverified: not compiled, linted or tested; module
//! registration in `lib.rs` is the parent's job.
use crate::matching_dispatch_native::{NativeFuture, NativeMatchingApi as DispatchApi};
use cadence_proto::native::{self as n, NativeMatchingApi as ServiceApi};
use cadence_proto::native_service::matching_api_client::MatchingApiClient;
use std::sync::Arc;
use tonic::{transport::Channel, Request, Status};

const POLL_DECISION: &str = "matching-poll-for-decision-task";
const POLL_ACTIVITY: &str = "matching-poll-for-activity-task";
const ADD_DECISION: &str = "matching-add-decision-task";
const ADD_ACTIVITY: &str = "matching-add-activity-task";

fn timed<T>(message: T, deadline: tokio::time::Instant) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
    request
}

/// Parent served by this process through the full native service trait.
pub struct InProcessNativeMatching(pub Arc<dyn ServiceApi>);

impl DispatchApi for InProcessNativeMatching {
    fn poll_for_decision_task(
        &self,
        request: n::matching::PollForDecisionTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<n::matching::PollForDecisionTaskResponse, Status>> {
        let service = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(POLL_DECISION, async move {
                service.poll_for_decision_task(timed(request, deadline)).await
            })
            .await
            .map(tonic::Response::into_inner)
        })
    }
    fn poll_for_activity_task(
        &self,
        request: n::matching::PollForActivityTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<n::matching::PollForActivityTaskResponse, Status>> {
        let service = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(POLL_ACTIVITY, async move {
                service.poll_for_activity_task(timed(request, deadline)).await
            })
            .await
            .map(tonic::Response::into_inner)
        })
    }
    fn add_decision_task(
        &self,
        request: n::matching::AddDecisionTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<(), Status>> {
        let service = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(ADD_DECISION, async move {
                service.add_decision_task(timed(request, deadline)).await
            })
            .await
            .map(|_| ())
        })
    }
    fn add_activity_task(
        &self,
        request: n::matching::AddActivityTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<(), Status>> {
        let service = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(ADD_ACTIVITY, async move {
                service.add_activity_task(timed(request, deadline)).await
            })
            .await
            .map(|_| ())
        })
    }
}

/// Parent reached over a tonic channel (native raw-byte codec client).
pub struct RemoteNativeMatching(pub Channel);

fn remote<T>(message: T, deadline: tokio::time::Instant) -> Request<T> {
    let mut request = timed(message, deadline);
    crate::matching_dispatch::matching_yarpc_grpc_metadata(&mut request);
    request
}

impl DispatchApi for RemoteNativeMatching {
    fn poll_for_decision_task(
        &self,
        request: n::matching::PollForDecisionTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<n::matching::PollForDecisionTaskResponse, Status>> {
        let channel = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(POLL_DECISION, async move {
                MatchingApiClient::new(channel)
                    .poll_for_decision_task(remote(request, deadline))
                    .await
            })
            .await
            .map(tonic::Response::into_inner)
        })
    }
    fn poll_for_activity_task(
        &self,
        request: n::matching::PollForActivityTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<n::matching::PollForActivityTaskResponse, Status>> {
        let channel = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(POLL_ACTIVITY, async move {
                MatchingApiClient::new(channel)
                    .poll_for_activity_task(remote(request, deadline))
                    .await
            })
            .await
            .map(tonic::Response::into_inner)
        })
    }
    fn add_decision_task(
        &self,
        request: n::matching::AddDecisionTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<(), Status>> {
        let channel = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(ADD_DECISION, async move {
                MatchingApiClient::new(channel)
                    .add_decision_task(remote(request, deadline))
                    .await
            })
            .await
            .map(|_| ())
        })
    }
    fn add_activity_task(
        &self,
        request: n::matching::AddActivityTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<(), Status>> {
        let channel = self.0.clone();
        Box::pin(async move {
            crate::matching_fault_injection::inject_client_call(ADD_ACTIVITY, async move {
                MatchingApiClient::new(channel)
                    .add_activity_task(remote(request, deadline))
                    .await
            })
            .await
            .map(|_| ())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_proto::native::{api, matching as m};
    use std::{sync::Mutex, time::Duration};

    type CapturedHeaders = Arc<Mutex<Vec<(String, Vec<(String, Vec<u8>)>)>>>;
    #[derive(Clone, Default)]
    struct Peer {
        headers: CapturedHeaders,
        added: Arc<Mutex<Option<m::AddDecisionTaskRequest>>>,
        polled: Arc<Mutex<Option<m::PollForDecisionTaskRequest>>>,
    }
    impl Peer {
        fn note<T>(&self, op: &str, request: &Request<T>) {
            let headers = request
                .metadata()
                .iter()
                .filter_map(|entry| match entry {
                    tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                        Some((k.as_str().to_owned(), v.as_bytes().to_vec()))
                    }
                    _ => None,
                })
                .collect();
            self.headers.lock().unwrap().push((op.to_owned(), headers));
        }
    }

    macro_rules! peer_service {
        ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
            #[tonic::async_trait]
            impl ServiceApi for Peer {
                async fn poll_for_decision_task(&self, request: Request<m::PollForDecisionTaskRequest>) -> Result<tonic::Response<m::PollForDecisionTaskResponse>, Status> {
                    self.note("poll-decision", &request);
                    let request = request.into_inner();
                    let identity = request.request.clone().unwrap_or_default().identity;
                    *self.polled.lock().unwrap() = Some(request);
                    Ok(tonic::Response::new(m::PollForDecisionTaskResponse {
                        workflow_type: Some(api::WorkflowType { name: identity }),
                        ..Default::default()
                    }))
                }
                async fn poll_for_activity_task(&self, request: Request<m::PollForActivityTaskRequest>) -> Result<tonic::Response<m::PollForActivityTaskResponse>, Status> {
                    self.note("poll-activity", &request);
                    // Scripted stall: the caller's deadline must expire first.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok(tonic::Response::new(Default::default()))
                }
                async fn add_decision_task(&self, request: Request<m::AddDecisionTaskRequest>) -> Result<tonic::Response<m::AddDecisionTaskResponse>, Status> {
                    self.note("add-decision", &request);
                    *self.added.lock().unwrap() = Some(request.into_inner());
                    Ok(tonic::Response::new(Default::default()))
                }
                $(async fn $method(&self, _request: Request<m::$request>) -> Result<tonic::Response<m::$response>, Status> {
                    Ok(tonic::Response::new(Default::default()))
                })*
            }
        };
    }
    peer_service! {
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

    fn header<'a>(headers: &'a [(String, Vec<u8>)], name: &str) -> Option<&'a [u8]> {
        headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_slice())
    }

    #[tokio::test]
    async fn remote_adapter_preserves_raw_fields_headers_deadline_and_reply() {
        tokio::time::timeout(Duration::from_secs(20), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let peer = Peer::default();
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(
                        cadence_proto::native_service::matching_api_server::MatchingApiServer::new(
                            peer.clone(),
                        ),
                    )
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
            let adapter = RemoteNativeMatching(channel);
            let deadline = || tokio::time::Instant::now() + Duration::from_secs(5);

            let add = m::AddDecisionTaskRequest {
                domain_id: vec![0xff].into(),
                workflow_execution: Some(api::WorkflowExecution {
                    workflow_id: vec![0xfe, 0].into(),
                    run_id: "run".into(),
                }),
                forwarded_from: vec![0xc3].into(),
                partition_config: [(vec![0xff].into(), vec![0xfe].into())].into(),
                ..Default::default()
            };
            adapter.add_decision_task(add.clone(), deadline()).await.unwrap();
            assert_eq!(peer.added.lock().unwrap().as_ref(), Some(&add));

            let poll = m::PollForDecisionTaskRequest {
                domain_id: vec![0xfd].into(),
                poller_id: vec![0xfc].into(),
                forwarded_from: vec![0xe1, 0x80].into(),
                request: Some(api::PollForDecisionTaskRequest {
                    identity: vec![0xe1, 0x80, 0xff].into(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let reply = adapter.poll_for_decision_task(poll.clone(), deadline()).await.unwrap();
            assert_eq!(peer.polled.lock().unwrap().as_ref(), Some(&poll));
            assert_eq!(reply.workflow_type.unwrap().name.as_bytes(), &[0xe1, 0x80, 0xff]);

            {
                let seen = peer.headers.lock().unwrap();
                assert_eq!(seen.len(), 2);
                for (_, headers) in seen.iter() {
                    assert_eq!(header(headers, "rpc-service"), Some(&b"cadence-matching"[..]));
                    assert_eq!(header(headers, "rpc-caller"), Some(&b"cadence-matching"[..]));
                    assert_eq!(header(headers, "rpc-encoding"), Some(&b"proto"[..]));
                    assert!(header(headers, "grpc-timeout").is_some());
                }
            }

            let started = std::time::Instant::now();
            let timeout = adapter
                .poll_for_activity_task(
                    Default::default(),
                    tokio::time::Instant::now() + Duration::from_millis(200),
                )
                .await
                .expect_err("stalled peer must hit the caller deadline");
            assert!(started.elapsed() < Duration::from_secs(10), "{timeout:?}");

            drop(adapter);
            stop.send(()).unwrap();
            server.await.unwrap().unwrap();
        })
        .await
        .expect("remote native adapter test timed out");
    }
}
