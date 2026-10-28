//! Real TCP boundary tests for the native handler attached to both Thrift listeners.
use cadence_common::go_string::GoString;
use cadence_matching::{grpc_matching::Matching, matching_native_status::{raw_detailed_status, raw_status}, tchannel, thrift_matching};
use cadence_proto::native::{matching as m, NativeMatchingApi};
use cadence_service_core::{ringpop_tchannel::{RingpopError, RingpopHandler}, tchannel_json_client::call_thrift};
use cadence_thrift::{value::{read_struct, write_struct, Struct, Value}, Reader, Writer};
use std::{sync::{Arc, Mutex}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::mpsc};
use tonic::{Code, Request, Response, Status};

const LIMIT: Duration = Duration::from_secs(15);
const PROCEDURE: &str = "MatchingService::QueryWorkflow";

#[derive(Default)]
struct Peer {
    seen: Mutex<Vec<(Vec<u8>, bool)>>,
    dropped: Mutex<Option<mpsc::UnboundedSender<()>>>,
}
struct DropSignal(mpsc::UnboundedSender<()>);
impl Drop for DropSignal {
    fn drop(&mut self) { let _ = self.0.send(()); }
}
macro_rules! service {
    ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
        #[tonic::async_trait]
        impl NativeMatchingApi for Peer {
            async fn query_workflow(&self, request: Request<m::QueryWorkflowRequest>) -> Result<Response<m::QueryWorkflowResponse>, Status> {
                let deadline = request.metadata().contains_key("grpc-timeout");
                let name = request.into_inner().task_list.unwrap_or_default().name.as_bytes().to_vec();
                self.seen.lock().unwrap().push((name.clone(), deadline));
                if name == b"wait" {
                    let _guard = DropSignal(self.dropped.lock().unwrap().as_ref().unwrap().clone());
                    std::future::pending::<()>().await;
                }
                if name == [0xff] {
                    Err(raw_detailed_status(Code::InvalidArgument, &GoString::from(&b"query\xff"[..]), "QueryFailedError", Vec::new()))
                } else {
                    Err(raw_status(Code::Internal, &GoString::from(&b"system\xfe"[..]), Default::default()))
                }
            }
            $(async fn $method(&self, _request: Request<m::$request>) -> Result<Response<m::$response>, Status> { Ok(Response::new(Default::default())) })*
        }
    };
}
service! {
    poll_for_decision_task: PollForDecisionTaskRequest => PollForDecisionTaskResponse,
    add_decision_task: AddDecisionTaskRequest => AddDecisionTaskResponse,
    poll_for_activity_task: PollForActivityTaskRequest => PollForActivityTaskResponse,
    add_activity_task: AddActivityTaskRequest => AddActivityTaskResponse,
    respond_query_task_completed: RespondQueryTaskCompletedRequest => RespondQueryTaskCompletedResponse,
    cancel_outstanding_poll: CancelOutstandingPollRequest => CancelOutstandingPollResponse,
    describe_task_list: DescribeTaskListRequest => DescribeTaskListResponse,
    list_task_list_partitions: ListTaskListPartitionsRequest => ListTaskListPartitionsResponse,
    get_task_lists_by_domain: GetTaskListsByDomainRequest => GetTaskListsByDomainResponse,
    update_task_list_partition_config: UpdateTaskListPartitionConfigRequest => UpdateTaskListPartitionConfigResponse,
    refresh_task_list_partition_config: RefreshTaskListPartitionConfigRequest => RefreshTaskListPartitionConfigResponse,
    acquire_semaphore: AcquireSemaphoreRequest => AcquireSemaphoreResponse,
}
fn request(name: &[u8]) -> Vec<u8> {
    let args = Struct { fields: vec![(1, Value::Struct(Struct { fields: vec![(20, Value::Struct(Struct { fields: vec![(10, Value::Bytes(name.to_vec()))] }))] }))] };
    let mut writer = Writer::new();
    write_struct(&mut writer, &args);
    writer.into_bytes()
}
fn assert_exception(body: &[u8], expected_name: &str, message: &[u8]) {
    let result = read_struct(&mut Reader::new(body)).unwrap();
    let method = cadence_thrift::service::find_method(PROCEDURE).unwrap();
    let id = method.exceptions.iter().find(|(name, _)| *name == expected_name).unwrap().1;
    assert_eq!(result.fields, vec![(id, Value::Struct(Struct { fields: vec![(1, Value::Bytes(message.to_vec()))] }))]);
}
async fn http_call(addr: std::net::SocketAddr, name: &[u8], ttl: u64) -> (String, Vec<u8>) {
    let body = request(name);
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let headers = format!("POST / HTTP/1.1\r\nHost: localhost\r\nRpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\nRpc-Procedure: {PROCEDURE}\r\nRpc-Caller: test-client\r\nContext-TTL-MS: {ttl}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let split = response.windows(4).position(|v| v == b"\r\n\r\n").unwrap() + 4;
    (String::from_utf8(response[..split].to_vec()).unwrap().to_ascii_lowercase(), response[split..].to_vec())
}
struct Abort(tokio::task::AbortHandle);
impl Drop for Abort { fn drop(&mut self) { self.0.abort(); } }

#[tokio::test]
async fn attached_native_api_thrift_listeners_preserve_bytes_and_enforce_deadlines() {
    tokio::time::timeout(LIMIT, async {
        let peer = Arc::new(Peer::default());
        let (dropped, mut drops) = mpsc::unbounded_channel();
        *peer.dropped.lock().unwrap() = Some(dropped);
        let matching = Matching::new(Default::default()).with_native_service(peer.clone());
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http.local_addr().unwrap();
        let http_server = tokio::spawn(async move { axum::serve(http, thrift_matching::router(matching)).await.unwrap() });
        let _http_guard = Abort(http_server.abort_handle());
        let tc = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tc_addr = tc.local_addr().unwrap();
        let matching = Matching::new(Default::default()).with_native_service(peer.clone());
        let ringpop = RingpopHandler::new(|_, _, _| async { Err(RingpopError::System("unused".into())) });
        let tc_server = tokio::spawn(tchannel::serve_matching_with_ringpop_listener(tc, matching, ringpop));
        let _tc_guard = Abort(tc_server.abort_handle());
        for (name, error, message) in [(&b"\xff"[..], "QueryFailedError", &b"query\xff"[..]), (&b"\xfe"[..], "InternalServiceError", &b"system\xfe"[..])] {
            let (headers, body) = http_call(http_addr, name, 5000).await;
            assert!(headers.starts_with("http/1.1 200"), "{headers}");
            assert!(headers.contains("rpc-status: error"), "{headers}");
            assert!(headers.contains(&format!("rpc-application-error-name: {}", error.to_ascii_lowercase())), "{headers}");
            assert!(headers.contains("rpc-application-error-details:"), "{headers}");
            assert_exception(&body, error, message);
            let response = call_thrift(&tc_addr.to_string(), "cadence-matching", PROCEDURE, "test-client", &[], &request(name), Duration::from_secs(5)).await.unwrap();
            assert!(response.application_error);
            assert!(response.headers.iter().any(|(k, v)| k == "$rpc$-application-error-name" && v == error));
            assert!(response.headers.iter().any(|(k, _)| k == "$rpc$-application-error-details"));
            assert_exception(&response.body, error, message);
        }
        let (headers, _) = http_call(http_addr, b"wait", 100).await;
        assert!(headers.starts_with("http/1.1 504"), "{headers}");
        tokio::time::timeout(Duration::from_secs(2), drops.recv()).await.unwrap().unwrap();
        let result = call_thrift(&tc_addr.to_string(), "cadence-matching", PROCEDURE, "test-client", &[], &request(b"wait"), Duration::from_millis(250)).await;
        assert!(result.is_err());
        tokio::time::timeout(Duration::from_secs(2), drops.recv()).await.unwrap().unwrap();
        assert_eq!(*peer.seen.lock().unwrap(), vec![(vec![0xff], true), (vec![0xff], true), (vec![0xfe], true), (vec![0xfe], true), (b"wait".to_vec(), true), (b"wait".to_vec(), true)]);
    }).await.expect("bounded transport test");
}
