//! Share one native handler across gRPC, HTTP Thrift and TChannel listeners.
use cadence_proto::native::{matching as m, NativeMatchingApi};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(crate) struct SharedNativeMatching(pub Arc<dyn NativeMatchingApi>);

static LIST_PARTITIONS_TRACE_ID: AtomicU64 = AtomicU64::new(1);

fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

macro_rules! forward {
    ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
        #[tonic::async_trait]
        impl NativeMatchingApi for SharedNativeMatching {
            async fn list_task_list_partitions(
                &self,
                request: Request<m::ListTaskListPartitionsRequest>,
            ) -> Result<Response<m::ListTaskListPartitionsResponse>, Status> {
                self.trace_list_task_list_partitions(request).await
            }
            $(async fn $method(&self, request: Request<m::$request>) -> Result<Response<m::$response>, Status> {
                self.0.$method(request).await
            })*
        }
    };
}

forward! {
    poll_for_decision_task: PollForDecisionTaskRequest => PollForDecisionTaskResponse,
    add_decision_task: AddDecisionTaskRequest => AddDecisionTaskResponse,
    poll_for_activity_task: PollForActivityTaskRequest => PollForActivityTaskResponse,
    add_activity_task: AddActivityTaskRequest => AddActivityTaskResponse,
    query_workflow: QueryWorkflowRequest => QueryWorkflowResponse,
    respond_query_task_completed: RespondQueryTaskCompletedRequest => RespondQueryTaskCompletedResponse,
    cancel_outstanding_poll: CancelOutstandingPollRequest => CancelOutstandingPollResponse,
    describe_task_list: DescribeTaskListRequest => DescribeTaskListResponse,
    get_task_lists_by_domain: GetTaskListsByDomainRequest => GetTaskListsByDomainResponse,
    update_task_list_partition_config: UpdateTaskListPartitionConfigRequest => UpdateTaskListPartitionConfigResponse,
    refresh_task_list_partition_config: RefreshTaskListPartitionConfigRequest => RefreshTaskListPartitionConfigResponse,
    acquire_semaphore: AcquireSemaphoreRequest => AcquireSemaphoreResponse,
}

impl SharedNativeMatching {
    async fn trace_list_task_list_partitions(
        &self,
        request: Request<m::ListTaskListPartitionsRequest>,
    ) -> Result<Response<m::ListTaskListPartitionsResponse>, Status> {
        // Presence opts into payload-free tracing at the shared native boundary.
        if std::env::var_os("CADENCE_MATCHING_TRACE_LIST_TASK_LIST_PARTITIONS").is_none() {
            return self.0.list_task_list_partitions(request).await;
        }

        let Ok(request_id) = LIST_PARTITIONS_TRACE_ID.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |id| (id <= 64).then_some(id + 1),
        ) else {
            return self.0.list_task_list_partitions(request).await;
        };
        let started = Instant::now();
        eprintln!(
            "matching_list_task_list_partitions event=entry request_id=lptp-{request_id:016x} timestamp_unix_ms={}",
            unix_timestamp_millis()
        );
        let result = self.0.list_task_list_partitions(request).await;
        let elapsed_us = started.elapsed().as_micros();
        match &result {
            Ok(_) => eprintln!(
                "matching_list_task_list_partitions event=completion request_id=lptp-{request_id:016x} timestamp_unix_ms={} elapsed_us={elapsed_us} category=ok",
                unix_timestamp_millis()
            ),
            Err(status) => eprintln!(
                "matching_list_task_list_partitions event=error request_id=lptp-{request_id:016x} timestamp_unix_ms={} elapsed_us={elapsed_us} category=grpc_status_{:?}",
                unix_timestamp_millis(),
                status.code()
            ),
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::go_string::GoString;
    use cadence_thrift::value::{Struct, Value};
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct NativePeer(Mutex<Vec<Vec<u8>>>);

    macro_rules! peer {
        ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
            #[tonic::async_trait]
            impl NativeMatchingApi for NativePeer {
                async fn query_workflow(&self, request: Request<m::QueryWorkflowRequest>)
                    -> Result<Response<m::QueryWorkflowResponse>, Status> {
                    self.0.lock().unwrap().push(request.into_inner().task_list.unwrap_or_default().name.as_bytes().to_vec());
                    Err(crate::matching_native_status::raw_detailed_status(tonic::Code::InvalidArgument,
                        &GoString::from(&b"bad\xff"[..]), "QueryFailedError", Vec::new()))
                }
                $(async fn $method(&self, _request: Request<m::$request>)
                    -> Result<Response<m::$response>, Status> {
                    Ok(Response::new(Default::default()))
                })*
            }
        };
    }
    peer! {
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

    struct AbortOnDrop(tokio::task::AbortHandle);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    #[tokio::test]
    async fn shared_matching_handle_routes_raw_thrift_and_grpc_calls_to_native_service() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let peer = Arc::new(NativePeer::default());
            let matching = crate::grpc_matching::Matching::new(Default::default())
                .with_native_service(peer.clone());
            let args = Struct {
                fields: vec![(
                    1,
                    Value::Struct(Struct {
                        fields: vec![(
                            20,
                            Value::Struct(Struct {
                                fields: vec![(10, Value::Bytes(vec![0xff]))],
                            }),
                        )],
                    }),
                )],
            };
            let (result, summary) = crate::thrift_matching::dispatch(
                &matching,
                "MatchingService::QueryWorkflow",
                &args,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(summary.unwrap().0, "QueryFailedError");
            let Value::Struct(error) = &result.fields[0].1 else {
                panic!("exception struct")
            };
            assert_eq!(error.fields, vec![(1, Value::Bytes(b"bad\xff".to_vec()))]);

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(crate::grpc_matching::serve_with_listener(
                listener, matching,
            ));
            let _cleanup = AbortOnDrop(server.abort_handle());
            let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut client =
                cadence_proto::native_service::matching_api_client::MatchingApiClient::new(channel);
            let mut request = Request::new(m::QueryWorkflowRequest {
                task_list: Some(cadence_proto::native::api::TaskList {
                    name: GoString::from(vec![0xfe]),
                    ..Default::default()
                }),
                ..Default::default()
            });
            request.set_timeout(Duration::from_secs(5));
            let error = client.query_workflow(request).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert_eq!(error.message(), "bad�");
            assert!(error.details().is_empty());
            assert!(error
                .metadata()
                .get_bin(crate::matching_native_status::RAW_MESSAGE_MARKER)
                .is_none());
            assert!(error
                .metadata()
                .get_bin(crate::matching_native_status::RAW_ERROR_DETAILS_MARKER)
                .is_none());
            assert_eq!(*peer.0.lock().unwrap(), vec![vec![0xff], vec![0xfe]]);
            server.abort();
            let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
        })
        .await
        .unwrap();
    }
}
