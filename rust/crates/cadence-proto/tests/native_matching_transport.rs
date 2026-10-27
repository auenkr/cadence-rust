use cadence_proto::{
    native::{api, matching as m, NativeMatchingApi},
    native_service,
};
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
struct Echo(Arc<Mutex<Option<m::AddDecisionTaskRequest>>>);

macro_rules! echo_service {
    ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
        #[tonic::async_trait]
        impl NativeMatchingApi for Echo {
            async fn add_decision_task(&self, request: Request<m::AddDecisionTaskRequest>) -> Result<Response<m::AddDecisionTaskResponse>, Status> {
                *self.0.lock().unwrap() = Some(request.into_inner());
                Ok(Response::new(Default::default()))
            }
            async fn poll_for_decision_task(&self, request: Request<m::PollForDecisionTaskRequest>) -> Result<Response<m::PollForDecisionTaskResponse>, Status> {
                Ok(Response::new(m::PollForDecisionTaskResponse {
                    workflow_type: Some(api::WorkflowType { name: request.into_inner().request.unwrap_or_default().identity }),
                    ..Default::default()
                }))
            }
            $(async fn $method(&self, _request: Request<m::$request>) -> Result<Response<m::$response>, Status> {
                Ok(Response::new(Default::default()))
            })*
        }
    };
}

echo_service! {
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

#[tokio::test]
async fn native_tonic_preserves_raw_requests_replies_and_public_client_compatibility() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let echo = Echo::default();
        let observed = echo.0.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(native_service::matching_api_server::MatchingApiServer::new(
                    echo,
                ))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                ),
        );
        let url = format!("http://{address}");
        let mut client =
            native_service::matching_api_client::MatchingApiClient::connect(url.clone())
                .await
                .unwrap();
        let request = m::AddDecisionTaskRequest {
            domain_id: vec![0xff].into(),
            workflow_execution: Some(api::WorkflowExecution {
                workflow_id: vec![0xfe, 0].into(),
                run_id: "run".into(),
            }),
            partition_config: [
                (vec![0xff].into(), vec![0xfe].into()),
                ("�".into(), "distinct".into()),
            ]
            .into(),
            ..Default::default()
        };
        client.add_decision_task(request.clone()).await.unwrap();
        assert_eq!(observed.lock().unwrap().as_ref(), Some(&request));
        let response = client
            .poll_for_decision_task(m::PollForDecisionTaskRequest {
                request: Some(api::PollForDecisionTaskRequest {
                    identity: vec![0xe1, 0x80, 0xff].into(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.workflow_type.unwrap().name.as_bytes(),
            &[0xe1, 0x80, 0xff]
        );
        let mut public =
            cadence_proto::matching::matching_api_client::MatchingApiClient::connect(url)
                .await
                .unwrap();
        let response = public
            .poll_for_decision_task(cadence_proto::matching::PollForDecisionTaskRequest {
                request: Some(cadence_proto::api::PollForDecisionTaskRequest {
                    identity: "sdk-identity".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.workflow_type.unwrap().name, "sdk-identity");
        drop(public);
        drop(client);
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    })
    .await
    .expect("native tonic test timed out");
}
