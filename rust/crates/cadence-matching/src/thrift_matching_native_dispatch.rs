//! Native invocation for every method in the pinned Matching Thrift service.
//! The lifecycle handle supplies the existing startup/metrics and nil-request
//! behavior; non-nil task data goes exclusively through the native service.
use super::*;
use crate::{
    thrift_matching_native as decision_bytes, thrift_matching_native_extra as requests,
    thrift_matching_native_responses as responses,
};
use cadence_proto::native::NativeMatchingApi;

pub async fn dispatch_native_with_context(
    lifecycle: &Matching,
    service: &dyn NativeMatchingApi,
    procedure: &str,
    args: &Struct,
    deadline: Instant,
    context: crate::matching_metrics::InboundMetricContext,
) -> Result<(Struct, Option<(&'static str, String)>), String> {
    lifecycle
        .inbound_metrics()
        .with_inbound_context(context, async {
            let method = cadence_thrift::service::find_method(procedure)
                .ok_or_else(|| format!("Unknown Matching procedure {procedure}"))?;
            if method.service == "Meta" {
                return dispatch_inner(lifecycle, procedure, args, deadline).await;
            }
            if method.service != "MatchingService" {
                return Err("Unknown Matching service".into());
            }
            lifecycle.wait_started().await;
            let request = args.fields.iter().rev().find_map(|(id, value)| {
                if let (1, Value::Struct(value)) = (id, value) {
                    Some(value)
                } else {
                    None
                }
            });
            // Only nil requests use the existing mapper-panic/admission path.
            // No identifier bytes are present in that case.
            if request.is_none()
                && matches!(
                    method.method,
                    "PollForDecisionTask"
                        | "PollForActivityTask"
                        | "AddDecisionTask"
                        | "AddActivityTask"
                        | "DescribeTaskList"
                )
            {
                return dispatch_inner(lifecycle, procedure, args, deadline).await;
            }
            let empty = Struct::default();
            let request = request.unwrap_or(&empty);
            let normalized = request_struct(method.method).map(|name| normalize(name, request));
            let request = normalized.as_ref().unwrap_or(request);
            let outcome = if matches!(method.method, "PollForDecisionTask" | "PollForActivityTask")
            {
                let budget = deadline
                    .saturating_duration_since(Instant::now())
                    .saturating_sub(std::time::Duration::from_secs(1))
                    .min(lifecycle.poll_expiration);
                match tokio::time::timeout(
                    budget,
                    invoke(service, method.method, request, deadline),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) if method.method == "PollForDecisionTask" => {
                        decision_bytes::poll_for_decision_task_response(Default::default())
                            .map(Some)
                            .map_err(Status::internal)
                    }
                    Err(_) => Ok(Some(responses::poll_for_activity_task_response(
                        Default::default(),
                    ))),
                }
            } else {
                invoke(service, method.method, request, deadline).await
            };
            match outcome {
                Ok(Some(value)) => Ok((
                    Struct {
                        fields: vec![(0, Value::Struct(value))],
                    },
                    None,
                )),
                Ok(None) => Ok((Struct::default(), None)),
                Err(status) if crate::matching_panic::is_captured_panic(&status) => {
                    Err(status.message().to_owned())
                }
                Err(status) => Ok(application_error_bytes(method, status)),
            }
        })
        .await
}

async fn invoke(
    service: &dyn NativeMatchingApi,
    method: &str,
    request: &Struct,
    deadline: Instant,
) -> Result<Option<Struct>, Status> {
    macro_rules! call {
        ($method:ident, $decoder:path) => {
            service
                .$method(rpc_request($decoder(request).map_err(invalid)?, deadline))
                .await?
                .into_inner()
        };
    }
    Ok(match method {
        "PollForDecisionTask" => Some(
            decision_bytes::poll_for_decision_task_response(call!(
                poll_for_decision_task,
                decision_bytes::poll_for_decision_task
            ))
            .map_err(Status::internal)?,
        ),
        "PollForActivityTask" => Some(responses::poll_for_activity_task_response(call!(
            poll_for_activity_task,
            requests::poll_for_activity_task
        ))),
        "AddDecisionTask" => {
            call!(add_decision_task, decision_bytes::add_decision_task);
            None
        }
        "AddActivityTask" => {
            call!(add_activity_task, requests::add_activity_task);
            None
        }
        "QueryWorkflow" => Some(responses::query_workflow_response(call!(
            query_workflow,
            requests::query_workflow
        ))),
        "RespondQueryTaskCompleted" => {
            call!(
                respond_query_task_completed,
                requests::respond_query_task_completed
            );
            None
        }
        "CancelOutstandingPoll" => {
            call!(cancel_outstanding_poll, requests::cancel_outstanding_poll);
            None
        }
        "DescribeTaskList" => Some(responses::describe_task_list_response(call!(
            describe_task_list,
            requests::describe_task_list
        ))),
        "GetTaskListsByDomain" => Some(responses::get_task_lists_by_domain_response(call!(
            get_task_lists_by_domain,
            requests::get_task_lists_by_domain
        ))),
        "ListTaskListPartitions" => Some(responses::list_task_list_partitions_response(call!(
            list_task_list_partitions,
            requests::list_task_list_partitions
        ))),
        _ => return Err(Status::unimplemented("Unknown MatchingService method.")),
    })
}

#[derive(Clone, PartialEq, Message)]
struct RawEnvelope {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(bytes = "vec", tag = "2")]
    message: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

// Every field of the supported typed error payloads is a protobuf string.
// Decode them as repeated byte fields to retain invalid UTF-8 and repeated
// active-cluster entries; singular fields use the last occurrence.
#[derive(Clone, PartialEq, Message)]
struct ErrorStrings {
    #[prost(bytes = "vec", repeated, tag = "1")]
    one: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    two: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "3")]
    three: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    four: Vec<Vec<u8>>,
}

fn application_error_bytes(
    method: &cadence_thrift::service::MethodDef,
    status: Status,
) -> (Struct, Option<(&'static str, String)>) {
    use crate::matching_native_status::{RAW_ERROR_DETAILS_MARKER, RAW_MESSAGE_MARKER};
    let envelope = status
        .metadata()
        .get_bin(RAW_ERROR_DETAILS_MARKER)
        .and_then(|value| value.to_bytes().ok())
        .and_then(|bytes| RawEnvelope::decode(bytes).ok())
        .or_else(|| RawEnvelope::decode(status.details()).ok());
    let detail = envelope.and_then(|value| value.details.into_iter().next());
    let typed = detail
        .as_ref()
        .and_then(|value| value.type_url.rsplit('.').next());
    let raw_message = status
        .metadata()
        .get_bin(RAW_MESSAGE_MARKER)
        .and_then(|value| value.to_bytes().ok())
        .map(|value| value.to_vec())
        .unwrap_or_else(|| status.message().as_bytes().to_vec());
    let (mut result, mut summary) = application_error(method, status);
    let name = typed
        .and_then(|name| {
            method
                .exceptions
                .iter()
                .find(|(candidate, _)| *candidate == name)
                .map(|(name, _)| *name)
        })
        .or_else(|| summary.as_ref().map(|(name, _)| *name))
        .unwrap_or("InternalServiceError");
    let mut fields = vec![(
        if name == "RemoteSyncMatchedError" {
            10
        } else {
            1
        },
        Value::Bytes(raw_message),
    )];
    let strings = detail
        .and_then(|value| ErrorStrings::decode(value.value.as_slice()).ok())
        .unwrap_or_default();
    let scalar = |items: Vec<Vec<u8>>| Value::Bytes(items.into_iter().last().unwrap_or_default());
    let list = |items: Vec<Vec<u8>>| Value::List(items.into_iter().map(Value::Bytes).collect());
    match name {
        "DomainNotActiveError" => fields.extend([
            (2, scalar(strings.one)),
            (3, scalar(strings.two)),
            (4, scalar(strings.three)),
            (5, list(strings.four)),
        ]),
        "EntityNotExistsError" => fields.extend([
            (2, scalar(strings.one)),
            (3, scalar(strings.two)),
            (4, list(strings.three)),
        ]),
        "ServiceBusyError" => fields.push((2, scalar(strings.one))),
        "ClientVersionNotSupportedError" => {
            fields = vec![
                (1, scalar(strings.one)),
                (2, scalar(strings.two)),
                (3, scalar(strings.three)),
            ]
        }
        "TaskListNotOwnedByHostError" => {
            fields = vec![
                (1, scalar(strings.one)),
                (2, scalar(strings.two)),
                (3, scalar(strings.three)),
            ]
        }
        _ => {}
    }
    let id = method
        .exceptions
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, id)| *id)
        .unwrap_or(2);
    result.fields = vec![(id, Value::Struct(Struct { fields }))];
    if let Some((old_name, _)) = &mut summary {
        *old_name = name;
    }
    (result, summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_native_status::raw_detailed_status;
    use cadence_common::go_string::GoString;
    use cadence_proto::native::matching as m;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CapturingService(Mutex<Vec<m::AddDecisionTaskRequest>>, Option<Status>);

    macro_rules! service {
        ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
            #[tonic::async_trait]
            impl NativeMatchingApi for CapturingService {
                async fn add_decision_task(&self, request: Request<m::AddDecisionTaskRequest>)
                    -> Result<tonic::Response<m::AddDecisionTaskResponse>, Status> {
                    assert!(request.metadata().contains_key("grpc-timeout"));
                    self.0.lock().unwrap().push(request.into_inner());
                    match &self.1 {
                        Some(error) => Err(error.clone()),
                        None => Ok(tonic::Response::new(Default::default())),
                    }
                }
                $(async fn $method(&self, _request: Request<m::$request>)
                    -> Result<tonic::Response<m::$response>, Status> {
                    match &self.1 {
                        Some(error) => Err(error.clone()),
                        None => Ok(tonic::Response::new(Default::default())),
                    }
                })*
            }
        };
    }
    service! {
        poll_for_decision_task: PollForDecisionTaskRequest => PollForDecisionTaskResponse,
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
    async fn raw_add_identifiers_and_deadline_reach_native_service() {
        let service = CapturingService::default();
        let request = Struct {
            fields: vec![
                (10, Value::Bytes(vec![0xff])),
                (
                    20,
                    Value::Struct(Struct {
                        fields: vec![
                            (10, Value::Bytes(vec![0xfe])),
                            (20, Value::Bytes(vec![0, 0xff])),
                        ],
                    }),
                ),
                (
                    30,
                    Value::Struct(Struct {
                        fields: vec![(10, Value::Bytes(vec![0xfd]))],
                    }),
                ),
            ],
        };
        assert!(invoke(
            &service,
            "AddDecisionTask",
            &request,
            Instant::now() + std::time::Duration::from_secs(5)
        )
        .await
        .unwrap()
        .is_none());
        let requests = service.0.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].domain_id.as_bytes(), &[0xff]);
        assert_eq!(
            requests[0].task_list.as_ref().unwrap().name.as_bytes(),
            &[0xfd]
        );
        let execution = requests[0].workflow_execution.as_ref().unwrap();
        assert_eq!(execution.workflow_id.as_bytes(), &[0xfe]);
        assert_eq!(execution.run_id.as_bytes(), &[0, 0xff]);
    }

    #[tokio::test]
    async fn every_pinned_thrift_matching_method_has_native_dispatch() {
        let service = CapturingService::default();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        for method in cadence_thrift::service::METHODS
            .iter()
            .filter(|method| method.service == "MatchingService")
        {
            invoke(&service, method.method, &Struct::default(), deadline)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn every_pinned_thrift_matching_wrapper_returns_internal_error_without_success() {
        let service = CapturingService(Mutex::new(Vec::new()), Some(Status::internal("test")));
        let lifecycle = Matching::new(Default::default());
        // A present request reaches the injected native handler even for methods
        // whose nil-request path deliberately uses the legacy panic mapper.
        let args = Struct {
            fields: vec![(1, Value::Struct(Struct::default()))],
        };
        for method in cadence_thrift::service::METHODS
            .iter()
            .filter(|method| method.service == "MatchingService")
        {
            let procedure = format!("MatchingService::{}", method.method);
            let (result, summary) = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                dispatch_native_with_context(
                    &lifecycle,
                    &service,
                    &procedure,
                    &args,
                    Instant::now() + std::time::Duration::from_secs(5),
                    Default::default(),
                ),
            )
            .await
            .expect("bounded native wrapper call")
            .unwrap();
            assert_eq!(
                summary.as_ref().map(|(name, _)| *name),
                Some("InternalServiceError"),
                "{procedure}"
            );
            // The pinned Thrift schema places InternalServiceError at field2.
            // An error reply must contain no success field0, including methods
            // with a response struct and methods with void responses.
            assert_eq!(
                result.fields,
                vec![(
                    2,
                    Value::Struct(Struct {
                        fields: vec![(1, Value::Bytes(b"test".to_vec()))],
                    })
                )],
                "{procedure}"
            );
        }
    }

    #[test]
    fn raw_query_failure_keeps_declared_exception_and_message() {
        let method =
            cadence_thrift::service::find_method("MatchingService::QueryWorkflow").unwrap();
        let raw = [0xff, 0xfe, 0];
        let status = raw_detailed_status(
            tonic::Code::InvalidArgument,
            &GoString::from(raw.to_vec()),
            "QueryFailedError",
            Vec::new(),
        );
        assert!(status.details().is_empty(), "gRPC suppresses this envelope");
        let (result, summary) = application_error_bytes(method, status);
        let id = method
            .exceptions
            .iter()
            .find(|(name, _)| *name == "QueryFailedError")
            .unwrap()
            .1;
        assert_eq!(summary.unwrap().0, "QueryFailedError");
        assert_eq!(
            result,
            Struct {
                fields: vec![(
                    id,
                    Value::Struct(Struct {
                        fields: vec![(1, Value::Bytes(raw.to_vec()))]
                    })
                )]
            }
        );
    }

    #[test]
    fn typed_error_payload_strings_remain_raw() {
        let method =
            cadence_thrift::service::find_method("MatchingService::QueryWorkflow").unwrap();
        let detail = ErrorStrings {
            one: vec![vec![0xff]],
            two: vec![vec![0xfe]],
            three: vec!["�".as_bytes().to_vec()],
            ..Default::default()
        }
        .encode_to_vec();
        let status = raw_detailed_status(
            tonic::Code::FailedPrecondition,
            &GoString::from("task list not owned"),
            "TaskListNotOwnedByHostError",
            detail,
        );
        let (result, summary) = application_error_bytes(method, status);
        assert_eq!(summary.unwrap().0, "TaskListNotOwnedByHostError");
        assert_eq!(
            result.fields[0].0,
            cadence_thrift::service::exception_field(method, "TaskListNotOwnedByHostError")
                .unwrap()
        );
        let Value::Struct(error) = &result.fields[0].1 else {
            panic!("exception struct")
        };
        assert_eq!(
            error.fields,
            vec![
                (1, Value::Bytes(vec![0xff])),
                (2, Value::Bytes(vec![0xfe])),
                (3, Value::Bytes("�".as_bytes().to_vec()))
            ]
        );
    }
}
