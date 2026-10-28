//! Exercises the native backlog and synchronous paths through the actual actors.
use cadence_common::{go_string::GoString, poller_bytes};
use cadence_matching::{
    matching_dispatch_native::{NativePollLifecycle, NativeTaskDispatcher},
    matching_history::MatchingHistoryClient,
    matching_limiter::TaskListLimiter,
    matching_reader_native::{NativeIsolationConfig, NativeTaskReader, ReaderConfig},
    matching_sync_native::NativeSyncMatcher,
    matching_writer::WriterConfig,
    matching_writer_native::NativeTaskWriter,
};
use cadence_persistence::{
    matching_blob::{BlobEncoding, TaskDataBytes},
    matching_native_queue::{DurableTaskQueueBytes, PendingTaskBytes},
    matching_native_task_store::{AllocatedTaskBytes, MatchingTaskStoreBytes, TaskListKeyBytes},
    matching_sqlite::SqliteTaskStore,
};
use cadence_proto::native as n;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    num::NonZeroU32,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use uuid::Uuid;

const LIMIT: Duration = Duration::from_secs(5);
const PATH: &str = "/uber.cadence.history.v1.HistoryAPI/RecordDecisionTaskStarted";
type Captured = Arc<Mutex<Vec<n::history::RecordDecisionTaskStartedRequest>>>;

// The server task is detached. Abort it if an assertion unwinds before the
// explicit graceful shutdown at the end of the test.
struct AbortOnDrop(tokio::task::JoinHandle<Result<(), tonic::transport::Error>>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone)]
struct History(Captured);
impl tonic::server::NamedService for History {
    const NAME: &'static str = "uber.cadence.history.v1.HistoryAPI";
}
impl tower::Service<tonic::codegen::http::Request<tonic::body::BoxBody>> for History {
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;
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
            type Future = Pin<
                Box<
                    dyn std::future::Future<Output = Result<Response<Self::Response>, Status>>
                        + Send,
                >,
            >;
            fn call(
                &mut self,
                request: Request<n::history::RecordDecisionTaskStartedRequest>,
            ) -> Self::Future {
                let captured = self.0.clone();
                Box::pin(async move {
                    let request = request.into_inner();
                    let schedule_id = request.schedule_id;
                    captured.lock().unwrap().push(request);
                    Ok(Response::new(
                        n::history::RecordDecisionTaskStartedResponse {
                            scheduled_event_id: schedule_id,
                            started_event_id: schedule_id + 1,
                            attempt: 1,
                            ..Default::default()
                        },
                    ))
                })
            }
        }
        let captured = self.0.clone();
        Box::pin(async move {
            if request.uri().path() != PATH {
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
fn gs(bytes: &[u8]) -> GoString {
    GoString::from(bytes)
}
fn data(workflow: &[u8], group: &[u8]) -> TaskDataBytes {
    TaskDataBytes {
        workflow_id: gs(workflow),
        run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
        schedule_id: 42,
        partition_config: Some(BTreeMap::from([(gs(b"isolation-group"), gs(group))])),
        ..Default::default()
    }
}
fn lifecycle(group: &[u8]) -> NativePollLifecycle {
    NativePollLifecycle {
        manager: Arc::default(),
        id: gs(b"poller"),
        cancel: None,
        info: poller_bytes::Info {
            identity: gs(b"worker"),
            rate_per_second: 0.0,
            isolation_group: gs(group),
        },
        forwarded: false,
        observer: None,
    }
}
async fn poll(
    dispatcher: &NativeTaskDispatcher,
    group: &[u8],
    duration: Duration,
) -> n::matching::PollForDecisionTaskResponse {
    tokio::time::timeout(
        LIMIT,
        dispatcher.poll_decision_with_poller(
            n::api::PollForDecisionTaskRequest::default(),
            duration,
            lifecycle(group),
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn native_actors_deliver_raw_bytes_and_ack_only_persisted_tasks() {
    let captured: Captured = Arc::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel::<()>();
    let mut server = AbortOnDrop(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(History(captured.clone()))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    ));
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy(),
        LIMIT,
        1 << 20,
    );
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let domain = Uuid::from_u128(1);
    let key = TaskListKeyBytes {
        domain_id: domain,
        name: gs(b"queue-\xff"),
        task_type: 0,
    };
    let queue = Arc::new(
        DurableTaskQueueBytes::open(
            store.clone(),
            key.clone(),
            0,
            NonZeroU32::new(100).unwrap(),
            1,
        )
        .unwrap(),
    );
    let dispatcher =
        NativeTaskDispatcher::new(queue.clone(), history, GoString::from(domain.to_string()));
    let writer = NativeTaskWriter::new(
        queue.clone(),
        dispatcher.task_notification(),
        WriterConfig::default(),
    );
    let isolation = NativeIsolationConfig {
        groups: vec![gs(b"g\xff"), gs(b"g\xfe")],
        select: Arc::new(|task| {
            (
                task.partition_config.as_ref().unwrap()[b"isolation-group".as_slice()].clone(),
                0,
                None,
            )
        }),
        should_reload: Arc::new(|| false),
    };
    let reader = NativeTaskReader::with_fatal_invariant_sink(
        queue.clone(),
        dispatcher.task_notification(),
        ReaderConfig::default(),
        Arc::new(|| true),
        Arc::new(TaskListLimiter::with_defaults()),
        isolation,
        None,
        None,
        None,
        None,
    );
    let matcher = Arc::new(NativeSyncMatcher::new(
        &[gs(b"g\xff"), gs(b"g\xfe")],
        reader.stop_signal().subscribe(),
    ));
    let dispatcher = dispatcher
        .with_writer(writer.clone())
        .with_reader(reader.clone())
        .with_sync_matcher(matcher.clone());

    // The offer waits for the dispatcher's real poll and finishes only after History starts it.
    let sync_task = AllocatedTaskBytes {
        task_id: 999,
        data: data(b"sync-\xff", b"g\xff"),
    };
    let sync = tokio::spawn({
        let matcher = matcher.clone();
        async move {
            matcher
                .offer(
                    sync_task,
                    &gs(b"g\xff"),
                    &GoString::from(domain.to_string()),
                    false,
                    LIMIT,
                    tokio::time::Instant::now() + LIMIT,
                )
                .await
        }
    });
    let result = poll(&dispatcher, b"g\xff", LIMIT).await;
    assert_eq!(
        result.workflow_execution.unwrap().workflow_id.as_bytes(),
        b"sync-\xff"
    );
    assert!(tokio::time::timeout(LIMIT, sync).await.unwrap().unwrap());
    assert!(
        MatchingTaskStoreBytes::get_tasks(store.as_ref(), &key, 0, i64::MAX, 100)
            .unwrap()
            .is_empty()
    );

    let allocated = tokio::time::timeout(
        LIMIT,
        writer.append(PendingTaskBytes {
            data: data(b"backlog-\xfe", b"g\xfe"),
            source_domain: domain,
            schedule_to_start_seconds: 0,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let rows = MatchingTaskStoreBytes::get_tasks(store.as_ref(), &key, 0, i64::MAX, 100).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.workflow_id.as_bytes(), b"backlog-\xfe");
    assert_eq!(
        rows[0].data.partition_config.as_ref().unwrap()[b"isolation-group".as_slice()].as_bytes(),
        b"g\xfe"
    );
    let wrong = poll(&dispatcher, b"g\xff", Duration::from_millis(150)).await;
    assert!(wrong.workflow_execution.is_none());
    let result = poll(&dispatcher, b"g\xfe", LIMIT).await;
    assert_eq!(
        result.workflow_execution.unwrap().workflow_id.as_bytes(),
        b"backlog-\xfe"
    );
    // The dispatcher completes the task after the History response; wait for its detached worker.
    tokio::time::timeout(LIMIT, async {
        while queue.levels().1 < allocated.task_id {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
    let seen = captured.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].task_id, 0);
    assert_eq!(
        seen[0]
            .workflow_execution
            .as_ref()
            .unwrap()
            .workflow_id
            .as_bytes(),
        b"sync-\xff"
    );
    assert_ne!(
        seen[0]
            .workflow_execution
            .as_ref()
            .unwrap()
            .workflow_id
            .as_bytes(),
        "sync-�".as_bytes()
    );
    assert_eq!(seen[1].task_id, allocated.task_id);
    assert_eq!(
        seen[1]
            .workflow_execution
            .as_ref()
            .unwrap()
            .workflow_id
            .as_bytes(),
        b"backlog-\xfe"
    );
    }
    reader.stop();
    writer.stop();
    tokio::time::timeout(LIMIT, reader.wait_stopped())
        .await
        .unwrap();
    tokio::time::timeout(LIMIT, writer.wait_stopped())
        .await
        .unwrap();
    drop(dispatcher);
    let _ = shutdown.send(());
    tokio::time::timeout(LIMIT, &mut server.0)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
