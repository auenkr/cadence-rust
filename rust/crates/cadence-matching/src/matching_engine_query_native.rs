//! Native Matching QueryWorkflow queue path. Handler admission, ownership and
//! byte-preserving parent transport are supplied by the integration context.
use super::NativeMatchingEngine;
use crate::matching_dispatch_native::NativeFuture;
use crate::matching_query_native::{query_outcome, NativeQueryError, OfferResult, PendingQuery, QueryShape};
use crate::matching_registry_native::NativeQueueHandle;
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use cadence_proto::{api as public_api, native::matching};
use std::sync::Arc;
use tokio::time::Instant;
use tonic::Status;

/// A parent RPC must preserve native request fields and typed raw query errors.
pub trait NativeQueryParent: Send + Sync {
    fn query_workflow(
        &self,
        request: matching::QueryWorkflowRequest,
        deadline: Instant,
    ) -> NativeFuture<Result<matching::QueryWorkflowResponse, NativeQueryError>>;
}

/// The handler has already checked startup, quota, domain and entry ownership.
/// Parent resolution must distinguish a local owner from a remote owner and
/// return routing failures instead of silently treating them as local.
pub trait NativeQueryContext: Send + Sync {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status>;
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status>;
    fn forward_degree(&self, domain: uuid::Uuid, root: &GoString) -> i64;
    fn remote_parent(&self, name: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeQueryParent>>, Status>>;
    /// Handler's deadline policy when the caller did not supply a deadline.
    fn default_deadline(&self) -> Instant;
}

fn status(error: Status) -> NativeQueryError {
    NativeQueryError::Status(error)
}

impl NativeMatchingEngine {
    /// Dispatch and wait for a query. Dropping this future withdraws the
    /// registration, including its task ID in the shared completion index.
    pub async fn query_workflow(
        &self,
        request: matching::QueryWorkflowRequest,
        request_deadline: Option<Instant>,
        context: &dyn NativeQueryContext,
    ) -> Result<matching::QueryWorkflowResponse, NativeQueryError> {
        let deadline = request_deadline.unwrap_or_else(|| context.default_deadline());
        tokio::time::timeout_at(deadline, self.query_workflow_inner(request, deadline, context))
            .await
            .unwrap_or_else(|_| Err(status(Status::internal("context deadline exceeded"))))
    }

    async fn query_workflow_inner(
        &self,
        request: matching::QueryWorkflowRequest,
        deadline: Instant,
        context: &dyn NativeQueryContext,
    ) -> Result<matching::QueryWorkflowResponse, NativeQueryError> {
        let mut shape = QueryShape {
            task_list_absent: request.task_list.is_none(),
            query_absent: false,
        };
        let mut payload = request.request.clone().unwrap_or_default();
        shape.query_absent = payload.query.is_none();
        payload.domain = context.domain_name(&request.domain_id).map_err(status)?;
        let strong = payload.query_consistency_level == public_api::QueryConsistencyLevel::Strong as i32;
        let mut list = request.task_list.clone().unwrap_or_default();
        let parsed = crate::matching_handler_native::task_list_name(&list.name).map_err(status)?;
        let domain = request.domain_id.as_utf8().ok()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .ok_or_else(|| status(Status::invalid_argument("DomainUUID is not valid.")))?;
        let kind = crate::matching_handler_native::persistence_kind(list.kind);
        let key = TaskListKeyBytes { domain_id: domain, name: list.name.clone(), task_type: 0 };
        let now = crate::grpc_matching::matching_now();
        let mut handle = self.registry.get(key.clone(), kind, now).await.map_err(status)?;
        context.check_queue(&key, &handle).map_err(status)?;
        if list.kind == 2 && !handle.has_poller_after(now.saturating_sub(10_000_000_000)) {
            return Err(status(crate::grpc::detailed_status(
                tonic::Code::Unavailable,
                "sticky worker is unavailable, please use non-sticky task list.",
                "StickyWorkerUnavailableError",
                public_api::StickyWorkerUnavailableError {},
            )));
        }
        let partitions = handle.partitions.clone();
        let degree = context.forward_degree(domain, parsed.root()).max(1);
        let task_id = GoString::from(uuid::Uuid::new_v4().to_string());
        let mut permits = Vec::new();
        let mut force_local = false;
        loop {
            let query = PendingQuery {
                task_id: task_id.clone(),
                domain_id: request.domain_id.clone(),
                task_list: list.name.clone(),
                request: payload.clone(),
                shape,
            };
            let (receiver, registration) = handle.queries.register_guarded(&task_id);
            let current = crate::matching_handler_native::task_list_name(&list.name).map_err(status)?;
            let can_forward = !force_local && list.kind != 2 && !current.is_root();
            let forward = async {
                if can_forward {
                    // The queue must expose RequestTokens::acquire, so a released
                    // token wakes this race without polling.
                    handle.wait_forward_query().await
                } else {
                    std::future::pending().await
                }
            };
            let permit = match handle.queries.offer_or_forward(query, forward).await {
                OfferResult::Matched => {
                    let result = receiver.await.map_err(|_| status(Status::unavailable("Matching query channel closed.")))?;
                    let mut response = query_outcome(result, strong)?;
                    response.partition_config = response_config_from_cache(&partitions);
                    return Ok(response);
                }
                OfferResult::Forward(Some(permit)) => permit,
                OfferResult::Forward(None) => return Err(status(Status::unavailable("Matching forward token channel closed."))),
            };
            drop(registration);
            let parent = current.parent(degree);
            if parent.is_empty() {
                return Err(status(Status::internal("cannot find parent task list for forwarding")));
            }
            permits.push(permit);
            let previous = list.name.clone();
            let previous_list = list.clone();
            let previous_shape = shape;
            list = cadence_proto::native::api::TaskList { name: parent.clone(), kind: list.kind, ..Default::default() };
            shape.task_list_absent = false;
            if let Some(remote) = context.remote_parent(parent.clone()).await.map_err(status)? {
                let forwarded = matching::QueryWorkflowRequest {
                    domain_id: request.domain_id.clone(),
                    task_list: Some(list.clone()),
                    request: Some(payload.clone()),
                    forwarded_from: previous.clone(),
                };
                match remote.query_workflow(forwarded, deadline).await {
                    Err(NativeQueryError::Status(error)) if crate::matching_query::query_forward_slow_down(&error) => {
                        permits.pop();
                        list = previous_list;
                        shape = previous_shape;
                        force_local = true;
                    }
                    Ok(mut response) => {
                        response.partition_config = response_config_from_cache(&partitions);
                        return Ok(response);
                    }
                    Err(error) => return Err(error),
                }
                continue;
            }
            let key = TaskListKeyBytes { domain_id: domain, name: parent, task_type: 0 };
            handle = self.registry.get(key.clone(), kind, crate::grpc_matching::matching_now()).await.map_err(status)?;
            context.check_queue(&key, &handle).map_err(status)?;
        }
    }
}

fn response_config_from_cache(cache: &crate::matching_partition_native::NativePartitionCache) -> Option<matching::TaskListPartitionConfig> {
    cache.get().map(|config| {
        let read_count = config.read_partitions.len() as i32;
        let write_count = config.write_partitions.len() as i32;
        let convert = |map: std::collections::HashMap<i32, Vec<GoString>>| {
            map.into_iter().map(|(id, isolation_groups)| (id, matching::TaskListPartition { isolation_groups })).collect()
        };
        matching::TaskListPartitionConfig {
            version: config.version,
            num_read_partitions: read_count,
            num_write_partitions: write_count,
            read_partitions: convert(config.read_partitions),
            write_partitions: convert(config.write_partitions),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_query_native::{QueryOutcome, WorkerAnswer};
    use crate::matching_registry_native::NativeQueueRegistry;
    use cadence_proto::native::api;
    use std::time::Duration;

    struct Context;
    impl NativeQueryContext for Context {
        fn domain_name(&self, _: &GoString) -> Result<GoString, Status> { Ok(vec![0xfe, 0].into()) }
        fn check_queue(&self, _: &TaskListKeyBytes, _: &NativeQueueHandle) -> Result<(), Status> { Ok(()) }
        fn forward_degree(&self, _: uuid::Uuid, _: &GoString) -> i64 { 20 }
        fn remote_parent(&self, _: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeQueryParent>>, Status>> {
            Box::pin(async { Ok(None) })
        }
        fn default_deadline(&self) -> Instant { Instant::now() + Duration::from_secs(10) }
    }

    fn fixture() -> (tempfile::TempDir, NativeMatchingEngine) {
        use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let history = crate::matching_history::MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1), 32 * 1024 * 1024,
        );
        let registry = NativeQueueRegistry::new(store, history, std::num::NonZeroU32::new(100).unwrap());
        (dir, NativeMatchingEngine::new(registry))
    }

    fn request(domain: uuid::Uuid, name: GoString) -> matching::QueryWorkflowRequest {
        matching::QueryWorkflowRequest {
            domain_id: domain.to_string().into(),
            task_list: Some(api::TaskList { name, ..Default::default() }),
            request: Some(api::QueryWorkflowRequest {
                workflow_execution: Some(api::WorkflowExecution {
                    workflow_id: vec![0xff, 0].into(),
                    run_id: vec![0xfe, 0].into(),
                }),
                query: Some(api::WorkflowQuery { query_type: vec![0x80, 0].into(), ..Default::default() }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn raw_query_fields_and_worker_result_use_real_index() {
        let (_dir, engine) = fixture();
        let domain = uuid::Uuid::from_u128(1);
        let name = GoString::from(&b"queue-\xff"[..]);
        let key = TaskListKeyBytes { domain_id: domain, name: name.clone(), task_type: 0 };
        let queue = engine.registry.get(key, 0, crate::grpc_matching::matching_now()).await.unwrap();
        let caller = engine.clone();
        let task = tokio::spawn(async move { caller.query_workflow(request(domain, name), None, &Context).await });
        let query = tokio::time::timeout(Duration::from_secs(2), queue.queries.next()).await.unwrap();
        assert!(uuid::Uuid::parse_str(query.task_id.as_utf8().unwrap()).is_ok());
        assert_eq!(query.task_list.as_bytes(), b"queue-\xff");
        assert_eq!(query.request.domain.as_bytes(), &[0xfe, 0]);
        assert_eq!(query.request.workflow_execution.unwrap().workflow_id.as_bytes(), &[0xff, 0]);
        assert_eq!(query.request.query.unwrap().query_type.as_bytes(), &[0x80, 0]);
        engine.registry.deliver_query_result(&query.task_id, QueryOutcome::Worker(WorkerAnswer::new(api::WorkflowQueryResult {
            result_type: public_api::QueryResultType::Answered as i32,
            answer: Some(api::Payload { data: vec![0xff, 0] }),
            ..Default::default()
        }))).unwrap();
        assert_eq!(task.await.unwrap().unwrap().query_result.unwrap().data, vec![0xff, 0]);
        assert!(engine.registry.deliver_query_result(&query.task_id, QueryOutcome::Internal(Status::internal("late"))).is_err());
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_query_withdraws_index_entry_after_delivery() {
        let (_dir, engine) = fixture();
        let domain = uuid::Uuid::from_u128(1);
        let name = GoString::from(&b"cancel-\xff"[..]);
        let key = TaskListKeyBytes { domain_id: domain, name: name.clone(), task_type: 0 };
        let queue = engine.registry.get(key, 0, crate::grpc_matching::matching_now()).await.unwrap();
        let caller = engine.clone();
        let task = tokio::spawn(async move { caller.query_workflow(request(domain, name), None, &Context).await });
        let query = tokio::time::timeout(Duration::from_secs(2), queue.queries.next()).await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(engine.registry.deliver_query_result(&query.task_id, QueryOutcome::Internal(Status::internal("late"))).is_err());
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }
}
