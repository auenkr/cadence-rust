//! Native Matching engine foundations and task add flows.
//!
//! This module is deliberately not wired into the service: `get` by itself is
//! not the handler ownership check, and bypassing the forwarding chain can lose
//! tasks. See memory/matching-native-engine-20261028.md.
use std::collections::BTreeMap;

use cadence_common::go_string::GoString;
use cadence_persistence::matching_blob::{TaskDataBytes, GO_ZERO_TIME_NANOS};
use cadence_proto::native::{api, matching};
use cadence_persistence::matching_native_task_store::{AllocatedTaskBytes, TaskListKeyBytes};
use std::{sync::Arc, time::Duration};
use tonic::Status;
use crate::matching_dispatch_native::{NativeFuture, NativeMatchingApi};
use crate::matching_registry_native::{NativeQueueHandle, SyncMatchOutcome};

use crate::matching_registry_native::NativeQueueRegistry;

#[path = "matching_engine_poll_native.rs"]
mod poll;
pub use poll::{NativePollContext, NativePollRegistration};
#[path = "matching_engine_query_native.rs"]
mod query;
pub use query::{NativeQueryContext, NativeQueryParent};

/// Shared state for the native engine. Domain, admission, ownership and
/// configuration boundaries still need to be supplied before RPCs can run.
#[derive(Clone)]
pub struct NativeMatchingEngine {
    pub registry: NativeQueueRegistry,
}

impl NativeMatchingEngine {
    pub fn new(registry: NativeQueueRegistry) -> Self {
        Self { registry }
    }
}

/// Production dependencies outside the queue algorithm. The caller has already
/// performed handler startup, quota, domain lookup and entry ownership checks.
/// Parent routing returns None for a local owner; routing errors are returned.
pub trait DecisionAddContext: Send + Sync {
    fn domain_active(&self, domain: uuid::Uuid) -> Result<bool, Status>;
    fn forward_degree(&self, domain: uuid::Uuid, root: &GoString, task_type: i32) -> i64;
    fn check_write_partition(&self, key: &TaskListKeyBytes, handle: &NativeQueueHandle) -> Result<(), Status>;
    fn remote_parent(&self, name: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>>;
}

fn response_config(handle: &NativeQueueHandle) -> Option<matching::TaskListPartitionConfig> {
    handle.partitions.get().map(|config| {
        let read_count = config.read_partitions.len() as i32;
        let write_count = config.write_partitions.len() as i32;
        let convert = |map: std::collections::HashMap<i32, Vec<GoString>>| {
            map.into_iter().map(|(id, isolation_groups)| {
                (id, matching::TaskListPartition { isolation_groups })
            }).collect()
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

// The two RPCs share the queue algorithm but retain their distinct wire fields.
enum AddRequest {
    Decision(Box<matching::AddDecisionTaskRequest>),
    Activity(Box<matching::AddActivityTaskRequest>),
}

impl NativeMatchingEngine {
    /// Executes the queue portion after handler admission and entry ownership.
    pub async fn add_decision_task(
        &self,
        request: matching::AddDecisionTaskRequest,
        request_deadline: Option<tokio::time::Instant>,
        context: &dyn DecisionAddContext,
    ) -> Result<matching::AddDecisionTaskResponse, Status> {
        let partition_config = self.add_task(AddRequest::Decision(Box::new(request)), request_deadline, context).await?;
        Ok(matching::AddDecisionTaskResponse { partition_config })
    }

    /// Executes the activity queue portion; admission and domain lookup are external.
    pub async fn add_activity_task(
        &self,
        request: matching::AddActivityTaskRequest,
        request_deadline: Option<tokio::time::Instant>,
        context: &dyn DecisionAddContext,
    ) -> Result<matching::AddActivityTaskResponse, Status> {
        let partition_config = self.add_task(AddRequest::Activity(Box::new(request)), request_deadline, context).await?;
        Ok(matching::AddActivityTaskResponse { partition_config })
    }

    async fn add_task(
        &self,
        request: AddRequest,
        request_deadline: Option<tokio::time::Instant>,
        context: &dyn DecisionAddContext,
    ) -> Result<Option<matching::TaskListPartitionConfig>, Status> {
        let (domain_id, source_id, list, execution, schedule_id, timeout, task_source, forwarded_from, config, task_type) = match &request {
            AddRequest::Decision(r) => (&r.domain_id, &r.domain_id, r.task_list.clone(), r.workflow_execution.as_ref(), r.schedule_id, r.schedule_to_start_timeout, r.source, &r.forwarded_from, &r.partition_config, 0),
            AddRequest::Activity(r) => (&r.domain_id, &r.source_domain_id, r.task_list.clone(), r.workflow_execution.as_ref(), r.schedule_id, r.schedule_to_start_timeout, r.source, &r.forwarded_from, &r.partition_config, 1),
        };
        let list = list.unwrap_or_default();
        let parsed = crate::matching_handler_native::task_list_name(&list.name)?;
        let domain = domain_id.as_utf8().ok()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .ok_or_else(|| Status::invalid_argument("DomainUUID is not valid."))?;
        let source_domain = source_id.as_utf8().ok()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .ok_or_else(|| Status::invalid_argument("Source domain UUID is not valid."))?;
        let key = TaskListKeyBytes { domain_id: domain, name: list.name.clone(), task_type };
        let kind = crate::matching_handler_native::persistence_kind(list.kind);
        let now = crate::grpc_matching::matching_now();
        let entry = self.registry.get(key.clone(), kind, now).await?;
        if entry.stop_if_isolation_changed() {
            return Err(Status::internal("task list shutting down"));
        }
        context.check_write_partition(&key, &entry)?;
        if task_type == 0 && list.kind == 2 && !entry.has_poller_after(now.saturating_sub(10_000_000_000)) {
            return Err(crate::grpc::detailed_status(
                tonic::Code::Unavailable,
                "sticky worker is unavailable, please use non-sticky task list.",
                "StickyWorkerUnavailableError",
                cadence_proto::api::StickyWorkerUnavailableError {},
            ));
        }
        let data = add_task_data(execution, schedule_id, config.clone().into_iter().collect(), now);
        let forwarded = !forwarded_from.is_empty();
        if !forwarded {
            entry.mark_alive();
            if let Some(group) = data.partition_config.as_ref().and_then(|c| c.get(&GoString::from("isolation-group"))) {
                entry.qps.report_group(group.as_bytes(), 1);
            } else {
                entry.qps.report_counter(1);
            }
        }
        let now_instant = tokio::time::Instant::now();
        let deadline = if forwarded {
            request_deadline.unwrap_or(now_instant + Duration::from_millis(200))
        } else {
            now_instant + crate::matching_sync::sync_match_budget(
                request_deadline.map(|d| d.saturating_duration_since(now_instant)))
        };
        let source = GoString::from(source_domain.to_string());
        let task = AllocatedTaskBytes { task_id: 0, data: data.clone() };
        if context.domain_active(source_domain)? {
            let degree = context.forward_degree(domain, parsed.root(), task_type).max(1);
            let mut current = list.clone();
            let mut handle = entry.clone();
            let mut ancestor = false;
            let mut permits = Vec::new();
            loop {
                let outcome = if ancestor || forwarded {
                    if handle.try_sync_match_forwarded(&task, &source, deadline).await {
                        SyncMatchOutcome::Matched
                    } else {
                        SyncMatchOutcome::Unmatched
                    }
                } else {
                    handle.try_sync_match_observed(&task, &source, deadline).await
                };
                if outcome == SyncMatchOutcome::Matched {
                    return Ok(response_config(&entry));
                }
                let name = crate::matching_handler_native::task_list_name(&current.name)?;
                if task_source == 2 && (ancestor || forwarded) && name.is_root() {
                    if handle.offer_forwarded_backlog(&task, &source, deadline).await {
                        return Ok(response_config(&entry));
                    }
                    break;
                }
                if name.is_root() || current.kind == 2 || tokio::time::Instant::now() >= deadline { break; }
                let parent = name.parent(degree);
                if parent.is_empty() { break; }
                let Some(permit) = handle.try_forward_task() else { break; };
                permits.push(permit);
                let previous = current.name.clone();
                current.name = parent.clone();
                if let Some(remote) = context.remote_parent(parent.clone()).await? {
                    let result = match &request {
                        AddRequest::Decision(r) => {
                            let mut next = r.as_ref().clone();
                            next.domain_id = source.clone();
                            next.task_list = Some(current.clone());
                            next.forwarded_from = previous;
                            remote.add_decision_task(next, deadline).await
                        }
                        AddRequest::Activity(r) => {
                            let mut next = r.as_ref().clone();
                            next.source_domain_id = source.clone();
                            next.task_list = Some(current.clone());
                            next.forwarded_from = previous;
                            // The legacy forwarder constructs a fresh request without dispatch info.
                            next.activity_task_dispatch_info = None;
                            remote.add_activity_task(next, deadline).await
                        }
                    };
                    if result.is_ok() { return Ok(response_config(&entry)); }
                    break;
                }
                let parent_key = TaskListKeyBytes { domain_id: domain, name: parent, task_type };
                handle = match self.registry.get(parent_key.clone(), kind, crate::grpc_matching::matching_now()).await {
                    Ok(handle) => handle,
                    Err(_) => break,
                };
                if handle.stop_if_isolation_changed() || context.check_write_partition(&parent_key, &handle).is_err() { break; }
                ancestor = true;
            }
        }
        if forwarded { return Err(crate::matching_dispatch::remote_sync_match_failed()); }
        entry.enqueue(data, source_domain, timeout.map(|d| d.seconds as i32).unwrap_or_default()).await?;
        Ok(response_config(&entry))
    }
}

/// Construct the common AddDecisionTask/AddActivityTask payload exactly as in
/// `grpc_matching.rs`: expiry is assigned by the writer, and an empty map is
/// represented as absent. Keep the request run ID as bytes until persistence.
fn add_task_data(
    execution: Option<&api::WorkflowExecution>,
    schedule_id: i64,
    partition_config: BTreeMap<GoString, GoString>,
    now: i64,
) -> TaskDataBytes {
    let execution = execution.cloned().unwrap_or_default();
    TaskDataBytes {
        workflow_id: execution.workflow_id,
        schedule_id,
        created_nanos: now,
        expiry_nanos: GO_ZERO_TIME_NANOS,
        partition_config: if partition_config.is_empty() {
            None
        } else {
            Some(partition_config)
        },
        ..Default::default()
    }
    .with_request_run_id(execution.run_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestContext;
    impl DecisionAddContext for TestContext {
        fn domain_active(&self, _: uuid::Uuid) -> Result<bool, Status> { Ok(true) }
        fn forward_degree(&self, _: uuid::Uuid, _: &GoString, _: i32) -> i64 { 20 }
        fn check_write_partition(&self, _: &TaskListKeyBytes, _: &NativeQueueHandle) -> Result<(), Status> { Ok(()) }
        fn remote_parent(&self, _: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>> {
            Box::pin(async { Ok(None) })
        }
    }

    fn fixture() -> (tempfile::TempDir, NativeMatchingEngine, Arc<cadence_persistence::matching_sqlite::SqliteTaskStore>) {
        use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let history = crate::matching_history::MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1), 32 * 1024 * 1024,
        );
        let registry = NativeQueueRegistry::new(store.clone(), history, std::num::NonZeroU32::new(100).unwrap());
        (dir, NativeMatchingEngine::new(registry), store)
    }

    #[tokio::test]
    async fn forwarded_failure_never_persists_raw_identifiers() {
        let (_dir, engine, _store) = fixture();
        let domain = uuid::Uuid::from_u128(1);
        let request = matching::AddDecisionTaskRequest {
            domain_id: domain.to_string().into(),
            task_list: Some(api::TaskList { name: vec![0xff, 0xfe].into(), ..Default::default() }),
            workflow_execution: Some(api::WorkflowExecution { workflow_id: vec![0xff].into(), run_id: vec![0xfe].into() }),
            forwarded_from: vec![0x80].into(),
            ..Default::default()
        };
        let error = engine.add_decision_task(request, Some(tokio::time::Instant::now()), &TestContext).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        let queue = engine.registry.get(TaskListKeyBytes { domain_id: domain, name: vec![0xff, 0xfe].into(), task_type: 0 }, 0, crate::grpc_matching::matching_now()).await.unwrap();
        assert_eq!(queue.queue.task_list_size().unwrap(), 0);
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }

    #[tokio::test]
    async fn invalid_raw_domain_is_rejected_without_loading_queue() {
        let (_dir, engine, _store) = fixture();
        let request = matching::AddDecisionTaskRequest { domain_id: vec![0xff, 0].into(), ..Default::default() };
        let error = engine.add_decision_task(request, None, &TestContext).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(engine.registry.loaded_owner_count(), 0);
    }

    struct InactiveSource(uuid::Uuid);
    impl DecisionAddContext for InactiveSource {
        fn domain_active(&self, domain: uuid::Uuid) -> Result<bool, Status> {
            assert_eq!(domain, self.0);
            Ok(false)
        }
        fn forward_degree(&self, _: uuid::Uuid, _: &GoString, _: i32) -> i64 { 20 }
        fn check_write_partition(&self, _: &TaskListKeyBytes, _: &NativeQueueHandle) -> Result<(), Status> { Ok(()) }
        fn remote_parent(&self, _: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>> {
            Box::pin(async { panic!("inactive source must not forward") })
        }
    }

    #[tokio::test]
    async fn activity_persists_in_source_domain_with_target_queue() {
        use cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes;
        let (_dir, engine, store) = fixture();
        let target = uuid::Uuid::from_u128(1);
        let source = uuid::Uuid::from_u128(2);
        let run = uuid::Uuid::from_u128(3);
        let name = GoString::from(&b"activity-\xff"[..]);
        let request = matching::AddActivityTaskRequest {
            domain_id: target.to_string().into(),
            source_domain_id: source.to_string().into(),
            task_list: Some(api::TaskList { name: name.clone(), kind: 2, ..Default::default() }),
            workflow_execution: Some(api::WorkflowExecution { workflow_id: vec![0xff, 0].into(), run_id: run.to_string().into() }),
            schedule_id: 42,
            partition_config: [(GoString::from(&b"\xff"[..]), GoString::from(&b"\xfe"[..]))].into_iter().collect(),
            ..Default::default()
        };
        engine.add_activity_task(request, None, &InactiveSource(source)).await.unwrap();
        let target_key = TaskListKeyBytes { domain_id: target, name, task_type: 1 };
        let source_key = TaskListKeyBytes { domain_id: source, ..target_key.clone() };
        assert!(MatchingTaskStoreBytes::get_tasks(store.as_ref(), &target_key, 0, i64::MAX, 10).unwrap().is_empty());
        let rows = MatchingTaskStoreBytes::get_tasks(store.as_ref(), &source_key, 0, i64::MAX, 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data.workflow_id.as_bytes(), &[0xff, 0]);
        assert_eq!(rows[0].data.run_id_bytes(), run.to_string().as_bytes());
        assert_eq!(rows[0].data.schedule_id, 42);
        assert_eq!(rows[0].data.partition_config.as_ref().unwrap().get(&GoString::from(&b"\xff"[..])).unwrap().as_bytes(), &[0xfe]);
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }

    #[tokio::test]
    async fn forwarded_activity_with_raw_fields_never_persists() {
        use cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes;
        let (_dir, engine, store) = fixture();
        let target = uuid::Uuid::from_u128(1);
        let source = uuid::Uuid::from_u128(2);
        let name = GoString::from(&b"\xff\xfe"[..]);
        let request = matching::AddActivityTaskRequest {
            domain_id: target.to_string().into(), source_domain_id: source.to_string().into(),
            task_list: Some(api::TaskList { name: name.clone(), ..Default::default() }),
            workflow_execution: Some(api::WorkflowExecution { workflow_id: vec![0xff, 0].into(), run_id: vec![0xfe, 0].into() }),
            forwarded_from: vec![0x80].into(),
            partition_config: [(GoString::from(&b"\xff"[..]), GoString::from(&b"\xfe"[..]))].into_iter().collect(),
            ..Default::default()
        };
        let error = engine.add_activity_task(request, None, &InactiveSource(source)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        for domain_id in [target, source] {
            let key = TaskListKeyBytes { domain_id, name: name.clone(), task_type: 1 };
            assert!(MatchingTaskStoreBytes::get_tasks(store.as_ref(), &key, 0, i64::MAX, 10).unwrap().is_empty());
        }
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }

    struct CaptureRemote(Arc<std::sync::Mutex<Option<matching::AddActivityTaskRequest>>>);
    impl NativeMatchingApi for CaptureRemote {
        fn poll_for_decision_task(&self, _: matching::PollForDecisionTaskRequest, _: tokio::time::Instant) -> NativeFuture<Result<matching::PollForDecisionTaskResponse, Status>> {
            Box::pin(async { unreachable!() })
        }
        fn poll_for_activity_task(&self, _: matching::PollForActivityTaskRequest, _: tokio::time::Instant) -> NativeFuture<Result<matching::PollForActivityTaskResponse, Status>> {
            Box::pin(async { unreachable!() })
        }
        fn add_decision_task(&self, _: matching::AddDecisionTaskRequest, _: tokio::time::Instant) -> NativeFuture<Result<(), Status>> {
            Box::pin(async { unreachable!() })
        }
        fn add_activity_task(&self, request: matching::AddActivityTaskRequest, _: tokio::time::Instant) -> NativeFuture<Result<(), Status>> {
            *self.0.lock().unwrap() = Some(request);
            Box::pin(async { Ok(()) })
        }
    }
    struct CaptureContext(Arc<std::sync::Mutex<Option<matching::AddActivityTaskRequest>>>);
    impl DecisionAddContext for CaptureContext {
        fn domain_active(&self, _: uuid::Uuid) -> Result<bool, Status> { Ok(true) }
        fn forward_degree(&self, _: uuid::Uuid, _: &GoString, _: i32) -> i64 { 20 }
        fn check_write_partition(&self, _: &TaskListKeyBytes, _: &NativeQueueHandle) -> Result<(), Status> { Ok(()) }
        fn remote_parent(&self, _: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>> {
            let remote: Arc<dyn NativeMatchingApi> = Arc::new(CaptureRemote(self.0.clone()));
            Box::pin(async move { Ok(Some(remote)) })
        }
    }

    #[tokio::test]
    async fn activity_forwarding_preserves_raw_fields_and_domains() {
        use cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes;
        let (_dir, engine, store) = fixture();
        let target = uuid::Uuid::from_u128(1);
        let source = uuid::Uuid::from_u128(2);
        let name = GoString::from(&b"/__cadence_sys/\xff/1"[..]);
        let capture = Arc::new(std::sync::Mutex::new(None));
        let request = matching::AddActivityTaskRequest {
            domain_id: target.to_string().into(), source_domain_id: source.to_string().into(),
            task_list: Some(api::TaskList { name: name.clone(), ..Default::default() }),
            workflow_execution: Some(api::WorkflowExecution { workflow_id: vec![0xff].into(), run_id: vec![0xfe].into() }),
            forwarded_from: vec![0x80].into(),
            partition_config: [(GoString::from(&b"\xff"[..]), GoString::from(&b"\xfe"[..]))].into_iter().collect(),
            ..Default::default()
        };
        engine.add_activity_task(request, Some(tokio::time::Instant::now() + Duration::from_secs(2)), &CaptureContext(capture.clone())).await.unwrap();
        let sent = capture.lock().unwrap().take().unwrap();
        assert_eq!(sent.domain_id.as_bytes(), target.to_string().as_bytes());
        assert_eq!(sent.source_domain_id.as_bytes(), source.to_string().as_bytes());
        assert_eq!(sent.forwarded_from, name);
        assert_eq!(sent.task_list.unwrap().name.as_bytes(), &[0xff]);
        let execution = sent.workflow_execution.unwrap();
        assert_eq!(execution.workflow_id.as_bytes(), &[0xff]);
        assert_eq!(execution.run_id.as_bytes(), &[0xfe]);
        assert_eq!(sent.partition_config.get(&GoString::from(&b"\xff"[..])).unwrap().as_bytes(), &[0xfe]);
        let key = TaskListKeyBytes { domain_id: target, name, task_type: 1 };
        assert!(MatchingTaskStoreBytes::get_tasks(store.as_ref(), &key, 0, i64::MAX, 10).unwrap().is_empty());
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }

    struct RoutingFailure;
    impl DecisionAddContext for RoutingFailure {
        fn domain_active(&self, _: uuid::Uuid) -> Result<bool, Status> { Ok(true) }
        fn forward_degree(&self, _: uuid::Uuid, _: &GoString, _: i32) -> i64 { 20 }
        fn check_write_partition(&self, _: &TaskListKeyBytes, _: &NativeQueueHandle) -> Result<(), Status> { Ok(()) }
        fn remote_parent(&self, _: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>> {
            Box::pin(async { Err(Status::internal("routing failed")) })
        }
    }

    #[tokio::test]
    async fn decision_parent_routing_error_is_returned_without_persisting() {
        use cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes;
        let (_dir, engine, store) = fixture();
        let domain = uuid::Uuid::from_u128(1);
        let name = GoString::from("/__cadence_sys/list/1");
        let request = matching::AddDecisionTaskRequest {
            domain_id: domain.to_string().into(),
            task_list: Some(api::TaskList { name: name.clone(), ..Default::default() }),
            ..Default::default()
        };
        let error = engine.add_decision_task(request, None, &RoutingFailure).await.unwrap_err();
        assert_eq!(error.message(), "routing failed");
        let key = TaskListKeyBytes { domain_id: domain, name, task_type: 0 };
        assert!(MatchingTaskStoreBytes::get_tasks(store.as_ref(), &key, 0, i64::MAX, 10).unwrap().is_empty());
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }

    #[test]
    fn add_payload_preserves_arbitrary_string_bytes() {
        let execution = api::WorkflowExecution {
            workflow_id: vec![0xff, 0, 0x80].into(),
            run_id: vec![0xfe, 0, 0xff].into(),
        };
        let mut config = BTreeMap::new();
        config.insert(vec![0xff].into(), vec![0xfe, 0].into());
        config.insert(vec![0xfe].into(), vec![0xff, 0].into());
        let data = add_task_data(Some(&execution), 42, config.clone(), 123);
        assert_eq!(data.workflow_id.as_bytes(), &[0xff, 0, 0x80]);
        assert_eq!(data.run_id_bytes(), vec![0xfe, 0, 0xff]);
        assert_eq!(data.partition_config, Some(config));
        assert_eq!((data.schedule_id, data.created_nanos, data.expiry_nanos), (42, 123, GO_ZERO_TIME_NANOS));
    }

    #[test]
    fn absent_execution_and_empty_config_follow_go_getters() {
        let data = add_task_data(None, 1, BTreeMap::new(), 2);
        assert!(data.workflow_id.is_empty());
        assert!(data.run_id_bytes().is_empty());
        assert_eq!(data.partition_config, None);
    }
}
