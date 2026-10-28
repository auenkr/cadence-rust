//! Byte-preserving control operations for the native Matching engine.
//! Handler startup, quota and metrics precede these calls. The query mapper
//! validation must precede even handler admission.
use cadence_common::{go_string::GoString, poller_bytes::HistoricalInfo};
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use cadence_proto::native::{api, matching};
use tonic::Status;
use super::NativeMatchingEngine;
use crate::matching_handler_native as helper;
use crate::matching_partition_native::NativeStoredPartitionConfig;
use crate::matching_registry_native::NativeQueueHandle;

/// Domain cache, ownership and live dynamic configuration. Host lookup errors are intentionally
/// ignored by ListTaskListPartitions, as in Go.
pub trait NativeControlContext: Send + Sync {
    fn domain_id(&self, name: &GoString) -> Result<uuid::Uuid, Status>;
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status>;
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status>;
    fn return_all_kinds(&self) -> bool;
    fn scaler_enabled(&self, domain: &GoString, list: &GoString, task_type: i32) -> bool;
    fn isolation_groups(&self) -> Vec<GoString>;
    fn poller_window_nanos(&self, domain: &GoString, list: &GoString, task_type: i32) -> i64;
    fn write_partition_count(&self, domain: &GoString, root: &GoString, task_type: i32) -> i64;
    fn owner_host(&self, partition: GoString) -> crate::matching_dispatch_native::NativeFuture<Result<GoString, Status>>;
}

fn task_type(value: i32) -> i32 { if value == 2 { 1 } else { 0 } }
fn domain_uuid(id: &GoString) -> Result<uuid::Uuid, Status> {
    id.as_utf8().ok().and_then(|s| uuid::Uuid::parse_str(s).ok())
        .ok_or_else(|| Status::invalid_argument("DomainUUID is not valid."))
}
fn pollers(items: Vec<HistoricalInfo>) -> Vec<api::PollerInfo> {
    items.into_iter().map(|p| api::PollerInfo {
        identity: p.identity, rate_per_second: p.rate_per_second,
        last_access_time: Some(prost_types::Timestamp {
            seconds: p.last_access_time.div_euclid(1_000_000_000),
            nanos: p.last_access_time.rem_euclid(1_000_000_000) as i32,
        }),
    }).collect()
}
#[allow(deprecated)]
fn config_to_proto(config: NativeStoredPartitionConfig) -> api::TaskListPartitionConfig {
    let convert = |map: std::collections::HashMap<i32, Vec<GoString>>| {
        map.into_iter().map(|(id, isolation_groups)| (id, api::TaskListPartition { isolation_groups })).collect()
    };
    api::TaskListPartitionConfig {
        version: config.version,
        num_read_partitions: config.read_partitions.len() as i32,
        num_write_partitions: config.write_partitions.len() as i32,
        read_partitions: convert(config.read_partitions), write_partitions: convert(config.write_partitions),
    }
}
fn description(name: GoString, kind: i16, history: Vec<HistoricalInfo>, config: Option<NativeStoredPartitionConfig>) -> matching::DescribeTaskListResponse {
    matching::DescribeTaskListResponse {
        pollers: pollers(history), task_list_status: None,
        task_list: Some(api::TaskList { name, kind: i32::from(kind) + 1, ..Default::default() }),
        partition_config: config.map(config_to_proto),
    }
}
impl NativeMatchingEngine {
    async fn control_queue(&self, id: &GoString, list: &api::TaskList, ty: i32, now: i64, context: &dyn NativeControlContext) -> Result<(TaskListKeyBytes, NativeQueueHandle), Status> {
        let key = TaskListKeyBytes { domain_id: domain_uuid(id)?, name: list.name.clone(), task_type: ty };
        let queue = self.registry.get(key.clone(), helper::persistence_kind(list.kind), now).await?;
        context.check_queue(&key, &queue)?;
        Ok((key, queue))
    }
    pub async fn describe_task_list(&self, request: matching::DescribeTaskListRequest, context: &dyn NativeControlContext) -> Result<matching::DescribeTaskListResponse, Status> {
        let payload = request.request.unwrap_or_default();
        let list = payload.task_list.unwrap_or_default();
        helper::task_list_name(&list.name)?;
        let domain = context.domain_name(&request.domain_id)?;
        let ty = task_type(payload.task_list_type);
        let now = crate::grpc_matching::matching_now();
        let (_, queue) = self.control_queue(&request.domain_id, &list, ty, now, context).await?;
        let history = queue.pollers.lock().expect("poller history lock").list(now);
        let mut response = description(list.name.clone(), queue.queue.task_list_kind(), history, queue.partitions.get());
        if payload.include_task_list_status {
            let (read_level, ack_level, backlog_count_hint, start_id, end_id, empty) = queue.queue.status();
            let window = context.poller_window_nanos(&domain, &list.name, ty);
            let counts = queue.pollers.lock().expect("poller history lock").count_by_group(now, now.saturating_sub(window));
            let isolation_group_metrics = context.isolation_groups().into_iter().map(|group| {
                let metrics = api::IsolationGroupMetrics { new_tasks_per_second: queue.qps.group_qps(group.as_bytes()), poller_count: counts.get(&group).copied().unwrap_or_default() as i64 };
                (group, metrics)
            }).collect();
            response.task_list_status = Some(api::TaskListStatus { read_level, ack_level, backlog_count_hint, task_id_block: Some(api::TaskIdBlock { start_id, end_id }), empty, rate_per_second: queue.limiter.limit(), new_tasks_per_second: queue.qps.qps(), isolation_group_metrics });
        }
        Ok(response)
    }
    pub fn get_task_lists_by_domain(&self, request: matching::GetTaskListsByDomainRequest, context: &dyn NativeControlContext) -> Result<matching::GetTaskListsByDomainResponse, Status> {
        let domain = context.domain_id(&request.domain)?;
        let snapshot = self.registry.describe_by_domain(domain, context.return_all_kinds(), crate::grpc_matching::matching_now());
        let convert = |items: Vec<crate::matching_registry_native::NativeLoadedTaskList>| {
            items.into_iter().map(|item| (item.root, description(item.name, item.kind, item.pollers, item.partition_config))).collect()
        };
        Ok(matching::GetTaskListsByDomainResponse { decision_task_list_map: convert(snapshot.decision), activity_task_list_map: convert(snapshot.activity) })
    }
    pub async fn cancel_outstanding_poll(&self, request: matching::CancelOutstandingPollRequest, context: &dyn NativeControlContext) -> Result<matching::CancelOutstandingPollResponse, Status> {
        context.domain_name(&request.domain_id)?;
        let list = request.task_list.unwrap_or_default();
        helper::task_list_name(&list.name)?;
        let (_, queue) = self.control_queue(&request.domain_id, &list, task_type(request.task_list_type), crate::grpc_matching::matching_now(), context).await?;
        helper::cancel_outstanding_poll(&queue, &request.poller_id);
        Ok(matching::CancelOutstandingPollResponse::default())
    }
    /// Invoke mapper validation before handler admission, even when the domain is invalid.
    pub fn validate_query_completion_mapper(request: &matching::RespondQueryTaskCompletedRequest) -> Result<(), Status> {
        helper::validate_query_completion_mapper(request)
    }
    pub fn respond_query_task_completed(&self, request: matching::RespondQueryTaskCompletedRequest) -> Result<matching::RespondQueryTaskCompletedResponse, Status> {
        helper::validate_query_completion_mapper(&request)?;
        helper::deliver_query_completion(&self.registry, request)
    }
    pub async fn update_task_list_partition_config(&self, request: matching::UpdateTaskListPartitionConfigRequest, deadline: Option<tokio::time::Instant>, context: &dyn NativeControlContext) -> Result<matching::UpdateTaskListPartitionConfigResponse, Status> {
        let domain = context.domain_name(&request.domain_id)?;
        let list = request.task_list.unwrap_or_default();
        let ty = task_type(request.task_list_type);
        if context.scaler_enabled(&domain, &list.name, ty) {
            return Err(Status::invalid_argument("Manual update is not allowed because adaptive scaler is enabled."));
        }
        let config = helper::validate_partition_update(&list, request.partition_config)?;
        let now = crate::grpc_matching::matching_now();
        let (key, queue) = self.control_queue(&request.domain_id, &list, ty, now, context).await?;
        // Metrics currently require UTF-8; the registry uses this only as a metric label.
        let metric_domain = domain.as_utf8().unwrap_or_default().to_owned();
        helper::update_partition_config(&self.registry, key, queue, config, now, deadline, metric_domain).await?;
        Ok(matching::UpdateTaskListPartitionConfigResponse::default())
    }
    pub async fn refresh_task_list_partition_config(&self, request: matching::RefreshTaskListPartitionConfigRequest, deadline: Option<tokio::time::Instant>, context: &dyn NativeControlContext) -> Result<matching::RefreshTaskListPartitionConfigResponse, Status> {
        context.domain_name(&request.domain_id)?;
        let list = request.task_list.unwrap_or_default();
        let config = helper::validate_partition_refresh(&list, request.partition_config)?;
        let (key, queue) = self.control_queue(&request.domain_id, &list, task_type(request.task_list_type), crate::grpc_matching::matching_now(), context).await?;
        helper::refresh_partition_config(&self.registry, key, queue, config, deadline).await?;
        Ok(matching::RefreshTaskListPartitionConfigResponse::default())
    }
    pub async fn list_task_list_partitions(&self, request: matching::ListTaskListPartitionsRequest, context: &dyn NativeControlContext) -> Result<matching::ListTaskListPartitionsResponse, Status> {
        context.domain_id(&request.domain)?;
        let list = request.task_list.unwrap_or_default();
        let name = helper::task_list_name(&list.name)?;
        let mut result = matching::ListTaskListPartitionsResponse::default();
        for ty in [1, 0] {
            let count = context.write_partition_count(&request.domain, name.root(), ty).max(1);
            let mut partitions = Vec::new();
            for i in 0..count {
                let key = name.partition_name(i);
                let owner_host_name = context.owner_host(key.clone()).await.unwrap_or_default();
                partitions.push(api::TaskListPartitionMetadata { key, owner_host_name });
            }
            if ty == 1 { result.activity_task_list_partitions = partitions; }
            else { result.decision_task_list_partitions = partitions; }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};
    struct Context;
    impl NativeControlContext for Context {
        fn domain_id(&self, _: &GoString) -> Result<uuid::Uuid, Status> { Ok(uuid::Uuid::from_u128(1)) }
        fn domain_name(&self, _: &GoString) -> Result<GoString, Status> { Ok(vec![0xff].into()) }
        fn check_queue(&self, _: &TaskListKeyBytes, _: &NativeQueueHandle) -> Result<(), Status> { Ok(()) }
        fn return_all_kinds(&self) -> bool { true }
        fn scaler_enabled(&self, _: &GoString, _: &GoString, _: i32) -> bool { true }
        fn isolation_groups(&self) -> Vec<GoString> { vec![vec![0xfe].into()] }
        fn poller_window_nanos(&self, _: &GoString, _: &GoString, _: i32) -> i64 { 2_000_000_000 }
        fn write_partition_count(&self, _: &GoString, _: &GoString, _: i32) -> i64 { 2 }
        fn owner_host(&self, _: GoString) -> crate::matching_dispatch_native::NativeFuture<Result<GoString, Status>> { Box::pin(async { Err(Status::unavailable("owner")) }) }
    }
    fn fixture() -> (tempfile::TempDir, NativeMatchingEngine) {
        use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap());
        let history = crate::matching_history::MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(), Duration::from_secs(1), 32 * 1024 * 1024,
        );
        let registry = crate::matching_registry_native::NativeQueueRegistry::new(store, history, std::num::NonZeroU32::new(100).unwrap());
        (dir, NativeMatchingEngine::new(registry))
    }
    #[tokio::test]
    async fn loaded_descriptions_keep_raw_names_and_optional_status() {
        let (_dir, engine) = fixture();
        let domain = uuid::Uuid::from_u128(1).to_string();
        let raw = GoString::from(&b"list-\xff"[..]);
        let key = TaskListKeyBytes { domain_id: uuid::Uuid::from_u128(1), name: raw.clone(), task_type: 0 };
        let now = crate::grpc_matching::matching_now();
        let queue = engine.registry.get(key, 0, now).await.unwrap();
        queue.pollers.lock().unwrap().start(b"id", cadence_common::poller_bytes::Info { identity: vec![0xfe, 0].into(), rate_per_second: 3.0, isolation_group: vec![0xfe].into() }, now);
        let lists = engine.get_task_lists_by_domain(matching::GetTaskListsByDomainRequest::default(), &Context).unwrap();
        let listed = lists.decision_task_list_map.get(raw.as_bytes()).unwrap();
        assert_eq!(listed.task_list.as_ref().unwrap().name, raw);
        assert_eq!(listed.pollers[0].identity.as_bytes(), &[0xfe, 0]);
        assert!(listed.task_list_status.is_none());
        let response = engine.describe_task_list(matching::DescribeTaskListRequest { domain_id: domain.into(), request: Some(api::DescribeTaskListRequest { task_list: Some(api::TaskList { name: raw, ..Default::default() }), include_task_list_status: true, ..Default::default() }) }, &Context).await.unwrap();
        assert!(response.task_list_status.unwrap().isolation_group_metrics.contains_key(&GoString::from(vec![0xfe])));
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }
    #[tokio::test]
    async fn cancellation_uses_exact_poller_bytes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (_dir, engine) = fixture();
        let domain = uuid::Uuid::from_u128(1);
        let name = GoString::from(&b"queue-\xff"[..]);
        let key = TaskListKeyBytes { domain_id: domain, name: name.clone(), task_type: 0 };
        let queue = engine.registry.get(key, 0, crate::grpc_matching::matching_now()).await.unwrap();
        let called = Arc::new(AtomicUsize::new(0));
        let counter = called.clone();
        queue.pollers.lock().unwrap().set_cancel(&[0xff], Arc::new(move || { counter.fetch_add(1, Ordering::SeqCst); }));
        let make = |id: Vec<u8>| matching::CancelOutstandingPollRequest { domain_id: domain.to_string().into(), task_list: Some(api::TaskList { name: name.clone(), ..Default::default() }), poller_id: id.into(), ..Default::default() };
        engine.cancel_outstanding_poll(make(vec![0xfe]), &Context).await.unwrap();
        assert_eq!(called.load(Ordering::SeqCst), 0);
        engine.cancel_outstanding_poll(make(vec![0xff]), &Context).await.unwrap();
        assert_eq!(called.load(Ordering::SeqCst), 1);
        engine.registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
    }
    #[tokio::test]
    async fn missing_query_task_is_reported_without_domain_or_list_validation() {
        let (_dir, engine) = fixture();
        let request = matching::RespondQueryTaskCompletedRequest { domain_id: vec![0xff].into(), task_id: vec![0xfe].into(), ..Default::default() };
        let error = engine.respond_query_task_completed(request).unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
    }
    #[tokio::test]
    async fn scaler_precedes_kind_and_missing_config_validation() {
        let (_dir, engine) = fixture();
        let request = matching::UpdateTaskListPartitionConfigRequest { task_list: Some(api::TaskList { kind: 2, ..Default::default() }), ..Default::default() };
        let error = engine.update_task_list_partition_config(request, None, &Context).await.unwrap_err();
        assert_eq!(error.message(), "Manual update is not allowed because adaptive scaler is enabled.");
        assert_eq!(engine.registry.loaded_owner_count(), 0);
    }
    #[tokio::test]
    async fn list_partitions_keeps_raw_root_and_discards_owner_error() {
        let (_dir, engine) = fixture();
        let request = matching::ListTaskListPartitionsRequest { task_list: Some(api::TaskList { name: vec![0xff, 0].into(), ..Default::default() }), ..Default::default() };
        let response = engine.list_task_list_partitions(request, &Context).await.unwrap();
        assert_eq!(response.activity_task_list_partitions[0].key.as_bytes(), &[0xff, 0]);
        assert_eq!(response.activity_task_list_partitions[1].key.as_bytes(), b"/__cadence_sys/\xff\0/1");
        assert!(response.activity_task_list_partitions[1].owner_host_name.is_empty());
    }
    #[test]
    fn mapper_failure_precedes_handler_and_preserves_missing_nested_request() {
        let request = matching::RespondQueryTaskCompletedRequest { request: Some(api::RespondQueryTaskCompletedRequest::default()), domain_id: vec![0xff].into(), ..Default::default() };
        assert_eq!(NativeMatchingEngine::validate_query_completion_mapper(&request).unwrap_err().code(), tonic::Code::Unknown);
        assert!(NativeMatchingEngine::validate_query_completion_mapper(&matching::RespondQueryTaskCompletedRequest::default()).is_ok());
    }
    #[test]
    fn partition_mapper_retains_raw_isolation_groups_and_presence() {
        let config = NativeStoredPartitionConfig { version: 7, read_partitions: [(0, vec![vec![0xff, 0].into()])].into_iter().collect(), write_partitions: Default::default() };
        let mapped = config_to_proto(config);
        assert_eq!(mapped.read_partitions[&0].isolation_groups[0].as_bytes(), &[0xff, 0]);
        assert_eq!(mapped.version, 7);
        let description = description(vec![0xfe].into(), 0, Vec::new(), None);
        assert!(description.partition_config.is_none());
        assert!(description.task_list_status.is_none());
    }
}
