//! Host assembly for the byte-valued Matching service.
use std::{collections::HashMap, sync::{Arc, Mutex}, time::Duration};
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use cadence_proto::native as n;
use tonic::{Request, Response, Status};
use crate::{
    handlers::AppState,
    matching_control_context_native::NativeMatchingControlContext,
    matching_dispatch_native::{NativeFuture, NativeMatchingApi, NativePollObserver},
    matching_domain_cache::MatchingDomainCache,
    matching_domain_uuid::DomainIdParse,
    matching_engine_context_native::{NativeMatchingEngineContext, PollObserverFactory, PollRegistrar},
    matching_engine_native::{DecisionAddContext, NativeControlContext, NativeMatchingEngine, NativePollContext, NativePollRegistration, NativeQueryContext, NativeQueryParent},
    matching_membership::Membership,
    matching_registry_native::{NativeQueueHandle, NativeQueueRegistry},
    matching_service_native::{NativeMatchingIntegration, NativeMatchingService},
    matching_shard_routing::MatchingShardRouter,
};
use crate::matching_quota::{MatchingRpc, MatchingRpcQuota};

pub type NativeSemaphore = Arc<dyn Fn(Request<n::matching::AcquireSemaphoreRequest>) -> NativeFuture<Result<Response<n::matching::AcquireSemaphoreResponse>, Status>> + Send + Sync>;
pub type NativeRequestMetric = Arc<dyn Fn(&'static str) + Send + Sync>;
pub type NativeFailureMetric = Arc<dyn Fn(&'static str, &Status) + Send + Sync>;

#[derive(Clone, Hash, PartialEq, Eq)]
struct PollKey { domain: GoString, list: GoString, task_type: i32, poller: GoString }
type PollMap = Arc<Mutex<HashMap<PollKey, Arc<tokio::sync::Notify>>>>;
struct PollGuard { polls: PollMap, key: PollKey }
impl Drop for PollGuard {
    fn drop(&mut self) { self.polls.lock().expect("poll cancellation lock").remove(&self.key); }
}
fn register(polls: &PollMap, domain: &GoString, list: &GoString, task_type: i32, poller: &GoString) -> NativePollRegistration {
    let key = PollKey { domain: domain.clone(), list: list.clone(), task_type, poller: poller.clone() };
    let cancel = Arc::new(tokio::sync::Notify::new());
    polls.lock().expect("poll cancellation lock").insert(key.clone(), cancel.clone());
    NativePollRegistration { cancel, guard: Box::new(PollGuard { polls: polls.clone(), key }) }
}

/// Keeps the startup gate and handler state shared with the assembler's service.
/// The launcher must mark the gate started only after engine and membership startup.
pub struct NativeMatchingHost {
    engine: NativeMatchingEngineContext,
    control: Arc<NativeMatchingControlContext>,
    registry: NativeQueueRegistry,
    started: tokio::sync::watch::Sender<u8>,
    quota: Arc<MatchingRpcQuota>,
    domain_parse: DomainIdParse,
    semaphore: NativeSemaphore,
    request_metric: NativeRequestMetric,
    failure_metric: NativeFailureMetric,
}

impl NativeMatchingHost {
    #[allow(clippy::too_many_arguments)]
    pub fn assemble(state: AppState, cache: Arc<MatchingDomainCache>, registry: NativeQueueRegistry, membership: Membership, shard_router: Option<Arc<MatchingShardRouter>>, quota: Arc<MatchingRpcQuota>, domain_parse: DomainIdParse, semaphore: NativeSemaphore, request_metric: NativeRequestMetric, failure_metric: NativeFailureMetric, observer: Option<PollObserverFactory>) -> NativeMatchingService<Self> {
        let control = Arc::new(NativeMatchingControlContext::new(state.clone(), cache.clone(), registry.clone(), membership.clone(), shard_router.clone()));
        let polls: PollMap = Arc::default();
        let register: PollRegistrar = Arc::new(move |domain, list, kind, poller| register(&polls, domain, list, kind, poller));
        let engine_context = NativeMatchingEngineContext::with_control(state, cache, control.clone(), membership, shard_router, register, observer);
        let started = tokio::sync::watch::channel(0).0;
        NativeMatchingService::new(NativeMatchingEngine::new(registry.clone()), Self { engine: engine_context, control, registry, started, quota, domain_parse, semaphore, request_metric, failure_metric })
    }
    /// Open admission after engine and membership startup has completed.
    pub fn mark_started(&self) { self.started.send_if_modified(|state| { if *state == 0 { *state = 1; true } else { false } }); }
    /// Close admission during drain. Existing queue ownership remains governed
    /// by the registry's cached-owner rules; no new RPC may enter the host.
    pub fn begin_shutdown(&self) { self.started.send_replace(2); }
}

fn rpc(method: &str) -> Option<MatchingRpc> {
    Some(match method {
        "add_decision_task" => MatchingRpc::AddDecisionTask, "add_activity_task" => MatchingRpc::AddActivityTask,
        "poll_for_decision_task" => MatchingRpc::PollForDecisionTask, "poll_for_activity_task" => MatchingRpc::PollForActivityTask,
        "query_workflow" => MatchingRpc::QueryWorkflow, "respond_query_task_completed" => MatchingRpc::RespondQueryTaskCompleted,
        "cancel_outstanding_poll" => MatchingRpc::CancelOutstandingPoll, "describe_task_list" => MatchingRpc::DescribeTaskList,
        "list_task_list_partitions" => MatchingRpc::ListTaskListPartitions, "get_task_lists_by_domain" => MatchingRpc::GetTaskListsByDomain,
        "update_task_list_partition_config" => MatchingRpc::UpdateTaskListPartitionConfig, "refresh_task_list_partition_config" => MatchingRpc::RefreshTaskListPartitionConfig,
        "acquire_semaphore" => MatchingRpc::AcquireSemaphore, _ => return None,
    })
}
async fn wait_for_startup(receiver: &mut tokio::sync::watch::Receiver<u8>) -> Result<(), Status> {
    let state = receiver.wait_for(|state| *state != 0).await
        .map_err(|_| Status::unavailable("Matching startup stopped"))?;
    if *state == 1 { Ok(()) } else { Err(Status::unavailable("Matching host is shutting down")) }
}
impl NativeMatchingIntegration for NativeMatchingHost {
    fn check_started(&self) -> NativeFuture<Result<(), Status>> {
        let mut receiver = self.started.subscribe();
        Box::pin(async move { wait_for_startup(&mut receiver).await })
    }
    fn admit(&self, method: &'static str, domain: Option<&GoString>) -> Result<(), Status> {
        let method = rpc(method).ok_or_else(|| Status::internal("unknown Matching RPC"))?;
        let direct = matches!(method, MatchingRpc::ListTaskListPartitions | MatchingRpc::GetTaskListsByDomain);
        let name = if direct { domain.cloned() } else { domain.map(|id| NativeControlContext::domain_name(self.control.as_ref(), id).unwrap_or_default()) };
        if self.quota.allow_go_string(method, name.as_ref()) { Ok(()) } else { Err(crate::grpc::detailed_status(tonic::Code::ResourceExhausted, "Matching host rps exceeded", "ServiceBusyError", cadence_proto::api::ServiceBusyError { reason: String::new() })) }
    }
    fn record_request(&self, method: &'static str) { (self.request_metric)(method); }
    fn record_failure(&self, method: &'static str, status: &Status) { (self.failure_metric)(method, status); }
    fn check_owned(&self, key: &TaskListKeyBytes) -> Result<(), Status> { self.registry.check_task_list_ownership(&key.name, true) }
    fn domain_for_name(&self, name: &GoString) -> Result<uuid::Uuid, Status> { self.control.domain_id(name) }
    fn check_domain(&self, id: &GoString, method: &'static str) -> Result<(), Status> {
        if let Ok(id) = std::str::from_utf8(id.as_bytes()) { crate::matching_domain_uuid::domain_lookup_guard(self.domain_parse, id, method) }
        else if matches!(self.domain_parse, DomainIdParse::GoogleUuidPanic) {
            let bytes = id.as_bytes();
            let error = if bytes.len() == 45 && !bytes[..9].eq_ignore_ascii_case(b"urn:uuid:") {
                format!("invalid urn prefix: {}", GoString::from(&bytes[..9]).go_quoted_string())
            } else if matches!(bytes.len(), 32 | 36 | 38 | 45) {
                "invalid UUID format".to_string()
            } else { format!("invalid UUID length: {}", bytes.len()) };
            let mut panic = b"uuid: Parse(".to_vec();
            panic.extend_from_slice(bytes);
            panic.extend_from_slice(format!("): {error}").as_bytes());
            let message = format!("panic object is not error: {}", GoString::from(panic).go_quoted_string());
            Err(Status::unknown(message))
        } else { Ok(()) }
    }
    fn acquire_semaphore(&self, request: Request<n::matching::AcquireSemaphoreRequest>) -> NativeFuture<Result<Response<n::matching::AcquireSemaphoreResponse>, Status>> { (self.semaphore)(request) }
}

impl DecisionAddContext for NativeMatchingHost {
    fn domain_active(&self, id: uuid::Uuid) -> Result<bool, Status> { DecisionAddContext::domain_active(&self.engine, id) }
    fn forward_degree(&self, id: uuid::Uuid, list: &GoString, kind: i32) -> i64 { DecisionAddContext::forward_degree(&self.engine, id, list, kind) }
    fn check_write_partition(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> { self.engine.check_write_partition(key, queue) }
    fn remote_parent(&self, name: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>> { DecisionAddContext::remote_parent(&self.engine, name) }
}
impl NativePollContext for NativeMatchingHost {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> { NativePollContext::domain_name(&self.engine, id) }
    fn domain_active(&self, id: &GoString) -> Result<bool, Status> { NativePollContext::domain_active(&self.engine, id) }
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> { NativePollContext::check_queue(&self.engine, key, queue) }
    fn register_poll(&self, domain: &GoString, list: &GoString, kind: i32, poller: &GoString) -> NativePollRegistration { self.engine.register_poll(domain, list, kind, poller) }
    fn failover(&self, domain: &GoString) -> NativeFuture<()> { self.engine.failover(domain) }
    fn expiration(&self, domain: &GoString, list: &GoString, kind: i32) -> Duration { self.engine.expiration(domain, list, kind) }
    fn override_rate(&self, domain: &GoString, list: &GoString, kind: i32) -> f64 { self.engine.override_rate(domain, list, kind) }
    fn auto_config_enabled(&self, domain: &GoString, list: &GoString, kind: i32) -> bool { self.engine.auto_config_enabled(domain, list, kind) }
    fn observer(&self, domain: &GoString, list: &GoString, kind: i32) -> Option<Arc<dyn NativePollObserver>> { self.engine.observer(domain, list, kind) }
}
impl NativeQueryContext for NativeMatchingHost {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> { NativeQueryContext::domain_name(&self.engine, id) }
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> { NativeQueryContext::check_queue(&self.engine, key, queue) }
    fn forward_degree(&self, id: uuid::Uuid, list: &GoString) -> i64 { NativeQueryContext::forward_degree(&self.engine, id, list) }
    fn remote_parent(&self, name: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeQueryParent>>, Status>> { NativeQueryContext::remote_parent(&self.engine, name) }
    fn default_deadline(&self) -> tokio::time::Instant { self.engine.default_deadline() }
}
impl NativeControlContext for NativeMatchingHost {
    fn domain_id(&self, name: &GoString) -> Result<uuid::Uuid, Status> { self.control.domain_id(name) }
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> { self.control.domain_name(id) }
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> { self.control.check_queue(key, queue) }
    fn return_all_kinds(&self) -> bool { self.control.return_all_kinds() }
    fn scaler_enabled(&self, domain: &GoString, list: &GoString, kind: i32) -> bool { self.control.scaler_enabled(domain, list, kind) }
    fn isolation_groups(&self) -> Vec<GoString> { self.control.isolation_groups() }
    fn poller_window_nanos(&self, domain: &GoString, list: &GoString, kind: i32) -> i64 { self.control.poller_window_nanos(domain, list, kind) }
    fn write_partition_count(&self, domain: &GoString, list: &GoString, kind: i32) -> i64 { self.control.write_partition_count(domain, list, kind) }
    fn owner_host(&self, partition: GoString) -> NativeFuture<Result<GoString, Status>> { self.control.owner_host(partition) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn poll_guard_preserves_raw_keys_and_go_collision_cleanup() {
        let polls: PollMap = Arc::default();
        let domain = GoString::from("domain");
        let list = GoString::from(&b"list-\xff"[..]);
        let id = GoString::from(&b"id-\xff"[..]);
        let replacement = GoString::from("id-�");
        let first = register(&polls, &domain, &list, 1, &id);
        let other = register(&polls, &domain, &list, 1, &replacement);
        let second = register(&polls, &domain, &list, 1, &id);
        assert_eq!(polls.lock().unwrap().len(), 2);
        drop(first.guard);
        assert_eq!(polls.lock().unwrap().len(), 1);
        other.cancel.notify_one();
        tokio::time::timeout(Duration::from_millis(50), other.cancel.notified()).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(10), second.cancel.notified()).await.is_err());
    }
    #[tokio::test]
    async fn startup_gate_opens_and_shutdown_wakes_waiters_with_unavailable() {
        let (sender, mut receiver) = tokio::sync::watch::channel(0);
        let waiting = tokio::spawn(async move { wait_for_startup(&mut receiver).await });
        sender.send_replace(1);
        assert!(waiting.await.unwrap().is_ok());
        sender.send_replace(2);
        let mut after_drain = sender.subscribe();
        assert_eq!(wait_for_startup(&mut after_drain).await.unwrap_err().code(), tonic::Code::Unavailable);

        let (sender, mut startup_waiter) = tokio::sync::watch::channel(0);
        let waiting = tokio::spawn(async move { wait_for_startup(&mut startup_waiter).await });
        sender.send_replace(2);
        assert_eq!(waiting.await.unwrap().unwrap_err().code(), tonic::Code::Unavailable);
    }
    #[test]
    fn every_rpc_has_explicit_quota_policy() {
        for method in ["add_decision_task", "add_activity_task", "poll_for_decision_task", "poll_for_activity_task", "query_workflow", "respond_query_task_completed", "cancel_outstanding_poll", "describe_task_list", "list_task_list_partitions", "get_task_lists_by_domain", "update_task_list_partition_config", "refresh_task_list_partition_config", "acquire_semaphore"] { assert!(rpc(method).is_some()); }
        assert!(rpc("unknown").is_none());
    }
}
