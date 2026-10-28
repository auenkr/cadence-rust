//! Native byte-preserving Matching semaphore handler. Runtime owns its lifecycle.
use std::{sync::Arc, time::Duration};
use cadence_common::{go_string::GoString, semaphore_bytes::parse_owner_bytes};
use cadence_proto::native as n;
use prost::Message;
use tonic::{Code, Request, Response, Status};
use crate::{
    grpc_matching::CallerCancel,
    matching_host_native::NativeSemaphore,
    matching_membership::Membership,
    matching_semaphore_manager_native::{ManagerError, Registry},
    matching_semaphore_native::{IdentifierBytes, IdentifierError},
};

/// Domain cache lookup and dynamic configuration must be supplied by the host.
pub type DomainName = Arc<dyn Fn(&GoString) -> Result<GoString, Status> + Send + Sync>;
pub type DomainConfig = Arc<dyn Fn(&GoString) -> (bool, i64) + Send + Sync>;
pub type IsShuttingDown = Arc<dyn Fn() -> bool + Send + Sync>;
/// Called exactly when an engine error is replaced by the caller's context error.
pub type ContextTimeoutMetric = Arc<dyn Fn(&GoString, &GoString, &tonic::metadata::MetadataMap) + Send + Sync>;

#[derive(Clone)]
pub struct NativeSemaphoreService {
    registry: Registry,
    membership: Membership,
    domain_name: DomainName,
    config: DomainConfig,
    shutting_down: IsShuttingDown,
    context_timeout: ContextTimeoutMetric,
}

impl NativeSemaphoreService {
    pub fn new(registry: Registry, membership: Membership, domain_name: DomainName, config: DomainConfig, shutting_down: IsShuttingDown, context_timeout: ContextTimeoutMetric) -> Self {
        Self { registry, membership, domain_name, config, shutting_down, context_timeout }
    }
    pub fn callback(&self) -> NativeSemaphore {
        let service = self.clone();
        Arc::new(move |request| {
            let service = service.clone();
            Box::pin(async move { service.acquire(request).await })
        })
    }
    pub fn stop_all(&self) {
        for manager in self.registry.all_managers() { manager.stop(); }
    }
    pub fn sweep_idle(&self, now_nanos: i64) { self.registry.sweep_idle(now_nanos); }
    /// Called after membership changes; lookup failures leave managers running for a later sweep.
    pub fn sweep_non_owned(&self) -> Vec<Status> {
        let mut errors = Vec::new();
        for manager in self.registry.all_managers() {
            match self.membership.owns_bytes(manager.identifier().ring_key().as_bytes()) {
                Ok(true) => {},
                Ok(false) => manager.stop(),
                Err(error) => errors.push(error),
            }
        }
        errors
    }
    fn check_ownership(&self, id: &IdentifierBytes) -> Result<(), Status> {
        if (self.shutting_down)() { return Err(not_owned("not known", self.membership.identity(), id)); }
        let owner = self.membership.owner_bytes(id.ring_key().as_bytes()).map_err(|error| {
            let mut msg = b"failed to look up the owner of semaphore bucket ".to_vec();
            msg.extend_from_slice(id.to_log_string().as_bytes());
            msg.extend_from_slice(b": ");
            msg.extend_from_slice(error.message().as_bytes());
            crate::matching_native_status::raw_status(Code::Internal, &GoString::from(msg), Default::default())
        })?;
        if owner.identity() == self.membership.identity() { Ok(()) } else { Err(not_owned(owner.identity(), self.membership.identity(), id)) }
    }
    pub async fn acquire(&self, request: Request<n::matching::AcquireSemaphoreRequest>) -> Result<Response<n::matching::AcquireSemaphoreResponse>, Status> {
        let deadline = crate::matching_sync::request_deadline(&request);
        let cancelled = request.extensions().get::<CallerCancel>().map(|c| c.0.clone());
        let r = request.get_ref();
        let result = self.acquire_inner(r, deadline, cancelled.clone()).await;
        match result {
            Err(error) => match caller_ended(deadline, cancelled.as_ref()) {
                Some(ended) => {
                    // The host resolves the metric domain using its handler cache.
                    (self.context_timeout)(&r.domain_id, &r.semaphore_name, request.metadata());
                    Err(Status::internal(ended))
                }
                None => Err(error),
            },
            ok => ok,
        }
    }
    async fn acquire_inner(&self, r: &n::matching::AcquireSemaphoreRequest, deadline: Option<tokio::time::Instant>, cancelled: Option<tokio::sync::watch::Receiver<bool>>) -> Result<Response<n::matching::AcquireSemaphoreResponse>, Status> {
        parse_owner_bytes(r.owner_id.as_bytes()).map_err(|e| Status::invalid_argument(format!("invalid owner id: {e}")))?;
        let id = IdentifierBytes::new(r.domain_id.clone(), r.semaphore_name.clone(), r.bucket).map_err(identifier_status)?;
        // Go gives only Manager.Start its independent background timeout. Domain
        // lookup and ring ownership checks happen before that clock starts.
        let manager = if let Some(manager) = self.registry.by_identifier(&id) {
            tokio::time::timeout(Duration::from_secs(30), manager.start())
                .await
                .map_err(|_| Status::deadline_exceeded("context deadline exceeded"))?
                .map_err(status)?;
            manager
        } else {
            let domain = (self.domain_name)(&id.domain_id)?;
            let (enabled, ttl) = (self.config)(&domain);
            if !enabled { return Err(status(ManagerError::Disabled)); }
            self.check_ownership(&id)?;
            tokio::time::timeout(Duration::from_secs(30), self.registry.get_or_create_with_idle_ttl(id, ttl))
                .await
                .map_err(|_| Status::deadline_exceeded("context deadline exceeded"))?
                .map_err(status)?
        };
        if let Some(ended) = caller_ended(deadline, cancelled.as_ref()) { return Err(Status::internal(ended)); }
        let acquire = manager.acquire(&r.owner_id, deadline);
        tokio::pin!(acquire);
        let result = if let Some(mut cancel) = cancelled {
            tokio::select! {
                result = &mut acquire => result.map_err(status)?,
                _ = async { while !*cancel.borrow_and_update() { if cancel.changed().await.is_err() { std::future::pending::<()>().await; } } } => return Err(Status::internal("context canceled")),
            }
        } else { acquire.await.map_err(status)? };
        let outcome = match result.outcome {
            cadence_api::enums::SemaphoreAcquireOutcome::Acquired => 1,
            cadence_api::enums::SemaphoreAcquireOutcome::NoSlot => 2,
            _ => 0,
        };
        Ok(Response::new(n::matching::AcquireSemaphoreResponse { outcome, token_id: result.token_id }))
    }
}

fn caller_ended(deadline: Option<tokio::time::Instant>, cancelled: Option<&tokio::sync::watch::Receiver<bool>>) -> Option<&'static str> {
    if cancelled.is_some_and(|c| *c.borrow()) { Some("context canceled") }
    else if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) { Some("context deadline exceeded") }
    else { None }
}
fn identifier_status(error: IdentifierError) -> Status {
    status(ManagerError::BadRequest(match error {
        IdentifierError::MissingDomain => "domainID is required".into(),
        IdentifierError::MissingName => "semaphoreName is required".into(),
        IdentifierError::NegativeBucket(n) => format!("bucket must not be negative, got {n}"),
    }))
}
fn status(error: ManagerError) -> Status {
    match error {
        ManagerError::BadRequest(message) => Status::invalid_argument(message),
        ManagerError::Disabled => Status::invalid_argument("distributed semaphore is not enabled for this domain"),
        ManagerError::NotFound(message) => Status::not_found(message),
        ManagerError::Deadline => Status::deadline_exceeded("semaphore acquire deadline exceeded"),
        ManagerError::NotReady => busy("semaphore manager is not ready".into()),
        ManagerError::ServiceBusy(message) => busy(message),
        ManagerError::Internal(message) => Status::internal(message),
    }
}
fn busy(message: String) -> Status {
    crate::grpc::detailed_status(Code::ResourceExhausted, message, "ServiceBusyError", cadence_proto::api::ServiceBusyError { reason: String::new() })
}

#[derive(Clone, PartialEq, Message)]
struct RawOwnershipDetail {
    #[prost(bytes = "vec", tag = "1")] owner: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")] identity: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")] bucket: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
struct RawEnvelope {
    #[prost(int32, tag = "1")] code: i32,
    #[prost(bytes = "vec", tag = "2")] message: Vec<u8>,
    #[prost(message, repeated, tag = "3")] details: Vec<prost_types::Any>,
}
fn not_owned(owner: &str, identity: &str, id: &IdentifierBytes) -> Status {
    let bucket = id.to_log_string();
    let mut message = format!("semaphore bucket is not owned by this host: OwnedBy: {owner}, Me: {identity}, BucketID: ").into_bytes();
    message.extend_from_slice(bucket.as_bytes());
    let detail = RawOwnershipDetail { owner: owner.as_bytes().to_vec(), identity: identity.as_bytes().to_vec(), bucket: bucket.as_bytes().to_vec() };
    let envelope = RawEnvelope { code: Code::Aborted as i32, message: message.clone(), details: vec![prost_types::Any { type_url: "type.googleapis.com/uber.cadence.shared.v1.SemaphoreNotOwnedByHostError".into(), value: detail.encode_to_vec() }] }.encode_to_vec();
    let details = if std::str::from_utf8(&message).is_ok() { envelope.clone().into() } else { Default::default() };
    let mut status = crate::matching_native_status::raw_status(Code::Aborted, &GoString::from(message), details);
    status.metadata_mut().insert_bin(tonic::metadata::BinaryMetadataKey::from_static(crate::matching_native_status::RAW_ERROR_DETAILS_MARKER), tonic::metadata::BinaryMetadataValue::from_bytes(&envelope));
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::membership::Host;
    use cadence_persistence::semaphores_native::{InMemorySemaphorePersistenceBytes, SemaphorePersistenceBytes};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn bytes(value: &[u8]) -> GoString { GoString::from(value) }
    fn request(owner: &[u8]) -> Request<n::matching::AcquireSemaphoreRequest> {
        Request::new(n::matching::AcquireSemaphoreRequest { domain_id: bytes(b"d"), semaphore_name: bytes(b"s\xff"), bucket: 0, owner_id: bytes(owner) })
    }
    #[tokio::test]
    async fn raw_owners_cached_fast_path_disabled_and_cancelled() {
        let store = Arc::new(InMemorySemaphorePersistenceBytes::default());
        store.create_semaphore(&bytes(b"d"), &bytes(b"s\xff"), 1, 1).await.unwrap();
        let enabled = Arc::new(AtomicBool::new(true));
        let lookups = Arc::new(AtomicUsize::new(0));
        let metrics = Arc::new(AtomicUsize::new(0));
        let service = NativeSemaphoreService::new(
            Registry::new(store.clone()),
            Membership::new("self", vec![Host { address: "self".into(), identity: "self".into(), grpc_address: String::new() }]),
            { let lookups = lookups.clone(); Arc::new(move |_| { lookups.fetch_add(1, Ordering::SeqCst); Ok(bytes(b"domain")) }) },
            { let enabled = enabled.clone(); Arc::new(move |_| (enabled.load(Ordering::SeqCst), 1_000_000_000)) },
            Arc::new(|| false),
            { let metrics = metrics.clone(); Arc::new(move |_, _, _| { metrics.fetch_add(1, Ordering::SeqCst); }) },
        );
        let owner = b"1:\xff:\xfe:1";
        let first = service.acquire(request(owner)).await.unwrap().into_inner();
        assert_eq!(first.outcome, 1);
        assert_eq!(store.get_semaphore_ownership_by_token(&bytes(b"d"), &bytes(b"s\xff"), 0, first.token_id).await.unwrap().as_bytes(), owner);
        enabled.store(false, Ordering::SeqCst);
        assert_eq!(service.acquire(request(owner)).await.unwrap().into_inner().token_id, first.token_id);
        assert_eq!(lookups.load(Ordering::SeqCst), 1);
        assert_eq!(service.acquire(request(b"1:x:r:2")).await.unwrap().into_inner().outcome, 2);
        let (tx, rx) = tokio::sync::watch::channel(true);
        let mut cancelled = request(b"1:x:r:2");
        cancelled.extensions_mut().insert(CallerCancel(rx));
        assert_eq!(service.acquire(cancelled).await.unwrap_err().message(), "context canceled");
        assert_eq!(metrics.load(Ordering::SeqCst), 1);
        drop(tx);
        service.stop_all();
        assert_eq!(service.acquire(request(owner)).await.unwrap_err().message(), "distributed semaphore is not enabled for this domain");
        assert_eq!(service.acquire(request(b"\xff")).await.unwrap_err().code(), Code::InvalidArgument);
    }
    #[test]
    fn ownership_error_retains_raw_bucket_detail() {
        let id = IdentifierBytes::new(bytes(b"d"), bytes(b"s\xff"), 3).unwrap();
        let error = not_owned("other", "self", &id);
        assert_eq!(error.code(), Code::Aborted);
        assert!(error.details().is_empty());
        let encoded = error.metadata().get_bin(crate::matching_native_status::RAW_ERROR_DETAILS_MARKER).unwrap().to_bytes().unwrap();
        let envelope = RawEnvelope::decode(encoded).unwrap();
        let detail = RawOwnershipDetail::decode(envelope.details[0].value.as_slice()).unwrap();
        assert_eq!(detail.bucket, b"d/s\xff/3");
    }
}
