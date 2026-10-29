//! Native byte-preserving Matching semaphore handler. Runtime owns its lifecycle.
use crate::{
    grpc_matching::CallerCancel,
    matching_host_native::NativeSemaphore,
    matching_membership::Membership,
    matching_semaphore_manager_native::{ManagerError, Registry},
    matching_semaphore_native::{IdentifierBytes, IdentifierError},
};
use cadence_common::{go_string::GoString, semaphore_bytes::parse_owner_bytes};
use cadence_proto::native as n;
use prost::Message;
use std::{sync::Arc, time::Duration};
use tonic::{Code, Request, Response, Status};

/// Domain cache lookup and dynamic configuration must be supplied by the host.
pub type DomainName = Arc<dyn Fn(&GoString) -> Result<GoString, Status> + Send + Sync>;
pub type DomainConfig = Arc<dyn Fn(&GoString) -> (bool, i64) + Send + Sync>;
pub type IsShuttingDown = Arc<dyn Fn() -> bool + Send + Sync>;
/// Called exactly when an engine error is replaced by the caller's context error.
pub type ContextTimeoutMetric =
    Arc<dyn Fn(&GoString, &GoString, &tonic::metadata::MetadataMap) + Send + Sync>;

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
    pub fn new(
        registry: Registry,
        membership: Membership,
        domain_name: DomainName,
        config: DomainConfig,
        shutting_down: IsShuttingDown,
        context_timeout: ContextTimeoutMetric,
    ) -> Self {
        Self {
            registry,
            membership,
            domain_name,
            config,
            shutting_down,
            context_timeout,
        }
    }
    pub fn callback(&self) -> NativeSemaphore {
        let service = self.clone();
        Arc::new(move |request| {
            let service = service.clone();
            Box::pin(async move { service.acquire(request).await })
        })
    }
    pub fn stop_all(&self) {
        for manager in self.registry.all_managers() {
            manager.stop();
        }
    }
    pub fn sweep_idle(&self, now_nanos: i64) {
        self.registry.sweep_idle(now_nanos);
    }
    /// Called after membership changes; lookup failures leave managers running for a later sweep.
    pub fn sweep_non_owned(&self) -> Vec<Status> {
        let mut errors = Vec::new();
        for manager in self.registry.all_managers() {
            match self
                .membership
                .owns_bytes(manager.identifier().ring_key().as_bytes())
            {
                Ok(true) => {}
                Ok(false) => manager.stop(),
                Err(error) => errors.push(error),
            }
        }
        errors
    }
    fn check_ownership(&self, id: &IdentifierBytes) -> Result<(), Status> {
        if (self.shutting_down)() {
            return Err(not_owned("not known", self.membership.identity(), id));
        }
        let owner = self
            .membership
            .owner_bytes(id.ring_key().as_bytes())
            .map_err(|error| {
                let mut msg = b"failed to look up the owner of semaphore bucket ".to_vec();
                msg.extend_from_slice(id.to_log_string().as_bytes());
                msg.extend_from_slice(b": ");
                msg.extend_from_slice(error.message().as_bytes());
                crate::matching_native_status::raw_status(
                    Code::Internal,
                    &GoString::from(msg),
                    Default::default(),
                )
            })?;
        if owner.identity() == self.membership.identity() {
            Ok(())
        } else {
            Err(not_owned(owner.identity(), self.membership.identity(), id))
        }
    }
    pub async fn acquire(
        &self,
        request: Request<n::matching::AcquireSemaphoreRequest>,
    ) -> Result<Response<n::matching::AcquireSemaphoreResponse>, Status> {
        // Tonic drops the handler future on a wire deadline or disconnect. Go's
        // handler keeps running while Start uses its background timeout, then
        // observes the canceled caller and records the context-error metric.
        struct CancelOnDrop(tokio::sync::watch::Sender<bool>);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                self.0.send_replace(true);
            }
        }
        let (cancel_sender, dropped) = tokio::sync::watch::channel(false);
        let _cancel_on_drop = CancelOnDrop(cancel_sender);
        let service = self.clone();
        tokio::spawn(async move { service.acquire_owned(request, dropped).await })
            .await
            .map_err(|_| Status::internal("semaphore handler task failed"))?
    }

    async fn acquire_owned(
        &self,
        request: Request<n::matching::AcquireSemaphoreRequest>,
        dropped: tokio::sync::watch::Receiver<bool>,
    ) -> Result<Response<n::matching::AcquireSemaphoreResponse>, Status> {
        let deadline = crate::matching_sync::request_deadline(&request);
        let cancelled = request
            .extensions()
            .get::<CallerCancel>()
            .map(|c| c.0.clone());
        let r = request.get_ref();
        let result = self
            .acquire_inner(r, deadline, cancelled.clone(), dropped.clone())
            .await;
        match result {
            Err(error) => match caller_ended(deadline, cancelled.as_ref(), &dropped) {
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
    async fn acquire_inner(
        &self,
        r: &n::matching::AcquireSemaphoreRequest,
        deadline: Option<tokio::time::Instant>,
        cancelled: Option<tokio::sync::watch::Receiver<bool>>,
        mut dropped: tokio::sync::watch::Receiver<bool>,
    ) -> Result<Response<n::matching::AcquireSemaphoreResponse>, Status> {
        let owner = parse_owner_bytes(r.owner_id.as_bytes())
            .map_err(|e| Status::invalid_argument(format!("invalid owner id: {e}")))?;
        if let Ok(domain) = r.domain_id.as_utf8() {
            crate::matching_task_info_log::emit(
                domain,
                "Received AcquireSemaphore",
                &[
                    ("wf-id", owner.workflow_id.go_json_string()),
                    ("wf-run-id", owner.run_id.go_json_string()),
                    ("wf-domain-id", r.domain_id.go_json_string()),
                    ("wf-schedule-id", owner.hold_id.to_string()),
                ],
            );
        }
        let id = IdentifierBytes::new(r.domain_id.clone(), r.semaphore_name.clone(), r.bucket)
            .map_err(identifier_status)?;
        // Go gives only Manager.Start its independent background timeout. Domain
        // lookup and ring ownership checks happen before that clock starts.
        let manager = if let Some(manager) = self.registry.by_identifier(&id) {
            tokio::time::timeout(Duration::from_secs(30), manager.start())
                .await
                .map_err(|_| startup_timeout_status(&id))?
                .map_err(status)?;
            manager
        } else {
            let domain = (self.domain_name)(&id.domain_id)?;
            let (enabled, ttl) = (self.config)(&domain);
            if !enabled {
                return Err(status(ManagerError::Disabled));
            }
            self.check_ownership(&id)?;
            tokio::time::timeout(
                Duration::from_secs(30),
                self.registry.get_or_create_with_idle_ttl(id.clone(), ttl),
            )
            .await
            .map_err(|_| startup_timeout_status(&id))?
            .map_err(status)?
        };
        if let Some(ended) = caller_ended(deadline, cancelled.as_ref(), &dropped) {
            return Err(Status::internal(ended));
        }
        let acquire = manager.acquire(&r.owner_id, deadline);
        tokio::pin!(acquire);
        let result = tokio::select! {
            result = &mut acquire => result.map_err(status)?,
            _ = wait_deadline(deadline) => return Err(Status::internal("context deadline exceeded")),
            _ = wait_cancelled(cancelled) => return Err(Status::internal("context canceled")),
            _ = wait_for_cancel(&mut dropped) => return Err(Status::internal("context canceled")),
        };
        let outcome = match result.outcome {
            cadence_api::enums::SemaphoreAcquireOutcome::Acquired => 1,
            cadence_api::enums::SemaphoreAcquireOutcome::NoSlot => 2,
            _ => 0,
        };
        Ok(Response::new(n::matching::AcquireSemaphoreResponse {
            outcome,
            token_id: result.token_id,
        }))
    }
}

fn startup_timeout_status(id: &IdentifierBytes) -> Status {
    // Go wraps the scan error in load, then handleErr makes the live caller's
    // uncategorized error an InternalServiceError. The identifier can contain raw bytes.
    let mut message = b"load semaphore bucket ".to_vec();
    message.extend_from_slice(id.to_log_string().as_bytes());
    message.extend_from_slice(b": context deadline exceeded");
    crate::matching_native_status::raw_status(
        Code::Internal,
        &GoString::from(message),
        Default::default(),
    )
}

fn caller_ended(
    deadline: Option<tokio::time::Instant>,
    cancelled: Option<&tokio::sync::watch::Receiver<bool>>,
    dropped: &tokio::sync::watch::Receiver<bool>,
) -> Option<&'static str> {
    if cancelled.is_some_and(|c| *c.borrow()) {
        Some("context canceled")
    } else if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
        Some("context deadline exceeded")
    } else if *dropped.borrow() {
        Some("context canceled")
    } else {
        None
    }
}

async fn wait_for_cancel(cancel: &mut tokio::sync::watch::Receiver<bool>) {
    while !*cancel.borrow_and_update() {
        if cancel.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn wait_deadline(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn wait_cancelled(mut cancel: Option<tokio::sync::watch::Receiver<bool>>) {
    if let Some(cancel) = &mut cancel {
        wait_for_cancel(cancel).await;
    } else {
        std::future::pending::<()>().await;
    }
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
        ManagerError::Disabled => {
            Status::invalid_argument("distributed semaphore is not enabled for this domain")
        }
        ManagerError::NotFound(message) => Status::not_found(message),
        ManagerError::Deadline => Status::deadline_exceeded("semaphore acquire deadline exceeded"),
        ManagerError::NotReady => busy("semaphore manager is not ready".into()),
        ManagerError::ServiceBusy(message) => busy(message),
        ManagerError::Internal(message) => Status::internal(message),
    }
}
fn busy(message: String) -> Status {
    crate::grpc::detailed_status(
        Code::ResourceExhausted,
        message,
        "ServiceBusyError",
        cadence_proto::api::ServiceBusyError {
            reason: String::new(),
        },
    )
}

#[derive(Clone, PartialEq, Message)]
struct RawOwnershipDetail {
    #[prost(bytes = "vec", tag = "1")]
    owner: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    identity: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    bucket: Vec<u8>,
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
fn not_owned(owner: &str, identity: &str, id: &IdentifierBytes) -> Status {
    let bucket = id.to_log_string();
    let mut message = format!(
        "semaphore bucket is not owned by this host: OwnedBy: {owner}, Me: {identity}, BucketID: "
    )
    .into_bytes();
    message.extend_from_slice(bucket.as_bytes());
    let detail = RawOwnershipDetail {
        owner: owner.as_bytes().to_vec(),
        identity: identity.as_bytes().to_vec(),
        bucket: bucket.as_bytes().to_vec(),
    };
    let envelope = RawEnvelope {
        code: Code::Aborted as i32,
        message: message.clone(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/uber.cadence.shared.v1.SemaphoreNotOwnedByHostError"
                .into(),
            value: detail.encode_to_vec(),
        }],
    }
    .encode_to_vec();
    let details = if std::str::from_utf8(&message).is_ok() {
        envelope.clone().into()
    } else {
        Default::default()
    };
    let mut status =
        crate::matching_native_status::raw_status(Code::Aborted, &GoString::from(message), details);
    status.metadata_mut().insert_bin(
        tonic::metadata::BinaryMetadataKey::from_static(
            crate::matching_native_status::RAW_ERROR_DETAILS_MARKER,
        ),
        tonic::metadata::BinaryMetadataValue::from_bytes(&envelope),
    );
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::membership::Host;
    use cadence_persistence::semaphores_native::{
        InMemorySemaphorePersistenceBytes, SemaphorePersistenceBytes,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn bytes(value: &[u8]) -> GoString {
        GoString::from(value)
    }
    fn request(owner: &[u8]) -> Request<n::matching::AcquireSemaphoreRequest> {
        Request::new(n::matching::AcquireSemaphoreRequest {
            domain_id: bytes(b"d"),
            semaphore_name: bytes(b"s\xff"),
            bucket: 0,
            owner_id: bytes(owner),
        })
    }
    #[tokio::test]
    async fn raw_owners_cached_fast_path_disabled_and_cancelled() {
        let store = Arc::new(InMemorySemaphorePersistenceBytes::default());
        store
            .create_semaphore(&bytes(b"d"), &bytes(b"s\xff"), 1, 1)
            .await
            .unwrap();
        let enabled = Arc::new(AtomicBool::new(true));
        let lookups = Arc::new(AtomicUsize::new(0));
        let metrics = Arc::new(AtomicUsize::new(0));
        let service = NativeSemaphoreService::new(
            Registry::new(store.clone()),
            Membership::new(
                "self",
                vec![Host {
                    address: "self".into(),
                    identity: "self".into(),
                    grpc_address: String::new(),
                }],
            ),
            {
                let lookups = lookups.clone();
                Arc::new(move |_| {
                    lookups.fetch_add(1, Ordering::SeqCst);
                    Ok(bytes(b"domain"))
                })
            },
            {
                let enabled = enabled.clone();
                Arc::new(move |_| (enabled.load(Ordering::SeqCst), 1_000_000_000))
            },
            Arc::new(|| false),
            {
                let metrics = metrics.clone();
                Arc::new(move |_, _, _| {
                    metrics.fetch_add(1, Ordering::SeqCst);
                })
            },
        );
        let owner = b"1:\xff:\xfe:1";
        let first = service.acquire(request(owner)).await.unwrap().into_inner();
        assert_eq!(first.outcome, 1);
        assert_eq!(
            store
                .get_semaphore_ownership_by_token(&bytes(b"d"), &bytes(b"s\xff"), 0, first.token_id)
                .await
                .unwrap()
                .as_bytes(),
            owner
        );
        enabled.store(false, Ordering::SeqCst);
        assert_eq!(
            service
                .acquire(request(owner))
                .await
                .unwrap()
                .into_inner()
                .token_id,
            first.token_id
        );
        assert_eq!(lookups.load(Ordering::SeqCst), 1);
        assert_eq!(
            service
                .acquire(request(b"1:x:r:2"))
                .await
                .unwrap()
                .into_inner()
                .outcome,
            2
        );
        let (tx, rx) = tokio::sync::watch::channel(true);
        let mut cancelled = request(b"1:x:r:2");
        cancelled.extensions_mut().insert(CallerCancel(rx));
        assert_eq!(
            service.acquire(cancelled).await.unwrap_err().message(),
            "context canceled"
        );
        assert_eq!(metrics.load(Ordering::SeqCst), 1);
        drop(tx);
        service.stop_all();
        assert_eq!(
            service.acquire(request(owner)).await.unwrap_err().message(),
            "distributed semaphore is not enabled for this domain"
        );
        assert_eq!(
            service.acquire(request(b"\xff")).await.unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    #[tokio::test]
    async fn slow_start_keeps_running_after_caller_cancel_and_reuses_manager() {
        use crate::matching_semaphore_manager_native::tests::ParkGrant;
        use tokio::sync::Notify;

        let inner = InMemorySemaphorePersistenceBytes::default();
        inner
            .create_semaphore(&bytes(b"d"), &bytes(b"s\xff"), 1, 1)
            .await
            .unwrap();
        let store = Arc::new(ParkGrant {
            inner,
            entered: Notify::new(),
            release: Notify::new(),
            scan_pages: AtomicUsize::new(0),
            park_scan: AtomicBool::new(true),
            fail_scan: AtomicBool::new(false),
            scan_entered: Notify::new(),
            scan_release: Notify::new(),
        });
        let registry = Registry::new(store.clone());
        let metrics = Arc::new(AtomicUsize::new(0));
        let service = NativeSemaphoreService::new(
            registry.clone(),
            Membership::new(
                "self",
                vec![Host {
                    address: "self".into(),
                    identity: "self".into(),
                    grpc_address: String::new(),
                }],
            ),
            Arc::new(|_| Ok(bytes(b"domain"))),
            Arc::new(|_| (true, 1_000_000_000)),
            Arc::new(|| false),
            {
                let metrics = metrics.clone();
                Arc::new(move |_, _, _| {
                    metrics.fetch_add(1, Ordering::SeqCst);
                })
            },
        );
        let (cancel, receiver) = tokio::sync::watch::channel(false);
        let mut first = request(b"1:x:r:1");
        first.extensions_mut().insert(CallerCancel(receiver));
        let first = {
            let service = service.clone();
            tokio::spawn(async move { service.acquire(first).await })
        };
        store.scan_entered.notified().await;
        let manager = registry.all_managers().pop().unwrap();
        cancel.send(true).unwrap();
        assert!(
            !first.is_finished(),
            "the initiating load is independent of caller cancellation"
        );

        let mut expired = request(b"1:y:r:2");
        expired
            .metadata_mut()
            .insert("grpc-timeout", "0n".parse().unwrap());
        let error = service.acquire(expired).await.unwrap_err();
        assert_eq!(error.message(), "context deadline exceeded");
        assert_eq!(metrics.load(Ordering::SeqCst), 1);
        assert_eq!(store.scan_pages.load(Ordering::SeqCst), 1);

        store.scan_release.notify_one();
        let error = first.await.unwrap().unwrap_err();
        assert_eq!(error.message(), "context canceled");
        assert_eq!(metrics.load(Ordering::SeqCst), 2);
        assert!(Arc::ptr_eq(&manager, &registry.all_managers()[0]));
        // A caller deadline also bounds an in-flight persistence grant, even
        // when there is no transport task to drop the handler.
        let mut grant_request = request(b"8:deadline:r:3");
        grant_request.set_timeout(Duration::from_millis(200));
        let grant = {
            let service = service.clone();
            tokio::spawn(async move { service.acquire(grant_request).await })
        };
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), grant)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.message(), "context deadline exceeded");
        assert_eq!(metrics.load(Ordering::SeqCst), 3);
        store.release.notify_one();
        assert_eq!(
            service
                .acquire(request(b"1:z:r:3"))
                .await
                .unwrap()
                .into_inner()
                .outcome,
            1
        );
        assert_eq!(store.scan_pages.load(Ordering::SeqCst), 1);
        service.stop_all();
    }

    #[tokio::test(start_paused = true)]
    async fn independent_startup_timeout_stops_manager_and_allows_fresh_acquire() {
        use crate::matching_semaphore_manager_native::tests::ParkGrant;
        use tokio::sync::Notify;

        let inner = InMemorySemaphorePersistenceBytes::default();
        inner
            .create_semaphore(&bytes(b"d"), &bytes(b"s\xff"), 1, 1)
            .await
            .unwrap();
        let store = Arc::new(ParkGrant {
            inner,
            entered: Notify::new(),
            release: Notify::new(),
            scan_pages: AtomicUsize::new(0),
            park_scan: AtomicBool::new(true),
            fail_scan: AtomicBool::new(false),
            scan_entered: Notify::new(),
            scan_release: Notify::new(),
        });
        let registry = Registry::new(store.clone());
        let metrics = Arc::new(AtomicUsize::new(0));
        let service = NativeSemaphoreService::new(
            registry.clone(),
            Membership::new(
                "self",
                vec![Host {
                    address: "self".into(),
                    identity: "self".into(),
                    grpc_address: String::new(),
                }],
            ),
            Arc::new(|_| Ok(bytes(b"domain"))),
            Arc::new(|_| (true, 1_000_000_000)),
            Arc::new(|| false),
            {
                let metrics = metrics.clone();
                Arc::new(move |_, _, _| {
                    metrics.fetch_add(1, Ordering::SeqCst);
                })
            },
        );
        // Neither request has a caller deadline; only the loader's clock can expire.
        let first = {
            let service = service.clone();
            tokio::spawn(async move { service.acquire(request(b"1:x:r:1")).await })
        };
        store.scan_entered.notified().await;
        let old = registry.all_managers().pop().unwrap();
        let waiter = service.acquire(request(b"1:y:r:2"));
        tokio::pin!(waiter);
        tokio::select! {
            result = &mut waiter => panic!("waiter finished during startup: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!first.is_finished());
        assert!(Arc::ptr_eq(&old, &registry.all_managers()[0]));
        assert_eq!(store.scan_pages.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        let error = first.await.unwrap().unwrap_err();
        assert_eq!(error.code(), Code::Internal);
        assert_eq!(
            error
                .metadata()
                .get_bin(crate::matching_native_status::RAW_MESSAGE_MARKER)
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            b"load semaphore bucket d/s\xff/0: context deadline exceeded"
        );
        let error = waiter.await.unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(error.message(), "semaphore manager is not ready");
        assert!(registry.all_managers().is_empty());
        assert_eq!(metrics.load(Ordering::SeqCst), 0);

        store.release.notify_one();
        let acquired = service
            .acquire(request(b"1:z:r:3"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(acquired.outcome, 1);
        assert!(!Arc::ptr_eq(&old, &registry.all_managers()[0]));
        assert_eq!(store.scan_pages.load(Ordering::SeqCst), 2);
        assert_eq!(
            store
                .get_semaphore_ownership_by_token(
                    &bytes(b"d"),
                    &bytes(b"s\xff"),
                    0,
                    acquired.token_id,
                )
                .await
                .unwrap()
                .as_bytes(),
            b"1:z:r:3"
        );
        assert_eq!(
            service
                .acquire(request(b"1:w:r:4"))
                .await
                .unwrap()
                .into_inner()
                .outcome,
            2
        );
        service.stop_all();
    }

    #[test]
    fn ownership_error_retains_raw_bucket_detail() {
        let id = IdentifierBytes::new(bytes(b"d"), bytes(b"s\xff"), 3).unwrap();
        let error = not_owned("other", "self", &id);
        assert_eq!(error.code(), Code::Aborted);
        assert!(error.details().is_empty());
        let encoded = error
            .metadata()
            .get_bin(crate::matching_native_status::RAW_ERROR_DETAILS_MARKER)
            .unwrap()
            .to_bytes()
            .unwrap();
        let envelope = RawEnvelope::decode(encoded).unwrap();
        let detail = RawOwnershipDetail::decode(envelope.details[0].value.as_slice()).unwrap();
        assert_eq!(detail.bucket, b"d/s\xff/3");
    }
}
