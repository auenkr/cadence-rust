//! Go parity ports of test functions 19 through 22 in
//! `go/service/matching/handler/engine_test.go`, in file order:
//! `TestGetOrCreateSemaphoreManager` (eight subtests),
//! `TestUnloadSemaphoreManager`, `TestStopSemaphoreManagers`, and
//! `TestAcquireSemaphore` (four subtests).
//!
//! Rust has no `matchingEngineImpl`. The registry cases run on the public
//! `matching_semaphore::Registry` over a scripted `SemaphorePersistence`, which
//! plays the role of Go's `MockSemaphoreTokenManager`. The handler cases run
//! through the public `MatchingApi::acquire_semaphore` on `Matching`, with the
//! same store installed as `AppState::semaphore_persistence`. Go's engine
//! shutdown channel, `Engine.Stop`, and the cancellable startup wait have no
//! public Rust seam, so those cases are `#[ignore]` with the gap named.
//!
//! Go's `testSelfHost` maps to Rust membership identity `local` and
//! `testOtherHost` to `remote`. The domain ID is a UUID, because the handler
//! parses it. Owner IDs use the canonical length-prefixed encoding.
use cadence_api::{
    admin::{DynamicConfigFilter, DynamicConfigValue},
    enums::EncodingType,
    types::{DataBlob, DomainInfo},
};
use cadence_common::{membership::Host, semaphore::Owner};
use cadence_matching::{
    grpc_matching::Matching,
    matching_membership::Membership,
    matching_semaphore::{Identifier, ManagerError, Registry},
};
use cadence_persistence::semaphores::{
    AcquireError, GrantOutcome, SemaphoreError, SemaphoreMetadata, SemaphorePersistence,
    SemaphoreRow,
};
use cadence_proto::matching as m;
use cadence_service_core::handlers::AppState;
use m::matching_api_server::MatchingApi;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tonic::{metadata::{Ascii, MetadataValue}, Code, Request};
use uuid::Uuid;

const DOMAIN: &str = "test-domain";
const SEMAPHORE: &str = "sem-1";
const ENABLE_SEMAPHORE: &str = "system.enableDistributedSemaphore";

fn domain_id() -> Uuid {
    Uuid::from_u128(1)
}

/// Go `mustNewSemaphoreIdentifier`.
fn identifier(bucket: i32) -> Identifier {
    Identifier::new(&domain_id().to_string(), SEMAPHORE, bucket).unwrap()
}

/// Go `testSemaphoreOwnerID`, built through the encoder.
fn owner_id() -> String {
    Owner {
        workflow_id: "wf-1".into(),
        run_id: "run-1".into(),
        hold_id: 7,
    }
    .encode()
}

fn add_domain(state: &AppState, name: &str, id: Uuid) {
    state
        .domains
        .create(
            DomainInfo {
                name: name.into(),
                uuid: id.to_string(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
}

fn bool_config(value: bool, domain: Option<&str>) -> DynamicConfigValue {
    let blob = |value: serde_json::Value| {
        Some(DataBlob::new(
            EncodingType::JSON,
            serde_json::to_vec(&value).unwrap(),
        ))
    };
    DynamicConfigValue {
        value: blob(serde_json::json!(value)),
        filters: domain
            .into_iter()
            .map(|name| DynamicConfigFilter {
                name: "domainName".into(),
                value: blob(serde_json::json!(name)),
            })
            .collect(),
    }
}

/// `count` free token slots, numbered from 1 as Go's `expectOneScan` does.
fn free_tokens(count: i32) -> Vec<SemaphoreRow> {
    (1..=count)
        .map(|token_id| SemaphoreRow::Token {
            token_id,
            holder: String::new(),
        })
        .collect()
}

/// Go `MockSemaphoreTokenManager`. Counts scans, returns scripted scan results
/// (an empty bucket once the script runs out), and can hold the scan of one
/// bucket open until the test releases it.
#[derive(Debug)]
struct SemaphoreStore {
    scans: AtomicUsize,
    scan_results: Mutex<VecDeque<Result<Vec<SemaphoreRow>, AcquireError>>>,
    held_bucket: i32,
    holding: AtomicBool,
    scan_started: Notify,
    release_scan: Notify,
}

impl SemaphoreStore {
    fn new() -> Arc<Self> {
        Self::build(-1, false)
    }

    /// The scan of `bucket` blocks until `release_hold` is called.
    fn holding(bucket: i32) -> Arc<Self> {
        Self::build(bucket, true)
    }

    fn build(held_bucket: i32, holding: bool) -> Arc<Self> {
        Arc::new(Self {
            scans: AtomicUsize::new(0),
            scan_results: Mutex::new(VecDeque::new()),
            held_bucket,
            holding: AtomicBool::new(holding),
            scan_started: Notify::new(),
            release_scan: Notify::new(),
        })
    }

    fn push_scan(&self, result: Result<Vec<SemaphoreRow>, AcquireError>) {
        self.scan_results.lock().unwrap().push_back(result);
    }

    fn scan_count(&self) -> usize {
        self.scans.load(Ordering::SeqCst)
    }

    async fn wait_scan_started(&self) {
        self.scan_started.notified().await;
    }

    fn release_hold(&self) {
        self.holding.store(false, Ordering::SeqCst);
        self.release_scan.notify_one();
    }
}

#[async_trait::async_trait]
impl SemaphorePersistence for SemaphoreStore {
    async fn create_semaphore(
        &self,
        _domain_id: &str,
        _name: &str,
        _size: i32,
        _bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        Err(SemaphoreError::UnsupportedBackend)
    }

    async fn get_semaphore_metadata(
        &self,
        _domain_id: &str,
        _name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        Ok(SemaphoreMetadata {
            size: 10,
            bucket_size: 3,
        })
    }

    async fn scan_semaphore_bucket(
        &self,
        _domain_id: &str,
        _name: &str,
        bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        if bucket == self.held_bucket && self.holding.load(Ordering::SeqCst) {
            self.scan_started.notify_one();
            self.release_scan.notified().await;
        }
        let next = self.scan_results.lock().unwrap().pop_front();
        next.unwrap_or(Ok(Vec::new()))
    }

    async fn get_semaphore_ownership_by_token(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _token_id: i32,
    ) -> Result<String, AcquireError> {
        Err(AcquireError::Internal("not scripted".into()))
    }

    async fn grant_semaphore_token(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _token_id: i32,
        _owner_id: &str,
    ) -> Result<GrantOutcome, AcquireError> {
        Ok(GrantOutcome::Applied)
    }
}

fn install_store(state: &mut AppState, store: &Arc<SemaphoreStore>) {
    let persistence: Arc<dyn SemaphorePersistence> = store.clone();
    state.semaphore_persistence = Some(persistence);
}

/// Go `newSemaphoreEngine` for the handler path: the domain exists and the
/// flag is set for it, with every bucket scanned from `store`.
fn semaphore_matching(store: &Arc<SemaphoreStore>, enabled: bool) -> Matching {
    let mut state = AppState::default();
    add_domain(&state, DOMAIN, domain_id());
    state
        .admin
        .dynamic_config
        .lock()
        .unwrap()
        .update(false, ENABLE_SEMAPHORE, vec![bool_config(enabled, Some(DOMAIN))]);
    install_store(&mut state, store);
    Matching::new(state)
}

fn acquire_request(bucket: i32, owner: &str) -> Request<m::AcquireSemaphoreRequest> {
    Request::new(m::AcquireSemaphoreRequest {
        domain_id: domain_id().to_string(),
        semaphore_name: SEMAPHORE.into(),
        bucket,
        owner_id: owner.into(),
    })
}

// ---------------------------------------------------------------------------
// TestGetOrCreateSemaphoreManager
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_or_create_semaphore_manager_second_request_reuses_built_manager() {
    // A bucket is built once, so the second request costs no scan.
    let store = SemaphoreStore::new();
    store.push_scan(Ok(free_tokens(3)));
    let registry = Registry::new(store.clone());

    let first = registry.get_or_create(identifier(0)).await.unwrap();
    let second = registry.get_or_create(identifier(0)).await.unwrap();

    assert!(
        Arc::ptr_eq(&first, &second),
        "the second call must reuse the registered manager"
    );
    assert_eq!(store.scan_count(), 1);
}

#[ignore = "parity gap: a request cancelled during the startup load leaves the Rust manager in Starting; Manager::start returns Ok for Starting without loading, so the next get_or_create never runs the scan and the bucket stays unloaded"]
#[tokio::test]
async fn get_or_create_semaphore_manager_starts_unstarted_registered_manager() {
    // The first request is cut off while its load is in flight, like a handler
    // that never reaches Start. The next request must start the manager.
    let store = SemaphoreStore::holding(0);
    let registry = Registry::new(store.clone());
    let cut_off = tokio::time::timeout(
        Duration::from_millis(50),
        registry.get_or_create(identifier(0)),
    )
    .await;
    assert!(cut_off.is_err(), "the first request must be cut off mid-load");
    store.release_hold();

    let got = registry.get_or_create(identifier(0)).await.unwrap();

    assert_eq!(got.identifier(), &identifier(0));
    assert_eq!(
        store.scan_count(),
        2,
        "the next request must start the registered manager"
    );
}

#[ignore = "parity gap: Rust Matching has no engine shutdown state; acquire_semaphore has no Go-style check that refuses to load a new bucket while shutting down, so the bucket is always scanned and registered"]
#[tokio::test]
async fn get_or_create_semaphore_manager_refused_while_engine_shuts_down() {}

#[tokio::test]
async fn get_or_create_semaphore_manager_domain_has_semaphores_disabled() {
    // Costs nothing: the flag is checked before any bucket is built, so no
    // scan happens for a disabled domain.
    let store = SemaphoreStore::new();
    let matching = semaphore_matching(&store, false);

    let status = MatchingApi::acquire_semaphore(&matching, acquire_request(0, &owner_id()))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "distributed semaphore is not enabled for this domain"
    );
    assert_eq!(store.scan_count(), 0, "no bucket may load for a disabled domain");
}

#[tokio::test]
async fn get_or_create_semaphore_manager_bucket_is_owned_by_another_host() {
    // The error names that host, so the caller can go there instead of retrying here.
    let store = SemaphoreStore::new();
    let matching = semaphore_matching(&store, true).with_membership(Membership::new(
        "local",
        vec![Host {
            address: "remote:7935".into(),
            identity: "remote".into(),
            grpc_address: "127.0.0.1:1".into(),
        }],
    ));

    let status = MatchingApi::acquire_semaphore(&matching, acquire_request(0, &owner_id()))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Aborted);
    assert!(
        status
            .message()
            .contains("OwnedBy: remote, Me: local"),
        "the error must name the owning host: {}",
        status.message()
    );
    assert_eq!(store.scan_count(), 0, "a bucket owned elsewhere must not load here");
}

#[tokio::test]
async fn get_or_create_semaphore_manager_failed_start_is_not_left_registered() {
    // Its Acquire answers NotReady with nothing to clear it, so leaving it in
    // place would wedge the bucket until the host restarted.
    let store = SemaphoreStore::new();
    store.push_scan(Err(AcquireError::Internal("cassandra unavailable".into())));
    let registry = Registry::new(store.clone());

    let err = registry
        .get_or_create(identifier(0))
        .await
        .err()
        .expect("a failing startup scan must fail the request");

    assert_eq!(err, ManagerError::Internal("cassandra unavailable".into()));
    assert!(
        registry.by_identifier(&identifier(0)).is_none(),
        "a manager that cannot start must not stay registered"
    );

    // The next request builds a fresh one, so a passing outage is recoverable.
    let retry = registry.get_or_create(identifier(0)).await.unwrap();
    assert_eq!(retry.identifier(), &identifier(0));
    assert_eq!(store.scan_count(), 2);
}

#[tokio::test]
async fn get_or_create_semaphore_manager_domain_lookup_fails() {
    // The domain is not registered, so the flag cannot be read and no bucket
    // is built. The lookup failure comes back as its own status.
    let store = SemaphoreStore::new();
    let mut state = AppState::default();
    install_store(&mut state, &store);
    let matching = Matching::new(state);

    let status = MatchingApi::acquire_semaphore(&matching, acquire_request(0, &owner_id()))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(store.scan_count(), 0);
}

#[tokio::test]
async fn get_or_create_semaphore_manager_load_runs_outside_creation_lock() {
    // One slow load must not block other buckets. The scan for bucket 0 is held
    // open until bucket 1 has been built, which can only finish if the two
    // loads are independent.
    let store = SemaphoreStore::holding(0);
    let registry = Registry::new(store.clone());
    let slow_registry = registry.clone();
    let slow = tokio::spawn(async move {
        slow_registry
            .get_or_create(identifier(0))
            .await
            .map(|_| ())
    });
    // Wait until the slow load is in flight, so the second creation really
    // overlaps it rather than slipping in first by luck.
    store.wait_scan_started().await;

    let second = tokio::time::timeout(
        Duration::from_secs(5),
        registry.get_or_create(identifier(1)),
    )
    .await
    .expect("the second bucket must not wait behind the slow load")
    .expect("the second bucket must load");
    assert_eq!(second.identifier(), &identifier(1));

    store.release_hold();
    slow.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// TestUnloadSemaphoreManager
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unload_semaphore_manager_stops_it_and_next_request_builds_fresh() {
    // Unloading takes a manager out of the registry and stops it, so the next
    // request builds a fresh one instead of finding the retired manager.
    let store = SemaphoreStore::new();
    let registry = Registry::new(store.clone());
    let mgr = registry.get_or_create(identifier(0)).await.unwrap();

    assert!(registry.unregister(&mgr));
    mgr.stop();
    assert!(registry.all_managers().is_empty());

    assert_eq!(
        mgr.acquire("owner-1").await.err(),
        Some(ManagerError::NotReady),
        "an unloaded manager must stop serving"
    );

    let fresh = registry.get_or_create(identifier(0)).await.unwrap();
    assert!(!Arc::ptr_eq(&mgr, &fresh), "the next request must build a new manager");
    assert_eq!(store.scan_count(), 2);
}

// ---------------------------------------------------------------------------
// TestStopSemaphoreManagers
// ---------------------------------------------------------------------------

#[ignore = "parity gap: Rust has no shutdown path that stops every semaphore manager; Matching::maintain_membership_until stops only buckets the ring no longer assigns to this host, and Registry exposes no stop-all"]
#[tokio::test]
async fn stop_semaphore_managers_stops_every_bucket_on_engine_stop() {}

// ---------------------------------------------------------------------------
// TestAcquireSemaphore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acquire_semaphore_free_slot_is_granted_and_named_in_response() {
    let store = SemaphoreStore::new();
    store.push_scan(Ok(free_tokens(3)));
    let matching = semaphore_matching(&store, true);

    let resp = MatchingApi::acquire_semaphore(&matching, acquire_request(0, &owner_id()))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.outcome, 1, "a free slot is Acquired");
    assert!(
        resp.token_id >= 1,
        "slot ids start at 1, so 0 would mean no slot"
    );
    assert_eq!(store.scan_count(), 1);
}

#[tokio::test]
async fn acquire_semaphore_full_bucket_answers_no_slot_rather_than_error() {
    // Nothing is wrong: the caller asked and the answer is no. An empty bucket
    // is reported as NoSlot, not as an error.
    let store = SemaphoreStore::new();
    let matching = semaphore_matching(&store, true);

    let resp = MatchingApi::acquire_semaphore(&matching, acquire_request(0, &owner_id()))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.outcome, 2, "a full bucket is NoSlot");
    assert_eq!(resp.token_id, 0);
    assert_eq!(store.scan_count(), 1);
}

#[tokio::test]
async fn acquire_semaphore_undecodable_owner_is_refused_before_any_store_call() {
    // Reconciliation reads the stored owner id to attribute a held slot to a
    // workflow, so one it cannot decode names a hold nothing could recover.
    let store = SemaphoreStore::new();
    let matching = semaphore_matching(&store, true);

    let status = MatchingApi::acquire_semaphore(&matching, acquire_request(0, "not-an-owner-id"))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(store.scan_count(), 0);
}

#[ignore = "parity gap: Rust Manager::acquire waits for the startup load with no deadline, and the handler applies the gRPC deadline only after acquire returns, so a request arriving during a slow load waits until that load ends"]
#[tokio::test]
async fn acquire_semaphore_request_during_load_gives_up_at_its_own_deadline() {
    let store = SemaphoreStore::holding(0);
    let matching = semaphore_matching(&store, true);

    // The first request triggers the load and is held inside it.
    let loading = {
        let matching = matching.clone();
        tokio::spawn(async move {
            MatchingApi::acquire_semaphore(&matching, acquire_request(0, &owner_id())).await
        })
    };
    store.wait_scan_started().await;

    // The second request carries a 20ms gRPC deadline and must not wait on the load.
    let mut request = acquire_request(0, &owner_id());
    let timeout = "20m"
        .parse::<MetadataValue<Ascii>>()
        .unwrap();
    request.metadata_mut().insert("grpc-timeout", timeout);
    let status = tokio::time::timeout(
        Duration::from_secs(5),
        MatchingApi::acquire_semaphore(&matching, request),
    )
    .await
    .expect("the second request must give up at its own deadline")
    .unwrap_err();

    assert_eq!(status.code(), Code::Internal);
    assert_eq!(status.message(), "context deadline exceeded");

    store.release_hold();
    loading.await.unwrap().unwrap();
}
