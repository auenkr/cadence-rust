//! Go semaphore_manager_test.go parity cases. Scripted persistence deliberately
//! panics on unexpected reads/writes, so retry counts and request fields matter.
use super::*;
use cadence_persistence::{SemaphoreError, SemaphoreMetadata};
use std::collections::{HashSet, VecDeque};
use tokio::sync::Notify;

#[derive(Debug)]
struct ScriptStore {
    rows: Mutex<Option<Result<Vec<SemaphoreRow>, AcquireError>>>,
    grants: Mutex<VecDeque<Result<GrantOutcome, AcquireError>>>,
    reads: Mutex<VecDeque<Result<String, AcquireError>>>,
    writes: Mutex<Vec<(i32, String)>>,
    read_tokens: Mutex<Vec<i32>>,
    scans: std::sync::atomic::AtomicUsize,
    block_scan: bool,
    scanning: Notify,
    finish_scan: Notify,
}

impl ScriptStore {
    fn new(rows: Vec<SemaphoreRow>, grants: Vec<Result<GrantOutcome, AcquireError>>) -> Arc<Self> {
        Arc::new(Self {
            rows: Mutex::new(Some(Ok(rows))),
            grants: Mutex::new(grants.into()),
            reads: Mutex::new(VecDeque::new()),
            writes: Mutex::new(Vec::new()),
            read_tokens: Mutex::new(Vec::new()),
            scans: std::sync::atomic::AtomicUsize::new(0),
            block_scan: false,
            scanning: Notify::new(),
            finish_scan: Notify::new(),
        })
    }

    fn assert_done(&self) {
        assert!(
            self.grants.lock().unwrap().is_empty(),
            "missing grant calls"
        );
        assert!(
            self.reads.lock().unwrap().is_empty(),
            "missing confirming reads"
        );
    }
}

fn request(domain: &str, name: &str, bucket: i32) {
    assert_eq!((domain, name, bucket), ("uuid-d", "sem", 2));
}

#[async_trait::async_trait]
impl SemaphorePersistence for ScriptStore {
    async fn create_semaphore(
        &self,
        _: &str,
        _: &str,
        _: i32,
        _: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        panic!("unexpected metadata write")
    }
    async fn get_semaphore_metadata(
        &self,
        domain: &str,
        name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        assert_eq!((domain, name), ("uuid-d", "sem"));
        // Deliberately larger than the rows: absent rows must never become free slots.
        Ok(SemaphoreMetadata {
            size: 10,
            bucket_size: 10,
        })
    }
    async fn scan_semaphore_bucket(
        &self,
        domain: &str,
        name: &str,
        bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError> {
        request(domain, name, bucket);
        self.scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.block_scan {
            self.scanning.notify_one();
            self.finish_scan.notified().await;
        }
        self.rows
            .lock()
            .unwrap()
            .take()
            .expect("unexpected extra scan")
    }
    async fn get_semaphore_ownership_by_token(
        &self,
        domain: &str,
        name: &str,
        bucket: i32,
        token: i32,
    ) -> Result<String, AcquireError> {
        request(domain, name, bucket);
        self.read_tokens.lock().unwrap().push(token);
        self.reads
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected confirming read")
    }
    async fn grant_semaphore_token(
        &self,
        domain: &str,
        name: &str,
        bucket: i32,
        token: i32,
        owner: &str,
    ) -> Result<GrantOutcome, AcquireError> {
        request(domain, name, bucket);
        self.writes.lock().unwrap().push((token, owner.into()));
        self.grants
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected grant/retry")
    }
}

fn token(id: i32, holder: &str) -> SemaphoreRow {
    SemaphoreRow::Token {
        token_id: id,
        holder: holder.into(),
    }
}
fn owner(id: &str, held: i32) -> SemaphoreRow {
    SemaphoreRow::Owner {
        owner_id: id.into(),
        held_token: held,
    }
}
fn free(ids: &[i32]) -> Vec<SemaphoreRow> {
    ids.iter().map(|id| token(*id, "")).collect()
}
fn new_manager(store: &Arc<ScriptStore>) -> Arc<Manager> {
    Arc::new(Manager::new(
        Identifier::new("uuid-d", "sem", 2).unwrap(),
        store.clone(),
        DEFAULT_IDLE_TTL_NANOS,
    ))
}
async fn started(store: &Arc<ScriptStore>) -> Arc<Manager> {
    let manager = new_manager(store);
    manager.start().await.unwrap();
    manager
}
fn consistent(manager: &Manager) {
    let index = manager.index.lock().unwrap();
    assert_eq!(index.free_list.len(), index.free_index.len());
    for (position, token) in index.free_list.iter().enumerate() {
        assert_eq!(index.free_index.get(token), Some(&position));
    }
}
fn acquired(token_id: i32) -> AcquireResult {
    AcquireResult {
        outcome: SemaphoreAcquireOutcome::Acquired,
        token_id,
    }
}
fn no_slot() -> AcquireResult {
    AcquireResult {
        outcome: SemaphoreAcquireOutcome::NoSlot,
        token_id: 0,
    }
}
fn write_error() -> AcquireError {
    AcquireError::Internal("write timed out".into())
}

// TestStartBuildsTheFreeSetAndTheHeldIndexAcrossPages (180): aggregate
// row/index assertions; the page-token contract is tracked separately below.
#[tokio::test]
async fn matching_semaphore_start_builds_free_and_held_indexes() {
    let store = ScriptStore::new(
        vec![
            token(1, ""),
            token(2, "x"),
            token(3, ""),
            token(4, "y"),
            owner("x", 2),
            owner("y", 4),
        ],
        vec![],
    );
    let manager = started(&store).await;
    assert_eq!(manager.free_count(), 2);
    // Go: only the unheld slots are free, so the free set is exactly {1, 3}, not just its size.
    assert_eq!(manager.index.lock().unwrap().free_list, vec![1, 3]);
    assert_eq!(
        manager.index.lock().unwrap().held,
        HashMap::from([("x".into(), 2), ("y".into(), 4)])
    );
    consistent(&manager);
}

// TestStartKeepsAClaimedSlotOutOfTheFreeSet (206).
#[tokio::test]
async fn matching_semaphore_start_excludes_owner_claim_even_when_token_is_free() {
    let store = ScriptStore::new(vec![token(1, ""), token(2, ""), owner("x", 2)], vec![]);
    let manager = started(&store).await;
    let index = manager.index.lock().unwrap();
    assert_eq!(index.free_list, vec![1]);
    assert_eq!(index.held, HashMap::from([("x".into(), 2)]));
}

// TestStartSkipsRowsItCannotClassify (227), representable malformed variants.
#[tokio::test]
async fn matching_semaphore_start_skips_malformed_known_rows() {
    let store = ScriptStore::new(
        vec![token(1, ""), token(0, ""), owner("x", 0), owner("", 3)],
        vec![],
    );
    let manager = started(&store).await;
    assert_eq!(manager.index.lock().unwrap().free_list, vec![1]);
    assert!(manager.index.lock().unwrap().held.is_empty());
}

// TestStartLeavesStateUntouchedWhenTheScanFails (254). The Rust persistence
// boundary returns an aggregate scan, so a partial-page error is one Err here.
#[tokio::test]
async fn matching_semaphore_scan_failure_leaves_indexes_untouched() {
    let store = ScriptStore::new(vec![], vec![]);
    *store.rows.lock().unwrap() = Some(Err(AcquireError::Internal("scan failed".into())));
    let manager = new_manager(&store);
    assert_eq!(
        manager.start().await,
        Err(ManagerError::Internal("scan failed".into()))
    );
    assert_eq!(manager.free_count(), 0);
    assert!(manager.index.lock().unwrap().held.is_empty());
    assert!(!manager.is_running());
}

// TestSecondStartIsANoOp (282).
#[tokio::test]
async fn matching_semaphore_second_start_is_noop() {
    let store = ScriptStore::new(free(&[1, 2]), vec![]);
    let manager = started(&store).await;
    manager.start().await.unwrap();
    assert_eq!(manager.free_count(), 2);
    assert_eq!(store.scans.load(std::sync::atomic::Ordering::SeqCst), 1);
}

// TestAFailedLoadUnregistersTheManager (295); also registry
// TestSemaphoreRegistry_GetOrCreateHoldsNothingWhenTheBuildFails (129).
#[tokio::test]
async fn matching_semaphore_failed_load_unregisters_and_allows_fresh_retry() {
    let store = ScriptStore::new(vec![], vec![]);
    *store.rows.lock().unwrap() = Some(Err(AcquireError::Internal("scan failed".into())));
    let registry = Registry::new(store.clone());
    let id = Identifier::new("uuid-d", "sem", 2).unwrap();
    assert!(matches!(
        registry.get_or_create(id.clone()).await,
        Err(ManagerError::Internal(_))
    ));
    assert!(registry.by_identifier(&id).is_none());
    *store.rows.lock().unwrap() = Some(Ok(free(&[1, 2])));
    let manager = registry.get_or_create(id.clone()).await.unwrap();
    assert_eq!(manager.free_count(), 2);
    assert!(Arc::ptr_eq(
        &manager,
        &registry.get_or_create(id).await.unwrap()
    ));
    assert_eq!(store.scans.load(std::sync::atomic::Ordering::SeqCst), 2);
}

// TestAcquireBeforeTheBucketIsUsable (320), all three Go setups.
#[tokio::test]
async fn matching_semaphore_acquire_before_bucket_usable() {
    let store = ScriptStore::new(free(&[1, 2]), vec![]);
    let manager = new_manager(&store);
    manager.stop();
    assert_eq!(manager.acquire("a").await, Err(ManagerError::NotReady));

    let store = ScriptStore::new(vec![], vec![]);
    *store.rows.lock().unwrap() = Some(Err(AcquireError::Internal("scan failed".into())));
    let manager = new_manager(&store);
    assert!(manager.start().await.is_err());
    assert_eq!(manager.acquire("a").await, Err(ManagerError::NotReady));

    let mut store = ScriptStore::new(free(&[1, 2]), vec![]);
    Arc::get_mut(&mut store).unwrap().block_scan = true;
    let manager = new_manager(&store);
    let loading = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.start().await })
    };
    store.scanning.notified().await;
    manager.stop();
    store.finish_scan.notify_one();
    assert_eq!(loading.await.unwrap(), Err(ManagerError::NotReady));
    assert_eq!(manager.acquire("a").await, Err(ManagerError::NotReady));
    assert_eq!(manager.free_count(), 0);
}

// TestStartDoesNotWaitOnALoadInFlight (420).
#[tokio::test]
async fn matching_semaphore_second_start_does_not_wait_for_scan() {
    let mut store = ScriptStore::new(free(&[1]), vec![]);
    Arc::get_mut(&mut store).unwrap().block_scan = true;
    let manager = new_manager(&store);
    let loading = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.start().await })
    };
    store.scanning.notified().await;
    tokio::time::timeout(std::time::Duration::from_secs(5), manager.start())
        .await
        .unwrap()
        .unwrap();
    store.finish_scan.notify_one();
    loading.await.unwrap().unwrap();
    assert_eq!(store.scans.load(std::sync::atomic::Ordering::SeqCst), 1);
}

// TestAcquireHandsBackWhatTheGrantDecided (454).
#[tokio::test]
async fn matching_semaphore_acquire_passes_through_grant_result() {
    let store = ScriptStore::new(vec![token(1, ""), token(5, "a"), owner("a", 5)], vec![]);
    store.reads.lock().unwrap().push_back(Ok("a".into()));
    let manager = started(&store).await;
    assert_eq!(manager.acquire("a").await.unwrap(), acquired(5));
    assert_eq!(*store.read_tokens.lock().unwrap(), vec![5]);
    store.assert_done();
    let store = ScriptStore::new(vec![token(1, "x"), owner("x", 1)], vec![]);
    let manager = started(&store).await;
    assert_eq!(manager.acquire("a").await.unwrap(), no_slot());
    assert!(store.writes.lock().unwrap().is_empty());
}

// TestAcquireSurfacesAFailedGrant (496).
#[tokio::test]
async fn matching_semaphore_acquire_surfaces_failed_grant() {
    let store = ScriptStore::new(free(&[1]), vec![Err(write_error())]);
    let manager = started(&store).await;
    assert_eq!(
        manager.acquire("a").await,
        Err(ManagerError::Internal("write timed out".into()))
    );
    assert_eq!(manager.free_count(), 1);
    assert_eq!(store.writes.lock().unwrap().len(), 1);
    store.assert_done();
}

// TestGrantOnAFreeSlot (509).
#[tokio::test]
async fn matching_semaphore_grant_on_free_slot_records_request_and_hold() {
    let store = ScriptStore::new(free(&[7]), vec![Ok(GrantOutcome::Applied)]);
    let manager = started(&store).await;
    assert_eq!(manager.grant("a").await.unwrap(), acquired(7));
    assert_eq!(*store.writes.lock().unwrap(), vec![(7, "a".into())]);
    assert_eq!(manager.free_count(), 0);
    assert_eq!(
        manager.index.lock().unwrap().held,
        HashMap::from([("a".into(), 7)])
    );
    store.assert_done();
}

// TestGrantRetriesADifferentSlotWhenTheWriteSaysTaken (537).
#[tokio::test]
async fn matching_semaphore_grant_retries_different_slot_when_taken() {
    let store = ScriptStore::new(
        free(&[1, 2, 3]),
        vec![Ok(GrantOutcome::SlotTaken), Ok(GrantOutcome::Applied)],
    );
    let manager = started(&store).await;
    let result = manager.grant("a").await.unwrap();
    let writes = store.writes.lock().unwrap();
    assert_eq!(writes.len(), 2);
    assert_ne!(writes[0].0, writes[1].0);
    assert_eq!(result, acquired(writes[1].0));
    assert_eq!(manager.free_count(), 1);
    assert!(!manager
        .index
        .lock()
        .unwrap()
        .free_index
        .contains_key(&writes[0].0));
    consistent(&manager);
    store.assert_done();
}

// TestGrantGivesUpAfterMaxAttempts (562).
#[tokio::test]
async fn matching_semaphore_grant_gives_up_after_max_attempts() {
    assert_eq!(MAX_GRANT_ATTEMPTS, 3, "Go maxGrantAttempts");
    let store = ScriptStore::new(
        free(&(1..=10).collect::<Vec<_>>()),
        (0..3).map(|_| Ok(GrantOutcome::SlotTaken)).collect(),
    );
    let manager = started(&store).await;
    assert_eq!(manager.grant("a").await.unwrap(), no_slot());
    let writes = store.writes.lock().unwrap();
    assert_eq!(writes.len(), 3);
    assert_eq!(writes.iter().map(|w| w.0).collect::<HashSet<_>>().len(), 3);
    assert_eq!(manager.free_count(), 7);
    store.assert_done();
}

// TestGrantWhenTheWriteSaysTheOwnerAlreadyHolds (600), both Go subcases.
#[tokio::test]
async fn matching_semaphore_write_already_held_returns_reserved_slot() {
    for (held, count) in [(9, 3), (2, 2)] {
        let store = ScriptStore::new(free(&[1, 2, 3]), vec![Ok(GrantOutcome::AlreadyHeld(held))]);
        let manager = started(&store).await;
        assert_eq!(manager.grant("a").await.unwrap(), acquired(held));
        assert_eq!(manager.free_count(), count);
        assert_eq!(
            manager.index.lock().unwrap().held,
            HashMap::from([("a".into(), held)])
        );
        assert_eq!(store.writes.lock().unwrap().len(), 1);
        consistent(&manager);
        store.assert_done();
    }
}

// TestGrantRejectsAnAlreadyHeldWriteWithNoToken (651).
#[tokio::test]
async fn matching_semaphore_already_held_without_token_is_internal_error() {
    let store = ScriptStore::new(
        free(&[1, 2, 3]),
        vec![Ok(GrantOutcome::AlreadyHeld(0)), Ok(GrantOutcome::Applied)],
    );
    let manager = started(&store).await;
    assert!(
        matches!(manager.grant("a").await, Err(ManagerError::Internal(message)) if message.contains("already-held slot without a token"))
    );
    assert_eq!(manager.free_count(), 3);
    assert!(manager.index.lock().unwrap().held.is_empty());
    consistent(&manager);
    assert_eq!(
        manager.grant("a").await.unwrap().outcome,
        SemaphoreAcquireOutcome::Acquired
    );
    assert_eq!(store.writes.lock().unwrap().len(), 2);
    store.assert_done();
}

// TestGrantReturnsTheSlotWhenTheWriteFails (679).
#[tokio::test]
async fn matching_semaphore_failed_writes_do_not_drain_free_set() {
    let store = ScriptStore::new(
        free(&[1, 2, 3, 4, 5]),
        (0..3).map(|_| Err(write_error())).collect(),
    );
    let manager = started(&store).await;
    for i in 0..3 {
        assert_eq!(
            manager.grant(&format!("owner-{i}")).await,
            Err(ManagerError::Internal("write timed out".into()))
        );
        assert_eq!(manager.free_count(), 5);
        assert_eq!(store.writes.lock().unwrap().len(), i + 1);
        assert!(manager.index.lock().unwrap().held.is_empty());
        consistent(&manager);
    }
    store.assert_done();
}

// TestGrantWhenTheReverseIndexIsStale (716), all three Go subcases.
#[tokio::test]
async fn matching_semaphore_stale_reverse_index_free_taken_and_missing() {
    let missing =
        AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new("not found"));
    for (read, count) in [
        (Ok(String::new()), 3),
        (Ok("b".into()), 2),
        (Err(missing), 2),
    ] {
        let mut rows = free(&[1, 2, 3]);
        rows.push(owner("a", 5));
        let store = ScriptStore::new(rows, vec![Ok(GrantOutcome::Applied)]);
        store.reads.lock().unwrap().push_back(read);
        let manager = started(&store).await;
        let result = manager.grant("a").await.unwrap();
        assert_eq!(result.outcome, SemaphoreAcquireOutcome::Acquired);
        assert_eq!(manager.free_count(), count);
        assert_eq!(*store.read_tokens.lock().unwrap(), vec![5]);
        assert_eq!(
            manager.index.lock().unwrap().held.get("a"),
            Some(&result.token_id)
        );
        consistent(&manager);
        store.assert_done();
    }
}

// TestGrantSurfacesAConfirmingReadFailure (772).
#[tokio::test]
async fn matching_semaphore_confirming_read_failure_is_preserved() {
    let store = ScriptStore::new(vec![token(1, ""), owner("a", 5)], vec![]);
    store
        .reads
        .lock()
        .unwrap()
        .push_back(Err(AcquireError::Internal("cassandra unavailable".into())));
    let manager = started(&store).await;
    assert_eq!(
        manager.grant("a").await,
        Err(ManagerError::Internal("cassandra unavailable".into()))
    );
    assert_eq!(manager.free_count(), 1);
    assert_eq!(manager.index.lock().unwrap().held.get("a"), Some(&5));
    assert!(store.writes.lock().unwrap().is_empty());
    store.assert_done();
}

// TestDropStaleHold (786), both Go subcases.
#[tokio::test]
async fn matching_semaphore_drop_stale_hold_only_returns_proven_free_slot() {
    for (still_free, count) in [(false, 2), (true, 3)] {
        let store = ScriptStore::new(
            vec![token(1, ""), token(2, ""), token(5, "a"), owner("a", 5)],
            vec![],
        );
        let manager = started(&store).await;
        manager.drop_stale_hold("a", 5, still_free);
        assert!(manager.index.lock().unwrap().held.is_empty());
        assert_eq!(manager.free_count(), count);
        consistent(&manager);
    }
}

// TestFreeSetSurvivesConcurrentReserveAndUnreserve (854).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_semaphore_free_set_survives_concurrent_reserve_unreserve() {
    let store = ScriptStore::new(free(&(1..=8).collect::<Vec<_>>()), vec![]);
    let manager = started(&store).await;
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let manager = &manager;
            scope.spawn(move || {
                for _ in 0..50 {
                    if let Some(id) = manager.reserve() {
                        manager.unreserve(id);
                    }
                }
            });
        }
    });
    assert_eq!(manager.free_count(), 8);
    consistent(&manager);
}

// TestConcurrentGrantsHandOutDistinctSlots (880).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_semaphore_concurrent_grants_are_distinct() {
    let store = ScriptStore::new(
        free(&(1..=20).collect::<Vec<_>>()),
        (0..20).map(|_| Ok(GrantOutcome::Applied)).collect(),
    );
    let manager = started(&store).await;
    let mut tasks = Vec::new();
    for i in 0..20 {
        let manager = manager.clone();
        tasks.push(tokio::spawn(async move {
            manager.grant(&format!("owner-{i}")).await.unwrap()
        }));
    }
    let mut seen = HashSet::new();
    for task in tasks {
        let result = task.await.unwrap();
        assert_eq!(result.outcome, SemaphoreAcquireOutcome::Acquired);
        assert!(seen.insert(result.token_id), "duplicate slot");
    }
    assert_eq!(manager.free_count(), 0);
    consistent(&manager);
    store.assert_done();
}

// TestLifecycleUnderConcurrentGrants (919). State/grant results depend on
// scheduling, as in Go; only consistency and completion are asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_semaphore_lifecycle_under_concurrent_grants() {
    for _ in 0..10 {
        let store = ScriptStore::new(
            free(&(1..=8).collect::<Vec<_>>()),
            (0..8).map(|_| Ok(GrantOutcome::Applied)).collect(),
        );
        let manager = new_manager(&store);
        let barrier = Arc::new(tokio::sync::Barrier::new(10));
        let mut tasks = Vec::new();
        for action in 0..10 {
            let manager = manager.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                match action {
                    0 => {
                        let _ = manager.start().await;
                    }
                    1 => manager.stop(),
                    _ => {
                        let _ = manager.grant(&format!("owner-{action}")).await;
                    }
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        consistent(&manager);
    }
}

// TestAcquireKeepsAManagerLoaded (999), using Rust's explicit idle sweep.
#[tokio::test]
async fn matching_semaphore_acquire_refreshes_idle_time() {
    let store = ScriptStore::new(vec![token(1, "x"), owner("x", 1)], vec![]);
    let ttl = 10_000_000_000;
    let registry = Registry::new(store).with_idle_ttl(ttl);
    let id = Identifier::new("uuid-d", "sem", 2).unwrap();
    let manager = registry.get_or_create(id.clone()).await.unwrap();
    let before = now_nanos();
    // Pin the old mark at half the TTL ago without wall-clock sleeping.
    manager.liveness.mark_alive(before - ttl / 2);
    assert_eq!(manager.acquire("a").await.unwrap(), no_slot());
    registry.sweep_idle(before + ttl / 2);
    assert!(registry.by_identifier(&id).is_some());
    registry.sweep_idle(now_nanos() + 2 * ttl);
    assert!(registry.by_identifier(&id).is_none());
    assert!(!manager.is_running());
}

// Explicit gaps: these are deliberately failing tests when run with
// --include-ignored, never empty passing placeholders.
#[test]
#[ignore = "parity gap: Rust Manager::new clamps invalid TTL and lacks Go parameter validation. Go go/service/matching/semaphore/semaphore_manager_test.go:136"]
fn matching_semaphore_new_manager_validates_params() {
    let store = ScriptStore::new(vec![], vec![]);
    let invalid = Manager::new(
        Identifier {
            domain_id: String::new(),
            semaphore_name: String::new(),
            bucket: -1,
        },
        store,
        0,
    );
    assert!(
        !invalid.is_alive(now_nanos()),
        "Go rejects invalid parameters without constructing a manager"
    );
    panic!("Go requires BadRequest and no manager, Rust constructor is infallible");
}

#[test]
#[ignore = "parity gap: scan returns Vec without page tokens; cannot assert page threading. Go go/service/matching/semaphore/semaphore_manager_test.go:180"]
fn matching_semaphore_scan_threads_page_tokens() {
    panic!("SemaphorePersistence has no page-token input/output; Go requires page-1 then page-2");
}

#[test]
#[ignore = "parity gap: SemaphoreRow cannot represent nil/unknown rows; Cassandra rejects unknown row types. Go go/service/matching/semaphore/semaphore_manager_test.go:227"]
fn matching_semaphore_scan_skips_nil_and_unknown_rows() {
    panic!("Go skips nil and unknown rows; typed Rust scan cannot inject them");
}

#[tokio::test]
#[ignore = "parity gap: Manager acquire has no deadline/context and waits indefinitely during startup. Go go/service/matching/semaphore/semaphore_manager_test.go:389"]
async fn matching_semaphore_acquire_honors_deadline_while_starting() {
    let mut store = ScriptStore::new(free(&[1]), vec![]);
    Arc::get_mut(&mut store).unwrap().block_scan = true;
    let manager = new_manager(&store);
    let loading = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.start().await })
    };
    store.scanning.notified().await;
    // An outer watchdog is not a port of Go's context. It only bounds this
    // known failing test: acquire itself must return the caller's error.
    let result =
        tokio::time::timeout(std::time::Duration::from_millis(20), manager.acquire("a")).await;
    store.finish_scan.notify_one();
    loading.await.unwrap().unwrap();
    assert!(
        result.is_ok(),
        "acquire never returned a deadline error itself"
    );
    panic!("Rust has no deadline error variant");
}

#[tokio::test]
#[ignore = "parity gap: grant has no cancellation/context check between attempts. Go go/service/matching/semaphore/semaphore_manager_test.go:577"]
async fn matching_semaphore_grant_stops_retrying_at_deadline() {
    let store = ScriptStore::new(
        free(&[1, 2, 3, 4, 5]),
        (0..3).map(|_| Ok(GrantOutcome::SlotTaken)).collect(),
    );
    let manager = started(&store).await;
    // Go cancels context when the first write returns SlotTaken. There is
    // no Rust context input to cancel; the manager continues all 3 writes.
    let result = manager.grant("a").await;
    assert!(
        result.is_err(),
        "Go returns context.Canceled instead of NoSlot"
    );
    assert_eq!(store.writes.lock().unwrap().len(), 1);
    assert_eq!(manager.free_count(), 4);
}

#[test]
#[ignore = "parity gap: GrantOutcome is exhaustive and cannot inject Go Unknown outcome. Go go/service/matching/semaphore/semaphore_manager_test.go:699"]
fn matching_semaphore_grant_rejects_unknown_write_outcome() {
    panic!(
        "Go requires InternalServiceError and unreserve; Rust trait cannot express unknown outcome"
    );
}

#[tokio::test]
#[ignore = "parity gap: Manager::stop has no registry callback; direct stop does not unregister itself. Go go/service/matching/semaphore/semaphore_manager_test.go:1028"]
async fn matching_semaphore_late_stop_unregisters_only_itself() {
    let store = ScriptStore::new(free(&[1]), vec![]);
    let registry = Registry::new(store.clone());
    let id = Identifier::new("uuid-d", "sem", 2).unwrap();
    let old = registry.get_or_create(id.clone()).await.unwrap();
    assert!(registry.unregister(&old));
    *store.rows.lock().unwrap() = Some(Ok(free(&[1])));
    let replacement = registry.get_or_create(id.clone()).await.unwrap();
    old.stop();
    assert!(Arc::ptr_eq(
        &replacement,
        &registry.by_identifier(&id).unwrap()
    ));
    replacement.stop();
    assert!(
        registry.by_identifier(&id).is_none(),
        "Go Stop invokes onStopFn"
    );
}
