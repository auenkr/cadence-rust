//! Go `service/matching/semaphore`: the per-bucket manager, its registry, and
//! the identifier. A semaphore of `size` slots is split into
//! `ceil(size/bucket_size)` buckets; each bucket is served by one manager on
//! one host.
//!
//! The manager keeps a free-set (available token ids) and a reverse index
//! (owner -> token) as a cache over the token rows. Every grant settles with a
//! conditional write, so a stale cache costs a retry, never a wrong grant.
//! Startup scans the bucket to rebuild both indexes; an idle bucket unloads
//! itself after `matching.semaphoreIdleTime` (default 5m).
//!
//! Single process: one host owns every bucket, so the membership ring check
//! that routes buckets to hosts is a self-check.

use cadence_api::enums::SemaphoreAcquireOutcome;
use cadence_common::liveness::Liveness;
use cadence_persistence::{
    AcquireError, GrantOutcome, SemaphorePersistence, SemaphoreRow,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

/// Go `maxGrantAttempts`: cap on conditional writes one acquire may make.
pub const MAX_GRANT_ATTEMPTS: usize = 3;
/// Go `matching.semaphoreIdleTime` default: 5 minutes in nanoseconds.
pub const DEFAULT_IDLE_TTL_NANOS: i64 = 5 * 60 * 1_000_000_000;

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}

/// Go `semaphore.Identifier`: one bucket of one semaphore. Used as a map key,
/// so it is comparable and hashed by its three fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier {
    pub domain_id: String,
    pub semaphore_name: String,
    pub bucket: i32,
}

impl Identifier {
    /// Go `NewIdentifier`: reject what persistence would reject anyway.
    pub fn new(
        domain_id: &str,
        semaphore_name: &str,
        bucket: i32,
    ) -> Result<Self, ManagerError> {
        let id = Self {
            domain_id: domain_id.into(),
            semaphore_name: semaphore_name.into(),
            bucket,
        };
        id.validate()?;
        Ok(id)
    }

    /// Go `Identifier.validate`, shared by `new` and `Manager::new` so a
    /// struct literal cannot skip the checks.
    fn validate(&self) -> Result<(), ManagerError> {
        if self.domain_id.is_empty() {
            return Err(ManagerError::BadRequest("domainID is required".into()));
        }
        if self.semaphore_name.is_empty() {
            return Err(ManagerError::BadRequest("semaphoreName is required".into()));
        }
        if self.bucket < 0 {
            return Err(ManagerError::BadRequest(format!(
                "bucket must not be negative, got {}",
                self.bucket
            )));
        }
        Ok(())
    }
    /// Go `Identifier.String()`: `domain/semaphore/bucket`, for logs.
    pub fn to_log_string(&self) -> String {
        format!("{}/{}/{}", self.domain_id, self.semaphore_name, self.bucket)
    }
    /// Go `Identifier.RingKey()`: `domain_semaphore_bucket`, for the ring.
    pub fn ring_key(&self) -> String {
        format!("{}_{}_{}", self.domain_id, self.semaphore_name, self.bucket)
    }
}

/// Manager-level errors, mirroring Go's typed matching errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagerError {
    /// `types.BadRequestError`.
    BadRequest(String),
    /// `ErrSemaphoreDisabled`: distributed semaphores are off for the domain.
    Disabled,
    /// `ErrNotReady`: the manager is loading, stopped, or unloaded.
    NotReady,
    /// `context.DeadlineExceeded`: the caller's deadline passed while waiting
    /// for startup or before the next grant attempt.
    Deadline,
    /// `types.EntityNotExistsError`: the semaphore itself is missing.
    NotFound(String),
    /// `types.InternalServiceError`.
    Internal(String),
    /// `types.ServiceBusyError` returned by the Go persistence limiter.
    ServiceBusy(String),
}

impl From<AcquireError> for ManagerError {
    fn from(error: AcquireError) -> Self {
        match error {
            AcquireError::NotFound(error) => ManagerError::NotFound(error.message),
            AcquireError::Invalid(message) => ManagerError::BadRequest(message),
            AcquireError::Internal(message) => ManagerError::Internal(message),
            AcquireError::Throttled(message) => ManagerError::ServiceBusy(message),
        }
    }
}

/// One acquire's answer. `token_id` names a slot only when acquired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquireResult {
    pub outcome: SemaphoreAcquireOutcome,
    pub token_id: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum State {
    #[default]
    Created,
    Starting,
    Running,
    Stopped,
}

#[derive(Default)]
struct Index {
    state: State,
    /// Ids of available tokens.
    free_list: Vec<i32>,
    /// Reverse map id -> position in `free_list`.
    free_index: HashMap<i32, usize>,
    /// owner_id -> token_id, mirroring the bucket's owner rows.
    held: HashMap<String, i32>,
}

/// One bucket's manager, mirroring Go's `semaphoreManagerImpl`.
pub struct Manager {
    id: Identifier,
    store: Arc<dyn SemaphorePersistence>,
    liveness: Liveness,
    index: Mutex<Index>,
    /// Go `startupDoneCh`: set once the startup load ended (or the manager stopped).
    startup_done: tokio::sync::watch::Sender<bool>,
}

impl Manager {
    /// Go `NewManager`: a bad identifier or a non-positive idle TTL is a
    /// BadRequest, and no manager is built.
    fn new(
        id: Identifier,
        store: Arc<dyn SemaphorePersistence>,
        idle_ttl_nanos: i64,
    ) -> Result<Self, ManagerError> {
        id.validate()?;
        // Go rejects this too: liveness builds a ticker from the TTL, and a non-positive one panics.
        if idle_ttl_nanos <= 0 {
            return Err(ManagerError::BadRequest(
                "ManagerParams.IdleTTL must be positive".into(),
            ));
        }
        Ok(Self {
            id,
            store,
            liveness: Liveness::new(now_nanos(), idle_ttl_nanos),
            index: Mutex::new(Index::default()),
            startup_done: tokio::sync::watch::channel(false).0,
        })
    }

    pub fn identifier(&self) -> &Identifier {
        &self.id
    }

    /// Go `Manager.Start`: only the caller that finds the manager unstarted
    /// runs the scan; an already starting/running manager is a no-op. A
    /// stopped manager is not ready.
    pub async fn start(&self) -> Result<(), ManagerError> {
        {
            let mut index = self.index.lock().expect("semaphore manager lock");
            match index.state {
                State::Created => index.state = State::Starting,
                State::Starting | State::Running => return Ok(()),
                State::Stopped => return Err(ManagerError::NotReady),
            }
        }
        let loaded = self.load().await;
        if loaded.is_err() {
            self.stop();
        }
        // Go defers markStartupDone: waiters are released however load returns.
        self.startup_done.send_replace(true);
        loaded
    }

    async fn load(&self) -> Result<(), ManagerError> {
        self.store.get_semaphore_metadata(
            &self.id.domain_id,
            &self.id.semaphore_name,
        ).await?;
        let rows = self.store.scan_semaphore_bucket(
            &self.id.domain_id,
            &self.id.semaphore_name,
            self.id.bucket,
        ).await?;
        // Go builds the free set only from rows returned by the scan. Metadata
        // does not prove that an absent token row exists or is unheld.
        let mut free: HashMap<i32, ()> = HashMap::new();
        let mut held: HashMap<String, i32> = HashMap::new();
        for row in rows {
            match row {
                SemaphoreRow::Token { token_id, holder } => {
                    if holder.is_empty() && token_id > 0 {
                        free.insert(token_id, ());
                    } else {
                        free.remove(&token_id);
                    }
                }
                SemaphoreRow::Owner {
                    owner_id,
                    held_token,
                } => {
                    if !owner_id.is_empty() && held_token > 0 {
                        held.insert(owner_id, held_token);
                    }
                }
            }
        }
        // A slot an owner row claims is not free, whatever its token row said.
        for token_id in held.values() {
            free.remove(token_id);
        }
        let mut free_list: Vec<i32> = free.into_keys().collect();
        // Go's map order is random; sort so a scan is deterministic.
        free_list.sort_unstable();
        let free_index = free_list
            .iter()
            .enumerate()
            .map(|(position, token_id)| (*token_id, position))
            .collect();
        let mut index = self.index.lock().expect("semaphore manager lock");
        if index.state == State::Stopped {
            return Err(ManagerError::NotReady);
        }
        index.free_list = free_list;
        index.free_index = free_index;
        index.held = held;
        index.state = State::Running;
        self.liveness.mark_alive(now_nanos());
        Ok(())
    }

    /// Go `Manager.Stop`: later acquires get `NotReady`.
    pub fn stop(&self) {
        self.index.lock().expect("semaphore manager lock").state = State::Stopped;
        self.startup_done.send_replace(true);
    }

    fn is_running(&self) -> bool {
        self.index.lock().expect("semaphore manager lock").state == State::Running
    }

    fn is_alive(&self, now_nanos: i64) -> bool {
        self.liveness.is_alive(now_nanos)
    }

    /// Go `Manager.Acquire`: mark alive, require startup, then grant. `deadline`
    /// is the caller's own deadline, not the startup scan's.
    pub async fn acquire(
        &self,
        owner_id: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<AcquireResult, ManagerError> {
        if owner_id.is_empty() {
            return Err(ManagerError::BadRequest("ownerID is required".into()));
        }
        // Marked before the startup wait so a bucket whose first request
        // arrives during a slow scan is not counted idle once it finishes.
        self.liveness.mark_alive(now_nanos());
        // Go `awaitStartup`: callers that find the load in flight wait for it
        // only until their own deadline, instead of failing or outliving it.
        let starting = self.index.lock().expect("semaphore manager lock").state == State::Starting;
        if starting {
            let mut done = self.startup_done.subscribe();
            let finished = done.wait_for(|finished| *finished);
            let _ = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, finished)
                    .await
                    .map_err(|_| ManagerError::Deadline)?,
                None => finished.await,
            };
        }
        if !self.is_running() {
            return Err(ManagerError::NotReady);
        }
        self.grant(owner_id, deadline).await
    }

    /// Go `grant`: confirm an existing hold, else draw a free id and settle it
    /// with a conditional write. Stops retrying once `deadline` has passed.
    async fn grant(
        &self,
        owner_id: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<AcquireResult, ManagerError> {
        let existing = self
            .index
            .lock()
            .expect("semaphore manager lock")
            .held
            .get(owner_id)
            .copied();
        if let Some(token_id) = existing {
            if self.confirm_hold(owner_id, token_id).await? {
                return Ok(AcquireResult {
                    outcome: SemaphoreAcquireOutcome::Acquired,
                    token_id,
                });
            }
        }
        for _ in 0..MAX_GRANT_ATTEMPTS {
            // Go checks ctx before each write: a caller that gave up stops here
            // instead of letting the write fail as a store timeout.
            if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                return Err(ManagerError::Deadline);
            }
            let Some(token_id) = self.reserve() else {
                break;
            };
            let outcome = self.store.grant_semaphore_token(
                &self.id.domain_id,
                &self.id.semaphore_name,
                self.id.bucket,
                token_id,
                owner_id,
            ).await;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    // Like Go, an ambiguous failed write must not drain the
                    // cache. The next conditional write resolves ownership.
                    self.unreserve(token_id);
                    return Err(error.into());
                }
            };
            match outcome {
                GrantOutcome::Applied => {
                    self.record_hold(owner_id, token_id);
                    return Ok(AcquireResult {
                        outcome: SemaphoreAcquireOutcome::Acquired,
                        token_id,
                    });
                }
                GrantOutcome::SlotTaken => continue,
                GrantOutcome::Unknown => {
                    // Like Go's default branch: an outcome we cannot read says
                    // nothing about the slot, so the id goes back.
                    self.unreserve(token_id);
                    return Err(ManagerError::Internal(format!(
                        "unexpected grant outcome for bucket {}",
                        self.id.to_log_string()
                    )));
                }
                GrantOutcome::AlreadyHeld(held_token) => {
                    self.unreserve(token_id);
                    if held_token < 1 {
                        return Err(ManagerError::Internal(format!(
                            "grant reported an already-held slot without a token for bucket {}",
                            self.id.to_log_string()
                        )));
                    }
                    self.record_hold(owner_id, held_token);
                    return Ok(AcquireResult {
                        outcome: SemaphoreAcquireOutcome::Acquired,
                        token_id: held_token,
                    });
                }
            }
        }
        Ok(AcquireResult {
            outcome: SemaphoreAcquireOutcome::NoSlot,
            token_id: 0,
        })
    }

    /// Go `confirmHold`: verify the reverse-index entry against the token row.
    async fn confirm_hold(&self, owner_id: &str, token_id: i32) -> Result<bool, ManagerError> {
        match self.store.get_semaphore_ownership_by_token(
            &self.id.domain_id,
            &self.id.semaphore_name,
            self.id.bucket,
            token_id,
        ).await {
            Ok(holder) if holder == owner_id => Ok(true),
            Ok(holder) => {
                // The index is stale; return the slot only if the row says it
                // really is unheld.
                self.drop_stale_hold(owner_id, token_id, holder.is_empty());
                Ok(false)
            }
            Err(AcquireError::NotFound(_)) => {
                self.drop_stale_hold(owner_id, token_id, false);
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn reserve(&self) -> Option<i32> {
        let mut index = self.index.lock().expect("semaphore manager lock");
        if index.free_list.is_empty() {
            return None;
        }
        let position = rand_index(index.free_list.len());
        let token_id = index.free_list[position];
        remove_from_free_set(&mut index, token_id);
        Some(token_id)
    }

    fn unreserve(&self, token_id: i32) {
        add_to_free_set(&mut self.index.lock().expect("semaphore manager lock"), token_id);
    }

    fn record_hold(&self, owner_id: &str, token_id: i32) {
        let mut index = self.index.lock().expect("semaphore manager lock");
        index.held.insert(owner_id.to_string(), token_id);
        remove_from_free_set(&mut index, token_id);
    }

    fn drop_stale_hold(&self, owner_id: &str, token_id: i32, still_free: bool) {
        let mut index = self.index.lock().expect("semaphore manager lock");
        if index.held.get(owner_id).copied() == Some(token_id) {
            index.held.remove(owner_id);
        }
        if still_free {
            add_to_free_set(&mut index, token_id);
        }
    }

    #[cfg(test)]
    fn free_count(&self) -> usize {
        self.index.lock().expect("semaphore manager lock").free_list.len()
    }
}

/// Uniform-random index in `0..len`, using a bounded draw so an empty set is
/// never indexed.
fn rand_index(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    (rand::random::<u64>() % len as u64) as usize
}

/// Go `addToFreeSetLocked`: put an id back, ignoring one already there so the
/// list never holds duplicates.
fn add_to_free_set(index: &mut Index, token_id: i32) {
    if index.free_index.contains_key(&token_id) {
        return;
    }
    index.free_index.insert(token_id, index.free_list.len());
    index.free_list.push(token_id);
}

/// Go `removeFromFreeSetLocked`: swap-remove in constant time.
fn remove_from_free_set(index: &mut Index, token_id: i32) {
    let Some(position) = index.free_index.get(&token_id).copied() else {
        return;
    };
    let last = index.free_list.len() - 1;
    let moved = index.free_list[last];
    index.free_list[position] = moved;
    index.free_index.insert(moved, position);
    index.free_list.truncate(last);
    // When the removed id was itself the tail, moved == token_id and the line
    // above just re-added it, so this delete has to come last.
    index.free_index.remove(&token_id);
}

/// Go `semaphoreRegistryImpl`: one manager per bucket, whoever asks first.
#[derive(Clone)]
pub struct Registry {
    managers: Arc<Mutex<HashMap<Identifier, Arc<Manager>>>>,
    store: Arc<dyn SemaphorePersistence>,
    idle_ttl_nanos: i64,
}

impl Registry {
    pub fn new(store: Arc<dyn SemaphorePersistence>) -> Self {
        Self {
            managers: Arc::new(Mutex::new(HashMap::new())),
            store,
            idle_ttl_nanos: DEFAULT_IDLE_TTL_NANOS,
        }
    }

    pub fn with_idle_ttl(mut self, idle_ttl_nanos: i64) -> Self {
        self.idle_ttl_nanos = idle_ttl_nanos;
        self
    }

    /// Go `GetOrCreate`: register before starting so a concurrent request
    /// reuses the manager; then always `Start`, since the request that
    /// registered it may not have started it.
    pub async fn get_or_create(&self, id: Identifier) -> Result<Arc<Manager>, ManagerError> {
        self.get_or_create_with_idle_ttl(id, self.idle_ttl_nanos).await
    }

    /// Go passes `IdleTTL: cfg.SemaphoreIdleTime(domainName)` when it creates
    /// the manager; an existing manager keeps the TTL it was created with.
    pub async fn get_or_create_with_idle_ttl(
        &self,
        id: Identifier,
        idle_ttl_nanos: i64,
    ) -> Result<Arc<Manager>, ManagerError> {
        if let Some(existing) = self.by_identifier(&id) {
            existing.start().await?;
            return Ok(existing);
        }
        let manager = Arc::new(Manager::new(id.clone(), self.store.clone(), idle_ttl_nanos)?);
        // The registry lock is released before any await, so the guard never
        // crosses a suspension point.
        let existing = {
            let mut managers = self.managers.lock().expect("semaphore registry lock");
            match managers.get(&id).cloned() {
                Some(existing) => Some(existing),
                None => {
                    managers.insert(id, manager.clone());
                    None
                }
            }
        };
        if let Some(existing) = existing {
            existing.start().await?;
            return Ok(existing);
        }
        if let Err(error) = manager.start().await {
            self.unregister(&manager);
            return Err(error);
        }
        Ok(manager)
    }

    /// Go `Unregister`: only remove if the map still holds this manager.
    pub fn unregister(&self, manager: &Arc<Manager>) -> bool {
        let mut managers = self.managers.lock().expect("semaphore registry lock");
        let id = manager.identifier();
        match managers.get(id) {
            Some(current) if Arc::ptr_eq(current, manager) => {
                managers.remove(id);
                true
            }
            _ => false,
        }
    }

    pub fn by_identifier(&self, id: &Identifier) -> Option<Arc<Manager>> {
        self.managers
            .lock()
            .expect("semaphore registry lock")
            .get(id)
            .cloned()
    }

    pub fn all_managers(&self) -> Vec<Arc<Manager>> {
        self.managers
            .lock()
            .expect("semaphore registry lock")
            .values()
            .cloned()
            .collect()
    }

    /// Go's liveness callback: unregister and stop a bucket that has gone
    /// `idle_ttl` without a request.
    pub fn sweep_idle(&self, now_nanos: i64) {
        for manager in self.all_managers() {
            if !manager.is_alive(now_nanos) && self.unregister(&manager) {
                manager.stop();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::InMemoryExecutionStore;

    fn store() -> InMemoryExecutionStore {
        let store = InMemoryExecutionStore::new();
        store.register_domain("d", "uuid-d");
        store.create_semaphore("d", "sem", 10, 2).unwrap();
        store
    }

    fn identifier(bucket: i32) -> Identifier {
        Identifier::new("uuid-d", "sem", bucket).unwrap()
    }

    #[test]
    fn matching_semaphore_identifier_matches_go_validation() {
        assert!(Identifier::new("", "sem", 0).is_err());
        assert!(Identifier::new("uuid-d", "", 0).is_err());
        assert!(Identifier::new("uuid-d", "sem", -1).is_err());
        assert_eq!(identifier(2).to_log_string(), "uuid-d/sem/2");
        assert_eq!(identifier(2).ring_key(), "uuid-d_sem_2");
    }

    #[tokio::test]
    async fn matching_semaphore_registry_shares_one_manager_per_bucket() {
        let registry = Registry::new(Arc::new(store()));
        let first = registry.get_or_create(identifier(0)).await.unwrap();
        let second = registry.get_or_create(identifier(0)).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(registry.all_managers().len(), 1);
        // Unregister only removes the manager it still holds.
        assert!(registry.unregister(&first));
        assert!(registry.by_identifier(&identifier(0)).is_none());
        assert!(!registry.unregister(&first));
        let rebuilt = registry.get_or_create(identifier(0)).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &rebuilt));
    }

    #[tokio::test]
    async fn matching_semaphore_grants_until_full_then_reports_no_slot() {
        let registry = Registry::new(Arc::new(store()));
        let manager = registry.get_or_create(identifier(0)).await.unwrap();
        let first = manager.acquire("1:w:r1:1", None).await.unwrap();
        assert_eq!(first.outcome, SemaphoreAcquireOutcome::Acquired);
        assert!((1..=2).contains(&first.token_id));
        let second = manager.acquire("1:w:r2:1", None).await.unwrap();
        assert_eq!(second.outcome, SemaphoreAcquireOutcome::Acquired);
        assert_ne!(first.token_id, second.token_id);
        let full = manager.acquire("1:w:r3:1", None).await.unwrap();
        assert_eq!(full.outcome, SemaphoreAcquireOutcome::NoSlot);
        assert_eq!(full.token_id, 0);
        // A re-acquire by a holder returns the same slot.
        let again = manager.acquire("1:w:r1:1", None).await.unwrap();
        assert_eq!(again.token_id, first.token_id);
        assert_eq!(manager.free_count(), 0);
    }

    #[tokio::test]
    async fn matching_semaphore_load_rebuilds_index_from_persisted_holds() {
        let store = store();
        // A hold granted by a previous host: the scan must exclude its slot.
        store
            .grant_semaphore_token("uuid-d", "sem", 0, 2, "1:w:r1:1")
            .unwrap();
        let registry = Registry::new(Arc::new(store));
        let manager = registry.get_or_create(identifier(0)).await.unwrap();
        assert_eq!(manager.free_count(), 1);
        // The persisted holder keeps its slot without a new grant.
        let held = manager.acquire("1:w:r1:1", None).await.unwrap();
        assert_eq!((held.outcome, held.token_id), (SemaphoreAcquireOutcome::Acquired, 2));
        let other = manager.acquire("1:w:r2:1", None).await.unwrap();
        assert_eq!((other.outcome, other.token_id), (SemaphoreAcquireOutcome::Acquired, 1));
    }

    #[tokio::test]
    async fn matching_semaphore_confirm_hold_drops_stale_index_entry() {
        let store = store();
        let registry = Registry::new(Arc::new(store.clone()));
        let manager = registry.get_or_create(identifier(0)).await.unwrap();
        let granted = manager.acquire("1:w:r1:1", None).await.unwrap();
        // Simulate another host taking the slot behind this cache.
        store.release_semaphore_hold("uuid-d", "sem", 0, "1:w:r1:1");
        store
            .grant_semaphore_token("uuid-d", "sem", 0, granted.token_id, "1:w:r9:1")
            .unwrap();
        // The stale reverse-index entry is dropped and r1 is given the slot
        // that is genuinely free, never the one r9 now holds.
        let stale = manager.acquire("1:w:r1:1", None).await.unwrap();
        assert_eq!(stale.outcome, SemaphoreAcquireOutcome::Acquired);
        assert_ne!(stale.token_id, granted.token_id);
    }

    #[tokio::test]
    async fn matching_semaphore_missing_semaphore_is_not_ready_after_scan() {
        let store = InMemoryExecutionStore::new();
        store.register_domain("d", "uuid-d");
        let registry = Registry::new(Arc::new(store));
        let error = match registry.get_or_create(identifier(0)).await {
            Ok(_) => panic!("a missing semaphore must fail the startup scan"),
            Err(error) => error,
        };
        assert!(matches!(error, ManagerError::NotFound(_)));
        // A failed start does not leave a manager registered.
        assert_eq!(registry.all_managers().len(), 0);
    }

    #[tokio::test]
    async fn matching_semaphore_sweep_unloads_idle_buckets() {
        let registry = Registry::new(Arc::new(store())).with_idle_ttl(1_000_000_000);
        let manager = registry.get_or_create(identifier(0)).await.unwrap();
        registry.sweep_idle(now_nanos());
        assert!(registry.by_identifier(&identifier(0)).is_some());
        registry.sweep_idle(now_nanos() + 2_000_000_000);
        assert!(registry.by_identifier(&identifier(0)).is_none());
        assert!(!manager.is_running());
        // A stopped manager refuses further acquires until rebuilt.
        assert_eq!(manager.acquire("1:w:r1:1", None).await, Err(ManagerError::NotReady));
    }

    // Go: TestAcquireRejectsAnEmptyOwnerID (semaphore/semaphore_manager_test.go:307). A rejected
    // request must not touch the free set.
    #[tokio::test]
    async fn matching_semaphore_rejects_empty_owner_without_touching_free_set() {
        let registry = Registry::new(Arc::new(store()));
        let manager = registry.get_or_create(identifier(0)).await.unwrap();
        assert_eq!(
            manager.acquire("", None).await,
            Err(ManagerError::BadRequest("ownerID is required".into()))
        );
        assert_eq!(manager.free_count(), 2);
    }

    // Go: TestUnreserveIsIdempotent (semaphore/semaphore_manager_test.go:829). A double unreserve
    // must not list the same slot twice, or two acquires could draw it.
    #[tokio::test]
    async fn matching_semaphore_unreserve_is_idempotent() {
        let registry = Registry::new(Arc::new(store()));
        let manager = registry.get_or_create(identifier(0)).await.unwrap();
        let token_id = manager.reserve().unwrap();
        assert_eq!(manager.free_count(), 1);
        manager.unreserve(token_id);
        assert_eq!(manager.free_count(), 2);
        manager.unreserve(token_id);
        assert_eq!(manager.free_count(), 2);
    }

    // Go: TestRingKey. Buckets of one semaphore must hash to different ring keys.
    #[test]
    fn matching_semaphore_ring_key_separates_buckets() {
        assert_eq!(identifier(0).ring_key(), "uuid-d_sem_0");
        assert_ne!(identifier(0).ring_key(), identifier(1).ring_key());
    }

    // Go: TestIdentifierIsUsableAsAMapKey. Buckets are looked up by value.
    #[test]
    fn matching_semaphore_identifier_is_usable_as_a_map_key() {
        let mut buckets = HashMap::new();
        buckets.insert(identifier(0), "first");
        buckets.insert(identifier(1), "second");
        assert_eq!(buckets.get(&identifier(0)), Some(&"first"));
        assert_eq!(buckets.len(), 2);
    }

    // Go: TestSemaphoreRegistry_RegisterLookupAndUnregister.
    #[tokio::test]
    async fn matching_semaphore_registry_register_lookup_and_unregister() {
        let registry = Registry::new(Arc::new(store()));
        assert!(registry.by_identifier(&identifier(0)).is_none(), "an empty registry holds nothing");
        let manager = registry.get_or_create(identifier(0)).await.unwrap();
        let held = registry.by_identifier(&identifier(0)).expect("a registered manager is found");
        assert!(Arc::ptr_eq(&manager, &held));
        assert!(registry.unregister(&manager), "unregistering the held manager reports true");
        assert!(registry.by_identifier(&identifier(0)).is_none());
        assert!(!registry.unregister(&manager), "unregistering twice reports false");
    }

    // Go: TestSemaphoreRegistry_UnregisterIgnoresAStaleManager. A stale manager tearing down
    // must not evict the manager that now serves the same bucket.
    #[tokio::test]
    async fn matching_semaphore_unregister_ignores_a_stale_manager() {
        let registry = Registry::new(Arc::new(store()));
        let stopping = registry.get_or_create(identifier(0)).await.unwrap();
        assert!(registry.unregister(&stopping));
        let current = registry.get_or_create(identifier(0)).await.unwrap();
        assert!(!registry.unregister(&stopping), "the registry no longer holds this manager");
        let held = registry
            .by_identifier(&identifier(0))
            .expect("the manager now serving the bucket must survive");
        assert!(Arc::ptr_eq(&current, &held));
    }

    // Go: TestSemaphoreRegistry_AllManagersIsASnapshot.
    #[tokio::test]
    async fn matching_semaphore_all_managers_is_a_snapshot() {
        let registry = Registry::new(Arc::new(store()));
        registry.get_or_create(identifier(0)).await.unwrap();
        registry.get_or_create(identifier(1)).await.unwrap();
        let snapshot = registry.all_managers();
        assert_eq!(snapshot.len(), 2);
        for manager in &snapshot {
            assert!(registry.unregister(manager));
        }
        assert_eq!(snapshot.len(), 2, "emptying the registry must not change a snapshot already taken");
        assert!(registry.all_managers().is_empty());
    }

    // Go: TestSemaphoreRegistry_GetOrCreateBuildsOnceUnderConcurrentCallers. Every caller of a
    // cold bucket must get the one manager that was registered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn matching_semaphore_concurrent_callers_share_one_manager() {
        let registry = Registry::new(Arc::new(store()));
        let tasks: Vec<_> = (0..32)
            .map(|_| {
                let registry = registry.clone();
                tokio::spawn(async move { registry.get_or_create(identifier(0)).await.unwrap() })
            })
            .collect();
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        let held = registry.by_identifier(&identifier(0)).expect("the bucket is registered");
        assert_eq!(registry.all_managers().len(), 1);
        for got in &results {
            assert!(Arc::ptr_eq(&held, got), "every caller must get the one registered manager");
        }
    }
}

#[cfg(test)]
#[path = "matching_semaphore_parity_tests.rs"]
mod parity_tests;
