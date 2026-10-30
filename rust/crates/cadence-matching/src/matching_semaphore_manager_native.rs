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

use crate::matching_semaphore_native::{IdentifierBytes, IdentifierError};
use cadence_api::enums::SemaphoreAcquireOutcome;
use cadence_common::go_string::GoString;
use cadence_common::liveness::Liveness;
use cadence_persistence::{
    semaphores_native::{SemaphoreCallContext, SemaphorePersistenceBytes, SemaphoreRowBytes},
    AcquireError, GrantOutcome,
};
pub type Identifier = IdentifierBytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

/// Go `maxGrantAttempts`: cap on conditional writes one acquire may make.
pub const MAX_GRANT_ATTEMPTS: usize = 3;
/// Go `matching.semaphoreIdleTime` default: 5 minutes in nanoseconds.
pub const DEFAULT_IDLE_TTL_NANOS: i64 = 5 * 60 * 1_000_000_000;
/// Go `scanPageSize`: one larger than a full bucket (one token row per slot plus one owner row
/// per hold), so the startup scan takes a single round trip. Paging runs to the end regardless.
pub const SCAN_PAGE_SIZE: usize = 2 * cadence_persistence::semaphores::MAX_BUCKET_SIZE as usize + 1;

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}

fn validate_identifier(id: &Identifier) -> Result<(), ManagerError> {
    match id.validate() {
        Ok(()) => Ok(()),
        Err(IdentifierError::MissingDomain) => {
            Err(ManagerError::BadRequest("domainID is required".into()))
        }
        Err(IdentifierError::MissingName) => {
            Err(ManagerError::BadRequest("semaphoreName is required".into()))
        }
        Err(IdentifierError::NegativeBucket(bucket)) => Err(ManagerError::BadRequest(format!(
            "bucket must not be negative, got {bucket}"
        ))),
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
    NotFound(cadence_api::errors::EntityNotExistsError),
    /// `types.InternalServiceError`.
    Internal(String),
    /// A typed internal error, distinct from an untyped persistence failure.
    TypedInternal(String),
    /// ServiceBusy with its original reason.
    ServiceBusyReason { message: String, reason: String },
    /// `types.ServiceBusyError` returned by the Go persistence limiter.
    ServiceBusy(String),
    /// Startup scan failure with its original classification and raw identifier.
    Load(GoString, Box<ManagerError>),
}

impl From<AcquireError> for ManagerError {
    fn from(error: AcquireError) -> Self {
        match error {
            AcquireError::NotFound(error) => ManagerError::NotFound(error),
            AcquireError::Invalid(message) => ManagerError::BadRequest(message),
            AcquireError::Internal(message) => ManagerError::Internal(message),
            AcquireError::TypedInternal(message) => ManagerError::TypedInternal(message),
            AcquireError::ServiceBusy { message, reason } => ManagerError::ServiceBusyReason { message, reason },
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
    held: HashMap<GoString, i32>,
}

/// Go `onStopFn`: called once from `stop` with the manager being stopped.
type OnStop = Box<dyn Fn(&Manager) + Send + Sync>;

/// Stops a manager whose startup load was dropped before it returned, so the
/// bucket is not left in `Starting` with nothing to finish the load.
struct LoadInFlight<'a> {
    manager: &'a Manager,
    completed: bool,
}

/// Restores a reserved slot when a conditional write future is cancelled or
/// returns an ambiguous error. A confirmed SlotTaken disarms this guard.
struct Reservation<'a> {
    manager: &'a Manager,
    token_id: i32,
    restore: bool,
}

impl Reservation<'_> {
    fn consumed(&mut self) {
        self.restore = false;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.restore {
            self.manager.restore_reservation(self.token_id);
        }
    }
}

impl Drop for LoadInFlight<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.manager.stop();
        }
    }
}

/// One bucket's manager, mirroring Go's `semaphoreManagerImpl`.
pub struct Manager {
    id: Identifier,
    store: Arc<dyn SemaphorePersistenceBytes>,
    liveness: Liveness,
    index: Mutex<Index>,
    /// Go `startupDoneCh`: set once the startup load ended (or the manager stopped).
    startup_done: tokio::sync::watch::Sender<bool>,
    /// Go `onStopFn`: unregisters this manager. Required, as in Go.
    on_stop: OnStop,
}

impl Manager {
    /// Go `NewManager`: validates the parameters before building anything, so a bad
    /// one is a BadRequest and no manager exists.
    fn new(
        id: Identifier,
        store: Arc<dyn SemaphorePersistenceBytes>,
        idle_ttl_nanos: i64,
        on_stop: OnStop,
    ) -> Result<Self, ManagerError> {
        validate_identifier(&id)?;
        // Go rejects a non-positive IdleTTL: the liveness timer cannot be built from it.
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
            on_stop,
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
        let mut load = LoadInFlight {
            manager: self,
            completed: false,
        };
        let loaded = self.load().await;
        load.completed = true;
        if loaded.is_err() {
            self.stop();
        }
        // Go defers markStartupDone: waiters are released however load returns.
        self.startup_done.send_replace(true);
        loaded
    }

    /// Go `loadTokenOwnership` paging: follows each page token to the end, so the load
    /// sees the whole bucket however many pages it takes.
    async fn scan_bucket(&self) -> Result<Vec<SemaphoreRowBytes>, AcquireError> {
        let mut rows = Vec::new();
        let mut page_token: Option<Vec<u8>> = None;
        loop {
            let page = self
                .store
                .scan_semaphore_bucket(
                    &self.id.domain_id,
                    &self.id.semaphore_name,
                    self.id.bucket,
                    SCAN_PAGE_SIZE,
                    page_token.as_deref(),
                )
                .await?;
            rows.extend(page.rows);
            // Go stops on an empty token, not only a missing one.
            match page.next_page_token.filter(|token| !token.is_empty()) {
                Some(token) => page_token = Some(token),
                None => return Ok(rows),
            }
        }
    }

    async fn load(&self) -> Result<(), ManagerError> {
        let rows = self.scan_bucket().await.map_err(|error| {
            ManagerError::Load(self.id.to_log_string(), Box::new(error.into()))
        })?;
        // Go builds the free set only from rows returned by the scan. Metadata
        // does not prove that an absent token row exists or is unheld.
        let mut free: HashMap<i32, ()> = HashMap::new();
        let mut held: HashMap<GoString, i32> = HashMap::new();
        for row in rows {
            match row {
                SemaphoreRowBytes::Token { token_id, holder } => {
                    if holder.is_empty() && token_id > 0 {
                        free.insert(token_id, ());
                    } else {
                        free.remove(&token_id);
                    }
                }
                SemaphoreRowBytes::Owner {
                    owner_id,
                    held_token,
                } => {
                    if !owner_id.is_empty() && held_token > 0 {
                        held.insert(owner_id, held_token);
                    }
                }
                // Go's load counts and steps over a row it cannot classify.
                SemaphoreRowBytes::Unknown => {}
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

    /// Go `Manager.Stop`: later acquires get `NotReady`. Runs the teardown once,
    /// so `on_stop` fires once and a repeat call changes nothing.
    pub fn stop(&self) {
        let first_stop = {
            let mut index = self.index.lock().expect("semaphore manager lock");
            let first_stop = index.state != State::Stopped;
            index.state = State::Stopped;
            first_stop
        };
        if !first_stop {
            return;
        }
        // Unregistered before startup is released, so nothing is handed a manager
        // that can no longer serve. The index lock is not held across the callback.
        (self.on_stop)(self);
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
        owner_id: &GoString,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<AcquireResult, ManagerError> {
        self.acquire_with_context(owner_id, &SemaphoreCallContext {
            deadline,
            ..Default::default()
        }).await
    }

    pub async fn acquire_with_context(
        &self,
        owner_id: &GoString,
        context: &SemaphoreCallContext,
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
            tokio::select! {
                _ = finished => {},
                _ = context.wait() => return Err(ManagerError::Deadline),
            }
        }
        if !self.is_running() {
            return Err(ManagerError::NotReady);
        }
        self.grant(owner_id, context).await
    }

    /// Go `grant`: confirm an existing hold, else draw a free id and settle it
    /// with a conditional write. Stops retrying once `deadline` has passed.
    async fn grant(
        &self,
        owner_id: &GoString,
        context: &SemaphoreCallContext,
    ) -> Result<AcquireResult, ManagerError> {
        let existing = self
            .index
            .lock()
            .expect("semaphore manager lock")
            .held
            .get(owner_id)
            .copied();
        if let Some(token_id) = existing {
            if self.confirm_hold(owner_id, token_id, context).await? {
                return Ok(AcquireResult {
                    outcome: SemaphoreAcquireOutcome::Acquired,
                    token_id,
                });
            }
        }
        for _ in 0..MAX_GRANT_ATTEMPTS {
            // Go checks ctx before each write: a caller that gave up stops here
            // instead of letting the write fail as a store timeout.
            if context.ended() {
                return Err(ManagerError::Deadline);
            }
            let Some(token_id) = self.reserve() else {
                break;
            };
            let mut reservation = Reservation {
                manager: self,
                token_id,
                restore: true,
            };
            let outcome = self
                .store
                .grant_with_context(
                    &self.id.domain_id,
                    &self.id.semaphore_name,
                    self.id.bucket,
                    token_id,
                    owner_id,
                    context,
                )
                .await;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    // Like Go, an ambiguous failed write must not drain the
                    // cache. The next conditional write resolves ownership.
                    return Err(error.into());
                }
            };
            match outcome {
                GrantOutcome::Applied => {
                    reservation.consumed();
                    self.record_hold_if_running(owner_id, token_id);
                    return Ok(AcquireResult {
                        outcome: SemaphoreAcquireOutcome::Acquired,
                        token_id,
                    });
                }
                GrantOutcome::SlotTaken => {
                    reservation.consumed();
                    continue;
                }
                GrantOutcome::Unknown => {
                    // Like Go's default branch: an outcome we cannot read says
                    // nothing about the slot, so the id goes back.
                    return Err(ManagerError::Internal(format!(
                        "unexpected grant outcome for bucket {}",
                        self.id.to_log_string().go_quoted_string()
                    )));
                }
                GrantOutcome::AlreadyHeld(held_token) => {
                    // Return the tentative slot before removing the authoritative
                    // held slot; the ids may be the same.
                    drop(reservation);
                    if held_token < 1 {
                        return Err(ManagerError::Internal(format!(
                            "grant reported an already-held slot without a token for bucket {}",
                            self.id.to_log_string().go_quoted_string()
                        )));
                    }
                    self.record_hold_if_running(owner_id, held_token);
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
    async fn confirm_hold(&self, owner_id: &GoString, token_id: i32, context: &SemaphoreCallContext) -> Result<bool, ManagerError> {
        match self
            .store
            .confirm_with_context(
                &self.id.domain_id,
                &self.id.semaphore_name,
                self.id.bucket,
                token_id,
                context,
            )
            .await
        {
            Ok(holder) if holder == *owner_id => Ok(true),
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

    fn restore_reservation(&self, token_id: i32) {
        let mut index = self.index.lock().expect("semaphore manager lock");
        if index.state == State::Running {
            add_to_free_set(&mut index, token_id);
        }
    }

    fn record_hold_if_running(&self, owner_id: &GoString, token_id: i32) {
        let mut index = self.index.lock().expect("semaphore manager lock");
        if index.state == State::Running {
            index.held.insert(owner_id.clone(), token_id);
            remove_from_free_set(&mut index, token_id);
        }
    }

    fn drop_stale_hold(&self, owner_id: &GoString, token_id: i32, still_free: bool) {
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
        self.index
            .lock()
            .expect("semaphore manager lock")
            .free_list
            .len()
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

/// Removes `id` only while the map still holds this exact manager, so a stopping
/// manager cannot remove the replacement that took its place. Go `Unregister`.
fn remove_current(
    managers: &Mutex<HashMap<Identifier, Arc<Manager>>>,
    manager: &Manager,
    id: &Identifier,
) -> bool {
    let mut managers = managers.lock().expect("semaphore registry lock");
    match managers.get(id) {
        Some(current) if std::ptr::eq(Arc::as_ptr(current), manager) => {
            managers.remove(id);
            true
        }
        _ => false,
    }
}

/// Go `semaphoreRegistryImpl`: one manager per bucket, whoever asks first.
#[derive(Clone)]
pub struct Registry {
    managers: Arc<Mutex<HashMap<Identifier, Arc<Manager>>>>,
    store: Arc<dyn SemaphorePersistenceBytes>,
    idle_ttl_nanos: i64,
}

impl Registry {
    pub fn new(store: Arc<dyn SemaphorePersistenceBytes>) -> Self {
        Self {
            managers: Arc::new(Mutex::new(HashMap::new())),
            store,
            idle_ttl_nanos: DEFAULT_IDLE_TTL_NANOS,
        }
    }

    /// Kept as given, not clamped: a non-positive TTL fails the manager's creation, as Go's does.
    pub fn with_idle_ttl(mut self, idle_ttl_nanos: i64) -> Self {
        self.idle_ttl_nanos = idle_ttl_nanos;
        self
    }

    /// Go `GetOrCreate`: register before starting so a concurrent request
    /// reuses the manager; then always `Start`, since the request that
    /// registered it may not have started it.
    pub async fn get_or_create(&self, id: Identifier) -> Result<Arc<Manager>, ManagerError> {
        self.get_or_create_with_idle_ttl(id, self.idle_ttl_nanos)
            .await
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
        // Construct under the registry lock like Go GetOrCreate. If another
        // request wins between this optimistic lookup and the lock, return its
        // manager even when this caller's current TTL is invalid. The lock is
        // held only for synchronous construction and map insertion, then
        // released before any await.
        let existing = {
            let mut managers = self.managers.lock().expect("semaphore registry lock");
            match managers.get(&id).cloned() {
                Some(existing) => Some(existing),
                None => {
                    // Weak, so a manager stopped after the registry is gone
                    // does not keep the map alive.
                    let managers = Arc::downgrade(&self.managers);
                    let on_stop: OnStop = Box::new(move |stopping: &Manager| {
                        if let Some(managers) = managers.upgrade() {
                            remove_current(&managers, stopping, stopping.identifier());
                        }
                    });
                    let manager = Arc::new(Manager::new(
                        id.clone(),
                        self.store.clone(),
                        idle_ttl_nanos,
                        on_stop,
                    )?);
                    managers.insert(id, manager.clone());
                    Some(manager)
                }
            }
        };
        let manager = existing.expect("created or found under registry lock");
        manager.start().await?;
        Ok(manager)
    }

    /// Go `Unregister`: only remove if the map still holds this manager.
    pub fn unregister(&self, manager: &Arc<Manager>) -> bool {
        remove_current(&self.managers, manager, manager.identifier())
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
pub(crate) mod tests {
    use super::*;
    use async_trait::async_trait;
    use cadence_persistence::{
        semaphores::{AcquireError, GrantOutcome, SemaphoreError, SemaphoreMetadata},
        semaphores_native::{
            InMemorySemaphorePersistenceBytes, SemaphoreBucketPageBytes, SemaphorePersistenceBytes,
        },
    };
    use tokio::sync::Notify;

    fn bytes(value: &[u8]) -> GoString {
        GoString::from(value.to_vec())
    }

    #[derive(Debug)]
    pub(crate) struct ParkGrant {
        pub(crate) inner: InMemorySemaphorePersistenceBytes,
        pub(crate) entered: Notify,
        pub(crate) release: Notify,
        pub(crate) scan_pages: std::sync::atomic::AtomicUsize,
        pub(crate) park_scan: std::sync::atomic::AtomicBool,
        pub(crate) fail_scan: std::sync::atomic::AtomicBool,
        pub(crate) scan_entered: Notify,
        pub(crate) scan_release: Notify,
        pub(crate) park_confirm: std::sync::atomic::AtomicBool,
        pub(crate) cancel_grant: std::sync::atomic::AtomicBool,
    }
    #[async_trait]
    impl SemaphorePersistenceBytes for ParkGrant {
        async fn create_semaphore(
            &self,
            d: &GoString,
            n: &GoString,
            s: i32,
            b: i32,
        ) -> Result<SemaphoreMetadata, SemaphoreError> {
            self.inner.create_semaphore(d, n, s, b).await
        }
        async fn get_semaphore_metadata(
            &self,
            d: &GoString,
            n: &GoString,
        ) -> Result<SemaphoreMetadata, AcquireError> {
            self.inner.get_semaphore_metadata(d, n).await
        }
        async fn scan_semaphore_bucket(
            &self,
            d: &GoString,
            n: &GoString,
            b: i32,
            _p: usize,
            t: Option<&[u8]>,
        ) -> Result<SemaphoreBucketPageBytes, AcquireError> {
            self.scan_pages
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self
                .park_scan
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.scan_entered.notify_one();
                self.scan_release.notified().await;
                if self.fail_scan.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(AcquireError::Internal("injected scan failure".into()));
                }
            }
            let all = self
                .inner
                .scan_semaphore_bucket(d, n, b, usize::MAX, None)
                .await?;
            let offset = t
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            let end = (offset + 1).min(all.rows.len());
            let next_page_token = (end < all.rows.len()).then(|| end.to_string().into_bytes());
            Ok(SemaphoreBucketPageBytes {
                rows: all.rows.into_iter().skip(offset).take(1).collect(),
                next_page_token,
            })
        }
        async fn get_semaphore_ownership_by_token(
            &self,
            d: &GoString,
            n: &GoString,
            b: i32,
            t: i32,
        ) -> Result<GoString, AcquireError> {
            self.inner
                .get_semaphore_ownership_by_token(d, n, b, t)
                .await
        }
        async fn get_semaphore_ownership_by_owner(
            &self,
            d: &GoString,
            n: &GoString,
            b: i32,
            o: &GoString,
        ) -> Result<i32, AcquireError> {
            self.inner
                .get_semaphore_ownership_by_owner(d, n, b, o)
                .await
        }
        async fn grant_semaphore_token(
            &self,
            d: &GoString,
            n: &GoString,
            b: i32,
            t: i32,
            o: &GoString,
        ) -> Result<GrantOutcome, AcquireError> {
            self.entered.notify_one();
            self.release.notified().await;
            self.inner.grant_semaphore_token(d, n, b, t, o).await
        }
        async fn confirm_with_context(
            &self, d: &GoString, n: &GoString, b: i32, t: i32,
            _context: &SemaphoreCallContext,
        ) -> Result<GoString, AcquireError> {
            if self.park_confirm.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.get_semaphore_ownership_by_token(d, n, b, t).await
        }
        async fn grant_with_context(
            &self, d: &GoString, n: &GoString, b: i32, t: i32, o: &GoString,
            context: &SemaphoreCallContext,
        ) -> Result<GrantOutcome, AcquireError> {
            if self.cancel_grant.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                context.wait().await;
                return Err(AcquireError::Internal("context canceled".into()));
            }
            self.grant_semaphore_token(d, n, b, t, o).await
        }
        async fn release_semaphore_token(
            &self,
            d: &GoString,
            n: &GoString,
            b: i32,
            t: i32,
            o: &GoString,
        ) -> Result<bool, AcquireError> {
            self.inner.release_semaphore_token(d, n, b, t, o).await
        }
    }

    #[tokio::test]
    async fn cancelled_conditional_write_restores_reservation_and_restart_scans_consistently() {
        let inner = InMemorySemaphorePersistenceBytes::default();
        let domain = bytes(b"cancel-d");
        let name = bytes(b"cancel-s");
        inner.create_semaphore(&domain, &name, 3, 3).await.unwrap();
        let store = Arc::new(ParkGrant {
            inner,
            entered: Notify::new(),
            release: Notify::new(),
            scan_pages: std::sync::atomic::AtomicUsize::new(0),
            park_scan: std::sync::atomic::AtomicBool::new(false),
            fail_scan: std::sync::atomic::AtomicBool::new(false),
            scan_entered: Notify::new(),
            scan_release: Notify::new(), park_confirm: std::sync::atomic::AtomicBool::new(false), cancel_grant: std::sync::atomic::AtomicBool::new(false),
        });
        let registry = Registry::new(store.clone());
        let id = Identifier::new(domain.clone(), name.clone(), 0).unwrap();
        let manager = registry.get_or_create(id.clone()).await.unwrap();
        assert!(
            store.scan_pages.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "startup fixture must page across multiple rows"
        );
        assert_eq!(manager.free_count(), 3);
        let pending = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.acquire(&bytes(b"1:x:r:1"), None).await })
        };
        store.entered.notified().await;
        assert_eq!(manager.free_count(), 2);
        pending.abort();
        let _ = pending.await;
        store.release.notify_one();
        assert_eq!(manager.free_count(), 3);
        let result = manager.acquire(&bytes(b"1:y:r:2"), None).await.unwrap();
        assert_eq!(result.outcome, SemaphoreAcquireOutcome::Acquired);
        assert_eq!(manager.free_count(), 2);
        assert_eq!(
            store
                .get_semaphore_ownership_by_token(&domain, &name, 0, result.token_id)
                .await
                .unwrap(),
            bytes(b"1:y:r:2")
        );
        manager.stop();
        let restarted = registry.get_or_create(id).await.unwrap();
        assert_eq!(restarted.free_count(), 2);
        assert_eq!(
            restarted
                .acquire(&bytes(b"1:y:r:2"), None)
                .await
                .unwrap()
                .token_id,
            result.token_id
        );
    }

    #[tokio::test]
    async fn context_aware_grant_cancellation_restores_reservation() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let domain = bytes(b"cancel-d");
        let name = bytes(b"cancel-s");
        let inner = InMemorySemaphorePersistenceBytes::default();
        inner.create_semaphore(&domain, &name, 1, 1).await.unwrap();
        let store = Arc::new(ParkGrant {
            inner, entered: Notify::new(), release: Notify::new(),
            scan_pages: AtomicUsize::new(0), park_scan: AtomicBool::new(false),
            fail_scan: AtomicBool::new(false), scan_entered: Notify::new(),
            scan_release: Notify::new(), park_confirm: AtomicBool::new(false),
            cancel_grant: AtomicBool::new(true),
        });
        let limiter = crate::matching_persistence_limiter::MatchingPersistenceLimiter::new(
            Arc::new(std::sync::Mutex::new(crate::admin::DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let registry = Registry::new(crate::matching_persistence::limit_semaphore_store_bytes(store.clone(), limiter));
        let manager = registry.get_or_create(Identifier::new(domain, name, 0).unwrap()).await.unwrap();
        let (cancel, receiver) = tokio::sync::watch::channel(false);
        let pending = {
            let manager = manager.clone();
            tokio::spawn(async move {
                let context = SemaphoreCallContext { cancelled: Some(receiver), ..Default::default() };
                manager.acquire_with_context(&bytes(b"1:x:r:1"), &context).await
            })
        };
        store.entered.notified().await;
        assert_eq!(manager.free_count(), 0);
        cancel.send(true).unwrap();
        assert!(matches!(pending.await.unwrap(), Err(ManagerError::Internal(_))));
        assert_eq!(manager.free_count(), 1);
        store.release.notify_one();
        assert_eq!(manager.acquire(&bytes(b"1:y:r:2"), None).await.unwrap().outcome, SemaphoreAcquireOutcome::Acquired);
    }

    #[tokio::test]
    async fn failed_start_releases_waiter_and_next_request_rebuilds_manager() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let inner = InMemorySemaphorePersistenceBytes::default();
        let domain = bytes(b"failure-d");
        let name = bytes(b"failure-s");
        inner.create_semaphore(&domain, &name, 1, 1).await.unwrap();
        let store = Arc::new(ParkGrant {
            inner,
            entered: Notify::new(),
            release: Notify::new(),
            scan_pages: AtomicUsize::new(0),
            park_scan: AtomicBool::new(true),
            fail_scan: AtomicBool::new(true),
            scan_entered: Notify::new(),
            scan_release: Notify::new(), park_confirm: std::sync::atomic::AtomicBool::new(false), cancel_grant: std::sync::atomic::AtomicBool::new(false),
        });
        let registry = Registry::new(store.clone());
        let id = Identifier::new(domain, name, 0).unwrap();
        let start = {
            let registry = registry.clone();
            let id = id.clone();
            tokio::spawn(async move { registry.get_or_create(id).await })
        };
        store.scan_entered.notified().await;
        let manager = registry.get_or_create(id.clone()).await.unwrap();
        let owner = bytes(b"1:waiter:r:1");
        let waiting = manager.acquire(&owner, None);
        tokio::pin!(waiting);
        tokio::select! {
            biased;
            result = &mut waiting => panic!("waiter finished before startup: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        store.scan_release.notify_one();
        assert_eq!(
            start.await.unwrap().err().unwrap(),
            ManagerError::Load(
                id.to_log_string(),
                Box::new(ManagerError::Internal("injected scan failure".into())),
            )
        );
        assert_eq!(waiting.await.unwrap_err(), ManagerError::NotReady);
        assert!(registry.by_identifier(&id).is_none());
        let replacement = registry.get_or_create(id).await.unwrap();
        assert!(!Arc::ptr_eq(&manager, &replacement));
        assert_eq!(store.scan_pages.load(Ordering::SeqCst), 2);
        store.release.notify_one();
        assert_eq!(
            replacement.acquire(&owner, None).await.unwrap().outcome,
            SemaphoreAcquireOutcome::Acquired
        );
        replacement.stop();
    }

    #[tokio::test]
    async fn native_acquire_reacquires_exact_owner_bytes_and_respects_fencing() {
        let store = Arc::new(InMemorySemaphorePersistenceBytes::default());
        let domain = bytes(b"dom\xff");
        let name = bytes(b"sem\xfe\xef\xbf\xbd");
        store.create_semaphore(&domain, &name, 2, 2).await.unwrap();
        // Includes malformed UTF-8 bytes, a distinct U+FFFD spelling, and separator bytes.
        let owner = bytes(b"1:\xff:\xfe\xef\xbf\xbd:1");
        let registry = Registry::new(store.clone());
        let manager = registry
            .get_or_create(Identifier::new(domain.clone(), name.clone(), 0).unwrap())
            .await
            .unwrap();
        let first = manager.acquire(&owner, None).await.unwrap();
        assert_eq!(first.outcome, SemaphoreAcquireOutcome::Acquired);
        assert_eq!(manager.acquire(&owner, None).await.unwrap(), first);
        assert_eq!(
            store
                .get_semaphore_ownership_by_token(&domain, &name, 0, first.token_id)
                .await
                .unwrap(),
            owner
        );

        let replacement = bytes("1:�:�:1".as_bytes());
        assert!(!store
            .release_semaphore_token(&domain, &name, 0, first.token_id, &replacement)
            .await
            .unwrap());
        assert_eq!(
            store
                .get_semaphore_ownership_by_token(&domain, &name, 0, first.token_id)
                .await
                .unwrap(),
            owner
        );
        assert!(store
            .release_semaphore_token(&domain, &name, 0, first.token_id, &owner)
            .await
            .unwrap());
        manager.stop();
        let restarted = registry
            .get_or_create(Identifier::new(domain.clone(), name.clone(), 0).unwrap())
            .await
            .unwrap();
        let again = restarted.acquire(&owner, None).await.unwrap();
        assert_eq!(again.outcome, SemaphoreAcquireOutcome::Acquired);
        assert_eq!(
            store
                .get_semaphore_ownership_by_token(&domain, &name, 0, again.token_id)
                .await
                .unwrap(),
            owner
        );
    }

    #[tokio::test]
    async fn startup_discovers_hold_and_stopped_manager_is_replaced() {
        let store = Arc::new(InMemorySemaphorePersistenceBytes::default());
        let domain = bytes(b"d");
        let name = bytes(b"s");
        store.create_semaphore(&domain, &name, 3, 3).await.unwrap();
        // Seed a hold that startup must discover.
        let held_owner = bytes(b"1:w:r:9");
        store
            .grant_semaphore_token(&domain, &name, 0, 2, &held_owner)
            .await
            .unwrap();
        let registry = Registry::new(store);
        let id = Identifier::new(domain, name, 0).unwrap();
        let old = registry.get_or_create(id.clone()).await.unwrap();
        assert_eq!(old.free_count(), 2);
        old.stop();
        assert!(matches!(
            old.acquire(&bytes(b"1:x:r:1"), None).await,
            Err(ManagerError::NotReady)
        ));
        let replacement = registry.get_or_create(id).await.unwrap();
        assert!(!Arc::ptr_eq(&old, &replacement));
        assert_eq!(replacement.free_count(), 2);
        assert!(registry.by_identifier(replacement.identifier()).is_some());
    }
}
