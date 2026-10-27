//! Matching's read-through cache for durable Go domain metadata.
//!
//! This intentionally contains no domain write path. Reads and refreshes use
//! an injected source so the Go SQL reader, an in-memory compatibility source,
//! and deterministic parity tests share the same cache semantics.

use cadence_persistence::matching_domain_metadata::{
    MatchingDomainMetadata, MatchingDomainMetadataError, MatchingDomainMetadataStore,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const DOMAIN_CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
pub const DOMAIN_CACHE_MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
pub const DOMAIN_CACHE_REFRESH_PAGE_SIZE: usize = 200;
/// Go gives the complete metadata-first refresh scan one three-second context
/// deadline; page requests inherit its remaining time. Each cache-miss
/// existence check gets its own three-second context. The SQLite adapter
/// bounds connection-lock waiting, SQLite busy waiting, and VM execution with
/// the supplied remaining time. Blob decoding happens after the SQL call, as
/// it does in Go's sqlDomainStore, which does not recheck the context there.
pub const DOMAIN_CACHE_PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(3);
const DOMAIN_CACHE_RETRY_INITIAL_INTERVAL: Duration = Duration::from_secs(1);
const DOMAIN_CACHE_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);
const DOMAIN_CACHE_RETRY_EXPIRATION: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainMetadataSourceError {
    pub message: String,
    pub retryable: bool,
    /// Go persistence error type, used only for metered-wrapper counters.
    pub class: DomainErrorClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DomainErrorClass {
    #[default]
    Internal,
    NotFound,
    BadRequest,
    DbUnavailable,
}

impl DomainMetadataSourceError {
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            class: DomainErrorClass::Internal,
        }
    }

    pub fn with_class(mut self, class: DomainErrorClass) -> Self {
        self.class = class;
        self
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            class: DomainErrorClass::Internal,
        }
    }
}

impl From<&str> for DomainMetadataSourceError {
    fn from(message: &str) -> Self {
        Self::permanent(message)
    }
}

impl From<String> for DomainMetadataSourceError {
    fn from(message: String) -> Self {
        Self::permanent(message)
    }
}

pub trait DomainMetadataSource: Send + Sync + 'static {
    /// Normalize IDs only when the backing store's UUID decoder does so.
    /// SQL stores keep their strict UUID handling; gocql accepts UUID hex
    /// with optional hyphens between byte pairs.
    fn normalize_domain_id(&self, id: &str) -> Option<String> {
        Some(id.to_owned())
    }
    fn metadata_version(&self, timeout: Duration) -> Result<i64, DomainMetadataSourceError>;
    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError>;
    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError>;
}

fn metadata_source_error(error: MatchingDomainMetadataError) -> DomainMetadataSourceError {
    use rusqlite::ErrorCode;

    let retryable = match &error {
        // Go's SQLite IsTimeoutError maps context deadlines, SQLITE_INTERRUPT,
        // and SQLITE_BUSY_TIMEOUT to persistence.TimeoutError. That type is
        // not accepted by IsServiceTransientError, so the cache does not retry
        // it. Other SQL failures become InternalServiceError and are retryable.
        MatchingDomainMetadataError::OperationTimedOut => false,
        MatchingDomainMetadataError::Sqlite(rusqlite::Error::SqliteFailure(failure, _)) => {
            failure.code != ErrorCode::OperationInterrupted
                && failure.extended_code != rusqlite::ffi::SQLITE_BUSY_TIMEOUT
        }
        // Go converts other SQL scan/driver failures to InternalServiceError,
        // which the domain-cache retry policy treats as transient.
        MatchingDomainMetadataError::Sqlite(_) => true,
        // Go's MySQL SQL plugin maps driver failures to timeout, throttled,
        // or internal service errors. Domain cache refresh retries those.
        MatchingDomainMetadataError::MySql(_)
        | MatchingDomainMetadataError::MySqlAsync(_)
        | MatchingDomainMetadataError::MySqlPool(_)
        | MatchingDomainMetadataError::Postgres(_)
        | MatchingDomainMetadataError::Cassandra(_) => true,
        _ => false,
    };
    let class = match &error {
        MatchingDomainMetadataError::NotFound(_) => DomainErrorClass::NotFound,
        MatchingDomainMetadataError::BothIdAndName
        | MatchingDomainMetadataError::EmptyIdentity
        | MatchingDomainMetadataError::InvalidUuid(_) => DomainErrorClass::BadRequest,
        _ => DomainErrorClass::Internal,
    };
    DomainMetadataSourceError {
        message: error.to_string(),
        retryable,
        class,
    }
}

fn mysql_metadata_source_error(error: MatchingDomainMetadataError) -> DomainMetadataSourceError {
    if matches!(error, MatchingDomainMetadataError::OperationTimedOut) {
        // Go's SQL MySQL checker wraps context and driver deadlines in
        // persistence.TimeoutError, which its persistence retry policy treats
        // as transient.
        DomainMetadataSourceError::transient(error.to_string())
    } else {
        metadata_source_error(error)
    }
}

fn validate_typed_domain_config(
    domain: &MatchingDomainMetadata,
) -> Result<(), DomainMetadataSourceError> {
    if let Some(error) = domain.config_decode_error.as_deref() {
        return Err(DomainMetadataSourceError::permanent(format!(
            "converting Matching domain metadata: {error}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct DomainMetadataPage {
    pub domains: Vec<MatchingDomainMetadata>,
    pub next_page_token: Option<Vec<u8>>,
}

impl DomainMetadataSource for MatchingDomainMetadataStore {
    fn metadata_version(&self, _timeout: Duration) -> Result<i64, DomainMetadataSourceError> {
        self.get_metadata_version_with_timeout(_timeout)
            .map_err(metadata_source_error)
    }

    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        _timeout: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        let page = MatchingDomainMetadataStore::list_domains_with_timeout(
            self,
            page_size,
            next_page_token,
            _timeout,
        )
        .map_err(metadata_source_error)?;
        for domain in &page.domains {
            validate_typed_domain_config(domain)?;
        }
        Ok(DomainMetadataPage {
            domains: page.domains,
            next_page_token: page.next_page_token,
        })
    }

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        _timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        let domain = MatchingDomainMetadataStore::get_domain_with_timeout(self, name, id, _timeout)
            .map_err(metadata_source_error)?;
        validate_typed_domain_config(&domain)?;
        Ok(domain)
    }
}

impl DomainMetadataSource for cadence_persistence::matching_domain_metadata::CassandraDomainMetadataStore {
    fn normalize_domain_id(&self, id: &str) -> Option<String> {
        gocql_uuid_string(id)
    }

    fn metadata_version(&self, _timeout: Duration) -> Result<i64, DomainMetadataSourceError> {
        self.metadata_version().map_err(|error| DomainMetadataSourceError::transient(error.to_string()))
    }

    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        _timeout: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        let page = cadence_persistence::matching_domain_metadata::CassandraDomainMetadataStore::list_domains(
            self, page_size, next_page_token,
        ).map_err(|error| match error {
            MatchingDomainMetadataError::InvalidPageSize | MatchingDomainMetadataError::InvalidBlob(_) => DomainMetadataSourceError::permanent(error.to_string()),
            _ => DomainMetadataSourceError::transient(error.to_string()),
        })?;
        for domain in &page.domains { validate_typed_domain_config(domain)?; }
        Ok(DomainMetadataPage { domains: page.domains, next_page_token: page.next_page_token })
    }

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        _timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        cadence_persistence::matching_domain_metadata::CassandraDomainMetadataStore::get_domain(self, name, id)
            .map_err(|error| match error {
                MatchingDomainMetadataError::NotFound(_) | MatchingDomainMetadataError::BothIdAndName
                | MatchingDomainMetadataError::EmptyIdentity | MatchingDomainMetadataError::InvalidUuid(_) => {
                    let class = if matches!(error, MatchingDomainMetadataError::NotFound(_)) {
                        DomainErrorClass::NotFound
                    } else {
                        DomainErrorClass::BadRequest
                    };
                    DomainMetadataSourceError::permanent(error.to_string()).with_class(class)
                }
                _ => DomainMetadataSourceError::transient(error.to_string()),
            })
    }
}

/// Mirrors the pinned gocql ParseUUID: 32 hex digits, with hyphens ignored
/// only at byte boundaries. Unlike RFC UUID parsing, hyphens need not be at
/// the canonical 8-4-4-4-12 positions.
fn gocql_uuid_string(input: &str) -> Option<String> {
    let mut bytes = [0_u8; 16];
    let mut digit_index = 0;
    for byte in input.bytes() {
        if byte == b'-' && digit_index % 2 == 0 {
            continue;
        }
        let value = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        if digit_index == 32 {
            return None;
        }
        let shift = if digit_index % 2 == 0 { 4 } else { 0 };
        bytes[digit_index / 2] |= value << shift;
        digit_index += 1;
    }
    (digit_index == 32).then(|| uuid::Uuid::from_bytes(bytes).to_string())
}

impl DomainMetadataSource for cadence_persistence::matching_domain_mysql::MySqlDomainMetadataStore {
    fn metadata_version(&self, timeout: Duration) -> Result<i64, DomainMetadataSourceError> {
        self.get_metadata_version_with_timeout(timeout)
            .map_err(mysql_metadata_source_error)
    }

    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        let page = self
            .list_domains_with_timeout(page_size, next_page_token, timeout)
            .map_err(mysql_metadata_source_error)?;
        for domain in &page.domains {
            validate_typed_domain_config(domain)?;
        }
        Ok(DomainMetadataPage {
            domains: page.domains,
            next_page_token: page.next_page_token,
        })
    }

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        let domain = self
            .get_domain_with_timeout(name, id, timeout)
            .map_err(mysql_metadata_source_error)?;
        validate_typed_domain_config(&domain)?;
        Ok(domain)
    }
}

impl DomainMetadataSource
    for cadence_persistence::matching_domain_postgres::PostgresDomainMetadataStore
{
    fn metadata_version(&self, timeout: Duration) -> Result<i64, DomainMetadataSourceError> {
        self.get_metadata_version_with_timeout(timeout)
            .map_err(postgres_metadata_source_error)
    }

    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        let page = self
            .list_domains_with_timeout(page_size, next_page_token, timeout)
            .map_err(postgres_metadata_source_error)?;
        for domain in &page.domains {
            validate_typed_domain_config(domain)?;
        }
        Ok(DomainMetadataPage {
            domains: page.domains,
            next_page_token: page.next_page_token,
        })
    }

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        let domain = self
            .get_domain_with_timeout(name, id, timeout)
            .map_err(postgres_metadata_source_error)?;
        validate_typed_domain_config(&domain)?;
        Ok(domain)
    }
}

fn postgres_metadata_source_error(error: MatchingDomainMetadataError) -> DomainMetadataSourceError {
    if matches!(error, MatchingDomainMetadataError::OperationTimedOut) {
        DomainMetadataSourceError::transient(error.to_string())
    } else {
        metadata_source_error(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainCacheError {
    EmptyName,
    EmptyId,
    Source(String),
    ExistsButNotLoaded(String),
    Stopped,
}

impl std::fmt::Display for DomainCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName => f.write_str("Domain name is empty"),
            Self::EmptyId => f.write_str("DomainID is empty."),
            Self::Source(error) => f.write_str(error),
            Self::ExistsButNotLoaded(identity) => write!(
                f,
                "DefaultDomainCache encounter case where domain exists but cannot be loaded: {identity}"
            ),
            Self::Stopped => f.write_str("domain cache is stopped"),
        }
    }
}

impl std::error::Error for DomainCacheError {}

impl From<MatchingDomainMetadataError> for DomainCacheError {
    fn from(error: MatchingDomainMetadataError) -> Self {
        Self::Source(error.to_string())
    }
}

pub type PrepareCallback = Arc<dyn Fn() + Send + Sync>;
pub type DomainChangeCallback = Arc<dyn Fn(&[MatchingDomainMetadata]) + Send + Sync>;
pub type CatchUpCallback =
    Arc<dyn Fn(&[MatchingDomainMetadata], &PrepareCallback, &DomainChangeCallback) + Send + Sync>;

#[derive(Default)]
struct CacheState {
    by_id: HashMap<String, MatchingDomainMetadata>,
    name_to_id: HashMap<String, String>,
}

#[derive(Default)]
struct Callbacks {
    prepare: HashMap<String, PrepareCallback>,
    changed: HashMap<String, DomainChangeCallback>,
}

/// Clock seam for deterministic min-refresh and lifecycle tests.
pub trait DomainCacheClock: Send + Sync + 'static {
    fn now(&self) -> Duration;
    fn wait(&self, duration: Duration, stopped: &std::sync::atomic::AtomicBool);
}

pub struct SystemDomainCacheClock {
    origin: std::time::Instant,
}

impl Default for SystemDomainCacheClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl DomainCacheClock for SystemDomainCacheClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
    fn wait(&self, duration: Duration, stopped: &std::sync::atomic::AtomicBool) {
        let deadline = Instant::now() + duration;
        while !stopped.load(std::sync::atomic::Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50)),
            );
        }
    }
}

struct Lifecycle {
    started: bool,
    stopped: bool,
    worker: Option<JoinHandle<()>>,
}

fn join_worker(worker: JoinHandle<()>) {
    if worker.thread().id() != thread::current().id() {
        let _ = worker.join();
    }
}

pub struct MatchingDomainCache {
    source: Arc<dyn DomainMetadataSource>,
    clock: Arc<dyn DomainCacheClock>,
    cache: RwLock<CacheState>,
    callbacks: Mutex<Callbacks>,
    refresh_lock: Mutex<()>,
    last_refresh: Mutex<Option<Duration>>,
    last_source_error_retryable: Mutex<Option<bool>>,
    stopped: Arc<std::sync::atomic::AtomicBool>,
    lifecycle: Mutex<Lifecycle>,
    domain_id_parse: crate::matching_domain_uuid::DomainIdParse,
}

impl MatchingDomainCache {
    /// Construct the production reader from Go's configured defaultStore.
    /// The Matching reader currently supports only file-backed SQLite; it
    /// never falls back to the JSON snapshot or another configured datastore.
    pub fn from_default_store(
        persistence: &crate::config::PersistenceConfig,
        current_cluster: &str,
    ) -> anyhow::Result<Arc<Self>> {
        Self::from_default_store_inner(persistence, current_cluster, None)
    }

    pub fn from_default_store_with_limiter(
        persistence: &crate::config::PersistenceConfig,
        current_cluster: &str,
        limiter: Arc<crate::matching_persistence_limiter::MatchingPersistenceLimiter>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::from_default_store_inner(persistence, current_cluster, Some(limiter))
    }

    fn from_default_store_inner(
        persistence: &crate::config::PersistenceConfig,
        current_cluster: &str,
        limiter: Option<Arc<crate::matching_persistence_limiter::MatchingPersistenceLimiter>>,
    ) -> anyhow::Result<Arc<Self>> {
        // Go indexes persistence.datastores by the literal defaultStore value.
        let name = persistence.default_store.as_str();
        if name.is_empty() {
            anyhow::bail!("Matching domain cache requires persistence.defaultStore");
        }
        let datastore = persistence.datastores.get(name).ok_or_else(|| {
            anyhow::anyhow!(
                "Matching defaultStore {name:?} is not present in persistence.datastores"
            )
        })?;
        if datastore.nosql.is_some() {
            anyhow::bail!(
                "Matching domain metadata reader does not support configured NoSQL defaultStore {name:?}"
            );
        }
        let sql = datastore.sql.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Matching defaultStore {name:?} must configure SQL plugin sqlite")
        })?;
        if sql.plugin_name != "sqlite" {
            anyhow::bail!(
                "Matching domain metadata reader does not support SQL plugin {:?}; supported plugin is sqlite",
                sql.plugin_name
            );
        }
        if sql.database_name.trim().is_empty() || sql.database_name == ":memory:" {
            anyhow::bail!(
                "Matching domain metadata reader requires a file-backed SQLite databaseName"
            );
        }
        // Go's SQLite plugin constructs `file:<databaseName>?<attrs>` without
        // escaping DatabaseName. A literal '?' is therefore already the URI
        // attribute separator, and appending our read-only mode would silently
        // reinterpret the configured DSN. Percent-escaped `%3F` remains valid
        // and is decoded by SQLite on both sides.
        if sql.database_name.contains('?') {
            anyhow::bail!(
                "Matching domain metadata reader does not support SQLite databaseName containing URI query attributes"
            );
        }
        let mut uri_mode = "rwc";
        let mut busy_timeout = Duration::from_secs(60);
        for (key, value) in &sql.connect_attributes {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim();
            match key.as_str() {
                "mode" => uri_mode = value,
                "_pragma.busy_timeout" => {
                    let milliseconds = value.parse::<u64>().map_err(|_| {
                        anyhow::anyhow!("invalid SQLite _pragma.busy_timeout {value:?}")
                    })?;
                    busy_timeout = Duration::from_millis(milliseconds);
                }
                // Go applies these when opening its read/write SQL connection.
                // The journal mode is stored on the database; it does not
                // change SELECT results. `_timefmt` only encodes SQL time
                // columns, while this reader takes times from Thrift DomainInfo
                // blobs and therefore never invokes that codec.
                "_pragma.journal_mode" | "_timefmt" => {}
                _ => anyhow::bail!(
                    "Matching domain metadata reader does not support SQLite connectAttribute {key:?}"
                ),
            }
        }
        let source = cadence_persistence::matching_domain_metadata::MatchingDomainMetadataStore::open_read_only_with_options(
            &sql.database_name,
            current_cluster,
            uri_mode,
            busy_timeout,
        )?;
        let source: Arc<dyn DomainMetadataSource> = Arc::new(source);
        let source = match limiter {
            Some(limiter) => crate::matching_persistence::limit_domain_source(source, limiter),
            None => source,
        };
        // Go's sqlite plugin uses the SQL domain store (MustParseUUID).
        Ok(Arc::new(Self::new(source).with_domain_id_parse(
            crate::matching_domain_uuid::DomainIdParse::GoogleUuidPanic,
        )))
    }

    pub fn new(source: Arc<dyn DomainMetadataSource>) -> Self {
        Self::with_clock(source, Arc::new(SystemDomainCacheClock::default()))
    }

    pub fn with_clock(
        source: Arc<dyn DomainMetadataSource>,
        clock: Arc<dyn DomainCacheClock>,
    ) -> Self {
        Self {
            source,
            clock,
            cache: RwLock::new(CacheState::default()),
            callbacks: Mutex::new(Callbacks::default()),
            refresh_lock: Mutex::new(()),
            last_refresh: Mutex::new(None),
            last_source_error_retryable: Mutex::new(None),
            stopped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            lifecycle: Mutex::new(Lifecycle {
                started: false,
                stopped: false,
                worker: None,
            }),
            domain_id_parse: Default::default(),
        }
    }

    /// Backend parse mode for the pre-handler domain lookup guard (set before sharing).
    pub fn with_domain_id_parse(mut self, mode: crate::matching_domain_uuid::DomainIdParse) -> Self {
        self.domain_id_parse = mode;
        self
    }

    pub fn domain_id_parse(&self) -> crate::matching_domain_uuid::DomainIdParse {
        self.domain_id_parse
    }

    /// Initial scan is synchronous, matching Go Start; a failed initial scan is
    /// returned to the owner instead of terminating the process.
    pub fn start(self: &Arc<Self>) -> Result<(), DomainCacheError> {
        {
            let mut lifecycle = self.lifecycle.lock().expect("domain cache lifecycle lock");
            if lifecycle.started || lifecycle.stopped {
                return Ok(());
            }
            lifecycle.started = true;
        }
        if let Err(error) = self.refresh_domains() {
            self.lifecycle
                .lock()
                .expect("domain cache lifecycle lock")
                .started = false;
            return Err(error);
        }
        let weak_cache = Arc::downgrade(self);
        let clock = Arc::clone(&self.clock);
        let stopped = Arc::clone(&self.stopped);
        let worker = match thread::Builder::new()
            .name("matching-domain-cache".into())
            .spawn(move || {
                while !stopped.load(std::sync::atomic::Ordering::Acquire) {
                    clock.wait(DOMAIN_CACHE_REFRESH_INTERVAL, &stopped);
                    if stopped.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                    if let Some(cache) = weak_cache.upgrade() {
                        let _ = cache.refresh_domains();
                    } else {
                        break;
                    }
                }
            }) {
            Ok(worker) => worker,
            Err(error) => {
                self.lifecycle
                    .lock()
                    .expect("domain cache lifecycle lock")
                    .started = false;
                return Err(DomainCacheError::Source(error.to_string()));
            }
        };
        let mut worker = Some(worker);
        {
            let mut lifecycle = self.lifecycle.lock().expect("domain cache lifecycle lock");
            if !lifecycle.stopped {
                lifecycle.worker = worker.take();
            }
        }
        if let Some(worker) = worker {
            join_worker(worker);
        }
        Ok(())
    }

    pub fn stop(&self) {
        let worker = {
            let mut lifecycle = self.lifecycle.lock().expect("domain cache lifecycle lock");
            if lifecycle.stopped || !lifecycle.started {
                return;
            }
            lifecycle.stopped = true;
            self.stopped
                .store(true, std::sync::atomic::Ordering::Release);
            lifecycle.worker.take()
        };
        if let Some(worker) = worker {
            // The worker temporarily upgrades its Weak cache reference while
            // refreshing. If that reference owns the final Arc, Drop runs on
            // this thread and must not try to join itself.
            join_worker(worker);
        }
    }

    pub fn register_callback(
        &self,
        id: impl Into<String>,
        catch_up: CatchUpCallback,
        prepare: PrepareCallback,
        changed: DomainChangeCallback,
    ) {
        // Refresh publishes callback notifications while holding refresh_lock.
        // Take the same lock across registration and catch-up so a concurrent
        // refresh cannot publish between callback installation and the
        // catch-up snapshot. This uses the same refresh -> callbacks lock order
        // as refresh_domains_locked and avoids a stale baseline race.
        let _refresh = self.refresh_lock.lock().expect("domain refresh lock");
        let id = id.into();
        {
            let mut callbacks = self.callbacks.lock().expect("domain cache callback lock");
            callbacks.prepare.insert(id.clone(), Arc::clone(&prepare));
            callbacks.changed.insert(id, Arc::clone(&changed));
        }
        let entries = self.get_all_domains();
        catch_up(&entries, &prepare, &changed);
    }

    pub fn unregister_callback(&self, id: &str) {
        let mut callbacks = self.callbacks.lock().expect("domain cache callback lock");
        callbacks.prepare.remove(id);
        callbacks.changed.remove(id);
    }

    pub fn get_all_domains(&self) -> Vec<MatchingDomainMetadata> {
        let cache = self.cache.read().expect("domain cache lock");
        cache.by_id.values().cloned().collect()
    }

    pub fn get_cache_size(&self) -> (usize, usize) {
        let cache = self.cache.read().expect("domain cache lock");
        (cache.by_id.len(), cache.name_to_id.len())
    }

    pub fn get_domain(&self, name: &str) -> Result<MatchingDomainMetadata, DomainCacheError> {
        if name.is_empty() {
            return Err(DomainCacheError::EmptyName);
        }
        if let Some(entry) = self.by_name(name) {
            return Ok(entry);
        }
        self.source
            .get_domain(Some(name), None, DOMAIN_CACHE_PERSISTENCE_TIMEOUT)
            .map_err(|error| DomainCacheError::Source(error.message))?;
        self.refresh_after_miss(Some(name), None)?;
        self.by_name(name)
            .ok_or_else(|| DomainCacheError::ExistsButNotLoaded(name.to_string()))
    }

    pub fn get_domain_by_id(&self, id: &str) -> Result<MatchingDomainMetadata, DomainCacheError> {
        if id.is_empty() {
            return Err(DomainCacheError::EmptyId);
        }
        // Keep malformed IDs on the source's normal validation path so its
        // backend-specific error class and precedence remain unchanged.
        let lookup_id = self
            .source
            .normalize_domain_id(id)
            .unwrap_or_else(|| id.to_owned());
        if let Some(entry) = self.by_id(&lookup_id) {
            return Ok(entry);
        }
        self.source
            .get_domain(None, Some(&lookup_id), DOMAIN_CACHE_PERSISTENCE_TIMEOUT)
            .map_err(|error| DomainCacheError::Source(error.message))?;
        self.refresh_after_miss(None, Some(&lookup_id))?;
        self.by_id(&lookup_id)
            .ok_or_else(|| DomainCacheError::ExistsButNotLoaded(id.to_string()))
    }

    pub fn get_domain_id(&self, name: &str) -> Result<String, DomainCacheError> {
        Ok(self.get_domain(name)?.info.uuid)
    }

    pub fn get_domain_name(&self, id: &str) -> Result<String, DomainCacheError> {
        Ok(self.get_domain_by_id(id)?.info.name)
    }

    /// Resolve a domain name from the current snapshot without querying or
    /// refreshing persistence. Telemetry must never make a cache miss block an
    /// RPC handler on the synchronous domain metadata store.
    pub fn cached_domain_name(&self, id: &str) -> Option<String> {
        let id = self.source.normalize_domain_id(id)?;
        self.by_id(&id).map(|domain| domain.info.name)
    }

    fn by_name(&self, name: &str) -> Option<MatchingDomainMetadata> {
        let cache = self.cache.read().expect("domain cache lock");
        cache
            .name_to_id
            .get(name)
            .and_then(|id| cache.by_id.get(id))
            .cloned()
    }

    fn by_id(&self, id: &str) -> Option<MatchingDomainMetadata> {
        self.cache
            .read()
            .expect("domain cache lock")
            .by_id
            .get(id)
            .cloned()
    }

    pub fn refresh_domains(&self) -> Result<(), DomainCacheError> {
        let _refresh = self.refresh_lock.lock().expect("domain refresh lock");
        self.refresh_with_retry()
    }

    /// Go performs the persistence existence check outside the refresh lock,
    /// then reacquires the cache under that lock before doing another scan.
    /// This avoids redundant work when another miss has already populated it.
    fn refresh_after_miss(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<(), DomainCacheError> {
        let _refresh = self.refresh_lock.lock().expect("domain refresh lock");
        if name.is_some_and(|name| self.by_name(name).is_some())
            || id.is_some_and(|id| self.by_id(id).is_some())
        {
            return Ok(());
        }
        self.refresh_with_retry()
    }

    fn refresh_with_retry(&self) -> Result<(), DomainCacheError> {
        let started = self.clock.now();
        let mut retry_count: u32 = 0;
        let mut previous_error = None;
        loop {
            match self.refresh_domains_locked() {
                Ok(()) => return Ok(()),
                Err(DomainCacheError::Source(message)) => {
                    let retryable = self
                        .last_source_error_retryable
                        .lock()
                        .expect("domain cache source error lock")
                        .take()
                        .unwrap_or(false);
                    if !retryable {
                        return Err(previous_error.unwrap_or(DomainCacheError::Source(message)));
                    }
                    let elapsed = self.clock.now().saturating_sub(started);
                    if elapsed > DOMAIN_CACHE_RETRY_EXPIRATION {
                        return Err(previous_error.unwrap_or(DomainCacheError::Source(message)));
                    }
                    let exponential = DOMAIN_CACHE_RETRY_INITIAL_INTERVAL
                        .saturating_mul(2_u32.saturating_pow(retry_count))
                        .min(DOMAIN_CACHE_RETRY_MAX_INTERVAL);
                    let remaining = DOMAIN_CACHE_RETRY_EXPIRATION.saturating_sub(elapsed);
                    let interval = exponential.min(remaining);
                    // Go's ComputeNextDelay stops instead of scheduling a
                    // shortened retry when the remaining budget is below the
                    // initial one-second interval.
                    if interval < DOMAIN_CACHE_RETRY_INITIAL_INTERVAL {
                        return Err(previous_error.unwrap_or(DomainCacheError::Source(message)));
                    }
                    let prior_error = previous_error;
                    let jitter_portion = interval / 5;
                    let jitter_nanos = jitter_portion.as_nanos().min(u128::from(u64::MAX)) as u64;
                    let jitter = if jitter_nanos == 0 {
                        Duration::ZERO
                    } else {
                        Duration::from_nanos(rand::Rng::gen_range(
                            &mut rand::thread_rng(),
                            0..jitter_nanos,
                        ))
                    };
                    let delay = interval
                        .saturating_sub(jitter_portion)
                        .saturating_add(jitter);
                    self.clock.wait(delay, &self.stopped);
                    if self.stopped.load(std::sync::atomic::Ordering::Acquire) {
                        return Err(prior_error.unwrap_or(DomainCacheError::Source(message)));
                    }
                    previous_error = Some(DomainCacheError::Source(message));
                    retry_count = retry_count.saturating_add(1);
                }
                Err(error) => return Err(previous_error.unwrap_or(error)),
            }
        }
    }

    fn refresh_domains_locked(&self) -> Result<(), DomainCacheError> {
        if self
            .lifecycle
            .lock()
            .expect("domain cache lifecycle lock")
            .stopped
        {
            return Err(DomainCacheError::Stopped);
        }
        let now = self.clock.now();
        if self
            .last_refresh
            .lock()
            .expect("domain refresh time lock")
            .is_some_and(|last| now.saturating_sub(last) < DOMAIN_CACHE_MIN_REFRESH_INTERVAL)
        {
            return Ok(());
        }

        // Go reads metadata first. Any failed page leaves both existing maps
        // untouched, so readers never observe a partial scan.
        // Go derives each page-call context from the metadata context, so the
        // whole metadata+pages scan shares one three-second deadline.
        let refresh_deadline = Instant::now() + DOMAIN_CACHE_PERSISTENCE_TIMEOUT;
        let metadata_version = self
            .source
            .metadata_version(refresh_deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| {
                *self
                    .last_source_error_retryable
                    .lock()
                    .expect("domain cache source error lock") = Some(error.retryable);
                DomainCacheError::Source(error.message)
            })?;
        let mut token: Option<Vec<u8>> = None;
        let mut records = Vec::new();
        loop {
            let page = self
                .source
                .list_domains(
                    DOMAIN_CACHE_REFRESH_PAGE_SIZE,
                    token.as_deref(),
                    refresh_deadline.saturating_duration_since(Instant::now()),
                )
                .map_err(|error| {
                    *self
                        .last_source_error_retryable
                        .lock()
                        .expect("domain cache source error lock") = Some(error.retryable);
                    DomainCacheError::Source(error.message)
                })?;
            if let Some(error) = page
                .domains
                .iter()
                .find_map(|domain| domain.config_decode_error.as_deref())
            {
                *self
                    .last_source_error_retryable
                    .lock()
                    .expect("domain cache source error lock") = Some(false);
                return Err(DomainCacheError::Source(format!(
                    "converting Matching domain metadata: {error}"
                )));
            }
            records.extend(page.domains);
            token = page.next_page_token;
            if token.is_none() {
                break;
            }
        }
        records.sort_by_key(|record| record.notification_version);
        let old = self.cache.read().expect("domain cache lock");
        let mut next = CacheState {
            by_id: old.by_id.clone(),
            name_to_id: old.name_to_id.clone(),
        };
        drop(old);
        let mut changed_entries = Vec::new();
        for mut record in records {
            if record.notification_version >= metadata_version {
                break;
            }
            if let Some(id) = self.source.normalize_domain_id(&record.info.uuid) {
                record.info.uuid = id;
            }
            let previous = next.by_id.get(&record.info.uuid);
            if previous
                .is_some_and(|entry| record.notification_version > entry.notification_version)
            {
                changed_entries.push(record.clone());
            }
            next.name_to_id
                .insert(record.info.name.clone(), record.info.uuid.clone());
            next.by_id.insert(record.info.uuid.clone(), record);
        }

        // Match Go's prepare -> atomic map publication -> callback ordering.
        let callbacks = self.callbacks.lock().expect("domain cache callback lock");
        for prepare in callbacks.prepare.values() {
            prepare();
        }
        *self.cache.write().expect("domain cache lock") = next;
        for changed in callbacks.changed.values() {
            changed(&changed_entries);
        }
        drop(callbacks);
        *self.last_refresh.lock().expect("domain refresh time lock") = Some(now);
        Ok(())
    }
}

impl Drop for MatchingDomainCache {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::{DomainConfiguration, DomainInfo, DomainReplicationConfiguration};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::{Receiver, SyncSender};

    #[test]
    fn mysql_persistence_timeouts_are_transient_for_domain_refresh() {
        let error = mysql_metadata_source_error(MatchingDomainMetadataError::OperationTimedOut);
        assert!(error.retryable);
    }

    #[test]
    fn production_source_uses_only_the_selected_sqlite_default_store() {
        let empty = crate::config::PersistenceConfig::default();
        assert!(MatchingDomainCache::from_default_store(&empty, "active")
            .err()
            .unwrap()
            .to_string()
            .contains("persistence.defaultStore"));

        let padded_name = crate::config::PersistenceConfig {
            default_store: " sqlite ".into(),
            datastores: HashMap::from([(
                "sqlite".into(),
                crate::config::DatastoreConfig::default(),
            )]),
            ..Default::default()
        };
        assert!(
            MatchingDomainCache::from_default_store(&padded_name, "active")
                .err()
                .unwrap()
                .to_string()
                .contains("defaultStore \" sqlite \" is not present")
        );

        let unsupported = crate::config::PersistenceConfig {
            default_store: "no-sql".into(),
            datastores: HashMap::from([(
                "no-sql".into(),
                crate::config::DatastoreConfig {
                    nosql: Some(crate::config::NosqlConfig {
                        plugin_name: "cassandra".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        assert!(
            MatchingDomainCache::from_default_store(&unsupported, "active")
                .err()
                .unwrap()
                .to_string()
                .contains("does not support configured NoSQL")
        );

        let wrong_sql = crate::config::PersistenceConfig {
            default_store: "mysql".into(),
            datastores: HashMap::from([(
                "mysql".into(),
                crate::config::DatastoreConfig {
                    sql: Some(crate::config::SqlConfig {
                        plugin_name: "mysql".into(),
                        database_name: "/tmp/cadence.db".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        assert!(
            MatchingDomainCache::from_default_store(&wrong_sql, "active")
                .err()
                .unwrap()
                .to_string()
                .contains("supported plugin is sqlite")
        );

        let ambiguous_path = crate::config::PersistenceConfig {
            default_store: "sqlite".into(),
            datastores: HashMap::from([(
                "sqlite".into(),
                crate::config::DatastoreConfig {
                    sql: Some(crate::config::SqlConfig {
                        plugin_name: "sqlite".into(),
                        database_name: "/tmp/cadence?mode=memory".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        assert!(
            MatchingDomainCache::from_default_store(&ambiguous_path, "active")
                .err()
                .unwrap()
                .to_string()
                .contains("databaseName containing URI query attributes")
        );
    }

    #[derive(Default)]
    struct FakeClock(AtomicU64);
    impl DomainCacheClock for FakeClock {
        fn now(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
        fn wait(&self, duration: Duration, stopped: &std::sync::atomic::AtomicBool) {
            if !stopped.load(Ordering::Acquire) {
                let elapsed = duration
                    .as_secs()
                    .saturating_add(u64::from(duration.subsec_nanos() > 0));
                self.0.fetch_add(elapsed, Ordering::SeqCst);
            }
        }
    }

    struct ExpiringClock(AtomicU64);
    impl DomainCacheClock for ExpiringClock {
        fn now(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
        fn wait(&self, _duration: Duration, stopped: &std::sync::atomic::AtomicBool) {
            if !stopped.load(Ordering::Acquire) {
                self.0.store(120, Ordering::SeqCst);
            }
        }
    }

    #[derive(Default)]
    struct FakeSource {
        gocql_ids: bool,
        version: Mutex<i64>,
        records: Mutex<Vec<MatchingDomainMetadata>>,
        yield_page_size: Mutex<Option<usize>>,
        fail_page: Mutex<Option<usize>>,
        retryable_page_failures: Mutex<usize>,
        retryable_page_messages: Mutex<Vec<String>>,
        calls: Mutex<Vec<String>>,
        timeouts: Mutex<Vec<Duration>>,
        metadata_delay: Mutex<Duration>,
        metadata_gate: Mutex<Option<(SyncSender<()>, Receiver<()>)>>,
    }
    impl FakeSource {
        fn put(&self, record: MatchingDomainMetadata) {
            let mut records = self.records.lock().unwrap();
            if let Some(existing) = records
                .iter_mut()
                .find(|item| item.info.uuid == record.info.uuid)
            {
                *existing = record;
            } else {
                records.push(record);
            }
        }
    }
    impl DomainMetadataSource for FakeSource {
        fn normalize_domain_id(&self, id: &str) -> Option<String> {
            if self.gocql_ids {
                gocql_uuid_string(id)
            } else {
                Some(id.to_owned())
            }
        }
        fn metadata_version(&self, timeout: Duration) -> Result<i64, DomainMetadataSourceError> {
            assert!(!timeout.is_zero() && timeout <= DOMAIN_CACHE_PERSISTENCE_TIMEOUT);
            self.calls.lock().unwrap().push("metadata".into());
            self.timeouts.lock().unwrap().push(timeout);
            if let Some((entered, release)) = self.metadata_gate.lock().unwrap().take() {
                entered.send(()).unwrap();
                release.recv().unwrap();
            }
            thread::sleep(*self.metadata_delay.lock().unwrap());
            Ok(*self.version.lock().unwrap())
        }
        fn list_domains(
            &self,
            page_size: usize,
            token: Option<&[u8]>,
            timeout: Duration,
        ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
            assert_eq!(page_size, DOMAIN_CACHE_REFRESH_PAGE_SIZE);
            assert!(!timeout.is_zero() && timeout <= DOMAIN_CACHE_PERSISTENCE_TIMEOUT);
            self.timeouts.lock().unwrap().push(timeout);
            let offset = token
                .and_then(|bytes| bytes.try_into().ok())
                .map(usize::from_be_bytes)
                .unwrap_or(0);
            self.calls.lock().unwrap().push(format!("page:{offset}"));
            if *self.fail_page.lock().unwrap() == Some(offset) {
                let mut remaining = self.retryable_page_failures.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    let message = self
                        .retryable_page_messages
                        .lock()
                        .unwrap()
                        .pop()
                        .unwrap_or_else(|| "injected transient page failure".into());
                    return Err(DomainMetadataSourceError::transient(message));
                }
                return Err(DomainMetadataSourceError::permanent(
                    "injected page failure",
                ));
            }
            let records = self.records.lock().unwrap();
            let returned_page_size = self.yield_page_size.lock().unwrap().unwrap_or(page_size);
            let end = (offset + returned_page_size).min(records.len());
            let domains = records.get(offset..end).unwrap_or_default().to_vec();
            let next_page_token = (end < records.len()).then(|| end.to_be_bytes().to_vec());
            Ok(DomainMetadataPage {
                domains,
                next_page_token,
            })
        }
        fn get_domain(
            &self,
            name: Option<&str>,
            id: Option<&str>,
            timeout: Duration,
        ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
            assert!(!timeout.is_zero() && timeout <= DOMAIN_CACHE_PERSISTENCE_TIMEOUT);
            self.timeouts.lock().unwrap().push(timeout);
            self.calls
                .lock()
                .unwrap()
                .push(format!("get:{}", name.or(id).unwrap_or("")));
            let domain = self
                .records
                .lock()
                .unwrap()
                .iter()
                .find(|record| {
                    name.is_some_and(|name| record.info.name == name)
                        || id.is_some_and(|id| record.info.uuid == id)
                })
                .cloned()
                .ok_or_else(|| {
                    DomainMetadataSourceError::permanent(format!(
                        "Domain {} does not exist.",
                        name.or(id).unwrap_or("")
                    ))
                })?;
            validate_typed_domain_config(&domain)?;
            Ok(domain)
        }
    }

    fn record(id: usize, notification_version: i64) -> MatchingDomainMetadata {
        let name = format!("domain-{id}");
        MatchingDomainMetadata {
            info: DomainInfo {
                name,
                uuid: format!("{id:032x}"),
                ..DomainInfo::default()
            },
            configuration: DomainConfiguration::default(),
            replication: DomainReplicationConfiguration::default(),
            is_global_domain: false,
            archival_bucket: String::new(),
            status_raw: 0,
            archival_status_raw: 0,
            history_archival_status_raw: 0,
            visibility_archival_status_raw: 0,
            config_version: notification_version,
            notification_version,
            failover_notification_version: notification_version,
            failover_version: notification_version,
            previous_failover_version: -1,
            failover_end_time_nanos: None,
            last_updated_time_nanos: 0,
            bad_binaries: None,
            isolation_groups: None,
            async_workflow_config: None,
            active_clusters_config: None,
            decoded_bad_binaries: None,
            decoded_isolation_groups: None,
            decoded_async_workflow_config: None,
            config_decode_error: None,
        }
    }

    #[test]
    fn cassandra_uuid_spellings_resolve_to_the_same_cached_domain() {
        assert_eq!(
            gocql_uuid_string("0000000000000000000000000000000A").as_deref(),
            Some("00000000-0000-0000-0000-00000000000a")
        );
        assert_eq!(
            gocql_uuid_string("00000000-0000-0000-0000-00000000000A").as_deref(),
            Some("00000000-0000-0000-0000-00000000000a")
        );
        assert!(gocql_uuid_string("0000000-0000-0000-0000-00000000000a").is_none());
        assert!(gocql_uuid_string("00000000-0000-0000-0000-00000000000ag").is_none());

        let source = Arc::new(FakeSource {
            gocql_ids: true,
            ..FakeSource::default()
        });
        let mut metadata = record(10, 1);
        metadata.info.uuid = "00000000-0000-0000-0000-00000000000A".into();
        metadata.info.name = "quota-label-domain".into();
        source.put(metadata);
        *source.version.lock().unwrap() = 2;
        let cache = MatchingDomainCache::new(source);
        cache.refresh_domains().unwrap();

        assert_eq!(
            cache.cached_domain_name("0000000000000000000000000000000a").as_deref(),
            Some("quota-label-domain")
        );
        assert!(cache.cached_domain_name("not-a-uuid").is_none());
    }

    #[test]
    fn non_cassandra_domain_cache_keeps_uuid_keys_exact() {
        let source = Arc::new(FakeSource::default());
        let mut metadata = record(10, 1);
        metadata.info.uuid = "00000000-0000-0000-0000-00000000000a".into();
        metadata.info.name = "sql-domain".into();
        source.put(metadata);
        *source.version.lock().unwrap() = 2;
        let cache = MatchingDomainCache::new(source);
        cache.refresh_domains().unwrap();

        assert_eq!(
            cache.cached_domain_name("00000000-0000-0000-0000-00000000000a").as_deref(),
            Some("sql-domain")
        );
        assert!(cache
            .cached_domain_name("0000000000000000000000000000000a")
            .is_none());
    }

    fn oracle_record(
        id: usize,
        name: &str,
        notification: i64,
        config_version: i64,
        failover_version: i64,
        failover_notification_version: i64,
    ) -> MatchingDomainMetadata {
        let mut domain = record(id, notification);
        domain.info.uuid = format!("00000000-0000-0000-0000-{id:012x}");
        domain.info.name = name.to_string();
        domain
            .configuration
            .workflow_execution_retention_period_in_days = i32::try_from(config_version).unwrap();
        domain.config_version = config_version;
        domain.failover_version = failover_version;
        domain.failover_notification_version = failover_notification_version;
        domain.previous_failover_version = 3;
        domain
    }

    fn cache(source: &Arc<FakeSource>, clock: &Arc<FakeClock>) -> Arc<MatchingDomainCache> {
        Arc::new(MatchingDomainCache::with_clock(
            Arc::clone(source) as Arc<dyn DomainMetadataSource>,
            Arc::clone(clock) as Arc<dyn DomainCacheClock>,
        ))
    }

    #[test]
    fn callback_catch_up_serializes_with_refresh_publication() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 10;
        source.put(record(1, 1));
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();

        let (snapshot_tx, snapshot_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_observed = Arc::clone(&observed);
        let callback_release_rx = Arc::clone(&release_rx);
        let catch_up: CatchUpCallback = Arc::new(move |entries, _prepare, changed| {
            snapshot_tx.send(entries[0].notification_version).unwrap();
            callback_release_rx.lock().unwrap().recv().unwrap();
            changed(entries);
        });
        let changed: DomainChangeCallback = Arc::new(move |entries| {
            callback_observed
                .lock()
                .unwrap()
                .extend(entries.iter().map(|entry| entry.notification_version));
        });
        let registering_cache = Arc::clone(&cache);
        let register = thread::spawn(move || {
            registering_cache.register_callback("test", catch_up, Arc::new(|| {}), changed);
        });

        assert_eq!(snapshot_rx.recv().unwrap(), 1);
        assert!(cache.refresh_lock.try_lock().is_err());
        release_tx.send(()).unwrap();
        register.join().unwrap();

        source.put(record(1, 2));
        clock.0.store(2, Ordering::SeqCst);
        cache.refresh_domains().unwrap();
        assert_eq!(*observed.lock().unwrap(), [1, 2]);
        cache.unregister_callback("test");
    }

    #[test]
    fn typed_domain_config_error_during_refresh_keeps_raw_cache_snapshot() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 10;
        source.put(record(1, 1));
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();

        *source.version.lock().unwrap() = 11;
        let mut malformed = record(2, 11);
        malformed.config_decode_error = Some("invalid opaque domain config".into());
        source.put(malformed);
        clock.0.store(2, Ordering::SeqCst);
        let error = cache.refresh_domains().unwrap_err().to_string();
        assert!(error.contains("invalid opaque domain config"));
        assert_eq!(
            cache.get_domain_id("domain-1").unwrap(),
            format!("{:032x}", 1)
        );
        assert!(cache.get_domain_id("domain-2").is_err());
    }

    #[test]
    fn malformed_typed_domain_config_fails_cache_miss_without_loading_entry() {
        let source = Arc::new(FakeSource::default());
        let mut malformed = record(1, 1);
        malformed.config_decode_error = Some("invalid opaque domain config".into());
        source.put(malformed);
        let cache = cache(&source, &Arc::new(FakeClock::default()));

        let error = cache.get_domain("domain-1").unwrap_err();
        assert!(error.to_string().contains("invalid opaque domain config"));
        assert_eq!(cache.get_cache_size(), (0, 0));
        assert_eq!(*source.calls.lock().unwrap(), ["get:domain-1"]);
    }

    #[test]
    fn trace_matches_pinned_go_domain_cache_scan_update_callback_error_and_misses() {
        let id1 = "00000000-0000-0000-0000-000000000001";
        let id2 = "00000000-0000-0000-0000-000000000002";
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 3;
        *source.yield_page_size.lock().unwrap() = Some(1);
        source.put(oracle_record(1, "alpha", 1, 5, 7, 2));
        source.put(oracle_record(2, "beta", 3, 9, 11, 3));
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();

        let mut initial_ids: Vec<_> = cache
            .get_all_domains()
            .into_iter()
            .map(|domain| domain.info.uuid)
            .collect();
        initial_ids.sort();
        let catchup_max = Arc::new(Mutex::new(0_i64));
        let catchup_max_callback = Arc::clone(&catchup_max);
        let catch_up: CatchUpCallback = Arc::new(move |domains, _, _| {
            *catchup_max_callback.lock().unwrap() = domains
                .iter()
                .map(|domain| domain.failover_notification_version)
                .max()
                .unwrap_or_default();
        });
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let prepare_events = Arc::clone(&events);
        let changed_events = Arc::clone(&events);
        cache.register_callback(
            "matching",
            catch_up,
            Arc::new(move || prepare_events.lock().unwrap().push("prepare".into())),
            Arc::new(move |domains| {
                for domain in domains {
                    changed_events.lock().unwrap().push(format!(
                        "changed:{}:{}",
                        domain.info.uuid, domain.notification_version
                    ));
                }
            }),
        );

        *source.version.lock().unwrap() = 4;
        source.put(oracle_record(1, "alpha", 2, 6, 8, 4));
        clock.0.store(2, Ordering::SeqCst);
        cache.refresh_domains().unwrap();

        *source.version.lock().unwrap() = 5;
        source.put(oracle_record(1, "alpha", 4, 8, 10, 5));
        clock.0.store(3, Ordering::SeqCst);
        *source.fail_page.lock().unwrap() = Some(1);
        let page_error = cache.refresh_domains().unwrap_err().to_string();
        *source.fail_page.lock().unwrap() = None;

        let missing_name = cache.get_domain("late").is_err();
        let missing_id = cache
            .get_domain_by_id("00000000-0000-0000-0000-000000000003")
            .is_err();
        let mut final_entries: Vec<_> = cache
            .get_all_domains()
            .into_iter()
            .map(|domain| {
                serde_json::json!({
                    "ID": domain.info.uuid,
                    "Name": domain.info.name,
                    "Notification": domain.notification_version,
                    "Config": domain.config_version,
                    "Failover": domain.failover_version,
                    "FailoverNotification": domain.failover_notification_version,
                    "Retention": domain.configuration.workflow_execution_retention_period_in_days,
                })
            })
            .collect();
        final_entries.sort_by(|left, right| left["ID"].as_str().cmp(&right["ID"].as_str()));
        let trace = serde_json::json!({
            "MetadataFirstPageSize": DOMAIN_CACHE_REFRESH_PAGE_SIZE,
            "InitialIDs": initial_ids,
            "CatchupMax": *catchup_max.lock().unwrap(),
            "Events": *events.lock().unwrap(),
            "FinalEntries": final_entries,
            "PageError": page_error,
            "RetainedNotification": cache.by_id(id1).unwrap().notification_version,
            "NameMiss": missing_name,
            "IDMiss": missing_id,
        });
        assert_eq!(
            trace,
            serde_json::json!({
                "MetadataFirstPageSize": 200,
                "InitialIDs": [id1],
                "CatchupMax": 2,
                "Events": ["prepare", format!("changed:{id1}:2")],
                "FinalEntries": [
                    {
                        "ID": id1,
                        "Name": "alpha",
                        "Notification": 2,
                        "Config": 6,
                        "Failover": 8,
                        "FailoverNotification": 4,
                        "Retention": 6
                    },
                    {
                        "ID": id2,
                        "Name": "beta",
                        "Notification": 3,
                        "Config": 9,
                        "Failover": 11,
                        "FailoverNotification": 3,
                        "Retention": 9
                    }
                ],
                "PageError": "injected page failure",
                "RetainedNotification": 2,
                "NameMiss": true,
                "IDMiss": true
            })
        );
        eprintln!("RUST_DOMAIN_CACHE_TRACE={trace}");
    }

    #[test]
    fn metadata_first_pages_of_two_hundred_and_notification_cutoff_match_go_scan() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 202;
        for id in 0..201 {
            source.put(record(id, i64::try_from(id).unwrap()));
        }
        source.put(record(201, 203)); // Go eligibility is strictly notification < metadata.
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();
        assert_eq!(cache.get_cache_size().0, 201);
        assert_eq!(
            &source.calls.lock().unwrap()[..3],
            &["metadata", "page:0", "page:200"]
        );
        assert!(cache.get_domain_by_id(&format!("{:032x}", 201)).is_err());
    }

    #[test]
    fn failed_later_page_preserves_old_snapshot_and_retry_refreshes() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 202;
        source.put(record(1, 1));
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();
        for id in 2..201 {
            source.put(record(id, i64::try_from(id).unwrap()));
        }
        source.put(record(201, 201));
        source.put(record(1, 202)); // metadata version itself is not eligible
        clock.0.store(2, Ordering::SeqCst);
        *source.fail_page.lock().unwrap() = Some(200);
        assert!(cache.refresh_domains().is_err());
        assert_eq!(
            cache
                .get_domain_by_id(&format!("{:032x}", 1))
                .unwrap()
                .notification_version,
            1
        );
        *source.fail_page.lock().unwrap() = None;
        cache.refresh_domains().unwrap();
        assert_eq!(cache.get_cache_size(), (201, 201));
        assert_eq!(
            cache
                .by_id(&format!("{:032x}", 1))
                .unwrap()
                .notification_version,
            1
        );
        assert!(cache.by_id(&format!("{:032x}", 201)).is_some());
    }

    #[test]
    fn cache_misses_read_through_and_can_retry_after_domain_appears() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 10;
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        assert!(cache.get_domain("late").is_err());
        source.put(record(4, 1));
        // ID misses check persistence and then hydrate through the metadata scan.
        let id = format!("{:032x}", 4);
        assert_eq!(cache.get_domain_by_id(&id).unwrap().info.name, "domain-4");
        // The name is now cached, while a second new name exercises name read-through.
        source.put(record(5, 2));
        clock.0.store(1, Ordering::SeqCst);
        assert_eq!(
            cache.get_domain("domain-5").unwrap().info.uuid,
            format!("{:032x}", 5)
        );
        assert_eq!(cache.get_domain_name(&id).unwrap(), "domain-4");
        assert_eq!(
            source.calls.lock().unwrap().as_slice(),
            vec![
                "get:late".to_string(),
                format!("get:{id}"),
                "metadata".to_string(),
                "page:0".to_string(),
                "get:domain-5".to_string(),
                "metadata".to_string(),
                "page:0".to_string()
            ]
            .as_slice()
        );
    }

    #[test]
    fn duplicate_callback_registration_replaces_pair_and_refresh_pairs_prepare_with_change() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 2;
        source.put(record(1, 1));
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();
        let counts = Arc::new(Mutex::new(Vec::<String>::new()));
        let old = Arc::clone(&counts);
        let catch_up: CatchUpCallback = Arc::new(|_, _, _| {});
        cache.register_callback(
            "matching",
            Arc::clone(&catch_up),
            Arc::new(move || old.lock().unwrap().push("old-prepare".into())),
            Arc::new(|_| {}),
        );
        let new = Arc::clone(&counts);
        cache.register_callback(
            "matching",
            catch_up,
            Arc::new(move || new.lock().unwrap().push("prepare".into())),
            {
                let new = Arc::clone(&counts);
                Arc::new(move |entries| {
                    new.lock()
                        .unwrap()
                        .push(format!("callback:{}", entries.len()))
                })
            },
        );
        source.put(record(1, 2));
        *source.version.lock().unwrap() = 3;
        clock.0.store(2, Ordering::SeqCst);
        cache.refresh_domains().unwrap();
        assert_eq!(*counts.lock().unwrap(), ["prepare", "callback:1"]);
    }

    #[test]
    fn catch_up_registration_observes_current_entries_after_callbacks_are_installed() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 4;
        source.put(record(1, 1));
        source.put(record(2, 3));
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();

        let observed = Arc::new(Mutex::new(None));
        let observed_catchup = Arc::clone(&observed);
        let catch_up: CatchUpCallback = Arc::new(move |entries, prepare, changed| {
            prepare();
            changed(entries);
            *observed_catchup.lock().unwrap() = Some(
                entries
                    .iter()
                    .map(|entry| entry.failover_notification_version)
                    .max()
                    .unwrap_or_default(),
            );
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        let prepare_events = Arc::clone(&events);
        let callback_events = Arc::clone(&events);
        cache.register_callback(
            "matching",
            catch_up,
            Arc::new(move || prepare_events.lock().unwrap().push("prepare")),
            Arc::new(move |entries| {
                callback_events.lock().unwrap().push(if entries.len() == 2 {
                    "catch-up"
                } else {
                    "refresh"
                });
            }),
        );
        assert_eq!(*observed.lock().unwrap(), Some(3));
        assert_eq!(*events.lock().unwrap(), ["prepare", "catch-up"]);
    }

    #[test]
    fn cache_retains_independent_config_failover_versions_and_raw_nested_blobs() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 13;
        let mut domain = record(1, 12);
        domain.config_version = 5;
        domain.failover_version = 7;
        domain.failover_notification_version = 11;
        domain.previous_failover_version = 3;
        domain.failover_end_time_nanos = Some(1_728_000_000_000_000_000);
        domain.bad_binaries = Some(
            cadence_persistence::matching_domain_metadata::GoDomainBlob {
                data: vec![0, 1, 2, 255],
                encoding: "thriftrw_snappy".into(),
            },
        );
        domain.isolation_groups = Some(
            cadence_persistence::matching_domain_metadata::GoDomainBlob {
                data: vec![3, 4, 5],
                encoding: "thriftrw".into(),
            },
        );
        source.put(domain.clone());
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();

        let cached = cache.by_id(&domain.info.uuid).unwrap();
        assert_eq!(cached.config_version, 5);
        assert_eq!(cached.failover_version, 7);
        assert_eq!(cached.failover_notification_version, 11);
        assert_eq!(cached.previous_failover_version, 3);
        assert_eq!(
            cached.failover_end_time_nanos,
            Some(1_728_000_000_000_000_000)
        );
        assert_eq!(cached.bad_binaries, domain.bad_binaries);
        assert_eq!(cached.isolation_groups, domain.isolation_groups);
    }

    #[test]
    fn refresh_is_throttled_for_one_second_after_success_but_not_after_failure() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 1;
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);
        cache.refresh_domains().unwrap();
        assert_eq!(source.calls.lock().unwrap().len(), 2);

        source.calls.lock().unwrap().clear();
        cache.refresh_domains().unwrap();
        assert!(source.calls.lock().unwrap().is_empty());

        *source.fail_page.lock().unwrap() = Some(0);
        clock.0.store(1, Ordering::SeqCst);
        assert!(cache.refresh_domains().is_err());
        source.calls.lock().unwrap().clear();
        *source.fail_page.lock().unwrap() = None;
        cache.refresh_domains().unwrap();
        assert_eq!(
            source.calls.lock().unwrap().as_slice(),
            ["metadata", "page:0"]
        );
    }

    #[test]
    fn page_calls_inherit_remaining_metadata_refresh_deadline() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 1;
        *source.metadata_delay.lock().unwrap() = Duration::from_millis(20);
        let clock = Arc::new(FakeClock::default());
        cache(&source, &clock).refresh_domains().unwrap();
        let timeouts = source.timeouts.lock().unwrap();
        assert_eq!(timeouts.len(), 2);
        assert!(timeouts[1] < timeouts[0]);
        assert!(timeouts[0] - timeouts[1] >= Duration::from_millis(10));
    }

    #[test]
    fn retryable_refresh_error_retries_and_returns_previous_error_if_retry_fails() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 1;
        *source.fail_page.lock().unwrap() = Some(0);
        *source.retryable_page_failures.lock().unwrap() = 1;
        let clock = Arc::new(FakeClock::default());
        let cache = cache(&source, &clock);

        assert_eq!(
            cache.refresh_domains(),
            Err(DomainCacheError::Source(
                "injected transient page failure".into()
            ))
        );
        assert_eq!(
            source.calls.lock().unwrap().as_slice(),
            ["metadata", "page:0", "metadata", "page:0"]
        );
    }

    #[test]
    fn retry_expiration_below_initial_interval_returns_previous_attempt_error() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 1;
        *source.fail_page.lock().unwrap() = Some(0);
        *source.retryable_page_failures.lock().unwrap() = 2;
        *source.retryable_page_messages.lock().unwrap() =
            vec!["second transient".into(), "first transient".into()];
        let clock = Arc::new(ExpiringClock(AtomicU64::new(0)));
        let cache = Arc::new(MatchingDomainCache::with_clock(
            Arc::clone(&source) as Arc<dyn DomainMetadataSource>,
            clock,
        ));
        assert_eq!(
            cache.refresh_domains(),
            Err(DomainCacheError::Source("first transient".into()))
        );
        assert_eq!(
            source.calls.lock().unwrap().as_slice(),
            ["metadata", "page:0", "metadata", "page:0"]
        );
    }

    #[test]
    fn sqlite_timeout_errors_follow_go_sql_error_conversion_retryability() {
        let sqlite_error = |code| {
            MatchingDomainMetadataError::Sqlite(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            ))
        };
        assert!(!metadata_source_error(MatchingDomainMetadataError::OperationTimedOut).retryable);
        assert!(metadata_source_error(sqlite_error(rusqlite::ffi::SQLITE_BUSY)).retryable);
        assert!(metadata_source_error(sqlite_error(rusqlite::ffi::SQLITE_LOCKED)).retryable);
        assert!(!metadata_source_error(sqlite_error(rusqlite::ffi::SQLITE_INTERRUPT)).retryable);
        assert!(!metadata_source_error(sqlite_error(rusqlite::ffi::SQLITE_BUSY_TIMEOUT)).retryable);
        assert!(metadata_source_error(sqlite_error(rusqlite::ffi::SQLITE_ERROR)).retryable);
        assert!(
            metadata_source_error(MatchingDomainMetadataError::Sqlite(
                rusqlite::Error::InvalidQuery
            ))
            .retryable
        );
    }

    #[test]
    fn lifecycle_start_stop_is_idempotent_and_stops_owned_worker() {
        let source = Arc::new(FakeSource::default());
        *source.version.lock().unwrap() = 1;
        let cache = Arc::new(MatchingDomainCache::new(source));
        cache.start().unwrap();
        cache.start().unwrap();
        cache.stop();
        cache.stop();
        assert_eq!(cache.refresh_domains(), Err(DomainCacheError::Stopped));
    }

    #[test]
    fn joining_worker_from_its_own_thread_detaches_instead_of_panicking() {
        let handle_cell = Arc::new(Mutex::new(None));
        let child_handle_cell = Arc::clone(&handle_cell);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let handle = loop {
                if let Some(handle) = child_handle_cell.lock().unwrap().take() {
                    break handle;
                }
                thread::yield_now();
            };
            join_worker(handle);
            done_tx.send(()).unwrap();
        });
        *handle_cell.lock().unwrap() = Some(worker);
        ready_rx.recv().unwrap();
        // Completion proves the worker dropped its own JoinHandle without a
        // self-join panic or deadlock.
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }
}
