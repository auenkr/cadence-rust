//! Semaphore metadata and token grants ported from Go's semaphore
//! persistence (`go/common/persistence/semaphore_metadata_manager.go`) and
//! matching bucket managers
//! (`go/service/matching/semaphore/semaphore_manager.go`).
//!
//! One bucket holds `bucket_size` token slots (IDs 1..=budget, default 100
//! when unset, max 250). Grants draw a uniform-random free slot like Go's
//! reservation; a full bucket reports `NoSlot` immediately instead of
//! blocking for a waiter, which needs the decision-level release path still
//! being ported. Re-acquires by the same owner return the held slot.

use std::collections::HashMap;

use cadence_api::enums::SemaphoreAcquireOutcome;

/// Default per-bucket token budget, mirroring Go's
/// `DefaultSemaphoreBucketSize`.
pub const DEFAULT_BUCKET_SIZE: i32 = 100;
/// Maximum per-bucket token budget, mirroring Go's
/// `MaxSemaphoreBucketSize`.
pub const MAX_BUCKET_SIZE: i32 = 250;

/// Semaphore metadata: total size and per-bucket budget.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SemaphoreMetadata {
    pub size: i32,
    pub bucket_size: i32,
}

/// One granted hold: owner ID plus the slot it holds.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SemaphoreHold {
    pub owner_id: String,
    pub token_id: i32,
}

/// One row of a bucket scan, mirroring Go's `SemaphoreOwnership` with its
/// `RowType` discriminator: a token slot and its holder, or an owner and the
/// token it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemaphoreRow {
    Token { token_id: i32, holder: String },
    Owner { owner_id: String, held_token: i32 },
    /// A row whose type this version cannot classify. Go's scan returns it
    /// and the manager's load skips it, so an unknown row never fails the load.
    Unknown,
}

/// Outcome of a conditional grant, mirroring Go's `SemaphoreGrant*` outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantOutcome {
    Applied,
    SlotTaken,
    AlreadyHeld(i32),
    /// Go `SemaphoreGrantUnknown`: an outcome the caller cannot read. The
    /// bundled stores never return it; the manager still rejects it.
    Unknown,
}

/// Errors for semaphore creation, mirroring Go's metadata manager and
/// frontend duplicate mapping.
#[derive(Debug, thiserror::Error)]
pub enum SemaphoreError {
    #[error(transparent)]
    NotFound(#[from] cadence_api::errors::EntityNotExistsError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Exists(String),
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    Throttled(String),
    /// The Go SQL metadata/token stores deliberately do not implement these
    /// operations. Do not invent a Rust-only schema in an existing database.
    #[error("distributed semaphore is not supported on the SQL persistence backend")]
    UnsupportedBackend,
}

/// Errors for semaphore acquisition, mirroring Go matching's mapping:
/// bad identifiers/owners are `BadRequest`, unknown semaphores are
/// `EntityNotExists`.
#[derive(Debug, thiserror::Error)]
pub enum AcquireError {
    #[error(transparent)]
    NotFound(#[from] cadence_api::errors::EntityNotExistsError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    Throttled(String),
}

/// Persistence boundary for Matching's semaphore metadata and bucket rows.
/// Embedded mode uses the in-memory adapter; configured SQL preserves Go's
/// explicit unsupported-backend behavior. A Cassandra adapter can implement
/// this contract using Go's semaphore_metadata and semaphore_tokens tables.
#[async_trait::async_trait]
pub trait SemaphorePersistence: std::fmt::Debug + Send + Sync {
    async fn create_semaphore(
        &self,
        domain_id: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError>;
    async fn get_semaphore_metadata(
        &self,
        domain_id: &str,
        name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError>;
    async fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError>;
    async fn get_semaphore_ownership_by_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> Result<String, AcquireError>;
    async fn get_semaphore_ownership_by_owner(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _owner_id: &str,
    ) -> Result<i32, AcquireError> {
        Err(AcquireError::Invalid(SqlUnsupportedSemaphorePersistence::MESSAGE.into()))
    }
    async fn grant_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<GrantOutcome, AcquireError>;
    async fn release_semaphore_token(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _token_id: i32,
        _owner_id: &str,
    ) -> Result<bool, AcquireError> {
        Err(AcquireError::Invalid(SqlUnsupportedSemaphorePersistence::MESSAGE.into()))
    }
}

/// Go's SQL metadata and token stores return this BadRequest for every call.
#[derive(Debug, Clone, Copy, Default)]
pub struct SqlUnsupportedSemaphorePersistence;

impl SqlUnsupportedSemaphorePersistence {
    pub const MESSAGE: &'static str =
        "distributed semaphore is not supported on the SQL persistence backend";
}

#[async_trait::async_trait]
impl SemaphorePersistence for SqlUnsupportedSemaphorePersistence {
    async fn create_semaphore(
        &self,
        _domain: &str,
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
        Err(AcquireError::Invalid(Self::MESSAGE.into()))
    }

    async fn scan_semaphore_bucket(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError> {
        Err(AcquireError::Invalid(Self::MESSAGE.into()))
    }

    async fn get_semaphore_ownership_by_token(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _token_id: i32,
    ) -> Result<String, AcquireError> {
        Err(AcquireError::Invalid(Self::MESSAGE.into()))
    }

    async fn get_semaphore_ownership_by_owner(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _owner_id: &str,
    ) -> Result<i32, AcquireError> {
        Err(AcquireError::Invalid(Self::MESSAGE.into()))
    }

    async fn grant_semaphore_token(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _token_id: i32,
        _owner_id: &str,
    ) -> Result<GrantOutcome, AcquireError> {
        Err(AcquireError::Invalid(Self::MESSAGE.into()))
    }

    async fn release_semaphore_token(
        &self,
        _domain_id: &str,
        _name: &str,
        _bucket: i32,
        _token_id: i32,
        _owner_id: &str,
    ) -> Result<bool, AcquireError> {
        Err(AcquireError::Invalid(Self::MESSAGE.into()))
    }
}

pub fn bucket_budget(bucket_size: i32) -> i32 {
    if bucket_size <= 0 {
        DEFAULT_BUCKET_SIZE
    } else {
        bucket_size.min(MAX_BUCKET_SIZE)
    }
}

impl super::InMemoryExecutionStore {
    /// Create semaphore metadata, mirroring Go's metadata manager: sizes are
    /// validated, an unset bucket size defaults, and duplicates are an
    /// exists error (the frontend maps it to `BadRequest`).
    pub fn create_semaphore(
        &self,
        domain: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        if size <= 0 {
            return Err(SemaphoreError::Invalid(format!(
                "Size must be positive, got {size}"
            )));
        }
        if !(0..=MAX_BUCKET_SIZE).contains(&bucket_size) {
            return Err(SemaphoreError::Invalid(format!(
                "BucketSize must not exceed {MAX_BUCKET_SIZE}, got {bucket_size}"
            )));
        }
        let namespace = self.inner.read().expect("execution store lock").domain_id(domain);
        self.create_semaphore_by_id(&namespace, name, size, bucket_size)
    }

    /// Persist metadata after the frontend has resolved a domain name through
    /// its domain cache. Cassandra keys metadata by domain UUID, as Go does.
    pub fn create_semaphore_by_id(
        &self,
        domain_id: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        if size <= 0 {
            return Err(SemaphoreError::Invalid(format!("Size must be positive, got {size}")));
        }
        if !(0..=MAX_BUCKET_SIZE).contains(&bucket_size) {
            return Err(SemaphoreError::Invalid(format!(
                "BucketSize must not exceed {MAX_BUCKET_SIZE}, got {bucket_size}"
            )));
        }
        let mut inner = self.inner.write().expect("execution store lock");
        let namespace = domain_id.to_string();
        let key = (namespace, name.to_string());
        if inner.semaphores.contains_key(&key) {
            return Err(SemaphoreError::Exists(format!(
                "semaphore {name:?} already exists in domain {domain_id:?}"
            )));
        }
        let metadata = SemaphoreMetadata {
            size,
            bucket_size: if bucket_size == 0 {
                DEFAULT_BUCKET_SIZE
            } else {
                bucket_size
            },
        };
        inner.semaphores.insert(key, metadata.clone());
        Ok(metadata)
    }

    pub fn get_semaphore_metadata(
        &self,
        domain_id: &str,
        name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        self.inner.read().expect("execution store lock")
            .semaphores.get(&(domain_id.to_string(), name.to_string()))
            .cloned()
            .ok_or_else(|| AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(
                format!("semaphore {name:?} does not exist in domain {domain_id:?}"),
            )))
    }

    /// Grant a token slot immediately or report `NoSlot`, mirroring Go's
    /// grant step without the blocking waiter queue (which needs the
    /// decision-level token release path). Re-acquires by the same owner
    /// return the already-held slot.
    pub fn acquire_semaphore(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        owner_id: &str,
    ) -> Result<(SemaphoreAcquireOutcome, i32), AcquireError> {
        use cadence_api::enums::SemaphoreAcquireOutcome as Outcome;
        if domain_id.is_empty() {
            return Err(AcquireError::Invalid("domainID is required".to_string()));
        }
        if name.is_empty() {
            return Err(AcquireError::Invalid("semaphoreName is required".to_string()));
        }
        if bucket < 0 {
            return Err(AcquireError::Invalid(format!(
                "bucket must not be negative, got {bucket}"
            )));
        }
        if owner_id.is_empty() {
            return Err(AcquireError::Invalid("ownerID is required".to_string()));
        }
        cadence_common::semaphore::parse_owner(owner_id)
            .map_err(|e| AcquireError::Invalid(format!("invalid owner id: {e}")))?;
        let mut inner = self.inner.write().expect("execution store lock");
        let metadata = inner
            .semaphores
            .get(&(domain_id.to_string(), name.to_string()))
            .cloned()
            .ok_or_else(|| {
                AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                    "semaphore {name:?} does not exist in domain {domain_id:?}"
                )))
            })?;
        let budget = bucket_budget(metadata.bucket_size);
        let key = (
            domain_id.to_string(),
            name.to_string(),
            bucket,
        );
        let holds = inner.semaphore_holds.entry(key).or_default();
        // Idempotent re-acquire by the same owner.
        if let Some(hold) = holds.iter().find(|h| h.owner_id == owner_id) {
            return Ok((Outcome::Acquired, hold.token_id));
        }
        let taken: std::collections::HashSet<i32> =
            holds.iter().map(|h| h.token_id).collect();
        let free: Vec<i32> = (1..=budget).filter(|id| !taken.contains(id)).collect();
        if free.is_empty() {
            return Ok((Outcome::NoSlot, 0));
        }
        // Uniform-random free slot like Go's reservation draw.
        let token_id = free[rand::random::<usize>() % free.len()];
        holds.push(SemaphoreHold {
            owner_id: owner_id.to_string(),
            token_id,
        });
        Ok((Outcome::Acquired, token_id))
    }

    /// Release a hold by owner ID, freeing its slot. Unknown owners are a
    /// no-op success, mirroring conditional-write semantics.
    pub fn release_semaphore_hold(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        owner_id: &str,
    ) {        let mut inner = self.inner.write().expect("execution store lock");
        if let Some(holds) = inner.semaphore_holds.get_mut(&(
            domain_id.to_string(),
            name.to_string(),
            bucket,
        )) {
            holds.retain(|h| h.owner_id != owner_id);
        }
    }

    /// Semaphore metadata for a domain, used by describe-style reads.
    pub fn list_semaphores(&self, domain: &str) -> HashMap<String, SemaphoreMetadata> {
        let inner = self.inner.read().expect("execution store lock");
        let namespace = inner.domain_id(domain);
        inner
            .semaphores
            .iter()
            .filter(|((d, _), _)| d == &namespace)
            .map(|((_, name), meta)| (name.clone(), meta.clone()))
            .collect()
    }

    fn semaphore_budget(
        inner: &super::executions::Inner,
        domain_id: &str,
        name: &str,
    ) -> Result<i32, AcquireError> {
        inner
            .semaphores
            .get(&(domain_id.to_string(), name.to_string()))
            .map(|metadata| bucket_budget(metadata.bucket_size))
            .ok_or_else(|| {
                AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                    "semaphore {name:?} does not exist in domain {domain_id:?}"
                )))
            })
    }

    /// One bucket's token and owner rows, mirroring Go's
    /// `ScanSemaphoreBucket`. Token rows are the seeded slots; owner rows are
    /// the held slots. Like Go, a missing semaphore is an empty scan; the
    /// matching manager checks metadata before scanning, so the caller maps
    /// NotFound itself.
    pub fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError> {
        let inner = self.inner.read().expect("execution store lock");
        let budget = Self::semaphore_budget(&inner, domain_id, name)?;
        let holds: Vec<SemaphoreHold> = inner
            .semaphore_holds
            .get(&(domain_id.to_string(), name.to_string(), bucket))
            .cloned()
            .unwrap_or_default();
        let mut holders: HashMap<i32, String> = HashMap::new();
        for hold in &holds {
            holders.insert(hold.token_id, hold.owner_id.clone());
        }
        let mut rows = Vec::with_capacity(budget as usize + holds.len());
        for token_id in 1..=budget {
            rows.push(SemaphoreRow::Token {
                token_id,
                holder: holders.get(&token_id).cloned().unwrap_or_default(),
            });
        }
        for hold in holds {
            rows.push(SemaphoreRow::Owner {
                owner_id: hold.owner_id,
                held_token: hold.token_id,
            });
        }
        Ok(rows)
    }

    /// The holder of one token row, mirroring
    /// `GetSemaphoreOwnershipByToken`. A token outside the seeded range reads
    /// as a removed row: Go's token rows are seeded once, so a miss is
    /// `EntityNotExists` when the semaphore exists.
    pub fn get_semaphore_ownership_by_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> Result<String, AcquireError> {
        let inner = self.inner.read().expect("execution store lock");
        let budget = Self::semaphore_budget(&inner, domain_id, name)?;
        if !(1..=budget).contains(&token_id) {
            return Err(AcquireError::NotFound(
                cadence_api::errors::EntityNotExistsError::new(format!(
                    "semaphore token {token_id} does not exist in bucket {bucket}"
                )),
            ));
        }
        Ok(inner
            .semaphore_holds
            .get(&(domain_id.to_string(), name.to_string(), bucket))
            .and_then(|holds| {
                holds
                    .iter()
                    .find(|hold| hold.token_id == token_id)
                    .map(|hold| hold.owner_id.clone())
            })
            .unwrap_or_default())
    }

    pub fn get_semaphore_ownership_by_owner(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        owner_id: &str,
    ) -> Result<i32, AcquireError> {
        let inner = self.inner.read().expect("execution store lock");
        Self::semaphore_budget(&inner, domain_id, name)?;
        inner.semaphore_holds
            .get(&(domain_id.to_string(), name.to_string(), bucket))
            .and_then(|holds| holds.iter().find(|hold| hold.owner_id == owner_id))
            .map(|hold| hold.token_id)
            .ok_or_else(|| AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(
                format!("semaphore owner {owner_id:?} does not hold a token in bucket {bucket}"),
            )))
    }

    /// Conditional grant of one token to an owner, mirroring
    /// `GrantSemaphoreToken`. Returns `AlreadyHeld(held_token)` when the
    /// owner already holds a token, `SlotTaken` when another owner holds the
    /// token, else `Applied`.
    pub fn grant_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<GrantOutcome, AcquireError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let budget = Self::semaphore_budget(&inner, domain_id, name)?;
        if !(1..=budget).contains(&token_id) {
            return Err(AcquireError::Invalid(format!(
                "TokenID must be a seeded slot, got {token_id}"
            )));
        }
        let holds = inner
            .semaphore_holds
            .entry((domain_id.to_string(), name.to_string(), bucket))
            .or_default();
        if let Some(held) = holds.iter().find(|hold| hold.owner_id == owner_id) {
            return Ok(GrantOutcome::AlreadyHeld(held.token_id));
        }
        if let Some(holder) = holds.iter().find(|hold| hold.token_id == token_id) {
            let _ = holder;
            return Ok(GrantOutcome::SlotTaken);
        }
        holds.push(SemaphoreHold {
            owner_id: owner_id.to_string(),
            token_id,
        });
        Ok(GrantOutcome::Applied)
    }

    /// Release one owner's hold of a token, mirroring
    /// `ReleaseSemaphoreToken`. Returns whether the row was cleared.
    pub fn release_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<bool, AcquireError> {
        let mut inner = self.inner.write().expect("execution store lock");
        Self::semaphore_budget(&inner, domain_id, name)?;
        let mut applied = false;
        if let Some(holds) = inner
            .semaphore_holds
            .get_mut(&(domain_id.to_string(), name.to_string(), bucket))
        {
            let before = holds.len();
            holds.retain(|hold| !(hold.owner_id == owner_id && hold.token_id == token_id));
            applied = holds.len() != before;
        }
        Ok(applied)
    }
}

#[async_trait::async_trait]
impl SemaphorePersistence for super::InMemoryExecutionStore {
    async fn create_semaphore(
        &self,
        domain_id: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        super::InMemoryExecutionStore::create_semaphore_by_id(self, domain_id, name, size, bucket_size)
    }

    async fn get_semaphore_metadata(
        &self,
        domain_id: &str,
        name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        super::InMemoryExecutionStore::get_semaphore_metadata(self, domain_id, name)
    }

    async fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError> {
        super::InMemoryExecutionStore::scan_semaphore_bucket(self, domain_id, name, bucket)
    }

    async fn get_semaphore_ownership_by_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> Result<String, AcquireError> {
        super::InMemoryExecutionStore::get_semaphore_ownership_by_token(
            self, domain_id, name, bucket, token_id,
        )
    }

    async fn get_semaphore_ownership_by_owner(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        owner_id: &str,
    ) -> Result<i32, AcquireError> {
        super::InMemoryExecutionStore::get_semaphore_ownership_by_owner(
            self, domain_id, name, bucket, owner_id,
        )
    }

    async fn grant_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<GrantOutcome, AcquireError> {
        super::InMemoryExecutionStore::grant_semaphore_token(
            self, domain_id, name, bucket, token_id, owner_id,
        )
    }

    async fn release_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<bool, AcquireError> {
        super::InMemoryExecutionStore::release_semaphore_token(
            self, domain_id, name, bucket, token_id, owner_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executions::InMemoryExecutionStore;

    fn store() -> InMemoryExecutionStore {
        let store = InMemoryExecutionStore::new();
        store.register_domain("d", "uuid-d");
        store
    }

    #[test]
    fn create_validates_and_dedups() {
        let store = store();
        let meta = store.create_semaphore("d", "sem", 4, 0).unwrap();
        // Unset bucket size defaults to 100 like Go.
        assert_eq!(meta.bucket_size, 100);
        assert!(matches!(
            store.create_semaphore("d", "sem", 4, 0),
            Err(SemaphoreError::Exists(_))
        ));
        assert!(matches!(
            store.create_semaphore("d", "other", 0, 0),
            Err(SemaphoreError::Invalid(_))
        ));
        assert!(matches!(
            store.create_semaphore("d", "other", 4, 251),
            Err(SemaphoreError::Invalid(_))
        ));
    }

    #[test]
    fn acquire_grants_slots_then_reports_no_slot() {
        use cadence_api::enums::SemaphoreAcquireOutcome as Outcome;
        let store = store();
        store.create_semaphore("d", "sem", 10, 2).unwrap();
        let (outcome, first) = store.acquire_semaphore("uuid-d", "sem", 0, "1:w:r1:1").unwrap();
        assert_eq!(outcome, Outcome::Acquired);
        assert!((1..=2).contains(&first));
        let (outcome, second) = store.acquire_semaphore("uuid-d", "sem", 0, "1:w:r2:1").unwrap();
        assert_eq!(outcome, Outcome::Acquired);
        assert_ne!(first, second);
        // Bucket full: immediate NoSlot with token 0.
        let (outcome, token) = store.acquire_semaphore("uuid-d", "sem", 0, "1:w:r3:1").unwrap();
        assert_eq!(outcome, Outcome::NoSlot);
        assert_eq!(token, 0);
        // Same owner re-acquires its held slot idempotently.
        let (outcome, again) = store.acquire_semaphore("uuid-d", "sem", 0, "1:w:r1:1").unwrap();
        assert_eq!((outcome, again), (Outcome::Acquired, first));
        // Release frees the slot for the waiter.
        store.release_semaphore_hold("uuid-d", "sem", 0, "1:w:r1:1");
        let (outcome, _) = store.acquire_semaphore("uuid-d", "sem", 0, "1:w:r3:1").unwrap();
        assert_eq!(outcome, Outcome::Acquired);
    }

    #[test]
    fn acquire_rejects_bad_identifiers_and_unknown_semaphores() {
        let store = store();
        assert!(store.acquire_semaphore("", "sem", 0, "1:w:r:1").is_err());
        assert!(store.acquire_semaphore("uuid-d", "", 0, "1:w:r:1").is_err());
        assert!(store.acquire_semaphore("uuid-d", "sem", -1, "1:w:r:1").is_err());
        assert!(store.acquire_semaphore("uuid-d", "sem", 0, "").is_err());
        assert!(store.acquire_semaphore("uuid-d", "sem", 0, "nonsense").is_err());
        assert!(matches!(
            store.acquire_semaphore("uuid-d", "missing", 0, "1:w:r:1"),
            Err(AcquireError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn sql_backend_matches_go_unsupported_error_for_metadata_and_token_operations() {
        let sql = SqlUnsupportedSemaphorePersistence;
        assert!(matches!(
            sql.create_semaphore("d", "s", 1, 1).await,
            Err(SemaphoreError::UnsupportedBackend)
        ));
        assert_eq!(
            sql.scan_semaphore_bucket("d", "s", 0).await.unwrap_err().to_string(),
            SqlUnsupportedSemaphorePersistence::MESSAGE
        );
        assert_eq!(
            sql.get_semaphore_ownership_by_token("d", "s", 0, 1).await
                .unwrap_err()
                .to_string(),
            SqlUnsupportedSemaphorePersistence::MESSAGE
        );
        assert_eq!(
            sql.grant_semaphore_token("d", "s", 0, 1, "owner").await
                .unwrap_err()
                .to_string(),
            SqlUnsupportedSemaphorePersistence::MESSAGE
        );
    }
}
