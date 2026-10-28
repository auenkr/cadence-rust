//! Byte-preserving semaphore persistence for native Matching transports.
//!
//! This embedded adapter is deliberately separate from the legacy
//! `InMemoryExecutionStore`; it does not share metadata or holds with it.

use std::{collections::BTreeMap, sync::Mutex};

use async_trait::async_trait;
use cadence_common::go_string::GoString;

use crate::semaphores::{
    AcquireError, GrantOutcome, SemaphoreError, SemaphoreMetadata, DEFAULT_BUCKET_SIZE,
    MAX_BUCKET_SIZE, SEMAPHORE_SCAN_PAGE_SIZE_MESSAGE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemaphoreRowBytes {
    Token { token_id: i32, holder: GoString },
    Owner { owner_id: GoString, held_token: i32 },
    Unknown,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SemaphoreBucketPageBytes {
    pub rows: Vec<SemaphoreRowBytes>,
    pub next_page_token: Option<Vec<u8>>,
}

#[async_trait]
pub trait SemaphorePersistenceBytes: std::fmt::Debug + Send + Sync {
    async fn create_semaphore(&self, domain: &GoString, name: &GoString, size: i32, bucket_size: i32) -> Result<SemaphoreMetadata, SemaphoreError>;
    async fn get_semaphore_metadata(&self, domain: &GoString, name: &GoString) -> Result<SemaphoreMetadata, AcquireError>;
    async fn scan_semaphore_bucket(&self, domain: &GoString, name: &GoString, bucket: i32, page_size: usize, next_page_token: Option<&[u8]>) -> Result<SemaphoreBucketPageBytes, AcquireError>;
    async fn get_semaphore_ownership_by_token(&self, domain: &GoString, name: &GoString, bucket: i32, token_id: i32) -> Result<GoString, AcquireError>;
    async fn get_semaphore_ownership_by_owner(&self, domain: &GoString, name: &GoString, bucket: i32, owner_id: &GoString) -> Result<i32, AcquireError>;
    async fn grant_semaphore_token(&self, domain: &GoString, name: &GoString, bucket: i32, token_id: i32, owner_id: &GoString) -> Result<GrantOutcome, AcquireError>;
    async fn release_semaphore_token(&self, domain: &GoString, name: &GoString, bucket: i32, token_id: i32, owner_id: &GoString) -> Result<bool, AcquireError>;
}

#[async_trait]
impl SemaphorePersistenceBytes for crate::SqlUnsupportedSemaphorePersistence {
    async fn create_semaphore(&self, _: &GoString, _: &GoString, _: i32, _: i32) -> Result<SemaphoreMetadata, SemaphoreError> { Err(SemaphoreError::UnsupportedBackend) }
    async fn get_semaphore_metadata(&self, _: &GoString, _: &GoString) -> Result<SemaphoreMetadata, AcquireError> { Err(AcquireError::Invalid(Self::message())) }
    async fn scan_semaphore_bucket(&self, _: &GoString, _: &GoString, _: i32, _: usize, _: Option<&[u8]>) -> Result<SemaphoreBucketPageBytes, AcquireError> { Err(AcquireError::Invalid(Self::message())) }
    async fn get_semaphore_ownership_by_token(&self, _: &GoString, _: &GoString, _: i32, _: i32) -> Result<GoString, AcquireError> { Err(AcquireError::Invalid(Self::message())) }
    async fn get_semaphore_ownership_by_owner(&self, _: &GoString, _: &GoString, _: i32, _: &GoString) -> Result<i32, AcquireError> { Err(AcquireError::Invalid(Self::message())) }
    async fn grant_semaphore_token(&self, _: &GoString, _: &GoString, _: i32, _: i32, _: &GoString) -> Result<GrantOutcome, AcquireError> { Err(AcquireError::Invalid(Self::message())) }
    async fn release_semaphore_token(&self, _: &GoString, _: &GoString, _: i32, _: i32, _: &GoString) -> Result<bool, AcquireError> { Err(AcquireError::Invalid(Self::message())) }
}

impl crate::SqlUnsupportedSemaphorePersistence {
    fn message() -> String { Self::MESSAGE.to_owned() }
}

#[derive(Debug, Default)]
pub struct InMemorySemaphorePersistenceBytes {
    state: Mutex<ByteState>,
}

#[derive(Debug, Default)]
struct ByteState {
    metadata: BTreeMap<(GoString, GoString), SemaphoreMetadata>,
    holds: BucketHolds,
}

type BucketHolds = BTreeMap<(GoString, GoString, i32), BTreeMap<i32, GoString>>;

fn not_found(message: String) -> AcquireError {
    AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(message))
}

#[async_trait]
impl SemaphorePersistenceBytes for InMemorySemaphorePersistenceBytes {
    async fn create_semaphore(&self, domain: &GoString, name: &GoString, size: i32, bucket_size: i32) -> Result<SemaphoreMetadata, SemaphoreError> {
        if size <= 0 { return Err(SemaphoreError::Invalid(format!("Size must be positive, got {size}"))); }
        if !(0..=MAX_BUCKET_SIZE).contains(&bucket_size) { return Err(SemaphoreError::Invalid(format!("BucketSize must not exceed {MAX_BUCKET_SIZE}, got {bucket_size}"))); }
        let metadata = SemaphoreMetadata { size, bucket_size: if bucket_size == 0 { DEFAULT_BUCKET_SIZE } else { bucket_size } };
        let mut state = self.state.lock().expect("semaphore bytes lock");
        let key = (domain.clone(), name.clone());
        if state.metadata.contains_key(&key) { return Err(SemaphoreError::Exists("semaphore already exists".into())); }
        state.metadata.insert(key, metadata.clone());
        Ok(metadata)
    }
    async fn get_semaphore_metadata(&self, domain: &GoString, name: &GoString) -> Result<SemaphoreMetadata, AcquireError> {
        self.state.lock().expect("semaphore bytes lock").metadata.get(&(domain.clone(), name.clone())).cloned().ok_or_else(|| not_found("semaphore not found".into()))
    }
    async fn scan_semaphore_bucket(&self, domain: &GoString, name: &GoString, bucket: i32, page_size: usize, next_page_token: Option<&[u8]>) -> Result<SemaphoreBucketPageBytes, AcquireError> {
        let state = self.state.lock().expect("semaphore bytes lock");
        if !state.metadata.contains_key(&(domain.clone(), name.clone())) { return Err(not_found("semaphore not found".into())); }
        if page_size == 0 { return Err(AcquireError::Invalid(SEMAPHORE_SCAN_PAGE_SIZE_MESSAGE.into())); }
        let metadata = state.metadata.get(&(domain.clone(), name.clone())).unwrap();
        let holds = state.holds.get(&(domain.clone(), name.clone(), bucket));
        let mut rows = Vec::with_capacity(metadata.bucket_size as usize + holds.map_or(0, BTreeMap::len));
        for token in 1..=metadata.bucket_size {
            let holder = holds.and_then(|h| h.get(&token)).cloned().unwrap_or_default();
            rows.push(SemaphoreRowBytes::Token { token_id: token, holder });
        }
        if let Some(holds) = holds {
            for (&token, owner) in holds { rows.push(SemaphoreRowBytes::Owner { owner_id: owner.clone(), held_token: token }); }
        }
        let start = match next_page_token {
            None => 0,
            Some(raw) if raw.len() == 8 => usize::try_from(u64::from_be_bytes(raw.try_into().unwrap())).map_err(|_| AcquireError::Invalid("invalid semaphore page token".into()))?,
            Some(_) => return Err(AcquireError::Invalid("invalid semaphore page token".into())),
        };
        if start > rows.len() { return Err(AcquireError::Invalid("invalid semaphore page token".into())); }
        let end = start.saturating_add(page_size).min(rows.len());
        Ok(SemaphoreBucketPageBytes { rows: rows[start..end].to_vec(), next_page_token: (end < rows.len()).then(|| (end as u64).to_be_bytes().to_vec()) })
    }
    async fn get_semaphore_ownership_by_token(&self, domain: &GoString, name: &GoString, bucket: i32, token_id: i32) -> Result<GoString, AcquireError> {
        let state = self.state.lock().expect("semaphore bytes lock");
        if !state.metadata.contains_key(&(domain.clone(), name.clone())) { return Err(not_found("semaphore not found".into())); }
        state.holds.get(&(domain.clone(), name.clone(), bucket)).and_then(|h| h.get(&token_id)).cloned().ok_or_else(|| not_found("semaphore token is not held".into()))
    }
    async fn get_semaphore_ownership_by_owner(&self, domain: &GoString, name: &GoString, bucket: i32, owner_id: &GoString) -> Result<i32, AcquireError> {
        let state = self.state.lock().expect("semaphore bytes lock");
        if !state.metadata.contains_key(&(domain.clone(), name.clone())) { return Err(not_found("semaphore not found".into())); }
        state.holds.get(&(domain.clone(), name.clone(), bucket)).and_then(|h| h.iter().find_map(|(&token, owner)| (owner == owner_id).then_some(token))).ok_or_else(|| not_found("semaphore owner does not hold a token".into()))
    }
    async fn grant_semaphore_token(&self, domain: &GoString, name: &GoString, bucket: i32, token_id: i32, owner_id: &GoString) -> Result<GrantOutcome, AcquireError> {
        let mut state = self.state.lock().expect("semaphore bytes lock");
        let metadata = state.metadata.get(&(domain.clone(), name.clone())).ok_or_else(|| not_found("semaphore not found".into()))?;
        if !(1..=metadata.bucket_size).contains(&token_id) { return Err(AcquireError::Invalid(format!("TokenID must be a seeded slot, got {token_id}"))); }
        let holds = state.holds.entry((domain.clone(), name.clone(), bucket)).or_default();
        if let Some((&held, _)) = holds.iter().find(|(_, owner)| *owner == owner_id) { return Ok(GrantOutcome::AlreadyHeld(held)); }
        if holds.contains_key(&token_id) { return Ok(GrantOutcome::SlotTaken); }
        holds.insert(token_id, owner_id.clone());
        Ok(GrantOutcome::Applied)
    }
    async fn release_semaphore_token(&self, domain: &GoString, name: &GoString, bucket: i32, token_id: i32, owner_id: &GoString) -> Result<bool, AcquireError> {
        let mut state = self.state.lock().expect("semaphore bytes lock");
        if !state.metadata.contains_key(&(domain.clone(), name.clone())) { return Err(not_found("semaphore not found".into())); }
        let holds = state.holds.entry((domain.clone(), name.clone(), bucket)).or_default();
        if holds.get(&token_id) == Some(owner_id) { holds.remove(&token_id); Ok(true) } else { Ok(false) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gs(value: &[u8]) -> GoString { GoString::from(value.to_vec()) }

    #[tokio::test]
    async fn sql_is_unsupported_for_every_operation_even_with_raw_identifiers() {
        let sql = crate::SqlUnsupportedSemaphorePersistence;
        let d = gs(&[0xff]); let n = gs(&[0xfe]); let o = gs(&[0xef, 0xbf, 0xbd]);
        assert!(matches!(sql.create_semaphore(&d, &n, 1, 1).await, Err(SemaphoreError::UnsupportedBackend)));
        let expected = crate::SqlUnsupportedSemaphorePersistence::MESSAGE;
        assert!(matches!(sql.get_semaphore_metadata(&d, &n).await, Err(AcquireError::Invalid(m)) if m == expected));
        assert!(matches!(sql.scan_semaphore_bucket(&d, &n, 0, 1, Some(&[0xff])).await, Err(AcquireError::Invalid(m)) if m == expected));
        assert!(matches!(sql.get_semaphore_ownership_by_token(&d, &n, 0, 1).await, Err(AcquireError::Invalid(m)) if m == expected));
        assert!(matches!(sql.get_semaphore_ownership_by_owner(&d, &n, 0, &o).await, Err(AcquireError::Invalid(m)) if m == expected));
        assert!(matches!(sql.grant_semaphore_token(&d, &n, 0, 1, &o).await, Err(AcquireError::Invalid(m)) if m == expected));
        assert!(matches!(sql.release_semaphore_token(&d, &n, 0, 1, &o).await, Err(AcquireError::Invalid(m)) if m == expected));
    }

    #[tokio::test]
    async fn raw_keys_fencing_consistency_and_paging() {
        let store = InMemorySemaphorePersistenceBytes::default();
        let d_ff = gs(&[0xff]); let d_replacement = gs(&[0xef, 0xbf, 0xbd]);
        let n = gs(b"name"); let owner_ff = gs(&[0xfe]); let owner_replacement = gs(&[0xef, 0xbf, 0xbd]);
        store.create_semaphore(&d_ff, &n, 4, 1).await.unwrap();
        store.create_semaphore(&d_replacement, &n, 4, 1).await.unwrap();
        assert_eq!(store.grant_semaphore_token(&d_ff, &n, 0, 1, &owner_ff).await.unwrap(), GrantOutcome::Applied);
        assert_eq!(store.grant_semaphore_token(&d_ff, &n, 0, 1, &owner_ff).await.unwrap(), GrantOutcome::AlreadyHeld(1));
        assert!(matches!(store.grant_semaphore_token(&d_ff, &n, 0, 2, &owner_ff).await, Err(AcquireError::Invalid(_))));
        assert_eq!(store.grant_semaphore_token(&d_ff, &n, 0, 1, &owner_replacement).await.unwrap(), GrantOutcome::SlotTaken);
        assert_eq!(store.get_semaphore_ownership_by_token(&d_ff, &n, 0, 1).await.unwrap(), owner_ff);
        assert_eq!(store.get_semaphore_ownership_by_owner(&d_ff, &n, 0, &gs(&[0xfe])).await.unwrap(), 1);
        assert!(store.get_semaphore_ownership_by_owner(&d_replacement, &n, 0, &owner_ff).await.is_err());
        let first = store.scan_semaphore_bucket(&d_ff, &n, 0, 1, None).await.unwrap();
        assert_eq!(first.rows, vec![SemaphoreRowBytes::Token { token_id: 1, holder: gs(&[0xfe]) }]);
        let second = store.scan_semaphore_bucket(&d_ff, &n, 0, 1, first.next_page_token.as_deref()).await.unwrap();
        assert_eq!(second.rows, vec![SemaphoreRowBytes::Owner { owner_id: gs(&[0xfe]), held_token: 1 }]);
        assert!(second.next_page_token.is_none());
        assert!(!store.release_semaphore_token(&d_ff, &n, 0, 1, &owner_replacement).await.unwrap());
        assert!(store.release_semaphore_token(&d_ff, &n, 0, 1, &owner_ff).await.unwrap());
    }
}
