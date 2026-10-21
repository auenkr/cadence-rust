//! Shared persistence-QPS guards for Matching's configured stores.

use crate::{
    matching_domain_cache::{DomainMetadataPage, DomainMetadataSource, DomainMetadataSourceError},
    matching_persistence_limiter::MatchingPersistenceLimiter,
};
use cadence_persistence::{
    matching_blob::TaskListData,
    matching_task_store::{
        AllocatedTask, MatchingTaskStore, MatchingTaskStoreError, Result as TaskResult,
        TaskListKey, TaskListRecord,
    },
    semaphores::{
        AcquireError, GrantOutcome, SemaphoreError, SemaphoreMetadata, SemaphorePersistence,
        SemaphoreRow,
    },
};
use std::sync::Arc;
use uuid::Uuid;

const REJECTED: &str = "Persistence Max QPS Reached.";

pub fn limit_task_store(
    store: Arc<dyn MatchingTaskStore>,
    limiter: Arc<MatchingPersistenceLimiter>,
) -> Arc<dyn MatchingTaskStore> {
    Arc::new(RateLimitedTaskStore { store, limiter })
}

pub fn limit_domain_source(
    source: Arc<dyn DomainMetadataSource>,
    limiter: Arc<MatchingPersistenceLimiter>,
) -> Arc<dyn DomainMetadataSource> {
    Arc::new(RateLimitedDomainSource { source, limiter })
}

pub fn limit_semaphore_store(
    store: Arc<dyn SemaphorePersistence>,
    limiter: Arc<MatchingPersistenceLimiter>,
) -> Arc<dyn SemaphorePersistence> {
    Arc::new(RateLimitedSemaphoreStore { store, limiter })
}

struct RateLimitedTaskStore {
    store: Arc<dyn MatchingTaskStore>,
    limiter: Arc<MatchingPersistenceLimiter>,
}

impl RateLimitedTaskStore {
    fn permit(&self) -> TaskResult<()> {
        if self.limiter.allow() {
            Ok(())
        } else {
            Err(MatchingTaskStoreError::Backend {
                kind: cadence_persistence::matching_task_store::BackendErrorKind::Throttled,
                message: REJECTED.into(),
            })
        }
    }
}

impl MatchingTaskStore for RateLimitedTaskStore {
    fn get_task_list(&self, key: &TaskListKey) -> TaskResult<TaskListRecord> {
        self.permit()?;
        self.store.get_task_list(key)
    }
    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> TaskResult<TaskListRecord> {
        self.permit()?;
        self.store.lease(key, kind, previous_range, now_nanos)
    }
    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        data: TaskListData,
        now_nanos: i64,
    ) -> TaskResult<()> {
        self.permit()?;
        self.store.update_task_list(key, range, data, now_nanos)
    }
    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> TaskResult<()> {
        self.permit()?;
        self.store
            .create_tasks_with_domains(key, range, now_nanos, tasks)
    }
    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> TaskResult<Vec<AllocatedTask>> {
        self.permit()?;
        self.store
            .get_tasks(key, read_level, max_read_level, batch_size)
    }
    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> TaskResult<i64> {
        self.permit()?;
        self.store.task_list_size(key, ack_level)
    }
    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> TaskResult<()> {
        self.permit()?;
        self.store.complete_task(key, task_id)
    }
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> TaskResult<usize> {
        self.permit()?;
        self.store
            .complete_tasks_less_than(key, inclusive_task_id, limit)
    }
    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> TaskResult<()> {
        self.permit()?;
        self.store.delete_task_list(key, range)
    }
}

struct RateLimitedDomainSource {
    source: Arc<dyn DomainMetadataSource>,
    limiter: Arc<MatchingPersistenceLimiter>,
}

impl RateLimitedDomainSource {
    fn permit(&self) -> Result<(), DomainMetadataSourceError> {
        if self.limiter.allow() {
            Ok(())
        } else {
            Err(DomainMetadataSourceError::transient(REJECTED))
        }
    }
}

impl DomainMetadataSource for RateLimitedDomainSource {
    fn metadata_version(
        &self,
        timeout: std::time::Duration,
    ) -> Result<i64, DomainMetadataSourceError> {
        self.permit()?;
        self.source.metadata_version(timeout)
    }
    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: std::time::Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        self.permit()?;
        self.source
            .list_domains(page_size, next_page_token, timeout)
    }
    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        timeout: std::time::Duration,
    ) -> Result<
        cadence_persistence::matching_domain_metadata::MatchingDomainMetadata,
        DomainMetadataSourceError,
    > {
        self.permit()?;
        self.source.get_domain(name, id, timeout)
    }
}

#[derive(Debug)]
struct RateLimitedSemaphoreStore {
    store: Arc<dyn SemaphorePersistence>,
    limiter: Arc<MatchingPersistenceLimiter>,
}

impl RateLimitedSemaphoreStore {
    fn permit_acquire(&self) -> Result<(), AcquireError> {
        if self.limiter.allow() {
            Ok(())
        } else {
            Err(AcquireError::Throttled(REJECTED.into()))
        }
    }
    fn permit_create(&self) -> Result<(), SemaphoreError> {
        if self.limiter.allow() {
            Ok(())
        } else {
            Err(SemaphoreError::Throttled(REJECTED.into()))
        }
    }
}

#[async_trait::async_trait]
impl SemaphorePersistence for RateLimitedSemaphoreStore {
    async fn create_semaphore(
        &self,
        domain_id: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        self.permit_create()?;
        self.store
            .create_semaphore(domain_id, name, size, bucket_size)
            .await
    }
    async fn get_semaphore_metadata(
        &self,
        domain_id: &str,
        name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        self.permit_acquire()?;
        self.store.get_semaphore_metadata(domain_id, name).await
    }
    async fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError> {
        self.permit_acquire()?;
        self.store
            .scan_semaphore_bucket(domain_id, name, bucket)
            .await
    }
    async fn get_semaphore_ownership_by_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> Result<String, AcquireError> {
        self.permit_acquire()?;
        self.store
            .get_semaphore_ownership_by_token(domain_id, name, bucket, token_id)
            .await
    }
    async fn get_semaphore_ownership_by_owner(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        owner_id: &str,
    ) -> Result<i32, AcquireError> {
        self.permit_acquire()?;
        self.store
            .get_semaphore_ownership_by_owner(domain_id, name, bucket, owner_id)
            .await
    }
    async fn grant_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<GrantOutcome, AcquireError> {
        self.permit_acquire()?;
        self.store
            .grant_semaphore_token(domain_id, name, bucket, token_id, owner_id)
            .await
    }
    async fn release_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<bool, AcquireError> {
        self.permit_acquire()?;
        self.store
            .release_semaphore_token(domain_id, name, bucket, token_id, owner_id)
            .await
    }
}
