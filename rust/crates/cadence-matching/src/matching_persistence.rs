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
    fn permit(&self, operation: &'static str, domain_id: Option<Uuid>) -> TaskResult<()> {
        if self.limiter.allow() {
            Ok(())
        } else {
            self.limiter.record_task_rejection(operation, domain_id);
            Err(MatchingTaskStoreError::Backend {
                kind: cadence_persistence::matching_task_store::BackendErrorKind::Throttled,
                message: REJECTED.into(),
            })
        }
    }
}

impl MatchingTaskStore for RateLimitedTaskStore {
    fn get_task_list(&self, key: &TaskListKey) -> TaskResult<TaskListRecord> {
        self.permit("GetTaskList", None)?;
        let started = std::time::Instant::now();
        let result = self.store.get_task_list(key);
        self.limiter
            .record_task_call("GetTaskList", None, started.elapsed(), &result);
        result
    }
    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> TaskResult<TaskListRecord> {
        self.permit("LeaseTaskList", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.lease(key, kind, previous_range, now_nanos);
        self.limiter.record_task_call(
            "LeaseTaskList",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        data: TaskListData,
        now_nanos: i64,
    ) -> TaskResult<()> {
        self.permit("UpdateTaskList", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.update_task_list(key, range, data, now_nanos);
        self.limiter.record_task_call(
            "UpdateTaskList",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> TaskResult<()> {
        self.permit("CreateTask", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self
            .store
            .create_tasks_with_domains(key, range, now_nanos, tasks);
        self.limiter.record_task_call(
            "CreateTask",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> TaskResult<Vec<AllocatedTask>> {
        self.permit("GetTasks", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self
            .store
            .get_tasks(key, read_level, max_read_level, batch_size);
        self.limiter
            .record_task_call("GetTasks", Some(key.domain_id), started.elapsed(), &result);
        if matches!(&result, Ok(tasks) if tasks.is_empty()) {
            self.limiter.record_empty_response("GetTasks");
        }
        result
    }
    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> TaskResult<i64> {
        self.permit("GetTaskListSize", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.task_list_size(key, ack_level);
        self.limiter.record_task_call(
            "GetTaskListSize",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> TaskResult<()> {
        self.permit("CompleteTask", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.complete_task(key, task_id);
        self.limiter.record_task_call(
            "CompleteTask",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> TaskResult<usize> {
        self.permit("CompleteTasksLessThan", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self
            .store
            .complete_tasks_less_than(key, inclusive_task_id, limit);
        self.limiter.record_task_call(
            "CompleteTasksLessThan",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> TaskResult<()> {
        self.permit("DeleteTaskList", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.delete_task_list(key, range);
        self.limiter.record_task_call(
            "DeleteTaskList",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
}

struct RateLimitedDomainSource {
    source: Arc<dyn DomainMetadataSource>,
    limiter: Arc<MatchingPersistenceLimiter>,
}

impl RateLimitedDomainSource {
    fn permit(
        &self,
        operation: &'static str,
        domain: Option<&str>,
    ) -> Result<(), DomainMetadataSourceError> {
        if self.limiter.allow() {
            Ok(())
        } else {
            self.limiter
                .record_rejection(operation, domain, Some(operation));
            Err(DomainMetadataSourceError::transient(REJECTED))
        }
    }
}

impl DomainMetadataSource for RateLimitedDomainSource {
    fn metadata_version(
        &self,
        timeout: std::time::Duration,
    ) -> Result<i64, DomainMetadataSourceError> {
        self.permit("GetMetadata", None)?;
        self.source.metadata_version(timeout)
    }
    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: std::time::Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        self.permit("ListDomain", None)?;
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
        self.permit("GetDomain", name)?;
        self.source.get_domain(name, id, timeout)
    }
}

#[derive(Debug)]
struct RateLimitedSemaphoreStore {
    store: Arc<dyn SemaphorePersistence>,
    limiter: Arc<MatchingPersistenceLimiter>,
}

impl RateLimitedSemaphoreStore {
    fn permit_acquire(&self, operation: &'static str, domain_id: &str) -> Result<(), AcquireError> {
        if self.limiter.allow() {
            Ok(())
        } else {
            self.limiter
                .record_rejection(operation, Some(domain_id), None);
            Err(AcquireError::Throttled(REJECTED.into()))
        }
    }
    fn permit_create(
        &self,
        operation: &'static str,
        domain_id: &str,
    ) -> Result<(), SemaphoreError> {
        if self.limiter.allow() {
            Ok(())
        } else {
            self.limiter
                .record_rejection(operation, Some(domain_id), None);
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
        self.permit_create("CreateSemaphore", domain_id)?;
        self.store
            .create_semaphore(domain_id, name, size, bucket_size)
            .await
    }
    async fn get_semaphore_metadata(
        &self,
        domain_id: &str,
        name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        self.permit_acquire("GetSemaphoreMetadata", domain_id)?;
        self.store.get_semaphore_metadata(domain_id, name).await
    }
    async fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
    ) -> Result<Vec<SemaphoreRow>, AcquireError> {
        self.permit_acquire("ScanSemaphoreBucket", domain_id)?;
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
        self.permit_acquire("GetSemaphoreOwnershipByToken", domain_id)?;
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
        self.permit_acquire("GetSemaphoreOwnershipByOwner", domain_id)?;
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
        self.permit_acquire("GrantSemaphoreToken", domain_id)?;
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
        self.permit_acquire("ReleaseSemaphoreToken", domain_id)?;
        self.store
            .release_semaphore_token(domain_id, name, bucket, token_id, owner_id)
            .await
    }
}
