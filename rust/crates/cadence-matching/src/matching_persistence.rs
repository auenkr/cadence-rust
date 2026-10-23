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

/// Go `GetTasksResponse.ByteSize` (amd64): slice header plus, per `TaskInfo`,
/// `Sizeof` (128) and the string/map payload lengths.
fn get_tasks_byte_size(tasks: &[cadence_persistence::matching_task_store::AllocatedTask]) -> u64 {
    const RESPONSE_SIZE: u64 = 24;
    const TASK_INFO_SIZE: u64 = 128;
    // DomainID is not part of the Rust row; the Go value is a 36-byte UUID string.
    const DOMAIN_ID_LEN: u64 = 36;
    RESPONSE_SIZE
        + tasks
            .iter()
            .map(|task| {
                let run_id = if task.data.run_id.is_some() { 36 } else { 0 };
                let partition = task.data.partition_config.as_ref().map_or(0, |map| {
                    map.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
                });
                TASK_INFO_SIZE
                    + DOMAIN_ID_LEN
                    + task.data.workflow_id.len() as u64
                    + run_id
                    + partition
            })
            .sum::<u64>()
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
        if let Ok(tasks) = &result {
            self.limiter.record_list_response(
                "GetTasks",
                tasks.len() as u64,
                get_tasks_byte_size(tasks),
            );
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

impl RateLimitedDomainSource {
    fn record<T>(
        &self,
        operation: &'static str,
        elapsed: std::time::Duration,
        result: &Result<T, DomainMetadataSourceError>,
    ) {
        use crate::matching_domain_cache::DomainErrorClass;
        use crate::matching_persistence_limiter::PersistenceErrorClass;
        let class = result.as_ref().err().map(|error| match error.class {
            DomainErrorClass::NotFound => PersistenceErrorClass::NotFound,
            DomainErrorClass::BadRequest => PersistenceErrorClass::BadRequest,
            DomainErrorClass::DbUnavailable => PersistenceErrorClass::DbUnavailable,
            DomainErrorClass::Internal => PersistenceErrorClass::Internal,
        });
        self.limiter.record_domain_call(operation, elapsed, class);
    }
}

impl DomainMetadataSource for RateLimitedDomainSource {
    fn metadata_version(
        &self,
        timeout: std::time::Duration,
    ) -> Result<i64, DomainMetadataSourceError> {
        self.permit("GetMetadata", None)?;
        let started = std::time::Instant::now();
        let result = self.source.metadata_version(timeout);
        self.record("GetMetadata", started.elapsed(), &result);
        result
    }
    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: std::time::Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        self.permit("ListDomain", None)?;
        let started = std::time::Instant::now();
        let result = self
            .source
            .list_domains(page_size, next_page_token, timeout);
        self.record("ListDomain", started.elapsed(), &result);
        if let Ok(page) = &result {
            // Go `ListDomainsResponse.ByteSize`: Sizeof (32) + token + domains.
            let payload = 32
                + page.next_page_token.as_ref().map_or(0, |t| t.len() as u64)
                + page
                    .domains
                    .iter()
                    .map(|d| {
                        // Config and replication sizes are not modeled; the
                        // info part follows `DomainInfo.ByteSize`.
                        128 + 36
                            + (d.info.name.len() + d.info.description.len() + d.info.owner_email.len())
                                as u64
                            + d.info.data.iter().flat_map(|m| m.iter()).map(|(k, v)| (k.len() + v.len()) as u64).sum::<u64>()
                    })
                    .sum::<u64>();
            self.limiter
                .record_list_response("ListDomain", page.domains.len() as u64, payload);
        }
        result
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
        let started = std::time::Instant::now();
        let result = self.source.get_domain(name, id, timeout);
        self.record("GetDomain", started.elapsed(), &result);
        result
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
