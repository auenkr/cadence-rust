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
        AcquireError, GrantOutcome, SemaphoreBucketPage, SemaphoreError, SemaphoreMetadata,
        SemaphorePersistence,
    },
};
use std::sync::Arc;
use uuid::Uuid;

#[path = "matching_persistence_native.rs"]
mod native;
pub use native::{limit_semaphore_store_bytes, limit_task_store_bytes};

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
                // Go accounts the TaskInfo RunID string payload, in bytes. Preserve a
                // raw request string while it is in memory; persisted UUID bytes read
                // back as the canonical 36-byte UUID string.
                let run_id = task.data.raw_run_id.as_ref().map_or_else(
                    || if task.data.run_id.is_some() { 36 } else { 0 },
                    |raw| raw.len() as u64,
                );
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
            DomainErrorClass::Internal | DomainErrorClass::CapturedPanic => PersistenceErrorClass::Internal,
        });
        self.limiter.record_domain_call(operation, elapsed, class);
    }
}

// 64-bit `unsafe.Sizeof` of the Go structs in go/common/persistence/data_manager_interfaces.go.
const LIST_DOMAINS_RESPONSE_SIZEOF: u64 = 48; // []*GetDomainResponse + []byte
const GET_DOMAIN_RESPONSE_SIZEOF: u64 = 88;
const DOMAIN_INFO_SIZEOF: u64 = 80;
const DOMAIN_CONFIG_SIZEOF: u64 = 120;
const ASYNC_WORKFLOW_CONFIG_SIZEOF: u64 = 48;

fn string_map_size<'a>(entries: impl Iterator<Item = (&'a String, &'a String)>) -> u64 {
    entries.map(|(k, v)| (k.len() + v.len()) as u64).sum()
}

/// Go `GetDomainResponse.ByteSize` (go/common/persistence/metered.go:115) with
/// `DomainInfo`, `DomainConfig` and `DomainReplicationConfig` `ByteSize`.
pub(crate) fn get_domain_byte_size(
    d: &cadence_persistence::matching_domain_metadata::MatchingDomainMetadata,
) -> u64 {
    let info = DOMAIN_INFO_SIZEOF
        + (d.info.uuid.len() + d.info.name.len() + d.info.description.len() + d.info.owner_email.len()) as u64
        + d.info.data.as_ref().map_or(0, |m| string_map_size(m.iter()));

    let mut config = DOMAIN_CONFIG_SIZEOF
        + (d.configuration.history_archival_uri.len() + d.configuration.visibility_archival_uri.len()) as u64
        + ASYNC_WORKFLOW_CONFIG_SIZEOF;
    if let Some(a) = &d.decoded_async_workflow_config {
        config += (a.predefined_queue_name.len() + a.queue_type.len()) as u64;
        config += a.queue_config.as_ref().map_or(0, |q| q.data.len() as u64);
    }
    for (key, value) in d.decoded_bad_binaries.iter().filter_map(|b| b.binaries.as_ref()).flatten() {
        config += key.len() as u64;
        if let Some(v) = value {
            config += (v.reason.len() + v.operator.len()) as u64;
        }
    }
    // Go adds len(key)+len(value.Name) per group (key == name when decoded).
    for group in d.decoded_isolation_groups.iter().flat_map(|g| g.isolation_groups.iter()) {
        config += (group.name.len() * 2) as u64;
    }

    let replication = d.replication.active_cluster_name.len() as u64
        + d.replication.clusters.iter().map(|c| c.cluster_name.len() as u64).sum::<u64>();
    GET_DOMAIN_RESPONSE_SIZEOF + info + config + replication
}

/// Go `ListDomainsResponse.ByteSize` (go/common/persistence/metered.go:102).
pub(crate) fn list_domains_byte_size(
    page: &DomainMetadataPage,
) -> u64 {
    LIST_DOMAINS_RESPONSE_SIZEOF
        + page.next_page_token.as_ref().map_or(0, |t| t.len() as u64)
        + page.domains.iter().map(get_domain_byte_size).sum::<u64>()
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
            let payload = list_domains_byte_size(page);
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
    fn get_domain_by_raw_id(
        &self,
        id: &[u8],
        timeout: std::time::Duration,
        mode: crate::matching_domain_uuid::DomainIdParse,
    ) -> Result<cadence_persistence::matching_domain_metadata::MatchingDomainMetadata, DomainMetadataSourceError> {
        use crate::matching_domain_cache::DomainErrorClass;
        use crate::matching_persistence_limiter::PersistenceErrorClass;
        self.limiter.record_domain_request("GetDomain");
        let started = std::time::Instant::now();
        let result = if self.limiter.allow() {
            self.source.get_domain_by_raw_id(id, timeout, mode)
        } else {
            self.limiter.record_domain_rejection("GetDomain");
            let result = Err(DomainMetadataSourceError::transient(REJECTED));
            self.limiter.record_domain_result("GetDomain", started.elapsed(), Some(PersistenceErrorClass::Busy));
            return result;
        };
        if !result.as_ref().err().is_some_and(|error| error.class == DomainErrorClass::CapturedPanic) {
            let class = result.as_ref().err().map(|error| match error.class {
                DomainErrorClass::NotFound => PersistenceErrorClass::NotFound,
                DomainErrorClass::BadRequest => PersistenceErrorClass::BadRequest,
                DomainErrorClass::DbUnavailable => PersistenceErrorClass::DbUnavailable,
                DomainErrorClass::Internal | DomainErrorClass::CapturedPanic => PersistenceErrorClass::Internal,
            });
            self.limiter.record_domain_result("GetDomain", started.elapsed(), class);
        }
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
        page_size: usize,
        next_page_token: Option<&[u8]>,
    ) -> Result<SemaphoreBucketPage, AcquireError> {
        self.permit_acquire("ScanSemaphoreBucket", domain_id)?;
        self.store
            .scan_semaphore_bucket(domain_id, name, bucket, page_size, next_page_token)
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

#[cfg(test)]
mod list_domain_metric_tests {
    use super::*;
    use crate::matching_metrics::{
        MetricValue, RecordingMetricSink, PERSISTENCE_EMPTY_RESPONSE,
        PERSISTENCE_RESPONSE_PAYLOAD_SIZE, PERSISTENCE_RESPONSE_ROW_SIZE,
    };
    use std::sync::{atomic::AtomicUsize, Mutex};

    struct EmptyDomainList;

    impl DomainMetadataSource for EmptyDomainList {
        fn metadata_version(
            &self,
            _timeout: std::time::Duration,
        ) -> Result<i64, DomainMetadataSourceError> {
            Ok(0)
        }

        fn list_domains(
            &self,
            _page_size: usize,
            _next_page_token: Option<&[u8]>,
            _timeout: std::time::Duration,
        ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
            Ok(DomainMetadataPage {
                domains: Vec::new(),
                next_page_token: None,
            })
        }

        fn get_domain(
            &self,
            _name: Option<&str>,
            _id: Option<&str>,
            _timeout: std::time::Duration,
        ) -> Result<
            cadence_persistence::matching_domain_metadata::MatchingDomainMetadata,
            DomainMetadataSourceError,
        > {
            Err(DomainMetadataSourceError::transient("unused"))
        }
    }

    #[test]
    fn raw_sql_miss_rate_limit_precedes_parse_and_panic_only_counts_request() {
        use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
        use crate::matching_domain_cache::MatchingDomainCache;
        use crate::matching_domain_uuid::DomainIdParse;
        let overlay = Arc::new(Mutex::new(crate::admin::DynamicConfigOverlay::default()));
        overlay.lock().unwrap().update(false, "matching.persistenceMaxQPS", vec![DynamicConfigValue {
            filters: vec![], value: Some(DataBlob { encoding_type: Some(EncodingType::JSON), data: b"1".to_vec() }),
        }]);
        let limiter = MatchingPersistenceLimiter::new(overlay, Arc::new(AtomicUsize::new(1)));
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        let cache = MatchingDomainCache::new(limit_domain_source(Arc::new(EmptyDomainList), limiter.clone()))
            .with_domain_id_parse(DomainIdParse::GoogleUuidPanic);
        let panic = cache.get_domain_by_raw_id(&[0xff]).unwrap_err();
        assert!(crate::matching_panic::is_captured_panic(&panic));
        let events = sink.events();
        assert_eq!(events.iter().filter(|e| e.name == "persistence_requests").count(), 1);
        assert!(!events.iter().any(|e| e.name.starts_with("persistence_errors") || e.name.starts_with("persistence_latency")));
        // The first call used the only token, so even malformed bytes are rejected
        // before the SQL parser is reached on the second call.
        let rejected = cache.get_domain_by_raw_id(b"bad").unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::Internal);
        assert_eq!(rejected.message(), REJECTED);
        assert_eq!(limiter.rejected(), 1);
        let events = sink.events();
        assert_eq!(events.iter().filter(|e| e.name == "persistence_requests").count(), 2);
        assert_eq!(events.iter().filter(|e| e.name == "persistence_latency").count(), 1);
        assert_eq!(events.iter().filter(|e| e.name == "persistence_errors_busy").count(), 1);
    }

    #[test]
    fn raw_sql_accepted_non_utf8_wrapper_reaches_source_with_uuid_body() {
        use crate::matching_domain_cache::MatchingDomainCache;
        use crate::matching_domain_uuid::DomainIdParse;
        let cache = MatchingDomainCache::new(Arc::new(EmptyDomainList))
            .with_domain_id_parse(DomainIdParse::GoogleUuidPanic);
        let mut id = b"x0123abcd-4567-89ef-abcd-0123456789aby".to_vec();
        id[0] = 0xff;
        id[37] = 0xfe;
        let error = cache.get_domain_by_raw_id(&id).unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "unused");
    }

    #[test]
    fn raw_domain_request_metric_is_visible_while_source_is_blocked() {
        use crate::matching_domain_cache::MatchingDomainCache;
        use crate::matching_domain_uuid::DomainIdParse;
        use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
        use std::sync::mpsc;

        struct InspectingSource {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }

        impl DomainMetadataSource for InspectingSource {
            fn metadata_version(
                &self,
                _: std::time::Duration,
            ) -> Result<i64, DomainMetadataSourceError> {
                Ok(0)
            }
            fn list_domains(
                &self,
                _: usize,
                _: Option<&[u8]>,
                _: std::time::Duration,
            ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
                unreachable!("not used")
            }
            fn get_domain(
                &self,
                _: Option<&str>,
                _: Option<&str>,
                _: std::time::Duration,
            ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
                unreachable!("raw lookup override is used")
            }
            fn get_domain_by_raw_id(
                &self,
                _: &[u8],
                _: std::time::Duration,
                _: DomainIdParse,
            ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
                Err(DomainMetadataSourceError::transient("source finished"))
            }
        }

        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(crate::admin::DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let source = Arc::new(InspectingSource {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let cache = MatchingDomainCache::new(limit_domain_source(source, limiter))
            .with_domain_id_parse(DomainIdParse::GoogleUuidPanic);
        let lookup = std::thread::spawn(move || cache.get_domain_by_raw_id(b"01234567890123456789012345678901"));

        entered_rx.recv().unwrap();
        let events = sink.events();
        assert_eq!(events.iter().filter(|e| e.name == "persistence_requests").count(), 1);
        assert!(!events.iter().any(|e| e.name.starts_with("persistence_latency") || e.name.starts_with("persistence_errors")));

        release_tx.send(()).unwrap();
        assert!(lookup.join().unwrap().is_err());
        let events = sink.events();
        assert_eq!(events.iter().filter(|e| e.name == "persistence_requests").count(), 1);
        assert_eq!(events.iter().filter(|e| e.name == "persistence_latency").count(), 1);
        assert_eq!(events.iter().filter(|e| e.name == "persistence_errors_db_unavailable").count(), 1);
    }

    #[test]
    fn empty_list_domains_emits_go_empty_count_and_envelope_payload() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(crate::admin::DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        let source = RateLimitedDomainSource {
            source: Arc::new(EmptyDomainList),
            limiter,
        };
        assert!(source
            .list_domains(10, None, std::time::Duration::from_secs(1))
            .is_ok());

        let events = sink.events();
        let empty = events
            .iter()
            .find(|e| e.name == PERSISTENCE_EMPTY_RESPONSE)
            .expect("empty-response count");
        assert_eq!(empty.tags["operation"], "ListDomain");
        assert_eq!(empty.tags["task_category"], "none");
        assert!(!empty.tags.contains_key("domain"));
        assert!(!events.iter().any(|e| e.name == PERSISTENCE_RESPONSE_ROW_SIZE));

        // Go ListDomainsResponse.ByteSize with no domains and no token is Sizeof(48) on 64-bit.
        let payload = events
            .iter()
            .find(|e| e.name == PERSISTENCE_RESPONSE_PAYLOAD_SIZE)
            .expect("payload-size histogram");
        assert_eq!(payload.value, MetricValue::Count(48));
        assert_eq!(payload.tags["operation"], "ListDomain");
        assert_eq!(payload.tags["task_category"], "none");

        let request = events
            .iter()
            .find(|e| e.name == "persistence_requests")
            .expect("overall request counter");
        assert_eq!(request.tags["operation"], "ListDomain");
        assert!(!request.tags.contains_key("domain"));
    }

    /// Mirrors go/common/persistence/metered_test.go "full response" (535):
    /// 48+3 (token) + 88 (empty GetDomainResponse, nil sub-structs) + 88+103 (info)
    /// + (120+3+3 +48+4+4+3 +6) (config=191) + 14 (replication).
    ///
    /// The nil entry in Go's list contributes 0 and has no Rust analog.
    #[test]
    fn list_domains_byte_size_matches_go_metered_test_vector() {
        use cadence_api::types::*;
        use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
        let base = || MatchingDomainMetadata {
            info: DomainInfo::default(),
            configuration: DomainConfiguration::default(),
            replication: DomainReplicationConfiguration::default(),
            is_global_domain: false,
            archival_bucket: String::new(),
            status_raw: 0,
            archival_status_raw: 0,
            history_archival_status_raw: 0,
            visibility_archival_status_raw: 0,
            config_version: 0,
            notification_version: 0,
            failover_notification_version: 0,
            failover_version: 0,
            previous_failover_version: 0,
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
        };
        let empty = base();
        // Rust always carries non-nil info/config/replication: 88 + 80 + (120+48) + 0.
        assert_eq!(get_domain_byte_size(&empty), 336);
        let mut full = base();
        full.info = DomainInfo {
            uuid: "ID".into(),
            name: "Name".into(),
            description: "Desc".into(),
            owner_email: "Email".into(),
            data: Some([("key".to_owned(), "value".to_owned())].into()),
            ..Default::default()
        };
        full.configuration.history_archival_uri = "URI".into();
        full.configuration.visibility_archival_uri = "URI".into();
        full.decoded_isolation_groups = Some(IsolationGroupConfiguration {
            isolation_groups: vec![IsolationGroupPartition { name: "abc".into(), state: None }],
        });
        // Go's map key is the partition name when decoded from thrift, so len(key)+len(Name) = 2*len(name).
        full.decoded_async_workflow_config = Some(AsyncWorkflowConfiguration {
            enabled: false,
            predefined_queue_name: "name".into(),
            queue_type: "type".into(),
            queue_config: Some(DataBlob { encoding_type: None, data: vec![1, 2, 3] }),
        });
        full.replication.active_cluster_name = "cluster".into();
        full.replication.clusters = vec![ClusterReplicationConfiguration { cluster_name: "cluster".into() }];
        assert_eq!(get_domain_byte_size(&full), 88 + 103 + 191 + 14);
        let page = DomainMetadataPage {
            domains: vec![empty, full],
            next_page_token: Some(vec![1, 2, 3]),
        };
        // 535 - 88 (Go's empty entry has nil sub-structs) + 336 = 783.
        assert_eq!(list_domains_byte_size(&page), 783);
    }
}
