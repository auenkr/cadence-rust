//! Go task-list manager's partition configuration cache, independent of History.
use cadence_persistence::{
    matching_blob::{Partition, PartitionConfig},
    matching_queue::{DurableTaskQueue, QueueError},
    matching_task_store::{MatchingTaskStore, StorageError, TaskListKey},
    StoredPartitionConfig,
};
use std::sync::Mutex;
use tonic::Status;

/// Go `taskListManagerImpl.updatePartitionConfig` returns the raw persistence error.
/// Only a throttled SQL backend becomes `types.ServiceBusyError`, which `handleErr`
/// passes through (ResourceExhausted plus typed detail). Every other error, including
/// ConditionFailed, timeouts and DBUnavailable, reaches `handleErr`'s default arm and
/// becomes `InternalServiceError` (`Status::internal`, no detail).
fn update_failure_status(error: &StorageError) -> Status {
    if matches!(
        error,
        StorageError::Backend {
            kind: cadence_persistence::matching_task_store::BackendErrorKind::Throttled,
            ..
        }
    ) {
        return crate::grpc::detailed_status(
            tonic::Code::ResourceExhausted,
            error.to_string(),
            "ServiceBusyError",
            cadence_proto::api::ServiceBusyError {
                reason: String::new(),
            },
        );
    }
    // Go `handleErr` passes a raw BadRequestError through as InvalidArgument.
    if matches!(
        error,
        StorageError::Backend {
            kind: cadence_persistence::matching_task_store::BackendErrorKind::BadRequest,
            ..
        }
    ) {
        return Status::invalid_argument(error.to_string());
    }
    Status::internal(error.to_string())
}

#[derive(Default)]
pub struct PartitionCache(Mutex<Option<StoredPartitionConfig>>);

fn from_storage(config: PartitionConfig) -> StoredPartitionConfig {
    // Go ToInternalType synthesizes default entries for legacy count fields.
    let convert = |count, map: Option<std::collections::BTreeMap<i32, Partition>>| {
        cadence_common::task_list::partition_map(
            count,
            map.unwrap_or_default().into_iter().collect(),
        )
        .into_iter()
        .map(|(id, partition)| (id, partition.isolation_groups.unwrap_or_default()))
        .collect()
    };
    StoredPartitionConfig {
        version: config.version,
        read_partitions: convert(config.num_read_partitions, config.read_partitions),
        write_partitions: convert(config.num_write_partitions, config.write_partitions),
    }
}

fn to_storage(config: &StoredPartitionConfig) -> PartitionConfig {
    let convert = |map: &std::collections::HashMap<i32, Vec<String>>| {
        Some(
            map.iter()
                .map(|(id, groups)| {
                    (
                        *id,
                        Partition {
                            isolation_groups: Some(groups.clone()),
                        },
                    )
                })
                .collect(),
        )
    };
    // SQL serialization keeps count fields in agreement with the maps.
    PartitionConfig {
        version: config.version,
        num_read_partitions: config.read_partitions.len() as i32,
        num_write_partitions: config.write_partitions.len() as i32,
        read_partitions: convert(&config.read_partitions),
        write_partitions: convert(&config.write_partitions),
    }
}

pub fn validate(config: &StoredPartitionConfig) -> Result<(), Status> {
    let read = &config.read_partitions;
    let write = &config.write_partitions;
    let message = if read.is_empty() {
        Some("read partitions < 1".into())
    } else if write.is_empty() {
        Some("write partitions < 1".into())
    } else if read.len() < write.len() {
        Some(format!(
            "read partitions ({}) < write partitions ({})",
            read.len(),
            write.len()
        ))
    } else if !read.contains_key(&0) {
        Some("the root partition must always be in read partitions".into())
    } else if !write.contains_key(&0) {
        Some("the root partition must always be in write partitions".into())
    } else {
        write
            .keys()
            .find(|id| !read.contains_key(id))
            .map(|id| format!("partition {id} included in write but not read"))
    };
    match message {
        Some(message) => Err(Status::invalid_argument(message)),
        None => Ok(()),
    }
}

impl PartitionCache {
    pub fn load(
        store: &dyn MatchingTaskStore,
        queue: &DurableTaskQueue,
        key: &TaskListKey,
        kind: i16,
    ) -> Result<Self, Status> {
        if kind != 0 {
            return Ok(Self::default());
        }
        let name = cadence_common::task_list::TaskListName::parse(&key.name)
            .map_err(Status::invalid_argument)?;
        let config = if name.is_root() {
            queue.partition_config()
        } else {
            let root = TaskListKey {
                name: name.root().into(),
                ..key.clone()
            };
            match cadence_persistence::matching_retry::persistence(|| store.get_task_list(&root)) {
                Ok(info) => info.data.adaptive_partition_config,
                Err(StorageError::NotFound) => None,
                Err(error) => return Err(Status::unavailable(error.to_string())),
            }
        };
        Ok(Self(Mutex::new(config.map(from_storage))))
    }
    pub fn get(&self) -> Option<StoredPartitionConfig> {
        self.0
            .lock()
            .expect("Matching partition cache lock")
            .clone()
    }
    /// Pushed notifications only replace older versions, including when an
    /// earlier notification arrives after a concurrent later update.
    pub fn push(&self, config: &StoredPartitionConfig) {
        let mut cached = self.0.lock().expect("Matching partition cache lock");
        if cached
            .as_ref()
            .is_none_or(|old| old.version < config.version)
        {
            *cached = Some(config.clone());
        }
    }
    pub fn refresh(&self, store: &dyn MatchingTaskStore, root: &TaskListKey) -> Result<(), Status> {
        // Explicit reload replaces even a newer cache and can clear it.
        let config = cadence_persistence::matching_retry::persistence(|| store.get_task_list(root))
            .map_err(|error| match error {
                StorageError::NotFound => Status::not_found(error.to_string()),
                _ => Status::unavailable(error.to_string()),
            })?
            .data
            .adaptive_partition_config
            .map(from_storage);
        *self.0.lock().expect("Matching partition cache lock") = config;
        Ok(())
    }
    /// Caller owns the root manager. Serialize version selection with all
    /// updates/refreshes and publish only after successful fenced persistence.
    pub fn update(
        &self,
        queue: &DurableTaskQueue,
        config: StoredPartitionConfig,
        now: i64,
    ) -> Result<Option<(Option<StoredPartitionConfig>, StoredPartitionConfig)>, Status> {
        self.update_until(queue, config, now, || false, || {})
    }
    /// Go `updatePartitionConfig`: `cancelled` is the caller context's `Done()` for the
    /// persistence retry; `on_persist_failure` runs once at the failure point
    /// (Go's TaskListPartitionUpdateFailed counter), never for validation errors.
    pub fn update_until(
        &self,
        queue: &DurableTaskQueue,
        mut config: StoredPartitionConfig,
        now: i64,
        cancelled: impl Fn() -> bool,
        on_persist_failure: impl FnOnce(),
    ) -> Result<Option<(Option<StoredPartitionConfig>, StoredPartitionConfig)>, Status> {
        validate(&config)?;
        let mut cached = self.0.lock().expect("Matching partition cache lock");
        if let Some(old) = &*cached {
            if old.read_partitions == config.read_partitions
                && old.write_partitions == config.write_partitions
            {
                return Ok(None);
            }
        } else if config.read_partitions.len() == 1 && config.write_partitions.len() == 1 {
            return Ok(None);
        }
        config.version = cached
            .as_ref()
            .map(|old| old.version)
            .unwrap_or_default()
            .wrapping_add(1);
        let result = cadence_persistence::matching_retry::persistence_until(
            || {
                queue
                    .update_partition_config(Some(to_storage(&config)), now)
                    .map_err(|error| match error {
                        QueueError::Storage(error) => error,
                        error => StorageError::ConditionFailed(error.to_string()),
                    })
            },
            cancelled,
        );
        if let Err(error) = result {
            on_persist_failure();
            queue.stop();
            return Err(update_failure_status(&error));
        }
        let old = cached.replace(config.clone());
        Ok(Some((old, config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};
    #[test]
    fn matching_native_partition_updates_match_original_go_fixtures() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-partitions.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let directory = tempfile::tempdir().unwrap();
            let store = std::sync::Arc::new(
                SqliteTaskStore::create(directory.path().join("queue.db"), BlobEncoding::ThriftRW)
                    .unwrap(),
            );
            let key = TaskListKey {
                domain_id: uuid::Uuid::from_u128(1),
                name: "queue".into(),
                task_type: 0,
            };
            let queue = DurableTaskQueue::open(
                store.clone(),
                key.clone(),
                0,
                std::num::NonZeroU32::new(100).unwrap(),
                1,
            )
            .unwrap();
            let cache = PartitionCache::load(store.as_ref(), &queue, &key, 0).unwrap();
            let mut writes = 0;
            for (update, expected) in case["updates"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case["results"].as_array().unwrap())
            {
                let config: StoredPartitionConfig = serde_json::from_value(update.clone()).unwrap();
                let error = match cache.update(&queue, config, 2) {
                    Ok(Some(_)) => {
                        writes += 1;
                        String::new()
                    }
                    Ok(None) => String::new(),
                    Err(error) => error.message().to_string(),
                };
                assert_eq!(error, expected["error"].as_str().unwrap());
                assert_eq!(writes, expected["writes"].as_i64().unwrap());
                assert_eq!(
                    serde_json::to_value(cache.get()).unwrap(),
                    expected["config"]
                );
                let persisted = store
                    .get_task_list(&key)
                    .unwrap()
                    .data
                    .adaptive_partition_config
                    .map(from_storage);
                assert_eq!(serde_json::to_value(persisted).unwrap(), expected["config"]);
            }
        }
    }
    // Go: TestUpdateTaskListPartitionConfig (task_list_manager_test.go:1498), over a SQLite store.
    #[test]
    fn update_partition_config_cases_match_go_task_list_manager_test() {
        let parts = |count: i32| -> std::collections::HashMap<i32, Vec<String>> {
            (0..count).map(|id| (id, Vec::new())).collect()
        };
        let request = |read: i32, write: i32| StoredPartitionConfig {
            version: 0,
            read_partitions: parts(read),
            write_partitions: parts(write),
        };
        let fixture = || {
            let directory = tempfile::tempdir().unwrap();
            let store = std::sync::Arc::new(
                SqliteTaskStore::create(directory.path().join("queue.db"), BlobEncoding::ThriftRW)
                    .unwrap(),
            );
            let key = TaskListKey { domain_id: uuid::Uuid::from_u128(1), name: "tl".into(), task_type: 0 };
            let queue = DurableTaskQueue::open(
                store.clone(),
                key.clone(),
                0,
                std::num::NonZeroU32::new(100).unwrap(),
                1,
            )
            .unwrap();
            let cache = PartitionCache::load(store.as_ref(), &queue, &key, 0).unwrap();
            (directory, store, key, queue, cache)
        };
        let persisted = |store: &SqliteTaskStore, key: &TaskListKey| {
            store.get_task_list(key).unwrap().data.adaptive_partition_config.map(from_storage)
        };

        // "success - nil original config, request of one partition": no-op, nothing persisted.
        let (_d, store, key, queue, cache) = fixture();
        assert!(cache.update(&queue, request(1, 1), 2).unwrap().is_none());
        assert!(cache.get().is_none());
        assert!(persisted(&store, &key).is_none());

        // "success - update" then "success - no op" (same partitions) at version 1 -> 2.
        let (_d, store, key, queue, cache) = fixture();
        assert!(cache.update(&queue, request(3, 3), 2).unwrap().is_some());
        assert_eq!(cache.get().unwrap().version, 1);
        assert!(cache.update(&queue, request(3, 3), 3).unwrap().is_none());
        assert_eq!(cache.get().unwrap().version, 1);
        let (old, new) = cache.update(&queue, request(3, 1), 4).unwrap().unwrap();
        assert_eq!(old.unwrap().version, 1);
        assert_eq!((new.version, new.read_partitions.len(), new.write_partitions.len()), (2, 3, 1));
        assert_eq!(
            serde_json::to_value(persisted(&store, &key)).unwrap(),
            serde_json::to_value(cache.get()).unwrap()
        );

        // Invalid config (read < write): rejected before any write, cache unchanged.
        let error = cache.update(&queue, request(1, 3), 5).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(cache.get().unwrap().version, 2);

        // "failed to update": persistence error is returned, cache keeps the old config and the
        // queue is stopped (Go asserts tlm.stopped == 1).
        let (_d, store, key, queue, cache) = fixture();
        cache.update(&queue, request(3, 3), 2).unwrap();
        // A newer lease owner fences this stale queue's config update.
        let _owner = DurableTaskQueue::open(
            store.clone(),
            key.clone(),
            0,
            std::num::NonZeroU32::new(100).unwrap(),
            3,
        )
        .unwrap();
        let error = cache.update(&queue, request(3, 1), 4).unwrap_err();
        // Go handleErr default arm: ConditionFailed becomes InternalServiceError.
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error.details().is_empty());
        assert_eq!(cache.get().unwrap().version, 1);
        assert_eq!(cache.get().unwrap().write_partitions.len(), 3);
        assert!(queue.is_stopped());
    }

    #[test]
    fn update_failure_status_matches_go_handle_err_categories() {
        use cadence_persistence::matching_task_store::BackendErrorKind as K;
        let backend = |kind| StorageError::Backend {
            kind,
            message: "boom".into(),
        };
        // InternalServiceError, TimeoutError, DBUnavailableError, ConditionFailedError and
        // untyped errors all hit the default arm.
        for error in [
            backend(K::Timeout),
            backend(K::Internal),
            backend(K::Permanent),
            backend(K::DbUnavailable),
            StorageError::ConditionFailed("lease lost".into()),
        ] {
            let status = update_failure_status(&error);
            assert_eq!(status.code(), tonic::Code::Internal, "{error:?}");
            assert_eq!(status.message(), error.to_string());
            assert!(status.details().is_empty());
        }
        // Throttling is Go ServiceBusyError, passed through typed.
        let status = update_failure_status(&backend(K::Throttled));
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(!status.details().is_empty());
    }

    #[test]
    fn bad_request_backend_error_is_invalid_argument() {
        let status = update_failure_status(&StorageError::Backend {
            kind: cadence_persistence::matching_task_store::BackendErrorKind::BadRequest,
            message: "bad".into(),
        });
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn failure_callback_fires_once_for_persistence_failure_not_for_validation() {
        let directory = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(
            SqliteTaskStore::create(directory.path().join("queue.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let key = TaskListKey {
            domain_id: uuid::Uuid::from_u128(1),
            name: "tl".into(),
            task_type: 0,
        };
        let open = |lease| {
            DurableTaskQueue::open(
                store.clone(),
                key.clone(),
                0,
                std::num::NonZeroU32::new(100).unwrap(),
                lease,
            )
            .unwrap()
        };
        let queue = open(1);
        let cache = PartitionCache::load(store.as_ref(), &queue, &key, 0).unwrap();
        let parts = |count: i32| -> std::collections::HashMap<i32, Vec<String>> {
            (0..count).map(|id| (id, Vec::new())).collect()
        };
        let request = |read, write| StoredPartitionConfig {
            version: 0,
            read_partitions: parts(read),
            write_partitions: parts(write),
        };
        let failures = std::cell::Cell::new(0);
        // Validation failure: no counter.
        cache
            .update_until(&queue, request(1, 3), 2, || false, || failures.set(failures.get() + 1))
            .unwrap_err();
        assert_eq!(failures.get(), 0);
        // Fencing failure, even with an already cancelled caller: first attempt runs and
        // the counter fires exactly once; ConditionFailed is not retried.
        let _owner = open(3);
        let error = cache
            .update_until(&queue, request(3, 3), 4, || true, || failures.set(failures.get() + 1))
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(failures.get(), 1);
        assert!(queue.is_stopped());
    }
}
