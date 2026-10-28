//! Byte-preserving port of `matching_partition.rs` (Go task-list manager's
//! partition configuration cache) over the native queue/store contracts.
//! Isolation-group strings stay `GoString` end to end; nothing is converted
//! through UTF-8.
use cadence_common::{
    go_string::GoString,
    task_list::{partition_map, TaskListNameBytes},
};
use cadence_persistence::{
    matching_blob::{PartitionBytes, PartitionConfigBytes},
    matching_native_queue::{DurableTaskQueueBytes, NativeQueueError},
    matching_native_task_store::{
        MatchingTaskStoreBytes, MatchingTaskStoreBytesError as StoreError, TaskListKeyBytes,
    },
    matching_retry::{persistence_bytes, persistence_bytes_until},
    matching_task_store::{BackendErrorKind, MatchingTaskStoreError as Legacy},
};
use std::{collections::HashMap, sync::Mutex};
use tonic::Status;

/// Native analogue of `StoredPartitionConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NativeStoredPartitionConfig {
    pub version: i64,
    pub read_partitions: HashMap<i32, Vec<GoString>>,
    pub write_partitions: HashMap<i32, Vec<GoString>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionErrorKind {
    InvalidArgument,
    NotFound,
    /// Go `types.ServiceBusyError` (throttled SQL backend).
    ServiceBusy,
    /// Go `InternalServiceError` default arm of `handleErr`.
    Internal,
}

/// Error whose message keeps the original Go bytes (task-list names can be
/// embedded in persistence errors). `to_diagnostic_status` is diagnostic only: tonic
/// messages are `String`, so invalid UTF-8 is lossily rendered there. Native
/// transports must use `message` and `kind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionError {
    pub kind: PartitionErrorKind,
    pub message: GoString,
}

impl PartitionError {
    fn new(kind: PartitionErrorKind, message: impl Into<GoString>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    /// Requires RawStatusService around the native unary service.
    pub fn to_native_status(&self) -> Status {
        use crate::matching_native_status::{raw_detailed_status, raw_status};
        let code = match self.kind {
            PartitionErrorKind::InvalidArgument => tonic::Code::InvalidArgument,
            PartitionErrorKind::NotFound => tonic::Code::NotFound,
            PartitionErrorKind::Internal => tonic::Code::Internal,
            PartitionErrorKind::ServiceBusy => tonic::Code::ResourceExhausted,
        };
        if self.kind == PartitionErrorKind::ServiceBusy {
            raw_detailed_status(code, &self.message, "ServiceBusyError", Vec::new())
        } else {
            raw_status(code, &self.message, tonic::codegen::Bytes::new())
        }
    }

    pub fn to_diagnostic_status(&self) -> Status {
        let message = String::from_utf8_lossy(self.message.as_bytes()).into_owned();
        match self.kind {
            PartitionErrorKind::InvalidArgument => Status::invalid_argument(message),
            PartitionErrorKind::NotFound => Status::not_found(message),
            PartitionErrorKind::Internal => Status::internal(message),
            PartitionErrorKind::ServiceBusy => crate::grpc::detailed_status(
                tonic::Code::ResourceExhausted,
                message,
                "ServiceBusyError",
                cadence_proto::api::ServiceBusyError {
                    reason: String::new(),
                },
            ),
        }
    }
}

fn backend_kind(error: &StoreError) -> Option<BackendErrorKind> {
    match error {
        StoreError::Backend { kind, .. } | StoreError::Legacy(Legacy::Backend { kind, .. }) => {
            Some(*kind)
        }
        _ => None,
    }
}

fn error_message(error: &StoreError) -> GoString {
    match error {
        StoreError::ConditionFailed { message } => message.clone(),
        other => other.to_string().into(),
    }
}

/// Same categories as the legacy `update_failure_status`: throttled is a typed
/// ServiceBusy, BadRequest passes through as InvalidArgument, everything else
/// (ConditionFailed, timeouts, DBUnavailable) is InternalServiceError.
fn failure(error: &StoreError) -> PartitionError {
    let kind = match backend_kind(error) {
        Some(BackendErrorKind::Throttled) => PartitionErrorKind::ServiceBusy,
        Some(BackendErrorKind::BadRequest) => PartitionErrorKind::InvalidArgument,
        _ => PartitionErrorKind::Internal,
    };
    PartitionError::new(kind, error_message(error))
}

fn is_not_found(error: &StoreError) -> bool {
    matches!(error, StoreError::Legacy(Legacy::NotFound))
}

pub(crate) fn from_storage(config: PartitionConfigBytes) -> NativeStoredPartitionConfig {
    // Go ToInternalType synthesizes default entries for legacy count fields.
    let convert = |count, map: Option<std::collections::BTreeMap<i32, PartitionBytes>>| {
        partition_map(count, map.unwrap_or_default().into_iter().collect())
            .into_iter()
            .map(|(id, partition)| (id, partition.isolation_groups.unwrap_or_default()))
            .collect()
    };
    NativeStoredPartitionConfig {
        version: config.version,
        read_partitions: convert(config.num_read_partitions, config.read_partitions),
        write_partitions: convert(config.num_write_partitions, config.write_partitions),
    }
}

pub(crate) fn to_storage(config: &NativeStoredPartitionConfig) -> PartitionConfigBytes {
    let convert = |map: &HashMap<i32, Vec<GoString>>| {
        Some(
            map.iter()
                .map(|(id, groups)| {
                    (
                        *id,
                        PartitionBytes {
                            isolation_groups: Some(groups.clone()),
                        },
                    )
                })
                .collect(),
        )
    };
    // SQL serialization keeps count fields in agreement with the maps.
    PartitionConfigBytes {
        version: config.version,
        num_read_partitions: config.read_partitions.len() as i32,
        num_write_partitions: config.write_partitions.len() as i32,
        read_partitions: convert(&config.read_partitions),
        write_partitions: convert(&config.write_partitions),
    }
}

pub fn validate(config: &NativeStoredPartitionConfig) -> Result<(), PartitionError> {
    let read = &config.read_partitions;
    let write = &config.write_partitions;
    let message = if read.is_empty() {
        Some("read partitions < 1".to_owned())
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
        Some(message) => Err(PartitionError::new(
            PartitionErrorKind::InvalidArgument,
            message.as_str(),
        )),
        None => Ok(()),
    }
}

pub type PartitionUpdate = Option<(
    Option<NativeStoredPartitionConfig>,
    NativeStoredPartitionConfig,
)>;

#[derive(Default)]
pub struct NativePartitionCache(Mutex<Option<NativeStoredPartitionConfig>>);

impl NativePartitionCache {
    pub fn load(
        store: &dyn MatchingTaskStoreBytes,
        queue: &DurableTaskQueueBytes,
        key: &TaskListKeyBytes,
        kind: i16,
    ) -> Result<Self, PartitionError> {
        if kind != 0 {
            return Ok(Self::default());
        }
        let name = TaskListNameBytes::parse(key.name.as_bytes())
            .map_err(|message| PartitionError::new(PartitionErrorKind::InvalidArgument, message))?;
        let config = if name.is_root() {
            queue.partition_config()
        } else {
            let root = TaskListKeyBytes {
                name: name.root().clone(),
                ..key.clone()
            };
            match persistence_bytes(|| store.get_task_list(&root)) {
                Ok(info) => info.data.adaptive_partition_config,
                Err(error) if is_not_found(&error) => None,
                Err(error) => return Err(failure(&error)),
            }
        };
        Ok(Self(Mutex::new(config.map(from_storage))))
    }
    pub fn get(&self) -> Option<NativeStoredPartitionConfig> {
        self.0
            .lock()
            .expect("Matching partition cache lock")
            .clone()
    }
    /// Pushed notifications only replace older versions, including when an
    /// earlier notification arrives after a concurrent later update.
    pub fn push(&self, config: &NativeStoredPartitionConfig) {
        let mut cached = self.0.lock().expect("Matching partition cache lock");
        if cached
            .as_ref()
            .is_none_or(|old| old.version < config.version)
        {
            *cached = Some(config.clone());
        }
    }
    pub fn refresh(
        &self,
        store: &dyn MatchingTaskStoreBytes,
        root: &TaskListKeyBytes,
    ) -> Result<(), PartitionError> {
        self.refresh_until(store, root, || false)
    }
    pub fn refresh_until(
        &self,
        store: &dyn MatchingTaskStoreBytes,
        root: &TaskListKeyBytes,
        cancelled: impl Fn() -> bool,
    ) -> Result<(), PartitionError> {
        // Explicit reload replaces even a newer cache and can clear it.
        let config = persistence_bytes_until(|| store.get_task_list(root), cancelled)
            .map_err(|error| {
                if is_not_found(&error) {
                    PartitionError::new(PartitionErrorKind::NotFound, error_message(&error))
                } else {
                    failure(&error)
                }
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
        queue: &DurableTaskQueueBytes,
        config: NativeStoredPartitionConfig,
        now: i64,
    ) -> Result<PartitionUpdate, PartitionError> {
        self.update_until(queue, config, now, || false, || {})
    }
    /// Go `updatePartitionConfig`: `cancelled` is the caller context's `Done()` for the
    /// persistence retry; `on_persist_failure` runs once at the failure point
    /// (Go's TaskListPartitionUpdateFailed counter), never for validation errors.
    pub fn update_until(
        &self,
        queue: &DurableTaskQueueBytes,
        mut config: NativeStoredPartitionConfig,
        now: i64,
        cancelled: impl Fn() -> bool,
        on_persist_failure: impl FnOnce(),
    ) -> Result<PartitionUpdate, PartitionError> {
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
        let result = persistence_bytes_until(
            || {
                queue
                    .update_partition_config(Some(to_storage(&config)), now)
                    .map_err(|error| match error {
                        NativeQueueError::Storage(error) => error,
                        error => StoreError::ConditionFailed {
                            message: error.to_string().into(),
                        },
                    })
            },
            cancelled,
        );
        if let Err(error) = result {
            on_persist_failure();
            queue.stop();
            return Err(failure(&error));
        }
        let old = cached.replace(config.clone());
        Ok(Some((old, config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskListDataBytes},
        matching_native_task_store::{AllocatedTaskBytes, TaskListRecordBytes},
        matching_sqlite::SqliteTaskStore,
        matching_task_store::{MatchingTaskStore, TaskListKey},
    };
    use std::{
        collections::VecDeque,
        num::NonZeroU32,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    use uuid::Uuid;

    fn n100() -> NonZeroU32 {
        NonZeroU32::new(100).unwrap()
    }
    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::from_u128(1),
            name: GoString::from(name),
            task_type: 0,
        }
    }
    fn sqlite(dir: &tempfile::TempDir) -> Arc<SqliteTaskStore> {
        Arc::new(SqliteTaskStore::create(dir.path().join("q.db"), BlobEncoding::ThriftRW).unwrap())
    }
    fn open(
        store: &Arc<SqliteTaskStore>,
        k: &TaskListKeyBytes,
        lease: i64,
    ) -> DurableTaskQueueBytes {
        DurableTaskQueueBytes::open(store.clone(), k.clone(), 0, n100(), lease).unwrap()
    }
    fn parts(count: i32) -> HashMap<i32, Vec<GoString>> {
        (0..count).map(|id| (id, Vec::new())).collect()
    }
    fn request(read: i32, write: i32) -> NativeStoredPartitionConfig {
        NativeStoredPartitionConfig {
            version: 0,
            read_partitions: parts(read),
            write_partitions: parts(write),
        }
    }
    fn persisted(
        store: &SqliteTaskStore,
        k: &TaskListKeyBytes,
    ) -> Option<NativeStoredPartitionConfig> {
        MatchingTaskStoreBytes::get_task_list(store, k)
            .unwrap()
            .data
            .adaptive_partition_config
            .map(from_storage)
    }

    #[test]
    fn invalid_byte_isolation_groups_round_trip_and_queues_are_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(&dir);
        let groups = vec![
            GoString::from(vec![0xff]),
            GoString::from(vec![0xfe]),
            GoString::from("\u{fffd}"),
            GoString::from(vec![b'a', 0, b'b']),
        ];
        let (ka, kb) = (key(b"\xff"), key(b"\xfe"));
        let (qa, qb) = (open(&store, &ka, 1), open(&store, &kb, 1));
        let cache = NativePartitionCache::load(store.as_ref(), &qa, &ka, 0).unwrap();
        let mut config = request(2, 2);
        config.read_partitions.insert(1, groups.clone());
        config.write_partitions.insert(1, groups.clone());
        let (old, new) = cache.update(&qa, config, 2).unwrap().unwrap();
        assert!(old.is_none());
        assert_eq!(new.version, 1);
        // Persisted bytes round-trip exactly and the sibling raw name is untouched.
        let stored = persisted(&store, &ka).unwrap();
        assert_eq!(stored.read_partitions[&1], groups);
        assert_eq!(stored.write_partitions[&1], groups);
        assert_eq!(cache.get().unwrap(), stored);
        assert!(persisted(&store, &kb).is_none());
        assert!(qb.partition_config().is_none());
        // A non-root partition reads the raw-named root's config.
        let root = TaskListNameBytes::parse(b"\xff").unwrap();
        let child = key(root.partition_name(1).as_bytes());
        let child_queue = open(&store, &child, 1);
        let child_cache =
            NativePartitionCache::load(store.as_ref(), &child_queue, &child, 0).unwrap();
        assert_eq!(child_cache.get().unwrap().read_partitions[&1], groups);
        // Reopening the root loads the config from queue metadata.
        let reopened = open(&store, &ka, 5);
        let reloaded = NativePartitionCache::load(store.as_ref(), &reopened, &ka, 0).unwrap();
        assert_eq!(reloaded.get().unwrap().write_partitions[&1], groups);
    }

    #[test]
    fn valid_utf8_groups_are_visible_through_the_legacy_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(&dir);
        let k = key(b"queue");
        let queue = open(&store, &k, 1);
        let cache = NativePartitionCache::load(store.as_ref(), &queue, &k, 0).unwrap();
        let mut config = request(2, 2);
        config
            .read_partitions
            .insert(1, vec!["a".into(), "é".into()]);
        cache.update(&queue, config, 2).unwrap();
        let legacy_key = TaskListKey {
            domain_id: k.domain_id,
            name: "queue".into(),
            task_type: 0,
        };
        let legacy = MatchingTaskStore::get_task_list(store.as_ref(), &legacy_key)
            .unwrap()
            .data
            .adaptive_partition_config
            .unwrap();
        assert_eq!(legacy.version, 1);
        assert_eq!(
            legacy.read_partitions.unwrap()[&1].isolation_groups,
            Some(vec!["a".to_owned(), "é".to_owned()])
        );
    }

    #[test]
    fn legacy_count_fields_synthesize_default_partitions() {
        let config = from_storage(PartitionConfigBytes {
            version: 4,
            num_read_partitions: 3,
            num_write_partitions: 1,
            read_partitions: Some(Default::default()),
            write_partitions: Some(
                [(
                    0,
                    PartitionBytes {
                        isolation_groups: Some(vec![GoString::from(vec![0xff])]),
                    },
                )]
                .into(),
            ),
        });
        assert_eq!(config.version, 4);
        assert_eq!(config.read_partitions, parts(3));
        // Count agrees with the map, so the map's groups are kept.
        assert_eq!(
            config.write_partitions[&0],
            vec![GoString::from(vec![0xff])]
        );
        let absent = from_storage(PartitionConfigBytes {
            num_read_partitions: 2,
            ..Default::default()
        });
        assert_eq!(absent.read_partitions, parts(2));
        assert!(absent.write_partitions.is_empty());
    }

    #[test]
    fn validation_order_and_wording_without_failure_count() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(&dir);
        let k = key(b"tl");
        let queue = open(&store, &k, 1);
        let cache = NativePartitionCache::load(store.as_ref(), &queue, &k, 0).unwrap();
        let only = |ids: &[i32]| -> HashMap<i32, Vec<GoString>> {
            ids.iter().map(|id| (*id, Vec::new())).collect()
        };
        let cfg = |read: HashMap<i32, Vec<GoString>>, write| NativeStoredPartitionConfig {
            version: 0,
            read_partitions: read,
            write_partitions: write,
        };
        let cases = [
            (cfg(only(&[]), only(&[])), "read partitions < 1"),
            (cfg(only(&[0]), only(&[])), "write partitions < 1"),
            (
                cfg(only(&[0]), only(&[0, 1])),
                "read partitions (1) < write partitions (2)",
            ),
            (
                cfg(only(&[1, 2]), only(&[1])),
                "the root partition must always be in read partitions",
            ),
            (
                cfg(only(&[0, 1]), only(&[1])),
                "the root partition must always be in write partitions",
            ),
            (
                cfg(only(&[0, 1]), only(&[0, 2])),
                "partition 2 included in write but not read",
            ),
        ];
        let failures = AtomicUsize::new(0);
        for (config, message) in cases {
            let error = cache
                .update_until(
                    &queue,
                    config,
                    2,
                    || false,
                    || {
                        failures.fetch_add(1, Ordering::SeqCst);
                    },
                )
                .unwrap_err();
            assert_eq!(error.kind, PartitionErrorKind::InvalidArgument);
            assert_eq!(error.message.as_bytes(), message.as_bytes());
            assert_eq!(
                error.to_diagnostic_status().code(),
                tonic::Code::InvalidArgument
            );
        }
        assert_eq!(failures.load(Ordering::SeqCst), 0);
        assert!(!queue.is_stopped());
        assert!(cache.get().is_none());
    }

    #[test]
    fn update_rules_match_go_task_list_manager() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(&dir);
        let k = key(b"tl");
        let queue = open(&store, &k, 1);
        let cache = NativePartitionCache::load(store.as_ref(), &queue, &k, 0).unwrap();
        // One partition with no cached config is a no-op.
        assert!(cache.update(&queue, request(1, 1), 2).unwrap().is_none());
        assert!(cache.get().is_none() && persisted(&store, &k).is_none());
        assert!(cache.update(&queue, request(3, 3), 2).unwrap().is_some());
        assert_eq!(cache.get().unwrap().version, 1);
        // Same partitions: no version bump, nothing written.
        assert!(cache.update(&queue, request(3, 3), 3).unwrap().is_none());
        let (old, new) = cache.update(&queue, request(3, 1), 4).unwrap().unwrap();
        assert_eq!(old.unwrap().version, 1);
        assert_eq!(new.version, 2);
        assert_eq!(persisted(&store, &k).unwrap(), cache.get().unwrap());
        cache.update(&queue, request(1, 3), 5).unwrap_err();
        assert_eq!(cache.get().unwrap().version, 2);
    }

    #[test]
    fn fenced_update_stops_queue_and_fires_callback_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(&dir);
        let k = key(b"\xffq");
        let queue = open(&store, &k, 1);
        let cache = NativePartitionCache::load(store.as_ref(), &queue, &k, 0).unwrap();
        cache.update(&queue, request(3, 3), 2).unwrap();
        let _owner = open(&store, &k, 3);
        let failures = AtomicUsize::new(0);
        // Cancelled caller: first attempt still runs; ConditionFailed is not retried.
        let error = cache
            .update_until(
                &queue,
                request(3, 1),
                4,
                || true,
                || {
                    failures.fetch_add(1, Ordering::SeqCst);
                },
            )
            .unwrap_err();
        assert_eq!(error.kind, PartitionErrorKind::Internal);
        assert_eq!(error.to_diagnostic_status().code(), tonic::Code::Internal);
        assert!(error.to_diagnostic_status().details().is_empty());
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        assert!(queue.is_stopped());
        let kept = cache.get().unwrap();
        assert_eq!((kept.version, kept.write_partitions.len()), (1, 3));
    }

    #[test]
    fn push_only_replaces_older_versions() {
        let cache = NativePartitionCache::default();
        let at = |version| NativeStoredPartitionConfig {
            version,
            ..request(2, 2)
        };
        cache.push(&at(3));
        assert_eq!(cache.get().unwrap().version, 3);
        cache.push(&at(2));
        cache.push(&at(3));
        assert_eq!(cache.get().unwrap().version, 3);
        cache.push(&at(4));
        assert_eq!(cache.get().unwrap().version, 4);
    }

    /// Delegates to SQLite but fails `get_task_list` with scripted errors.
    struct Faulty {
        inner: Arc<SqliteTaskStore>,
        gets: AtomicUsize,
        script: Mutex<VecDeque<StoreError>>,
    }
    impl MatchingTaskStoreBytes for Faulty {
        fn get_task_list(
            &self,
            key: &TaskListKeyBytes,
        ) -> cadence_persistence::matching_native_task_store::Result<TaskListRecordBytes> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.script.lock().unwrap().pop_front() {
                return Err(error);
            }
            MatchingTaskStoreBytes::get_task_list(self.inner.as_ref(), key)
        }
        fn lease(
            &self,
            key: &TaskListKeyBytes,
            kind: i16,
            previous_range: i64,
            now_nanos: i64,
        ) -> cadence_persistence::matching_native_task_store::Result<TaskListRecordBytes> {
            MatchingTaskStoreBytes::lease(self.inner.as_ref(), key, kind, previous_range, now_nanos)
        }
        fn update_task_list(
            &self,
            key: &TaskListKeyBytes,
            range: i64,
            data: TaskListDataBytes,
            now_nanos: i64,
        ) -> cadence_persistence::matching_native_task_store::Result<()> {
            MatchingTaskStoreBytes::update_task_list(
                self.inner.as_ref(),
                key,
                range,
                data,
                now_nanos,
            )
        }
        fn create_tasks_with_domains(
            &self,
            key: &TaskListKeyBytes,
            range: i64,
            now_nanos: i64,
            tasks: &[(Uuid, &AllocatedTaskBytes)],
        ) -> cadence_persistence::matching_native_task_store::Result<()> {
            MatchingTaskStoreBytes::create_tasks_with_domains(
                self.inner.as_ref(),
                key,
                range,
                now_nanos,
                tasks,
            )
        }
        fn get_tasks(
            &self,
            key: &TaskListKeyBytes,
            read_level: i64,
            max_read_level: i64,
            batch_size: i64,
        ) -> cadence_persistence::matching_native_task_store::Result<Vec<AllocatedTaskBytes>>
        {
            MatchingTaskStoreBytes::get_tasks(
                self.inner.as_ref(),
                key,
                read_level,
                max_read_level,
                batch_size,
            )
        }
        fn task_list_size(
            &self,
            key: &TaskListKeyBytes,
            ack_level: i64,
        ) -> cadence_persistence::matching_native_task_store::Result<i64> {
            MatchingTaskStoreBytes::task_list_size(self.inner.as_ref(), key, ack_level)
        }
        fn complete_task(
            &self,
            key: &TaskListKeyBytes,
            task_id: i64,
        ) -> cadence_persistence::matching_native_task_store::Result<()> {
            MatchingTaskStoreBytes::complete_task(self.inner.as_ref(), key, task_id)
        }
        fn complete_tasks_less_than(
            &self,
            key: &TaskListKeyBytes,
            inclusive_task_id: i64,
            limit: i64,
        ) -> cadence_persistence::matching_native_task_store::Result<usize> {
            MatchingTaskStoreBytes::complete_tasks_less_than(
                self.inner.as_ref(),
                key,
                inclusive_task_id,
                limit,
            )
        }
        fn delete_task_list(
            &self,
            key: &TaskListKeyBytes,
            range: i64,
        ) -> cadence_persistence::matching_native_task_store::Result<()> {
            MatchingTaskStoreBytes::delete_task_list(self.inner.as_ref(), key, range)
        }
    }
    fn backend(kind: BackendErrorKind, message: &str) -> StoreError {
        StoreError::Backend {
            kind,
            message: message.into(),
        }
    }

    #[test]
    fn refresh_replaces_newer_cache_and_maps_failures() {
        let dir = tempfile::tempdir().unwrap();
        let inner = sqlite(&dir);
        let k = key(b"\xffroot");
        let _queue = open(&inner, &k, 1);
        let faulty = Faulty {
            inner: inner.clone(),
            gets: AtomicUsize::new(0),
            script: Mutex::new(VecDeque::new()),
        };
        let cache = NativePartitionCache::default();
        cache.push(&NativeStoredPartitionConfig {
            version: 9,
            ..request(2, 2)
        });
        // Direct refresh ignores versions: nothing is persisted, so the cache clears.
        cache.refresh(&faulty, &k).unwrap();
        assert!(cache.get().is_none());
        // Missing root is NotFound and leaves the cache as it was.
        cache.push(&request(2, 2));
        let missing = key(b"missing");
        let error = cache.refresh(&faulty, &missing).unwrap_err();
        assert_eq!(error.kind, PartitionErrorKind::NotFound);
        assert!(cache.get().is_some());
        // Throttled maps to typed ServiceBusy only after the retry policy stops; use a
        // cancelled context so the first failure is final.
        for (kind, expected) in [
            (BackendErrorKind::Throttled, PartitionErrorKind::ServiceBusy),
            (
                BackendErrorKind::BadRequest,
                PartitionErrorKind::InvalidArgument,
            ),
            (BackendErrorKind::Timeout, PartitionErrorKind::Internal),
        ] {
            let before = faulty.gets.load(Ordering::SeqCst);
            faulty
                .script
                .lock()
                .unwrap()
                .push_back(backend(kind, "boom"));
            let error = cache.refresh_until(&faulty, &k, || true).unwrap_err();
            assert_eq!(error.kind, expected, "{kind:?}");
            assert_eq!(faulty.gets.load(Ordering::SeqCst) - before, 1);
            assert!(cache.get().is_some());
        }
        let status =
            PartitionError::new(PartitionErrorKind::ServiceBusy, "busy").to_diagnostic_status();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(!status.details().is_empty());
    }

    #[test]
    fn refresh_cancellation_returns_previous_error_and_keeps_raw_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let inner = sqlite(&dir);
        let k = key(b"root");
        let _queue = open(&inner, &k, 1);
        let faulty = Faulty {
            inner,
            gets: AtomicUsize::new(0),
            script: Mutex::new(
                [
                    backend(BackendErrorKind::Internal, "first"),
                    backend(BackendErrorKind::Internal, "second"),
                ]
                .into(),
            ),
        };
        let cache = NativePartitionCache::default();
        // Context ends after the second attempt; Go returns prevErr, not the latest.
        let error = cache
            .refresh_until(&faulty, &k, || faulty.gets.load(Ordering::SeqCst) >= 2)
            .unwrap_err();
        assert_eq!(faulty.gets.load(Ordering::SeqCst), 2);
        assert_eq!(error.kind, PartitionErrorKind::Internal);
        assert!(String::from_utf8_lossy(error.message.as_bytes()).contains("first"));
        // A non-transient native error keeps its raw message bytes through retry.
        let raw = GoString::from(vec![b'x', 0xff]);
        faulty
            .script
            .lock()
            .unwrap()
            .push_back(StoreError::ConditionFailed {
                message: raw.clone(),
            });
        let error = cache.refresh(&faulty, &k).unwrap_err();
        assert_eq!(error.kind, PartitionErrorKind::Internal);
        assert_eq!(error.message, raw);
    }

    #[test]
    fn load_rejects_invalid_partitioned_name_with_raw_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(&dir);
        let mut name = b"/__cadence_sys/".to_vec();
        name.extend_from_slice(&[0xff, b'/', b'0']);
        let k = key(&name);
        let queue = open(&store, &k, 1);
        let error = NativePartitionCache::load(store.as_ref(), &queue, &k, 0)
            .err()
            .unwrap();
        assert_eq!(error.kind, PartitionErrorKind::InvalidArgument);
        let mut expected = b"invalid partitioned task list name ".to_vec();
        expected.extend_from_slice(&name);
        assert_eq!(error.message.as_bytes(), expected);
        // Sticky lists (kind != 0) never parse or read.
        assert!(NativePartitionCache::load(store.as_ref(), &queue, &k, 1)
            .unwrap()
            .get()
            .is_none());
    }
}
