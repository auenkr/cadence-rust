//! Go task-list manager's partition configuration cache, independent of History.
use cadence_persistence::{
    matching_blob::{Partition, PartitionConfig},
    matching_queue::{DurableTaskQueue, QueueError},
    matching_sqlite::{StorageError, TaskListKey},
    matching_task_store::MatchingTaskStore,
    StoredPartitionConfig,
};
use std::sync::Mutex;
use tonic::Status;

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
        mut config: StoredPartitionConfig,
        now: i64,
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
        let result = cadence_persistence::matching_retry::persistence(|| {
            queue
                .update_partition_config(Some(to_storage(&config)), now)
                .map_err(|error| match error {
                    QueueError::Storage(error) => error,
                    error => StorageError::ConditionFailed(error.to_string()),
                })
        });
        if let Err(error) = result {
            queue.stop();
            return Err(Status::unavailable(error.to_string()));
        }
        let old = cached.replace(config.clone());
        Ok(Some((old, config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::matching_blob::BlobEncoding;
    use cadence_persistence::matching_sqlite::SqliteTaskStore;
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
}
