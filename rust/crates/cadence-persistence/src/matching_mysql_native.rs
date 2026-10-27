//! Byte-preserving `MatchingTaskStoreBytes` for the MySQL backend.
//!
//! Go binds task-list names as raw `string` bytes through database/sql, so the
//! driver sends them unchanged and the server's column charset decides whether
//! to accept them. We bind `&[u8]` parameters (`Value::Bytes`, the same wire
//! representation the legacy adapter produces for `&String`) and never convert
//! to UTF-8 or substitute replacement runes. Any rejection of invalid bytes
//! comes from MySQL itself and is classified by `normalize_mysql_error`, as in
//! Go. Lease-renewal errors retain the original name in a raw-byte message.

use super::*;
use crate::matching_blob::TaskListDataBytes;
use crate::matching_native_task_store::{
    AllocatedTaskBytes, MatchingTaskStoreBytes, MatchingTaskStoreBytesError as BytesError,
    Result as BytesResult, TaskListKeyBytes, TaskListRecordBytes,
};

fn leg<E: Into<StorageError>>(error: E) -> BytesError {
    BytesError::Legacy(error.into())
}

impl MySqlTaskStore {
    fn pool_bytes(&self, key: &TaskListKeyBytes) -> &Arc<SyncConnectionPool> {
        let index = mysql_database_index(self.database_shard_bytes(key) as usize, self.pools.len());
        &self.pools[index]
    }

    fn database_shard_bytes(&self, key: &TaskListKeyBytes) -> i32 {
        crate::matching_task_store::go_db_shard_id_bytes(
            &key.domain_id,
            key.name.as_bytes(),
            self.n_shards,
        )
    }

    fn write_encoding_bytes(&self) -> BytesResult<BlobEncoding> {
        self.write_encoding().map_err(leg)
    }

    fn encode_task_list_bytes(
        &self,
        data: &TaskListDataBytes,
    ) -> BytesResult<(Vec<u8>, &'static str)> {
        let encoding = self.write_encoding_bytes()?;
        Ok((
            encoding
                .encode(&blob::encode_task_list_bytes(data))
                .map_err(leg)?,
            encoding.as_str(),
        ))
    }

    fn decode_task_list_row_bytes(row: (i64, Vec<u8>, String)) -> BytesResult<TaskListRecordBytes> {
        let (range_id, bytes, encoding) = row;
        let encoded = BlobEncoding::parse(&encoding)
            .map_err(leg)?
            .decode(&bytes)
            .map_err(leg)?;
        Ok(TaskListRecordBytes {
            range_id,
            data: blob::decode_task_list_bytes(&encoded).map_err(leg)?,
        })
    }

    fn read_task_list_row_bytes(
        &self,
        connection: &mut ConnectionLease,
        key: &TaskListKeyBytes,
    ) -> BytesResult<Option<(i64, Vec<u8>, String)>> {
        let shard_id = self.database_shard_bytes(key);
        let discard = connection.discard.clone();
        type Row = (Vec<u8>, i64, Vec<u8>, i32, Vec<u8>, String);
        let row: Option<Row> = connection
            .exec_first(
                SELECT_TASK_LIST,
                (
                    shard_id,
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        Ok(row.map(|(_, range_id, _, _, data, encoding)| (range_id, data, encoding)))
    }

    fn lock_and_check_bytes(
        transaction: &mut Transaction<'_>,
        discard: &AtomicBool,
        shard_id: i32,
        key: &TaskListKeyBytes,
        expected_range: i64,
    ) -> BytesResult<()> {
        let range: Option<i64> = transaction
            .exec_first(
                LOCK_TASK_LIST,
                (
                    shard_id,
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, discard)))?;
        match range {
            Some(range) if range == expected_range => Ok(()),
            Some(range) => Err(leg(StorageError::ConditionFailed(format!(
                "Task list range ID was {range} when it was should have been {expected_range}"
            )))),
            None => Err(leg(StorageError::ConditionFailed(
                "Task list does not exist.".into(),
            ))),
        }
    }
}

impl MatchingTaskStoreBytes for MySqlTaskStore {
    fn fetch_latest_config(
        &self,
        row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        MatchingTaskStore::fetch_latest_config(self, row_type)
    }

    fn get_task_list(&self, key: &TaskListKeyBytes) -> BytesResult<TaskListRecordBytes> {
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let row = self
            .read_task_list_row_bytes(&mut connection, key)?
            .ok_or_else(|| leg(StorageError::NotFound))?;
        Self::decode_task_list_row_bytes(row)
    }

    fn lease(
        &self,
        key: &TaskListKeyBytes,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> BytesResult<TaskListRecordBytes> {
        let shard_id = self.database_shard_bytes(key);
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        let stale = |got: i64| BytesError::lease_renewal(key, got);
        let mut current = match self.read_task_list_row_bytes(&mut connection, key)? {
            Some((range_id, bytes, encoding)) => {
                // Go checks the requested range before decoding persisted data.
                if previous_range > 0 && previous_range != range_id {
                    return Err(stale(range_id));
                }
                Self::decode_task_list_row_bytes((range_id, bytes, encoding))?
            }
            None => {
                let data = TaskListDataBytes {
                    kind,
                    last_updated_nanos: now_nanos,
                    ..Default::default()
                };
                let (bytes, encoding) = self.encode_task_list_bytes(&data)?;
                connection
                    .exec_drop(
                        INSERT_TASK_LIST,
                        (
                            shard_id,
                            key.domain_id.as_bytes().as_slice(),
                            key.name.as_bytes(),
                            key.task_type,
                            0_i64,
                            bytes,
                            encoding,
                        ),
                    )
                    .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
                TaskListRecordBytes { range_id: 0, data }
            }
        };
        if previous_range > 0 && previous_range != current.range_id {
            return Err(stale(current.range_id));
        }
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        discard.store(true, Ordering::Release);
        Self::lock_and_check_bytes(&mut transaction, &discard, shard_id, key, current.range_id)?;
        current.range_id = current.range_id.wrapping_add(1);
        current.data.last_updated_nanos = now_nanos;
        let (bytes, encoding) = self.encode_task_list_bytes(&current.data)?;
        transaction
            .exec_drop(
                UPDATE_TASK_LIST,
                (
                    current.range_id,
                    bytes,
                    encoding,
                    shard_id,
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        if transaction.affected_rows() != 1 {
            return Err(leg(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!(
                    "lease update affected {} rows instead of 1",
                    transaction.affected_rows()
                ),
            }));
        }
        transaction
            .commit()
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        discard.store(false, Ordering::Release);
        current.data.kind = kind;
        Ok(current)
    }

    fn update_task_list(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        mut data: TaskListDataBytes,
        now_nanos: i64,
    ) -> BytesResult<()> {
        data.last_updated_nanos = now_nanos;
        data.expiry_nanos = if matches!(data.kind, 1 | 2) {
            now_nanos.wrapping_add(86_400_000_000_000)
        } else {
            0
        };
        let shard_id = self.database_shard_bytes(key);
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        discard.store(true, Ordering::Release);
        Self::lock_and_check_bytes(&mut transaction, &discard, shard_id, key, range)?;
        let (bytes, encoding) = self.encode_task_list_bytes(&data)?;
        transaction
            .exec_drop(
                UPDATE_TASK_LIST,
                (
                    range,
                    bytes,
                    encoding,
                    shard_id,
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        if transaction.affected_rows() != 1 {
            return Err(leg(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!(
                    "task-list update affected {} rows instead of 1",
                    transaction.affected_rows()
                ),
            }));
        }
        transaction
            .commit()
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        discard.store(false, Ordering::Release);
        Ok(())
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTaskBytes)],
    ) -> BytesResult<()> {
        let shard_id = self.database_shard_bytes(key);
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        discard.store(true, Ordering::Release);
        let encoding = self.write_encoding_bytes()?;
        for (domain, task) in tasks {
            // Malformed RunID bytes surface with the raw bytes retained.
            let mut data = task.data.for_sql()?;
            data.created_nanos = now_nanos;
            let bytes = encoding
                .encode(
                    &blob::encode_task_bytes(&data)
                        .map_err(|m| leg(StorageError::BackendConfiguration(m)))?,
                )
                .map_err(leg)?;
            transaction
                .exec_drop(
                    INSERT_TASK,
                    (
                        domain.as_bytes().as_slice(),
                        key.name.as_bytes(),
                        key.task_type,
                        task.task_id,
                        bytes,
                        encoding.as_str(),
                    ),
                )
                .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        }
        Self::lock_and_check_bytes(&mut transaction, &discard, shard_id, key, range)?;
        transaction
            .commit()
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        discard.store(false, Ordering::Release);
        Ok(())
    }

    fn get_tasks(
        &self,
        key: &TaskListKeyBytes,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> BytesResult<Vec<AllocatedTaskBytes>> {
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        let rows: Vec<(i64, Vec<u8>, String)> = connection
            .exec(
                SELECT_TASKS,
                (
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                    read_level,
                    max_read_level,
                    batch_size,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        rows.into_iter()
            .map(|(task_id, bytes, encoding)| {
                let encoded = BlobEncoding::parse(&encoding)
                    .map_err(leg)?
                    .decode(&bytes)
                    .map_err(leg)?;
                Ok(AllocatedTaskBytes {
                    task_id,
                    data: blob::decode_task_bytes(&encoded).map_err(leg)?,
                })
            })
            .collect()
    }

    fn task_list_size(&self, key: &TaskListKeyBytes, ack_level: i64) -> BytesResult<i64> {
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        let size: Option<i64> = connection
            .exec_first(
                COUNT_TASKS,
                (
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                    ack_level,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        size.ok_or_else(|| {
            leg(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: "COUNT(*) returned no row".into(),
            })
        })
    }

    fn complete_task(&self, key: &TaskListKeyBytes, task_id: i64) -> BytesResult<()> {
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        connection
            .exec_drop(
                DELETE_TASK,
                (
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                    task_id,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKeyBytes,
        inclusive_task_id: i64,
        limit: i64,
    ) -> BytesResult<usize> {
        if limit == 0 {
            return Err(leg(StorageError::MissingLimit));
        }
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        connection
            .exec_drop(
                RANGE_DELETE_TASKS,
                (
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                    inclusive_task_id,
                    limit,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        usize::try_from(connection.affected_rows()).map_err(|error| {
            leg(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!("MySQL affected-row count did not fit usize: {error}"),
            })
        })
    }

    fn delete_task_list(&self, key: &TaskListKeyBytes, range: i64) -> BytesResult<()> {
        let shard_id = self.database_shard_bytes(key);
        let mut connection = self.pool_bytes(key).checkout().map_err(leg)?;
        let discard = connection.discard.clone();
        connection
            .exec_drop(
                DELETE_TASK_LIST,
                (
                    shard_id,
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type,
                    range,
                ),
            )
            .map_err(|error| leg(normalize_mysql_error_on_lease(error, &discard)))?;
        let affected = connection.affected_rows();
        if affected != 1 {
            return Err(leg(StorageError::DeleteCount(affected as usize)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_blob::{TaskData, TaskDataBytes};
    use cadence_common::go_string::GoString;
    use std::collections::BTreeMap;

    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::nil(),
            name: GoString::from(name),
            task_type: 0,
        }
    }

    #[test]
    fn bound_name_parameter_keeps_exact_bytes() {
        for raw in [&b"\xff"[..], b"\xfe", "\u{fffd}".as_bytes()] {
            let k = key(raw);
            let value: mysql::Value = k.name.as_bytes().into();
            assert_eq!(value, mysql::Value::Bytes(raw.to_vec()));
        }
        // Distinct raw values never share a parameter.
        assert_ne!(key(b"\xff").name.as_bytes(), key(b"\xfe").name.as_bytes());
        assert_ne!(key(b"\xff").name.as_bytes(), "\u{fffd}".as_bytes());
    }

    #[test]
    fn shard_hash_uses_raw_bytes_and_matches_legacy_for_utf8() {
        let d = Uuid::nil();
        let legacy = crate::matching_task_store::go_db_shard_id(&d, "queue", 16);
        assert_eq!(
            crate::matching_task_store::go_db_shard_id_bytes(&d, b"queue", 16),
            legacy
        );
        // Replacement rune hashes its three UTF-8 bytes, not the single byte ff.
        let mut input = d.to_string().into_bytes();
        input.extend_from_slice(b"_\xff");
        assert_eq!(
            crate::matching_task_store::go_db_shard_id_bytes(&d, b"\xff", 1 << 20),
            (farmhash::hash32(&input) % (1 << 20)) as i32
        );
    }

    #[test]
    fn raw_blob_bytes_roundtrip_and_match_legacy_encoding_for_utf8() {
        let mut cfg = BTreeMap::new();
        cfg.insert(GoString::from(&b"\xff"[..]), GoString::from(&b"\xfe"[..]));
        let raw = TaskDataBytes {
            workflow_id: GoString::from(&b"wf\xff"[..]),
            run_id: None,
            raw_run_id: None,
            schedule_id: 3,
            expiry_nanos: 4,
            created_nanos: 5,
            partition_config: Some(cfg),
        };
        let decoded = blob::decode_task_bytes(&blob::encode_task_bytes(&raw).unwrap()).unwrap();
        assert_eq!(decoded, raw);
        assert_eq!(decoded.workflow_id.as_bytes(), b"wf\xff");

        let legacy = TaskData {
            workflow_id: "wf".into(),
            run_id: None,
            raw_run_id: None,
            schedule_id: 1,
            expiry_nanos: 2,
            created_nanos: 3,
            partition_config: None,
        };
        assert_eq!(
            blob::encode_task_bytes(&TaskDataBytes::from(legacy.clone())).unwrap(),
            blob::encode_task(&legacy).unwrap()
        );
    }

    #[test]
    fn malformed_run_id_error_retains_raw_bytes_before_any_write() {
        let task = AllocatedTaskBytes {
            task_id: 1,
            data: TaskDataBytes {
                workflow_id: GoString::default(),
                run_id: None,
                raw_run_id: Some(GoString::from(&b"\xff\xfe"[..])),
                schedule_id: 0,
                expiry_nanos: 0,
                created_nanos: 0,
                partition_config: None,
            },
        };
        let err: BytesError = task.data.for_sql().unwrap_err().into();
        match err {
            BytesError::MalformedSqlRunId(e) => assert_eq!(e.raw.as_bytes(), b"\xff\xfe"),
            other => panic!("unexpected {other:?}"),
        }
        assert!(!BytesError::from(task.data.for_sql().unwrap_err()).is_transient());
    }

    #[test]
    fn lease_fence_message_is_condition_failed_not_transient() {
        let err = leg(StorageError::ConditionFailed("x".into()));
        assert!(!err.is_transient());
        let err = BytesError::lease_renewal(&key(b"\xffq"), 7);
        assert!(!err.is_transient());
        let BytesError::ConditionFailed { message } = err else {
            panic!("wrong error category")
        };
        assert_eq!(message.as_bytes(), b"leaseTaskList:renew failed:taskList:\xffq, taskListType:0, haveRangeID:0, gotRangeID:7");
    }
}
