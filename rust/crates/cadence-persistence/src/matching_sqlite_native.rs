//! `MatchingTaskStoreBytes` for the Go SQLite task schema.
//!
//! Go's SQLite driver binds Go strings with `sqlite3_bind_text` and an explicit
//! length: arbitrary bytes are stored as TEXT without UTF-8 validation or
//! replacement. rusqlite can only bind validated `&str` as TEXT, so raw names
//! are bound as BLOB parameters and converted with `CAST(? AS TEXT)`, which
//! reinterprets the exact bytes as TEXT in SQLite. Key lookups therefore hit
//! the same rows as rows written by the legacy (valid UTF-8) API and by Go.
//! Lease-renewal errors also retain the task-list name bytes verbatim.
use super::{blob, BlobEncoding, SqliteTaskStore, StorageError};
use crate::matching_blob::{SqlRunIdBytesError, TaskListDataBytes};
use crate::matching_native_task_store::{
    AllocatedTaskBytes, MatchingTaskStoreBytes, MatchingTaskStoreBytesError as Error, Result,
    TaskListKeyBytes, TaskListRecordBytes,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use uuid::Uuid;

fn legacy<T>(r: std::result::Result<T, impl Into<StorageError>>) -> Result<T> {
    r.map_err(|e| Error::Legacy(e.into()))
}

const LIST_KEY: &str = "shard_id=0 AND domain_id=? AND name=CAST(? AS TEXT) AND task_type=?";
const TASK_KEY: &str = "domain_id=? AND task_list_name=CAST(? AS TEXT) AND task_type=?";

impl SqliteTaskStore {
    fn n_read_list(
        connection: &Connection,
        key: &TaskListKeyBytes,
    ) -> Result<Option<TaskListRecordBytes>> {
        let row: Option<(i64, Vec<u8>, String)> = legacy(
            connection
                .query_row(
                    &format!(
                        "SELECT range_id, data, data_encoding FROM task_lists WHERE {LIST_KEY}"
                    ),
                    params![
                        key.domain_id.as_bytes().as_slice(),
                        key.name.as_bytes(),
                        key.task_type
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional(),
        )?;
        row.map(|(range_id, data, encoding)| {
            let raw = legacy(BlobEncoding::parse(&encoding).and_then(|e| e.decode(&data)))?;
            Ok(TaskListRecordBytes {
                range_id,
                data: legacy(blob::decode_task_list_bytes(&raw))?,
            })
        })
        .transpose()
    }
    fn n_fence(tx: &Transaction<'_>, key: &TaskListKeyBytes, expected: i64) -> Result<()> {
        let range: Option<i64> = legacy(
            tx.query_row(
                &format!("SELECT range_id FROM task_lists WHERE {LIST_KEY}"),
                params![
                    key.domain_id.as_bytes().as_slice(),
                    key.name.as_bytes(),
                    key.task_type
                ],
                |row| row.get(0),
            )
            .optional(),
        )?;
        match range {
            Some(r) if r == expected => Ok(()),
            Some(r) => Err(Error::Legacy(StorageError::ConditionFailed(format!(
                "Task list range ID was {r} when it was should have been {expected}"
            )))),
            None => Err(Error::Legacy(StorageError::ConditionFailed(
                "Task list does not exist.".into(),
            ))),
        }
    }
    fn n_write_list(
        &self,
        tx: &Transaction<'_>,
        key: &TaskListKeyBytes,
        range: i64,
        data: &TaskListDataBytes,
    ) -> Result<()> {
        let bytes = legacy(self.encoding.encode(&blob::encode_task_list_bytes(data)))?;
        legacy(tx.execute(
            &format!("UPDATE task_lists SET range_id=?, data=?, data_encoding=? WHERE {LIST_KEY}"),
            params![
                range,
                bytes,
                self.encoding.as_str(),
                key.domain_id.as_bytes().as_slice(),
                key.name.as_bytes(),
                key.task_type
            ],
        ))?;
        Ok(())
    }
}

impl MatchingTaskStoreBytes for SqliteTaskStore {
    fn fetch_latest_config(
        &self,
        row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        crate::config_store::sqlite_fetch_latest(
            &self.connection.lock().expect("matching SQL lock"),
            row_type,
        )
    }

    fn get_task_list(&self, key: &TaskListKeyBytes) -> Result<TaskListRecordBytes> {
        Self::n_read_list(&self.connection.lock().expect("matching SQL lock"), key)?
            .ok_or(Error::Legacy(StorageError::NotFound))
    }

    fn lease(
        &self,
        key: &TaskListKeyBytes,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecordBytes> {
        let mut connection = self.connection.lock().expect("matching SQL lock");
        let mut current = match Self::n_read_list(&connection, key)? {
            Some(record) => record,
            None => {
                let data = TaskListDataBytes {
                    kind,
                    last_updated_nanos: now_nanos,
                    ..Default::default()
                };
                let bytes = legacy(self.encoding.encode(&blob::encode_task_list_bytes(&data)))?;
                legacy(connection.execute(
                    "INSERT INTO task_lists (shard_id,domain_id,name,task_type,range_id,data,data_encoding) VALUES (0,?,CAST(? AS TEXT),?,0,?,?)",
                    params![key.domain_id.as_bytes().as_slice(), key.name.as_bytes(), key.task_type, bytes, self.encoding.as_str()],
                ))?;
                TaskListRecordBytes { range_id: 0, data }
            }
        };
        if previous_range > 0 && previous_range != current.range_id {
            // Go's error prints the still-zero local range variable here.
            return Err(Error::lease_renewal(key, current.range_id));
        }
        let tx = legacy(connection.transaction_with_behavior(TransactionBehavior::Immediate))?;
        Self::n_fence(&tx, key, current.range_id)?;
        current.range_id = current.range_id.wrapping_add(1);
        current.data.last_updated_nanos = now_nanos;
        self.n_write_list(&tx, key, current.range_id, &current.data)?;
        legacy(tx.commit())?;
        current.data.kind = kind;
        Ok(current)
    }

    fn update_task_list(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        mut data: TaskListDataBytes,
        now_nanos: i64,
    ) -> Result<()> {
        data.last_updated_nanos = now_nanos;
        data.expiry_nanos = if matches!(data.kind, 1 | 2) {
            now_nanos.wrapping_add(86_400_000_000_000)
        } else {
            0
        };
        let mut connection = self.connection.lock().expect("matching SQL lock");
        let tx = legacy(connection.transaction_with_behavior(TransactionBehavior::Immediate))?;
        Self::n_fence(&tx, key, range)?;
        self.n_write_list(&tx, key, range, &data)?;
        legacy(tx.commit())
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        _now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTaskBytes)],
    ) -> Result<()> {
        let mut connection = self.connection.lock().expect("matching SQL lock");
        let tx = legacy(connection.transaction_with_behavior(TransactionBehavior::Immediate))?;
        for (domain, task) in tasks {
            let data = task
                .data
                .for_sql()
                .map_err(|e: SqlRunIdBytesError| Error::MalformedSqlRunId(e))?;
            let bytes = legacy(
                blob::encode_task_bytes(&data)
                    .map_err(StorageError::BackendConfiguration)
                    .and_then(|b| self.encoding.encode(&b).map_err(StorageError::from)),
            )?;
            legacy(tx.execute(
                "INSERT INTO tasks (domain_id,task_list_name,task_type,task_id,data,data_encoding) VALUES (?,CAST(? AS TEXT),?,?,?,?)",
                params![domain.as_bytes().as_slice(), key.name.as_bytes(), key.task_type, task.task_id, bytes, self.encoding.as_str()],
            ))?;
        }
        Self::n_fence(&tx, key, range)?;
        legacy(tx.commit())
    }

    fn get_tasks(
        &self,
        key: &TaskListKeyBytes,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTaskBytes>> {
        let connection = self.connection.lock().expect("matching SQL lock");
        let mut query = legacy(connection.prepare(&format!(
            "SELECT task_id,data,data_encoding FROM tasks WHERE {TASK_KEY} AND task_id>? AND task_id<=? ORDER BY task_id LIMIT ?"
        )))?;
        let rows = legacy(query.query_map(
            params![
                key.domain_id.as_bytes().as_slice(),
                key.name.as_bytes(),
                key.task_type,
                read_level,
                max_read_level,
                batch_size
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        ))?;
        rows.map(|row| {
            let (task_id, bytes, encoding) = legacy(row)?;
            let raw = legacy(BlobEncoding::parse(&encoding).and_then(|e| e.decode(&bytes)))?;
            Ok(AllocatedTaskBytes {
                task_id,
                data: legacy(blob::decode_task_bytes(&raw))?,
            })
        })
        .collect()
    }

    fn task_list_size(&self, key: &TaskListKeyBytes, ack_level: i64) -> Result<i64> {
        legacy(
            self.connection
                .lock()
                .expect("matching SQL lock")
                .query_row(
                    &format!("SELECT COUNT(*) FROM tasks WHERE {TASK_KEY} AND task_id>?"),
                    params![
                        key.domain_id.as_bytes().as_slice(),
                        key.name.as_bytes(),
                        key.task_type,
                        ack_level
                    ],
                    |row| row.get(0),
                ),
        )
    }

    fn complete_task(&self, key: &TaskListKeyBytes, task_id: i64) -> Result<()> {
        legacy(self.connection.lock().expect("matching SQL lock").execute(
            &format!("DELETE FROM tasks WHERE {TASK_KEY} AND task_id=?"),
            params![
                key.domain_id.as_bytes().as_slice(),
                key.name.as_bytes(),
                key.task_type,
                task_id
            ],
        ))?;
        Ok(())
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKeyBytes,
        inclusive_task_id: i64,
        limit: i64,
    ) -> Result<usize> {
        if limit == 0 {
            return Err(Error::Legacy(StorageError::MissingLimit));
        }
        legacy(self.connection.lock().expect("matching SQL lock").execute(
            &format!("DELETE FROM tasks WHERE rowid IN (SELECT rowid FROM tasks WHERE {TASK_KEY} AND task_id<=? ORDER BY domain_id,task_list_name,task_type,task_id LIMIT ?)"),
            params![key.domain_id.as_bytes().as_slice(), key.name.as_bytes(), key.task_type, inclusive_task_id, limit],
        ))
    }

    fn delete_task_list(&self, key: &TaskListKeyBytes, range: i64) -> Result<()> {
        let count = legacy(self.connection.lock().expect("matching SQL lock").execute(
            &format!("DELETE FROM task_lists WHERE {LIST_KEY} AND range_id=?"),
            params![
                key.domain_id.as_bytes().as_slice(),
                key.name.as_bytes(),
                key.task_type,
                range
            ],
        ))?;
        if count != 1 {
            return Err(Error::Legacy(StorageError::DeleteCount(count)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_blob::{PartitionBytes, PartitionConfigBytes, TaskDataBytes};
    use crate::matching_task_store::{AllocatedTask, MatchingTaskStore, TaskListKey};
    use cadence_common::go_string::GoString;
    use std::collections::BTreeMap;

    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::from_u128(1),
            name: GoString::from(name),
            task_type: 0,
        }
    }
    fn task(id: i64, wf: &[u8]) -> AllocatedTaskBytes {
        AllocatedTaskBytes {
            task_id: id,
            data: TaskDataBytes {
                workflow_id: GoString::from(wf),
                run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
                raw_run_id: None,
                schedule_id: 2,
                expiry_nanos: 0,
                created_nanos: 0,
                partition_config: None,
            },
        }
    }
    fn store() -> (tempfile::TempDir, SqliteTaskStore) {
        let dir = tempfile::tempdir().unwrap();
        let s = SqliteTaskStore::create(dir.path().join("t.db"), BlobEncoding::ThriftRW).unwrap();
        (dir, s)
    }

    #[test]
    fn invalid_bytes_and_replacement_rune_are_separate_queues() {
        let (_d, s) = store();
        let names: [&[u8]; 3] = [b"\xff", b"\xfe", "\u{fffd}".as_bytes()];
        for (i, n) in names.iter().enumerate() {
            let k = key(n);
            let l = MatchingTaskStoreBytes::lease(&s, &k, 0, 0, 1).unwrap();
            assert_eq!(l.range_id, 1);
            MatchingTaskStoreBytes::create_tasks_with_domains(
                &s,
                &k,
                1,
                1,
                &[(k.domain_id, &task(i as i64 + 1, n))],
            )
            .unwrap();
        }
        for (i, n) in names.iter().enumerate() {
            let got = MatchingTaskStoreBytes::get_tasks(&s, &key(n), 0, 100, 10).unwrap();
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].task_id, i as i64 + 1);
            assert_eq!(got[0].data.workflow_id.as_bytes(), *n);
        }
        let c = s.connection.lock().unwrap();
        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM task_lists WHERE typeof(name)='text'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 3);
        let raw: Vec<u8> = c
            .query_row(
                "SELECT CAST(name AS BLOB) FROM task_lists WHERE hex(CAST(name AS BLOB))='FF'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(raw, b"\xff");
    }

    #[test]
    fn legacy_and_native_cross_read_valid_utf8() {
        let (_d, s) = store();
        let lk = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "queue".into(),
            task_type: 0,
        };
        let lease = MatchingTaskStore::lease(&s, &lk, 0, 0, 1).unwrap();
        let t = AllocatedTask {
            task_id: 5,
            data: crate::matching_blob::TaskData {
                workflow_id: "wf".into(),
                run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
                ..Default::default()
            },
        };
        MatchingTaskStore::create_tasks_with_domains(
            &s,
            &lk,
            lease.range_id,
            1,
            &[(lk.domain_id, &t)],
        )
        .unwrap();
        let nk = key(b"queue");
        let got = MatchingTaskStoreBytes::get_tasks(&s, &nk, 0, 10, 10).unwrap();
        assert_eq!(got[0].data.workflow_id.as_bytes(), b"wf");
        MatchingTaskStoreBytes::create_tasks_with_domains(
            &s,
            &nk,
            1,
            1,
            &[(nk.domain_id, &task(6, b"native"))],
        )
        .unwrap();
        let back = MatchingTaskStore::get_tasks(&s, &lk, 0, 10, 10).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(
            MatchingTaskStoreBytes::task_list_size(&s, &nk, 0).unwrap(),
            2
        );
    }

    #[test]
    fn lease_fencing_rolls_back_batch_and_preserves_raw_partition_bytes() {
        let (_d, s) = store();
        let k = key(b"\xff\xfe");
        MatchingTaskStoreBytes::lease(&s, &k, 0, 0, 1).unwrap();
        let err = MatchingTaskStoreBytes::create_tasks_with_domains(
            &s,
            &k,
            9,
            1,
            &[(k.domain_id, &task(1, b"a"))],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Legacy(StorageError::ConditionFailed(_))
        ));
        assert_eq!(
            MatchingTaskStoreBytes::task_list_size(&s, &k, 0).unwrap(),
            0
        );
        let mut m = BTreeMap::new();
        m.insert(
            0,
            PartitionBytes {
                isolation_groups: Some(vec![
                    GoString::from(&b"\xff"[..]),
                    GoString::from("\u{fffd}"),
                ]),
            },
        );
        let data = TaskListDataBytes {
            kind: 0,
            adaptive_partition_config: Some(PartitionConfigBytes {
                version: 1,
                num_read_partitions: 1,
                num_write_partitions: 1,
                read_partitions: Some(m.clone()),
                write_partitions: Some(m.clone()),
            }),
            ..Default::default()
        };
        assert!(MatchingTaskStoreBytes::update_task_list(&s, &k, 7, data.clone(), 5).is_err());
        MatchingTaskStoreBytes::update_task_list(&s, &k, 1, data, 5).unwrap();
        let rec = MatchingTaskStoreBytes::get_task_list(&s, &k).unwrap();
        assert_eq!(
            rec.data
                .adaptive_partition_config
                .unwrap()
                .read_partitions
                .unwrap(),
            m
        );
        MatchingTaskStoreBytes::delete_task_list(&s, &k, 1).unwrap();
        assert!(MatchingTaskStoreBytes::get_task_list(&s, &k).is_err());
    }

    #[test]
    fn malformed_run_id_bytes_are_retained_in_error() {
        let (_d, s) = store();
        let k = key(b"q");
        MatchingTaskStoreBytes::lease(&s, &k, 0, 0, 1).unwrap();
        let mut t = task(1, b"w");
        t.data = t.data.with_request_run_id(GoString::from(&b"\xff\xfe"[..]));
        match MatchingTaskStoreBytes::create_tasks_with_domains(&s, &k, 1, 1, &[(k.domain_id, &t)])
            .unwrap_err()
        {
            Error::MalformedSqlRunId(e) => assert_eq!(e.raw.as_bytes(), b"\xff\xfe"),
            e => panic!("{e:?}"),
        }
    }
}
