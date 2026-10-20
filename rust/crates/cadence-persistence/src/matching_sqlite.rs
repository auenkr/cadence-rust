//! Matching-owned task persistence on Go's SQLite task_lists/tasks schema.
//! This store is independent of the workflow execution engine. The standalone
//! Matching RPC delivery loop is a separate consumer of this persistence API.
#[cfg(test)]
use crate::matching_blob::TaskData;
use crate::matching_blob::{self as blob, BlobEncoding, TaskListData};
use crate::SemaphorePersistence;
pub use crate::matching_task_store::{
    AllocatedTask, MatchingTaskStoreError as StorageError, Result, TaskListKey, TaskListRecord,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::{path::Path, sync::Mutex, time::Duration};
use uuid::Uuid;

pub struct SqliteTaskStore {
    connection: Mutex<Connection>,
    encoding: BlobEncoding,
}

impl crate::matching_task_store::MatchingTaskStore for SqliteTaskStore {
    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord> {
        SqliteTaskStore::get_task_list(self, key)
    }

    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecord> {
        SqliteTaskStore::lease(self, key, kind, previous_range, now_nanos)
    }

    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        data: TaskListData,
        now_nanos: i64,
    ) -> Result<()> {
        SqliteTaskStore::update_task_list(self, key, range, data, now_nanos)
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> Result<()> {
        SqliteTaskStore::create_tasks_with_domains(
            self,
            key,
            range,
            tasks.iter().map(|(domain, task)| (*domain, *task)),
        )
    }

    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>> {
        SqliteTaskStore::get_tasks(self, key, read_level, max_read_level, batch_size)
    }

    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64> {
        SqliteTaskStore::task_list_size(self, key, ack_level)
    }

    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()> {
        SqliteTaskStore::complete_task(self, key, task_id)
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> Result<usize> {
        SqliteTaskStore::complete_tasks_less_than(self, key, inclusive_task_id, limit)
    }

    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()> {
        SqliteTaskStore::delete_task_list(self, key, range)
    }
}

#[async_trait::async_trait]
impl crate::SemaphorePersistence for SqliteTaskStore {
    async fn create_semaphore(
        &self,
        domain: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> std::result::Result<crate::SemaphoreMetadata, crate::SemaphoreError> {
        crate::SqlUnsupportedSemaphorePersistence.create_semaphore(
            domain,
            name,
            size,
            bucket_size,
        ).await
    }

    async fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
    ) -> std::result::Result<Vec<crate::SemaphoreRow>, crate::AcquireError> {
        crate::SqlUnsupportedSemaphorePersistence.scan_semaphore_bucket(domain_id, name, bucket).await
    }

    async fn get_semaphore_ownership_by_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> std::result::Result<String, crate::AcquireError> {
        crate::SqlUnsupportedSemaphorePersistence.get_semaphore_ownership_by_token(
            domain_id, name, bucket, token_id,
        ).await
    }

    async fn grant_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> std::result::Result<crate::GrantOutcome, crate::AcquireError> {
        crate::SqlUnsupportedSemaphorePersistence.grant_semaphore_token(
            domain_id, name, bucket, token_id, owner_id,
        ).await
    }
}

impl SqliteTaskStore {
    fn validate_schema(connection: &Connection) -> Result<()> {
        // These SELECTs verify every column used by Matching queue operations.
        // Task expiry and task-list last-updated timestamps live in the Thrift
        // `data` blobs in Go's SQLite schema; there are no SQL datetime columns
        // in either table, so `_timefmt` does not encode those fields.
        connection.prepare("SELECT shard_id, domain_id, name, task_type, range_id, data, data_encoding FROM task_lists LIMIT 0")?;
        connection.prepare("SELECT domain_id, task_list_name, task_type, task_id, data, data_encoding FROM tasks LIMIT 0")?;

        for (table, pragma, expected) in [
            (
                "task_lists",
                "PRAGMA table_info(task_lists)",
                &["shard_id", "domain_id", "name", "task_type"][..],
            ),
            (
                "tasks",
                "PRAGMA table_info(tasks)",
                &["domain_id", "task_list_name", "task_type", "task_id"][..],
            ),
        ] {
            let mut statement = connection.prepare(pragma)?;
            let columns = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut keys = columns
                .into_iter()
                .filter(|(_, primary_key_order)| *primary_key_order > 0)
                .collect::<Vec<_>>();
            keys.sort_by_key(|(_, order)| *order);
            let actual = keys.into_iter().map(|(name, _)| name).collect::<Vec<_>>();
            let matches = actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(actual, expected)| actual == expected);
            if !matches {
                return Err(StorageError::IncompatibleSchema(format!(
                    "{table} primary key is {actual:?}, expected {expected:?}"
                )));
            }
        }
        Ok(())
    }

    /// Open a database whose Cadence schema was already installed. No schema
    /// changes or execution-store access occur here.
    pub fn open(path: impl AsRef<Path>, encoding: BlobEncoding) -> Result<Self> {
        let connection =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        connection.busy_timeout(Duration::from_secs(10))?;
        Self::validate_schema(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            encoding,
        })
    }

    /// Open the configured Go SQLite task store. The database name is passed
    /// through Go's `file:<databaseName>?mode=<mode>` URI boundary; callers
    /// validate connect attributes and supply the effective Go defaults.
    /// Unlike `create`, this never installs a partial Cadence schema.
    pub fn open_with_go_sqlite_options(
        database_name: &str,
        encoding: BlobEncoding,
        mode: &str,
        busy_timeout: Duration,
        journal_mode: &str,
    ) -> Result<Self> {
        if busy_timeout.as_millis() > i32::MAX as u128 {
            return Err(StorageError::UnsupportedConfiguration(
                "busy timeout exceeds SQLite's supported millisecond range".into(),
            ));
        }
        let (uri_mode, flags) = match mode {
            "rw" => ("rw", rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE),
            "rwc" => (
                "rwc",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            ),
            "ro" => {
                return Err(StorageError::UnsupportedConfiguration(
                    "mode=ro cannot be used for Matching task persistence".into(),
                ));
            }
            _ => {
                return Err(StorageError::UnsupportedConfiguration(format!(
                    "unsupported SQLite URI mode {mode:?}"
                )));
            }
        };
        let journal_mode = journal_mode.to_ascii_lowercase();
        if !matches!(
            journal_mode.as_str(),
            "delete" | "truncate" | "persist" | "memory" | "wal" | "off"
        ) {
            return Err(StorageError::UnsupportedConfiguration(format!(
                "unsupported SQLite journal_mode {journal_mode:?}"
            )));
        }
        let uri = format!("file:{database_name}?mode={uri_mode}");
        let connection =
            Connection::open_with_flags(uri, flags | rusqlite::OpenFlags::SQLITE_OPEN_URI)?;
        connection.busy_timeout(busy_timeout)?;
        // journal_mode is selected from SQLite's closed set above, so it is
        // safe to include as a PRAGMA token. Go applies this on every pool
        // connection when it opens the configured SQLite datastore.
        let actual_mode: String = connection.query_row(
            &format!("PRAGMA journal_mode = {journal_mode}"),
            [],
            |row| row.get(0),
        )?;
        if !actual_mode.eq_ignore_ascii_case(&journal_mode) {
            return Err(StorageError::UnsupportedConfiguration(format!(
                "SQLite requested journal_mode {journal_mode:?}, opened as {actual_mode:?}"
            )));
        }
        Self::validate_schema(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            encoding,
        })
    }
    /// Bootstrap only Matching's two tables in a NEW database. Existing Cadence
    /// databases must use open(), leaving schema management to Cadence tooling.
    pub fn create(path: impl AsRef<Path>, encoding: BlobEncoding) -> Result<Self> {
        let connection = Connection::open_with_flags(
            path.as_ref(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        connection.busy_timeout(Duration::from_secs(10))?;
        connection.execute_batch("BEGIN IMMEDIATE")?;
        let installed = connection.execute_batch(include_str!("../../../sql/matching-sqlite.sql"));
        match installed {
            Ok(()) => connection.execute_batch("COMMIT")?,
            Err(error) => {
                connection.execute_batch("ROLLBACK")?;
                return Err(error.into());
            }
        }
        drop(connection);
        Self::open(path, encoding)
    }
    fn read_list(connection: &Connection, key: &TaskListKey) -> Result<Option<TaskListRecord>> {
        let row: Option<(i64, Vec<u8>, String)> = connection.query_row(
            "SELECT range_id, data, data_encoding FROM task_lists WHERE shard_id=0 AND domain_id=? AND name=? AND task_type=?",
            params![key.domain_id.as_bytes().as_slice(), key.name, key.task_type],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
        row.map(|(range_id, data, encoding)| {
            Ok(TaskListRecord {
                range_id,
                data: blob::decode_task_list(&BlobEncoding::parse(&encoding)?.decode(&data)?)?,
            })
        })
        .transpose()
    }
    pub fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord> {
        Self::read_list(&self.connection.lock().expect("matching SQL lock"), key)?
            .ok_or(StorageError::NotFound)
    }
    fn fence(tx: &Transaction<'_>, key: &TaskListKey, expected: i64) -> Result<()> {
        let range: Option<i64> = tx.query_row(
            "SELECT range_id FROM task_lists WHERE shard_id=0 AND domain_id=? AND name=? AND task_type=?",
            params![key.domain_id.as_bytes().as_slice(),key.name,key.task_type],|row|row.get(0)).optional()?;
        match range {
            Some(range) if range == expected => Ok(()),
            Some(range) => Err(StorageError::ConditionFailed(format!(
                "Task list range ID was {range} when it was should have been {expected}"
            ))),
            None => Err(StorageError::ConditionFailed(
                "Task list does not exist.".into(),
            )),
        }
    }
    fn write_list(
        &self,
        tx: &Transaction<'_>,
        key: &TaskListKey,
        range: i64,
        data: &TaskListData,
    ) -> Result<()> {
        let bytes = self.encoding.encode(&blob::encode_task_list(data))?;
        tx.execute("UPDATE task_lists SET range_id=?, data=?, data_encoding=? WHERE shard_id=0 AND domain_id=? AND name=? AND task_type=?",
            params![range,bytes,self.encoding.as_str(),key.domain_id.as_bytes().as_slice(),key.name,key.task_type])?;
        Ok(())
    }
    /// Acquire/renew a lease. A zero previous range steals the current lease;
    /// positive ranges must match. All subsequent writes fence on the range.
    pub fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecord> {
        let mut connection = self.connection.lock().expect("matching SQL lock");
        let mut current = match Self::read_list(&connection, key)? {
            Some(record) => record,
            None => {
                let data = TaskListData {
                    kind,
                    last_updated_nanos: now_nanos,
                    ..Default::default()
                };
                let bytes = self.encoding.encode(&blob::encode_task_list(&data))?;
                connection.execute("INSERT INTO task_lists (shard_id,domain_id,name,task_type,range_id,data,data_encoding) VALUES (0,?,?,?,0,?,?)",
                    params![key.domain_id.as_bytes().as_slice(),key.name,key.task_type,bytes,self.encoding.as_str()])?;
                TaskListRecord { range_id: 0, data }
            }
        };
        if previous_range > 0 && previous_range != current.range_id {
            // Go's error prints the still-zero local range variable here.
            return Err(StorageError::ConditionFailed(format!("leaseTaskList:renew failed:taskList:{}, taskListType:{}, haveRangeID:0, gotRangeID:{}",key.name,key.task_type,current.range_id)));
        }
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::fence(&tx, key, current.range_id)?;
        current.range_id = current.range_id.wrapping_add(1);
        current.data.last_updated_nanos = now_nanos;
        self.write_list(&tx, key, current.range_id, &current.data)?;
        tx.commit()?;
        // Go returns the requested kind while leaving existing stored kind alone.
        current.data.kind = kind;
        Ok(current)
    }
    pub fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        mut data: TaskListData,
        now_nanos: i64,
    ) -> Result<()> {
        data.last_updated_nanos = now_nanos;
        data.expiry_nanos = if matches!(data.kind, 1 | 2) {
            now_nanos.wrapping_add(86_400_000_000_000)
        } else {
            0
        };
        let mut connection = self.connection.lock().expect("matching SQL lock");
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::fence(&tx, key, range)?;
        self.write_list(&tx, key, range, &data)?;
        tx.commit()?;
        Ok(())
    }
    /// Atomically append a batch. A stale lease or duplicate task rolls back
    /// every inserted row, matching Go's insert-then-lock transaction.
    pub fn create_tasks(
        &self,
        key: &TaskListKey,
        range: i64,
        tasks: &[AllocatedTask],
    ) -> Result<()> {
        self.create_tasks_with_domains(key, range, tasks.iter().map(|task| (key.domain_id, task)))
    }
    /// Go fences the target task-list lease while each task row uses its own
    /// source domain. The domain is a SQL key, not a field in the task blob.
    pub fn create_tasks_with_domains<'a>(
        &self,
        key: &TaskListKey,
        range: i64,
        tasks: impl IntoIterator<Item = (Uuid, &'a AllocatedTask)>,
    ) -> Result<()> {
        let mut connection = self.connection.lock().expect("matching SQL lock");
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (domain, task) in tasks {
            let bytes = self.encoding.encode(&blob::encode_task(&task.data))?;
            tx.execute("INSERT INTO tasks (domain_id,task_list_name,task_type,task_id,data,data_encoding) VALUES (?,?,?,?,?,?)",
                params![domain.as_bytes().as_slice(),key.name,key.task_type,task.task_id,bytes,self.encoding.as_str()])?;
        }
        Self::fence(&tx, key, range)?;
        tx.commit()?;
        Ok(())
    }
    /// Go SQL read interval: (read_level, max_read_level], ordered by task ID.
    /// Expired tasks remain visible here; the queue reader handles their expiry.
    pub fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>> {
        let connection = self.connection.lock().expect("matching SQL lock");
        let mut query = connection.prepare("SELECT task_id,data,data_encoding FROM tasks WHERE domain_id=? AND task_list_name=? AND task_type=? AND task_id>? AND task_id<=? ORDER BY task_id LIMIT ?")?;
        let rows = query.query_map(
            params![
                key.domain_id.as_bytes().as_slice(),
                key.name,
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
        )?;
        rows.map(|row| {
            let (task_id, bytes, encoding) = row?;
            Ok(AllocatedTask {
                task_id,
                data: blob::decode_task(&BlobEncoding::parse(&encoding)?.decode(&bytes)?)?,
            })
        })
        .collect()
    }
    pub fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64> {
        Ok(self.connection.lock().expect("matching SQL lock").query_row("SELECT COUNT(*) FROM tasks WHERE domain_id=? AND task_list_name=? AND task_type=? AND task_id>?",
            params![key.domain_id.as_bytes().as_slice(),key.name,key.task_type,ack_level],|row|row.get(0))?)
    }
    /// Go does not require the current lease for single/batch task deletion.
    pub fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()> {
        self.connection.lock().expect("matching SQL lock").execute("DELETE FROM tasks WHERE domain_id=? AND task_list_name=? AND task_type=? AND task_id=?",
            params![key.domain_id.as_bytes().as_slice(),key.name,key.task_type,task_id])?;
        Ok(())
    }
    pub fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> Result<usize> {
        if limit == 0 {
            return Err(StorageError::MissingLimit);
        }
        Ok(self.connection.lock().expect("matching SQL lock").execute("DELETE FROM tasks WHERE rowid IN (SELECT rowid FROM tasks WHERE domain_id=? AND task_list_name=? AND task_type=? AND task_id<=? ORDER BY domain_id,task_list_name,task_type,task_id LIMIT ?)",
            params![key.domain_id.as_bytes().as_slice(),key.name,key.task_type,inclusive_task_id,limit])?)
    }
    pub fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()> {
        let count = self.connection.lock().expect("matching SQL lock").execute("DELETE FROM task_lists WHERE shard_id=0 AND domain_id=? AND name=? AND task_type=? AND range_id=?",
            params![key.domain_id.as_bytes().as_slice(),key.name,key.task_type,range])?;
        if count != 1 {
            return Err(StorageError::DeleteCount(count));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(name: &str) -> TaskListKey {
        TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: name.into(),
            task_type: 0,
        }
    }
    fn task(id: i64) -> AllocatedTask {
        AllocatedTask {
            task_id: id,
            data: TaskData {
                workflow_id: format!("workflow-{id}"),
                schedule_id: 2,
                run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
                ..Default::default()
            },
        }
    }
    #[test]
    fn matching_sqlite_schema_matches_go() {
        let source = include_str!("../../../../go/schema/sqlite/cadence/schema.sql");
        let source = &source[source.find("CREATE TABLE tasks\n").unwrap()
            ..source.find("CREATE TABLE replication_tasks\n").unwrap()];
        assert_eq!(include_str!("../../../sql/matching-sqlite.sql"), source);
    }
    #[test]
    fn matching_sqlite_leases_fence_writes_and_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let first = SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap();
        let key = key("queue");
        let lease = first.lease(&key, 0, 0, 10).unwrap();
        assert_eq!(lease.range_id, 1);
        first
            .create_tasks(&key, lease.range_id, &[task(1)])
            .unwrap();
        let second = SqliteTaskStore::open(&path, BlobEncoding::ThriftRWSnappy).unwrap();
        let stolen = second.lease(&key, 0, 0, 20).unwrap();
        assert_eq!(stolen.range_id, 2);
        assert!(matches!(
            first.lease(&key, 0, 1, 30),
            Err(StorageError::ConditionFailed(_))
        ));
        assert!(matches!(
            first.create_tasks(&key, 1, &[task(2)]),
            Err(StorageError::ConditionFailed(_))
        ));
        assert_eq!(second.task_list_size(&key, 0).unwrap(), 1);
        let mut changed = lease.data;
        changed.ack_level = 77;
        assert!(first.update_task_list(&key, 1, changed, 40).is_err());
        assert_eq!(second.get_task_list(&key).unwrap().data.ack_level, 0);
        second.create_tasks(&key, 2, &[task(3)]).unwrap();
        drop(first);
        drop(second);
        let recovered = SqliteTaskStore::open(&path, BlobEncoding::ThriftRW).unwrap();
        assert_eq!(recovered.get_task_list(&key).unwrap().range_id, 2);
        assert_eq!(
            recovered.get_tasks(&key, 0, 10, 10).unwrap(),
            vec![task(1), task(3)]
        );
    }

    #[test]
    fn configured_go_sqlite_open_uses_shared_schema_and_writer_options() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cadence.sqlite");
        drop(SqliteTaskStore::create(&path, BlobEncoding::ThriftRWSnappy).unwrap());
        let store = SqliteTaskStore::open_with_go_sqlite_options(
            path.to_str().unwrap(),
            BlobEncoding::ThriftRWSnappy,
            "rw",
            Duration::from_millis(2400),
            "WAL",
        )
        .unwrap();
        let observer = Connection::open(&path).unwrap();
        let journal_mode: String = observer
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        drop(observer);
        drop(store);
    }

    #[test]
    fn configured_go_sqlite_open_rejects_read_only_task_store_mode() {
        assert!(matches!(
            SqliteTaskStore::open_with_go_sqlite_options(
                "missing.sqlite",
                BlobEncoding::ThriftRWSnappy,
                "ro",
                Duration::from_secs(60),
                "wal",
            ),
            Err(StorageError::UnsupportedConfiguration(_))
        ));
    }

    #[test]
    fn configured_go_sqlite_open_rejects_incompatible_queue_keys() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wrong-schema.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE task_lists (shard_id INT, domain_id BLOB, name TEXT, task_type INT, range_id INT, data BLOB, data_encoding TEXT, PRIMARY KEY(domain_id, name, task_type));
                 CREATE TABLE tasks (domain_id BLOB, task_list_name TEXT, task_type INT, task_id INT, data BLOB, data_encoding TEXT, PRIMARY KEY(domain_id, task_list_name, task_type, task_id));",
            )
            .unwrap();
        drop(connection);
        assert!(matches!(
            SqliteTaskStore::open_with_go_sqlite_options(
                path.to_str().unwrap(),
                BlobEncoding::ThriftRWSnappy,
                "rw",
                Duration::from_secs(60),
                "wal",
            ),
            Err(StorageError::IncompatibleSchema(_))
        ));
    }
    #[test]
    fn matching_sqlite_atomic_batches_bounds_expiry_and_deletes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = SqliteTaskStore::create(&path, BlobEncoding::ThriftRWSnappy).unwrap();
        let key = key("queue");
        store.lease(&key, 1, 0, 10).unwrap();
        store
            .create_tasks(&key, 1, &[task(2), task(4), task(8)])
            .unwrap();
        assert!(store.create_tasks(&key, 1, &[task(9), task(4)]).is_err());
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 3);
        assert_eq!(store.get_tasks(&key, 2, 8, 1).unwrap(), vec![task(4)]);
        assert_eq!(store.get_tasks(&key, 4, 8, 10).unwrap(), vec![task(8)]);
        let mut metadata = store.get_task_list(&key).unwrap().data;
        metadata.ack_level = 4;
        store.update_task_list(&key, 1, metadata, 50).unwrap();
        assert_eq!(
            store.get_task_list(&key).unwrap().data.expiry_nanos,
            86_400_000_000_050
        );
        assert!(store.complete_tasks_less_than(&key, 8, 0).is_err());
        assert_eq!(store.complete_tasks_less_than(&key, 4, 1).unwrap(), 1);
        store.complete_task(&key, 4).unwrap();
        store.complete_task(&key, 4).unwrap();
        assert!(store.delete_task_list(&key, 0).is_err());
        store.delete_task_list(&key, 1).unwrap();
        // Go intentionally leaves orphan tasks for the background scanner.
        assert_eq!(store.get_tasks(&key, 0, 10, 10).unwrap(), vec![task(8)]);
        assert!(matches!(
            store.get_task_list(&key),
            Err(StorageError::NotFound)
        ));
    }
    #[test]
    fn matching_sqlite_preserves_go_signed_limit_behavior() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteTaskStore::create(
            directory.path().join("signed-limit.db"),
            BlobEncoding::ThriftRW,
        )
        .unwrap();
        let key = key("signed-limit");
        store.lease(&key, 0, 0, 10).unwrap();
        store
            .create_tasks(&key, 1, &[task(1), task(2), task(3)])
            .unwrap();
        assert!(matches!(
            store.complete_tasks_less_than(&key, 3, 0),
            Err(StorageError::MissingLimit)
        ));
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 3);
        assert_eq!(store.complete_tasks_less_than(&key, 3, -1).unwrap(), 3);
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        store.create_tasks(&key, 1, &[task(4), task(5)]).unwrap();
        assert_eq!(
            store
                .complete_tasks_less_than(&key, 5, i64::from(u32::MAX) + 1)
                .unwrap(),
            2
        );
    }
    #[test]
    fn matching_sqlite_namespaces_and_task_types_are_isolated() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap();
        let original = key("queue");
        for key in [
            original.clone(),
            TaskListKey {
                task_type: 1,
                ..original.clone()
            },
            TaskListKey {
                domain_id: Uuid::from_u128(3),
                ..original.clone()
            },
        ] {
            assert_eq!(store.lease(&key, 0, 0, 10).unwrap().range_id, 1);
            store.create_tasks(&key, 1, &[task(1)]).unwrap();
        }
        assert_eq!(store.task_list_size(&original, 0).unwrap(), 1);
        assert_eq!(store.task_list_size(&original, 1).unwrap(), 0);
        assert!(SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).is_err());
        assert_eq!(store.task_list_size(&original, 0).unwrap(), 1);
    }
}

#[cfg(test)]
mod interop_tests {
    use super::*;
    use crate::matching_blob::{Partition, PartitionConfig};
    use std::collections::BTreeMap;
    #[test]
    #[ignore = "requires original-Go SQLite seed; run tests/run-matching-storage-compat.py"]
    fn matching_sqlite_go_database_interop() {
        let path = std::env::var("CADENCE_MATCHING_SQLITE_DB").expect("Go fixture DB path");
        let phase = std::env::var("CADENCE_MATCHING_SQLITE_PHASE").unwrap();
        let store = SqliteTaskStore::open(&path, BlobEncoding::ThriftRWSnappy).unwrap();
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "go-to-rust".into(),
            task_type: 0,
        };
        let now = 1_700_000_000_123_456_789;
        let native_key = TaskListKey {
            name: "native-queue-config".into(),
            ..key.clone()
        };
        if phase == "verify" {
            let native = store.get_task_list(&native_key).unwrap();
            assert_eq!(native.data.adaptive_partition_config.unwrap().version, 11);
        } else {
            let native_store = std::sync::Arc::new(
                SqliteTaskStore::open(&path, BlobEncoding::ThriftRWSnappy).unwrap(),
            );
            let native = crate::matching_queue::DurableTaskQueue::open(
                native_store,
                native_key,
                0,
                std::num::NonZeroU32::new(100).unwrap(),
                now,
            )
            .unwrap();
            native
                .update_partition_config(
                    Some(PartitionConfig {
                        version: 11,
                        num_read_partitions: 2,
                        num_write_partitions: 1,
                        read_partitions: Some(BTreeMap::from([
                            (
                                0,
                                Partition {
                                    isolation_groups: Some(vec!["a".into(), "b".into()]),
                                },
                            ),
                            (
                                5,
                                Partition {
                                    isolation_groups: None,
                                },
                            ),
                        ])),
                        write_partitions: Some(BTreeMap::from([(
                            0,
                            Partition {
                                isolation_groups: Some(vec!["a".into()]),
                            },
                        )])),
                    }),
                    now + 2000,
                )
                .unwrap();
            native.persist_ack(now + 3000).unwrap();
        }
        let mut cross_target = key.clone();
        cross_target.name = "cross-domain".into();
        let mut cross_source = cross_target.clone();
        cross_source.domain_id = Uuid::from_u128(3);
        assert!(store
            .get_tasks(&cross_target, 0, 10, 10)
            .unwrap()
            .is_empty());
        let cross_tasks = store.get_tasks(&cross_source, 0, 10, 10).unwrap();
        assert_eq!(cross_tasks.len(), if phase == "verify" { 2 } else { 1 });
        assert_eq!(cross_tasks[0].data.workflow_id, "cross-go");
        if phase != "verify" {
            let cross = AllocatedTask {
                task_id: 2,
                data: TaskData {
                    workflow_id: "cross-rust".into(),
                    run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
                    ..Default::default()
                },
            };
            store
                .create_tasks_with_domains(&cross_target, 1, [(cross_source.domain_id, &cross)])
                .unwrap();
            store.lease(&cross_target, 0, 0, now).unwrap();
            let rejected = AllocatedTask {
                task_id: 3,
                ..cross
            };
            assert!(store
                .create_tasks_with_domains(&cross_target, 1, [(cross_source.domain_id, &rejected)])
                .is_err());
            assert_eq!(store.get_tasks(&cross_source, 0, 10, 10).unwrap().len(), 2);
        }
        if phase == "verify" {
            let tasks = store.get_tasks(&key, 0, 10, 10).unwrap();
            assert_eq!(
                tasks.iter().map(|task| task.task_id).collect::<Vec<_>>(),
                vec![2, 3]
            );
            return;
        }
        let metadata = store.get_task_list(&key).unwrap();
        assert_eq!(metadata.range_id, 1);
        assert_eq!(metadata.data.ack_level, 0);
        assert_eq!(
            metadata
                .data
                .adaptive_partition_config
                .as_ref()
                .unwrap()
                .version,
            3
        );
        assert_eq!(metadata.data.last_updated_nanos, now + 1000);
        let tasks = store.get_tasks(&key, 0, 10, 10).unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].data.expiry_nanos, -6_795_364_578_871_345_152);
        assert_eq!(tasks[1].data.expiry_nanos, now + 3_000_000_000);
        assert!(tasks
            .iter()
            .all(|task| task.data.created_nanos == now && task.data.workflow_id == "go-workflow"));
        let lease = store.lease(&key, 1, 0, now + 1500).unwrap();
        assert_eq!(lease.range_id, 2);
        let task = AllocatedTask {
            task_id: 3,
            data: TaskData {
                workflow_id: "rust-workflow".into(),
                run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
                schedule_id: 5,
                created_nanos: now + 2000,
                expiry_nanos: -6_795_364_578_871_345_152,
                partition_config: None,
            },
        };
        assert!(store
            .create_tasks(&key, 1, std::slice::from_ref(&task))
            .is_err());
        store.create_tasks(&key, 2, &[task]).unwrap();
        let partitions = |ids: &[i32]| {
            ids.iter()
                .map(|id| {
                    (
                        *id,
                        Partition {
                            isolation_groups: Some(vec!["a".into()]),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let mut data = lease.data;
        data.ack_level = 1;
        data.adaptive_partition_config = Some(PartitionConfig {
            version: 4,
            num_read_partitions: 2,
            num_write_partitions: 1,
            read_partitions: Some(partitions(&[0, 1])),
            write_partitions: Some(partitions(&[0])),
        });
        store.update_task_list(&key, 2, data, now + 2000).unwrap();
    }
}
