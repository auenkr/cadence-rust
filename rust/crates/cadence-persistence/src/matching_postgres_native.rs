//! Byte-preserving `MatchingTaskStoreBytes` implementation for PostgreSQL.
//!
//! Go binds task-list names (`string`) to lib/pq as text parameters without
//! validating UTF-8; PostgreSQL itself rejects bytes invalid in the database
//! encoding (e.g. `0xff`, `0xfe`, NUL) with SQLSTATE 22021. We therefore send
//! the exact bytes on the wire with `RawText`, so the server makes the same
//! accept/reject decision as for Go. Rejections surface through the same
//! `normalize_postgres_error` classification (unknown SQLSTATE => Internal).
//! No replacement runes, escaping or alias namespace is ever introduced; a
//! name containing U+FFFD stays a distinct, valid UTF-8 key.

use super::*;
use crate::matching_blob::{
    decode_task_bytes, decode_task_list_bytes, encode_task_bytes, encode_task_list_bytes,
    TaskListDataBytes,
};
use crate::matching_native_task_store::{
    AllocatedTaskBytes, MatchingTaskStoreBytes, MatchingTaskStoreBytesError as BytesError,
    Result as BytesResult, TaskListKeyBytes, TaskListRecordBytes,
};
use crate::matching_task_store::MatchingTaskStoreError;
use postgres::types::{private::BytesMut, to_sql_checked, IsNull, Type};

/// Text-typed parameter carrying arbitrary Go string bytes verbatim.
#[derive(Debug)]
struct RawText<'a>(&'a [u8]);

impl ToSql for RawText<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

fn lg(error: impl Into<MatchingTaskStoreError>) -> BytesError {
    BytesError::Legacy(error.into())
}

fn shard_id(store: &PostgresTaskStore, key: &TaskListKeyBytes) -> i32 {
    crate::matching_task_store::go_db_shard_id_bytes(
        &key.domain_id,
        key.name.as_bytes(),
        store.n_shards,
    )
}

fn task_type(key: &TaskListKeyBytes) -> BytesResult<i16> {
    i16::try_from(key.task_type).map_err(|_| {
        lg(configuration_error(format!(
            "PostgreSQL task type {} does not fit the schema SMALLINT column",
            key.task_type
        )))
    })
}

/// Keep the same Go message bytes, including a non-UTF-8 task-list name.
fn stale_range_bytes(key: &TaskListKeyBytes, range_id: i64) -> BytesError {
    BytesError::lease_renewal(key, range_id)
}

impl PostgresTaskStore {
    fn pool_bytes(&self, key: &TaskListKeyBytes) -> &Arc<PostgresConnectionPool> {
        &self.pools[database_index(shard_id(self, key) as usize, self.use_multiple_databases)]
    }

    fn encode_task_list_native(
        &self,
        data: &TaskListDataBytes,
    ) -> BytesResult<(Vec<u8>, &'static str)> {
        let encoding = self.write_encoding().map_err(BytesError::Legacy)?;
        let bytes = encoding.encode(&encode_task_list_bytes(data)).map_err(lg)?;
        Ok((bytes, encoding.as_str()))
    }

    fn read_task_list_row_native(
        &self,
        client: &mut PostgresLease,
        key: &TaskListKeyBytes,
    ) -> BytesResult<Option<(i64, Vec<u8>, String)>> {
        let shard = shard_id(self, key);
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let params: &[&(dyn ToSql + Sync)] =
            &[&shard, &key.domain_id.as_bytes().as_slice(), &name, &ty];
        match client.query_opt(SELECT_TASK_LIST, params) {
            Ok(row) => row
                .map(|row| {
                    Ok((
                        row.try_get(1)
                            .map_err(|e| lg(normalize_postgres_error(e)))?,
                        row.try_get(4)
                            .map_err(|e| lg(normalize_postgres_error(e)))?,
                        row.try_get(5)
                            .map_err(|e| lg(normalize_postgres_error(e)))?,
                    ))
                })
                .transpose(),
            Err(error) => {
                client.poison_if_closed(&error);
                Err(lg(normalize_postgres_error(error)))
            }
        }
    }

    fn decode_task_list_row_native(
        row: (i64, Vec<u8>, String),
    ) -> BytesResult<TaskListRecordBytes> {
        let (range_id, bytes, label) = row;
        let encoding = BlobEncoding::parse(&label).map_err(lg)?;
        let blob = encoding.decode(&bytes).map_err(lg)?;
        Ok(TaskListRecordBytes {
            range_id,
            data: decode_task_list_bytes(&blob).map_err(lg)?,
        })
    }

    fn lock_and_check_native(
        &self,
        transaction: &mut postgres::Transaction<'_>,
        key: &TaskListKeyBytes,
        expected_range: i64,
    ) -> BytesResult<()> {
        let shard = shard_id(self, key);
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let params: &[&(dyn ToSql + Sync)] =
            &[&shard, &key.domain_id.as_bytes().as_slice(), &name, &ty];
        let actual: Option<i64> = transaction
            .query_opt(LOCK_TASK_LIST, params)
            .map_err(|e| lg(normalize_postgres_error(e)))?
            .map(|row| row.get(0));
        let Some(actual) = actual else {
            return Err(lg(StorageError::ConditionFailed(
                "Task list does not exist.".into(),
            )));
        };
        if actual != expected_range {
            return Err(lg(StorageError::ConditionFailed(format!(
                "Task list range ID was {actual} when it was should have been {expected_range}"
            ))));
        }
        Ok(())
    }
}

fn pg<T>(result: std::result::Result<T, postgres::Error>) -> BytesResult<T> {
    result.map_err(|e| lg(normalize_postgres_error(e)))
}

fn pg_poison<T>(
    client: &mut PostgresLease,
    result: std::result::Result<T, postgres::Error>,
) -> BytesResult<T> {
    result.map_err(|error| {
        client.poison_if_closed(&error);
        lg(normalize_postgres_error(error))
    })
}

impl MatchingTaskStoreBytes for PostgresTaskStore {
    fn fetch_latest_config(
        &self,
        row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        MatchingTaskStore::fetch_latest_config(self, row_type)
    }

    fn get_task_list(&self, key: &TaskListKeyBytes) -> BytesResult<TaskListRecordBytes> {
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        self.read_task_list_row_native(&mut client, key)?
            .ok_or(lg(StorageError::NotFound))
            .and_then(Self::decode_task_list_row_native)
    }

    fn lease(
        &self,
        key: &TaskListKeyBytes,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> BytesResult<TaskListRecordBytes> {
        let shard = shard_id(self, key);
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let mut current = match self.read_task_list_row_native(&mut client, key)? {
            Some((range_id, bytes, encoding)) => {
                if previous_range > 0 && previous_range != range_id {
                    return Err(stale_range_bytes(key, range_id));
                }
                Self::decode_task_list_row_native((range_id, bytes, encoding))?
            }
            None => {
                let data = TaskListDataBytes {
                    kind,
                    last_updated_nanos: now_nanos,
                    ..Default::default()
                };
                let (bytes, encoding) = self.encode_task_list_native(&data)?;
                let params: &[&(dyn ToSql + Sync)] = &[
                    &shard,
                    &key.domain_id.as_bytes().as_slice(),
                    &name,
                    &ty,
                    &0_i64,
                    &bytes,
                    &encoding,
                ];
                let result = client.execute(INSERT_TASK_LIST, params);
                pg_poison(&mut client, result)?;
                TaskListRecordBytes { range_id: 0, data }
            }
        };
        if previous_range > 0 && previous_range != current.range_id {
            return Err(stale_range_bytes(key, current.range_id));
        }
        let mut transaction = pg(client.transaction())?;
        self.lock_and_check_native(&mut transaction, key, current.range_id)?;
        current.range_id = current.range_id.wrapping_add(1);
        current.data.last_updated_nanos = now_nanos;
        let (bytes, encoding) = self.encode_task_list_native(&current.data)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &current.range_id,
            &bytes,
            &encoding,
            &shard,
            &key.domain_id.as_bytes().as_slice(),
            &name,
            &ty,
        ];
        let affected = pg(transaction.execute(UPDATE_TASK_LIST, params))?;
        if affected != 1 {
            return Err(lg(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!("lease update affected {affected} rows instead of 1"),
            }));
        }
        pg(transaction.commit())?;
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
        let shard = shard_id(self, key);
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let mut transaction = pg(client.transaction())?;
        self.lock_and_check_native(&mut transaction, key, range)?;
        let (bytes, encoding) = self.encode_task_list_native(&data)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &range,
            &bytes,
            &encoding,
            &shard,
            &key.domain_id.as_bytes().as_slice(),
            &name,
            &ty,
        ];
        let affected = pg(transaction.execute(UPDATE_TASK_LIST, params))?;
        if affected != 1 {
            return Err(lg(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!("task-list update affected {affected} rows instead of 1"),
            }));
        }
        pg(transaction.commit())
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTaskBytes)],
    ) -> BytesResult<()> {
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let mut transaction = pg(client.transaction())?;
        let write_encoding = self.write_encoding().map_err(BytesError::Legacy)?;
        for (domain, task) in tasks {
            let mut data = task.data.for_sql()?;
            data.created_nanos = now_nanos;
            let encoded =
                encode_task_bytes(&data).map_err(|e| lg(StorageError::BackendConfiguration(e)))?;
            let bytes = write_encoding.encode(&encoded).map_err(lg)?;
            let encoding = write_encoding.as_str();
            let params: &[&(dyn ToSql + Sync)] = &[
                &domain.as_bytes().as_slice(),
                &name,
                &ty,
                &task.task_id,
                &bytes,
                &encoding,
            ];
            pg(transaction.execute(INSERT_TASK, params))?;
        }
        self.lock_and_check_native(&mut transaction, key, range)?;
        pg(transaction.commit())
    }

    fn get_tasks(
        &self,
        key: &TaskListKeyBytes,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> BytesResult<Vec<AllocatedTaskBytes>> {
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let params: &[&(dyn ToSql + Sync)] = &[
            &key.domain_id.as_bytes().as_slice(),
            &name,
            &ty,
            &read_level,
            &max_read_level,
            &batch_size,
        ];
        let result = client.query(SELECT_TASKS, params);
        let rows = pg_poison(&mut client, result)?;
        rows.into_iter()
            .map(|row| {
                let task_id: i64 = pg(row.try_get(0))?;
                let bytes: Vec<u8> = pg(row.try_get(1))?;
                let label: String = pg(row.try_get(2))?;
                let encoding = BlobEncoding::parse(&label).map_err(lg)?;
                let blob = encoding.decode(&bytes).map_err(lg)?;
                Ok(AllocatedTaskBytes {
                    task_id,
                    data: decode_task_bytes(&blob).map_err(lg)?,
                })
            })
            .collect()
    }

    fn task_list_size(&self, key: &TaskListKeyBytes, ack_level: i64) -> BytesResult<i64> {
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let params: &[&(dyn ToSql + Sync)] =
            &[&key.domain_id.as_bytes().as_slice(), &name, &ty, &ack_level];
        let result = client
            .query_one(COUNT_TASKS, params)
            .and_then(|row| row.try_get(0));
        pg_poison(&mut client, result)
    }

    fn complete_task(&self, key: &TaskListKeyBytes, task_id: i64) -> BytesResult<()> {
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let params: &[&(dyn ToSql + Sync)] =
            &[&key.domain_id.as_bytes().as_slice(), &name, &ty, &task_id];
        let result = client.execute(DELETE_TASK, params);
        pg_poison(&mut client, result).map(|_| ())
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKeyBytes,
        inclusive_task_id: i64,
        limit: i64,
    ) -> BytesResult<usize> {
        if limit == 0 {
            return Err(lg(StorageError::MissingLimit));
        }
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let params: &[&(dyn ToSql + Sync)] = &[
            &key.domain_id.as_bytes().as_slice(),
            &name,
            &ty,
            &inclusive_task_id,
            &limit,
        ];
        let result = client.execute(RANGE_DELETE_TASKS, params);
        let affected = pg_poison(&mut client, result)?;
        usize::try_from(affected).map_err(|error| {
            lg(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!("PostgreSQL affected-row count did not fit usize: {error}"),
            })
        })
    }

    fn delete_task_list(&self, key: &TaskListKeyBytes, range: i64) -> BytesResult<()> {
        let shard = shard_id(self, key);
        let mut client = self.pool_bytes(key).checkout().map_err(lg)?;
        let ty = task_type(key)?;
        let name = RawText(key.name.as_bytes());
        let params: &[&(dyn ToSql + Sync)] = &[
            &shard,
            &key.domain_id.as_bytes().as_slice(),
            &name,
            &ty,
            &range,
        ];
        let result = client.execute(DELETE_TASK_LIST, params);
        let affected = pg_poison(&mut client, result)?;
        if affected != 1 {
            return Err(lg(StorageError::DeleteCount(affected as usize)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_blob::{TaskDataBytes, TaskListDataBytes};
    use cadence_common::go_string::GoString;

    fn wire(bytes: &[u8]) -> Vec<u8> {
        let mut out = BytesMut::new();
        RawText(bytes).to_sql(&Type::TEXT, &mut out).unwrap();
        out.to_vec()
    }

    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::nil(),
            name: GoString::from(name),
            task_type: 0,
        }
    }

    #[test]
    fn raw_text_sends_exact_bytes_without_replacement() {
        assert_eq!(wire(b"\xff"), vec![0xff]);
        assert_eq!(wire(b"\xfe"), vec![0xfe]);
        assert_eq!(wire("\u{fffd}".as_bytes()), vec![0xef, 0xbf, 0xbd]);
        assert_eq!(wire(b"a\x00b"), b"a\x00b".to_vec());
        assert!(<RawText as ToSql>::accepts(&Type::TEXT));
        assert!(<RawText as ToSql>::accepts(&Type::VARCHAR));
        assert!(!<RawText as ToSql>::accepts(&Type::BYTEA));
    }

    #[test]
    fn invalid_and_replacement_names_hash_to_independent_wire_values() {
        let (a, b, c) = (wire(b"\xff"), wire(b"\xfe"), wire("\u{fffd}".as_bytes()));
        assert!(a != b && a != c && b != c);
        // The shard hash input is the same raw byte string as the wire value.
        let id = Uuid::nil();
        for n in [1_usize, 16, 1000] {
            for name in [&b"\xff"[..], b"\xfe", "\u{fffd}".as_bytes()] {
                let mut input = id.to_string().into_bytes();
                input.push(b'_');
                input.extend_from_slice(name);
                assert_eq!(
                    crate::matching_task_store::go_db_shard_id_bytes(&id, name, n),
                    (farmhash::hash32(&input) % n as u32) as i32
                );
            }
        }
    }

    #[test]
    fn valid_native_task_type_and_stale_message_follow_legacy() {
        let k = key(b"queue");
        assert_eq!(task_type(&k).unwrap(), 0);
        let mut big = k.clone();
        big.task_type = i32::MAX;
        assert!(matches!(
            task_type(&big),
            Err(BytesError::Legacy(StorageError::BackendConfiguration(_)))
        ));
        let legacy = stale_range(
            &crate::matching_task_store::TaskListKey {
                domain_id: Uuid::nil(),
                name: "queue".into(),
                task_type: 0,
            },
            7,
        );
        match (stale_range_bytes(&k, 7), legacy) {
            (BytesError::ConditionFailed { message: a }, StorageError::ConditionFailed(b)) => {
                assert_eq!(a.as_bytes(), b.as_bytes())
            }
            _ => panic!("unexpected stale-range classification"),
        }
    }

    #[test]
    fn malformed_run_id_bytes_are_retained_and_fatal() {
        let data = TaskDataBytes {
            raw_run_id: Some(GoString::from(&b"\xffnot-a-uuid"[..])),
            ..Default::default()
        };
        let error = BytesError::from(data.for_sql().unwrap_err());
        assert!(!error.is_transient());
        match error {
            BytesError::MalformedSqlRunId(e) => assert_eq!(e.raw.as_bytes(), b"\xffnot-a-uuid"),
            _ => panic!("expected malformed RunID"),
        }
    }

    /// Run against a disposable PostgreSQL with the Cadence schema installed:
    /// CADENCE_POSTGRES_ADDR/USER/PASSWORD/DATABASE. Compares Go semantics:
    /// a server that rejects invalid UTF-8 rejects `\xff` like lib/pq would,
    /// valid names (including U+FFFD) are distinct, leases fence, rows persist
    /// raw blob bytes, and legacy/native stores read each other's valid rows.
    #[test]
    #[ignore = "live PostgreSQL oracle; needs a disposable Cadence schema database"]
    fn live_native_store_raw_bytes_fencing_and_legacy_cross_read() {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.into());
        let store = PostgresTaskStore::connect(PostgresTaskStoreConfig {
            shards: vec![PostgresShardConfig {
                connect_addr: std::env::var("CADENCE_POSTGRES_ADDR")
                    .expect("CADENCE_POSTGRES_ADDR"),
                user: env("CADENCE_POSTGRES_USER", "postgres"),
                password: env("CADENCE_POSTGRES_PASSWORD", ""),
                database_name: env("CADENCE_POSTGRES_DATABASE", "cadence"),
                connect_attributes: Default::default(),
                tls: None,
            }],
            n_shards: 1,
            use_multiple_databases: false,
            max_conns: 4,
            max_idle_conns: 2,
            max_conn_lifetime: None,
            encoding: BlobEncoding::parse("thriftrw").unwrap(),
        })
        .unwrap();
        let domain = Uuid::new_v4();
        let k = |name: &[u8]| TaskListKeyBytes {
            domain_id: domain,
            name: GoString::from(name),
            task_type: 0,
        };

        // Invalid UTF-8 is rejected by the server (Go/lib/pq sends the same
        // bytes), classified like any unknown backend error, and writes nothing.
        let bad = k(b"\xff");
        let err = MatchingTaskStoreBytes::lease(&store, &bad, 0, 0, 1).unwrap_err();
        assert!(err.is_transient(), "{err}");
        assert!(MatchingTaskStoreBytes::get_task_list(&store, &bad).is_err());

        // U+FFFD is a different, valid key and does not alias \xff / \xfe.
        let repl = k("\u{fffd}".as_bytes());
        let leased = MatchingTaskStoreBytes::lease(&store, &repl, 0, 0, 1).unwrap();
        assert_eq!(leased.range_id, 1);
        assert!(MatchingTaskStoreBytes::get_task_list(&store, &k(b"\xfe")).is_err());

        // Lease fencing and stale tokens.
        assert!(MatchingTaskStoreBytes::lease(&store, &repl, 0, 99, 2).is_err());
        let second = MatchingTaskStoreBytes::lease(&store, &repl, 0, 1, 2).unwrap();
        assert_eq!(second.range_id, 2);
        assert!(MatchingTaskStoreBytes::update_task_list(
            &store,
            &repl,
            1,
            TaskListDataBytes::default(),
            3
        )
        .is_err());

        // Native write, legacy read of the same valid row.
        let task = AllocatedTaskBytes {
            task_id: 1,
            data: TaskDataBytes {
                workflow_id: GoString::from("wf"),
                schedule_id: 5,
                ..Default::default()
            },
        };
        MatchingTaskStoreBytes::create_tasks_with_domains(&store, &repl, 2, 10, &[(domain, &task)])
            .unwrap();
        let legacy_key = crate::matching_task_store::TaskListKey {
            domain_id: domain,
            name: "\u{fffd}".into(),
            task_type: 0,
        };
        let legacy = MatchingTaskStore::get_tasks(&store, &legacy_key, 0, 10, 10).unwrap();
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].data.workflow_id, "wf");
        let native = MatchingTaskStoreBytes::get_tasks(&store, &repl, 0, 10, 10).unwrap();
        assert_eq!(native[0].data.workflow_id.as_bytes(), b"wf");
        assert_eq!(
            MatchingTaskStoreBytes::task_list_size(&store, &repl, 0).unwrap(),
            1
        );
        assert_eq!(
            MatchingTaskStoreBytes::complete_tasks_less_than(&store, &repl, 1, 10).unwrap(),
            1
        );
        MatchingTaskStoreBytes::delete_task_list(&store, &repl, 2).unwrap();
    }
}
