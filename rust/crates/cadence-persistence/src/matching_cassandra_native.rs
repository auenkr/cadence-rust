//! Byte-preserving `MatchingTaskStoreBytes` for the Cassandra `tasks` table.
//!
//! Go's gocql marshals a `string` bound to a `text` column as its raw bytes
//! (no UTF-8 check, no normalization). The stock Scylla `String`/UDT serializers
//! would force a lossy `String`, so every identifier here is bound through
//! [`RawCql`], a driver `SerializeValue` that writes `GoString` bytes verbatim
//! (also inside UDT fields, sets and maps). Statement texts, LWT fencing, TTL
//! rules, retries and error classes are shared with the legacy store.
//!
//! Limitation: a real Cassandra server validates `text` as UTF-8, so non-UTF-8
//! bytes are sent exactly as Go would and rejected by the server as a backend
//! error; they are never replaced or rewritten client-side. Rows read back are
//! server-validated UTF-8 and convert to `GoString` without loss.

use super::{
    expiry_not_passed, timestamp, ttl_seconds, CassandraMatchingTaskStore as Store, COUNT_TASKS,
    DELETE_LIST, FALLBACK_CONSISTENCY, INITIAL_RANGE_ID, INSERT_TASK_LIST, INSERT_TASK_LIST_TTL,
    LIST_ROW, LIST_SENTINEL, RANGE_DELETE_TASKS, SELECT_LIST, SELECT_TASKS, TASK_ROW, UPDATE_LIST,
    UPDATE_LIST_TTL,
};
use crate::{
    cassandra::{CassandraTaskListUdt, CassandraTaskUdt},
    matching_blob::{PartitionBytes, PartitionConfigBytes, TaskDataBytes, TaskListDataBytes},
    matching_cassandra_exec::{bound, BoundValue, BoundValues, CqlStatement},
    matching_native_task_store::{
        AllocatedTaskBytes, MatchingTaskStoreBytes, MatchingTaskStoreBytesError as BytesError,
        Result, TaskListKeyBytes, TaskListRecordBytes,
    },
    matching_task_store::MatchingTaskStoreError,
};
use cadence_common::go_string::GoString;
use scylla::{
    cluster::metadata::{CollectionType, ColumnType, NativeType},
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
        SerializationError,
    },
};
use std::{collections::BTreeMap, fmt};
use uuid::Uuid;

const INSERT_TASK: &str = "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, task, created_time) VALUES (?, ?, ?, ?, ?, ?, ?)";
const RANGE_FENCE: &str = "UPDATE tasks SET range_id = ?, last_updated_time = ? WHERE domain_id = ? AND task_list_name = ? AND task_list_type = ? AND type = ? AND task_id = ? IF range_id = ?";

#[derive(Debug)]
struct RawSerError(String);
impl fmt::Display for RawSerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for RawSerError {}
fn ser_err(message: impl Into<String>) -> SerializationError {
    SerializationError::new(RawSerError(message.into()))
}

/// A CQL value tree whose text leaves are raw bytes.
#[derive(Debug)]
pub(super) enum RawCql {
    Null,
    Text(Vec<u8>),
    Scalar(Box<dyn BoundValue>),
    Udt(Vec<(&'static str, RawCql)>),
    /// `set<..>` / `list<..>` in caller order.
    Seq(Vec<RawCql>),
    Map(Vec<(RawCql, RawCql)>),
}

impl RawCql {
    fn text(value: &GoString) -> Self {
        Self::Text(value.as_bytes().to_vec())
    }
    fn scalar<T: BoundValue + 'static>(value: T) -> Self {
        Self::Scalar(Box::new(value))
    }
}

impl SerializeValue for RawCql {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> std::result::Result<WrittenCellProof<'b>, SerializationError> {
        let overflow = |_| ser_err("CQL cell size overflow");
        match self {
            RawCql::Null => Ok(writer.set_null()),
            RawCql::Text(bytes) => {
                if !matches!(
                    typ,
                    ColumnType::Native(NativeType::Text | NativeType::Ascii)
                ) {
                    return Err(ser_err(format!("raw text bound to non-text type {typ:?}")));
                }
                writer.set_value(bytes).map_err(overflow)
            }
            RawCql::Scalar(value) => value.serialize(typ, writer),
            RawCql::Udt(fields) => {
                let ColumnType::UserDefinedType { definition, .. } = typ else {
                    return Err(ser_err(format!("UDT bound to non-UDT type {typ:?}")));
                };
                if let Some((name, _)) = fields.iter().find(|(name, _)| {
                    !definition
                        .field_types
                        .iter()
                        .any(|(f, _)| f.as_ref() == *name)
                }) {
                    return Err(ser_err(format!(
                        "no field {name} in UDT {}",
                        definition.name
                    )));
                }
                let mut builder = writer.into_value_builder();
                for (field, field_type) in &definition.field_types {
                    let cell = builder.make_sub_writer();
                    match fields.iter().find(|(name, _)| *name == field.as_ref()) {
                        Some((_, value)) => {
                            value.serialize(field_type, cell)?;
                        }
                        None => {
                            cell.set_null();
                        }
                    }
                }
                builder.finish().map_err(overflow)
            }
            RawCql::Seq(items) => {
                let ColumnType::Collection {
                    typ: CollectionType::List(element) | CollectionType::Set(element),
                    ..
                } = typ
                else {
                    return Err(ser_err(format!("sequence bound to non-list/set {typ:?}")));
                };
                let mut builder = writer.into_value_builder();
                let count = i32::try_from(items.len()).map_err(|_| ser_err("too many elements"))?;
                builder.append_bytes(&count.to_be_bytes());
                for item in items {
                    item.serialize(element, builder.make_sub_writer())?;
                }
                builder.finish().map_err(overflow)
            }
            RawCql::Map(pairs) => {
                let ColumnType::Collection {
                    typ: CollectionType::Map(key_type, value_type),
                    ..
                } = typ
                else {
                    return Err(ser_err(format!("map bound to non-map {typ:?}")));
                };
                let mut builder = writer.into_value_builder();
                let count = i32::try_from(pairs.len()).map_err(|_| ser_err("too many entries"))?;
                builder.append_bytes(&count.to_be_bytes());
                for (key, value) in pairs {
                    key.serialize(key_type, builder.make_sub_writer())?;
                    value.serialize(value_type, builder.make_sub_writer())?;
                }
                builder.finish().map_err(overflow)
            }
        }
    }
}

fn partitions(parts: &Option<BTreeMap<i32, PartitionBytes>>) -> RawCql {
    let Some(parts) = parts else {
        return RawCql::Null;
    };
    RawCql::Map(
        parts
            .iter()
            .map(|(id, part)| {
                let groups = match &part.isolation_groups {
                    Some(groups) => RawCql::Seq(groups.iter().map(RawCql::text).collect()),
                    None => RawCql::Null,
                };
                (
                    RawCql::scalar(*id),
                    RawCql::Udt(vec![("isolation_groups", groups)]),
                )
            })
            .collect(),
    )
}

fn partition_config(config: &Option<PartitionConfigBytes>) -> RawCql {
    let Some(config) = config else {
        return RawCql::Null;
    };
    RawCql::Udt(vec![
        ("version", RawCql::scalar(config.version)),
        (
            "num_read_partitions",
            RawCql::scalar(config.num_read_partitions),
        ),
        (
            "num_write_partitions",
            RawCql::scalar(config.num_write_partitions),
        ),
        ("read_partitions", partitions(&config.read_partitions)),
        ("write_partitions", partitions(&config.write_partitions)),
    ])
}

fn task_list_udt(key: &TaskListKeyBytes, data: &TaskListDataBytes) -> Result<RawCql> {
    Ok(RawCql::Udt(vec![
        ("domain_id", RawCql::scalar(key.domain_id)),
        ("name", RawCql::text(&key.name)),
        ("type", RawCql::scalar(key.task_type)),
        ("ack_level", RawCql::scalar(data.ack_level)),
        ("kind", RawCql::scalar(i32::from(data.kind))),
        (
            "last_updated",
            RawCql::scalar(timestamp(data.last_updated_nanos)?),
        ),
        (
            "adaptive_partition_config",
            partition_config(&data.adaptive_partition_config),
        ),
    ]))
}

/// Go parses the RunID string for Cassandra (gocql.ParseUUID); a malformed
/// value is a non-retryable configuration error and nothing is written.
fn task_udt(domain: Uuid, data: &TaskDataBytes) -> Result<RawCql> {
    let run = data
        .cassandra_run_id()
        .map_err(MatchingTaskStoreError::BackendConfiguration)?;
    let run = match run {
        Some(bytes) => RawCql::scalar(Uuid::from_slice(&bytes).map_err(|error| {
            MatchingTaskStoreError::BackendConfiguration(format!("invalid task run ID: {error}"))
        })?),
        None => RawCql::Null,
    };
    let partition = match &data.partition_config {
        Some(map) => RawCql::Map(
            map.iter()
                .map(|(k, v)| (RawCql::text(k), RawCql::text(v)))
                .collect(),
        ),
        None => RawCql::Null,
    };
    Ok(RawCql::Udt(vec![
        ("domain_id", RawCql::scalar(domain)),
        ("workflow_id", RawCql::text(&data.workflow_id)),
        ("run_id", run),
        ("schedule_id", RawCql::scalar(data.schedule_id)),
        (
            "created_time",
            RawCql::scalar(timestamp(data.created_nanos)?),
        ),
        ("partition_config", partition),
    ]))
}

fn key_values(key: &TaskListKeyBytes, row: i32, id: i64) -> BoundValues {
    vec![
        bound(key.domain_id),
        bound(RawCql::text(&key.name)),
        bound(key.task_type),
        bound(row),
        bound(id),
    ]
}

fn task_list_bytes(row: CassandraTaskListUdt) -> Result<TaskListDataBytes> {
    Ok(Store::list_data(row)?.into())
}

fn task_bytes(task_id: i64, task: CassandraTaskUdt) -> Result<AllocatedTaskBytes> {
    let legacy = super::task_from_udt(task_id, task, None)?;
    Ok(legacy.into())
}

fn message(prefix: &str, name: &GoString, suffix: String) -> BytesError {
    let mut bytes = prefix.as_bytes().to_vec();
    bytes.extend_from_slice(name.as_bytes());
    bytes.extend_from_slice(suffix.as_bytes());
    BytesError::ConditionFailed {
        message: GoString::from(bytes),
    }
}

fn internal(error: impl fmt::Display + 'static) -> BytesError {
    BytesError::Legacy(Store::internal(error))
}

impl Store {
    /// Update statement values in `UPDATE_LIST*` order.
    fn native_update_values(
        key: &TaskListKeyBytes,
        range_id: i64,
        udt: RawCql,
        now_nanos: i64,
        condition_range: i64,
    ) -> Result<BoundValues> {
        Ok(vec![
            bound(range_id),
            bound(udt),
            bound(timestamp(now_nanos)?),
            bound(key.domain_id),
            bound(RawCql::text(&key.name)),
            bound(key.task_type),
            bound(LIST_ROW),
            bound(LIST_SENTINEL),
            bound(condition_range),
        ])
    }

    fn native_range_delete(
        &self,
        key: &TaskListKeyBytes,
        min_exclusive: i64,
        max_inclusive: i64,
    ) -> Result<()> {
        let values = || -> BoundValues {
            vec![
                bound(key.domain_id),
                bound(RawCql::text(&key.name)),
                bound(key.task_type),
                bound(TASK_ROW),
                bound(min_exclusive),
                bound(max_inclusive),
            ]
        };
        if (self.all_consistency_delete)() {
            let all = CqlStatement::with_consistency(
                RANGE_DELETE_TASKS,
                scylla::frame::types::Consistency::All,
            );
            match self.executor.exec(&all, values()) {
                Ok(()) => {}
                Err(error) if error.is_unavailable() => {
                    let fallback =
                        CqlStatement::with_consistency(RANGE_DELETE_TASKS, FALLBACK_CONSISTENCY);
                    self.executor.exec(&fallback, values()).map_err(internal)?;
                }
                Err(error) => return Err(internal(error)),
            }
        } else {
            self.executor
                .exec(&CqlStatement::new(RANGE_DELETE_TASKS), values())
                .map_err(internal)?;
        }
        Ok(())
    }
}

impl MatchingTaskStoreBytes for Store {
    fn fetch_latest_config(
        &self,
        row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        crate::matching_task_store::MatchingTaskStore::fetch_latest_config(self, row_type)
    }

    fn get_task_list(&self, key: &TaskListKeyBytes) -> Result<TaskListRecordBytes> {
        let Some((range_id, data)) = self
            .executor
            .select_task_list(
                &CqlStatement::new(SELECT_LIST),
                key_values(key, LIST_ROW, LIST_SENTINEL),
            )
            .map_err(internal)?
        else {
            return Err(MatchingTaskStoreError::NotFound.into());
        };
        Ok(TaskListRecordBytes {
            range_id,
            data: task_list_bytes(data)?,
        })
    }

    fn lease(
        &self,
        key: &TaskListKeyBytes,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecordBytes> {
        let current = match self.get_task_list(key) {
            Ok(row) => row,
            Err(BytesError::Legacy(MatchingTaskStoreError::NotFound)) => {
                let data = TaskListDataBytes {
                    kind,
                    last_updated_nanos: now_nanos,
                    ..Default::default()
                };
                let udt = task_list_udt(key, &data)?;
                let mut values = key_values(key, LIST_ROW, LIST_SENTINEL);
                values.push(bound(INITIAL_RANGE_ID));
                values.push(bound(udt));
                values.push(bound(timestamp(now_nanos)?));
                let outcome = self
                    .executor
                    .exec_lwt(&CqlStatement::new(INSERT_TASK_LIST), values)
                    .map_err(internal)?;
                if outcome.applied {
                    return Ok(TaskListRecordBytes {
                        range_id: INITIAL_RANGE_ID,
                        data,
                    });
                }
                // Go does not retry after losing the IF NOT EXISTS race.
                return Err(message(
                    "leaseTaskList: taskList:",
                    &key.name,
                    format!(
                        ", taskListType:{}, haveRangeID:{}, gotRangeID:{}",
                        key.task_type, INITIAL_RANGE_ID, outcome.range_id
                    ),
                ));
            }
            Err(error) => return Err(error),
        };
        if previous_range > 0 && previous_range != current.range_id {
            // Go message bytes: "leaseTaskList:renew failed: taskList:%v, ..." with
            // haveRangeID = the caller's range (the shared `lease_renewal`
            // helper hard-codes 0 and omits the space, so it is not used).
            return Err(message(
                "leaseTaskList:renew failed: taskList:",
                &key.name,
                format!(
                    ", taskListType:{}, haveRangeID:{}, gotRangeID:{}",
                    key.task_type, previous_range, current.range_id
                ),
            ));
        }
        let next = current.range_id.wrapping_add(1);
        let mut data = current.data;
        data.last_updated_nanos = now_nanos;
        let udt = task_list_udt(key, &data)?;
        let outcome = self
            .executor
            .exec_lwt(
                &CqlStatement::new(UPDATE_LIST),
                Self::native_update_values(key, next, udt, now_nanos, current.range_id)?,
            )
            .map_err(internal)?;
        if !outcome.applied {
            return Err(message(
                "leaseTaskList: taskList:",
                &key.name,
                format!(
                    ", taskListType:{}, haveRangeID:{}, gotRangeID:{}",
                    key.task_type, next, outcome.range_id
                ),
            ));
        }
        data.kind = kind;
        Ok(TaskListRecordBytes {
            range_id: next,
            data,
        })
    }

    fn update_task_list(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        mut data: TaskListDataBytes,
        now_nanos: i64,
    ) -> Result<()> {
        data.last_updated_nanos = now_nanos;
        let udt = task_list_udt(key, &data)?;
        let outcome = if matches!(data.kind, 1 | 2) {
            let mut insert = key_values(key, LIST_ROW, LIST_SENTINEL);
            insert.push(bound(timestamp(now_nanos)?));
            self.executor
                .batch_lwt(
                    &[
                        CqlStatement::new(INSERT_TASK_LIST_TTL),
                        CqlStatement::new(UPDATE_LIST_TTL),
                    ],
                    vec![
                        insert,
                        Self::native_update_values(key, range, udt, now_nanos, range)?,
                    ],
                )
                .map_err(internal)?
        } else {
            self.executor
                .exec_lwt(
                    &CqlStatement::new(UPDATE_LIST),
                    Self::native_update_values(key, range, udt, now_nanos, range)?,
                )
                .map_err(internal)?
        };
        if outcome.applied {
            Ok(())
        } else {
            Err(message(
                "Failed to update task list. name: ",
                &key.name,
                format!(
                    ", type: {}, rangeID: {}, columns: (range_id={})",
                    key.task_type, range, outcome.range_id
                ),
            ))
        }
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTaskBytes)],
    ) -> Result<()> {
        let mut statements = Vec::new();
        let mut values: Vec<BoundValues> = Vec::new();
        for (domain, task) in tasks {
            if !expiry_not_passed(task.data.expiry_nanos, now_nanos) {
                continue;
            }
            // Same as legacy: rows are stamped with the operation time and TTL
            // is measured from it.
            let mut data = task.data.clone();
            data.created_nanos = now_nanos;
            let ttl = ttl_seconds(data.expiry_nanos, data.created_nanos)
                .map(|ttl| format!(" USING TTL {ttl}"))
                .unwrap_or_default();
            statements.push(CqlStatement::new(format!("{INSERT_TASK}{ttl}")));
            let mut row = key_values(key, TASK_ROW, task.task_id);
            row.push(bound(task_udt(*domain, &data)?));
            row.push(bound(timestamp(now_nanos)?));
            values.push(row);
        }
        statements.push(CqlStatement::new(RANGE_FENCE));
        values.push(vec![
            bound(range),
            bound(timestamp(now_nanos)?),
            bound(key.domain_id),
            bound(RawCql::text(&key.name)),
            bound(key.task_type),
            bound(LIST_ROW),
            bound(LIST_SENTINEL),
            bound(range),
        ]);
        let outcome = self
            .executor
            .batch_lwt(&statements, values)
            .map_err(internal)?;
        if outcome.applied {
            Ok(())
        } else {
            Err(message(
                "Failed to insert tasks. name: ",
                &key.name,
                format!(
                    ", type: {}, rangeID: {}, columns: (range_id={})",
                    key.task_type, range, outcome.range_id
                ),
            ))
        }
    }

    fn get_tasks(
        &self,
        key: &TaskListKeyBytes,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTaskBytes>> {
        if batch_size <= 0 {
            return Ok(Vec::new());
        }
        let limit = i32::try_from(batch_size).map_err(|_| {
            MatchingTaskStoreError::BackendConfiguration(format!(
                "task batch size exceeds Cassandra limit: {batch_size}"
            ))
        })?;
        let mut values = key_values(key, TASK_ROW, read_level);
        values.push(bound(max_read_level));
        values.push(bound(limit));
        let rows = self
            .executor
            .select_tasks(&CqlStatement::new(SELECT_TASKS), values)
            .map_err(internal)?;
        let mut tasks = Vec::new();
        for (id, task, _ttl) in rows {
            tasks.push(task_bytes(id, task)?);
            if tasks.len() >= batch_size as usize {
                break;
            }
        }
        Ok(tasks)
    }

    fn task_list_size(&self, key: &TaskListKeyBytes, ack_level: i64) -> Result<i64> {
        self.executor
            .select_count(
                &CqlStatement::new(COUNT_TASKS),
                key_values(key, TASK_ROW, ack_level),
            )
            .map_err(internal)?
            .ok_or_else(|| internal("Cassandra count returned no row"))
    }

    fn complete_task(&self, key: &TaskListKeyBytes, task_id: i64) -> Result<()> {
        self.native_range_delete(key, task_id.wrapping_sub(1), task_id)
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKeyBytes,
        inclusive_task_id: i64,
        _limit: i64,
    ) -> Result<usize> {
        self.native_range_delete(key, 0, inclusive_task_id)?;
        Ok(0)
    }

    fn delete_task_list(&self, key: &TaskListKeyBytes, range: i64) -> Result<()> {
        let values = || -> BoundValues {
            let mut v = key_values(key, LIST_ROW, LIST_SENTINEL);
            v.push(bound(range));
            v
        };
        let outcome = {
            let all =
                CqlStatement::with_consistency(DELETE_LIST, scylla::frame::types::Consistency::All);
            match self.executor.exec_lwt(&all, values()) {
                Ok(outcome) => outcome,
                Err(error) if error.is_unavailable() => self
                    .executor
                    .exec_lwt(
                        &CqlStatement::with_consistency(DELETE_LIST, FALLBACK_CONSISTENCY),
                        values(),
                    )
                    .map_err(internal)?,
                Err(error) => return Err(internal(error)),
            }
        };
        if outcome.applied {
            Ok(())
        } else {
            Err(message(
                "Failed to delete task list. name: ",
                &key.name,
                format!(
                    ", type: {}, rangeID: {}, columns: (range_id={})",
                    key.task_type, range, outcome.range_id
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_blob::TaskListData;
    use crate::matching_cassandra_exec::{ConfigRow, CqlExecutor, ExecResult, LwtOutcome};
    use scylla::cluster::metadata::UserDefinedType;
    use std::{
        borrow::Cow,
        sync::{Arc, Mutex},
    };

    fn text() -> ColumnType<'static> {
        ColumnType::Native(NativeType::Text)
    }
    fn encode(value: &RawCql, typ: &ColumnType) -> Vec<u8> {
        let mut buf = Vec::new();
        value.serialize(typ, CellWriter::new(&mut buf)).unwrap();
        buf
    }

    #[test]
    fn raw_text_bytes_are_written_verbatim_without_normalization() {
        // 0xff is not UTF-8; U+FFFD and a decomposed/precomposed pair must not collapse.
        for raw in [
            &b"\xff"[..],
            "\u{fffd}".as_bytes(),
            "e\u{301}".as_bytes(),
            "\u{e9}".as_bytes(),
            b"",
        ] {
            let out = encode(&RawCql::Text(raw.to_vec()), &text());
            assert_eq!(&out[..4], &(raw.len() as i32).to_be_bytes());
            assert_eq!(&out[4..], raw);
        }
        assert!(RawCql::Text(vec![1])
            .serialize(
                &ColumnType::Native(NativeType::BigInt),
                CellWriter::new(&mut Vec::new())
            )
            .is_err());
    }

    #[test]
    fn udt_fields_collections_and_missing_fields_use_raw_bytes() {
        let udt = ColumnType::UserDefinedType {
            frozen: true,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("task"),
                keyspace: Cow::Borrowed("ks"),
                field_types: vec![
                    (Cow::Borrowed("workflow_id"), text()),
                    (
                        Cow::Borrowed("partition_config"),
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::Map(Box::new(text()), Box::new(text())),
                        },
                    ),
                    (Cow::Borrowed("absent"), text()),
                ],
            }),
        };
        let value = RawCql::Udt(vec![
            ("workflow_id", RawCql::Text(b"\xfe".to_vec())),
            (
                "partition_config",
                RawCql::Map(vec![(
                    RawCql::Text(b"k\xff".to_vec()),
                    RawCql::Text(b"v".to_vec()),
                )]),
            ),
        ]);
        let out = encode(&value, &udt);
        let mut expected = Vec::new();
        expected.extend_from_slice(&1i32.to_be_bytes());
        expected.push(0xfe);
        let mut map = 1i32.to_be_bytes().to_vec();
        map.extend_from_slice(&2i32.to_be_bytes());
        map.extend_from_slice(b"k\xff");
        map.extend_from_slice(&1i32.to_be_bytes());
        map.push(b'v');
        expected.extend_from_slice(&(map.len() as i32).to_be_bytes());
        expected.extend_from_slice(&map);
        expected.extend_from_slice(&(-1i32).to_be_bytes()); // absent => null
        assert_eq!(&out[..4], &(expected.len() as i32).to_be_bytes());
        assert_eq!(&out[4..], &expected[..]);
        // Unknown UDT field is rejected rather than dropped.
        let bad = RawCql::Udt(vec![("nope", RawCql::Null)]);
        assert!(bad
            .serialize(&udt, CellWriter::new(&mut Vec::new()))
            .is_err());
    }

    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<(String, Vec<String>)>>,
        outcome: Mutex<Option<LwtOutcome>>,
        list: Mutex<Option<(i64, CassandraTaskListUdt)>>,
    }
    impl Fake {
        fn rec(&self, text: &str, values: &[Box<dyn BoundValue>]) {
            self.calls.lock().unwrap().push((
                text.to_owned(),
                values.iter().map(|v| format!("{v:?}")).collect(),
            ));
        }
        fn out(&self) -> LwtOutcome {
            self.outcome.lock().unwrap().unwrap_or(LwtOutcome {
                applied: true,
                range_id: 0,
            })
        }
    }
    impl CqlExecutor for Fake {
        fn exec(&self, s: &CqlStatement, v: BoundValues) -> ExecResult<()> {
            self.rec(&s.text, &v);
            Ok(())
        }
        fn exec_lwt(&self, s: &CqlStatement, v: BoundValues) -> ExecResult<LwtOutcome> {
            self.rec(&s.text, &v);
            Ok(self.out())
        }
        fn batch_lwt(&self, s: &[CqlStatement], v: Vec<BoundValues>) -> ExecResult<LwtOutcome> {
            for (s, v) in s.iter().zip(&v) {
                self.rec(&s.text, v);
            }
            Ok(self.out())
        }
        fn select_task_list(
            &self,
            s: &CqlStatement,
            v: BoundValues,
        ) -> ExecResult<Option<(i64, CassandraTaskListUdt)>> {
            self.rec(&s.text, &v);
            Ok(self.list.lock().unwrap().clone())
        }
        fn select_tasks(
            &self,
            _: &CqlStatement,
            _: BoundValues,
        ) -> ExecResult<Vec<(i64, CassandraTaskUdt, Option<i32>)>> {
            Ok(Vec::new())
        }
        fn select_count(&self, _: &CqlStatement, _: BoundValues) -> ExecResult<Option<i64>> {
            Ok(Some(0))
        }
        fn select_config(&self, _: &CqlStatement, _: BoundValues) -> ExecResult<Option<ConfigRow>> {
            Ok(None)
        }
    }

    fn store(fake: Arc<Fake>) -> Store {
        Store::with_executor(fake, Arc::new(|| false))
    }
    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::nil(),
            name: GoString::from(name),
            task_type: 1,
        }
    }

    #[test]
    fn lease_renewal_failure_keeps_raw_name_bytes_and_go_text() {
        let fake = Arc::new(Fake::default());
        *fake.list.lock().unwrap() = Some((
            5,
            CassandraTaskListUdt {
                domain_id: Uuid::nil(),
                name: "x".into(),
                type_: 1,
                ack_level: 0,
                kind: 0,
                last_updated: chrono::DateTime::from_timestamp(1, 0).unwrap(),
                adaptive_partition_config: None,
            },
        ));
        let err = store(fake).lease(&key(b"a\xff"), 0, 1, 10).unwrap_err();
        let BytesError::ConditionFailed { message } = err else {
            panic!("{err:?}")
        };
        assert_eq!(
            message.as_bytes(),
            &b"leaseTaskList:renew failed: taskList:a\xff, taskListType:1, haveRangeID:1, gotRangeID:5"[..]
        );
    }

    #[test]
    fn create_tasks_binds_raw_names_fences_range_and_rejects_malformed_run_id() {
        let fake = Arc::new(Fake::default());
        let s = store(fake.clone());
        let data = TaskDataBytes {
            workflow_id: GoString::from(&b"w\xff"[..]),
            ..Default::default()
        };
        let task = AllocatedTaskBytes {
            task_id: 7,
            data: data.clone(),
        };
        s.create_tasks_with_domains(&key(b"q\xfe"), 3, 1_000_000_000, &[(Uuid::nil(), &task)])
            .unwrap();
        let calls = fake.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].0.starts_with("INSERT INTO tasks"));
        assert!(
            calls[0].1.iter().any(|v| v.contains("254")),
            "raw 0xfe name bound: {:?}",
            calls[0].1
        );
        assert!(calls[1].0.ends_with("IF range_id = ?"));
        // Lost fence: LWT reports the conflicting range, error is a condition failure with raw bytes.
        *fake.outcome.lock().unwrap() = Some(LwtOutcome {
            applied: false,
            range_id: 9,
        });
        let err = s
            .create_tasks_with_domains(&key(b"q\xfe"), 3, 1, &[])
            .unwrap_err();
        let BytesError::ConditionFailed { message } = err else {
            panic!("{err:?}")
        };
        assert!(message
            .as_bytes()
            .starts_with(b"Failed to insert tasks. name: q\xfe"));
        assert!(!BytesError::ConditionFailed { message }.is_transient());
        // Malformed RunID: nothing is sent.
        fake.calls.lock().unwrap().clear();
        let mut bad = data;
        bad.raw_run_id = Some(GoString::from(&b"not-a-uuid\xff"[..]));
        let bad = AllocatedTaskBytes {
            task_id: 8,
            data: bad,
        };
        let err = s
            .create_tasks_with_domains(&key(b"q"), 3, 1, &[(Uuid::nil(), &bad)])
            .unwrap_err();
        assert!(
            matches!(
                err,
                BytesError::Legacy(MatchingTaskStoreError::BackendConfiguration(_))
            ),
            "{err:?}"
        );
        assert!(fake.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn subsecond_expiry_skips_native_insert_but_keeps_fence() {
        let fake = Arc::new(Fake::default());
        let s = store(fake.clone());
        let mut task = AllocatedTaskBytes {
            task_id: 7,
            data: TaskDataBytes { expiry_nanos: 1_999_999_999, ..Default::default() },
        };
        s.create_tasks_with_domains(&key(b"q\xff"), 3, 1_000_000_000, &[(Uuid::nil(), &task)])
            .unwrap();
        let calls = fake.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].0.ends_with("IF range_id = ?"));
        fake.calls.lock().unwrap().clear();
        task.data.expiry_nanos = 2_000_000_000;
        s.create_tasks_with_domains(&key(b"q\xff"), 3, 1_000_000_000, &[(Uuid::nil(), &task)])
            .unwrap();
        let calls = fake.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].0.ends_with(" USING TTL 1"));
        assert!(calls[1].0.ends_with("IF range_id = ?"));
    }

    #[test]
    fn legacy_and_native_blob_models_cross_convert_losslessly_for_valid_utf8() {
        let data = TaskListData {
            kind: 1,
            ack_level: 4,
            ..Default::default()
        };
        let bytes = TaskListDataBytes::from(data.clone());
        assert_eq!(TaskListData::try_from(bytes).unwrap(), data);
    }
}
