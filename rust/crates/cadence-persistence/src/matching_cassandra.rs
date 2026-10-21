//! Matching task queues on Go's Cassandra `tasks` table.
//!
//! The Scylla session is driven by `CassandraBlockingSession` from Matching's
//! blocking worker boundary. This preserves the synchronous queue contract
//! without running driver futures on the RPC executor.

use crate::{
    cassandra::{CassandraBlockingSession, CassandraTaskListUdt, CassandraTaskUdt},
    matching_blob::{TaskData, TaskListData},
    matching_task_store::{
        AllocatedTask, BackendErrorKind, MatchingTaskStore, MatchingTaskStoreError, Result,
        TaskListKey, TaskListRecord,
    },
};
use chrono::{DateTime, Utc};
use scylla::{
    client::session::Session,
    errors::{DbError, ExecutionError, RequestAttemptError},
    response::query_result::QueryResult,
    statement::{
        batch::{Batch, BatchType},
        Statement,
    },
};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

const LIST_ROW: i32 = 1;
const TASK_ROW: i32 = 0;
const LIST_SENTINEL: i64 = -12345;
const MAX_CASSANDRA_TTL_SECONDS: i64 = 157_680_000;
const SELECT_LIST: &str = "SELECT range_id, task_list FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=?";
const UPDATE_LIST: &str = "UPDATE tasks SET range_id=?, task_list=?, last_updated_time=? WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=? IF range_id=?";
const UPDATE_LIST_TTL: &str = "UPDATE tasks USING TTL 86400 SET range_id=?, task_list=?, last_updated_time=? WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=? IF range_id=?";
const DELETE_LIST: &str = "DELETE FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=? IF range_id=?";
const SELECT_TASKS: &str = "SELECT task_id, task, TTL(task) FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id>? AND task_id<=? LIMIT ?";
fn task_list_lwt_applied(result: QueryResult) -> Result<bool> {
    let rows = result
        .into_rows_result()
        .map_err(CassandraMatchingTaskStore::internal)?;
    let row = rows
        .maybe_first_row::<scylla::value::Row>()
        .map_err(CassandraMatchingTaskStore::internal)?
        .ok_or_else(|| {
            CassandraMatchingTaskStore::internal("Cassandra LWT returned no result row")
        })?;
    match row.first() {
        Some(Some(scylla::value::CqlValue::Boolean(applied))) => Ok(*applied),
        Some(value) => Err(CassandraMatchingTaskStore::internal(format!(
            "Cassandra LWT returned invalid [applied] column: {value:?}"
        ))),
        None => Err(CassandraMatchingTaskStore::internal(
            "Cassandra LWT result row has no [applied] column",
        )),
    }
}

fn is_unavailable(error: &ExecutionError) -> bool {
    matches!(
        error,
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(
            DbError::Unavailable { .. },
            _
        ))
    )
}

pub struct CassandraMatchingTaskStore {
    driver: Arc<CassandraBlockingSession>,
    all_consistency_delete: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl std::fmt::Debug for CassandraMatchingTaskStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CassandraMatchingTaskStore")
            .finish_non_exhaustive()
    }
}

impl CassandraMatchingTaskStore {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self::with_all_consistency_delete(driver, Arc::new(|| false))
    }

    pub fn with_all_consistency_delete(
        driver: Arc<CassandraBlockingSession>,
        enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self {
            driver,
            all_consistency_delete: enabled,
        }
    }

    fn session(&self) -> Arc<Session> {
        self.driver.session()
    }
    fn internal(error: impl std::fmt::Display) -> MatchingTaskStoreError {
        MatchingTaskStoreError::Backend {
            kind: BackendErrorKind::Internal,
            message: error.to_string(),
        }
    }
    fn condition(message: impl Into<String>) -> MatchingTaskStoreError {
        MatchingTaskStoreError::ConditionFailed(message.into())
    }

    fn task_list_udt(key: &TaskListKey, data: &TaskListData) -> Result<CassandraTaskListUdt> {
        let last_updated = timestamp(data.last_updated_nanos)?;
        Ok(CassandraTaskListUdt {
            domain_id: key.domain_id,
            name: key.name.clone(),
            r#type: key.task_type,
            ack_level: data.ack_level,
            kind: i32::from(data.kind),
            last_updated,
            adaptive_partition_config: data.adaptive_partition_config.as_ref().map(|config| {
                use crate::cassandra::{
                    CassandraTaskListPartitionConfigUdt as Config,
                    CassandraTaskListPartitionUdt as Part,
                };
                let convert = |parts: &Option<
                    std::collections::BTreeMap<i32, crate::matching_blob::Partition>,
                >| {
                    parts.as_ref().map(|parts| {
                        parts
                            .iter()
                            .map(|(id, part)| {
                                (
                                    *id,
                                    Part {
                                        isolation_groups: part
                                            .isolation_groups
                                            .as_ref()
                                            .map(|groups| groups.iter().cloned().collect()),
                                    },
                                )
                            })
                            .collect()
                    })
                };
                Config {
                    version: config.version,
                    num_read_partitions: config.num_read_partitions,
                    num_write_partitions: config.num_write_partitions,
                    read_partitions: convert(&config.read_partitions),
                    write_partitions: convert(&config.write_partitions),
                }
            }),
        })
    }

    fn list_data(row: CassandraTaskListUdt) -> Result<TaskListData> {
        let adaptive_partition_config = row.adaptive_partition_config.map(|config| {
            use crate::matching_blob::{Partition, PartitionConfig};
            let convert = |parts: Option<
                std::collections::BTreeMap<i32, crate::cassandra::CassandraTaskListPartitionUdt>,
            >| {
                parts.map(|parts| {
                    parts
                        .into_iter()
                        .map(|(id, part)| {
                            (
                                id,
                                Partition {
                                    isolation_groups: part
                                        .isolation_groups
                                        .map(|groups| groups.into_iter().collect()),
                                },
                            )
                        })
                        .collect()
                })
            };
            PartitionConfig {
                version: config.version,
                num_read_partitions: config.num_read_partitions,
                num_write_partitions: config.num_write_partitions,
                read_partitions: convert(config.read_partitions),
                write_partitions: convert(config.write_partitions),
            }
        });
        Ok(TaskListData {
            kind: row.kind as i16,
            ack_level: row.ack_level,
            expiry_nanos: 0,
            last_updated_nanos: nanos(row.last_updated)?,
            adaptive_partition_config,
        })
    }

    fn task_udt(domain_id: Uuid, data: &TaskData) -> Result<CassandraTaskUdt> {
        let run_id = data
            .run_id
            .as_deref()
            .map(Uuid::from_slice)
            .transpose()
            .map_err(|error| Self::internal(format!("invalid task run ID: {error}")))?;
        Ok(CassandraTaskUdt {
            domain_id,
            workflow_id: data.workflow_id.clone(),
            run_id,
            schedule_id: data.schedule_id,
            created_time: timestamp(data.created_nanos)?,
            partition_config: data.partition_config.as_ref().map(|config| {
                config
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<HashMap<_, _>>()
            }),
        })
    }
}

fn timestamp(nanos: i64) -> Result<DateTime<Utc>> {
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsec = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(seconds, subsec).ok_or_else(|| {
        MatchingTaskStoreError::BackendConfiguration(format!("timestamp out of range: {nanos}"))
    })
}
fn nanos(time: DateTime<Utc>) -> Result<i64> {
    time.timestamp_nanos_opt().ok_or_else(|| {
        MatchingTaskStoreError::BackendConfiguration(format!(
            "Cassandra timestamp outside nanosecond range: {time}"
        ))
    })
}
fn task_from_udt(task_id: i64, task: CassandraTaskUdt, _ttl: Option<i32>) -> Result<AllocatedTask> {
    Ok(AllocatedTask {
        task_id,
        data: TaskData {
            workflow_id: task.workflow_id,
            run_id: task.run_id.map(|id| id.as_bytes().to_vec()),
            schedule_id: task.schedule_id,
            created_nanos: nanos(task.created_time)?,
            // Cassandra TTL expires the row itself. Go's Cassandra
            // persistence adapter reconstructs an expiry from TTL, but
            // nosqlTaskStore.toTaskInfo intentionally does not copy that
            // field into persistence.TaskInfo; Matching therefore relies on
            // Cassandra removing the row instead of dropping a returned row
            // based on a locally reconstructed deadline.
            expiry_nanos: 0,
            partition_config: task
                .partition_config
                .map(|config| config.into_iter().collect()),
        },
    })
}

fn cql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn cql_map(values: &std::collections::BTreeMap<String, String>) -> String {
    format!(
        "{{{}}}",
        values
            .iter()
            .map(|(k, v)| format!("{}: {}", cql_text(k), cql_text(v)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}
fn task_literal(domain: Uuid, data: &TaskData) -> Result<String> {
    let run = data
        .run_id
        .as_deref()
        .map(Uuid::from_slice)
        .transpose()
        .map_err(|error| {
            MatchingTaskStoreError::BackendConfiguration(format!("invalid task run ID: {error}"))
        })?
        .map(|id| id.to_string())
        .unwrap_or_else(|| "null".into());
    let partition = data
        .partition_config
        .as_ref()
        .map(cql_map)
        .unwrap_or_else(|| "null".into());
    Ok(format!("{{domain_id: {domain}, workflow_id: {}, run_id: {run}, schedule_id: {}, created_time: {}, partition_config: {partition}}}",
        cql_text(&data.workflow_id), data.schedule_id, data.created_nanos.div_euclid(1_000_000)))
}
fn task_ttl(data: &TaskData) -> Option<i32> {
    if data.expiry_nanos <= 0 || data.expiry_nanos <= data.created_nanos {
        return None;
    }
    let seconds = (data.expiry_nanos - data.created_nanos) / 1_000_000_000;
    (seconds > 0).then(|| seconds.min(MAX_CASSANDRA_TTL_SECONDS) as i32)
}

fn task_not_expired(data: &TaskData, now_nanos: i64) -> bool {
    data.expiry_nanos <= 0 || data.expiry_nanos > now_nanos
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cassandra_task_ttl_is_not_promoted_to_matching_task_expiry() {
        let created = DateTime::from_timestamp(1_700_000_000, 123_000_000).unwrap();
        let task = CassandraTaskUdt {
            domain_id: Uuid::from_u128(1),
            workflow_id: "ttl-backed-task".into(),
            run_id: None,
            schedule_id: 7,
            created_time: created,
            partition_config: None,
        };

        for ttl in [Some(1), Some(0), None] {
            let decoded = task_from_udt(9, task.clone(), ttl).unwrap();
            assert_eq!(decoded.data.created_nanos, 1_700_000_000_123_000_000);
            assert_eq!(decoded.data.expiry_nanos, 0);
        }
    }

    #[test]
    fn task_udt_literals_escape_go_strings_and_match_ttl_rounding() {
        let task = TaskData {
            workflow_id: "worker's workflow".into(),
            created_nanos: 1_000_000_000,
            expiry_nanos: 3_900_000_000,
            partition_config: Some([("group".to_string(), "blue".to_string())].into()),
            ..Default::default()
        };
        let literal = task_literal(Uuid::from_u128(1), &task).unwrap();
        assert!(literal.contains("workflow_id: 'worker''s workflow'"));
        assert!(literal.contains("partition_config: {'group': 'blue'}"));
        assert_eq!(task_ttl(&task), Some(2), "Go truncates TTL seconds");
        assert!(!task_not_expired(&task, task.expiry_nanos));
        assert!(task_not_expired(&task, task.expiry_nanos - 1));
        let capped = TaskData {
            created_nanos: 1,
            expiry_nanos: i64::MAX,
            ..Default::default()
        };
        assert_eq!(
            task_ttl(&capped),
            Some(MAX_CASSANDRA_TTL_SECONDS as i32),
            "Go caps task TTL at five years",
        );
        let subsecond = TaskData {
            expiry_nanos: 1_900_000_000,
            ..task
        };
        assert_eq!(task_ttl(&subsecond), None);
    }

    #[test]
    fn task_list_partition_udt_round_trips_go_field_values() {
        let key = TaskListKey {
            domain_id: Uuid::from_u128(11),
            name: "sticky".into(),
            task_type: 1,
        };
        let data = TaskListData {
            kind: 1,
            ack_level: 73,
            last_updated_nanos: 1_700_000_000_123_000_000,
            adaptive_partition_config: Some(crate::matching_blob::PartitionConfig {
                version: 9,
                num_read_partitions: 1,
                num_write_partitions: 1,
                read_partitions: Some(
                    [(
                        0,
                        crate::matching_blob::Partition {
                            isolation_groups: Some(vec!["blue".into()]),
                        },
                    )]
                    .into(),
                ),
                write_partitions: Some(
                    [(
                        0,
                        crate::matching_blob::Partition {
                            isolation_groups: Some(vec!["blue".into()]),
                        },
                    )]
                    .into(),
                ),
            }),
            ..Default::default()
        };
        let udt = CassandraMatchingTaskStore::task_list_udt(&key, &data).unwrap();
        assert_eq!(udt.r#type, 1);
        assert_eq!(udt.adaptive_partition_config.as_ref().unwrap().version, 9);
        assert_eq!(CassandraMatchingTaskStore::list_data(udt).unwrap(), data);
    }

    /// Cross-language probe: the Cassandra harness seeds this row from Go,
    /// runs this test, then asks Go to verify Rust's write.
    #[test]
    #[ignore = "requires tests/run-matching-cassandra-interop.py"]
    fn go_seed_rust_write_restart_go_verify() {
        let hosts = std::env::var("CADENCE_CASSANDRA_CONTACT_POINTS")
            .expect("harness sets contact points")
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let port = std::env::var("CADENCE_CASSANDRA_PORT")
            .ok()
            .and_then(|port| port.parse::<i32>().ok())
            .unwrap_or(9042);
        let config = crate::cassandra::CassandraSessionConfig::from_parts(
            hosts,
            port,
            std::env::var("CADENCE_CASSANDRA_USER").unwrap_or_default(),
            std::env::var("CADENCE_CASSANDRA_PASSWORD").unwrap_or_default(),
            std::env::var("CADENCE_CASSANDRA_KEYSPACE").expect("harness sets keyspace"),
        )
        .expect("valid Cassandra harness config");
        let domain_id = Uuid::parse_str(
            &std::env::var("CADENCE_CASSANDRA_TEST_DOMAIN_ID").expect("harness sets domain id"),
        )
        .expect("valid domain id");
        let key = TaskListKey {
            domain_id,
            name: std::env::var("CADENCE_CASSANDRA_TEST_TASK_LIST")
                .expect("harness sets task list"),
            task_type: 1,
        };
        let driver = Arc::new(
            CassandraBlockingSession::connect(&config).expect("connect to Go-seeded Cassandra"),
        );
        let store = CassandraMatchingTaskStore::new(driver.clone());
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 41);
        let now = Utc::now()
            .timestamp_nanos_opt()
            .expect("current timestamp range");
        let lease = store.lease(&key, 0, 41, now).unwrap();
        assert_eq!(lease.range_id, 42);
        let task = AllocatedTask {
            task_id: 101,
            data: TaskData {
                workflow_id: "rust-cassandra-interop".into(),
                schedule_id: 23,
                created_nanos: now - 1_000_000_000,
                ..Default::default()
            },
        };
        store
            .create_tasks_with_domains(&key, 42, now, &[(domain_id, &task)])
            .unwrap();
        drop(store);
        drop(driver);

        let restarted = Arc::new(
            CassandraBlockingSession::connect(&config)
                .expect("reconnect after persistence restart"),
        );
        let recovered = CassandraMatchingTaskStore::new(restarted);
        let tasks = recovered.get_tasks(&key, 100, 101, 10).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].data.workflow_id, "rust-cassandra-interop");
        assert_eq!(tasks[0].data.schedule_id, 23);
    }
}

impl MatchingTaskStore for CassandraMatchingTaskStore {
    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord> {
        let session = self.session();
        let result = self
            .driver
            .run(session.query_unpaged(
                SELECT_LIST,
                (
                    key.domain_id,
                    &key.name,
                    key.task_type,
                    LIST_ROW,
                    LIST_SENTINEL,
                ),
            ))
            .map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        let Some((range_id, data)) = rows
            .maybe_first_row::<(i64, CassandraTaskListUdt)>()
            .map_err(Self::internal)?
        else {
            return Err(MatchingTaskStoreError::NotFound);
        };
        Ok(TaskListRecord {
            range_id,
            data: Self::list_data(data)?,
        })
    }

    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecord> {
        let current = match self.get_task_list(key) {
            Ok(row) => row,
            Err(MatchingTaskStoreError::NotFound) => {
                let data = TaskListData {
                    kind,
                    last_updated_nanos: now_nanos,
                    ..Default::default()
                };
                let udt = Self::task_list_udt(key, &data)?;
                let session = self.session();
                let result = self.driver.run(session.query_unpaged(
                    "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, range_id, task_list, created_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
                    (key.domain_id, &key.name, key.task_type, LIST_ROW, LIST_SENTINEL, 1_i64, udt, timestamp(now_nanos)?)))
                    .map_err(Self::internal)?;
                let applied = task_list_lwt_applied(result)?;
                if applied {
                    return Ok(TaskListRecord { range_id: 1, data });
                }
                self.get_task_list(key)?
            }
            Err(error) => return Err(error),
        };
        if previous_range > 0 && previous_range != current.range_id {
            return Err(Self::condition(format!("leaseTaskList:renew failed:taskList:{}, taskListType:{}, haveRangeID:0, gotRangeID:{}", key.name, key.task_type, current.range_id)));
        }
        let next = current.range_id.wrapping_add(1);
        let mut data = current.data;
        data.last_updated_nanos = now_nanos;
        let udt = Self::task_list_udt(key, &data)?;
        let session = self.session();
        let result = self
            .driver
            .run(session.query_unpaged(
                UPDATE_LIST,
                (
                    next,
                    udt,
                    timestamp(now_nanos)?,
                    key.domain_id,
                    &key.name,
                    key.task_type,
                    LIST_ROW,
                    LIST_SENTINEL,
                    current.range_id,
                ),
            ))
            .map_err(Self::internal)?;
        if !task_list_lwt_applied(result)? {
            return Err(Self::condition(format!(
                "task list {} lease range changed",
                key.name
            )));
        }
        data.kind = kind;
        Ok(TaskListRecord {
            range_id: next,
            data,
        })
    }

    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        mut data: TaskListData,
        now_nanos: i64,
    ) -> Result<()> {
        data.last_updated_nanos = now_nanos;
        let udt = Self::task_list_udt(key, &data)?;
        let session = self.session();
        let result = if matches!(data.kind, 1 | 2) {
            let mut batch = Batch::new(BatchType::Logged);
            batch.append_statement(Statement::new(
                "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, created_time) VALUES (?, ?, ?, ?, ?, ?) USING TTL 86400",
            ));
            batch.append_statement(Statement::new(UPDATE_LIST_TTL));
            self.driver
                .run(session.batch(
                    &batch,
                    (
                        (
                            key.domain_id,
                            &key.name,
                            key.task_type,
                            LIST_ROW,
                            LIST_SENTINEL,
                            timestamp(now_nanos)?,
                        ),
                        (
                            range,
                            udt,
                            timestamp(now_nanos)?,
                            key.domain_id,
                            &key.name,
                            key.task_type,
                            LIST_ROW,
                            LIST_SENTINEL,
                            range,
                        ),
                    ),
                ))
                .map_err(Self::internal)?
        } else {
            self.driver
                .run(session.query_unpaged(
                    UPDATE_LIST,
                    (
                        range,
                        udt,
                        timestamp(now_nanos)?,
                        key.domain_id,
                        &key.name,
                        key.task_type,
                        LIST_ROW,
                        LIST_SENTINEL,
                        range,
                    ),
                ))
                .map_err(Self::internal)?
        };
        if task_list_lwt_applied(result)? {
            Ok(())
        } else {
            Err(Self::condition(format!(
                "task list {} range changed",
                key.name
            )))
        }
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> Result<()> {
        // Go uses one logged conditional batch so task inserts and the lease
        // fence commit together. Literal UDT values keep each statement's
        // shape exact while avoiding heterogeneous bind-row serialization.
        let mut batch = Batch::new(BatchType::Logged);
        let mut inserted = 0;
        for (_, task) in tasks {
            if !task_not_expired(&task.data, now_nanos) {
                continue;
            }
            // Go stamps every inserted row with the CreateTasks operation time;
            // its TTL is computed from that same time, even when re-adding a task.
            let mut data = task.data.clone();
            data.created_nanos = now_nanos;
            let ttl = task_ttl(&data)
                .map(|ttl| format!(" USING TTL {ttl}"))
                .unwrap_or_default();
            batch.append_statement(Statement::new(format!(
                "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, task, created_time) VALUES ({}, {}, {}, {}, {}, {}{})",
                key.domain_id, cql_text(&key.name), key.task_type, TASK_ROW, task.task_id,
                task_literal(key.domain_id, &data)?, data.created_nanos.div_euclid(1_000_000)
            ) + &ttl));
            inserted += 1;
        }
        batch.append_statement(Statement::new(format!(
            "UPDATE tasks SET range_id={}, last_updated_time={} WHERE domain_id={} AND task_list_name={} AND task_list_type={} AND type={} AND task_id={} IF range_id={}",
            range, now_nanos.div_euclid(1_000_000), key.domain_id, cql_text(&key.name), key.task_type, LIST_ROW, LIST_SENTINEL, range)));
        let values = std::iter::repeat(()).take(inserted + 1).collect::<Vec<_>>();
        let session = self.session();
        let result = self
            .driver
            .run(session.batch(&batch, values))
            .map_err(Self::internal)?;
        if task_list_lwt_applied(result)? {
            Ok(())
        } else {
            Err(Self::condition(format!(
                "task list {} lease range changed",
                key.name
            )))
        }
    }

    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>> {
        if batch_size <= 0 {
            return Ok(Vec::new());
        }
        let cql_batch_size = i32::try_from(batch_size).map_err(|_| {
            MatchingTaskStoreError::BackendConfiguration(format!(
                "task batch size exceeds Cassandra limit: {batch_size}"
            ))
        })?;
        let session = self.session();
        let result = self
            .driver
            .run(session.query_unpaged(
                SELECT_TASKS,
                (
                    key.domain_id,
                    &key.name,
                    key.task_type,
                    TASK_ROW,
                    read_level,
                    max_read_level,
                    cql_batch_size,
                ),
            ))
            .map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        let mut tasks = Vec::new();
        for row in rows
            .rows::<(i64, CassandraTaskUdt, Option<i32>)>()
            .map_err(Self::internal)?
        {
            let (id, task, ttl) = row.map_err(Self::internal)?;
            tasks.push(task_from_udt(id, task, ttl)?);
            if tasks.len() >= batch_size as usize {
                break;
            }
        }
        Ok(tasks)
    }

    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64> {
        let session = self.session();
        let result = self.driver.run(session.query_unpaged(
            "SELECT count(1) FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id>?",
            (key.domain_id, &key.name, key.task_type, TASK_ROW, ack_level))).map_err(Self::internal)?;
        result
            .into_rows_result()
            .map_err(Self::internal)?
            .maybe_first_row::<(i64,)>()
            .map_err(Self::internal)?
            .map(|(count,)| count)
            .ok_or_else(|| Self::internal("Cassandra count returned no row"))
    }

    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()> {
        let session = self.session();
        self.driver.run(session.query_unpaged(
            "DELETE FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=?",
            (key.domain_id, &key.name, key.task_type, TASK_ROW, task_id))).map_err(Self::internal)?;
        Ok(())
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        _limit: i64,
    ) -> Result<usize> {
        let session = self.session();
        let count = self.driver.run(session.query_unpaged(
            "SELECT count(1) FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id>? AND task_id<=?",
            (key.domain_id, &key.name, key.task_type, TASK_ROW, 0_i64, inclusive_task_id))).map_err(Self::internal)?
            .into_rows_result().map_err(Self::internal)?
            .maybe_first_row::<(i64,)>().map_err(Self::internal)?
            .map(|(count,)| usize::try_from(count).map_err(Self::internal))
            .ok_or_else(|| Self::internal("Cassandra count returned no row"))?;
        let count = count?;
        const DELETE: &str = "DELETE FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id>? AND task_id<=?";
        let params = (
            key.domain_id,
            &key.name,
            key.task_type,
            TASK_ROW,
            0_i64,
            inclusive_task_id,
        );
        if (self.all_consistency_delete)() {
            let mut all = Statement::new(DELETE);
            all.set_consistency(scylla::frame::types::Consistency::All);
            match self.driver.run(session.query_unpaged(all, params)) {
                Ok(_) => {}
                Err(error) if is_unavailable(&error) => {
                    let mut fallback = Statement::new(DELETE);
                    fallback.set_consistency(self.driver.default_consistency());
                    self.driver
                        .run(session.query_unpaged(fallback, params))
                        .map_err(Self::internal)?;
                }
                Err(error) => return Err(Self::internal(error)),
            }
        } else {
            self.driver
                .run(session.query_unpaged(DELETE, params))
                .map_err(Self::internal)?;
        }
        Ok(count)
    }

    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()> {
        let session = self.session();
        let params = (
            key.domain_id,
            &key.name,
            key.task_type,
            LIST_ROW,
            LIST_SENTINEL,
            range,
        );
        let result = if (self.all_consistency_delete)() {
            let mut all_consistency = Statement::new(DELETE_LIST);
            all_consistency.set_consistency(scylla::frame::types::Consistency::All);
            match self
                .driver
                .run(session.query_unpaged(all_consistency, params))
            {
                Ok(result) => result,
                Err(error) if is_unavailable(&error) => {
                    let mut fallback = Statement::new(DELETE_LIST);
                    fallback.set_consistency(self.driver.default_consistency());
                    self.driver
                        .run(session.query_unpaged(fallback, params))
                        .map_err(Self::internal)?
                }
                Err(error) => return Err(Self::internal(error)),
            }
        } else {
            self.driver
                .run(session.query_unpaged(DELETE_LIST, params))
                .map_err(Self::internal)?
        };
        if task_list_lwt_applied(result)? {
            Ok(())
        } else {
            Err(MatchingTaskStoreError::DeleteCount(0))
        }
    }
}
