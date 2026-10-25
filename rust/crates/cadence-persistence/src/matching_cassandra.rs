//! Matching task queues on Go's Cassandra `tasks` table.
//!
//! The Scylla session is driven by `CassandraBlockingSession` from Matching's
//! blocking worker boundary. This preserves the synchronous queue contract
//! without running driver futures on the RPC executor.

use crate::{
    cassandra::{CassandraBlockingSession, CassandraTaskListUdt, CassandraTaskUdt},
    matching_blob::{TaskData, TaskListData},
    matching_cassandra_exec::{
        bound, BoundValues, CqlExecutor, CqlStatement, ExecFailure, ScyllaExecutor,
    },
    matching_task_store::{
        AllocatedTask, BackendErrorKind, MatchingTaskStore, MatchingTaskStoreError, Result,
        TaskListKey, TaskListRecord,
    },
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use uuid::Uuid;

const LIST_ROW: i32 = 1;
const TASK_ROW: i32 = 0;
const LIST_SENTINEL: i64 = -12345;
/// Go `initialRangeID` (nosql_task_store.go:44): a new task list starts at 1.
const INITIAL_RANGE_ID: i64 = 1;
/// Go's `cassandraDefaultConsLevel`: the retry level after an ALL delete hits
/// UNAVAILABLE. It is a constant in Go, not the configured `consistency`.
const FALLBACK_CONSISTENCY: scylla::frame::types::Consistency =
    scylla::frame::types::Consistency::LocalQuorum;
const MAX_CASSANDRA_TTL_SECONDS: i64 = 157_680_000;
const SELECT_LIST: &str = "SELECT range_id, task_list FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=?";
const SELECT_CONFIG: &str =
    "SELECT version, values, encoding FROM cluster_config WHERE row_type = ? LIMIT 1";
const INSERT_TASK_LIST_TTL: &str = "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, created_time) VALUES (?, ?, ?, ?, ?, ?) USING TTL 86400";
const UPDATE_LIST: &str = "UPDATE tasks SET range_id=?, task_list=?, last_updated_time=? WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=? IF range_id=?";
const UPDATE_LIST_TTL: &str = "UPDATE tasks USING TTL 86400 SET range_id=?, task_list=?, last_updated_time=? WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=? IF range_id=?";
const DELETE_LIST: &str = "DELETE FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=? IF range_id=?";
const RANGE_DELETE_TASKS: &str = "DELETE FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_list_type = ? AND type = ? AND task_id > ? AND task_id <= ?";
const INSERT_TASK_LIST: &str = "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, range_id, task_list, created_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
const COUNT_TASKS: &str = "SELECT count(1) as count FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_list_type = ? AND type = ? AND task_id > ?";
const SELECT_TASKS: &str = "SELECT task_id, task, TTL(task) FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id>? AND task_id<=? LIMIT ?";
pub struct CassandraMatchingTaskStore {
    executor: Arc<dyn CqlExecutor>,
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
        Self::with_executor(Arc::new(ScyllaExecutor::new(driver)), enabled)
    }

    /// Build the store over any statement executor (the recording test double
    /// in tests; the Scylla session in production).
    pub fn with_executor(
        executor: Arc<dyn CqlExecutor>,
        enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self {
            executor,
            all_consistency_delete: enabled,
        }
    }

    fn internal(error: impl std::fmt::Display + 'static) -> MatchingTaskStoreError {
        let kind = match (&error as &dyn std::any::Any).downcast_ref::<ExecFailure>() {
            Some(error) if error.is_unavailable() => BackendErrorKind::DbUnavailable,
            _ => BackendErrorKind::Internal,
        };
        // Go nosql `convertCommonErrors`: gocql unavailability becomes
        // `DBUnavailableError`, everything else unclassified is internal.
        MatchingTaskStoreError::Backend {
            kind,
            message: error.to_string(),
        }
    }
    fn condition(message: impl Into<String>) -> MatchingTaskStoreError {
        MatchingTaskStoreError::ConditionFailed(message.into())
    }

    fn range_delete_values(
        key: &TaskListKey,
        min_exclusive: i64,
        max_inclusive: i64,
    ) -> BoundValues {
        vec![
            bound(key.domain_id),
            bound(key.name.clone()),
            bound(key.task_type),
            bound(TASK_ROW),
            bound(min_exclusive),
            bound(max_inclusive),
        ]
    }

    /// Bound values of `UPDATE_LIST`/`UPDATE_LIST_TTL`, in statement order.
    fn update_list_values(
        key: &TaskListKey,
        range_id: i64,
        udt: CassandraTaskListUdt,
        now_nanos: i64,
        condition_range: i64,
    ) -> Result<BoundValues> {
        Ok(vec![
            bound(range_id),
            bound(udt),
            bound(timestamp(now_nanos)?),
            bound(key.domain_id),
            bound(key.name.clone()),
            bound(key.task_type),
            bound(LIST_ROW),
            bound(LIST_SENTINEL),
            bound(condition_range),
        ])
    }

    /// Go `executeWithConsistencyAll`: with the dynamic flag set, delete at
    /// ALL and retry at LOCAL_QUORUM when ALL is unavailable.
    fn range_delete_tasks(
        &self,
        key: &TaskListKey,
        min_exclusive: i64,
        max_inclusive: i64,
    ) -> Result<()> {
        let values = || Self::range_delete_values(key, min_exclusive, max_inclusive);
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
                    self.executor
                        .exec(&fallback, values())
                        .map_err(Self::internal)?;
                }
                Err(error) => return Err(Self::internal(error)),
            }
        } else {
            self.executor
                .exec(&CqlStatement::new(RANGE_DELETE_TASKS), values())
                .map_err(Self::internal)?;
        }
        Ok(())
    }

    fn task_list_udt(key: &TaskListKey, data: &TaskListData) -> Result<CassandraTaskListUdt> {
        let last_updated = timestamp(data.last_updated_nanos)?;
        Ok(CassandraTaskListUdt {
            domain_id: key.domain_id,
            name: key.name.clone(),
            type_: key.task_type,
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
            raw_run_id: None,
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
/// Go `templateCreateTaskQuery` / `templateCreateTaskWithTTLQuery`
/// (cassandra/tasks_cql.go:44-50) with literal values; TTL is appended when
/// the task carries a positive schedule-to-start timeout.
fn insert_task_statement(key: &TaskListKey, task_id: i64, data: &TaskData) -> Result<String> {
    let ttl = task_ttl(data)
        .map(|ttl| format!(" USING TTL {ttl}"))
        .unwrap_or_default();
    Ok(format!(
        "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, task, created_time) VALUES ({}, {}, {}, {}, {}, {}, {}){}",
        key.domain_id,
        cql_text(&key.name),
        key.task_type,
        TASK_ROW,
        task_id,
        task_literal(key.domain_id, data)?,
        data.created_nanos.div_euclid(1_000_000),
        ttl
    ))
}

/// Go `templateUpdateTaskListRangeIDQuery` (tasks_cql.go:127-135), the
/// conditional statement that fences the task batch on the lease range.
fn range_fence_statement(key: &TaskListKey, range: i64, now_nanos: i64) -> String {
    format!(
        "UPDATE tasks SET range_id = {}, last_updated_time = {} WHERE domain_id = {} AND task_list_name = {} AND task_list_type = {} AND type = {} AND task_id = {} IF range_id = {}",
        range,
        now_nanos.div_euclid(1_000_000),
        key.domain_id,
        cql_text(&key.name),
        key.task_type,
        LIST_ROW,
        LIST_SENTINEL,
        range
    )
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


impl MatchingTaskStore for CassandraMatchingTaskStore {
    fn fetch_latest_config(
        &self,
        row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        // Go cassandra templateSelectLatestConfig: `version` is a DESC
        // clustering key, so LIMIT 1 is the latest and is stored positive.
        let row = self
            .executor
            .select_config(&CqlStatement::new(SELECT_CONFIG), vec![bound(row_type)])
            .map_err(|e| e.to_string())?;
        Ok(row.map(|(version, data, encoding)| crate::config_store::ConfigStoreRow {
            version,
            data: data.unwrap_or_default(),
            encoding: encoding.unwrap_or_default(),
        }))
    }

    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord> {
        let Some((range_id, data)) = self
            .executor
            .select_task_list(
                &CqlStatement::new(SELECT_LIST),
                vec![
                    bound(key.domain_id),
                    bound(key.name.clone()),
                    bound(key.task_type),
                    bound(LIST_ROW),
                    bound(LIST_SENTINEL),
                ],
            )
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
                let outcome = self
                    .executor
                    .exec_lwt(
                        &CqlStatement::new(INSERT_TASK_LIST),
                        vec![
                            bound(key.domain_id),
                            bound(key.name.clone()),
                            bound(key.task_type),
                            bound(LIST_ROW),
                            bound(LIST_SENTINEL),
                            bound(INITIAL_RANGE_ID),
                            bound(udt),
                            bound(timestamp(now_nanos)?),
                        ],
                    )
                    .map_err(Self::internal)?;
                if outcome.applied {
                    return Ok(TaskListRecord {
                        range_id: INITIAL_RANGE_ID,
                        data,
                    });
                }
                // Go (nosql_task_store.go:127-135) does not retry after losing
                // the IF NOT EXISTS race; it reports a condition failure.
                return Err(Self::condition(format!(
                    "leaseTaskList: taskList:{}, taskListType:{}, haveRangeID:{}, gotRangeID:{}",
                    key.name, key.task_type, INITIAL_RANGE_ID, outcome.range_id
                )));
            }
            Err(error) => return Err(error),
        };
        if previous_range > 0 && previous_range != current.range_id {
            return Err(Self::condition(format!("leaseTaskList:renew failed: taskList:{}, taskListType:{}, haveRangeID:{}, gotRangeID:{}", key.name, key.task_type, previous_range, current.range_id)));
        }
        let next = current.range_id.wrapping_add(1);
        let mut data = current.data;
        data.last_updated_nanos = now_nanos;
        let udt = Self::task_list_udt(key, &data)?;
        let outcome = self
            .executor
            .exec_lwt(
                &CqlStatement::new(UPDATE_LIST),
                Self::update_list_values(key, next, udt, now_nanos, current.range_id)?,
            )
            .map_err(Self::internal)?;
        if !outcome.applied {
            return Err(Self::condition(format!(
                "leaseTaskList: taskList:{}, taskListType:{}, haveRangeID:{}, gotRangeID:{}",
                key.name, key.task_type, next, outcome.range_id
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
        let outcome = if matches!(data.kind, 1 | 2) {
            self.executor
                .batch_lwt(
                    &[
                        CqlStatement::new(INSERT_TASK_LIST_TTL),
                        CqlStatement::new(UPDATE_LIST_TTL),
                    ],
                    vec![
                        vec![
                            bound(key.domain_id),
                            bound(key.name.clone()),
                            bound(key.task_type),
                            bound(LIST_ROW),
                            bound(LIST_SENTINEL),
                            bound(timestamp(now_nanos)?),
                        ],
                        Self::update_list_values(key, range, udt, now_nanos, range)?,
                    ],
                )
                .map_err(Self::internal)?
        } else {
            self.executor
                .exec_lwt(
                    &CqlStatement::new(UPDATE_LIST),
                    Self::update_list_values(key, range, udt, now_nanos, range)?,
                )
                .map_err(Self::internal)?
        };
        if outcome.applied {
            Ok(())
        } else {
            Err(Self::condition(format!(
                "Failed to update task list. name: {}, type: {}, rangeID: {}, columns: (range_id={})",
                key.name, key.task_type, range, outcome.range_id
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
        let mut statements = Vec::new();
        for (_, task) in tasks {
            if !task_not_expired(&task.data, now_nanos) {
                continue;
            }
            // Go stamps every inserted row with the CreateTasks operation time;
            // its TTL is computed from that same time, even when re-adding a task.
            let mut data = task.data.clone();
            data.created_nanos = now_nanos;
            statements.push(CqlStatement::new(insert_task_statement(
                key,
                task.task_id,
                &data,
            )?));
        }
        statements.push(CqlStatement::new(range_fence_statement(
            key, range, now_nanos,
        )));
        let values = statements.iter().map(|_| Vec::new()).collect();
        let outcome = self
            .executor
            .batch_lwt(&statements, values)
            .map_err(Self::internal)?;
        if outcome.applied {
            Ok(())
        } else {
            Err(Self::condition(format!(
                "Failed to insert tasks. name: {}, type: {}, rangeID: {}, columns: (range_id={})",
                key.name, key.task_type, range, outcome.range_id
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
        let rows = self
            .executor
            .select_tasks(
                &CqlStatement::new(SELECT_TASKS),
                vec![
                    bound(key.domain_id),
                    bound(key.name.clone()),
                    bound(key.task_type),
                    bound(TASK_ROW),
                    bound(read_level),
                    bound(max_read_level),
                    bound(cql_batch_size),
                ],
            )
            .map_err(Self::internal)?;
        let mut tasks = Vec::new();
        for (id, task, ttl) in rows {
            tasks.push(task_from_udt(id, task, ttl)?);
            if tasks.len() >= batch_size as usize {
                break;
            }
        }
        Ok(tasks)
    }

    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64> {
        self.executor
            .select_count(
                &CqlStatement::new(COUNT_TASKS),
                vec![
                    bound(key.domain_id),
                    bound(key.name.clone()),
                    bound(key.task_type),
                    bound(TASK_ROW),
                    bound(ack_level),
                ],
            )
            .map_err(Self::internal)?
            .ok_or_else(|| Self::internal("Cassandra count returned no row"))
    }

    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()> {
        // Go CompleteTask issues the same range delete as CompleteTasksLessThan
        // with exclusive min `taskID-1` and inclusive max `taskID`
        // (nosql_task_store.go:420-440), so it honors the ALL-consistency flag.
        self.range_delete_tasks(key, task_id.wrapping_sub(1), task_id)
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        _limit: i64,
    ) -> Result<usize> {
        // Go (tasks.go RangeDeleteTasks) deletes the whole range without a
        // COUNT query and reports `persistence.UnknownNumRowsAffected` (-1),
        // which task_gc.go treats as "fewer than a full batch": a single
        // delete drains everything. Zero is the usize equivalent.
        self.range_delete_tasks(key, 0, inclusive_task_id)?;
        Ok(0)
    }

    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()> {
        let values = || -> BoundValues {
            vec![
                bound(key.domain_id),
                bound(key.name.clone()),
                bound(key.task_type),
                bound(LIST_ROW),
                bound(LIST_SENTINEL),
                bound(range),
            ]
        };
        let outcome = if (self.all_consistency_delete)() {
            let all_consistency = CqlStatement::with_consistency(
                DELETE_LIST,
                scylla::frame::types::Consistency::All,
            );
            match self.executor.exec_lwt(&all_consistency, values()) {
                Ok(outcome) => outcome,
                Err(error) if error.is_unavailable() => {
                    let fallback =
                        CqlStatement::with_consistency(DELETE_LIST, FALLBACK_CONSISTENCY);
                    self.executor
                        .exec_lwt(&fallback, values())
                        .map_err(Self::internal)?
                }
                Err(error) => return Err(Self::internal(error)),
            }
        } else {
            self.executor
                .exec_lwt(&CqlStatement::new(DELETE_LIST), values())
                .map_err(Self::internal)?
        };
        if outcome.applied {
            Ok(())
        } else {
            Err(Self::condition(format!(
                "Failed to delete task list. name: {}, type: {}, rangeID: {}, columns: (range_id={})",
                key.name, key.task_type, range, outcome.range_id
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    const GO_UDT: &str = "{domain_id: ?, name: ?, type: ?, ack_level: ?, kind: ?, last_updated: ?, adaptive_partition_config: ? }";
    const GO_TASK_UDT: &str = "{domain_id: ?, workflow_id: ?, run_id: ?, schedule_id: ?, created_time: ?, partition_config: ? }";

    /// Go builds several statements by concatenating the named UDT templates
    /// `templateTaskListType` and `templateTaskType`, so the statement text is
    /// not contiguous in tasks_cql.go. Each template's literal must exist in Go
    /// and is substituted back in, giving the statement Go actually sends.
    fn go_composed(go: &str) -> String {
        use crate::matching_task_store::go_sql_pin::{assert_in_go_ci, strip};
        assert_in_go_ci(go, GO_UDT);
        assert_in_go_ci(go, GO_TASK_UDT);
        strip(go)
            .to_ascii_lowercase()
            .replace("templatetasklisttype", &strip(GO_UDT).to_ascii_lowercase())
            .replace("templatetasktype", &strip(GO_TASK_UDT).to_ascii_lowercase())
    }

    /// Statements bound with a single UDT parameter must equal Go's CQL with
    /// the inline UDT template (tasks_cql.go:25-33) replaced by `?`.
    #[test]
    fn cassandra_cql_matches_go_tasks_cql() {
        use crate::matching_task_store::go_sql_pin::{assert_in_go_ci, strip};
        let go = include_str!(
            "../../../../go/common/persistence/nosql/nosqlplugin/cassandra/tasks_cql.go"
        );
        for statement in [
            SELECT_LIST,
            DELETE_LIST,
            RANGE_DELETE_TASKS,
            COUNT_TASKS,
        ] {
            assert_in_go_ci(go, statement);
        }
        // templateGetTasksQuery adds only an alias; the Rust reader appends the
        // page limit that Go passes as a gocql page size.
        assert_in_go_ci(
            go,
            &SELECT_TASKS
                .replace("TTL(task)", "TTL(task) AS ttl")
                .replace(" LIMIT ?", ""),
        );
        // templateInsertTaskListQuery / templateUpdateTaskListQuery with the
        // UDT literal replaced by one bound UDT parameter.
        let go_stripped = go_composed(go);
        let udt = strip(GO_UDT).to_ascii_lowercase();
        for statement in [INSERT_TASK_LIST, UPDATE_LIST] {
            let wanted = strip(statement).to_ascii_lowercase();
            // Expand the single `task_list`-position `?` back into Go's UDT.
            let expanded = if statement == INSERT_TASK_LIST {
                wanted.replacen("?,?,?,?,?,?,?,?)", &format!("?,?,?,?,?,?,{udt},?)"), 1)
            } else {
                wanted.replacen("task_list=?", &format!("task_list={udt}"), 1)
            };
            assert!(go_stripped.contains(&expanded), "{statement}");
        }
        // Part 2 of the TTL update: Go binds the TTL, Rust inlines 86400.
        assert!(go_stripped.contains(
            &strip(&UPDATE_LIST_TTL.replace("USING TTL 86400", "USING TTL ?").replacen(
                "task_list=?",
                &format!("task_list={}", GO_UDT),
                1
            ))
            .to_ascii_lowercase()
        ));
    }

    #[test]
    fn cassandra_task_statements_have_go_shape() {
        use crate::matching_task_store::go_sql_pin::assert_in_go_ci;
        let go = include_str!(
            "../../../../go/common/persistence/nosql/nosqlplugin/cassandra/tasks_cql.go"
        );
        let key = TaskListKey {
            domain_id: Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888),
            name: "tl'x".into(),
            task_type: 1,
        };
        let run = Uuid::from_u128(7);
        let plain = TaskData {
            workflow_id: "wf".into(),
            run_id: Some(run.as_bytes().to_vec()),
            raw_run_id: None,
            schedule_id: 9,
            created_nanos: 1_700_000_000_123_456_789,
            expiry_nanos: crate::matching_blob::GO_ZERO_TIME_NANOS,
            partition_config: Some([("k".to_string(), "v".to_string())].into()),
        };
        let statement = insert_task_statement(&key, 42, &plain).unwrap();
        assert_eq!(
            statement,
            "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, task, created_time) VALUES (11112222-3333-4444-5555-666677778888, 'tl''x', 1, 0, 42, {domain_id: 11112222-3333-4444-5555-666677778888, workflow_id: 'wf', run_id: 00000000-0000-0000-0000-000000000007, schedule_id: 9, created_time: 1700000000123, partition_config: {'k': 'v'}}, 1700000000123)"
        );
        // Column list and UDT field order are Go's templateTaskType.
        assert_in_go_ci(&go_composed(go), "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, task, created_time) VALUES(?, ?, ?, ?, ?, {domain_id: ?, workflow_id: ?, run_id: ?, schedule_id: ?, created_time: ?, partition_config: ? }, ?)");
        // Positive timeout appends Go's TTL clause.
        let with_ttl = TaskData {
            expiry_nanos: plain.created_nanos + 30_000_000_000,
            ..plain.clone()
        };
        assert!(insert_task_statement(&key, 42, &with_ttl)
            .unwrap()
            .ends_with(" USING TTL 30"));
        assert_in_go_ci(go, "USING TTL ?");
        // Lease fence batch member.
        assert_eq!(
            range_fence_statement(&key, 5, 1_700_000_000_123_456_789),
            "UPDATE tasks SET range_id = 5, last_updated_time = 1700000000123 WHERE domain_id = 11112222-3333-4444-5555-666677778888 AND task_list_name = 'tl''x' AND task_list_type = 1 AND type = 1 AND task_id = -12345 IF range_id = 5"
        );
        assert_in_go_ci(go, "UPDATE tasks SET range_id = ?, last_updated_time = ? WHERE domain_id = ? and task_list_name = ? and task_list_type = ? and type = ? and task_id = ? IF range_id = ?");
    }

    /// Go tasks_test.go TestInsertTasks "successfully applied" (:757-794):
    /// TTL 0 uses the plain insert, a TTL above maxCassandraTTL is capped at
    /// 157680000, and the lease fence is the last batch member.
    #[test]
    fn go_insert_tasks_batch_ttl_variants_and_fence() {
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "tasklist1".into(),
            task_type: 1,
        };
        let base_nanos = 1_711_922_921_000_000_000_i64; // 2024-04-01T22:08:41Z
        let plain = TaskData {
            workflow_id: "wid1".into(),
            schedule_id: 42,
            created_nanos: base_nanos,
            expiry_nanos: 0,
            ..Default::default()
        };
        let capped = TaskData {
            workflow_id: "wid1".into(),
            schedule_id: 43,
            created_nanos: base_nanos + 1_000_000_000,
            expiry_nanos: base_nanos + 1_000_000_000 + (157_680_001_i64 * 1_000_000_000),
            ..Default::default()
        };
        let first = insert_task_statement(&key, 3, &plain).unwrap();
        let second = insert_task_statement(&key, 4, &capped).unwrap();
        assert!(!first.contains("USING TTL"), "TTL 0 uses the plain insert");
        assert!(first.contains("schedule_id: 42"));
        assert!(first.starts_with("INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, task, created_time) VALUES ("));
        assert!(second.ends_with(" USING TTL 157680000"), "{second}");
        assert!(second.contains(", 4, {domain_id:"));
        // Fence: range_id bound twice (SET and IF), type 1, sentinel -12345.
        let fence = range_fence_statement(&key, 25, base_nanos);
        assert!(fence.starts_with("UPDATE tasks SET range_id = 25, last_updated_time = 1711922921000 WHERE"));
        assert!(fence.contains("type = 1 AND task_id = -12345 IF range_id = 25"));
    }

    /// Go tasks_test.go TestRangeDeleteTasks / TestSelectTasks use the
    /// statement templates pinned by cassandra_cql_matches_go_tasks_cql;
    /// bound parameter order is domain, name, type, row type, then ids.
    #[test]
    fn cassandra_task_row_constants_match_go() {
        assert_eq!(TASK_ROW, 0, "Go rowTypeTask");
        assert_eq!(LIST_ROW, 1, "Go rowTypeTaskList");
        assert_eq!(LIST_SENTINEL, -12345, "Go taskListTaskID");
        assert_eq!(MAX_CASSANDRA_TTL_SECONDS, 157_680_000, "Go maxCassandraTTL");
    }

    #[test]
    fn delete_fallback_consistency_is_go_constant_local_quorum() {
        assert_eq!(
            super::FALLBACK_CONSISTENCY,
            scylla::frame::types::Consistency::LocalQuorum
        );
    }

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
        assert_eq!(udt.type_, 1);
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

/// Recording-executor tests: the store's statement text and ordered bound
/// values against go/common/persistence/nosql/nosqlplugin/cassandra/tasks_test.go.
/// Go's fake session prints bound values inline; the `?` placeholders below sit
/// where those values appear in Go's expected queries.
#[cfg(test)]
mod recording_tests {
    use super::*;
    use crate::matching_cassandra_exec::{
        BoundValue, ConfigRow, CqlExecutor, CqlStatement, ExecFailure, ExecResult, LwtOutcome,
    };
    use scylla::{
        errors::{DbError, ExecutionError, RequestAttemptError},
        frame::types::Consistency,
    };
    use std::{collections::VecDeque, sync::Mutex};

    const BASE_NANOS: i64 = 1_711_922_921_000_000_000; // 2024-04-01T22:08:41Z

    #[derive(Debug, Clone)]
    struct Call {
        text: String,
        consistency: Option<Consistency>,
        values: Vec<String>,
        batch: bool,
    }

    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<Call>>,
        lwt: Mutex<VecDeque<ExecResult<LwtOutcome>>>,
        exec: Mutex<VecDeque<ExecResult<()>>>,
        task_list: Mutex<Option<(i64, CassandraTaskListUdt)>>,
        tasks: Mutex<Vec<(i64, CassandraTaskUdt, Option<i32>)>>,
        count: Mutex<Option<i64>>,
    }

    fn render(values: &[Box<dyn BoundValue>]) -> Vec<String> {
        values.iter().map(|v| format!("{v:?}")).collect()
    }

    impl Recorder {
        fn record(&self, statement: &CqlStatement, values: &[Box<dyn BoundValue>], batch: bool) {
            self.calls.lock().unwrap().push(Call {
                text: statement.text.clone(),
                consistency: statement.consistency,
                values: render(values),
                batch,
            });
        }
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }
        fn push_lwt(&self, reply: ExecResult<LwtOutcome>) {
            self.lwt.lock().unwrap().push_back(reply);
        }
    }

    fn applied() -> LwtOutcome {
        LwtOutcome {
            applied: true,
            range_id: 0,
        }
    }

    fn unavailable() -> ExecFailure {
        ExecFailure::Execution(Box::new(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(
                DbError::Unavailable {
                    consistency: Consistency::All,
                    required: 3,
                    alive: 1,
                },
                "unavailable".into(),
            ),
        )))
    }

    impl CqlExecutor for Recorder {
        fn exec(&self, statement: &CqlStatement, values: BoundValues) -> ExecResult<()> {
            self.record(statement, &values, false);
            self.exec.lock().unwrap().pop_front().unwrap_or(Ok(()))
        }
        fn exec_lwt(
            &self,
            statement: &CqlStatement,
            values: BoundValues,
        ) -> ExecResult<LwtOutcome> {
            self.record(statement, &values, false);
            self.lwt.lock().unwrap().pop_front().unwrap_or(Ok(applied()))
        }
        fn batch_lwt(
            &self,
            statements: &[CqlStatement],
            values: Vec<BoundValues>,
        ) -> ExecResult<LwtOutcome> {
            for (statement, values) in statements.iter().zip(&values) {
                self.record(statement, values, true);
            }
            self.lwt.lock().unwrap().pop_front().unwrap_or(Ok(applied()))
        }
        fn select_task_list(
            &self,
            statement: &CqlStatement,
            values: BoundValues,
        ) -> ExecResult<Option<(i64, CassandraTaskListUdt)>> {
            self.record(statement, &values, false);
            Ok(self.task_list.lock().unwrap().clone())
        }
        fn select_tasks(
            &self,
            statement: &CqlStatement,
            values: BoundValues,
        ) -> ExecResult<Vec<(i64, CassandraTaskUdt, Option<i32>)>> {
            self.record(statement, &values, false);
            Ok(self.tasks.lock().unwrap().clone())
        }
        fn select_count(
            &self,
            statement: &CqlStatement,
            values: BoundValues,
        ) -> ExecResult<Option<i64>> {
            self.record(statement, &values, false);
            Ok(*self.count.lock().unwrap())
        }
        fn select_config(
            &self,
            statement: &CqlStatement,
            values: BoundValues,
        ) -> ExecResult<Option<ConfigRow>> {
            self.record(statement, &values, false);
            Ok(None)
        }
    }

    fn store(recorder: &Arc<Recorder>, all_consistency_delete: bool) -> CassandraMatchingTaskStore {
        CassandraMatchingTaskStore::with_executor(
            recorder.clone(),
            Arc::new(move || all_consistency_delete),
        )
    }

    fn key() -> TaskListKey {
        TaskListKey {
            domain_id: Uuid::from_u128(0x1111),
            name: "tasklist1".into(),
            task_type: 1,
        }
    }

    fn dbg<T: std::fmt::Debug>(value: T) -> String {
        format!("{value:?}")
    }

    /// Whitespace/case-insensitive statement comparison (Go's queries differ
    /// from Rust's only in spacing around `=` and `,`).
    fn squash(text: &str) -> String {
        text.split_whitespace().collect::<String>().to_ascii_lowercase()
    }

    fn assert_call(call: &Call, go_text: &str, values: Vec<String>) {
        assert_eq!(squash(&call.text), squash(go_text), "statement text");
        assert_eq!(call.values, values, "bound values");
    }

    fn list_udt(ack_level: i64, kind: i16) -> CassandraTaskListUdt {
        let data = TaskListData {
            kind,
            ack_level,
            last_updated_nanos: BASE_NANOS,
            ..Default::default()
        };
        CassandraMatchingTaskStore::task_list_udt(&key(), &data).unwrap()
    }

    const GO_UPDATE: &str = "UPDATE tasks SET range_id = ?, task_list = ?, last_updated_time = ? WHERE domain_id = ? and task_list_name = ? and task_list_type = ? and type = ? and task_id = ? IF range_id = ?";

    fn update_values(range: i64, udt: &CassandraTaskListUdt, cond: i64) -> Vec<String> {
        vec![
            dbg(range),
            dbg(udt),
            dbg(timestamp(BASE_NANOS).unwrap()),
            dbg(key().domain_id),
            dbg("tasklist1".to_string()),
            dbg(1_i32),
            dbg(LIST_ROW),
            dbg(LIST_SENTINEL),
            dbg(cond),
        ]
    }

    /// tasks_test.go TestSelectTaskList "success" (:109).
    #[test]
    fn go_select_task_list_statement_and_values() {
        let recorder = Arc::new(Recorder::default());
        *recorder.task_list.lock().unwrap() = Some((25, list_udt(1000, 2)));
        let record = store(&recorder, false).get_task_list(&key()).unwrap();
        assert_eq!(record.range_id, 25);
        assert_eq!(record.data.ack_level, 1000);
        let calls = recorder.calls();
        assert_eq!(calls.len(), 1);
        assert_call(
            &calls[0],
            "SELECT range_id, task_list FROM tasks WHERE domain_id = ? and task_list_name = ? and task_list_type = ? and type = ? and task_id = ?",
            vec![
                dbg(key().domain_id),
                dbg("tasklist1".to_string()),
                dbg(1_i32),
                dbg(LIST_ROW),
                dbg(LIST_SENTINEL),
            ],
        );
    }

    /// TestSelectTaskList: a missing row is NotFound.
    #[test]
    fn go_select_task_list_missing_row_is_not_found() {
        let recorder = Arc::new(Recorder::default());
        assert!(matches!(
            store(&recorder, false).get_task_list(&key()),
            Err(MatchingTaskStoreError::NotFound)
        ));
    }

    /// TestInsertTaskList "successfully applied" (:244-247): LeaseTaskList on a
    /// missing list inserts range 1 with IF NOT EXISTS.
    #[test]
    fn go_insert_task_list_statement_and_values() {
        let recorder = Arc::new(Recorder::default());
        let record = store(&recorder, false)
            .lease(&key(), 2, 0, BASE_NANOS)
            .unwrap();
        assert_eq!(record.range_id, 1);
        let calls = recorder.calls();
        assert_eq!(calls.len(), 2, "select then insert");
        let data = TaskListData {
            kind: 2,
            last_updated_nanos: BASE_NANOS,
            ..Default::default()
        };
        let udt = CassandraMatchingTaskStore::task_list_udt(&key(), &data).unwrap();
        assert_call(
            &calls[1],
            "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, range_id, task_list, created_time ) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
            vec![
                dbg(key().domain_id),
                dbg("tasklist1".to_string()),
                dbg(1_i32),
                dbg(LIST_ROW),
                dbg(LIST_SENTINEL),
                dbg(1_i64),
                dbg(&udt),
                dbg(timestamp(BASE_NANOS).unwrap()),
            ],
        );
    }

    /// TestInsertTaskList "not applied": the lost IF NOT EXISTS race is a
    /// condition failure, not a retry.
    #[test]
    fn go_insert_task_list_not_applied_is_condition_failure() {
        let recorder = Arc::new(Recorder::default());
        recorder.push_lwt(Ok(LwtOutcome {
            applied: false,
            range_id: 7,
        }));
        let result = store(&recorder, false).lease(&key(), 0, 0, BASE_NANOS);
        assert!(matches!(
            result,
            Err(MatchingTaskStoreError::ConditionFailed(_))
        ));
        assert_eq!(recorder.calls().len(), 2);
    }

    /// LeaseTaskList on an existing list: UPDATE (:394 shape) with range+1,
    /// conditioned on the previous range.
    #[test]
    fn go_lease_task_list_update_statement_and_values() {
        let recorder = Arc::new(Recorder::default());
        *recorder.task_list.lock().unwrap() = Some((25, list_udt(1000, 0)));
        let record = store(&recorder, false)
            .lease(&key(), 0, 25, BASE_NANOS)
            .unwrap();
        assert_eq!(record.range_id, 26);
        let calls = recorder.calls();
        assert_eq!(calls.len(), 2);
        let udt = list_udt(1000, 0);
        assert_call(&calls[1], GO_UPDATE, update_values(26, &udt, 25));
        assert_eq!(calls[1].consistency, None);
    }

    /// TestUpdateTaskList "successfully applied" (:394) for a normal list.
    #[test]
    fn go_update_task_list_statement_and_values() {
        let recorder = Arc::new(Recorder::default());
        let data = TaskListData {
            kind: 0,
            ack_level: 1000,
            ..Default::default()
        };
        store(&recorder, false)
            .update_task_list(&key(), 25, data, BASE_NANOS)
            .unwrap();
        let calls = recorder.calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].batch);
        assert_call(&calls[0], GO_UPDATE, update_values(25, &list_udt(1000, 0), 25));
    }

    /// TestUpdateTaskListWithTTL "successfully applied" (:505-506): sticky and
    /// ephemeral lists send the TTL insert then the TTL update in one batch.
    /// Go binds `taskListTTL` = 86400 (nosql_task_store.go:49,234); Rust inlines it.
    #[test]
    fn go_update_task_list_with_ttl_batch_statements_and_values() {
        for kind in [1_i16, 2] {
            let recorder = Arc::new(Recorder::default());
            let data = TaskListData {
                kind,
                ack_level: 1000,
                ..Default::default()
            };
            store(&recorder, false)
                .update_task_list(&key(), 25, data, BASE_NANOS)
                .unwrap();
            let calls = recorder.calls();
            assert_eq!(calls.len(), 2);
            assert!(calls.iter().all(|call| call.batch));
            assert_call(
                &calls[0],
                " INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, created_time ) VALUES (?, ?, ?, ?, ?, ?) USING TTL 86400",
                vec![
                    dbg(key().domain_id),
                    dbg("tasklist1".to_string()),
                    dbg(1_i32),
                    dbg(LIST_ROW),
                    dbg(LIST_SENTINEL),
                    dbg(timestamp(BASE_NANOS).unwrap()),
                ],
            );
            assert_call(
                &calls[1],
                "UPDATE tasks USING TTL 86400 SET range_id = ?, task_list = ?, last_updated_time = ? WHERE domain_id = ? and task_list_name = ? and task_list_type = ? and type = ? and task_id = ? IF range_id = ?",
                update_values(25, &list_udt(1000, kind), 25),
            );
        }
    }

    /// TestUpdateTaskList / WithTTL "not applied": a rejected LWT is a condition
    /// failure carrying the stored range.
    #[test]
    fn go_update_task_list_not_applied_is_condition_failure() {
        for kind in [0_i16, 1] {
            let recorder = Arc::new(Recorder::default());
            recorder.push_lwt(Ok(LwtOutcome {
                applied: false,
                range_id: 26,
            }));
            let data = TaskListData {
                kind,
                ..Default::default()
            };
            let result = store(&recorder, false).update_task_list(&key(), 25, data, BASE_NANOS);
            assert!(
                matches!(result, Err(MatchingTaskStoreError::ConditionFailed(ref m)) if m.contains("range_id=26")),
                "{result:?}"
            );
        }
    }

    /// TestInsertTasks (:790-792): task inserts then the lease fence, one batch,
    /// no bound values (Go's UDT literals are inlined by the Rust adapter).
    #[test]
    fn go_insert_tasks_batch_members_and_fence() {
        let recorder = Arc::new(Recorder::default());
        let plain = AllocatedTask {
            task_id: 3,
            data: TaskData {
                workflow_id: "wid1".into(),
                schedule_id: 42,
                created_nanos: BASE_NANOS,
                expiry_nanos: 0,
                ..Default::default()
            },
        };
        let capped = AllocatedTask {
            task_id: 4,
            data: TaskData {
                workflow_id: "wid1".into(),
                schedule_id: 43,
                created_nanos: BASE_NANOS,
                expiry_nanos: BASE_NANOS + 157_680_001_i64 * 1_000_000_000,
                ..Default::default()
            },
        };
        let domain = key().domain_id;
        store(&recorder, false)
            .create_tasks_with_domains(&key(), 25, BASE_NANOS, &[(domain, &plain), (domain, &capped)])
            .unwrap();
        let calls = recorder.calls();
        assert_eq!(calls.len(), 3);
        assert!(calls.iter().all(|call| call.batch && call.values.is_empty()));
        assert!(calls[0].text.starts_with(
            "INSERT INTO tasks (domain_id, task_list_name, task_list_type, type, task_id, task, created_time) VALUES ("
        ));
        assert!(!calls[0].text.contains("USING TTL"));
        assert!(calls[1].text.ends_with(" USING TTL 157680000"));
        assert_eq!(
            squash(&calls[2].text),
            squash(&format!(
                "UPDATE tasks SET range_id = 25, last_updated_time = {} WHERE domain_id = {} and task_list_name = 'tasklist1' and task_list_type = 1 and type = 1 and task_id = -12345 IF range_id = 25",
                BASE_NANOS / 1_000_000,
                domain
            ))
        );
    }

    /// TestInsertTasks "batch cas failed": a rejected fence is a condition failure.
    #[test]
    fn go_insert_tasks_not_applied_is_condition_failure() {
        let recorder = Arc::new(Recorder::default());
        recorder.push_lwt(Ok(LwtOutcome {
            applied: false,
            range_id: 30,
        }));
        let result = store(&recorder, false).create_tasks_with_domains(&key(), 25, BASE_NANOS, &[]);
        assert!(matches!(
            result,
            Err(MatchingTaskStoreError::ConditionFailed(_))
        ));
    }

    /// TestSelectTasks (:1073): Go pages with the gocql page size; Rust binds
    /// the page size as the trailing LIMIT.
    #[test]
    fn go_select_tasks_statement_and_values() {
        let recorder = Arc::new(Recorder::default());
        let tasks = store(&recorder, false).get_tasks(&key(), 0, 100, 100).unwrap();
        assert!(tasks.is_empty());
        let calls = recorder.calls();
        assert_call(
            &calls[0],
            "SELECT task_id, task, TTL(task) FROM tasks WHERE domain_id=? and task_list_name=? and task_list_type=? and type=? and task_id>? and task_id<=? LIMIT ?",
            vec![
                dbg(key().domain_id),
                dbg("tasklist1".to_string()),
                dbg(1_i32),
                dbg(TASK_ROW),
                dbg(0_i64),
                dbg(100_i64),
                dbg(100_i32),
            ],
        );
    }

    /// TestGetTasksCount "success" (:881).
    #[test]
    fn go_get_tasks_count_statement_and_values() {
        let recorder = Arc::new(Recorder::default());
        *recorder.count.lock().unwrap() = Some(5);
        assert_eq!(store(&recorder, false).task_list_size(&key(), 1).unwrap(), 5);
        let calls = recorder.calls();
        assert_call(
            &calls[0],
            "SELECT count(1) as count FROM tasks WHERE domain_id = ? and task_list_name = ? and task_list_type = ? and type = ? and task_id > ?",
            vec![
                dbg(key().domain_id),
                dbg("tasklist1".to_string()),
                dbg(1_i32),
                dbg(TASK_ROW),
                dbg(1_i64),
            ],
        );
    }

    const GO_RANGE_DELETE: &str = "DELETE FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_list_type = ? AND type = ? AND task_id > ? AND task_id <= ?";

    fn range_values(min: i64, max: i64) -> Vec<String> {
        vec![
            dbg(key().domain_id),
            dbg("tasklist1".to_string()),
            dbg(1_i32),
            dbg(TASK_ROW),
            dbg(min),
            dbg(max),
        ]
    }

    /// TestRangeDeleteTasks "success" (:1149): CompleteTasksLessThan deletes
    /// (0, id]; CompleteTask deletes (id-1, id].
    #[test]
    fn go_range_delete_tasks_statement_and_values() {
        let recorder = Arc::new(Recorder::default());
        let store = store(&recorder, false);
        store.complete_tasks_less_than(&key(), 100, 10).unwrap();
        store.complete_task(&key(), 7).unwrap();
        let calls = recorder.calls();
        assert_call(&calls[0], GO_RANGE_DELETE, range_values(0, 100));
        assert_call(&calls[1], GO_RANGE_DELETE, range_values(6, 7));
        assert!(calls.iter().all(|call| call.consistency.is_none()));
    }

    /// Range delete under the ALL-consistency flag falls back to LOCAL_QUORUM
    /// only when ALL is unavailable (Go executeWithConsistencyAll).
    #[test]
    fn go_range_delete_all_consistency_retries_at_local_quorum() {
        let recorder = Arc::new(Recorder::default());
        recorder.exec.lock().unwrap().push_back(Err(unavailable()));
        store(&recorder, true).complete_task(&key(), 7).unwrap();
        let calls = recorder.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].consistency, Some(Consistency::All));
        assert_eq!(calls[1].consistency, Some(Consistency::LocalQuorum));
        assert_eq!(calls[0].values, calls[1].values);
        assert_call(&calls[1], GO_RANGE_DELETE, range_values(6, 7));
    }

    #[test]
    fn go_range_delete_non_unavailable_error_is_not_retried() {
        let recorder = Arc::new(Recorder::default());
        recorder
            .exec
            .lock()
            .unwrap()
            .push_back(Err(ExecFailure::Other("boom".into())));
        assert!(store(&recorder, true).complete_task(&key(), 7).is_err());
        assert_eq!(recorder.calls().len(), 1);
    }

    const GO_DELETE_LIST: &str = "DELETE FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_list_type = ? AND type = ? AND task_id = ? IF range_id = ?";

    fn delete_values() -> Vec<String> {
        vec![
            dbg(key().domain_id),
            dbg("tasklist1".to_string()),
            dbg(1_i32),
            dbg(LIST_ROW),
            dbg(LIST_SENTINEL),
            dbg(35_i64),
        ]
    }

    /// TestDeleteTaskList (:617-700).
    #[test]
    fn go_delete_task_list_statement_values_and_consistency() {
        let recorder = Arc::new(Recorder::default());
        store(&recorder, true).delete_task_list(&key(), 35).unwrap();
        let calls = recorder.calls();
        assert_eq!(calls.len(), 1);
        assert_call(&calls[0], GO_DELETE_LIST, delete_values());
        assert_eq!(calls[0].consistency, Some(Consistency::All));

        // Flag off: default consistency.
        let recorder = Arc::new(Recorder::default());
        store(&recorder, false).delete_task_list(&key(), 35).unwrap();
        assert_eq!(recorder.calls()[0].consistency, None);
    }

    #[test]
    fn go_delete_task_list_not_applied_is_condition_failure() {
        let recorder = Arc::new(Recorder::default());
        recorder.push_lwt(Ok(LwtOutcome {
            applied: false,
            range_id: 36,
        }));
        assert!(matches!(
            store(&recorder, true).delete_task_list(&key(), 35),
            Err(MatchingTaskStoreError::ConditionFailed(_))
        ));
    }

    #[test]
    fn go_delete_task_list_retries_at_default_consistency_when_all_unavailable() {
        let recorder = Arc::new(Recorder::default());
        recorder.push_lwt(Err(unavailable()));
        store(&recorder, true).delete_task_list(&key(), 35).unwrap();
        let calls = recorder.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].consistency, Some(Consistency::All));
        assert_eq!(calls[1].consistency, Some(Consistency::LocalQuorum));
        assert_call(&calls[1], GO_DELETE_LIST, delete_values());

        // Retry also failing is an error (unavailable maps to DbUnavailable).
        let recorder = Arc::new(Recorder::default());
        recorder.push_lwt(Err(unavailable()));
        recorder.push_lwt(Err(unavailable()));
        let error = store(&recorder, true)
            .delete_task_list(&key(), 35)
            .unwrap_err();
        assert!(matches!(
            error,
            MatchingTaskStoreError::Backend {
                kind: BackendErrorKind::DbUnavailable,
                ..
            }
        ));

        // A non-consistency error is not retried.
        let recorder = Arc::new(Recorder::default());
        recorder.push_lwt(Err(ExecFailure::Other("boom".into())));
        assert!(store(&recorder, true).delete_task_list(&key(), 35).is_err());
        assert_eq!(recorder.calls().len(), 1);
    }
}
