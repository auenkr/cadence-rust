//! History task tables; payloads retain the Go serializer's data/encoding.
use crate::{
    history_contract::*,
    history_sql_backend::{internal, timestamp, Column, SqlBackend, SqlTx, Value},
    history_sql_queries::{Dialect, Statement},
};

#[derive(Clone)]
pub struct SqlHistoryTasks {
    pub backend: SqlBackend,
    pub shard_id: i32,
    pub encoding: crate::sqlblobs::Encoding,
    pub target_cluster: String,
}
#[derive(Debug, Clone, Default)]
pub struct HistoryTaskPage {
    pub tasks: Vec<Task>,
    pub next_page_token: Vec<u8>,
}
#[derive(Debug, Clone, Default)]
pub struct HistoryTaskQuery {
    pub shard_id: i32,
    pub category: HistoryTaskCategory,
    pub min_task_id: i64,
    pub max_task_id: i64,
    pub min_timestamp_unix_nanos: i64,
    pub max_timestamp_unix_nanos: i64,
    pub page_size: i32,
    pub next_page_token: Vec<u8>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct TimerCursor {
    #[serde(rename = "TaskID")]
    task_id: i64,
    #[serde(rename = "Timestamp")]
    timestamp: chrono::DateTime<chrono::Utc>,
}
#[derive(Debug, Clone, Default)]
pub struct ReplicationDlqQuery {
    pub shard_id: i32,
    pub source_cluster: String,
    pub read_level: i64,
    pub max_read_level: i64,
    pub page_size: i32,
    pub next_page_token: Vec<u8>,
}
#[derive(Debug, Clone, Default)]
pub struct ReplicationDlqPage {
    pub tasks: Vec<ReplicationTask>,
    pub next_page_token: Vec<u8>,
}
impl SqlHistoryTasks {
    pub fn insert_tx(
        tx: &mut dyn SqlTx,
        dialect: Dialect,
        shard_id: i32,
        category: &HistoryTaskCategory,
        tasks: &[Task],
    ) -> Result<(), PersistenceError> {
        let statement = match (category.category_type, category.category_id) {
            (1, 1) => Statement::InsertTransferTask,
            (1, 3) => Statement::InsertReplicationTask,
            (2, 2) => Statement::InsertTimerTask,
            _ => return Ok(()),
        };
        for task in tasks {
            let mut params = vec![Value::I32(shard_id)];
            if category.category_type == 2 {
                params.push(timestamp(task.visibility_timestamp_unix_nanos)?);
            }
            params.extend([
                Value::I64(task.task_id),
                Value::Bytes(task.data.data.clone()),
                Value::Text(task.data.encoding.clone()),
            ]);
            let count = tx.execute(dialect.statement(statement), &params)?;
            if count != 1 {
                return Err(internal(format!(
                    "create history task inserted {count} instead of 1 rows"
                )));
            }
        }
        Ok(())
    }
    pub fn create(
        &self,
        shard_id: i32,
        category: HistoryTaskCategory,
        tasks: &[Task],
    ) -> Result<(), PersistenceError> {
        self.backend.transaction(|tx| {
            Self::insert_tx(tx, self.backend.dialect(), shard_id, &category, tasks)
        })
    }
    pub fn create_history_tasks(&self) -> Result<(), PersistenceError> {
        Err(internal("create history tasks not implemented for SQL"))
    }
    pub fn select_workflow_timer_tasks(&self) -> Vec<HistoryTaskKey> {
        Vec::new()
    } // Go SQL TODO returns nil,nil.
    pub fn get_page(
        &self,
        request: &HistoryTaskQuery,
    ) -> Result<HistoryTaskPage, PersistenceError> {
        let dialect = self.backend.dialect();
        match (request.category.category_type, request.category.category_id) {
            (1, 1) | (1, 3) => {
                let min = if request.next_page_token.is_empty() {
                    request.min_task_id
                } else {
                    crate::history_sql::decode_history_node_page_token(&request.next_page_token)
                        .map_err(internal)?
                };
                let max =
                    if request.category.category_id == 3 && !request.next_page_token.is_empty() {
                        request
                            .max_task_id
                            .max(min.wrapping_add(i64::from(request.page_size)))
                    } else {
                        request.max_task_id
                    };
                let statement = if request.category.category_id == 1 {
                    Statement::SelectTransferTasks
                } else {
                    Statement::SelectReplicationTasks
                };
                let rows = self.backend.transaction(|tx| {
                    tx.query(
                        dialect.statement(statement),
                        &[
                            Value::I32(request.shard_id),
                            Value::I64(min),
                            Value::I64(max),
                            Value::I32(request.page_size),
                        ],
                        &[Column::I64, Column::Bytes, Column::Text],
                    )
                })?;
                let tasks = rows
                    .iter()
                    .map(|row| {
                        self.task(
                            &request.category,
                            row[0].integer()?,
                            row[1].bytes()?,
                            row[2].text()?,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut next_page_token = Vec::new();
                if let Some(last) = tasks.last() {
                    let next = last.task_id.wrapping_add(1);
                    if next < request.max_task_id {
                        next_page_token = crate::history_sql::encode_history_node_page_token(next);
                    }
                }
                Ok(HistoryTaskPage {
                    tasks,
                    next_page_token,
                })
            }
            (2, 2) => {
                let cursor = if request.next_page_token.is_empty() {
                    TimerCursor {
                        task_id: i64::MIN,
                        timestamp: match timestamp(request.min_timestamp_unix_nanos)? {
                            Value::Timestamp(v) => v.and_utc(),
                            _ => unreachable!(),
                        },
                    }
                } else {
                    serde_json::from_slice(&request.next_page_token).map_err(internal)?
                };
                let min = Value::Timestamp(cursor.timestamp.naive_utc());
                let mut params = vec![
                    Value::I32(request.shard_id),
                    min.clone(),
                    Value::I64(cursor.task_id),
                ];
                if dialect == Dialect::MySql {
                    params.push(min);
                }
                params.extend([
                    timestamp(request.max_timestamp_unix_nanos)?,
                    Value::I32(request.page_size.wrapping_add(1)),
                ]);
                let rows = self.backend.transaction(|tx| {
                    tx.query(
                        dialect.statement(Statement::SelectTimerTasks),
                        &params,
                        &[Column::Timestamp, Column::I64, Column::Bytes, Column::Text],
                    )
                })?;
                let mut tasks = rows
                    .iter()
                    .map(|row| {
                        self.task(
                            &request.category,
                            row[1].integer()?,
                            row[2].bytes()?,
                            row[3].text()?,
                        )
                        .and_then(|mut task| {
                            task.visibility_timestamp_unix_nanos = row[0].nanos()?;
                            Ok(task)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let next_page_token = if tasks.len() > request.page_size.max(0) as usize {
                    let row = &rows[request.page_size as usize];
                    let next = TimerCursor {
                        task_id: row[1].integer()?,
                        timestamp: match &row[0] {
                            Value::Timestamp(v) => v.and_utc(),
                            _ => unreachable!(),
                        },
                    };
                    tasks.truncate(request.page_size as usize);
                    serde_json::to_vec(&next).map_err(internal)?
                } else {
                    Vec::new()
                };
                Ok(HistoryTaskPage {
                    tasks,
                    next_page_token,
                })
            }
            _ => Err(PersistenceError::InvalidArgument(format!(
                "Unknown task category: {:?}",
                request.category
            ))),
        }
    }
    fn task(
        &self,
        category: &HistoryTaskCategory,
        task_id: i64,
        data: Vec<u8>,
        encoding: String,
    ) -> Result<Task, PersistenceError> {
        let value = crate::sqlblobs::Encoding::parse(&encoding)
            .and_then(|e| e.decode(&data))
            .map_err(internal)?;
        let task_type = match category.category_id {
            1 => {
                crate::sqlblobs::transfer_task_from_struct(&value)
                    .map_err(internal)?
                    .task_type
            }
            2 => {
                crate::sqlblobs::timer_task_from_struct(&value)
                    .map_err(internal)?
                    .task_type
            }
            3 => {
                crate::sqlblobs::replication_task_from_struct(&value)
                    .map_err(internal)?
                    .task_type
            }
            _ => return Err(internal("unsupported task category")),
        };
        Ok(Task {
            visibility_timestamp_unix_nanos: 0,
            task_type,
            task_id,
            data: DataBlob { data, encoding },
        })
    }
    pub fn complete(
        &self,
        shard_id: i32,
        category: &HistoryTaskCategory,
        task_id: i64,
        visibility_timestamp: i64,
    ) -> Result<(), PersistenceError> {
        let statement = match (category.category_type, category.category_id) {
            (1, 1) => Statement::DeleteTransferTask,
            (1, 3) => Statement::DeleteReplicationTask,
            (2, 2) => Statement::DeleteTimerTask,
            _ => {
                return Err(PersistenceError::InvalidArgument(
                    "Unknown task category".into(),
                ))
            }
        };
        let mut params = vec![Value::I32(shard_id)];
        if category.category_type == 2 {
            params.push(timestamp(visibility_timestamp)?);
        }
        params.push(Value::I64(task_id));
        self.backend
            .transaction(|tx| tx.execute(self.backend.dialect().statement(statement), &params))?;
        Ok(())
    }
    pub fn range_complete(&self, request: &HistoryTaskQuery) -> Result<u64, PersistenceError> {
        let mut params = vec![Value::I32(request.shard_id)];
        let statement = match (request.category.category_type, request.category.category_id) {
            (1, 1) => {
                params.extend([
                    Value::I64(request.min_task_id),
                    Value::I64(request.max_task_id),
                ]);
                Statement::BatchDeleteTransferTasks
            }
            (1, 3) => {
                params.push(Value::I64(request.max_task_id));
                Statement::BatchDeleteReplicationTasks
            }
            (2, 2) => {
                params.extend([
                    timestamp(request.min_timestamp_unix_nanos)?,
                    timestamp(request.max_timestamp_unix_nanos)?,
                ]);
                Statement::BatchDeleteTimerTasks
            }
            _ => {
                return Err(PersistenceError::InvalidArgument(
                    "Unknown task category".into(),
                ))
            }
        };
        let statement = if request.page_size > 0 {
            params.push(Value::I32(request.page_size));
            statement
        } else {
            match statement {
                Statement::BatchDeleteTransferTasks => Statement::RangeDeleteTransferTasks,
                Statement::BatchDeleteReplicationTasks => Statement::RangeDeleteReplicationTasks,
                Statement::BatchDeleteTimerTasks => Statement::RangeDeleteTimerTasks,
                _ => unreachable!(),
            }
        };
        self.backend
            .transaction(|tx| tx.execute(self.backend.dialect().statement(statement), &params))
    }
    pub fn put_dlq(
        &self,
        source_cluster: &str,
        task: &ReplicationTask,
    ) -> Result<(), PersistenceError> {
        let mut task = task.clone();
        task.event_store_version = 2;
        task.new_run_event_store_version = 2;
        let data = self
            .encoding
            .encode(&crate::sqlblobs::replication_task_to_struct(&task).map_err(internal)?)
            .map_err(internal)?;
        let result = self.backend.transaction(|tx| {
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::InsertReplicationTaskDLQ),
                &[
                    Value::Text(source_cluster.into()),
                    Value::I32(self.shard_id),
                    Value::I64(task.task_id),
                    Value::Bytes(data),
                    Value::Text(self.encoding.as_str().into()),
                ],
            )
        });
        match result {
            Err(PersistenceError::ConditionFailed(_)) => Ok(()),
            value => value.map(|_| ()),
        }
    }
    pub fn get_dlq(
        &self,
        source_cluster: &str,
        min: i64,
        max: i64,
        page_size: i32,
    ) -> Result<Vec<ReplicationTask>, PersistenceError> {
        let rows = self.backend.transaction(|tx| {
            tx.query(
                self.backend
                    .dialect()
                    .statement(Statement::SelectReplicationTasksDLQ),
                &[
                    Value::Text(source_cluster.into()),
                    Value::I32(self.shard_id),
                    Value::I64(min),
                    Value::I64(max),
                    Value::I32(page_size),
                ],
                &[Column::I64, Column::Bytes, Column::Text],
            )
        })?;
        rows.into_iter()
            .map(|row| {
                let mut task = crate::sqlblobs::Encoding::parse(&row[2].text()?)
                    .and_then(|e| {
                        e.decode(
                            &row[1]
                                .bytes()
                                .map_err(|e| cadence_thrift::Error::Invalid(e.to_string()))?,
                        )
                    })
                    .and_then(|v| crate::sqlblobs::replication_task_from_struct(&v))
                    .map_err(internal)?;
                task.task_id = row[0].integer()?;
                Ok(task)
            })
            .collect()
    }
    pub fn get_dlq_page(
        &self,
        request: &ReplicationDlqQuery,
    ) -> Result<ReplicationDlqPage, PersistenceError> {
        let min = if request.next_page_token.is_empty() {
            request.read_level
        } else {
            crate::history_sql::decode_history_node_page_token(&request.next_page_token)
                .map_err(internal)?
        };
        let max = request
            .max_read_level
            .max(min.wrapping_add(i64::from(request.page_size)));
        let mut store = self.clone();
        store.shard_id = request.shard_id;
        let tasks = store.get_dlq(&request.source_cluster, min, max, request.page_size)?;
        let next_page_token = tasks
            .last()
            .map(|task| task.task_id.wrapping_add(1))
            .filter(|next| *next < max)
            .map(crate::history_sql::encode_history_node_page_token)
            .unwrap_or_default();
        Ok(ReplicationDlqPage {
            tasks,
            next_page_token,
        })
    }
    pub fn dlq_size(&self, source_cluster: &str) -> Result<i64, PersistenceError> {
        let rows = self.backend.transaction(|tx| {
            tx.query(
                self.backend
                    .dialect()
                    .statement(Statement::CountReplicationTasksDLQ),
                &[
                    Value::Text(source_cluster.into()),
                    Value::I32(self.shard_id),
                ],
                &[Column::I64],
            )
        })?;
        rows.first()
            .ok_or_else(|| internal("DLQ count returned no row"))?[0]
            .integer()
    }
    pub fn delete_dlq(&self, source_cluster: &str, task_id: i64) -> Result<(), PersistenceError> {
        self.backend.transaction(|tx| {
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::DeleteReplicationTaskDLQ),
                &[
                    Value::Text(source_cluster.into()),
                    Value::I32(self.shard_id),
                    Value::I64(task_id),
                ],
            )
        })?;
        Ok(())
    }
    pub fn range_delete_dlq(
        &self,
        source_cluster: &str,
        min: i64,
        max: i64,
        page_size: i32,
    ) -> Result<u64, PersistenceError> {
        let mut params = vec![
            Value::Text(source_cluster.into()),
            Value::I32(self.shard_id),
            Value::I64(min),
            Value::I64(max),
        ];
        if page_size > 0 {
            params.push(Value::I32(page_size));
            return self.backend.transaction(|tx| {
                tx.execute(
                    self.backend
                        .dialect()
                        .statement(Statement::RangeDeleteReplicationTasksDLQ),
                    &params,
                )
            });
        }
        let query=match self.backend.dialect(){Dialect::MySql=>"DELETE FROM replication_tasks_dlq WHERE source_cluster_name=? AND shard_id=? AND task_id>=? AND task_id<?",Dialect::Postgres=>"DELETE FROM replication_tasks_dlq WHERE source_cluster_name=$1 AND shard_id=$2 AND task_id >= $3 AND task_id < $4"};
        self.backend.transaction(|tx| tx.execute(query, &params))
    }
}
