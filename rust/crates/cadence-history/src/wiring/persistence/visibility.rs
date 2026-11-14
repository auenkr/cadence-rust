//! SQL visibility writer: port of the write half of
//! `go/common/persistence/sql/sql_visibility_store.go` and the MySQL/Postgres
//! `InsertIntoVisibility` / `ReplaceIntoVisibility` templates.
//! `UpsertWorkflowExecution` is unsupported in the Go SQL store
//! (`ErrVisibilityOperationNotSupported`) and returns the same error here.
//! Memo serialization is the caller's job (`memo` + `encoding` are written verbatim).
use cadence_persistence::history_contract::PersistenceError;
use cadence_persistence::history_sql_backend::{timestamp, SqlBackend, SqlTx, Value};
use cadence_persistence::history_sql_queries::Dialect;

/// Go `persistence.ErrVisibilityOperationNotSupported` message.
pub const VISIBILITY_OPERATION_NOT_SUPPORTED: &str = "Operation is not supported";

/// Go `sqlplugin.VisibilityRow` (times are unix nanoseconds, `<= 0` is Go's zero time).
#[derive(Clone, Debug, Default)]
pub struct VisibilityWriteRow {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub start_time: i64,
    pub execution_time: i64,
    pub workflow_type_name: String,
    pub memo: Vec<u8>,
    pub encoding: String,
    pub is_cron: bool,
    pub cron_schedule: String,
    pub num_clusters: i16,
    pub update_time: i64,
    pub shard_id: i64,
    pub execution_status: i32,
    pub scheduled_execution_time: i64,
    /// closed rows only: (close_time nanos, close_status, history_length)
    pub closed: Option<(i64, i32, i64)>,
    /// Cassandra only (Go `TaskList`).
    pub task_list: String,
    /// Cassandra only: workflow timeout seconds (open row TTL base).
    pub workflow_timeout_seconds: i64,
    /// Cassandra only: closed row retention seconds (`0` = Go default one day).
    pub retention_seconds: i64,
    /// Advanced visibility only (Go request `TaskID`, used as the document version).
    pub task_id: i64,
    /// Advanced visibility only: the request's `ExecutionStatus` as passed (the SQL store
    /// derives `execution_status` from the close status instead).
    pub request_execution_status: i32,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    /// Advanced visibility only: custom search attributes (raw encoded values).
    pub search_attributes: std::collections::BTreeMap<String, Vec<u8>>,
}

/// Visibility writes used by the transfer / timer executors (SQL, Cassandra or ES/Kafka store).
#[async_trait::async_trait]
pub trait VisibilityWriter: Send + Sync {
    async fn record_started_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError>;
    async fn record_closed_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError>;
    fn record_uninitialized(&self) -> Result<(), PersistenceError>;
    fn upsert(&self) -> Result<(), PersistenceError>;
    async fn delete_async(&self, domain_id: String, run_id: String) -> Result<(), PersistenceError>;
    /// Go `RecordWorkflowExecutionUninitialized` (row-carrying form used by the ES store).
    async fn record_uninitialized_row(&self, _row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        self.record_uninitialized()
    }
    /// Go `UpsertWorkflowExecution`.
    async fn upsert_row(&self, _row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        self.upsert()
    }
    /// Go `DeleteWorkflowExecution` (ES needs workflowID and the task id as document version).
    async fn delete_row(&self, domain_id: String, _workflow_id: String, run_id: String, _task_id: i64) -> Result<(), PersistenceError> {
        self.delete_async(domain_id, run_id).await
    }
}

#[async_trait::async_trait]
impl VisibilityWriter for SqlVisibilityWriter {
    async fn record_started_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        SqlVisibilityWriter::record_started_async(self, row).await
    }
    async fn record_closed_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        SqlVisibilityWriter::record_closed_async(self, row).await
    }
    fn record_uninitialized(&self) -> Result<(), PersistenceError> {
        SqlVisibilityWriter::record_uninitialized(self)
    }
    fn upsert(&self) -> Result<(), PersistenceError> {
        SqlVisibilityWriter::upsert(self)
    }
    async fn delete_async(&self, domain_id: String, run_id: String) -> Result<(), PersistenceError> {
        SqlVisibilityWriter::delete_async(self, domain_id, run_id).await
    }
}

/// Go `nosqlVisibilityStore` write half over the Cassandra visibility keyspace.
#[derive(Clone)]
pub struct CassandraVisibilityWriter {
    store: cadence_persistence::history_visibility_cassandra::CassandraVisibilityStore,
}

impl CassandraVisibilityWriter {
    pub fn new(store: cadence_persistence::history_visibility_cassandra::CassandraVisibilityStore) -> Self {
        Self { store }
    }
}

/// Go `openExecutionTTLBuffer` / `defaultCloseTTLSeconds` (seconds).
const OPEN_EXECUTION_TTL_BUFFER: i64 = 86_400;
const DEFAULT_CLOSE_TTL_SECONDS: i64 = 86_400;

fn cassandra_row(row: &VisibilityWriteRow) -> cadence_persistence::history_visibility_cassandra::CassandraVisibilityRow {
    let (close_time, status, history_length) = row.closed.unwrap_or((0, 0, 0));
    cadence_persistence::history_visibility_cassandra::CassandraVisibilityRow {
        domain_id: row.domain_id.clone(),
        workflow_id: row.workflow_id.clone(),
        run_id: row.run_id.clone(),
        type_name: row.workflow_type_name.clone(),
        start_time: row.start_time,
        execution_time: row.execution_time,
        memo: row.memo.clone(),
        encoding: row.encoding.clone(),
        task_list: row.task_list.clone(),
        is_cron: row.is_cron,
        num_clusters: row.num_clusters as i32,
        update_time: row.update_time,
        shard_id: row.shard_id as i32,
        execution_status: row.execution_status,
        cron_schedule: row.cron_schedule.clone(),
        scheduled_execution_time: row.scheduled_execution_time,
        close_time,
        status,
        history_length,
    }
}

fn join_err(e: tokio::task::JoinError) -> PersistenceError {
    PersistenceError::Internal(e.to_string())
}

#[async_trait::async_trait]
impl VisibilityWriter for CassandraVisibilityWriter {
    async fn record_started_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            store.insert_visibility(row.workflow_timeout_seconds + OPEN_EXECUTION_TTL_BUFFER, &cassandra_row(&row))
        })
        .await
        .map_err(join_err)?
    }
    async fn record_closed_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        if row.closed.is_none() {
            return Err(PersistenceError::InvalidPersistenceRequest(
                "missing one of {closeStatus, closeTime, historyLength} params".into(),
            ));
        }
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let retention = if row.retention_seconds == 0 { DEFAULT_CLOSE_TTL_SECONDS } else { row.retention_seconds };
            store.update_visibility(retention, &cassandra_row(&row))
        })
        .await
        .map_err(join_err)?
    }
    /// Go nosql store: not implemented (ES only), returns nil.
    fn record_uninitialized(&self) -> Result<(), PersistenceError> {
        Ok(())
    }
    /// Go nosql store: `ErrVisibilityOperationNotSupported`.
    fn upsert(&self) -> Result<(), PersistenceError> {
        Err(PersistenceError::Internal(VISIBILITY_OPERATION_NOT_SUPPORTED.into()))
    }
    async fn delete_async(&self, domain_id: String, run_id: String) -> Result<(), PersistenceError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.delete_visibility(&domain_id, &run_id)).await.map_err(join_err)?
    }
}

/// Go `minMySQLDateTime`.
const MIN_MYSQL_DATETIME_SECONDS: i64 = -30_610_224_000;
/// 0001-01-01T00:00:00Z, Go's zero `time.Time`, in unix seconds.
const GO_ZERO_SECONDS: i64 = -62_135_596_800;

fn seconds_value(secs: i64) -> Result<Value, PersistenceError> {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|v| Value::Timestamp(v.naive_utc()))
        .ok_or_else(|| PersistenceError::Internal("SQL timestamp out of range".into()))
}

fn time_value(dialect: Dialect, nanos: i64) -> Result<Value, PersistenceError> {
    if nanos > 0 {
        return timestamp(nanos);
    }
    match dialect {
        Dialect::MySql => seconds_value(MIN_MYSQL_DATETIME_SECONDS),
        Dialect::Postgres => seconds_value(GO_ZERO_SECONDS),
    }
}

const COLS_STARTED: &str = "domain_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time";
const COLS_CLOSED: &str = "domain_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, close_time, close_status, history_length, memo, encoding, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time";

/// `bool_idx` / `small_idx` are the 1-based positions of the `is_cron` (BOOLEAN) and
/// `num_clusters` (SMALLINT) columns; Postgres needs explicit casts because the
/// driver adapter only binds integers.
fn placeholders(dialect: Dialect, n: usize, bool_idx: usize, small_idx: usize) -> String {
    (1..=n)
        .map(|i| match dialect {
            Dialect::MySql => "?".to_string(),
            Dialect::Postgres if i == bool_idx => format!("${i}::int::boolean"),
            Dialect::Postgres if i == small_idx => format!("${i}::int::smallint"),
            Dialect::Postgres => format!("${i}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn started_sql(dialect: Dialect) -> String {
    match dialect {
        Dialect::MySql => format!(
            "INSERT IGNORE INTO executions_visibility ({COLS_STARTED}) VALUES ({})",
            placeholders(dialect, 15, 9, 10)
        ),
        Dialect::Postgres => format!(
            "INSERT INTO executions_visibility ({COLS_STARTED}) VALUES ({}) ON CONFLICT (domain_id, run_id) DO NOTHING",
            placeholders(dialect, 15, 9, 10)
        ),
    }
}

fn closed_sql(dialect: Dialect) -> String {
    match dialect {
        Dialect::MySql => format!(
            "REPLACE INTO executions_visibility ({COLS_CLOSED}) VALUES ({})",
            placeholders(dialect, 18, 12, 13)
        ),
        Dialect::Postgres => format!(
            "INSERT INTO executions_visibility ({COLS_CLOSED}) VALUES ({}) ON CONFLICT (domain_id, run_id) DO UPDATE SET workflow_id = excluded.workflow_id, start_time = excluded.start_time, execution_time = excluded.execution_time, workflow_type_name = excluded.workflow_type_name, close_time = excluded.close_time, close_status = excluded.close_status, history_length = excluded.history_length, memo = excluded.memo, encoding = excluded.encoding, is_cron = excluded.is_cron, num_clusters = excluded.num_clusters, update_time = excluded.update_time, shard_id = excluded.shard_id, execution_status = excluded.execution_status, cron_schedule = excluded.cron_schedule, scheduled_execution_time = excluded.scheduled_execution_time",
            placeholders(dialect, 18, 12, 13)
        ),
    }
}

fn bool_value(_dialect: Dialect, v: bool) -> Value {
    Value::I32(v as i32)
}

/// Visibility store writer over the shared SQL backend (blocking driver; callers
/// on async paths should use `spawn_blocking` or the async methods below).
#[derive(Clone)]
pub struct SqlVisibilityWriter {
    backend: SqlBackend,
}

impl SqlVisibilityWriter {
    pub fn new(backend: SqlBackend) -> Self {
        Self { backend }
    }

    /// Go `RecordWorkflowExecutionStarted` (`InsertIntoVisibility`: keep an existing row).
    pub fn record_started(&self, row: &VisibilityWriteRow) -> Result<(), PersistenceError> {
        let d = self.backend.dialect();
        let params = vec![
            Value::Text(row.domain_id.clone()),
            Value::Text(row.workflow_id.clone()),
            Value::Text(row.run_id.clone()),
            time_value(d, row.start_time)?,
            time_value(d, row.execution_time)?,
            Value::Text(row.workflow_type_name.clone()),
            Value::Bytes(row.memo.clone()),
            Value::Text(row.encoding.clone()),
            bool_value(d, row.is_cron),
            Value::I32(row.num_clusters as i32),
            time_value(d, row.update_time)?,
            Value::I64(row.shard_id),
            Value::I32(row.execution_status),
            Value::Text(row.cron_schedule.clone()),
            time_value(d, row.scheduled_execution_time)?,
        ];
        let sql = started_sql(d);
        self.backend.transaction(|tx: &mut dyn SqlTx| tx.execute(&sql, &params)).map(|_| ())
    }

    /// Go `RecordWorkflowExecutionClosed` (`ReplaceIntoVisibility`); fails with the
    /// Go `errCloseParams` condition when the row carries no close data.
    pub fn record_closed(&self, row: &VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (close_time, close_status, history_length) = row.closed.ok_or_else(|| {
            PersistenceError::InvalidPersistenceRequest(
                "missing one of {closeStatus, closeTime, historyLength} params".into(),
            )
        })?;
        let d = self.backend.dialect();
        let params = vec![
            Value::Text(row.domain_id.clone()),
            Value::Text(row.workflow_id.clone()),
            Value::Text(row.run_id.clone()),
            time_value(d, row.start_time)?,
            time_value(d, row.execution_time)?,
            Value::Text(row.workflow_type_name.clone()),
            time_value(d, close_time)?,
            Value::I32(close_status),
            Value::I64(history_length),
            Value::Bytes(row.memo.clone()),
            Value::Text(row.encoding.clone()),
            bool_value(d, row.is_cron),
            Value::I32(row.num_clusters as i32),
            time_value(d, row.update_time)?,
            Value::I64(row.shard_id),
            Value::I32(row.execution_status),
            Value::Text(row.cron_schedule.clone()),
            time_value(d, row.scheduled_execution_time)?,
        ];
        let sql = closed_sql(d);
        let affected = self.backend.transaction(|tx: &mut dyn SqlTx| tx.execute(&sql, &params))?;
        if affected > 2 {
            // either adds a new row or deletes the old row and adds a new row
            return Err(PersistenceError::Internal(format!(
                "RecordWorkflowExecutionClosed unexpected numRows ({affected}) updated"
            )));
        }
        Ok(())
    }

    /// Go `RecordWorkflowExecutionUninitialized`: no-op for SQL.
    pub fn record_uninitialized(&self) -> Result<(), PersistenceError> {
        Ok(())
    }

    /// Go `UpsertWorkflowExecution`: not supported by the SQL visibility store.
    pub fn upsert(&self) -> Result<(), PersistenceError> {
        Err(PersistenceError::Internal(VISIBILITY_OPERATION_NOT_SUPPORTED.into()))
    }

    /// Go `DeleteWorkflowExecution` (`DeleteFromVisibility`).
    pub fn delete(&self, domain_id: &str, run_id: &str) -> Result<(), PersistenceError> {
        let d = self.backend.dialect();
        let sql = match d {
            Dialect::MySql => "DELETE FROM executions_visibility WHERE domain_id=? AND run_id=?",
            Dialect::Postgres => "DELETE FROM executions_visibility WHERE domain_id=$1 AND run_id=$2",
        };
        let params = vec![Value::Text(domain_id.into()), Value::Text(run_id.into())];
        self.backend.transaction(|tx: &mut dyn SqlTx| tx.execute(sql, &params)).map(|_| ())
    }

    pub async fn record_started_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let w = self.clone();
        tokio::task::spawn_blocking(move || w.record_started(&row))
            .await
            .map_err(|e| PersistenceError::Internal(e.to_string()))?
    }

    pub async fn record_closed_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let w = self.clone();
        tokio::task::spawn_blocking(move || w.record_closed(&row))
            .await
            .map_err(|e| PersistenceError::Internal(e.to_string()))?
    }

    pub async fn delete_async(&self, domain_id: String, run_id: String) -> Result<(), PersistenceError> {
        let w = self.clone();
        tokio::task::spawn_blocking(move || w.delete(&domain_id, &run_id))
            .await
            .map_err(|e| PersistenceError::Internal(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_templates_match_go_shapes() {
        assert!(started_sql(Dialect::MySql).starts_with("INSERT IGNORE INTO executions_visibility"));
        assert!(started_sql(Dialect::Postgres).ends_with("ON CONFLICT (domain_id, run_id) DO NOTHING"));
        assert!(closed_sql(Dialect::MySql).starts_with("REPLACE INTO executions_visibility"));
        assert!(closed_sql(Dialect::Postgres).contains("$18"));
        assert_eq!(placeholders(Dialect::MySql, 3, 0, 0), "?, ?, ?");
    }

    #[test]
    fn zero_times_map_to_min_datetime() {
        assert!(time_value(Dialect::MySql, 0).is_ok());
        assert!(time_value(Dialect::Postgres, 0).is_ok());
        assert!(time_value(Dialect::MySql, 1_700_000_000_000_000_000).is_ok());
    }
}
