//! Small synchronous SQL transaction adapter shared by History stores.
//! All writes supplied to `transaction` use one connection and one commit.
use crate::{
    history_contract::PersistenceError,
    history_sql_queries::Dialect,
    matching_mysql::{MySqlTaskStore, SyncConnectionPool},
    matching_postgres::{PostgresConnectionPool, PostgresTaskStore},
};
use mysql::prelude::Queryable;
use std::sync::Arc;

struct AssertSend<T>(T);
// SAFETY: see `SqlBackend::transaction`; the value is only used while the owning thread is blocked.
unsafe impl<T> Send for AssertSend<T> {}
#[derive(Clone)]
pub struct SqlBackend(Backend);
#[derive(Clone)]
enum Backend {
    MySql(Arc<SyncConnectionPool>),
    Postgres(Arc<PostgresConnectionPool>),
}
#[derive(Clone, Debug)]
pub enum Value {
    I32(i32),
    I64(i64),
    Bytes(Vec<u8>),
    Text(String),
    Timestamp(chrono::NaiveDateTime),
}
#[derive(Clone, Copy)]
pub enum Column {
    I32,
    I64,
    Bytes,
    Text,
    Timestamp,
}
pub trait SqlTx {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError>;
    fn query(
        &mut self,
        sql: &str,
        params: &[Value],
        columns: &[Column],
    ) -> Result<Vec<Vec<Value>>, PersistenceError>;
}
/// Runs synchronous SQL I/O from an async History path. Go's database/sql
/// call parks only its goroutine; on a multi-thread Tokio runtime
/// `block_in_place` gives the same effect by moving the worker's other tasks
/// (RPCs, queue processors, timers) to another thread for the duration.
/// Without it a slow commit stalled every task on that worker (p99 tail).
pub fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}
/// Env-gated diagnostics: `CADENCE_HISTORY_SQL_SLOW_MS=<ms>` logs transactions
/// slower than the threshold to stderr. Unset means no timing.
fn slow_threshold() -> Option<std::time::Duration> {
    static THRESHOLD: std::sync::OnceLock<Option<std::time::Duration>> = std::sync::OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("CADENCE_HISTORY_SQL_SLOW_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(std::time::Duration::from_millis)
    })
}
/// Env-gated SQL round-trip counters: `CADENCE_SQL_STATS_SECS=<n>` prints
/// cumulative History SQL transaction/statement counts to stderr every n
/// seconds. Unset means the counters are never touched.
pub mod sql_stats {
    use std::sync::atomic::{AtomicU64, Ordering};
    pub static TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
    pub static STATEMENTS: AtomicU64 = AtomicU64::new(0);
    pub static QUERIES: AtomicU64 = AtomicU64::new(0);
    pub static AUTOCOMMITS: AtomicU64 = AtomicU64::new(0);
    pub fn enabled() -> bool {
        static INTERVAL: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
        INTERVAL
            .get_or_init(|| {
                let secs = std::env::var("CADENCE_SQL_STATS_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)?;
                std::thread::Builder::new()
                    .name("sql-stats".into())
                    .spawn(move || loop {
                        std::thread::sleep(std::time::Duration::from_secs(secs));
                        eprintln!("{}", line());
                    })
                    .ok()?;
                Some(secs)
            })
            .is_some()
    }
    pub fn line() -> String {
        format!(
            "cadence-history sql stats: transactions={} autocommit={} exec={} query={}",
            TRANSACTIONS.load(Ordering::Relaxed),
            AUTOCOMMITS.load(Ordering::Relaxed),
            STATEMENTS.load(Ordering::Relaxed),
            QUERIES.load(Ordering::Relaxed)
        )
    }
    pub fn count(counter: &AtomicU64) {
        if enabled() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}
pub fn internal(error: impl std::fmt::Display) -> PersistenceError {
    PersistenceError::Internal(error.to_string())
}
fn mysql_error(error: mysql::Error) -> PersistenceError {
    match &error {
        mysql::Error::MySqlError(e) if e.code == 1062 => {
            PersistenceError::ConditionFailed(error.to_string())
        }
        mysql::Error::MySqlError(e)
            if matches!(e.code, 1159 | 1161 | 1205 | 1613 | 3024 | 3133 | 3699) =>
        {
            PersistenceError::Timeout(error.to_string())
        }
        mysql::Error::MySqlError(e) if matches!(e.code, 1040 | 1203 | 1637 | 3634) => {
            PersistenceError::ServiceBusy(error.to_string())
        }
        _ => internal(error),
    }
}
fn postgres_error(error: postgres::Error) -> PersistenceError {
    match error.code().map(|code| code.code()) {
        Some("23505") => PersistenceError::ConditionFailed(error.to_string()),
        Some("53300" | "53000") => PersistenceError::ServiceBusy(error.to_string()),
        _ => internal(error),
    }
}
fn mysql_params(params: &[Value]) -> mysql::Params {
    mysql::Params::Positional(
        params
            .iter()
            .map(|v| match v {
                Value::I32(v) => (*v).into(),
                Value::I64(v) => (*v).into(),
                Value::Bytes(v) => v.clone().into(),
                Value::Text(v) => v.clone().into(),
                Value::Timestamp(v) => (*v).into(),
            })
            .collect(),
    )
}
fn postgres_params(params: &[Value]) -> Vec<&(dyn postgres::types::ToSql + Sync)> {
    params
        .iter()
        .map(|v| match v {
            Value::I32(v) => v as &(dyn postgres::types::ToSql + Sync),
            Value::I64(v) => v,
            Value::Bytes(v) => v,
            Value::Text(v) => v,
            Value::Timestamp(v) => v,
        })
        .collect()
}
/// MySQL transaction that poisons its pooled connection on connectivity errors.
/// Generic over a transaction or a bare autocommit connection.
struct MySqlTx<'a, Q: Queryable> {
    tx: &'a mut Q,
    discard: &'a std::sync::atomic::AtomicBool,
}
impl<Q: Queryable> MySqlTx<'_, Q> {
    fn fail(&self, error: mysql::Error) -> PersistenceError {
        if error.is_connectivity_error() {
            self.discard
                .store(true, std::sync::atomic::Ordering::Release);
        }
        mysql_error(error)
    }
}
impl<Q: Queryable> SqlTx for MySqlTx<'_, Q> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError> {
        sql_stats::count(&sql_stats::STATEMENTS);
        let affected = match self.tx.exec_iter(sql, mysql_params(params)) {
            Ok(result) => {
                let affected = result.affected_rows();
                // Dropping the result drains any remaining packets.
                drop(result);
                affected
            }
            Err(error) => {
                // `self.tx` is still borrowed by the match; poison via the field.
                if error.is_connectivity_error() {
                    self.discard
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                return Err(mysql_error(error));
            }
        };
        Ok(affected)
    }
    fn query(
        &mut self,
        sql: &str,
        params: &[Value],
        columns: &[Column],
    ) -> Result<Vec<Vec<Value>>, PersistenceError> {
        sql_stats::count(&sql_stats::QUERIES);
        let rows: Vec<mysql::Row> = self
            .tx
            .exec(sql, mysql_params(params))
            .map_err(|e| self.fail(e))?;
        rows.into_iter()
            .map(|row| {
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        let bad = || {
                            PersistenceError::InternalDataInconsistency(format!(
                                "invalid SQL column {i}"
                            ))
                        };
                        match col {
                            Column::I32 => row
                                .get_opt::<i32, _>(i)
                                .ok_or_else(bad)?
                                .map(Value::I32)
                                .map_err(internal),
                            Column::I64 => row
                                .get_opt::<i64, _>(i)
                                .ok_or_else(bad)?
                                .map(Value::I64)
                                .map_err(internal),
                            Column::Bytes => row
                                .get_opt::<Option<Vec<u8>>, _>(i)
                                .ok_or_else(bad)?
                                .map(|value| Value::Bytes(value.unwrap_or_default()))
                                .map_err(internal),
                            Column::Text => row
                                .get_opt::<String, _>(i)
                                .ok_or_else(bad)?
                                .map(Value::Text)
                                .map_err(internal),
                            Column::Timestamp => row
                                .get_opt::<chrono::NaiveDateTime, _>(i)
                                .ok_or_else(bad)?
                                .map(Value::Timestamp)
                                .map_err(internal),
                        }
                    })
                    .collect()
            })
            .collect()
    }
}
macro_rules! postgres_sql_tx {
    ($ty:ty) => {
impl SqlTx for $ty {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError> {
        let statement = self.prepare(sql).map_err(postgres_error)?;
        let params = coerce_params(params, statement.params())?;
        <$ty>::execute(self, &statement, &postgres_params(&params))
            .map_err(postgres_error)
    }
    fn query(
        &mut self,
        sql: &str,
        params: &[Value],
        columns: &[Column],
    ) -> Result<Vec<Vec<Value>>, PersistenceError> {
        let statement = self.prepare(sql).map_err(postgres_error)?;
        let params = coerce_params(params, statement.params())?;
        <$ty>::query(self, &statement, &postgres_params(&params))
            .map_err(postgres_error)?
            .into_iter()
            .map(|row| {
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| match col {
                        Column::I32 => row.try_get(i).map(Value::I32).map_err(internal),
                        Column::I64 => row.try_get(i).map(Value::I64).map_err(internal),
                        Column::Bytes => row
                            .try_get::<_, Option<Vec<u8>>>(i)
                            .map(|value| Value::Bytes(value.unwrap_or_default()))
                            .map_err(internal),
                        Column::Text => row.try_get(i).map(Value::Text).map_err(internal),
                        Column::Timestamp => row.try_get(i).map(Value::Timestamp).map_err(internal),
                    })
                    .collect()
            })
            .collect()
    }
}
    };
}
postgres_sql_tx!(postgres::Transaction<'_>);
postgres_sql_tx!(postgres::Client);
impl SqlBackend {
    pub fn mysql(store: &MySqlTaskStore) -> Self {
        Self(Backend::MySql(store.history_pool()))
    }
    pub fn postgres(store: &PostgresTaskStore) -> Self {
        Self(Backend::Postgres(store.history_pool()))
    }
    pub fn dialect(&self) -> Dialect {
        match &self.0 {
            Backend::MySql(_) => Dialect::MySql,
            Backend::Postgres(_) => Dialect::Postgres,
        }
    }
    pub fn transaction<T>(
        &self,
        run: impl FnOnce(&mut dyn SqlTx) -> Result<T, PersistenceError>,
    ) -> Result<T, PersistenceError> {
        sql_stats::count(&sql_stats::TRANSACTIONS);
        blocking(|| {
            let started = slow_threshold().map(|t| (t, std::time::Instant::now()));
            let result = self.transaction_inner(run);
            if let Some((threshold, started)) = started {
                let elapsed = started.elapsed();
                if elapsed >= threshold {
                    eprintln!("cadence-history sql transaction slow: {} ms", elapsed.as_millis());
                }
            }
            result
        })
    }
    /// Runs statements on one pooled connection in autocommit mode (no
    /// START TRANSACTION/COMMIT), matching Go's non-transactional
    /// `db.SelectContext`/`ExecContext` calls for reads and single-statement
    /// writes. Like database/sql's `maxBadConnRetries`, a connectivity
    /// failure discards the connection and retries on another one (at most
    /// twice); callers use this only for idempotent work.
    pub fn autocommit<T>(
        &self,
        run: impl FnMut(&mut dyn SqlTx) -> Result<T, PersistenceError>,
    ) -> Result<T, PersistenceError> {
        self.autocommit_with_retries(2, run)
    }
    /// Autocommit for a non-idempotent single write (e.g. INSERT): never
    /// re-sent after a connectivity error, whose outcome is unknown.
    pub fn autocommit_once<T>(
        &self,
        run: impl FnMut(&mut dyn SqlTx) -> Result<T, PersistenceError>,
    ) -> Result<T, PersistenceError> {
        self.autocommit_with_retries(0, run)
    }
    fn autocommit_with_retries<T>(
        &self,
        retries: u32,
        mut run: impl FnMut(&mut dyn SqlTx) -> Result<T, PersistenceError>,
    ) -> Result<T, PersistenceError> {
        sql_stats::count(&sql_stats::AUTOCOMMITS);
        blocking(|| match &self.0 {
            Backend::MySql(pool) => {
                let mut attempt = 0;
                loop {
                    let mut conn = pool.checkout().map_err(internal)?;
                    let discard = conn.discard_flag();
                    let mut wrapped = MySqlTx {
                        tx: &mut *conn as &mut mysql::Conn,
                        discard: &discard,
                    };
                    match run(&mut wrapped) {
                        Err(_)
                            if attempt < retries
                                && discard.load(std::sync::atomic::Ordering::Acquire) =>
                        {
                            attempt += 1;
                        }
                        result => return result,
                    }
                }
            }
            Backend::Postgres(pool) => {
                let job = AssertSend(&mut run);
                crate::matching_postgres::outside_runtime(move || {
                    let job = job;
                    AssertSend((|| {
                        let mut conn = pool.checkout().map_err(internal)?;
                        (job.0)(&mut *conn)
                    })())
                })
                .0
            }
        })
    }
    fn transaction_inner<T>(
        &self,
        run: impl FnOnce(&mut dyn SqlTx) -> Result<T, PersistenceError>,
    ) -> Result<T, PersistenceError> {
        match &self.0 {
            Backend::MySql(pool) => {
                // A connection broken by a DB restart must not return to the pool
                // (Go database/sql drops connections that report driver.ErrBadConn
                // and retries BeginTx on another connection, maxBadConnRetries=2,
                // then once more on a fresh connection). Nothing has executed yet
                // when START TRANSACTION fails, so retrying is safe.
                let mut run = Some(run);
                let mut attempt = 0;
                loop {
                    let mut conn = pool.checkout().map_err(internal)?;
                    let discard = conn.discard_flag();
                    let poison = |error: mysql::Error| {
                        if error.is_connectivity_error() {
                            discard.store(true, std::sync::atomic::Ordering::Release);
                        }
                        mysql_error(error)
                    };
                    let mut tx = match conn.start_transaction(mysql::TxOpts::default()) {
                        Ok(tx) => tx,
                        Err(error) if error.is_connectivity_error() && attempt < 2 => {
                            discard.store(true, std::sync::atomic::Ordering::Release);
                            attempt += 1;
                            continue;
                        }
                        Err(error) => return Err(poison(error)),
                    };
                    let mut wrapped = MySqlTx {
                        tx: &mut tx,
                        discard: &discard,
                    };
                    let result = (run.take().expect("run consumed once"))(&mut wrapped)?;
                    tx.commit().map_err(poison)?;
                    return Ok(result);
                }
            }
            Backend::Postgres(pool) => {
                // The sync client must run outside the tokio runtime context. The
                // calling thread blocks until the scoped thread finishes, so the
                // (possibly non-Send) closure is never used concurrently.
                let job = AssertSend(run);
                crate::matching_postgres::outside_runtime(move || {
                    let job = job;
                    AssertSend((|| {
                        let mut conn = pool.checkout().map_err(internal)?;
                        let mut tx = conn.transaction().map_err(postgres_error)?;
                        let result = (job.0)(&mut tx)?;
                        tx.commit().map_err(postgres_error)?;
                        Ok(result)
                    })())
                })
                .0
            }
        }
    }
}
impl Value {
    pub fn integer(&self) -> Result<i64, PersistenceError> {
        match self {
            Self::I32(v) => Ok(i64::from(*v)),
            Self::I64(v) => Ok(*v),
            _ => Err(internal("expected SQL integer")),
        }
    }
    pub fn bytes(&self) -> Result<Vec<u8>, PersistenceError> {
        match self {
            Self::Bytes(v) => Ok(v.clone()),
            _ => Err(internal("expected SQL bytes")),
        }
    }
    pub fn text(&self) -> Result<String, PersistenceError> {
        match self {
            Self::Text(v) => Ok(v.clone()),
            _ => Err(internal("expected SQL text")),
        }
    }
}
pub fn uuid(value: &str) -> Result<Value, PersistenceError> {
    uuid::Uuid::parse_str(value)
        .map(|v| Value::Bytes(v.as_bytes().to_vec()))
        .map_err(|e| PersistenceError::InvalidPersistenceRequest(e.to_string()))
}
pub fn uuid_string(value: &Value) -> Result<String, PersistenceError> {
    uuid::Uuid::from_slice(&value.bytes()?)
        .map(|v| v.to_string())
        .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))
}

fn coerce_params(
    params: &[Value],
    types: &[postgres::types::Type],
) -> Result<Vec<Value>, PersistenceError> {
    params
        .iter()
        .zip(types)
        .map(|(value, ty)| {
            Ok(match (value, ty) {
                (Value::I32(v), &postgres::types::Type::INT8) => Value::I64(i64::from(*v)),
                (Value::I64(v), &postgres::types::Type::INT4) => {
                    Value::I32(i32::try_from(*v).map_err(internal)?)
                }
                _ => value.clone(),
            })
        })
        .collect()
}
pub fn timestamp(nanos: i64) -> Result<Value, PersistenceError> {
    chrono::DateTime::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        nanos.rem_euclid(1_000_000_000) as u32,
    )
    .map(|v| Value::Timestamp(v.naive_utc()))
    .ok_or_else(|| internal("SQL timestamp out of range"))
}
impl Value {
    pub fn nanos(&self) -> Result<i64, PersistenceError> {
        match self {
            Self::Timestamp(v) => v
                .and_utc()
                .timestamp_nanos_opt()
                .ok_or_else(|| internal("SQL timestamp out of range")),
            _ => Err(internal("expected SQL timestamp")),
        }
    }
}

#[cfg(test)]
mod blocking_tests {
    use super::blocking;

    #[test]
    fn blocking_runs_without_runtime() {
        assert_eq!(blocking(|| 7), 7);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_runs_inline_on_current_thread_runtime() {
        assert_eq!(blocking(|| 8), 8);
    }

    /// A blocking body on one worker must not stall other tasks: with
    /// `block_in_place` a concurrently spawned task still completes while the
    /// body waits for it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn blocking_lets_other_tasks_progress() {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = tokio::spawn(async move { tx.send(1).unwrap() });
        let got = blocking(|| rx.recv_timeout(std::time::Duration::from_secs(5)));
        assert_eq!(got, Ok(1));
        handle.await.unwrap();
    }
}

#[cfg(test)]
mod sql_stats_tests {
    #[test]
    fn stats_line_reports_every_counter() {
        let line = super::sql_stats::line();
        for key in ["transactions=", "autocommit=", "exec=", "query="] {
            assert!(line.contains(key), "{line}");
        }
    }
}
