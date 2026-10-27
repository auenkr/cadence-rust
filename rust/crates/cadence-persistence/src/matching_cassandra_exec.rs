//! Statement-executor seam under the Cassandra Matching task store.
//!
//! The store builds each statement text and its ordered bound values and
//! hands them to a `CqlExecutor`. The production implementation sends exactly
//! those statements and values through the Scylla session; tests substitute a
//! recorder so statement text and bound values can be asserted against Go's
//! `nosqlplugin/cassandra/tasks_test.go` without a live cluster.

use crate::cassandra::{CassandraBlockingSession, CassandraTaskListUdt, CassandraTaskUdt};
use scylla::{
    errors::{DbError, ExecutionError, RequestAttemptError},
    frame::types::Consistency,
    response::query_result::QueryResult,
    serialize::value::SerializeValue,
    statement::{
        batch::{Batch, BatchType},
        Statement,
    },
};
use std::{fmt, sync::Arc};

/// A bound CQL value: serializable by the driver and printable for recorders.
pub trait BoundValue: SerializeValue + fmt::Debug + Send + Sync {}
impl<T: SerializeValue + fmt::Debug + Send + Sync> BoundValue for T {}

pub type BoundValues = Vec<Box<dyn BoundValue>>;

pub(crate) fn bound<T: BoundValue + 'static>(value: T) -> Box<dyn BoundValue> {
    Box::new(value)
}

/// Statement text plus an optional per-statement consistency override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqlStatement {
    pub text: String,
    pub consistency: Option<Consistency>,
}

impl CqlStatement {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            consistency: None,
        }
    }

    pub fn with_consistency(text: impl Into<String>, consistency: Consistency) -> Self {
        Self {
            text: text.into(),
            consistency: Some(consistency),
        }
    }

    fn to_statement(&self) -> Statement {
        let mut statement = Statement::new(self.text.as_str());
        if let Some(consistency) = self.consistency {
            statement.set_consistency(consistency);
        }
        statement
    }
}

/// Outcome of a conditional statement. When Cassandra rejects an LWT it returns
/// the conflicting row's columns and Go reads `range_id` from them
/// (nosqlplugin/cassandra/tasks.go:243-256).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LwtOutcome {
    pub applied: bool,
    pub range_id: i64,
}

#[derive(Debug)]
pub enum ExecFailure {
    Execution(Box<ExecutionError>),
    Other(String),
}

impl ExecFailure {
    /// Go's timeout/throttle/database-unavailability categories. Consistency
    /// fallback deliberately uses the broader code-only `is_unavailable` below.
    pub fn backend_error_kind(&self) -> crate::matching_task_store::BackendErrorKind {
        use crate::matching_task_store::BackendErrorKind;
        let Self::Execution(error) = self else {
            return BackendErrorKind::Internal;
        };
        match error.as_ref() {
            ExecutionError::RequestTimeout(_) => BackendErrorKind::Timeout,
            ExecutionError::LastAttemptError(attempt) => match attempt {
                RequestAttemptError::BrokenConnectionError(_)
                | RequestAttemptError::DbError(DbError::WriteTimeout { .. }, _) => {
                    BackendErrorKind::Timeout
                }
                RequestAttemptError::DbError(DbError::Overloaded, _) => BackendErrorKind::Throttled,
                RequestAttemptError::DbError(DbError::Unavailable { .. }, message)
                    if message.contains("Cannot perform LWT operation")
                        || message.to_lowercase().contains("cannot achieve consistency level") =>
                {
                    BackendErrorKind::DbUnavailable
                }
                _ => BackendErrorKind::Internal,
            },
            _ => BackendErrorKind::Internal,
        }
    }

    pub fn is_unavailable(&self) -> bool {
        matches!(
            self,
            ExecFailure::Execution(error) if matches!(
                **error,
                ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::Unavailable { .. },
                    _
                ))
            )
        )
    }
}

impl fmt::Display for ExecFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecFailure::Execution(error) => fmt::Display::fmt(&**error, f),
            ExecFailure::Other(message) => f.write_str(message),
        }
    }
}

pub type ExecResult<T> = std::result::Result<T, ExecFailure>;

pub type ConfigRow = (i64, Option<Vec<u8>>, Option<String>);

pub trait CqlExecutor: Send + Sync {
    /// Plain statement whose result is ignored.
    fn exec(&self, statement: &CqlStatement, values: BoundValues) -> ExecResult<()>;
    /// Conditional statement; reads `[applied]` and `range_id`.
    fn exec_lwt(&self, statement: &CqlStatement, values: BoundValues) -> ExecResult<LwtOutcome>;
    /// Logged conditional batch, one value list per statement.
    fn batch_lwt(
        &self,
        statements: &[CqlStatement],
        values: Vec<BoundValues>,
    ) -> ExecResult<LwtOutcome>;
    fn select_task_list(
        &self,
        statement: &CqlStatement,
        values: BoundValues,
    ) -> ExecResult<Option<(i64, CassandraTaskListUdt)>>;
    fn select_tasks(
        &self,
        statement: &CqlStatement,
        values: BoundValues,
    ) -> ExecResult<Vec<(i64, CassandraTaskUdt, Option<i32>)>>;
    fn select_count(&self, statement: &CqlStatement, values: BoundValues)
        -> ExecResult<Option<i64>>;
    fn select_config(
        &self,
        statement: &CqlStatement,
        values: BoundValues,
    ) -> ExecResult<Option<ConfigRow>>;
}

fn other(error: impl fmt::Display) -> ExecFailure {
    ExecFailure::Other(error.to_string())
}

fn lwt_outcome(result: QueryResult) -> ExecResult<LwtOutcome> {
    let rows = result.into_rows_result().map_err(other)?;
    let names: Vec<String> = rows
        .column_specs()
        .iter()
        .map(|spec| spec.name().to_owned())
        .collect();
    let row = rows
        .maybe_first_row::<scylla::value::Row>()
        .map_err(other)?
        .ok_or_else(|| other("Cassandra LWT returned no result row"))?;
    let applied = match row.columns.first() {
        Some(Some(scylla::value::CqlValue::Boolean(applied))) => *applied,
        Some(value) => {
            return Err(other(format!(
                "Cassandra LWT returned invalid [applied] column: {value:?}"
            )))
        }
        None => return Err(other("Cassandra LWT result row has no [applied] column")),
    };
    let range_id = names
        .iter()
        .zip(&row.columns)
        .find_map(|(name, value)| match (name.as_str(), value) {
            ("range_id", Some(scylla::value::CqlValue::BigInt(range_id))) => Some(*range_id),
            _ => None,
        })
        .unwrap_or_default();
    Ok(LwtOutcome { applied, range_id })
}

/// Production executor: the Scylla session behind the blocking runtime.
pub struct ScyllaExecutor {
    driver: Arc<CassandraBlockingSession>,
}

impl ScyllaExecutor {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver }
    }

    fn query(
        &self,
        statement: &CqlStatement,
        values: &[Box<dyn BoundValue>],
    ) -> ExecResult<QueryResult> {
        let session = self.driver.session();
        self.driver
            .run(session.query_unpaged(statement.to_statement(), values))
            .map_err(|error| ExecFailure::Execution(Box::new(error)))
    }
}

impl CqlExecutor for ScyllaExecutor {
    fn exec(&self, statement: &CqlStatement, values: BoundValues) -> ExecResult<()> {
        self.query(statement, &values).map(|_| ())
    }

    fn exec_lwt(&self, statement: &CqlStatement, values: BoundValues) -> ExecResult<LwtOutcome> {
        lwt_outcome(self.query(statement, &values)?)
    }

    fn batch_lwt(
        &self,
        statements: &[CqlStatement],
        values: Vec<BoundValues>,
    ) -> ExecResult<LwtOutcome> {
        let mut batch = Batch::new(BatchType::Logged);
        for statement in statements {
            batch.append_statement(statement.to_statement());
        }
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.batch(&batch, values))
            .map_err(|error| ExecFailure::Execution(Box::new(error)))?;
        lwt_outcome(result)
    }

    fn select_task_list(
        &self,
        statement: &CqlStatement,
        values: BoundValues,
    ) -> ExecResult<Option<(i64, CassandraTaskListUdt)>> {
        let rows = self
            .query(statement, &values)?
            .into_rows_result()
            .map_err(other)?;
        rows.maybe_first_row::<(i64, CassandraTaskListUdt)>()
            .map_err(other)
    }

    fn select_tasks(
        &self,
        statement: &CqlStatement,
        values: BoundValues,
    ) -> ExecResult<Vec<(i64, CassandraTaskUdt, Option<i32>)>> {
        let rows = self
            .query(statement, &values)?
            .into_rows_result()
            .map_err(other)?;
        let mut out = Vec::new();
        for row in rows
            .rows::<(i64, CassandraTaskUdt, Option<i32>)>()
            .map_err(other)?
        {
            out.push(row.map_err(other)?);
        }
        Ok(out)
    }

    fn select_count(
        &self,
        statement: &CqlStatement,
        values: BoundValues,
    ) -> ExecResult<Option<i64>> {
        let rows = self
            .query(statement, &values)?
            .into_rows_result()
            .map_err(other)?;
        Ok(rows
            .maybe_first_row::<(i64,)>()
            .map_err(other)?
            .map(|(count,)| count))
    }

    fn select_config(
        &self,
        statement: &CqlStatement,
        values: BoundValues,
    ) -> ExecResult<Option<ConfigRow>> {
        let rows = self
            .query(statement, &values)?
            .into_rows_result()
            .map_err(other)?;
        rows.maybe_first_row::<ConfigRow>().map_err(other)
    }
}
