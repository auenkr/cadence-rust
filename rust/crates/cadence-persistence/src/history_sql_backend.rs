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
impl SqlTx for mysql::Transaction<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError> {
        self.exec_drop(sql, mysql_params(params))
            .map_err(mysql_error)?;
        Ok(self.affected_rows())
    }
    fn query(
        &mut self,
        sql: &str,
        params: &[Value],
        columns: &[Column],
    ) -> Result<Vec<Vec<Value>>, PersistenceError> {
        let rows: Vec<mysql::Row> = self.exec(sql, mysql_params(params)).map_err(mysql_error)?;
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
impl SqlTx for postgres::Transaction<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError> {
        let statement = self.prepare(sql).map_err(postgres_error)?;
        let params = coerce_params(params, statement.params())?;
        postgres::Transaction::execute(self, &statement, &postgres_params(&params))
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
        postgres::Transaction::query(self, &statement, &postgres_params(&params))
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
        match &self.0 {
            Backend::MySql(pool) => {
                let mut conn = pool.checkout().map_err(internal)?;
                let mut tx = conn
                    .start_transaction(mysql::TxOpts::default())
                    .map_err(mysql_error)?;
                let result = run(&mut tx)?;
                tx.commit().map_err(mysql_error)?;
                Ok(result)
            }
            Backend::Postgres(pool) => {
                let mut conn = pool.checkout().map_err(internal)?;
                let mut tx = conn.transaction().map_err(postgres_error)?;
                let result = run(&mut tx)?;
                tx.commit().map_err(postgres_error)?;
                Ok(result)
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
