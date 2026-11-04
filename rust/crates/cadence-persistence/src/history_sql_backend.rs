//! Small synchronous SQL transaction adapter shared by History stores.
//! All writes supplied to `transaction` use one connection and one commit.
use crate::{history_contract::PersistenceError, matching_mysql::{MySqlTaskStore, SyncConnectionPool}, matching_postgres::{PostgresTaskStore, PostgresConnectionPool}, history_sql_queries::Dialect};
use mysql::prelude::Queryable;
use std::sync::Arc;

#[derive(Clone)]
pub enum SqlBackend { MySql(Arc<SyncConnectionPool>), Postgres(Arc<PostgresConnectionPool>) }
#[derive(Clone, Debug)]
pub enum Value { I32(i32), I64(i64), Bytes(Vec<u8>), Text(String) }
#[derive(Clone, Copy)]
pub enum Column { I32, I64, Bytes, Text }
pub trait SqlTx {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError>;
    fn query(&mut self, sql: &str, params: &[Value], columns: &[Column]) -> Result<Vec<Vec<Value>>, PersistenceError>;
}
pub fn internal(error: impl std::fmt::Display) -> PersistenceError { PersistenceError::Internal(error.to_string()) }
fn mysql_error(error: mysql::Error) -> PersistenceError {
    if matches!(&error, mysql::Error::MySqlError(e) if e.code == 1062) { PersistenceError::ConditionFailed(error.to_string()) } else { internal(error) }
}
fn postgres_error(error: postgres::Error) -> PersistenceError {
    if error.code() == Some(&postgres::error::SqlState::UNIQUE_VIOLATION) { PersistenceError::ConditionFailed(error.to_string()) } else { internal(error) }
}
fn mysql_params(params: &[Value]) -> mysql::Params {
    mysql::Params::Positional(params.iter().map(|v| match v {
        Value::I32(v) => (*v).into(), Value::I64(v) => (*v).into(), Value::Bytes(v) => v.clone().into(), Value::Text(v) => v.clone().into(),
    }).collect())
}
fn postgres_params(params: &[Value]) -> Vec<&(dyn postgres::types::ToSql + Sync)> {
    params.iter().map(|v| match v {
        Value::I32(v) => v as &(dyn postgres::types::ToSql + Sync), Value::I64(v) => v, Value::Bytes(v) => v, Value::Text(v) => v,
    }).collect()
}
impl SqlTx for mysql::Transaction<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError> {
        self.exec_drop(sql, mysql_params(params)).map_err(mysql_error)?; Ok(self.affected_rows())
    }
    fn query(&mut self, sql: &str, params: &[Value], columns: &[Column]) -> Result<Vec<Vec<Value>>, PersistenceError> {
        let rows: Vec<mysql::Row> = self.exec(sql, mysql_params(params)).map_err(mysql_error)?;
        rows.into_iter().map(|row| columns.iter().enumerate().map(|(i, col)| {
            let bad = || PersistenceError::InternalDataInconsistency(format!("invalid SQL column {i}"));
            match col {
                Column::I32 => row.get_opt::<i32,_>(i).ok_or_else(bad)?.map(Value::I32).map_err(internal),
                Column::I64 => row.get_opt::<i64,_>(i).ok_or_else(bad)?.map(Value::I64).map_err(internal),
                Column::Bytes => row.get_opt::<Vec<u8>,_>(i).ok_or_else(bad)?.map(Value::Bytes).map_err(internal),
                Column::Text => row.get_opt::<String,_>(i).ok_or_else(bad)?.map(Value::Text).map_err(internal),
            }
        }).collect()).collect()
    }
}
impl SqlTx for postgres::Transaction<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, PersistenceError> { postgres::Transaction::execute(self, sql, &postgres_params(params)).map_err(postgres_error) }
    fn query(&mut self, sql: &str, params: &[Value], columns: &[Column]) -> Result<Vec<Vec<Value>>, PersistenceError> {
        postgres::Transaction::query(self, sql, &postgres_params(params)).map_err(postgres_error)?.into_iter().map(|row| columns.iter().enumerate().map(|(i,col)| match col {
            Column::I32 => row.try_get(i).map(Value::I32).map_err(internal),
            Column::I64 => row.try_get(i).map(Value::I64).map_err(internal),
            Column::Bytes => row.try_get(i).map(Value::Bytes).map_err(internal),
            Column::Text => row.try_get(i).map(Value::Text).map_err(internal),
        }).collect()).collect()
    }
}
impl SqlBackend {
    pub fn mysql(store: &MySqlTaskStore) -> Self { Self::MySql(store.history_pool()) }
    pub fn postgres(store: &PostgresTaskStore) -> Self { Self::Postgres(store.history_pool()) }
    pub fn dialect(&self) -> Dialect { match self { Self::MySql(_) => Dialect::MySql, Self::Postgres(_) => Dialect::Postgres } }
    pub fn transaction<T>(&self, run: impl FnOnce(&mut dyn SqlTx) -> Result<T, PersistenceError>) -> Result<T, PersistenceError> {
        match self {
            Self::MySql(pool) => { let mut conn = pool.checkout().map_err(internal)?; let mut tx = conn.start_transaction(mysql::TxOpts::default()).map_err(mysql_error)?; let result = run(&mut tx)?; tx.commit().map_err(mysql_error)?; Ok(result) }
            Self::Postgres(pool) => { let mut conn = pool.checkout().map_err(internal)?; let mut tx = conn.transaction().map_err(postgres_error)?; let result = run(&mut tx)?; tx.commit().map_err(postgres_error)?; Ok(result) }
        }
    }
}
impl Value {
    pub fn integer(&self) -> Result<i64, PersistenceError> { match self { Self::I32(v) => Ok(i64::from(*v)), Self::I64(v) => Ok(*v), _ => Err(internal("expected SQL integer")) } }
    pub fn bytes(&self) -> Result<Vec<u8>, PersistenceError> { match self { Self::Bytes(v) => Ok(v.clone()), _ => Err(internal("expected SQL bytes")) } }
    pub fn text(&self) -> Result<String, PersistenceError> { match self { Self::Text(v) => Ok(v.clone()), _ => Err(internal("expected SQL text")) } }
}
pub fn uuid(value: &str) -> Result<Value, PersistenceError> { uuid::Uuid::parse_str(value).map(|v| Value::Bytes(v.as_bytes().to_vec())).map_err(|e| PersistenceError::InvalidPersistenceRequest(e.to_string())) }
pub fn uuid_string(value: &Value) -> Result<String, PersistenceError> { uuid::Uuid::from_slice(&value.bytes()?).map(|v| v.to_string()).map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string())) }
