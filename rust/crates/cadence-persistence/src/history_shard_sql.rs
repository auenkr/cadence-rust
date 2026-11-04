//! MySQL and PostgreSQL implementations of History's shard row contract.
//! The connection pools are shared with the existing SQL persistence code.

use crate::{history_sql::{HistoryPersistenceError as E, ShardRow, ShardStore}, matching_mysql::{MySqlTaskStore, SyncConnectionPool}, matching_postgres::{PostgresConnectionPool, PostgresTaskStore}};
use async_trait::async_trait;
use mysql::prelude::Queryable;
use std::sync::Arc;

#[derive(Clone)]
pub struct MySqlShardStore { pool: Arc<SyncConnectionPool> }

impl MySqlShardStore {
    pub fn from_task_store(store: &MySqlTaskStore) -> Self { Self { pool: store.history_pool() } }
    fn error(error: impl std::fmt::Display) -> E { E::Internal(error.to_string()) }
}

#[async_trait]
impl ShardStore for MySqlShardStore {
    async fn create_shard(&self, row: ShardRow) -> Result<(), E> {
        let mut conn = self.pool.checkout().map_err(Self::error)?;
        conn.exec_drop(crate::history_sql_queries::Dialect::MySql.statement(crate::history_sql_queries::Statement::CreateShard), (row.shard_id, row.range_id, row.data, row.data_encoding)).map_err(Self::error)
    }
    async fn get_shard(&self, shard_id: i32) -> Result<ShardRow, E> {
        let mut conn = self.pool.checkout().map_err(Self::error)?;
        conn.exec_first(crate::history_sql_queries::Dialect::MySql.statement(crate::history_sql_queries::Statement::GetShard), (shard_id,)).map_err(Self::error)?.map(|(shard_id, range_id, data, data_encoding)| ShardRow { shard_id, range_id, data, data_encoding }).ok_or_else(|| E::EntityNotExists(format!("shard {shard_id} does not exist")))
    }
    async fn update_shard(&self, row: ShardRow, previous_range_id: i64) -> Result<(), E> {
        let mut conn = self.pool.checkout().map_err(Self::error)?;
        let mut tx = conn.start_transaction(mysql::TxOpts::default()).map_err(Self::error)?;
        let found: Option<i64> = tx.exec_first(crate::history_sql_queries::Dialect::MySql.statement(crate::history_sql_queries::Statement::LockShard), (row.shard_id,)).map_err(Self::error)?;
        let Some(found) = found else { return Err(E::Internal(format!("shard {} does not exist", row.shard_id))); };
        if found != previous_range_id { return Err(E::ShardOwnershipLost { shard_id: row.shard_id, previous_range_id, current_range_id: found }); }
        tx.exec_drop(crate::history_sql_queries::Dialect::MySql.statement(crate::history_sql_queries::Statement::UpdateShard), (row.range_id, row.data, row.data_encoding, row.shard_id)).map_err(Self::error)?;
        if tx.affected_rows() != 1 { return Err(E::Internal(format!("shard {} update affected an unexpected number of rows", row.shard_id))); }
        tx.commit().map_err(Self::error)
    }
}

#[derive(Clone)]
pub struct PostgresShardStore { pool: Arc<PostgresConnectionPool> }

impl PostgresShardStore {
    pub fn from_task_store(store: &PostgresTaskStore) -> Self { Self { pool: store.history_pool() } }
    fn error(error: impl std::fmt::Display) -> E { E::Internal(error.to_string()) }
}

#[async_trait]
impl ShardStore for PostgresShardStore {
    async fn create_shard(&self, row: ShardRow) -> Result<(), E> {
        let mut client = self.pool.checkout().map_err(Self::error)?;
        client.execute(crate::history_sql_queries::Dialect::Postgres.statement(crate::history_sql_queries::Statement::CreateShard), &[&row.shard_id, &row.range_id, &row.data, &row.data_encoding]).map_err(Self::error)?;
        Ok(())
    }
    async fn get_shard(&self, shard_id: i32) -> Result<ShardRow, E> {
        let mut client = self.pool.checkout().map_err(Self::error)?;
        let row = client.query_opt(crate::history_sql_queries::Dialect::Postgres.statement(crate::history_sql_queries::Statement::GetShard), &[&shard_id]).map_err(Self::error)?.ok_or_else(|| E::EntityNotExists(format!("shard {shard_id} does not exist")))?;
        Ok(ShardRow { shard_id: row.get(0), range_id: row.get(1), data: row.get(2), data_encoding: row.get(3) })
    }
    async fn update_shard(&self, row: ShardRow, previous_range_id: i64) -> Result<(), E> {
        let mut client = self.pool.checkout().map_err(Self::error)?;
        let mut tx = client.transaction().map_err(Self::error)?;
        let found = tx.query_opt(crate::history_sql_queries::Dialect::Postgres.statement(crate::history_sql_queries::Statement::LockShard), &[&row.shard_id]).map_err(Self::error)?.map(|r| r.get::<_, i64>(0));
        let Some(found) = found else { return Err(E::Internal(format!("shard {} does not exist", row.shard_id))); };
        if found != previous_range_id { return Err(E::ShardOwnershipLost { shard_id: row.shard_id, previous_range_id, current_range_id: found }); }
        let affected = tx.execute(crate::history_sql_queries::Dialect::Postgres.statement(crate::history_sql_queries::Statement::UpdateShard), &[&row.range_id, &row.data, &row.data_encoding, &row.shard_id]).map_err(Self::error)?;
        if affected != 1 { return Err(E::Internal(format!("shard {} update affected {affected} rows", row.shard_id))); }
        tx.commit().map_err(Self::error)
    }
}
