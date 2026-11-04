//! SQL helpers for signals-requested IDs and buffered history events.
//!
//! These tables are keyed by workflow identity. Signal IDs are set members;
//! inserts are idempotent on both supported SQL dialects.

#![allow(clippy::too_many_arguments)]

use crate::{
    history_contract::{DataBlob, PersistenceError},
    history_sql_queries::{Dialect, Statement},
    matching_mysql::{MySqlTaskStore, SyncConnectionPool},
    matching_postgres::{PostgresConnectionPool, PostgresTaskStore},
};
use mysql::prelude::Queryable;
use postgres::types::ToSql;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Debug, Clone, Copy)]
pub struct WorkflowSqlKey<'a> {
    pub shard_id: i32,
    pub domain_id: &'a [u8; 16],
    pub workflow_id: &'a str,
    pub run_id: &'a [u8; 16],
}

fn internal(error: impl std::fmt::Display) -> PersistenceError {
    PersistenceError::Internal(error.to_string())
}

#[derive(Clone)]
pub struct MySqlWorkflowNonMaps {
    pool: Arc<SyncConnectionPool>,
}

impl MySqlWorkflowNonMaps {
    pub fn from_task_store(store: &MySqlTaskStore) -> Self {
        Self {
            pool: store.history_pool(),
        }
    }

    pub fn update_signals_requested(
        &self,
        key: WorkflowSqlKey<'_>,
        insert: &[String],
        delete: &[String],
    ) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let insert_sql = Dialect::MySql.statement(Statement::InsertSignalRequested);
        for signal_id in insert {
            conn.exec_drop(
                insert_sql,
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id,
                    key.run_id.as_slice(),
                    signal_id,
                ),
            )
            .map_err(internal)?;
        }
        if !delete.is_empty() {
            let sql = crate::history_sql_queries::delete_signal_requested_keys_statement(
                Dialect::MySql,
                delete.len(),
            );
            let mut params = vec![
                key.shard_id.into(),
                key.domain_id.to_vec().into(),
                key.workflow_id.to_owned().into(),
                key.run_id.to_vec().into(),
            ];
            params.extend(delete.iter().cloned().map(Into::into));
            conn.exec_drop(sql, mysql::Params::Positional(params))
                .map_err(internal)?;
        }
        Ok(())
    }

    pub fn get_signals_requested(
        &self,
        key: WorkflowSqlKey<'_>,
    ) -> Result<BTreeSet<String>, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let rows: Vec<String> = conn
            .exec(
                Dialect::MySql.statement(Statement::SelectSignalRequested),
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id,
                    key.run_id.as_slice(),
                ),
            )
            .map_err(internal)?;
        Ok(rows.into_iter().collect())
    }

    pub fn delete_signals_requested(
        &self,
        key: WorkflowSqlKey<'_>,
    ) -> Result<u64, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(
            Dialect::MySql.statement(Statement::DeleteSignalRequested),
            (
                key.shard_id,
                key.domain_id.as_slice(),
                key.workflow_id,
                key.run_id.as_slice(),
            ),
        )
        .map_err(internal)?;
        Ok(conn.affected_rows())
    }

    pub fn append_buffered_event(
        &self,
        key: WorkflowSqlKey<'_>,
        event: &DataBlob,
    ) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(
            Dialect::MySql.statement(Statement::InsertBufferedEvent),
            (
                key.shard_id,
                key.domain_id.as_slice(),
                key.workflow_id,
                key.run_id.as_slice(),
                &event.data,
                &event.encoding,
            ),
        )
        .map_err(internal)
    }

    pub fn get_buffered_events(
        &self,
        key: WorkflowSqlKey<'_>,
    ) -> Result<Vec<DataBlob>, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let rows: Vec<(Vec<u8>, String)> = conn
            .exec(
                Dialect::MySql.statement(Statement::SelectBufferedEvents),
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id,
                    key.run_id.as_slice(),
                ),
            )
            .map_err(internal)?;
        Ok(rows
            .into_iter()
            .map(|(data, encoding)| DataBlob { data, encoding })
            .collect())
    }

    pub fn delete_buffered_events(&self, key: WorkflowSqlKey<'_>) -> Result<u64, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(
            Dialect::MySql.statement(Statement::DeleteBufferedEvents),
            (
                key.shard_id,
                key.domain_id.as_slice(),
                key.workflow_id,
                key.run_id.as_slice(),
            ),
        )
        .map_err(internal)?;
        Ok(conn.affected_rows())
    }

    // These variants join the caller's shard-locked execution transaction.
    pub fn update_signals_requested_tx(
        conn: &mut mysql::Transaction<'_>,
        key: WorkflowSqlKey<'_>,
        insert: &[String],
        delete: &[String],
    ) -> Result<(), PersistenceError> {
        let insert_sql = Dialect::MySql.statement(Statement::InsertSignalRequested);
        for signal_id in insert {
            conn.exec_drop(
                insert_sql,
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id,
                    key.run_id.as_slice(),
                    signal_id,
                ),
            )
            .map_err(internal)?;
        }
        if !delete.is_empty() {
            let sql = crate::history_sql_queries::delete_signal_requested_keys_statement(
                Dialect::MySql,
                delete.len(),
            );
            let mut params = vec![
                key.shard_id.into(),
                key.domain_id.to_vec().into(),
                key.workflow_id.to_owned().into(),
                key.run_id.to_vec().into(),
            ];
            params.extend(delete.iter().cloned().map(Into::into));
            conn.exec_drop(sql, mysql::Params::Positional(params))
                .map_err(internal)?;
        }
        Ok(())
    }

    pub fn get_signals_requested_tx(
        conn: &mut mysql::Transaction<'_>,
        key: WorkflowSqlKey<'_>,
    ) -> Result<BTreeSet<String>, PersistenceError> {
        let rows: Vec<String> = conn
            .exec(
                Dialect::MySql.statement(Statement::SelectSignalRequested),
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id,
                    key.run_id.as_slice(),
                ),
            )
            .map_err(internal)?;
        Ok(rows.into_iter().collect())
    }

    pub fn delete_signals_requested_tx(
        conn: &mut mysql::Transaction<'_>,
        key: WorkflowSqlKey<'_>,
    ) -> Result<u64, PersistenceError> {
        conn.exec_drop(
            Dialect::MySql.statement(Statement::DeleteSignalRequested),
            (
                key.shard_id,
                key.domain_id.as_slice(),
                key.workflow_id,
                key.run_id.as_slice(),
            ),
        )
        .map_err(internal)?;
        Ok(conn.affected_rows())
    }

    pub fn append_buffered_event_tx(
        conn: &mut mysql::Transaction<'_>,
        key: WorkflowSqlKey<'_>,
        event: &DataBlob,
    ) -> Result<(), PersistenceError> {
        conn.exec_drop(
            Dialect::MySql.statement(Statement::InsertBufferedEvent),
            (
                key.shard_id,
                key.domain_id.as_slice(),
                key.workflow_id,
                key.run_id.as_slice(),
                &event.data,
                &event.encoding,
            ),
        )
        .map_err(internal)
    }

    pub fn get_buffered_events_tx(
        conn: &mut mysql::Transaction<'_>,
        key: WorkflowSqlKey<'_>,
    ) -> Result<Vec<DataBlob>, PersistenceError> {
        let rows: Vec<(Vec<u8>, String)> = conn
            .exec(
                Dialect::MySql.statement(Statement::SelectBufferedEvents),
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id,
                    key.run_id.as_slice(),
                ),
            )
            .map_err(internal)?;
        Ok(rows
            .into_iter()
            .map(|(data, encoding)| DataBlob { data, encoding })
            .collect())
    }

    pub fn delete_buffered_events_tx(
        conn: &mut mysql::Transaction<'_>,
        key: WorkflowSqlKey<'_>,
    ) -> Result<u64, PersistenceError> {
        conn.exec_drop(
            Dialect::MySql.statement(Statement::DeleteBufferedEvents),
            (
                key.shard_id,
                key.domain_id.as_slice(),
                key.workflow_id,
                key.run_id.as_slice(),
            ),
        )
        .map_err(internal)?;
        Ok(conn.affected_rows())
    }
}

#[derive(Clone)]
pub struct PostgresWorkflowNonMaps {
    pool: Arc<PostgresConnectionPool>,
}

impl PostgresWorkflowNonMaps {
    pub fn from_task_store(store: &PostgresTaskStore) -> Self {
        Self {
            pool: store.history_pool(),
        }
    }

    pub fn update_signals_requested(
        &self,
        key: WorkflowSqlKey<'_>,
        insert: &[String],
        delete: &[String],
    ) -> Result<(), PersistenceError> {
        let mut client = self.pool.checkout().map_err(internal)?;
        let insert_sql = Dialect::Postgres.statement(Statement::InsertSignalRequested);
        for signal_id in insert {
            client
                .execute(
                    insert_sql,
                    &[
                        &key.shard_id,
                        &&key.domain_id[..],
                        &key.workflow_id,
                        &&key.run_id[..],
                        signal_id,
                    ],
                )
                .map_err(internal)?;
        }
        if !delete.is_empty() {
            let sql = crate::history_sql_queries::delete_signal_requested_keys_statement(
                Dialect::Postgres,
                delete.len(),
            );
            let domain = &key.domain_id[..];
            let run = &key.run_id[..];
            let mut params: Vec<&(dyn ToSql + Sync)> =
                vec![&key.shard_id, &domain, &key.workflow_id, &run];
            params.extend(
                delete
                    .iter()
                    .map(|signal_id| signal_id as &(dyn ToSql + Sync)),
            );
            client.execute(&sql, &params).map_err(internal)?;
        }
        Ok(())
    }

    pub fn get_signals_requested(
        &self,
        key: WorkflowSqlKey<'_>,
    ) -> Result<BTreeSet<String>, PersistenceError> {
        let mut client = self.pool.checkout().map_err(internal)?;
        let rows = client
            .query(
                Dialect::Postgres.statement(Statement::SelectSignalRequested),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    pub fn delete_signals_requested(
        &self,
        key: WorkflowSqlKey<'_>,
    ) -> Result<u64, PersistenceError> {
        let mut client = self.pool.checkout().map_err(internal)?;
        client
            .execute(
                Dialect::Postgres.statement(Statement::DeleteSignalRequested),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)
    }

    pub fn append_buffered_event(
        &self,
        key: WorkflowSqlKey<'_>,
        event: &DataBlob,
    ) -> Result<(), PersistenceError> {
        let mut client = self.pool.checkout().map_err(internal)?;
        client
            .execute(
                Dialect::Postgres.statement(Statement::InsertBufferedEvent),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                    &event.data,
                    &event.encoding,
                ],
            )
            .map_err(internal)?;
        Ok(())
    }

    pub fn get_buffered_events(
        &self,
        key: WorkflowSqlKey<'_>,
    ) -> Result<Vec<DataBlob>, PersistenceError> {
        let mut client = self.pool.checkout().map_err(internal)?;
        let rows = client
            .query(
                Dialect::Postgres.statement(Statement::SelectBufferedEvents),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)?;
        Ok(rows
            .into_iter()
            .map(|row| DataBlob {
                data: row.get(0),
                encoding: row.get(1),
            })
            .collect())
    }

    pub fn delete_buffered_events(&self, key: WorkflowSqlKey<'_>) -> Result<u64, PersistenceError> {
        let mut client = self.pool.checkout().map_err(internal)?;
        client
            .execute(
                Dialect::Postgres.statement(Statement::DeleteBufferedEvents),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)
    }

    // These variants join the caller's shard-locked execution transaction.
    pub fn update_signals_requested_tx(
        client: &mut impl postgres::GenericClient,
        key: WorkflowSqlKey<'_>,
        insert: &[String],
        delete: &[String],
    ) -> Result<(), PersistenceError> {
        let insert_sql = Dialect::Postgres.statement(Statement::InsertSignalRequested);
        for signal_id in insert {
            client
                .execute(
                    insert_sql,
                    &[
                        &key.shard_id,
                        &&key.domain_id[..],
                        &key.workflow_id,
                        &&key.run_id[..],
                        signal_id,
                    ],
                )
                .map_err(internal)?;
        }
        if !delete.is_empty() {
            let sql = crate::history_sql_queries::delete_signal_requested_keys_statement(
                Dialect::Postgres,
                delete.len(),
            );
            let domain = &key.domain_id[..];
            let run = &key.run_id[..];
            let mut params: Vec<&(dyn ToSql + Sync)> =
                vec![&key.shard_id, &domain, &key.workflow_id, &run];
            params.extend(
                delete
                    .iter()
                    .map(|signal_id| signal_id as &(dyn ToSql + Sync)),
            );
            client.execute(&sql, &params).map_err(internal)?;
        }
        Ok(())
    }

    pub fn get_signals_requested_tx(
        client: &mut impl postgres::GenericClient,
        key: WorkflowSqlKey<'_>,
    ) -> Result<BTreeSet<String>, PersistenceError> {
        let rows = client
            .query(
                Dialect::Postgres.statement(Statement::SelectSignalRequested),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    pub fn delete_signals_requested_tx(
        client: &mut impl postgres::GenericClient,
        key: WorkflowSqlKey<'_>,
    ) -> Result<u64, PersistenceError> {
        client
            .execute(
                Dialect::Postgres.statement(Statement::DeleteSignalRequested),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)
    }

    pub fn append_buffered_event_tx(
        client: &mut impl postgres::GenericClient,
        key: WorkflowSqlKey<'_>,
        event: &DataBlob,
    ) -> Result<(), PersistenceError> {
        client
            .execute(
                Dialect::Postgres.statement(Statement::InsertBufferedEvent),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                    &event.data,
                    &event.encoding,
                ],
            )
            .map_err(internal)?;
        Ok(())
    }

    pub fn get_buffered_events_tx(
        client: &mut impl postgres::GenericClient,
        key: WorkflowSqlKey<'_>,
    ) -> Result<Vec<DataBlob>, PersistenceError> {
        let rows = client
            .query(
                Dialect::Postgres.statement(Statement::SelectBufferedEvents),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)?;
        Ok(rows
            .into_iter()
            .map(|row| DataBlob {
                data: row.get(0),
                encoding: row.get(1),
            })
            .collect())
    }

    pub fn delete_buffered_events_tx(
        client: &mut impl postgres::GenericClient,
        key: WorkflowSqlKey<'_>,
    ) -> Result<u64, PersistenceError> {
        client
            .execute(
                Dialect::Postgres.statement(Statement::DeleteBufferedEvents),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)
    }
}
