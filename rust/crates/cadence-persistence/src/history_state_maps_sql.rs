//! SQL CRUD for the workflow-state maps used by History execution rows.

#![allow(clippy::too_many_arguments)]

use crate::{
    history_contract::{DataBlob, PersistenceError},
    history_sql_queries::{workflow_state_map_statement, Dialect, WorkflowMapOperation, WorkflowStateMap},
    matching_mysql::{MySqlTaskStore, SyncConnectionPool},
    matching_postgres::{PostgresConnectionPool, PostgresTaskStore},
};
use mysql::prelude::Queryable;
use postgres::types::ToSql;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowStateMapKey { Integer(i64), String(String) }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowStateMapEntry {
    pub shard_id: i32,
    pub domain_id: [u8; 16],
    pub workflow_id: String,
    pub run_id: [u8; 16],
    pub map: WorkflowStateMap,
    pub key: WorkflowStateMapKey,
    pub value: DataBlob,
    pub last_heartbeat_details: Vec<u8>,
    pub last_heartbeat_updated_time_unix_nanos: i64,
}

fn key_is_valid(map: WorkflowStateMap, key: &WorkflowStateMapKey) -> bool {
    matches!((map, key), (WorkflowStateMap::Timer, WorkflowStateMapKey::String(_)))
        || matches!((map, key), (WorkflowStateMap::Activity | WorkflowStateMap::ChildExecution | WorkflowStateMap::RequestCancel | WorkflowStateMap::Signal, WorkflowStateMapKey::Integer(_)))
}

fn invalid_key(map: WorkflowStateMap) -> PersistenceError {
    PersistenceError::InvalidArgument(format!("invalid key type for workflow-state map {map:?}"))
}

fn internal(error: impl std::fmt::Display) -> PersistenceError { PersistenceError::Internal(error.to_string()) }

fn heartbeat_time(unix_nanos: i64) -> Result<chrono::DateTime<chrono::Utc>, PersistenceError> {
    chrono::DateTime::from_timestamp(unix_nanos.div_euclid(1_000_000_000), unix_nanos.rem_euclid(1_000_000_000) as u32)
        .ok_or_else(|| PersistenceError::InvalidArgument("activity heartbeat timestamp is out of range".into()))
}

fn heartbeat_unix_nanos(value: chrono::DateTime<chrono::Utc>) -> Result<i64, PersistenceError> {
    value.timestamp_nanos_opt().ok_or_else(|| PersistenceError::InternalDataInconsistency("activity heartbeat timestamp is out of range".into()))
}

#[derive(Clone)]
pub struct MySqlWorkflowStateMaps { pool: Arc<SyncConnectionPool> }

impl MySqlWorkflowStateMaps {
    pub fn from_task_store(store: &MySqlTaskStore) -> Self { Self { pool: store.history_pool() } }

    pub fn upsert(&self, entry: &WorkflowStateMapEntry) -> Result<(), PersistenceError> {
        if !key_is_valid(entry.map, &entry.key) { return Err(invalid_key(entry.map)); }
        let query = workflow_state_map_statement(Dialect::MySql, entry.map, WorkflowMapOperation::Upsert, 0);
        let mut params = vec![entry.shard_id.into(), entry.domain_id.to_vec().into(), entry.workflow_id.clone().into()];
        params.push(entry.run_id.to_vec().into());
        params.push(match &entry.key { WorkflowStateMapKey::Integer(value) => (*value).into(), WorkflowStateMapKey::String(value) => value.clone().into() });
        params.push(entry.value.data.clone().into());
        params.push(entry.value.encoding.clone().into());
        if entry.map == WorkflowStateMap::Activity {
            params.push(entry.last_heartbeat_details.clone().into());
            params.push(mysql::Value::from(heartbeat_time(entry.last_heartbeat_updated_time_unix_nanos)?.naive_utc()));
        }
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(query, mysql::Params::Positional(params)).map_err(internal)
    }

    pub fn get(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<Vec<WorkflowStateMapEntry>, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::MySql, map, WorkflowMapOperation::Select, 0);
        let mut conn = self.pool.checkout().map_err(internal)?;
        let rows: Vec<mysql::Row> = conn.exec(query, (shard_id, domain_id.as_slice(), workflow_id, run_id.as_slice())).map_err(internal)?;
        rows.into_iter().map(|row| {
            let key = if map == WorkflowStateMap::Timer {
                WorkflowStateMapKey::String(row.get(0).ok_or_else(|| PersistenceError::InternalDataInconsistency("timer map row has no timer_id".into()))?)
            } else {
                WorkflowStateMapKey::Integer(row.get(0).ok_or_else(|| PersistenceError::InternalDataInconsistency("workflow map row has no integer key".into()))?)
            };
            let (details, updated) = if map == WorkflowStateMap::Activity {
                (row.get(3).ok_or_else(|| PersistenceError::InternalDataInconsistency("activity map row has no heartbeat details".into()))?, heartbeat_unix_nanos(row.get::<chrono::NaiveDateTime, _>(4).ok_or_else(|| PersistenceError::InternalDataInconsistency("activity map row has no heartbeat timestamp".into()))?.and_utc())?)
            } else { (Vec::new(), 0) };
            Ok(WorkflowStateMapEntry {
                shard_id, domain_id: *domain_id, workflow_id: workflow_id.to_owned(), run_id: *run_id, map, key,
                value: DataBlob {
                    data: row.get(1).ok_or_else(|| PersistenceError::InternalDataInconsistency("workflow map row has no data".into()))?,
                    encoding: row.get(2).ok_or_else(|| PersistenceError::InternalDataInconsistency("workflow map row has no data_encoding".into()))?,
                }, last_heartbeat_details: details, last_heartbeat_updated_time_unix_nanos: updated,
            })
        }).collect()
    }

    pub fn delete_all(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<u64, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::MySql, map, WorkflowMapOperation::DeleteAll, 0);
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(query, (shard_id, domain_id.as_slice(), workflow_id, run_id.as_slice())).map_err(internal)?;
        Ok(conn.affected_rows())
    }

    pub fn delete_keys(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap, keys: &[WorkflowStateMapKey]) -> Result<u64, PersistenceError> {
        if keys.iter().any(|key| !key_is_valid(map, key)) { return Err(invalid_key(map)); }
        if keys.is_empty() { return Ok(0); }
        let query = workflow_state_map_statement(Dialect::MySql, map, WorkflowMapOperation::DeleteKeys, keys.len());
        let mut params = vec![shard_id.into(), domain_id.to_vec().into(), workflow_id.to_owned().into(), run_id.to_vec().into()];
        params.extend(keys.iter().map(|key| match key { WorkflowStateMapKey::Integer(value) => (*value).into(), WorkflowStateMapKey::String(value) => value.clone().into() }));
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(query, mysql::Params::Positional(params)).map_err(internal)?;
        Ok(conn.affected_rows())
    }

    // These variants join the caller's shard-locked execution transaction.
    pub fn upsert_tx(conn: &mut mysql::Transaction<'_>, entry: &WorkflowStateMapEntry) -> Result<(), PersistenceError> {
        if !key_is_valid(entry.map, &entry.key) { return Err(invalid_key(entry.map)); }
        let query = workflow_state_map_statement(Dialect::MySql, entry.map, WorkflowMapOperation::Upsert, 0);
        let mut params = vec![entry.shard_id.into(), entry.domain_id.to_vec().into(), entry.workflow_id.clone().into()];
        params.push(entry.run_id.to_vec().into());
        params.push(match &entry.key { WorkflowStateMapKey::Integer(value) => (*value).into(), WorkflowStateMapKey::String(value) => value.clone().into() });
        params.push(entry.value.data.clone().into());
        params.push(entry.value.encoding.clone().into());
        if entry.map == WorkflowStateMap::Activity {
            params.push(entry.last_heartbeat_details.clone().into());
            params.push(mysql::Value::from(heartbeat_time(entry.last_heartbeat_updated_time_unix_nanos)?.naive_utc()));
        }
        conn.exec_drop(query, mysql::Params::Positional(params)).map_err(internal)
    }

    pub fn get_tx(conn: &mut mysql::Transaction<'_>, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<Vec<WorkflowStateMapEntry>, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::MySql, map, WorkflowMapOperation::Select, 0);
        let rows: Vec<mysql::Row> = conn.exec(query, (shard_id, domain_id.as_slice(), workflow_id, run_id.as_slice())).map_err(internal)?;
        rows.into_iter().map(|row| {
            let key = if map == WorkflowStateMap::Timer {
                WorkflowStateMapKey::String(row.get(0).ok_or_else(|| PersistenceError::InternalDataInconsistency("timer map row has no timer_id".into()))?)
            } else {
                WorkflowStateMapKey::Integer(row.get(0).ok_or_else(|| PersistenceError::InternalDataInconsistency("workflow map row has no integer key".into()))?)
            };
            let (details, updated) = if map == WorkflowStateMap::Activity {
                (row.get(3).ok_or_else(|| PersistenceError::InternalDataInconsistency("activity map row has no heartbeat details".into()))?, heartbeat_unix_nanos(row.get::<chrono::NaiveDateTime, _>(4).ok_or_else(|| PersistenceError::InternalDataInconsistency("activity map row has no heartbeat timestamp".into()))?.and_utc())?)
            } else { (Vec::new(), 0) };
            Ok(WorkflowStateMapEntry {
                shard_id, domain_id: *domain_id, workflow_id: workflow_id.to_owned(), run_id: *run_id, map, key,
                value: DataBlob {
                    data: row.get(1).ok_or_else(|| PersistenceError::InternalDataInconsistency("workflow map row has no data".into()))?,
                    encoding: row.get(2).ok_or_else(|| PersistenceError::InternalDataInconsistency("workflow map row has no data_encoding".into()))?,
                }, last_heartbeat_details: details, last_heartbeat_updated_time_unix_nanos: updated,
            })
        }).collect()
    }

    pub fn delete_all_tx(conn: &mut mysql::Transaction<'_>, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<u64, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::MySql, map, WorkflowMapOperation::DeleteAll, 0);
        conn.exec_drop(query, (shard_id, domain_id.as_slice(), workflow_id, run_id.as_slice())).map_err(internal)?;
        Ok(conn.affected_rows())
    }

    pub fn delete_keys_tx(conn: &mut mysql::Transaction<'_>, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap, keys: &[WorkflowStateMapKey]) -> Result<u64, PersistenceError> {
        if keys.iter().any(|key| !key_is_valid(map, key)) { return Err(invalid_key(map)); }
        if keys.is_empty() { return Ok(0); }
        let query = workflow_state_map_statement(Dialect::MySql, map, WorkflowMapOperation::DeleteKeys, keys.len());
        let mut params = vec![shard_id.into(), domain_id.to_vec().into(), workflow_id.to_owned().into(), run_id.to_vec().into()];
        params.extend(keys.iter().map(|key| match key { WorkflowStateMapKey::Integer(value) => (*value).into(), WorkflowStateMapKey::String(value) => value.clone().into() }));
        conn.exec_drop(query, mysql::Params::Positional(params)).map_err(internal)?;
        Ok(conn.affected_rows())
    }
}

#[derive(Clone)]
pub struct PostgresWorkflowStateMaps { pool: Arc<PostgresConnectionPool> }

impl PostgresWorkflowStateMaps {
    pub fn from_task_store(store: &PostgresTaskStore) -> Self { Self { pool: store.history_pool() } }

    pub fn upsert(&self, entry: &WorkflowStateMapEntry) -> Result<(), PersistenceError> {
        if !key_is_valid(entry.map, &entry.key) { return Err(invalid_key(entry.map)); }
        let query = workflow_state_map_statement(Dialect::Postgres, entry.map, WorkflowMapOperation::Upsert, 0);
        let heartbeat_updated_at = if entry.map == WorkflowStateMap::Activity { Some(heartbeat_time(entry.last_heartbeat_updated_time_unix_nanos)?) } else { None };
        let domain = &entry.domain_id[..];
        let run = &entry.run_id[..];
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&entry.shard_id, &domain, &entry.workflow_id, &run];
        match &entry.key { WorkflowStateMapKey::Integer(value) => params.push(value), WorkflowStateMapKey::String(value) => params.push(value) }
        params.push(&entry.value.data);
        params.push(&entry.value.encoding);
        if entry.map == WorkflowStateMap::Activity {
            params.push(&entry.last_heartbeat_details);
            params.push(heartbeat_updated_at.as_ref().ok_or_else(|| PersistenceError::Internal("activity timestamp was not prepared".into()))?);
        }
        let mut client = self.pool.checkout().map_err(internal)?;
        client.execute(&query, &params).map_err(internal)?;
        Ok(())
    }

    pub fn get(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<Vec<WorkflowStateMapEntry>, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::Postgres, map, WorkflowMapOperation::Select, 0);
        let mut client = self.pool.checkout().map_err(internal)?;
        let rows = client.query(&query, &[&shard_id, &&domain_id[..], &workflow_id, &&run_id[..]]).map_err(internal)?;
        rows.into_iter().map(|row| {
            let key = if map == WorkflowStateMap::Timer { WorkflowStateMapKey::String(row.get(0)) } else { WorkflowStateMapKey::Integer(row.get(0)) };
            let (details, updated) = if map == WorkflowStateMap::Activity { (row.get(3), heartbeat_unix_nanos(row.get(4))?) } else { (Vec::new(), 0) };
            Ok(WorkflowStateMapEntry { shard_id, domain_id: *domain_id, workflow_id: workflow_id.to_owned(), run_id: *run_id, map, key,
                value: DataBlob { data: row.get(1), encoding: row.get(2) }, last_heartbeat_details: details, last_heartbeat_updated_time_unix_nanos: updated })
        }).collect()
    }

    pub fn delete_all(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<u64, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::Postgres, map, WorkflowMapOperation::DeleteAll, 0);
        let mut client = self.pool.checkout().map_err(internal)?;
        client.execute(&query, &[&shard_id, &&domain_id[..], &workflow_id, &&run_id[..]]).map_err(internal)
    }

    pub fn delete_keys(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap, keys: &[WorkflowStateMapKey]) -> Result<u64, PersistenceError> {
        if keys.iter().any(|key| !key_is_valid(map, key)) { return Err(invalid_key(map)); }
        if keys.is_empty() { return Ok(0); }
        let query = workflow_state_map_statement(Dialect::Postgres, map, WorkflowMapOperation::DeleteKeys, keys.len());
        let domain = &domain_id[..];
        let run = &run_id[..];
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&shard_id, &domain, &workflow_id, &run];
        for key in keys {
            match key { WorkflowStateMapKey::Integer(value) => params.push(value), WorkflowStateMapKey::String(value) => params.push(value) }
        }
        let mut client = self.pool.checkout().map_err(internal)?;
        client.execute(&query, &params).map_err(internal)
    }

    // These variants join the caller's shard-locked execution transaction.
    pub fn upsert_tx(client: &mut impl postgres::GenericClient, entry: &WorkflowStateMapEntry) -> Result<(), PersistenceError> {
        if !key_is_valid(entry.map, &entry.key) { return Err(invalid_key(entry.map)); }
        let query = workflow_state_map_statement(Dialect::Postgres, entry.map, WorkflowMapOperation::Upsert, 0);
        let heartbeat_updated_at = if entry.map == WorkflowStateMap::Activity { Some(heartbeat_time(entry.last_heartbeat_updated_time_unix_nanos)?) } else { None };
        let domain = &entry.domain_id[..];
        let run = &entry.run_id[..];
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&entry.shard_id, &domain, &entry.workflow_id, &run];
        match &entry.key { WorkflowStateMapKey::Integer(value) => params.push(value), WorkflowStateMapKey::String(value) => params.push(value) }
        params.push(&entry.value.data);
        params.push(&entry.value.encoding);
        if entry.map == WorkflowStateMap::Activity {
            params.push(&entry.last_heartbeat_details);
            params.push(heartbeat_updated_at.as_ref().ok_or_else(|| PersistenceError::Internal("activity timestamp was not prepared".into()))?);
        }
        client.execute(&query, &params).map_err(internal)?;
        Ok(())
    }

    pub fn get_tx(client: &mut impl postgres::GenericClient, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<Vec<WorkflowStateMapEntry>, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::Postgres, map, WorkflowMapOperation::Select, 0);
        let rows = client.query(&query, &[&shard_id, &&domain_id[..], &workflow_id, &&run_id[..]]).map_err(internal)?;
        rows.into_iter().map(|row| {
            let key = if map == WorkflowStateMap::Timer { WorkflowStateMapKey::String(row.get(0)) } else { WorkflowStateMapKey::Integer(row.get(0)) };
            let (details, updated) = if map == WorkflowStateMap::Activity { (row.get(3), heartbeat_unix_nanos(row.get(4))?) } else { (Vec::new(), 0) };
            Ok(WorkflowStateMapEntry { shard_id, domain_id: *domain_id, workflow_id: workflow_id.to_owned(), run_id: *run_id, map, key,
                value: DataBlob { data: row.get(1), encoding: row.get(2) }, last_heartbeat_details: details, last_heartbeat_updated_time_unix_nanos: updated })
        }).collect()
    }

    pub fn delete_all_tx(client: &mut impl postgres::GenericClient, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap) -> Result<u64, PersistenceError> {
        let query = workflow_state_map_statement(Dialect::Postgres, map, WorkflowMapOperation::DeleteAll, 0);
        client.execute(&query, &[&shard_id, &&domain_id[..], &workflow_id, &&run_id[..]]).map_err(internal)
    }

    pub fn delete_keys_tx(client: &mut impl postgres::GenericClient, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str, run_id: &[u8; 16], map: WorkflowStateMap, keys: &[WorkflowStateMapKey]) -> Result<u64, PersistenceError> {
        if keys.iter().any(|key| !key_is_valid(map, key)) { return Err(invalid_key(map)); }
        if keys.is_empty() { return Ok(0); }
        let query = workflow_state_map_statement(Dialect::Postgres, map, WorkflowMapOperation::DeleteKeys, keys.len());
        let domain = &domain_id[..];
        let run = &run_id[..];
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&shard_id, &domain, &workflow_id, &run];
        for key in keys {
            match key { WorkflowStateMapKey::Integer(value) => params.push(value), WorkflowStateMapKey::String(value) => params.push(value) }
        }
        client.execute(&query, &params).map_err(internal)
    }
}
