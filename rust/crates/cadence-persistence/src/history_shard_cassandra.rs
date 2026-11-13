//! Cassandra implementation of History's shard row contract
//! (Go `nosqlplugin/cassandra/shard.go`, `shard_cql.go`, and
//! `nosql_shard_store.go`).
//!
//! The shard lives in the `executions` table as the `type = rowTypeShard`
//! row. Writes are LWTs: create is `IF NOT EXISTS`, update is
//! `IF range_id = ?`. The thriftrw `data` blob is authoritative (Go
//! `ReadNoSQLShardFromDataBlob`); the typed `shard` UDT carries the identity
//! fields Go always writes (shard_id, range_id, updated_at).
use crate::cassandra::CassandraBlockingSession;
use crate::history_sql::{HistoryPersistenceError as E, ShardRow, ShardStore};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::{value::Row, SerializeValue};
use std::sync::Arc;
use uuid::Uuid;

/// Go constants.go `rowTypeShard*`.
const ROW_TYPE_SHARD: i32 = 0;
const SHARD_DOMAIN_ID: Uuid = Uuid::from_u128(0x10000000_1000_f000_f000_000000000000);
const SHARD_WORKFLOW_ID: &str = "20000000-1000-f000-f000-000000000000";
const SHARD_RUN_ID: Uuid = Uuid::from_u128(0x30000000_1000_f000_f000_000000000000);
const SHARD_TASK_ID: i64 = -11;
/// Go `defaultVisibilityTimestamp` (2000-01-01T00:00:00Z).
const DEFAULT_VISIBILITY_MS: i64 = 946_684_800_000;

/// Go `templateCreateShardQuery` (the UDT is bound whole instead of inlined).
const CREATE: &str = "INSERT INTO executions (shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, shard, data, data_encoding, range_id) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
/// Go `templateGetShardQuery`.
const GET: &str = "SELECT shard, data, data_encoding, range_id FROM executions WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ?";
/// Go `templateUpdateShardQuery`.
const UPDATE: &str = "UPDATE executions SET shard = ?, data = ?, data_encoding = ?, range_id = ? WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ? IF range_id = ?";
/// Go `templateUpdateRangeIDQuery`.
const UPDATE_RANGE_ID: &str = "UPDATE executions SET range_id = ? WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ? IF range_id = ?";

/// Go `shard` UDT (schema.cql). Only the fields History persists typed are set.
#[derive(Debug, Clone, Default, SerializeValue, scylla::DeserializeValue)]
struct ShardUdt {
    shard_id: Option<i32>,
    owner: Option<String>,
    range_id: Option<i64>,
    stolen_since_renew: Option<i32>,
    updated_at: Option<DateTime<Utc>>,
    replication_ack_level: Option<i64>,
    transfer_ack_level: Option<i64>,
    timer_ack_level: Option<DateTime<Utc>>,
    domain_notification_version: Option<i64>,
}

/// Read-side view of the `shard` UDT: Go writes empty (zero-byte) timestamps for fields like `timer_ack_level`,
/// which a typed timestamp rejects; History only needs `range_id` (the `data` blob is authoritative).
#[derive(Debug, Clone, Default, scylla::DeserializeValue)]
struct ShardUdtRead {
    range_id: Option<i64>,
}

fn visibility_ts() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(DEFAULT_VISIBILITY_MS).expect("valid default visibility timestamp")
}

fn internal(error: impl std::fmt::Display) -> E {
    E::Internal(format!("Cassandra shard: {error}"))
}

/// Go `CDB` shard operations over the shared Cassandra session.
#[derive(Clone)]
pub struct CassandraShardStore {
    driver: Arc<CassandraBlockingSession>,
}

impl CassandraShardStore {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver }
    }

    /// Blocking Scylla calls must not run on an async worker.
    fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(Arc<CassandraBlockingSession>) -> Result<T, E> + Send + 'static,
    ) -> impl std::future::Future<Output = Result<T, E>> {
        let driver = self.driver.clone();
        async move {
            tokio::task::spawn_blocking(move || f(driver))
                .await
                .map_err(|error| internal(format!("blocking task: {error}")))?
        }
    }

    /// Reads `[applied]` and `range_id` from an LWT result (Go `MapScanCAS`).
    fn applied_and_range(result: scylla::response::query_result::QueryResult) -> Result<(bool, i64), E> {
        let rows = result.into_rows_result().map_err(internal)?;
        let names: Vec<String> = rows.column_specs().iter().map(|s| s.name().to_owned()).collect();
        let row = rows
            .maybe_first_row::<Row>()
            .map_err(internal)?
            .ok_or_else(|| internal("LWT returned no result row"))?;
        let applied = matches!(row.columns.first(), Some(Some(scylla::value::CqlValue::Boolean(true))));
        let range_id = names
            .iter()
            .zip(&row.columns)
            .find_map(|(name, value)| match (name.as_str(), value) {
                ("range_id", Some(scylla::value::CqlValue::BigInt(v))) => Some(*v),
                _ => None,
            })
            .unwrap_or_default();
        Ok((applied, range_id))
    }

    /// Go `UpdateRangeID`: repairs the column when the UDT range is ahead.
    fn update_range_id(
        driver: &CassandraBlockingSession,
        shard_id: i32,
        range_id: i64,
        previous_range_id: i64,
    ) -> Result<(), E> {
        let session = driver.session();
        let result = driver
            .run(session.query_unpaged(
                UPDATE_RANGE_ID,
                (
                    range_id,
                    shard_id,
                    ROW_TYPE_SHARD,
                    SHARD_DOMAIN_ID,
                    SHARD_WORKFLOW_ID,
                    SHARD_RUN_ID,
                    visibility_ts(),
                    SHARD_TASK_ID,
                    previous_range_id,
                ),
            ))
            .map_err(internal)?;
        let (applied, current) = Self::applied_and_range(result)?;
        if applied {
            Ok(())
        } else {
            Err(E::ShardOwnershipLost { shard_id, previous_range_id, current_range_id: current })
        }
    }
}

#[async_trait]
impl ShardStore for CassandraShardStore {
    async fn create_shard(&self, row: ShardRow) -> Result<(), E> {
        self.blocking(move |driver| {
            let udt = ShardUdt {
                shard_id: Some(row.shard_id),
                range_id: Some(row.range_id),
                updated_at: Some(Utc::now()),
                ..Default::default()
            };
            let session = driver.session();
            let result = driver
                .run(session.query_unpaged(
                    CREATE,
                    (
                        row.shard_id,
                        ROW_TYPE_SHARD,
                        SHARD_DOMAIN_ID,
                        SHARD_WORKFLOW_ID,
                        SHARD_RUN_ID,
                        visibility_ts(),
                        SHARD_TASK_ID,
                        udt,
                        row.data,
                        row.data_encoding,
                        row.range_id,
                    ),
                ))
                .map_err(internal)?;
            let (applied, current) = Self::applied_and_range(result)?;
            if applied {
                Ok(())
            } else {
                // Go convertToConflictedShardRow -> ShardOperationConditionFailure.
                Err(E::ShardOwnershipLost {
                    shard_id: row.shard_id,
                    previous_range_id: row.range_id,
                    current_range_id: current,
                })
            }
        })
        .await
    }

    async fn get_shard(&self, shard_id: i32) -> Result<ShardRow, E> {
        self.blocking(move |driver| {
            let session = driver.session();
            let result = driver
                .run(session.query_unpaged(
                    GET,
                    (
                        shard_id,
                        ROW_TYPE_SHARD,
                        SHARD_DOMAIN_ID,
                        SHARD_WORKFLOW_ID,
                        SHARD_RUN_ID,
                        visibility_ts(),
                        SHARD_TASK_ID,
                    ),
                ))
                .map_err(internal)?;
            let rows = result.into_rows_result().map_err(internal)?;
            let (udt, data, encoding, range_id) = rows
                .maybe_first_row::<(Option<ShardUdtRead>, Option<Vec<u8>>, Option<String>, Option<i64>)>()
                .map_err(internal)?
                .ok_or_else(|| E::EntityNotExists(format!("shard {shard_id} does not exist")))?;
            let column_range = range_id.unwrap_or_default();
            // Go GetShard: when the UDT range is ahead of the column, repair
            // the column and use the UDT value.
            let udt_range = udt.and_then(|u| u.range_id).unwrap_or(column_range);
            if udt_range > column_range {
                Self::update_range_id(&driver, shard_id, udt_range, column_range)?;
            }
            Ok(ShardRow {
                shard_id,
                range_id: udt_range.max(column_range),
                data: data.unwrap_or_default(),
                data_encoding: encoding.unwrap_or_default(),
            })
        })
        .await
    }

    async fn update_shard(&self, row: ShardRow, previous_range_id: i64) -> Result<(), E> {
        self.blocking(move |driver| {
            let udt = ShardUdt {
                shard_id: Some(row.shard_id),
                range_id: Some(row.range_id),
                updated_at: Some(Utc::now()),
                ..Default::default()
            };
            let session = driver.session();
            let result = driver
                .run(session.query_unpaged(
                    UPDATE,
                    (
                        udt,
                        row.data,
                        row.data_encoding,
                        row.range_id,
                        row.shard_id,
                        ROW_TYPE_SHARD,
                        SHARD_DOMAIN_ID,
                        SHARD_WORKFLOW_ID,
                        SHARD_RUN_ID,
                        visibility_ts(),
                        SHARD_TASK_ID,
                        previous_range_id,
                    ),
                ))
                .map_err(internal)?;
            let (applied, current) = Self::applied_and_range(result)?;
            if applied {
                Ok(())
            } else {
                Err(E::ShardOwnershipLost {
                    shard_id: row.shard_id,
                    previous_range_id,
                    current_range_id: current,
                })
            }
        })
        .await
    }
}
