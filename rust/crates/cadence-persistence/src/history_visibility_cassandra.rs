//! Cassandra visibility writer: port of the write half of
//! `go/common/persistence/nosql/nosqlplugin/cassandra/visibility.go` and `visibility_cql.go`
//! (`InsertVisibility`, `UpdateVisibility`, `DeleteVisibility`).
//! Times are unix nanoseconds (Go `UnixNanoToDBTimestamp` truncates to milliseconds).
use crate::cassandra::CassandraBlockingSession;
use crate::history_contract::PersistenceError;
use crate::history_execution_cassandra::{internal, parse_uuid};
use crate::history_execution_cassandra_write::ms_ts;
use crate::matching_cassandra_exec::{bound, BoundValues};
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::Statement;
use std::sync::Arc;

type E = PersistenceError;

/// Go `maxCassandraTTL`.
const MAX_CASSANDRA_TTL: i64 = 157_680_000;
/// Go `domainPartition`.
const DOMAIN_PARTITION: i32 = 0;

const OPEN_COLS: &str = "(domain_id, domain_partition,  workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time )";
const CLOSED_COLS: &str = "(domain_id, domain_partition,  workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time )";
const DELETE_OPEN: &str = "DELETE FROM open_executions WHERE domain_id = ? AND domain_partition = ? AND start_time = ? AND run_id = ?";

/// Go `nosqlplugin.VisibilityRow` (+ the update-only close fields).
#[derive(Clone, Debug, Default)]
pub struct CassandraVisibilityRow {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub type_name: String,
    pub start_time: i64,
    pub execution_time: i64,
    pub memo: Vec<u8>,
    pub encoding: String,
    pub task_list: String,
    pub is_cron: bool,
    pub num_clusters: i32,
    pub update_time: i64,
    pub shard_id: i32,
    pub execution_status: i32,
    pub cron_schedule: String,
    pub scheduled_execution_time: i64,
    /// closed rows only
    pub close_time: i64,
    pub status: i32,
    pub history_length: i64,
}

fn ts(nanos: i64) -> chrono::DateTime<chrono::Utc> {
    ms_ts(nanos.div_euclid(1_000_000))
}

fn ms(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000)
}

fn open_values(row: &CassandraVisibilityRow) -> Result<BoundValues, E> {
    Ok(vec![
        bound(parse_uuid(&row.domain_id)?),
        bound(DOMAIN_PARTITION),
        bound(row.workflow_id.clone()),
        bound(parse_uuid(&row.run_id)?),
        bound(ts(row.start_time)),
        bound(ts(row.execution_time)),
        bound(row.type_name.clone()),
        bound(row.memo.clone()),
        bound(row.encoding.clone()),
        bound(row.task_list.clone()),
        bound(row.is_cron),
        bound(row.num_clusters),
        bound(ts(row.update_time)),
        bound(row.shard_id),
        bound(row.execution_status),
        bound(row.cron_schedule.clone()),
        bound(ts(row.scheduled_execution_time)),
    ])
}

fn closed_values(row: &CassandraVisibilityRow) -> Result<BoundValues, E> {
    Ok(vec![
        bound(parse_uuid(&row.domain_id)?),
        bound(DOMAIN_PARTITION),
        bound(row.workflow_id.clone()),
        bound(parse_uuid(&row.run_id)?),
        bound(ts(row.start_time)),
        bound(ts(row.execution_time)),
        bound(ts(row.close_time)),
        bound(row.type_name.clone()),
        bound(row.status),
        bound(row.history_length),
        bound(row.memo.clone()),
        bound(row.encoding.clone()),
        bound(row.task_list.clone()),
        bound(row.is_cron),
        bound(row.num_clusters),
        bound(ts(row.update_time)),
        bound(row.shard_id),
        bound(row.execution_status),
        bound(row.cron_schedule.clone()),
        bound(ts(row.scheduled_execution_time)),
    ])
}

fn insert_text(table: &str, cols: &str, n: usize, with_ttl: bool) -> String {
    let marks = vec!["?"; n].join(", ");
    let ttl = if with_ttl { " using TTL ?" } else { "" };
    format!("INSERT INTO {table} {cols}VALUES ({marks}){ttl}")
}

#[derive(Clone)]
pub struct CassandraVisibilityStore {
    driver: Arc<CassandraBlockingSession>,
}

impl CassandraVisibilityStore {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver }
    }

    /// Go `InsertVisibility` (`ttlSeconds > maxCassandraTTL` selects the no-TTL template).
    pub fn insert_visibility(&self, ttl_seconds: i64, row: &CassandraVisibilityRow) -> Result<(), E> {
        let with_ttl = ttl_seconds <= MAX_CASSANDRA_TTL;
        let mut values = open_values(row)?;
        if with_ttl {
            values.push(bound(ttl_seconds as i32));
        }
        let mut statement = Statement::new(insert_text("open_executions", OPEN_COLS, 17, with_ttl));
        // Go `WithTimestamp(UnixNanoToDBTimestamp(start))` (the value is passed through as-is).
        statement.set_timestamp(Some(ms(row.start_time)));
        let session = self.driver.session();
        self.driver.run(session.query_unpaged(statement, &values)).map_err(internal)?;
        Ok(())
    }

    /// Go `UpdateVisibility` with `UpdateOpenToClose` (always set by the nosql store).
    pub fn update_visibility(&self, ttl_seconds: i64, row: &CassandraVisibilityRow) -> Result<(), E> {
        let with_ttl = ttl_seconds <= MAX_CASSANDRA_TTL;
        let mut batch = Batch::new(BatchType::Logged);
        let mut all: Vec<BoundValues> = Vec::new();

        batch.append_statement(Statement::new(DELETE_OPEN));
        all.push(vec![
            bound(parse_uuid(&row.domain_id)?),
            bound(DOMAIN_PARTITION),
            bound(ts(row.start_time)),
            bound(parse_uuid(&row.run_id)?),
        ]);
        for table in ["closed_executions", "closed_executions_v2"] {
            let mut values = closed_values(row)?;
            if with_ttl {
                values.push(bound(ttl_seconds as i32));
            }
            batch.append_statement(Statement::new(insert_text(table, CLOSED_COLS, 20, with_ttl)));
            all.push(values);
        }
        // Go: the close time, or start + 1s when the close time is before the start time.
        let query_time = if row.close_time < row.start_time { row.start_time + 1_000_000_000 } else { row.close_time };
        batch.set_timestamp(Some(ms(query_time)));
        let session = self.driver.session();
        self.driver.run(session.batch(&batch, all)).map_err(internal)?;
        Ok(())
    }

    /// Go `DeleteVisibility`: a no-op unless the admin-deletion context key is set (never set here).
    pub fn delete_visibility(&self, _domain_id: &str, _run_id: &str) -> Result<(), E> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_templates_match_go_shapes() {
        let t = insert_text("open_executions", OPEN_COLS, 17, true);
        assert!(t.starts_with("INSERT INTO open_executions (domain_id, domain_partition,"));
        assert!(t.ends_with("using TTL ?"));
        assert_eq!(t.matches('?').count(), 18);
        assert_eq!(insert_text("closed_executions", CLOSED_COLS, 20, false).matches('?').count(), 20);
    }
}
