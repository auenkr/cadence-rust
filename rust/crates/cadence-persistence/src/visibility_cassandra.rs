//! Read-only port of Go's Cassandra visibility store
//! (`common/persistence/nosql/nosql_visibility_store.go` and
//! `nosqlplugin/cassandra/visibility{,_cql}.go`): `open_executions`,
//! `closed_executions` and `closed_executions_v2` in the visibility keyspace.
//! Paging uses Cassandra's native paging state as the opaque page token and
//! ordering is the tables' clustering order (start_time or close_time DESC).
use crate::cassandra::CassandraBlockingSession;
use crate::visibility_reader::{
    ListPage, VisibilityError, VisibilityFilter, VisibilityRow, VisibilityStore,
};
use scylla::{
    response::PagingState, statement::Consistency, statement::Statement, value::CqlTimestamp,
};
use std::sync::Arc;

/// Go `domainPartition`.
const DOMAIN_PARTITION: i32 = 0;

const OPEN_COLUMNS: &str = " workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time ";
const CLOSED_COLUMNS: &str = " workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time ";

/// Extra bound value of a list query (Go passes it as the fifth argument).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Extra {
    None,
    Text(String),
    Status(i32),
}

fn internal(error: impl std::fmt::Display) -> VisibilityError {
    VisibilityError::Internal(format!("Cassandra visibility: {error}"))
}

/// Go `SelectVisibility` template choice. Precedence mirrors the SQL reader:
/// workflow ID, workflow type, close status.
pub fn list_query(filter: &VisibilityFilter, sort_by_close_time: bool) -> (String, Extra) {
    let closed = filter.closed
        || (filter.workflow_id.is_none()
            && filter.workflow_type_name.is_none()
            && filter.close_status.is_some());
    let (table, time_col) = if !closed {
        ("open_executions", "start_time")
    } else if sort_by_close_time {
        ("closed_executions_v2", "close_time")
    } else {
        ("closed_executions", "start_time")
    };
    let columns = if closed { CLOSED_COLUMNS } else { OPEN_COLUMNS };
    let (partition, tail, extra) = if let Some(id) = &filter.workflow_id {
        ("= ?", "AND workflow_id = ? ", Extra::Text(id.clone()))
    } else if let Some(name) = &filter.workflow_type_name {
        (
            "= ?",
            "AND workflow_type_name = ? ",
            Extra::Text(name.clone()),
        )
    } else if let (true, Some(status)) = (closed, filter.close_status) {
        ("= ?", "AND status = ? ", Extra::Status(status))
    } else {
        ("IN (?)", "", Extra::None)
    };
    (
        format!(
            "SELECT {columns}FROM {table} WHERE domain_id = ? AND domain_partition {partition} AND {time_col} >= ? AND {time_col} <= ? {tail}"
        ),
        extra,
    )
}

const GET_CLOSED: &str = "SELECT  workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time FROM closed_executions WHERE domain_id = ? AND domain_partition = ? AND workflow_id = ? AND run_id = ? ALLOW FILTERING ";

type OpenTuple = (
    String,
    uuid::Uuid,
    Option<CqlTimestamp>,
    Option<CqlTimestamp>,
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
    Option<String>,
    Option<bool>,
    Option<i32>,
    Option<CqlTimestamp>,
    Option<i32>,
    Option<i32>,
    Option<String>,
    Option<CqlTimestamp>,
);

/// Closed rows exceed the 16-column tuple limit, so decode by column name.
#[derive(scylla::DeserializeRow)]
struct ClosedRec {
    workflow_id: String,
    run_id: uuid::Uuid,
    start_time: Option<CqlTimestamp>,
    execution_time: Option<CqlTimestamp>,
    close_time: Option<CqlTimestamp>,
    workflow_type_name: Option<String>,
    status: Option<i32>,
    history_length: Option<i64>,
    memo: Option<Vec<u8>>,
    encoding: Option<String>,
    is_cron: Option<bool>,
    update_time: Option<CqlTimestamp>,
    shard_id: Option<i32>,
    // Selected columns must all be present in a by-name derive.
    #[allow(dead_code)]
    task_list: Option<String>,
    #[allow(dead_code)]
    num_clusters: Option<i32>,
    #[allow(dead_code)]
    execution_status: Option<i32>,
    #[allow(dead_code)]
    cron_schedule: Option<String>,
    #[allow(dead_code)]
    scheduled_execution_time: Option<CqlTimestamp>,
}

/// Cassandra timestamps are milliseconds; rows carry microseconds.
fn us(ts: Option<CqlTimestamp>) -> i64 {
    ts.map_or(0, |t| t.0.saturating_mul(1000))
}

fn open_row(t: OpenTuple) -> VisibilityRow {
    VisibilityRow {
        workflow_id: t.0,
        run_id: t.1.to_string(),
        start_time_us: us(t.2),
        execution_time_us: us(t.3),
        workflow_type_name: t.4.unwrap_or_default(),
        memo: t.5.unwrap_or_default(),
        encoding: t.6.unwrap_or_default(),
        is_cron: t.8.unwrap_or(false),
        update_time_us: us(t.10),
        shard_id: t.11.unwrap_or(0),
        ..Default::default()
    }
}

fn closed_row(t: ClosedRec) -> VisibilityRow {
    VisibilityRow {
        workflow_id: t.workflow_id,
        run_id: t.run_id.to_string(),
        start_time_us: us(t.start_time),
        execution_time_us: us(t.execution_time),
        close_time_us: Some(us(t.close_time)),
        workflow_type_name: t.workflow_type_name.unwrap_or_default(),
        close_status: Some(t.status.unwrap_or(0)),
        history_length: Some(t.history_length.unwrap_or(0)),
        memo: t.memo.unwrap_or_default(),
        encoding: t.encoding.unwrap_or_default(),
        is_cron: t.is_cron.unwrap_or(false),
        update_time_us: us(t.update_time),
        shard_id: t.shard_id.unwrap_or(0),
    }
}

#[derive(Debug)]
pub struct CassandraVisibilityStore {
    driver: Arc<CassandraBlockingSession>,
    /// Go `listClosedOrderingByCloseTime` (`system.enableReadFromClosedExecutionV2`).
    sort_by_close_time: bool,
}

impl CassandraVisibilityStore {
    pub fn new(driver: Arc<CassandraBlockingSession>, sort_by_close_time: bool) -> Self {
        Self {
            driver,
            sort_by_close_time,
        }
    }

    fn domain(id: &str) -> Result<uuid::Uuid, VisibilityError> {
        uuid::Uuid::parse_str(id).map_err(internal)
    }

    fn page(
        &self,
        filter: &VisibilityFilter,
        token: &[u8],
        earliest_us: i64,
        latest_us: i64,
    ) -> Result<ListPage, VisibilityError> {
        let (cql, extra) = list_query(filter, self.sort_by_close_time);
        let closed = cql.contains("closed_executions");
        let size = i32::try_from(filter.page_size)
            .ok()
            .filter(|s| *s > 0)
            .ok_or_else(|| VisibilityError::BadRequest("invalid page size".into()))?;
        let mut statement = Statement::new(cql);
        statement.set_page_size(size);
        // Go cassandraLowConslevel.
        statement.set_consistency(Consistency::LocalOne);
        let paging = if token.is_empty() {
            PagingState::start()
        } else {
            PagingState::new_from_raw_bytes(token.to_vec())
        };
        let domain = Self::domain(&filter.domain_id)?;
        let from = CqlTimestamp(earliest_us.div_euclid(1000));
        let to = CqlTimestamp(latest_us.div_euclid(1000));
        let session = self.driver.session();
        let (result, response) = self
            .driver
            .run(async {
                match extra {
                    Extra::None => {
                        session
                            .query_single_page(
                                statement,
                                (domain, DOMAIN_PARTITION, from, to),
                                paging,
                            )
                            .await
                    }
                    Extra::Text(text) => {
                        session
                            .query_single_page(
                                statement,
                                (domain, DOMAIN_PARTITION, from, to, text),
                                paging,
                            )
                            .await
                    }
                    Extra::Status(status) => {
                        session
                            .query_single_page(
                                statement,
                                (domain, DOMAIN_PARTITION, from, to, status),
                                paging,
                            )
                            .await
                    }
                }
            })
            .map_err(internal)?;
        let next = match response.into_paging_control_flow() {
            std::ops::ControlFlow::Continue(state) => state
                .as_bytes_slice()
                .map(|b| b.to_vec())
                .unwrap_or_default(),
            std::ops::ControlFlow::Break(()) => Vec::new(),
        };
        let result = result.into_rows_result().map_err(internal)?;
        let mut rows = Vec::new();
        if closed {
            for row in result.rows::<ClosedRec>().map_err(internal)? {
                rows.push(closed_row(row.map_err(internal)?));
            }
        } else {
            for row in result.rows::<OpenTuple>().map_err(internal)? {
                rows.push(open_row(row.map_err(internal)?));
            }
        }
        Ok(ListPage {
            rows,
            next_page_token: next,
        })
    }
}

impl VisibilityStore for CassandraVisibilityStore {
    fn select(&self, _filter: &VisibilityFilter) -> Result<Vec<VisibilityRow>, VisibilityError> {
        Err(VisibilityError::Internal(
            "Cassandra visibility uses native paging".into(),
        ))
    }

    fn list_native(
        &self,
        filter: &VisibilityFilter,
        page_token: &[u8],
        earliest_time_us: i64,
        latest_time_us: i64,
    ) -> Option<Result<ListPage, VisibilityError>> {
        Some(self.page(filter, page_token, earliest_time_us, latest_time_us))
    }

    fn get_closed(
        &self,
        _domain_id: &str,
        _run_id: &str,
    ) -> Result<Option<VisibilityRow>, VisibilityError> {
        Err(VisibilityError::Internal(
            "Cassandra closed lookup requires the workflow ID".into(),
        ))
    }

    fn get_closed_by_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<Option<VisibilityRow>, VisibilityError> {
        let domain = Self::domain(domain_id)?;
        let run = uuid::Uuid::parse_str(run_id).map_err(internal)?;
        let mut statement = Statement::new(GET_CLOSED);
        statement.set_consistency(self.driver.default_consistency());
        let session = self.driver.session();
        let workflow_id = workflow_id.to_string();
        let result = self
            .driver
            .run(session.query_unpaged(statement, (domain, DOMAIN_PARTITION, workflow_id, run)))
            .map_err(internal)?;
        let result = result.into_rows_result().map_err(internal)?;
        match result.rows::<ClosedRec>().map_err(internal)?.next() {
            Some(row) => Ok(Some(closed_row(row.map_err(internal)?))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter() -> VisibilityFilter {
        VisibilityFilter {
            domain_id: "d".into(),
            page_size: 10,
            ..Default::default()
        }
    }

    #[test]
    fn all_open_uses_partition_in_and_start_time() {
        let (cql, extra) = list_query(&filter(), false);
        assert!(cql.contains("FROM open_executions WHERE domain_id = ? AND domain_partition IN (?) AND start_time >= ? AND start_time <= ? "));
        assert_eq!(extra, Extra::None);
        assert!(!cql.contains("AND status"));
    }

    #[test]
    fn open_by_type_and_id() {
        let mut f = filter();
        f.workflow_type_name = Some("t".into());
        let (cql, extra) = list_query(&f, false);
        assert!(
            cql.contains("domain_partition = ?") && cql.ends_with("AND workflow_type_name = ? ")
        );
        assert_eq!(extra, Extra::Text("t".into()));
        f.workflow_id = Some("w".into());
        let (cql, extra) = list_query(&f, true);
        assert!(cql.ends_with("AND workflow_id = ? "));
        assert_eq!(extra, Extra::Text("w".into()));
    }

    #[test]
    fn closed_tables_and_ordering() {
        let mut f = filter();
        f.closed = true;
        let (cql, _) = list_query(&f, false);
        assert!(cql.contains("FROM closed_executions WHERE") && cql.contains("start_time >= ?"));
        assert!(cql.contains("close_time, workflow_type_name, status, history_length"));
        let (cql, _) = list_query(&f, true);
        assert!(
            cql.contains("FROM closed_executions_v2 WHERE")
                && cql.contains("close_time >= ? AND close_time <= ?")
        );
    }

    #[test]
    fn closed_by_status() {
        let mut f = filter();
        f.closed = true;
        f.close_status = Some(3);
        let (cql, extra) = list_query(&f, true);
        assert!(cql.ends_with("AND status = ? ") && cql.contains("closed_executions_v2"));
        assert_eq!(extra, Extra::Status(3));
        // status alone selects closed even without the closed flag (Go by-status API)
        f.closed = false;
        let (cql, _) = list_query(&f, false);
        assert!(cql.contains("FROM closed_executions WHERE"));
    }

    #[test]
    fn open_ignores_status() {
        let mut f = filter();
        f.close_status = None;
        let (_, extra) = list_query(&f, false);
        assert_eq!(extra, Extra::None);
    }

    #[test]
    fn get_closed_template_filters_by_id_and_run() {
        assert!(GET_CLOSED.contains("workflow_id = ? AND run_id = ? ALLOW FILTERING"));
    }

    #[test]
    fn timestamps_convert_ms_to_us() {
        assert_eq!(
            us(Some(CqlTimestamp(1_700_000_000_123))),
            1_700_000_000_123_000
        );
        assert_eq!(us(None), 0);
    }
}
