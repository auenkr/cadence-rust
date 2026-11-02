//! Go Cassandra history_node reader. Unlike SQL, Cassandra stores transaction
//! IDs unchanged and orders them DESC (go/schema/cassandra/cadence/schema.cql).
use crate::cassandra::CassandraBlockingSession;
use crate::history_reader::{HistoryNodeRow, HistoryNodeStore, HistoryReadError};
use scylla::{response::PagingState, statement::Statement};
use std::sync::Arc;

const SELECT: &str = "SELECT node_id, txn_id, data, data_encoding FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ?";

type Result<T> = std::result::Result<T, HistoryReadError>;
type Page = (Vec<HistoryNodeRow>, Option<Vec<u8>>);

fn internal(error: impl std::fmt::Display) -> HistoryReadError {
    HistoryReadError::Internal(format!("Cassandra history_node: {error}"))
}

#[derive(Debug)]
struct Query {
    tree: uuid::Uuid,
    branch: uuid::Uuid,
    min: i64,
    max: i64,
    size: i32,
    token: PagingState,
}

impl Query {
    fn new(
        tree: [u8; 16],
        branch: [u8; 16],
        min: i64,
        max: i64,
        size: i64,
        token: Option<&[u8]>,
    ) -> Result<Self> {
        let size = i32::try_from(size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| {
                HistoryReadError::InvalidRequest("invalid Cassandra history page size".into())
            })?;
        Ok(Self {
            tree: uuid::Uuid::from_bytes(tree),
            branch: uuid::Uuid::from_bytes(branch),
            min,
            max,
            size,
            token: token
                .filter(|t| !t.is_empty())
                .map(|t| PagingState::new_from_raw_bytes(t.to_vec()))
                .unwrap_or_else(PagingState::start),
        })
    }
}

fn history_row(node_id: i64, txn_id: i64, data: Vec<u8>, encoding: String) -> HistoryNodeRow {
    // No SQL sign conversion: Go's nosqlHistoryStore passes TransactionID
    // straight to Cassandra, whose clustering order returns newest txn first.
    HistoryNodeRow {
        node_id,
        txn_id,
        data,
        encoding,
    }
}

#[derive(Debug)]
pub struct CassandraHistoryNodeStore {
    driver: Arc<CassandraBlockingSession>,
}

impl CassandraHistoryNodeStore {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver }
    }

    fn read_page(&self, query: Query) -> Result<Page> {
        let mut statement = Statement::new(SELECT);
        statement.set_page_size(query.size);
        statement.set_consistency(self.driver.default_consistency());
        let session = self.driver.session();
        let (result, response) = self
            .driver
            .run(session.query_single_page(
                statement,
                (query.tree, query.branch, query.min, query.max),
                query.token,
            ))
            .map_err(internal)?;
        let next = match response.into_paging_control_flow() {
            std::ops::ControlFlow::Continue(state) => state.as_bytes_slice().map(|b| b.to_vec()),
            std::ops::ControlFlow::Break(()) => None,
        };
        let result = result.into_rows_result().map_err(internal)?;
        let mut rows = Vec::new();
        for row in result
            .rows::<(i64, i64, Vec<u8>, String)>()
            .map_err(internal)?
        {
            let (node, txn, data, encoding) = row.map_err(internal)?;
            rows.push(history_row(node, txn, data, encoding));
        }
        Ok((rows, next))
    }
}

impl HistoryNodeStore for CassandraHistoryNodeStore {
    fn select_nodes(
        &self,
        shard: i32,
        tree: [u8; 16],
        branch: [u8; 16],
        min: i64,
        max: i64,
        limit: i64,
    ) -> Result<Vec<HistoryNodeRow>> {
        self.select_page(shard, tree, branch, min, max, limit, None)
            .map(|(rows, _)| rows)
    }

    fn select_page(
        &self,
        _shard: i32,
        tree: [u8; 16],
        branch: [u8; 16],
        min: i64,
        max: i64,
        limit: i64,
        token: Option<&[u8]>,
    ) -> Result<Page> {
        self.read_page(Query::new(tree, branch, min, max, limit, token)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_opaque_page_state_preserve_go_query() {
        let query = Query::new([1; 16], [2; 16], 3, 20, 5, Some(&[255, 0, 7])).unwrap();
        assert_eq!(query.tree.as_bytes(), &[1; 16]);
        assert_eq!(query.branch.as_bytes(), &[2; 16]);
        assert_eq!((query.min, query.max, query.size), (3, 20, 5));
        assert_eq!(query.token.as_bytes_slice().unwrap().as_ref(), &[255, 0, 7]);
        assert!(!SELECT.contains("shard_id"));
        assert!(SELECT.ends_with("node_id >= ? AND node_id < ?"));
    }

    #[test]
    fn cassandra_transaction_ids_are_not_sql_negated() {
        for txn in [0, 42, i64::MAX, i64::MIN] {
            assert_eq!(history_row(1, txn, vec![9], "thriftrw".into()).txn_id, txn);
        }
    }

    #[test]
    fn validates_page_size_and_starts_without_state() {
        for size in [0, -1, i64::MAX] {
            assert!(Query::new([0; 16], [0; 16], 1, 10, size, None).is_err());
        }
        let query = Query::new([0; 16], [0; 16], 1, 10, 1, Some(&[])).unwrap();
        assert!(query.token.as_bytes_slice().is_none());
    }
}
