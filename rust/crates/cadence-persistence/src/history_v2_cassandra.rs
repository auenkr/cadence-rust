//! Cassandra implementation of History's `GoHistoryV2Store`
//! (Go `nosql/nosql_history_store.go` + `nosqlplugin/cassandra/history_events*.go`).
//! Transaction ids are stored unchanged (the table clusters `txn_id DESC`).
use crate::cassandra::CassandraBlockingSession;
use crate::history_contract::*;
use crate::history_execution_cassandra::{internal, parse_uuid};
use crate::history_execution_cassandra_write::{cell, Filled, get_i64, get_str, get_ts_nanos, ms_ts, udt_map, RowMap};
use crate::matching_cassandra_exec::{bound, BoundValues};
use async_trait::async_trait;
use scylla::frame::types::Consistency;
use scylla::response::PagingState;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::Statement;
use scylla::value::{CqlValue, Row};
use std::sync::Arc;

type E = PersistenceError;

const UPSERT_NODE: &str = "INSERT INTO history_node (tree_id, branch_id, node_id, txn_id, data, data_encoding, created_time) VALUES (?, ?, ?, ?, ?, ?, ?) ";
const READ_NODES: &str = "SELECT node_id, txn_id, data, data_encoding FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? ";
const RANGE_DELETE_NODES: &str = "DELETE FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id >= ? ";
const READ_BRANCHES: &str = "SELECT branch_id, ancestors, fork_time, info FROM history_tree WHERE tree_id = ? ";
const DELETE_BRANCH: &str = "DELETE FROM history_tree WHERE tree_id = ? AND branch_id = ? ";
const SCAN_TREES: &str = "SELECT tree_id, branch_id, fork_time, info FROM history_tree ";

#[derive(Clone)]
pub struct CassandraHistoryV2Store {
    driver: Arc<CassandraBlockingSession>,
}

impl CassandraHistoryV2Store {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver }
    }

    fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(Arc<CassandraBlockingSession>) -> Result<T, E> + Send + 'static,
    ) -> impl std::future::Future<Output = Result<T, E>> {
        let driver = self.driver.clone();
        async move { tokio::task::spawn_blocking(move || f(driver)).await.map_err(|e| internal(format!("blocking task: {e}")))? }
    }
}

fn begin_node_id(ancestors: &[HistoryBranchRange]) -> i64 {
    ancestors.last().map_or(1, |r| r.end_node_id)
}

fn ts(nanos: i64) -> chrono::DateTime<chrono::Utc> {
    ms_ts(nanos.div_euclid(1_000_000))
}

/// Bound `list<frozen<branch_range>>` value (bind variables are not allowed inside list literals).
fn ancestors_value(ancestors: &[HistoryBranchRange]) -> Result<Filled, E> {
    let mut items = Vec::with_capacity(ancestors.len());
    for a in ancestors {
        items.push(CqlValue::UserDefinedType {
            keyspace: String::new(),
            name: String::new(),
            fields: vec![
                ("branch_id".to_owned(), Some(CqlValue::Uuid(parse_uuid(&a.branch_id)?))),
                ("end_node_id".to_owned(), Some(CqlValue::BigInt(a.end_node_id))),
            ],
        });
    }
    Ok(Filled(CqlValue::List(items)))
}

fn tree_insert(request: &HistoryBranchRequest) -> Result<(String, BoundValues), E> {
    let mut v: BoundValues = vec![bound(parse_uuid(&request.tree_id)?), bound(parse_uuid(&request.branch_id)?)];
    v.push(bound(ancestors_value(&request.ancestors)?));
    v.push(bound(ts(request.current_timestamp_unix_nanos)));
    v.push(bound(request.info.clone()));
    v.push(bound(ts(request.current_timestamp_unix_nanos)));
    Ok((
        "INSERT INTO history_tree (tree_id, branch_id, ancestors, fork_time, info, created_time) VALUES (?, ?, ?, ?, ?, ?) ".to_string(),
        v,
    ))
}

fn run_batch(driver: &CassandraBlockingSession, items: Vec<(String, BoundValues)>, all: bool) -> Result<(), E> {
    let mut batch = Batch::new(BatchType::Logged);
    if all {
        batch.set_consistency(Consistency::All);
    }
    let mut values = Vec::with_capacity(items.len());
    for (text, v) in items {
        batch.append_statement(Statement::new(text));
        values.push(v);
    }
    let session = driver.session();
    driver.run(session.batch(&batch, values)).map_err(internal)?;
    Ok(())
}

fn exec(driver: &CassandraBlockingSession, text: String, values: BoundValues) -> Result<(), E> {
    let session = driver.session();
    driver.run(session.query_unpaged(Statement::new(text), &values)).map_err(internal)?;
    Ok(())
}

fn page(
    driver: &CassandraBlockingSession,
    text: &str,
    values: BoundValues,
    page_size: i32,
    token: &[u8],
) -> Result<(Vec<RowMap>, Vec<u8>), E> {
    let mut statement = Statement::new(text.to_owned());
    if page_size > 0 {
        statement.set_page_size(page_size);
    }
    let paging = if token.is_empty() { PagingState::start() } else { PagingState::new_from_raw_bytes(token.to_vec()) };
    let session = driver.session();
    let (result, response) = driver.run(session.query_single_page(statement, &values, paging)).map_err(internal)?;
    let next = match response.into_paging_control_flow() {
        std::ops::ControlFlow::Continue(state) => state.as_bytes_slice().map(|b| b.to_vec()).unwrap_or_default(),
        std::ops::ControlFlow::Break(()) => Vec::new(),
    };
    let rows = result.into_rows_result().map_err(internal)?;
    let names: Vec<String> = rows.column_specs().iter().map(|c| c.name().to_owned()).collect();
    let mut out = Vec::new();
    for row in rows.rows::<Row>().map_err(internal)? {
        let row = row.map_err(internal)?;
        out.push(names.iter().cloned().zip(row.columns).collect::<RowMap>());
    }
    Ok((out, next))
}

fn append(driver: &CassandraBlockingSession, r: HistoryBranchRequest) -> Result<(), E> {
    if r.node_id < begin_node_id(&r.ancestors) {
        return Err(E::InvalidPersistenceRequest("cannot append to ancestors' nodes".into()));
    }
    let node: BoundValues = vec![
        bound(parse_uuid(&r.tree_id)?),
        bound(parse_uuid(&r.branch_id)?),
        bound(r.node_id),
        bound(r.transaction_id),
        bound(r.data.data.clone()),
        bound(r.data.encoding.clone()),
        bound(ts(r.current_timestamp_unix_nanos)),
    ];
    if r.is_new_branch {
        run_batch(driver, vec![tree_insert(&r)?, (UPSERT_NODE.to_owned(), node)], false)
    } else {
        exec(driver, UPSERT_NODE.to_owned(), node)
    }
}

fn read(driver: &CassandraBlockingSession, r: HistoryBranchRequest) -> Result<HistoryBranchPage, E> {
    let v: BoundValues = vec![
        bound(parse_uuid(&r.tree_id)?),
        bound(parse_uuid(&r.branch_id)?),
        bound(r.min_node_id),
        bound(r.max_node_id),
    ];
    let (rows, token) = page(driver, READ_NODES, v, r.page_size, &r.next_page_token)?;
    let mut nodes = Vec::new();
    let mut last_node = r.last_node_id;
    let mut last_txn = r.last_transaction_id;
    for row in &rows {
        let node_id = get_i64(cell(row, "node_id")).unwrap_or_default();
        let txn_id = get_i64(cell(row, "txn_id")).unwrap_or_default();
        if txn_id < last_txn {
            continue;
        }
        if node_id < last_node {
            return Err(E::InternalDataInconsistency("corrupted data, nodeID cannot decrease".into()));
        }
        if node_id == last_node {
            return Err(E::InternalDataInconsistency("corrupted data, same nodeID must have smaller txnID".into()));
        }
        last_txn = txn_id;
        last_node = node_id;
        nodes.push(DataBlob {
            data: crate::history_execution_cassandra_write::get_blob(cell(row, "data")),
            encoding: get_str(cell(row, "data_encoding")),
        });
    }
    Ok(HistoryBranchPage { nodes, next_page_token: token, last_node_id: last_node, last_transaction_id: last_txn })
}

fn fork(driver: &CassandraBlockingSession, r: HistoryBranchRequest, fork_node_id: i64, new_branch_id: String) -> Result<HistoryBranchRequest, E> {
    let new = crate::history_branch::fork_request(&r, fork_node_id, new_branch_id);
    let (text, v) = tree_insert(&new)?;
    exec(driver, text, v)?;
    Ok(new)
}

fn get_tree(driver: &CassandraBlockingSession, shard_id: i32, tree_id: &str) -> Result<Vec<HistoryTreeBranch>, E> {
    let tree = parse_uuid(tree_id)?;
    let mut token: Vec<u8> = Vec::new();
    let mut out = Vec::new();
    loop {
        let (rows, next) = page(driver, READ_BRANCHES, vec![bound(tree)], 100, &token)?;
        for row in &rows {
            let mut ancestors: Vec<HistoryBranchRange> = Vec::new();
            if let Some(CqlValue::List(items) | CqlValue::Set(items)) = cell(row, "ancestors") {
                for item in items {
                    let m = udt_map(Some(item));
                    ancestors.push(HistoryBranchRange {
                        branch_id: get_str(m.get("branch_id").copied()),
                        begin_node_id: 0,
                        end_node_id: get_i64(m.get("end_node_id").copied()).unwrap_or_default(),
                    });
                }
            }
            if !ancestors.is_empty() {
                ancestors.sort_by_key(|a| a.end_node_id);
                ancestors[0].begin_node_id = 1;
                for i in 1..ancestors.len() {
                    ancestors[i].begin_node_id = ancestors[i - 1].end_node_id;
                }
            }
            out.push(HistoryTreeBranch {
                shard_id,
                tree_id: tree_id.to_owned(),
                branch_id: get_str(cell(row, "branch_id")),
                ancestors,
                ..Default::default()
            });
        }
        if next.is_empty() {
            break;
        }
        token = next;
    }
    Ok(out)
}

fn delete(driver: &CassandraBlockingSession, r: HistoryBranchRequest) -> Result<(), E> {
    let branches = get_tree(driver, r.shard_id, &r.tree_id)?;
    let referred = crate::history_branch::max_referred_node_ids(&branches);
    let tree = parse_uuid(&r.tree_id)?;
    let mut items: Vec<(String, BoundValues)> =
        vec![(DELETE_BRANCH.to_owned(), vec![bound(tree), bound(parse_uuid(&r.branch_id)?)])];
    let is_last_remaining = branches.len() <= 1;
    for br in crate::history_branch::deletion_ranges(&r) {
        let max_referred = referred.get(&br.branch_id).copied();
        let tree_exists = branches.iter().any(|b| b.branch_id == br.branch_id);
        if tree_exists && br.branch_id != r.branch_id {
            // out-of-order deletion: leave the ancestor's nodes to its own cleanup
            break;
        }
        let branch = parse_uuid(&br.branch_id)?;
        let (min, stop) = match (max_referred, is_last_remaining) {
            (Some(_), true) => (0, false),
            (Some(max), false) => (max, true),
            (None, _) => (br.begin_node_id, false),
        };
        items.push((RANGE_DELETE_NODES.to_owned(), vec![bound(tree), bound(branch), bound(min)]));
        if stop {
            break;
        }
    }
    run_batch(driver, items, true)
}

fn all_branches(driver: &CassandraBlockingSession, page_size: i32, token: Vec<u8>) -> Result<HistoryTreePage, E> {
    let (rows, next) = page(driver, SCAN_TREES, Vec::new(), page_size, &token)?;
    let branches = rows
        .iter()
        .map(|row| HistoryTreeBranch {
            shard_id: 0,
            tree_id: get_str(cell(row, "tree_id")),
            branch_id: get_str(cell(row, "branch_id")),
            fork_time_unix_nanos: get_ts_nanos(cell(row, "fork_time")),
            info: get_str(cell(row, "info")),
            ancestors: Vec::new(),
        })
        .collect();
    Ok(HistoryTreePage { branches, next_page_token: next })
}

#[async_trait]
impl GoHistoryV2Store for CassandraHistoryV2Store {
    async fn append_history_nodes(&self, request: HistoryBranchRequest) -> Result<(), E> {
        self.blocking(move |d| append(&d, request)).await
    }
    async fn read_history_branch(&self, request: HistoryBranchRequest) -> Result<HistoryBranchPage, E> {
        self.blocking(move |d| read(&d, request)).await
    }
    async fn fork_history_branch(
        &self,
        request: HistoryBranchRequest,
        fork_node_id: i64,
        new_branch_id: String,
    ) -> Result<HistoryBranchRequest, E> {
        self.blocking(move |d| fork(&d, request, fork_node_id, new_branch_id)).await
    }
    async fn delete_history_branch(&self, request: HistoryBranchRequest) -> Result<(), E> {
        self.blocking(move |d| delete(&d, request)).await
    }
    async fn get_history_tree(&self, request: HistoryBranchRequest) -> Result<Vec<HistoryTreeBranch>, E> {
        self.blocking(move |d| get_tree(&d, request.shard_id, &request.tree_id)).await
    }
    async fn get_all_history_tree_branches(
        &self,
        _shard_id: i32,
        page_size: i32,
        next_page_token: Vec<u8>,
    ) -> Result<HistoryTreePage, E> {
        self.blocking(move |d| all_branches(&d, page_size, next_page_token)).await
    }
}
