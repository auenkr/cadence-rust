//! SQL persistence contracts shared by the History service.
//!
//! The SQL schemas and error behavior are owned by Go's
//! `go/common/persistence/sql` stores.  Payloads here are the serialized
//! Cadence blobs (`data`, `data_encoding`); stores must preserve those bytes.
//! This module deliberately contains no schema migration logic.

use async_trait::async_trait;

/// A shard row as stored in the existing `shards` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRow {
    pub shard_id: i32,
    pub range_id: i64,
    pub data: Vec<u8>,
    pub data_encoding: String,
}

/// Errors which must retain the same category as Cadence persistence errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryPersistenceError {
    ShardOwnershipLost { shard_id: i32, previous_range_id: i64, current_range_id: i64 },
    ConditionFailed(String),
    WorkflowAlreadyStarted(String),
    CurrentWorkflowConditionFailed(String),
    EntityNotExists(String),
    AlreadyExists(String),
    InternalDataInconsistency(String),
    Internal(String),
}

impl std::fmt::Display for HistoryPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShardOwnershipLost { shard_id, previous_range_id, current_range_id } => write!(f, "shard {shard_id} ownership lost: range ID {previous_range_id} != {current_range_id}"),
            Self::ConditionFailed(message)
            | Self::WorkflowAlreadyStarted(message)
            | Self::CurrentWorkflowConditionFailed(message)
            | Self::EntityNotExists(message)
            | Self::AlreadyExists(message)
            | Self::InternalDataInconsistency(message)
            | Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for HistoryPersistenceError {}

/// Shard persistence contract used by the History shard controller.
#[async_trait]
pub trait ShardStore: Send + Sync {
    async fn create_shard(&self, row: ShardRow) -> Result<(), HistoryPersistenceError>;
    async fn get_shard(&self, shard_id: i32) -> Result<ShardRow, HistoryPersistenceError>;
    /// Update must compare the persisted range ID to `previous_range_id` in
    /// the same transaction as the write (Go `lockShard` semantics).
    async fn update_shard(&self, row: ShardRow, previous_range_id: i64) -> Result<(), HistoryPersistenceError>;
}

/// Identity columns used by `executions` and `current_executions`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkflowKey {
    pub shard_id: i32,
    pub domain_id: [u8; 16],
    pub workflow_id: String,
    pub run_id: [u8; 16],
}

/// Complete persisted mutable-state row. `data` and `data_encoding` use the
/// existing Cadence Thrift/blob encoding and must not be normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRow {
    pub key: WorkflowKey,
    pub next_event_id: i64,
    pub last_write_version: i64,
    pub data: Vec<u8>,
    pub data_encoding: String,
}

/// ExecutionStore is the complete typed Go-compatible contract. `ExecutionRow`
/// remains available for the current row adapters while they are brought up to
/// this interface.
pub use crate::history_contract::GoExecutionStore as ExecutionStore;

/// HistoryStore is the typed Go-compatible History-v2 interface.
pub use crate::history_contract::GoHistoryV2Store as HistoryStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryNode {
    pub node_id: i64,
    pub transaction_id: i64,
    pub data: Vec<u8>,
    pub data_encoding: String,
}

/// Go SQL History-v2 continuation tokens are a signed int64 in little-endian
/// order, not the Thrift branch token used by the history service.
pub fn encode_history_node_page_token(node_id: i64) -> Vec<u8> { node_id.to_le_bytes().to_vec() }

pub fn decode_history_node_page_token(token: &[u8]) -> Result<i64, HistoryPersistenceError> {
    let bytes: [u8; 8] = token.try_into().map_err(|_| HistoryPersistenceError::Internal(format!("invalid history page token length {}", token.len())))?;
    Ok(i64::from_le_bytes(bytes))
}

/// Apply Go's ordered node/transaction validation and page selection after a
/// SQL read. MySQL rows carry negative transaction IDs in storage and must be
/// normalized before calling this helper.
pub fn build_history_branch_page(
    rows: &[HistoryNode],
    mut last_node_id: i64,
    mut last_transaction_id: i64,
    page_size: i32,
) -> Result<crate::history_contract::HistoryBranchPage, HistoryPersistenceError> {
    let mut nodes = Vec::with_capacity(page_size.max(0) as usize);
    for row in rows {
        if row.transaction_id < last_transaction_id {
            if row.node_id < last_node_id {
                return Err(HistoryPersistenceError::InternalDataInconsistency("corrupted data, nodeID cannot decrease".into()));
            }
            if row.node_id > last_node_id { last_node_id = row.node_id; }
            continue;
        }
        if row.node_id < last_node_id {
            return Err(HistoryPersistenceError::InternalDataInconsistency("corrupted data, nodeID cannot decrease".into()));
        }
        if row.node_id == last_node_id {
            return Err(HistoryPersistenceError::InternalDataInconsistency("corrupted data, same nodeID must have smaller txnID".into()));
        }
        last_transaction_id = row.transaction_id;
        last_node_id = row.node_id;
        nodes.push(crate::history_contract::DataBlob { data: row.data.clone(), encoding: row.data_encoding.clone() });
    }
    let next_page_token = if rows.len() >= page_size.max(0) as usize { encode_history_node_page_token(last_node_id) } else { Vec::new() };
    Ok(crate::history_contract::HistoryBranchPage { nodes, next_page_token, last_node_id, last_transaction_id })
}

/// Encode/decode `history_tree.data` with the existing Go sqlblobs schema.
/// The selected SQL encoding is explicit, matching the Go parser configured
/// for the History persistence store.
pub fn encode_history_tree_info(info: &crate::history_contract::HistoryTreeInfo, encoding: crate::sqlblobs::Encoding) -> Result<(Vec<u8>, String), HistoryPersistenceError> {
    encoding.encode(&crate::sqlblobs::history_tree_to_struct(info))
        .map(|data| (data, encoding.as_str().to_owned()))
        .map_err(|error| HistoryPersistenceError::Internal(error.to_string()))
}

pub fn decode_history_tree_info(data: &[u8], data_encoding: &str) -> Result<crate::history_contract::HistoryTreeInfo, HistoryPersistenceError> {
    let encoding = crate::sqlblobs::Encoding::parse(data_encoding)
        .map_err(|error| HistoryPersistenceError::Internal(error.to_string()))?;
    let value = encoding.decode(data)
        .map_err(|error| HistoryPersistenceError::Internal(error.to_string()))?;
    crate::sqlblobs::history_tree_from_struct(&value)
        .map_err(|error| HistoryPersistenceError::Internal(error.to_string()))
}

#[cfg(test)]
mod history_v2_tests {
    use super::*;

    #[test]
    fn history_node_token_matches_go_little_endian_int64() {
        let token = encode_history_node_page_token(0x0102_0304_0506_0708);
        assert_eq!(token, [8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(decode_history_node_page_token(&token).unwrap(), 0x0102_0304_0506_0708);
        assert!(decode_history_node_page_token(&token[..7]).is_err());
    }

    #[test]
    fn history_page_picks_highest_transaction_for_each_node() {
        let rows = vec![
            HistoryNode { node_id: 2, transaction_id: 9, data: vec![9], data_encoding: "thriftrw".into() },
            HistoryNode { node_id: 2, transaction_id: 8, data: vec![8], data_encoding: "thriftrw".into() },
            HistoryNode { node_id: 4, transaction_id: 10, data: vec![10], data_encoding: "thriftrw".into() },
        ];
        let page = build_history_branch_page(&rows, 0, 0, 3).unwrap();
        assert_eq!(page.nodes.iter().map(|blob| blob.data[0]).collect::<Vec<_>>(), [9, 10]);
        assert_eq!((page.last_node_id, page.last_transaction_id), (4, 10));
        assert_eq!(decode_history_node_page_token(&page.next_page_token).unwrap(), 4);
    }

    #[test]
    fn history_page_rejects_decreasing_node_ids() {
        let rows = vec![
            HistoryNode { node_id: 4, transaction_id: 5, data: vec![], data_encoding: "thriftrw".into() },
            HistoryNode { node_id: 2, transaction_id: 6, data: vec![], data_encoding: "thriftrw".into() },
        ];
        assert!(matches!(build_history_branch_page(&rows, 0, 0, 10), Err(HistoryPersistenceError::InternalDataInconsistency(_))));
    }
}

