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

