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

/// SQL transaction boundary for History execution and task persistence.
/// Implementations must preserve Go create modes and condition/error classes.
#[async_trait]
pub trait ExecutionStore: Send + Sync {
    async fn create_workflow_execution(&self, row: ExecutionRow, mode: i32) -> Result<(), HistoryPersistenceError>;
    async fn get_workflow_execution(&self, key: &WorkflowKey) -> Result<ExecutionRow, HistoryPersistenceError>;
    async fn update_workflow_execution(&self, row: ExecutionRow, condition: i64) -> Result<(), HistoryPersistenceError>;
    async fn conflict_resolve_workflow_execution(&self, row: ExecutionRow, condition: i64) -> Result<(), HistoryPersistenceError>;
    async fn delete_workflow_execution(&self, key: &WorkflowKey) -> Result<(), HistoryPersistenceError>;
    async fn delete_current_workflow_execution(&self, key: &WorkflowKey, condition: i64) -> Result<(), HistoryPersistenceError>;
    async fn get_current_execution(&self, shard_id: i32, domain_id: [u8; 16], workflow_id: &str) -> Result<ExecutionRow, HistoryPersistenceError>;
    async fn is_workflow_execution_exists(&self, key: &WorkflowKey) -> Result<bool, HistoryPersistenceError>;
}

/// History-v2 `history_node` branch persistence contract.
#[async_trait]
pub trait HistoryStore: Send + Sync {
    async fn append_history_nodes(&self, shard_id: i32, tree_id: [u8; 16], branch_id: [u8; 16], nodes: Vec<HistoryNode>) -> Result<(), HistoryPersistenceError>;
    async fn read_history_branch(&self, request: crate::history_reader::ReadHistoryBranchRequest) -> Result<crate::history_reader::ReadHistoryBranchResponse, HistoryPersistenceError>;
    async fn fork_history_branch(&self, shard_id: i32, tree_id: [u8; 16], source_branch_id: [u8; 16], branch_id: [u8; 16], fork_node_id: i64) -> Result<(), HistoryPersistenceError>;
    async fn delete_history_branch(&self, shard_id: i32, tree_id: [u8; 16], branch_id: [u8; 16]) -> Result<(), HistoryPersistenceError>;
    async fn get_history_tree(&self, shard_id: i32, tree_id: [u8; 16]) -> Result<Vec<u8>, HistoryPersistenceError>;
    async fn get_all_history_tree_branches(&self, shard_id: i32, tree_id: [u8; 16]) -> Result<Vec<Vec<u8>>, HistoryPersistenceError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryNode {
    pub node_id: i64,
    pub transaction_id: i64,
    pub data: Vec<u8>,
    pub data_encoding: String,
}

