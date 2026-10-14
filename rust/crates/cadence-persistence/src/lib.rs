//! Persistence layer for the Cadence Rust port.
//!
//! Mirrors the `go/common/persistence` manager semantics. The default
//! implementation is in-memory so the Rust server runs standalone; durable
//! backends (SQL/NoSQL) implement the same traits in follow-ups.

pub mod domains;
pub mod executions;

pub use domains::{DomainRecord, InMemoryDomainStore};
pub use executions::{
    ExecutionRecord, HistoryPageError, InMemoryExecutionStore, QueryError, StartError, TaskError,
    WorkflowMutationError,
};
