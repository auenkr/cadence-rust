//! Persistence layer for the Cadence Rust port.
//!
//! Mirrors the `go/common/persistence` manager semantics. The default
//! implementation is in-memory so the Rust server runs standalone; durable
//! backends (SQL/NoSQL) implement the same traits in follow-ups.

pub mod domains;
pub mod domain_replication;
pub mod domain_update;
pub mod dynamic_config;
pub mod history_branch;
pub mod executions;
pub mod schedules;
pub mod semaphores;
pub mod visibility;
pub mod version_history;
pub mod raw_history;

pub use domains::{DomainRecord, FailoverError, InMemoryDomainStore};
pub use executions::{
    AsyncRequest, AsyncRequestKind, ExecutionRecord, HistoryPageError, InMemoryExecutionStore,
    MutableStateView, QueryError, RecordError, RecordedActivity, RecordedDecision, ResetError,
    StartError, StoredPartitionConfig, TaskError, WorkflowMutationError,
};
pub use schedules::{ScheduleError, ScheduleRecord};
pub use semaphores::{AcquireError, SemaphoreError, SemaphoreHold, SemaphoreMetadata};

pub mod matching_blob;
pub mod matching_sqlite;
pub mod matching_queue;

pub mod matching_gc;

pub mod matching_retry;
