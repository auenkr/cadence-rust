//! Workflow execution transitions ported from Go's `service/history/execution`.
//!
//! Each module follows the corresponding Go source file. The transaction state
//! itself remains in the parent `mutable_state` module.

pub mod timer;
pub mod child_workflow;
pub mod signal;
pub mod retry;

