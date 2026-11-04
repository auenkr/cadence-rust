//! Workflow execution transitions ported from Go's `service/history/execution`.
//!
//! Each module follows the corresponding Go source file. The transaction state
//! itself remains in the parent `mutable_state` module.

pub mod timer;
pub mod child_workflow;
pub mod signal;
pub mod retry;
pub mod workflow_close;
pub mod cancellation;
pub mod markers;
pub mod workflow_started;
pub mod state_builder;
pub mod mutable_state_util;
pub mod history_builder;
pub mod context;


pub mod integrity;
pub mod timer_sequence;
pub mod workflow;
