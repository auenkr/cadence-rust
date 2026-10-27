//! Shared Cadence API types.
//!
//! These mirror the Thrift IDL definitions consumed by the Go server
//! (`go/idls`, generated code under `go/.gen`). Any change here must keep
//! wire compatibility with the Cadence SDKs.
//!
//! Module layout:
//! - [`enums`] — Thrift enums (numeric values match Go `iota` order).
//! - [`types`] — core structs with Go-identical `serde` field names.
//! - [`errors`] — service error shapes.
//! - [`tasktoken`] — task-token JSON serialization (SDK-compatible).
//! - [`domain`] — domain helpers (`IsActiveActive`, failover lookup, ...).
//! - [`history_request`] — history start request (first decision backoff,
//!   retry expiration) mirroring `go/common/util.go`.
//! - [`validation`] — frontend request validation.

pub mod admin;
pub mod b64;
pub mod domain;
pub mod enums;
pub mod errors;
pub mod history_request;
pub mod schedule;
pub mod search_attributes;
pub mod service_types;
pub mod tasks;
pub mod tasktoken;
pub mod types;
pub mod validation;

/// Current API compatibility version marker.
pub const API_COMPAT_VERSION: &str = "v1-thrift";

/// Returns true if the given client version string is API-compatible.
/// Placeholder until real version negotiation is ported.
#[must_use]
pub fn is_compatible(_client_version: &str) -> bool {
    true
}

pub use cadence_common::go_string::GoString;
pub use enums::*;
pub use errors::CadenceError;
pub use types::{
    DataBlob, RetryPolicy, StartWorkflowExecutionRequest, StartWorkflowExecutionResponse, TaskList,
    WorkflowExecution, WorkflowType,
};
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compat_version_is_set() {
        assert!(!API_COMPAT_VERSION.is_empty());
    }
}
