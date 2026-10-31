//! gRPC/YARPC adapter registration surface (Go: wrappers/grpc).
//!
//! The generated protobuf bindings own field conversion. This module keeps
//! the service registration groups aligned with Go's APIHandler.Register.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiGroup {
    Domain,
    Workflow,
    Worker,
    Visibility,
    Meta,
    Schedule,
    Semaphore,
}

pub const API_GROUPS: &[ApiGroup] = &[
    ApiGroup::Domain,
    ApiGroup::Workflow,
    ApiGroup::Worker,
    ApiGroup::Visibility,
    ApiGroup::Meta,
    ApiGroup::Schedule,
    ApiGroup::Semaphore,
];

/// gRPC transport errors are converted from the same internal Cadence error
/// values used by the Thrift adapter; never turn a typed API error into text.
pub fn error_message(error: &cadence_api::errors::CadenceError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_all_go_api_procedure_groups() {
        assert_eq!(API_GROUPS.len(), 7);
        assert!(API_GROUPS.contains(&ApiGroup::Workflow));
        assert!(API_GROUPS.contains(&ApiGroup::Semaphore));
    }
}
