//! Shared Cadence API types.
//!
//! These mirror the Thrift IDL definitions consumed by the Go server
//! (`go/idls`, generated code under `go/.gen`). Any change here must keep
//! wire compatibility with the Cadence SDKs.

/// Current API compatibility version marker.
pub const API_COMPAT_VERSION: &str = "v1-thrift";

/// Returns true if the given client version string is API-compatible.
/// Placeholder until real version negotiation is ported.
#[must_use]
pub fn is_compatible(_client_version: &str) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compat_version_is_set() {
        assert!(!API_COMPAT_VERSION.is_empty());
    }
}
