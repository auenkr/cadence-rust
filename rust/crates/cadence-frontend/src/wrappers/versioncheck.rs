//! Client version checking decorator (Go: wrappers/versioncheck).

use cadence_api::errors::{CadenceError, ClientVersionNotSupportedError};

/// Go intentionally exempts these metadata/health calls from version
/// negotiation. Keep this list beside the policy so newly added endpoints do
/// not silently inherit a different behavior.
pub fn requires_version_check(operation: &str) -> bool {
    !matches!(
        operation,
        "GetClusterInfo" | "GetTaskListsByDomain" | "Health"
    )
}

pub trait VersionChecker {
    fn client_supported(
        &self,
        enabled: bool,
        client_impl: &str,
        feature_version: &str,
    ) -> Result<(), ClientVersionNotSupportedError>;
}

pub fn check(
    operation: &str,
    enabled: bool,
    client_impl: &str,
    feature_version: &str,
    checker: &dyn VersionChecker,
) -> Result<(), CadenceError> {
    if !requires_version_check(operation) {
        return Ok(());
    }
    checker
        .client_supported(enabled, client_impl, feature_version)
        .map_err(CadenceError::ClientVersionNotSupported)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Checker;
    impl VersionChecker for Checker {
        fn client_supported(
            &self,
            enabled: bool,
            client_impl: &str,
            feature_version: &str,
        ) -> Result<(), ClientVersionNotSupportedError> {
            if enabled && (client_impl.is_empty() || feature_version.is_empty()) {
                Err(ClientVersionNotSupportedError::default())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn metadata_exemptions_match_go_wrapper() {
        for method in ["GetClusterInfo", "GetTaskListsByDomain", "Health"] {
            assert!(!requires_version_check(method));
            assert!(check(method, true, "", "", &Checker).is_ok());
        }
        assert!(requires_version_check("StartWorkflowExecution"));
        assert_eq!(
            check("StartWorkflowExecution", true, "", "", &Checker).unwrap_err().to_string(),
            "client version not supported"
        );
    }

    #[test]
    fn disabled_version_check_preserves_checker_contract() {
        assert!(check("StartWorkflowExecution", false, "", "", &Checker).is_ok());
    }
}
