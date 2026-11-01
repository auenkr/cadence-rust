//! Client version checking decorator (Go: wrappers/versioncheck).

#![allow(clippy::result_large_err)]

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

/// Go `client.NewVersionChecker` supported-client table (highest accepted
/// client version per implementation).
#[derive(Clone, Copy, Debug, Default)]
pub struct GoVersionChecker;

fn parse_semver(value: &str) -> Option<(u64, u64, u64)> {
    let value = value.trim().trim_start_matches('v');
    let core = value.split(['-', '+']).next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().map_or(Some(0), |p| p.parse().ok())?;
    let patch = parts.next().map_or(Some(0), |p| p.parse().ok())?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

impl VersionChecker for GoVersionChecker {
    fn client_supported(
        &self,
        enabled: bool,
        client_impl: &str,
        feature_version: &str,
    ) -> Result<(), ClientVersionNotSupportedError> {
        if !enabled || feature_version.is_empty() {
            return Ok(());
        }
        let highest = match client_impl {
            "uber-go" | "cli" => "1.7.0",
            "uber-java" => "1.5.0",
            _ => return Ok(()),
        };
        let max = parse_semver(highest).expect("constant version");
        match parse_semver(feature_version) {
            Some(version) if version <= max => Ok(()),
            _ => Err(ClientVersionNotSupportedError {
                feature_version: feature_version.to_owned(),
                client_impl: client_impl.to_owned(),
                supported_versions: format!("<={highest}"),
            }),
        }
    }
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
            check("StartWorkflowExecution", true, "", "", &Checker)
                .unwrap_err()
                .to_string(),
            "client version not supported"
        );
    }

    #[test]
    fn disabled_version_check_preserves_checker_contract() {
        assert!(check("StartWorkflowExecution", false, "", "", &Checker).is_ok());
    }
}
