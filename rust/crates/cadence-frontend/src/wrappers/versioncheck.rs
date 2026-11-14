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

/// hashicorp/go-version `NewVersion` result: numeric segments (padded to at
/// least three) plus whether a prerelease part was present.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedVersion {
    segments: Vec<u64>,
    prerelease: bool,
}

impl ParsedVersion {
    /// Go constraint `Check` for `>=` / `<=` against a prerelease-free
    /// constraint: a prerelease version never satisfies it (go-version
    /// `prereleaseCheck`).
    fn cmp_release(&self, other: &[u64; 3]) -> Option<std::cmp::Ordering> {
        if self.prerelease {
            return None;
        }
        let len = self.segments.len().max(3);
        for k in 0..len {
            let a = self.segments.get(k).copied().unwrap_or(0);
            let b = other.get(k).copied().unwrap_or(0);
            if a != b {
                return Some(a.cmp(&b));
            }
        }
        Some(std::cmp::Ordering::Equal)
    }
}

fn is_ident_part(part: &str) -> bool {
    !part.is_empty()
        && part
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'~')
}

fn is_ident_dotted(value: &str) -> bool {
    !value.is_empty() && value.split('.').all(is_ident_part)
}

/// Port of go-version's anchored `versionRegexp`; no whitespace trimming.
fn parse_version(value: &str) -> Option<ParsedVersion> {
    let rest = value.strip_prefix('v').unwrap_or(value);
    let bytes = rest.as_bytes();
    let mut pos = 0;
    let mut segments = Vec::new();
    loop {
        let start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        if pos == start {
            return None;
        }
        segments.push(rest[start..pos].parse::<u64>().ok()?);
        if pos + 1 < bytes.len() && bytes[pos] == b'.' && bytes[pos + 1].is_ascii_digit() {
            pos += 1;
        } else {
            break;
        }
    }
    let tail = &rest[pos..];
    let (pre, meta) = match tail.split_once('+') {
        Some((pre, meta)) => (pre, Some(meta)),
        None => (tail, None),
    };
    let mut prerelease = false;
    if !pre.is_empty() {
        let ident = match pre.strip_prefix('-') {
            Some(ident) => ident,
            // Without the dash the part must start with a letter, '-' or '~'.
            None if !pre.as_bytes()[0].is_ascii_digit() => pre,
            None => return None,
        };
        if !is_ident_dotted(ident) {
            return None;
        }
        prerelease = true;
    }
    if let Some(meta) = meta {
        if !is_ident_dotted(meta) {
            return None;
        }
    }
    Some(ParsedVersion {
        segments,
        prerelease,
    })
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
        let max = [1_u64, if highest == "1.5.0" { 5 } else { 7 }, 0];
        match parse_version(feature_version).and_then(|v| v.cmp_release(&max)) {
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => Ok(()),
            _ => Err(ClientVersionNotSupportedError {
                feature_version: feature_version.to_owned(),
                client_impl: client_impl.to_owned(),
                supported_versions: format!("<={highest}"),
            }),
        }
    }
}

/// Go `versionChecker.SupportsRawHistoryQuery`: true when the client
/// implementation/version supports raw history blobs (uber-go >=1.6.0,
/// uber-java >=1.3.0, cli >=1.6.0). Missing impl or version means unsupported.
pub fn supports_raw_history_query(client_impl: &str, feature_version: &str) -> bool {
    if client_impl.is_empty() || feature_version.is_empty() {
        return false;
    }
    let minimum = match client_impl {
        "uber-go" | "cli" => (1, 6, 0_u64),
        "uber-java" => (1, 3, 0),
        _ => return false,
    };
    let minimum = [minimum.0, minimum.1, minimum.2];
    parse_version(feature_version)
        .and_then(|v| v.cmp_release(&minimum))
        .is_some_and(|o| o != std::cmp::Ordering::Less)
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

    #[test]
    fn raw_history_query_support_matches_go_table() {
        assert!(!supports_raw_history_query("", "1.7.0"));
        assert!(!supports_raw_history_query("uber-go", ""));
        assert!(!supports_raw_history_query("uber-go", "1.5.9"));
        assert!(supports_raw_history_query("uber-go", "1.6.0"));
        assert!(!supports_raw_history_query("uber-java", "1.2.9"));
        assert!(supports_raw_history_query("uber-java", "1.3.0"));
        assert!(supports_raw_history_query("cli", "1.7.0"));
        assert!(!supports_raw_history_query("unknown", "9.0.0"));
        assert!(!supports_raw_history_query("uber-go", "not-a-version"));
    }

    #[test]
    fn solfix18_prerelease_and_malformed_versions_follow_go_version() {
        // go-version: a prerelease never satisfies a release-only constraint.
        assert!(!supports_raw_history_query("uber-go", "1.6.0-rc.1"));
        assert!(supports_raw_history_query("uber-go", "v1.6.0+build.5"));
        assert!(supports_raw_history_query("uber-go", "1.6"));
        assert!(supports_raw_history_query("uber-go", "1.6.0.1"));
        assert!(!supports_raw_history_query("uber-go", " 1.6.0"));
        assert!(!supports_raw_history_query("uber-go", "1.6.0-"));
        assert!(!supports_raw_history_query("uber-go", "1.6.0-!"));
        assert!(parse_version("1.0beta").unwrap().prerelease);
        assert!(parse_version("1.0+").is_none());
        let checker = GoVersionChecker;
        assert!(checker.client_supported(true, "uber-go", "1.7.0").is_ok());
        assert!(checker
            .client_supported(true, "uber-go", "1.7.0.1")
            .is_err());
        assert!(checker
            .client_supported(true, "uber-go", "1.7.0-rc.1")
            .is_err());
        assert!(checker.client_supported(true, "uber-java", "1.5").is_ok());
        assert!(checker
            .client_supported(true, "uber-java", "1.5.1")
            .is_err());
    }
}
