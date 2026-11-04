//! `client.VersionChecker` (go/common/client/versionChecker.go), the part History
//! uses: `SupportsStickyQuery` behind the engine's `ClientChecker`.
use crate::engine::runtime::ClientChecker;

const GO_SDK: &str = "uber-go";
const JAVA_SDK: &str = "uber-java";
const STICKY_MIN: (u64, u64, u64) = (1, 0, 0);

/// Parses `major[.minor[.patch]]` like hashicorp/go-version (leading `v` allowed).
fn parse_version(s: &str) -> Option<(u64, u64, u64)> {
    let s = s.strip_prefix('v').unwrap_or(s);
    let core = s.split(['-', '+']).next()?;
    let mut it = core.split('.');
    let major = it.next()?.parse().ok()?;
    let minor = it.next().map_or(Some(0), |p| p.parse().ok())?;
    let patch = it.next().map_or(Some(0), |p| p.parse().ok())?;
    Some((major, minor, patch))
}

#[derive(Default)]
pub struct VersionChecker;

impl ClientChecker for VersionChecker {
    fn supports_sticky_query(&self, implementation: &str, feature_version: &str) -> bool {
        let Some(v) = parse_version(feature_version) else {
            return false;
        };
        match implementation {
            // Unknown/missing impl: StickyQueryUnknownImplConstraints (>=1.0.0), go & java: >=1.0.0.
            "" | GO_SDK | JAVA_SDK => v >= STICKY_MIN,
            // CLI and any other impl do not list the sticky-query feature.
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sticky_query_support() {
        let c = VersionChecker;
        assert!(c.supports_sticky_query("uber-go", "1.5.0"));
        assert!(!c.supports_sticky_query("uber-go", "0.9.0"));
        assert!(!c.supports_sticky_query("uber-go", ""));
        assert!(c.supports_sticky_query("", "1.0.0"));
        assert!(!c.supports_sticky_query("cli", "1.7.0"));
        assert!(!c.supports_sticky_query("unknown", "2.0.0"));
    }
}
