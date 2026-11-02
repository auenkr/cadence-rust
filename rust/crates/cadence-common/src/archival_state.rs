//! Port of Go `common/domain/archivalConfigStateMachine.go` plus the
//! register/update archival handling in `common/domain/handler.go`.

use std::sync::{Arc, RwLock};

use crate::archiver::{ArchiverError, ArchiverProvider};

const ERR_INVALID_STATE: &str =
    "Encountered illegal state: archival is enabled but URI is not set (should be impossible)";
const ERR_INVALID_EVENT: &str =
    "Encountered illegal event: default URI is not set (should be impossible)";
const ERR_URI_UPDATE: &str = "Cannot update existing archival URI";

/// Go `ArchivalState`: `enabled` mirrors `Status == ENABLED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivalState {
    pub enabled: bool,
    pub uri: String,
}

fn bad(msg: &str) -> ArchiverError {
    ArchiverError::BadRequest(msg.to_owned())
}

/// Go `ArchivalState.getNextState`. `status` is the event status
/// (`None` = no update attempted). Returns the next state and `changed`.
pub fn next_state(
    s: &ArchivalState,
    status: Option<bool>,
    event_uri: &str,
    default_uri: &str,
    validate_uri: &dyn Fn(&str) -> Result<(), ArchiverError>,
) -> Result<(ArchivalState, bool), ArchiverError> {
    if default_uri.is_empty() {
        return Err(bad(ERR_INVALID_EVENT));
    }
    if s.enabled && s.uri.is_empty() {
        return Err(bad(ERR_INVALID_STATE));
    }
    let state_uri_set = !s.uri.is_empty();
    let event_uri_set = !event_uri.is_empty();
    if state_uri_set && event_uri_set && s.uri != event_uri {
        return Err(bad(ERR_URI_UPDATE));
    }
    let with = |enabled: bool, uri: &str| ArchivalState {
        enabled,
        uri: uri.to_owned(),
    };
    let (next, changed) = if state_uri_set {
        match (s.enabled, status) {
            (true, Some(false)) => (with(false, &s.uri), true),
            (false, Some(true)) => (with(true, &s.uri), true),
            _ => (s.clone(), false),
        }
    } else {
        match status {
            Some(true) => (
                with(true, if event_uri_set { event_uri } else { default_uri }),
                true,
            ),
            _ if event_uri_set => (with(false, event_uri), true),
            _ => (s.clone(), false),
        }
    };
    if !next.uri.is_empty() {
        validate_uri(&next.uri)?;
    }
    Ok((next, changed))
}

/// Per-kind (history / visibility) cluster archival configuration.
#[derive(Clone)]
pub struct KindPolicy {
    /// Go `ClusterConfiguredForArchival` (static and dynamic status).
    pub configured: Arc<dyn Fn() -> bool + Send + Sync>,
    pub default_enabled: bool,
    pub default_uri: String,
}

/// Domain-handler archival policy, installed by the Frontend at startup.
#[derive(Clone)]
pub struct DomainArchivalPolicy {
    pub history: KindPolicy,
    pub visibility: KindPolicy,
    pub provider: ArchiverProvider,
}

static POLICY: RwLock<Option<Arc<DomainArchivalPolicy>>> = RwLock::new(None);

pub fn set_domain_archival_policy(policy: DomainArchivalPolicy) {
    if let Ok(mut guard) = POLICY.write() {
        *guard = Some(Arc::new(policy));
    }
}

pub fn domain_archival_policy() -> Option<Arc<DomainArchivalPolicy>> {
    POLICY.read().ok().and_then(|g| g.clone())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ArchivalKind {
    History,
    Visibility,
}

impl DomainArchivalPolicy {
    fn kind(&self, kind: ArchivalKind) -> &KindPolicy {
        match kind {
            ArchivalKind::History => &self.history,
            ArchivalKind::Visibility => &self.visibility,
        }
    }

    fn validator(&self, kind: ArchivalKind) -> impl Fn(&str) -> Result<(), ArchiverError> + '_ {
        move |uri| match kind {
            ArchivalKind::History => self.provider.validate_history_uri(uri),
            ArchivalKind::Visibility => self.provider.validate_visibility_uri(uri),
        }
    }

    /// Go `RegisterDomain` archival handling. Returns the stored
    /// (enabled, uri) pair.
    pub fn register(
        &self,
        kind: ArchivalKind,
        status: Option<bool>,
        uri: &str,
    ) -> Result<ArchivalState, ArchiverError> {
        let never = ArchivalState {
            enabled: false,
            uri: String::new(),
        };
        let k = self.kind(kind);
        if !(k.configured)() {
            return Ok(never);
        }
        let status = status.unwrap_or(k.default_enabled);
        next_state(
            &never,
            Some(status),
            uri,
            &k.default_uri,
            &self.validator(kind),
        )
        .map(|(s, _)| s)
    }

    /// Go `getHistoryArchivalState` / `getVisibilityArchivalState`:
    /// `Some(next)` only when the state changed.
    pub fn update(
        &self,
        kind: ArchivalKind,
        current: &ArchivalState,
        status: Option<bool>,
        uri: &str,
    ) -> Result<Option<ArchivalState>, ArchiverError> {
        let k = self.kind(kind);
        if !(k.configured)() {
            return Ok(None);
        }
        let (next, changed) =
            next_state(current, status, uri, &k.default_uri, &self.validator(kind))?;
        Ok(changed.then_some(next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(_: &str) -> Result<(), ArchiverError> {
        Ok(())
    }
    fn st(enabled: bool, uri: &str) -> ArchivalState {
        ArchivalState {
            enabled,
            uri: uri.into(),
        }
    }

    #[test]
    fn go_state_table() {
        let d = "file:///d";
        let cases = [
            // (state, status, uri, expected state, changed)
            (st(true, "file:///a"), Some(true), "", st(true, "file:///a"), false),
            (st(true, "file:///a"), Some(false), "", st(false, "file:///a"), true),
            (st(true, "file:///a"), None, "file:///a", st(true, "file:///a"), false),
            (st(false, "file:///a"), Some(true), "", st(true, "file:///a"), true),
            (st(false, "file:///a"), Some(false), "", st(false, "file:///a"), false),
            (st(false, ""), Some(true), "", st(true, d), true),
            (st(false, ""), Some(true), "file:///e", st(true, "file:///e"), true),
            (st(false, ""), Some(false), "file:///e", st(false, "file:///e"), true),
            (st(false, ""), None, "file:///e", st(false, "file:///e"), true),
            (st(false, ""), None, "", st(false, ""), false),
            (st(false, ""), Some(false), "", st(false, ""), false),
        ];
        for (s, status, uri, want, changed) in cases {
            let got = next_state(&s, status, uri, d, &ok).unwrap();
            assert_eq!(got, (want, changed), "{s:?} {status:?} {uri}");
        }
    }

    #[test]
    fn go_state_errors() {
        let e = next_state(&st(true, "file:///a"), None, "file:///b", "file:///d", &ok);
        assert_eq!(e.unwrap_err().message(), "Cannot update existing archival URI");
        let e = next_state(&st(false, ""), None, "", "", &ok);
        assert!(e.unwrap_err().message().starts_with("Encountered illegal event"));
        let e = next_state(&st(true, ""), None, "", "x", &ok);
        assert!(e.unwrap_err().message().starts_with("Encountered illegal state"));
        let reject = |_: &str| Err(ArchiverError::Other("nope".into()));
        let e = next_state(&st(false, ""), Some(true), "", "file:///d", &reject);
        assert_eq!(e.unwrap_err().message(), "nope");
    }

    #[test]
    fn policy_register_and_update() {
        let kind = |on: bool| KindPolicy {
            configured: Arc::new(move || on),
            default_enabled: true,
            default_uri: "file:///tmp/cadence_archival_default".into(),
        };
        let policy = DomainArchivalPolicy {
            history: kind(true),
            visibility: kind(false),
            provider: ArchiverProvider::new(["filestore".to_owned()]),
        };
        let r = policy.register(ArchivalKind::History, None, "").unwrap();
        assert!(r.enabled && r.uri.ends_with("default"));
        let r = policy.register(ArchivalKind::Visibility, Some(true), "file:///x").unwrap();
        assert_eq!(r, st(false, ""));
        let e = policy
            .register(ArchivalKind::History, Some(true), "s3://bucket/x")
            .unwrap_err();
        assert!(!e.message().is_empty());
        let u = policy
            .update(ArchivalKind::History, &st(true, "file:///a"), Some(false), "")
            .unwrap();
        assert_eq!(u, Some(st(false, "file:///a")));
        let u = policy
            .update(ArchivalKind::Visibility, &st(false, ""), Some(true), "")
            .unwrap();
        assert_eq!(u, None);
    }
}
