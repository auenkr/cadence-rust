//! Go `common/types/caller.go` and `common/quotas/caller_bypass.go`.
//!
//! Go carries `types.CallerInfo` in the request context: inbound middleware
//! (`rpc.CallerInfoMiddleware`) reads the `cadence-caller-type` header and
//! stores it; a missing/empty header reads as `"unknown"`. Rust carries it in a
//! tokio task-local that each inbound transport scopes around request handling.
//! Calls made outside any scope (background work, a detached `tokio::spawn`) read
//! `"unknown"`, as a Go context without caller info does.
use std::collections::HashMap;
use std::future::Future;

/// Go `types.CallerTypeHeaderName`.
pub const CALLER_TYPE_HEADER: &str = "cadence-caller-type";
/// Go `types.CallerTypeUnknown`.
pub const CALLER_TYPE_UNKNOWN: &str = "unknown";
/// Go `types.CallerTypeInternal`.
pub const CALLER_TYPE_INTERNAL: &str = "internal";

tokio::task_local! {
    /// `(parsed caller type, header was present on the inbound request)`.
    static CALLER_TYPE: (String, bool);
}

/// Go `types.ParseCallerType`: empty is `unknown`.
pub fn parse(value: &str) -> String {
    if value.is_empty() {
        CALLER_TYPE_UNKNOWN.to_owned()
    } else {
        value.to_owned()
    }
}

/// Go `GetCallerInfoFromContext(ctx).GetCallerType()`.
pub fn current() -> String {
    CALLER_TYPE
        .try_with(|(value, _)| value.clone())
        .unwrap_or_else(|_| CALLER_TYPE_UNKNOWN.to_owned())
}

/// Go `GetContextWithCallerInfoFromHeaders` for a lower-cased header map.
pub async fn scope_from_headers<F: Future>(
    headers: &HashMap<String, String>,
    future: F,
) -> F::Output {
    scope_from_header(headers.get(CALLER_TYPE_HEADER).map(String::as_str), future).await
}

/// Scope from the raw (possibly absent) inbound header value.
pub async fn scope_from_header<F: Future>(raw: Option<&str>, future: F) -> F::Output {
    CALLER_TYPE
        .scope((parse(raw.unwrap_or("")), raw.is_some()), future)
        .await
}

/// Run `future` with `caller_type` as the request's caller type.
pub async fn scope<F: Future>(caller_type: String, future: F) -> F::Output {
    CALLER_TYPE.scope((caller_type, true), future).await
}

/// Go `CallerBypass.ShouldBypass`: true when the context caller type equals a
/// entry of the configured list (callers drop non-string entries, as Go does).
pub fn should_bypass(bypass_types: &[String]) -> bool {
    if bypass_types.is_empty() {
        return false;
    }
    let caller = current();
    bypass_types.iter().any(|t| parse(t) == caller)
}

/// Caller-type header for an outbound request that does not already carry one.
///
/// Go `HeaderForwardingMiddleware` (default rules: everything but `grpc-*`)
/// forwards the inbound header while an inbound call is in scope, and
/// `CallerInfoOutboundMiddleware` tags `internal` only when no inbound call is
/// in scope (background/internal work). An inbound request without the header
/// leaves it unset so the receiver reads `unknown`.
pub fn outbound_header(existing_has_header: bool) -> Option<String> {
    if existing_has_header {
        return None;
    }
    match CALLER_TYPE.try_with(|(value, present)| present.then(|| value.clone())) {
        Ok(forwarded) => forwarded,
        Err(_) => Some(CALLER_TYPE_INTERNAL.to_owned()),
    }
}

/// Only Go's `CallerInfoOutboundMiddleware` half: `internal` when the request
/// has no caller-type header and no inbound call is in scope. Used where header
/// forwarding is already applied by configured rules.
pub fn outbound_internal_default(existing_has_header: bool) -> Option<&'static str> {
    if existing_has_header || CALLER_TYPE.try_with(|_| ()).is_ok() {
        None
    } else {
        Some(CALLER_TYPE_INTERNAL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unscoped_reads_unknown() {
        assert_eq!(current(), "unknown");
    }

    #[tokio::test]
    async fn header_scopes_and_empty_is_unknown() {
        let mut h = HashMap::new();
        h.insert(CALLER_TYPE_HEADER.to_owned(), "cli".to_owned());
        assert_eq!(scope_from_headers(&h, async { current() }).await, "cli");
        h.insert(CALLER_TYPE_HEADER.to_owned(), String::new());
        assert_eq!(scope_from_headers(&h, async { current() }).await, "unknown");
        assert_eq!(
            scope_from_headers(&HashMap::new(), async { current() }).await,
            "unknown"
        );
    }

    #[tokio::test]
    async fn bypass_matches_listed_entries() {
        let list = vec!["cli".to_owned(), "unknown".to_owned()];
        assert!(!should_bypass(&[]));
        assert!(scope("cli".into(), async { should_bypass(&list) }).await);
        assert!(!scope("sdk".into(), async { should_bypass(&list) }).await);
        // Go: absent caller info is "unknown" and an "unknown" entry matches it.
        assert!(should_bypass(&list));
    }

    #[tokio::test]
    async fn internal_default_only_outside_inbound_scope() {
        assert_eq!(outbound_internal_default(false), Some("internal"));
        assert_eq!(outbound_internal_default(true), None);
        let got = scope_from_headers(&HashMap::new(), async { outbound_internal_default(false) }).await;
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn outbound_header_rule() {
        assert_eq!(outbound_header(false).as_deref(), Some("internal"));
        assert_eq!(outbound_header(true), None);
        let mut h = HashMap::new();
        h.insert(CALLER_TYPE_HEADER.to_owned(), "ui".to_owned());
        // inbound with header: forwarded as-is
        let fwd = scope_from_headers(&h, async { outbound_header(false) }).await;
        assert_eq!(fwd.as_deref(), Some("ui"));
        // inbound without header: left unset (receiver reads unknown), never internal
        let none = scope_from_headers(&HashMap::new(), async { outbound_header(false) }).await;
        assert_eq!(none, None);
        // existing header never overwritten
        assert_eq!(scope_from_headers(&h, async { outbound_header(true) }).await, None);
    }
}
