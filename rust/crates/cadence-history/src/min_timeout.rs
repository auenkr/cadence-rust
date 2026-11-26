//! Go `shard/context.go` `ensureMinContextTimeout` (`minContextTimeout` = 1s).
//!
//! Go applies it only in the shard context's `CreateWorkflowExecution`, `UpdateWorkflowExecution` and
//! `ConflictResolveWorkflowExecution`: an already-expired parent context fails the call, a parent with less
//! than 1s left is replaced by a fresh 1s context, otherwise the parent deadline stays. The handler goroutine
//! itself is never cancelled, so RPC handlers here run detached and only the write is bounded.
use cadence_persistence::history_contract::PersistenceError;
use std::future::Future;
use std::time::{Duration, Instant};

pub const MIN_CONTEXT_TIMEOUT: Duration = Duration::from_secs(1);

tokio::task_local! {
    /// The caller's deadline (gRPC `grpc-timeout` / TChannel ttl): Go `ctx.Deadline()`.
    pub static CALL_DEADLINE: Instant;
}

#[derive(Debug, PartialEq, Eq)]
pub enum WriteBound {
    /// Parent has no deadline: unbounded.
    None,
    /// Parent already expired: Go returns `parent.Err()` before writing.
    Expired,
    Within(Duration),
}

/// Go `ensureMinContextTimeout` decision.
pub fn write_bound(deadline: Option<Instant>, now: Instant) -> WriteBound {
    match deadline {
        None => WriteBound::None,
        Some(d) if d <= now => WriteBound::Expired,
        Some(d) if d - now >= MIN_CONTEXT_TIMEOUT => WriteBound::Within(d - now),
        Some(_) => WriteBound::Within(MIN_CONTEXT_TIMEOUT),
    }
}

/// Parses a `grpc-timeout` header value (`<digits><H|M|S|m|u|n>`).
pub fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let (digits, unit) = value.split_at(value.len().checked_sub(1)?);
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n.checked_mul(3600)?),
        "M" => Duration::from_secs(n.checked_mul(60)?),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

/// Runs a workflow write under Go's `ensureMinContextTimeout` bound. On expiry the write future is dropped
/// (the driver may still commit it, as with a cancelled Go context) and a timeout error, whose outcome is
/// unknown, is returned so the caller renews the range.
pub async fn ensure_min_timeout<T>(
    write: impl Future<Output = Result<T, PersistenceError>>,
) -> Result<T, PersistenceError> {
    let deadline = CALL_DEADLINE.try_with(|d| *d).ok();
    match write_bound(deadline, Instant::now()) {
        WriteBound::None => write.await,
        WriteBound::Expired => Err(PersistenceError::Timeout("context deadline exceeded".into())),
        WriteBound::Within(limit) => tokio::time::timeout(limit, write)
            .await
            .unwrap_or_else(|_| Err(PersistenceError::Timeout("context deadline exceeded".into()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_matches_go() {
        let now = Instant::now();
        assert_eq!(write_bound(None, now), WriteBound::None);
        assert_eq!(write_bound(Some(now), now), WriteBound::Expired);
        assert_eq!(write_bound(Some(now + Duration::from_millis(5)), now), WriteBound::Within(MIN_CONTEXT_TIMEOUT));
        assert_eq!(write_bound(Some(now + Duration::from_millis(999)), now), WriteBound::Within(MIN_CONTEXT_TIMEOUT));
        assert_eq!(
            write_bound(Some(now + Duration::from_secs(3)), now),
            WriteBound::Within(Duration::from_secs(3))
        );
    }

    #[test]
    fn grpc_timeout_units() {
        assert_eq!(parse_grpc_timeout("50m"), Some(Duration::from_millis(50)));
        assert_eq!(parse_grpc_timeout("2S"), Some(Duration::from_secs(2)));
        assert_eq!(parse_grpc_timeout("1M"), Some(Duration::from_secs(60)));
        assert_eq!(parse_grpc_timeout("x"), None);
        assert_eq!(parse_grpc_timeout(""), None);
    }

    #[tokio::test]
    async fn short_deadline_still_gets_min_timeout() {
        let deadline = Instant::now() + Duration::from_millis(10);
        let out = CALL_DEADLINE
            .scope(deadline, async {
                ensure_min_timeout(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok::<_, PersistenceError>(7)
                })
                .await
            })
            .await;
        assert_eq!(out, Ok(7), "write finishing inside the 1s minimum must not be cancelled");
    }

    #[tokio::test]
    async fn slow_write_times_out_as_unknown_outcome() {
        let deadline = Instant::now() + Duration::from_millis(10);
        let out = CALL_DEADLINE
            .scope(deadline, async {
                ensure_min_timeout(async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok::<_, PersistenceError>(())
                })
                .await
            })
            .await;
        assert!(matches!(out, Err(PersistenceError::Timeout(_))));
    }

    #[tokio::test]
    async fn expired_parent_fails_without_writing() {
        let deadline = Instant::now() - Duration::from_millis(1);
        let wrote = std::sync::atomic::AtomicBool::new(false);
        let out = CALL_DEADLINE
            .scope(deadline, async {
                ensure_min_timeout(async {
                    wrote.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok::<_, PersistenceError>(())
                })
                .await
            })
            .await;
        assert!(out.is_err());
        assert!(!wrote.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn no_deadline_is_unbounded() {
        let out = ensure_min_timeout(async { Ok::<_, PersistenceError>(1) }).await;
        assert_eq!(out, Ok(1));
    }
}
