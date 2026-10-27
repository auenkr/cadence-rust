//! Matching persistence retry rules from Go's ThrottleRetry and persistence policy.
//! SQLite never classifies errors as throttling, so only the main policy applies.
use crate::matching_sqlite::Result;
use cadence_common::ExponentialRetryPolicy;
use std::time::{Duration, Instant};

fn is_throttle(error: &crate::matching_sqlite::StorageError) -> bool {
    matches!(
        error,
        crate::matching_sqlite::StorageError::Backend {
            kind: crate::matching_task_store::BackendErrorKind::Throttled,
            ..
        }
    )
}

/// Go `ThrottleRetry.Do`. The first attempt always runs, even for an already
/// cancelled context. `wait` returns true when the context ended during the
/// backoff wait; Go then returns `prevErr` (the error before the current attempt)
/// or else the current one, never the context error.
fn run<T>(
    mut operation: impl FnMut() -> Result<T>,
    mut next_delay: impl FnMut(usize) -> Option<Duration>,
    mut throttle_delay: impl FnMut() -> Option<Duration>,
    mut wait: impl FnMut(Duration) -> bool,
) -> Result<T> {
    let mut previous = None;
    let mut attempt = 0;
    loop {
        let error = match operation() {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        // Go SQL converts driver errors to transient InternalServiceError or
        // TimeoutError. Condition failures and serializer errors remain typed.
        if !error.is_transient() {
            return Err(previous.unwrap_or(error));
        }
        let Some(mut delay) = next_delay(attempt) else {
            return Err(previous.unwrap_or(error));
        };
        if is_throttle(&error) {
            if let Some(throttle) = throttle_delay() {
                delay = delay.max(throttle);
            }
        }
        if wait(delay) {
            return Err(previous.unwrap_or(error));
        }
        previous = Some(error);
        attempt += 1;
    }
}

/// Sleeps in short slices so a cancelled context is observed during the wait.
fn sleep_unless_cancelled(delay: Duration, cancelled: &dyn Fn() -> bool) -> bool {
    let end = Instant::now() + delay;
    loop {
        if cancelled() {
            return true;
        }
        let remaining = end.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}

/// Go persistence retry policy, also used by Matching manager metadata reads.
pub fn persistence<T>(operation: impl FnMut() -> Result<T>) -> Result<T> {
    persistence_until(operation, || false)
}

/// Same policy with Go's context-aware `Do`: `cancelled` models `ctx.Done()`.
pub fn persistence_until<T>(
    mut operation: impl FnMut() -> Result<T>,
    cancelled: impl Fn() -> bool,
) -> Result<T> {
    let policy = ExponentialRetryPolicy::new(Duration::from_millis(50))
        .with_maximum_interval(Duration::from_secs(10))
        .with_expiration_interval(Duration::from_secs(30));
    // Go's ThrottleRetry policy: 1s initial, 10s max, no expiration.
    let throttle = ExponentialRetryPolicy::new(Duration::from_secs(1))
        .with_maximum_interval(Duration::from_secs(10));
    let started = Instant::now();
    let mut throttle_attempt = 0;
    let mut rng = rand::thread_rng();
    run(
        &mut operation,
        |attempt| policy.compute_next_delay(&mut rng, started.elapsed(), attempt),
        || {
            let next = throttle.compute_next_delay(
                &mut rand::thread_rng(),
                Duration::ZERO,
                throttle_attempt,
            );
            throttle_attempt += 1;
            next
        },
        |delay| sleep_unless_cancelled(delay, &cancelled),
    )
}

/// Native-store retry: same algorithm as [`persistence_until`]. Legacy and backend
/// errors keep their transient classification; every other native error is
/// non-transient, stashed through the legacy-typed policy and restored unchanged
/// (raw message bytes and subtype intact).
pub fn persistence_bytes_until<T>(
    mut operation: impl FnMut() -> crate::matching_native_task_store::Result<T>,
    cancelled: impl Fn() -> bool,
) -> crate::matching_native_task_store::Result<T> {
    use crate::matching_native_task_store::MatchingTaskStoreBytesError as Native;
    const STASHED: &str = "\0native-retry-error-stashed";
    let stash = std::cell::RefCell::new(None);
    let result = persistence_until(
        || {
            operation().map_err(|error| match error {
                Native::Legacy(error) => error,
                Native::Backend { kind, message } => {
                    crate::matching_sqlite::StorageError::Backend { kind, message }
                }
                other => {
                    *stash.borrow_mut() = Some(other);
                    crate::matching_sqlite::StorageError::ConditionFailed(STASHED.into())
                }
            })
        },
        cancelled,
    );
    result.map_err(|error| match error {
        crate::matching_sqlite::StorageError::ConditionFailed(message) if message == STASHED => {
            stash.borrow_mut().take().unwrap_or(Native::Legacy(
                crate::matching_sqlite::StorageError::ConditionFailed(message),
            ))
        }
        other => Native::Legacy(other),
    })
}

pub fn persistence_bytes<T>(
    operation: impl FnMut() -> crate::matching_native_task_store::Result<T>,
) -> crate::matching_native_task_store::Result<T> {
    persistence_bytes_until(operation, || false)
}

pub(crate) fn lease<T>(now: i64, mut operation: impl FnMut(i64) -> Result<T>) -> Result<T> {
    let started = Instant::now();
    let mut first = true;
    persistence(|| {
        // Keep the supplied initial timestamp exact for the caller's clock;
        // later attempts advance with elapsed time, as Go RenewLease does.
        let attempt_now = if first {
            first = false;
            now
        } else {
            now.wrapping_add(started.elapsed().as_nanos() as i64)
        };
        operation(attempt_now)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_sqlite::StorageError;
    fn error(name: &str) -> StorageError {
        match name {
            "condition" => StorageError::ConditionFailed(name.into()),
            "not-found" => StorageError::NotFound,
            _ => StorageError::Sql(rusqlite::Error::InvalidParameterName(name.into())),
        }
    }
    #[test]
    fn matching_retry_original_go_outcomes() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-retry.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let errors = case["errors"].as_array().unwrap();
            let mut calls = 0;
            let mut pauses = 0;
            let result = run(
                || {
                    let value = errors.get(calls).map(|value| value.as_str().unwrap());
                    calls += 1;
                    match value {
                        Some("success") | None => Ok(()),
                        Some(name) => Err(error(name)),
                    }
                },
                |attempt| {
                    (attempt < case["retries"].as_u64().unwrap() as usize)
                        .then_some(Duration::from_millis(1))
                },
                || None,
                |_| {
                    pauses += 1;
                    false
                },
            );
            assert_eq!(calls, case["calls"].as_u64().unwrap() as usize, "{case}");
            assert_eq!(pauses, case["pauses"].as_u64().unwrap() as usize, "{case}");
            let outcome = match result {
                Ok(()) => "success".to_owned(),
                Err(StorageError::Sql(rusqlite::Error::InvalidParameterName(name))) => name,
                Err(StorageError::NotFound) => "not-found".into(),
                Err(StorageError::ConditionFailed(_)) => "condition".into(),
                Err(other) => panic!("{other}"),
            };
            assert_eq!(outcome, case["outcome"].as_str().unwrap(), "{case}");
        }
    }

    fn throttled() -> StorageError {
        StorageError::Backend {
            kind: crate::matching_task_store::BackendErrorKind::Throttled,
            message: "busy".into(),
        }
    }
    fn message(error: StorageError) -> String {
        match error {
            StorageError::Sql(rusqlite::Error::InvalidParameterName(name)) => name,
            other => other.to_string(),
        }
    }
    #[test]
    fn cancelled_context_still_runs_first_attempt() {
        let mut calls = 0;
        let result: Result<()> = run(
            || {
                calls += 1;
                Err(error("first"))
            },
            |_| Some(Duration::from_millis(1)),
            || None,
            |_| true,
        );
        assert_eq!(calls, 1);
        assert_eq!(message(result.unwrap_err()), "first");
    }
    #[test]
    fn cancel_in_second_wait_returns_previous_error_like_go_prev_err() {
        let mut calls = 0;
        let mut waits = 0;
        let result: Result<()> = run(
            || {
                calls += 1;
                Err(error(if calls == 1 { "first" } else { "second" }))
            },
            |_| Some(Duration::from_millis(1)),
            || None,
            |_| {
                waits += 1;
                waits == 2
            },
        );
        assert_eq!(calls, 2);
        assert_eq!(message(result.unwrap_err()), "first");
    }
    #[test]
    fn throttled_errors_wait_at_least_throttle_backoff_and_still_retry() {
        let mut calls = 0;
        let mut delays = Vec::new();
        let result = run(
            || {
                calls += 1;
                if calls < 3 {
                    Err(throttled())
                } else {
                    Ok(())
                }
            },
            |_| Some(Duration::from_millis(50)),
            || Some(Duration::from_secs(1)),
            |delay| {
                delays.push(delay);
                false
            },
        );
        assert!(result.is_ok());
        assert_eq!(delays, vec![Duration::from_secs(1); 2]);
        // Non-throttle errors keep the plain policy delay.
        let mut delays = Vec::new();
        let mut calls = 0;
        let _ = run(
            || {
                calls += 1;
                if calls < 2 {
                    Err(error("x"))
                } else {
                    Ok(())
                }
            },
            |_| Some(Duration::from_millis(50)),
            || Some(Duration::from_secs(1)),
            |delay| {
                delays.push(delay);
                false
            },
        );
        assert_eq!(delays, vec![Duration::from_millis(50)]);
    }
    #[test]
    fn real_wait_observes_cancellation_promptly() {
        let started = Instant::now();
        assert!(sleep_unless_cancelled(Duration::from_secs(30), &|| true));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!sleep_unless_cancelled(Duration::from_millis(1), &|| false));
    }
}
