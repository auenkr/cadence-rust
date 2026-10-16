//! Matching persistence retry rules from Go's ThrottleRetry and persistence policy.
//! SQLite never classifies errors as throttling, so only the main policy applies.
use crate::matching_sqlite::{Result, StorageError};
use cadence_common::ExponentialRetryPolicy;
use std::time::{Duration, Instant};

fn run<T>(
    mut operation: impl FnMut() -> Result<T>,
    mut next_delay: impl FnMut(usize) -> Option<Duration>,
    mut pause: impl FnMut(Duration),
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
        if !matches!(error, StorageError::Sql(_)) {
            return Err(previous.unwrap_or(error));
        }
        let Some(delay) = next_delay(attempt) else {
            return Err(previous.unwrap_or(error));
        };
        previous = Some(error);
        attempt += 1;
        pause(delay);
    }
}

pub(crate) fn persistence<T>(mut operation: impl FnMut() -> Result<T>) -> Result<T> {
    let policy = ExponentialRetryPolicy::new(Duration::from_millis(50))
        .with_maximum_interval(Duration::from_secs(10))
        .with_expiration_interval(Duration::from_secs(30));
    let started = Instant::now();
    let mut rng = rand::thread_rng();
    run(
        &mut operation,
        |attempt| policy.compute_next_delay(&mut rng, started.elapsed(), attempt),
        std::thread::sleep,
    )
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
                |_| pauses += 1,
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
}
