//! Retry-with-throttle helper mirroring the spirit of
//! `go/common/backoff/retry.go` (`ThrottleRetry::Do`): run an operation,
//! retry on retryable errors with backoff until the expiration elapses.

use std::time::Duration;

use rand::Rng;

use crate::backoff::ExponentialRetryPolicy;

pub type OperationResult<T, E> = Result<T, E>;

/// Decide whether an error is worth retrying.
pub trait Retryable<E> {
    fn is_retryable(&self) -> bool;
}

impl Retryable<String> for String {
    fn is_retryable(&self) -> bool {
        true
    }
}

/// Run `op` until it succeeds, fails non-retryably, or `expiration` elapses.
/// `on_backoff` receives each computed delay (useful for tests; pass `&mut |_| {}`).
pub fn retry_with_backoff<T, E, R, Op, IsRetryable, OnBackoff>(
    policy: &ExponentialRetryPolicy,
    rng: &mut R,
    expiration: Duration,
    mut op: Op,
    mut is_retryable: IsRetryable,
    mut on_backoff: OnBackoff,
) -> Result<T, E>
where
    R: Rng + ?Sized,
    Op: FnMut() -> OperationResult<T, E>,
    IsRetryable: FnMut(&E) -> bool,
    OnBackoff: FnMut(Duration),
{
    let start = std::time::Instant::now();
    let mut attempt: usize = 0;
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !is_retryable(&e) {
                    return Err(e);
                }
                let elapsed = start.elapsed();
                if elapsed >= expiration {
                    return Err(e);
                }
                match policy.compute_next_delay(rng, elapsed, attempt) {
                    None => return Err(e),
                    Some(d) => {
                        on_backoff(d);
                        attempt += 1;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn succeeds_after_retries() {
        let policy = ExponentialRetryPolicy::new(Duration::from_millis(1))
            .with_expiration_interval(Duration::from_secs(5));
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let mut calls = 0;
        let mut backoffs = Vec::new();
        let res = retry_with_backoff(
            &policy,
            &mut rng,
            Duration::from_secs(5),
            || {
                calls += 1;
                if calls < 3 {
                    Err::<u32, String>("transient".to_string())
                } else {
                    Ok(calls)
                }
            },
            |_| true,
            |d| backoffs.push(d),
        );
        assert_eq!(res.unwrap(), 3);
        assert_eq!(backoffs.len(), 2);
    }

    #[test]
    fn stops_on_non_retryable_error() {
        let policy = ExponentialRetryPolicy::new(Duration::from_millis(1));
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let mut backoffs = 0;
        let res = retry_with_backoff(
            &policy,
            &mut rng,
            Duration::from_secs(5),
            || Err::<u32, String>("fatal".to_string()),
            |e| e != "fatal",
            |_| backoffs += 1,
        );
        assert!(res.is_err());
        assert_eq!(backoffs, 0);
    }
}
