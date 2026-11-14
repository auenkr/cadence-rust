//! Retry timing and eligibility from `retry.go`.

use std::time::{Duration, SystemTime};

use cadence_api::enums::FailureCategory;

/// Go `types.FailureCategory` persisted integer (Standard=0, Poll=1, Fatal=2).
/// The Rust API enum uses the wire values (Poll=0, Standard=1), so persisted
/// activity info must go through this mapping to stay readable by Go History.
pub fn failure_category_to_go_i32(c: Option<FailureCategory>) -> i32 {
    match c {
        Some(FailureCategory::Poll) => 1,
        Some(FailureCategory::Fatal) => 2,
        _ => 0, // None / Standard
    }
}

/// Inverse of [`failure_category_to_go_i32`]; unknown values map to None.
pub fn failure_category_from_go_i32(v: i32) -> Option<FailureCategory> {
    match v {
        0 => Some(FailureCategory::Standard),
        1 => Some(FailureCategory::Poll),
        2 => Some(FailureCategory::Fatal),
        _ => None,
    }
}


/// Upper bound used by the Go implementation before converting seconds to a
/// nanosecond `time.Duration`.
pub const MAX_BACKOFF_INTERVAL_SECONDS: i64 = i64::MAX / 1_000_000_000;

/// Return whether another attempt is allowed. `fatal` represents Cadence's
/// `FailureCategoryFatal`; size-limit reasons match the four Go constants
/// which are explicitly non-retryable.
pub fn should_retry(
    next_scheduled_time: SystemTime,
    current_attempt: i32,
    max_attempts: i32,
    expiration_time: Option<SystemTime>,
    failure_reason: &str,
    non_retriable_errors: &[String],
    fatal: bool,
) -> bool {
    if fatal || (max_attempts == 0 && expiration_time.is_none()) { return false; }
    if max_attempts > 0 && current_attempt >= max_attempts - 1 { return false; }
    if expiration_time.is_some_and(|expiry| next_scheduled_time > expiry) { return false; }
    if matches!(failure_reason,
        "CANCEL_DETAILS_EXCEEDS_LIMIT" |
        "COMPLETE_RESULT_EXCEEDS_LIMIT" |
        "HEARTBEAT_EXCEEDS_LIMIT" |
        "DECISION_BLOB_SIZE_EXCEEDS_LIMIT") { return false; }
    !non_retriable_errors.iter().any(|reason| reason == failure_reason)
}

/// Calculate retry delay with caller-supplied jitter in [0, 1]. Supplying the
/// random sample keeps the Go's `(1 - jitter * rand.Float64())` rule
/// deterministic for callers and replay tests.
pub fn backoff_interval(
    current_attempt: i32,
    initial_interval_seconds: i32,
    max_interval_seconds: i32,
    backoff_coefficient: f64,
    jitter_coefficient: f64,
    jitter_sample: f64,
) -> Option<Duration> {
    let raw = (initial_interval_seconds as f64) * backoff_coefficient.powf(current_attempt as f64);
    let mut seconds = if !raw.is_finite() || raw <= 0.0 || raw > MAX_BACKOFF_INTERVAL_SECONDS as f64 {
        if max_interval_seconds > 0 { max_interval_seconds as i64 } else { return None; }
    } else { raw as i64 };
    if max_interval_seconds > 0 && seconds > max_interval_seconds as i64 { seconds = max_interval_seconds as i64; }
    let mut nanos = seconds as f64 * 1_000_000_000.0;
    if jitter_coefficient > 0.0 && jitter_coefficient <= 1.0 {
        let sample = jitter_sample.clamp(0.0, 1.0);
        nanos *= 1.0 - jitter_coefficient * sample;
    }
    Some(Duration::from_nanos(nanos.clamp(0.0, u64::MAX as f64) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_limits_and_jitter_match_go_rules() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        assert!(!should_retry(now, 2, 3, None, "err", &[], false));
        assert!(should_retry(now, 0, 0, Some(now), "err", &[], false));
        assert_eq!(backoff_interval(2, 3, 20, 2.0, 0.5, 0.25), Some(Duration::from_secs_f64(10.5)));
    }
}

#[cfg(test)]
mod failure_category_tests {
    use super::*;

    #[test]
    fn go_persisted_category_mapping() {
        assert_eq!(failure_category_to_go_i32(None), 0);
        assert_eq!(failure_category_to_go_i32(Some(FailureCategory::Standard)), 0);
        assert_eq!(failure_category_to_go_i32(Some(FailureCategory::Poll)), 1);
        assert_eq!(failure_category_to_go_i32(Some(FailureCategory::Fatal)), 2);
        for c in [FailureCategory::Standard, FailureCategory::Poll, FailureCategory::Fatal] {
            assert_eq!(failure_category_from_go_i32(failure_category_to_go_i32(Some(c))), Some(c));
        }
        assert_eq!(failure_category_from_go_i32(9), None);
    }
}
