//! `CreateHistoryStartWorkflowExecutionRequest` port of
//! `go/common/util.go`.
//!
//! Computes the first-decision backoff and the retry-policy expiration
//! timestamp that the history service uses when starting a run.

use cadence_common::cron;

use crate::types::StartWorkflowExecutionRequest;

/// Result of building the history start request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistoryStartRequest {
    /// Go's `FirstDecisionTaskBackoffSeconds`: seconds before the first
    /// decision task is scheduled.
    pub first_decision_task_backoff_seconds: i32,
    /// Go's `ExpirationTimestamp` in nanoseconds since the epoch, set when the
    /// retry policy has a positive expiration interval.
    pub expiration_timestamp: Option<i64>,
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Port of `CreateHistoryStartWorkflowRequest`.
///
/// `now_nanos` is the current wall clock in nanoseconds since the epoch.
/// `jitter` is the random `rand.Int31n(jitterStartSeconds + 1)` seconds already
/// drawn by the caller, so randomness stays out of this pure function.
pub fn create_history_start_workflow_request(
    start_request: &StartWorkflowExecutionRequest,
    now_nanos: i64,
    jitter: std::time::Duration,
) -> Result<HistoryStartRequest, String> {
    let delay_start_seconds = start_request.delay_start_seconds.unwrap_or(0);
    let jitter_start_seconds = start_request.jitter_start_seconds.unwrap_or(0);
    let first_run_at_timestamp = start_request.first_run_at_timestamp.unwrap_or(0);

    let mut first_decision_task_backoff_seconds = delay_start_seconds;

    // A first-run timestamp overrides delay, jitter and cron, exactly once.
    if first_run_at_timestamp > now_nanos {
        first_decision_task_backoff_seconds =
            i32::try_from((first_run_at_timestamp - now_nanos) / NANOS_PER_SECOND)
                .unwrap_or(i32::MAX);
    } else if !start_request.cron_schedule.is_empty() {
        let delayed_start_nanos = add_seconds(now_nanos, delay_start_seconds);
        let delayed_start = cron::naive_from_nanos(delayed_start_nanos);
        let backoff = cron::get_backoff_for_next_schedule_in_seconds(
            &start_request.cron_schedule,
            delayed_start,
            delayed_start,
            start_request.cron_overlap_policy.map(|p| p as i32),
            cron::naive_from_nanos(now_nanos),
            jitter,
        )?;
        // Backoff seconds are relative to the delayed start time.
        first_decision_task_backoff_seconds = backoff + delay_start_seconds;
    } else if jitter_start_seconds > 0 {
        first_decision_task_backoff_seconds += i32::try_from(jitter.as_secs()).unwrap_or(i32::MAX);
    }

    let expiration_timestamp = start_request
        .retry_policy
        .as_ref()
        .filter(|p| p.expiration_interval_in_seconds > 0)
        .map(|p| {
            let expiration_in_seconds = i64::from(p.expiration_interval_in_seconds)
                .saturating_add(i64::from(first_decision_task_backoff_seconds));
            // Go: now.Add(expiration seconds).Round(time.Millisecond).UnixNano()
            round_to_millis(add_seconds(
                now_nanos,
                expiration_in_seconds.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
            ))
        });

    Ok(HistoryStartRequest {
        first_decision_task_backoff_seconds,
        expiration_timestamp,
    })
}

fn add_seconds(nanos: i64, seconds: i32) -> i64 {
    nanos.saturating_add(i64::from(seconds) * NANOS_PER_SECOND)
}

/// Go's `time.Duration.Round(time.Millisecond)`: round half away from zero.
fn round_to_millis(nanos: i64) -> i64 {
    const MILLIS: i64 = 1_000_000;
    let rem = nanos % MILLIS;
    let base = nanos - rem;
    if rem.abs() * 2 >= MILLIS {
        if nanos < 0 {
            base - MILLIS
        } else {
            base + MILLIS
        }
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn request() -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: "d".to_owned(),
            workflow_id: "w".to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn delay_start_only() {
        let r = HistoryStartRequest {
            first_decision_task_backoff_seconds: 0,
            expiration_timestamp: None,
        };
        assert_eq!(r.first_decision_task_backoff_seconds, 0);
        let mut req = request();
        req.delay_start_seconds = Some(30);
        let got =
            create_history_start_workflow_request(&req, 1_000_000_000, Duration::ZERO).unwrap();
        assert_eq!(got.first_decision_task_backoff_seconds, 30);
        assert!(got.expiration_timestamp.is_none());
    }

    #[test]
    fn cron_backoff_uses_next_firing() {
        let mut req = request();
        req.cron_schedule = "0 * * * *".to_owned(); // every hour on the hour
        req.cron_overlap_policy = Some(crate::enums::CronOverlapPolicy::Skipped);
        // 2021-06-01T05:36:40Z -> next firing 06:00:00Z -> 1400 seconds.
        let now = 1_622_525_800_000_000_000i64;
        let got = create_history_start_workflow_request(&req, now, Duration::ZERO).unwrap();
        assert_eq!(got.first_decision_task_backoff_seconds, 1400);
    }

    #[test]
    fn cron_with_delay_adds_delay() {
        let mut req = request();
        req.cron_schedule = "0 * * * *".to_owned();
        req.delay_start_seconds = Some(60);
        let now = 1_622_525_800_000_000_000i64; // 05:36:40Z
        let got = create_history_start_workflow_request(&req, now, Duration::ZERO).unwrap();
        // Delayed start 05:37:40Z -> next firing 06:00:00Z is 1340s, plus the
        // 60s delay, so the first decision lands on 06:00:00Z.
        assert_eq!(got.first_decision_task_backoff_seconds, 1400);
    }

    #[test]
    fn first_run_at_timestamp_wins() {
        let mut req = request();
        req.cron_schedule = "0 * * * *".to_owned();
        req.jitter_start_seconds = Some(100);
        let now = 1_000_000_000i64;
        req.first_run_at_timestamp = Some(now + 250 * 1_000_000_000);
        let got =
            create_history_start_workflow_request(&req, now, Duration::from_secs(50)).unwrap();
        assert_eq!(got.first_decision_task_backoff_seconds, 250);
    }

    #[test]
    fn retry_expiration_adds_backoff() {
        let mut req = request();
        req.delay_start_seconds = Some(10);
        req.retry_policy = Some(crate::types::RetryPolicy {
            initial_interval_in_seconds: 1,
            backoff_coefficient: 2.0,
            maximum_interval_in_seconds: 60,
            expiration_interval_in_seconds: 600,
            ..Default::default()
        });
        let now = 1_000_000_000i64;
        let got = create_history_start_workflow_request(&req, now, Duration::ZERO).unwrap();
        assert_eq!(got.first_decision_task_backoff_seconds, 10);
        // now + (600 + 10)s, rounded to a millisecond.
        assert_eq!(got.expiration_timestamp, Some(now + 610 * 1_000_000_000));
    }

    #[test]
    fn invalid_cron_is_an_error() {
        let mut req = request();
        req.cron_schedule = "not a cron".to_owned();
        assert!(create_history_start_workflow_request(&req, 0, Duration::ZERO).is_err());
    }
}
