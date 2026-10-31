//! Decisions of Go `replicator/domain_replication_processor.go`.
//!
//! Go pulls domain replication messages from the source cluster's admin API on
//! a jittered poll timer, applies each task with retry, dead-letters tasks that
//! still fail, and advances its cursor after each fetched batch. The remote
//! fetch, timer, retry wrapper and domain executor are not ported yet. This
//! module holds the decisions those loops make so they can be wired without
//! re-deriving them.

use std::time::Duration;

/// Go `fetchTaskRequestTimeout`.
pub const FETCH_TASK_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Go `pollTimerJitterCoefficient`.
pub const POLL_TIMER_JITTER_COEFFICIENT: f64 = 0.2;
/// Go `pollIntervalSecs`.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Go `taskProcessorErrorRetryWait`.
pub const TASK_PROCESSOR_ERROR_RETRY_WAIT: Duration = Duration::from_secs(1);
/// Go `taskProcessorErrorRetryBackoffCoefficient`.
pub const TASK_PROCESSOR_ERROR_RETRY_BACKOFF_COEFFICIENT: f64 = 1.0;
/// Go `InternalServiceError` message for a task without domain attributes.
pub const MISSING_DOMAIN_ATTRIBUTE_MESSAGE: &str =
    "Domain replication task does not set domain task attribute";

/// Go `common.DaemonStatus*` values for the processor latch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonStatus {
    Initialized,
    Started,
    Stopped,
}

impl DaemonStatus {
    /// Go `Start`: only an initialized processor transitions to started.
    pub fn try_start(&mut self) -> bool {
        if *self != Self::Initialized {
            return false;
        }
        *self = Self::Started;
        true
    }
}

/// Message IDs of the replication cursor. Go starts both at -1 so the first
/// request asks the source for everything.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cursor {
    pub last_processed_message_id: i64,
    pub last_retrieved_message_id: i64,
}

impl Default for Cursor {
    fn default() -> Self {
        Self {
            last_processed_message_id: -1,
            last_retrieved_message_id: -1,
        }
    }
}

/// Go `isTransientRetryableError`: every error except `BadRequestError` retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyErrorKind {
    BadRequest,
    Other,
}

pub fn is_transient_retryable_error(kind: ApplyErrorKind) -> bool {
    kind != ApplyErrorKind::BadRequest
}

/// Go checks `fetchDomainReplicationTasks` runs only when this node owns the
/// source cluster ring slot.
pub fn owns_source_cluster(owner_identity: &str, own_identity: &str) -> bool {
    owner_identity == own_identity
}

/// Go `putDomainReplicationTaskToDLQ` precondition: a task without domain
/// attributes cannot be dead-lettered.
pub fn require_domain_attributes<A>(attributes: Option<A>) -> Result<A, &'static str> {
    attributes.ok_or(MISSING_DOMAIN_ATTRIBUTE_MESSAGE)
}

/// Applies one fetched batch in order. A task whose `apply` fails is handed to
/// `dead_letter`. If `dead_letter` fails, the batch stops there and the error is
/// returned, so the caller keeps its previous cursor, as Go does. Otherwise the
/// cursor moves to the response's last retrieved message ID.
///
/// `apply` and `dead_letter` should wrap their own retry, as Go's throttle
/// retry does.
pub fn apply_batch<T, E, D>(
    tasks: &[T],
    response_last_retrieved_message_id: i64,
    mut apply: impl FnMut(&T) -> Result<(), E>,
    mut dead_letter: impl FnMut(&T) -> Result<(), D>,
) -> Result<Cursor, D> {
    for task in tasks {
        if apply(task).is_err() {
            dead_letter(task)?;
        }
    }
    Ok(Cursor {
        last_processed_message_id: response_last_retrieved_message_id,
        last_retrieved_message_id: response_last_retrieved_message_id,
    })
}

/// Go `backoff.JitInt64`: a value in `[(1-c)*input, (1+c)*input)`. `addon` is
/// the random offset in `[0, 2*(input-base))` that the caller draws.
pub fn jit_int64_with_addon(input: i64, coefficient: f64, addon: i64) -> i64 {
    assert!(
        (0.0..=1.0).contains(&coefficient),
        "coefficient cannot be < 0 or > 1"
    );
    if coefficient == 0.0 || input == 0 {
        return input;
    }
    let base = (input as f64 * (1.0 - coefficient)) as i64;
    let span = 2 * (input - base);
    assert!((0..span).contains(&addon), "addon outside the jitter span");
    base + addon
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_go_poll_and_retry_constants() {
        assert_eq!(FETCH_TASK_REQUEST_TIMEOUT, Duration::from_secs(10));
        assert_eq!(POLL_TIMER_JITTER_COEFFICIENT, 0.2);
        assert_eq!(POLL_INTERVAL, Duration::from_secs(1));
        assert_eq!(TASK_PROCESSOR_ERROR_RETRY_WAIT, Duration::from_secs(1));
        assert_eq!(TASK_PROCESSOR_ERROR_RETRY_BACKOFF_COEFFICIENT, 1.0);
    }

    #[test]
    fn start_latch_transitions_once() {
        let mut status = DaemonStatus::Initialized;
        assert!(status.try_start());
        assert_eq!(status, DaemonStatus::Started);
        assert!(!status.try_start());
        status = DaemonStatus::Stopped;
        assert!(!status.try_start());
    }

    #[test]
    fn default_cursor_starts_before_message_zero() {
        assert_eq!(
            Cursor::default(),
            Cursor {
                last_processed_message_id: -1,
                last_retrieved_message_id: -1,
            }
        );
    }

    #[test]
    fn only_bad_request_errors_are_not_retried() {
        assert!(!is_transient_retryable_error(ApplyErrorKind::BadRequest));
        assert!(is_transient_retryable_error(ApplyErrorKind::Other));
    }

    #[test]
    fn fetch_runs_only_on_the_owner_of_the_source_cluster() {
        assert!(owns_source_cluster("host-a", "host-a"));
        assert!(!owns_source_cluster("host-b", "host-a"));
    }

    #[test]
    fn dead_letter_requires_domain_attributes() {
        assert_eq!(require_domain_attributes::<u8>(None), Err(MISSING_DOMAIN_ATTRIBUTE_MESSAGE));
        assert_eq!(require_domain_attributes(Some(7u8)), Ok(7));
    }

    #[test]
    fn successful_batch_advances_cursor_to_response_cursor() {
        let tasks = ["domain-1", "domain-2"];
        let cursor = apply_batch(
            &tasks,
            1000,
            |_| Ok::<(), ()>(()),
            |_: &&str| -> Result<(), ()> { panic!("dead letter must not run for applied tasks") },
        )
        .unwrap();
        assert_eq!(cursor.last_processed_message_id, 1000);
        assert_eq!(cursor.last_retrieved_message_id, 1000);
    }

    #[test]
    fn failed_tasks_are_dead_lettered_and_cursor_still_advances() {
        let tasks = ["domain-1", "domain-2"];
        let mut dead_lettered = Vec::new();
        let cursor = apply_batch(
            &tasks,
            1000,
            |_| Err::<(), &str>("apply failed"),
            |task| {
                dead_lettered.push(*task);
                Ok::<(), ()>(())
            },
        )
        .unwrap();
        assert_eq!(dead_lettered, vec!["domain-1", "domain-2"]);
        assert_eq!(cursor.last_processed_message_id, 1000);
        assert_eq!(cursor.last_retrieved_message_id, 1000);
    }

    #[test]
    fn dead_letter_failure_stops_batch_and_keeps_previous_cursor() {
        let tasks = ["domain-1", "domain-2", "domain-3"];
        let mut applied = Vec::new();
        let result = apply_batch(
            &tasks,
            1001,
            |task| {
                applied.push(*task);
                Err::<(), &str>("apply failed")
            },
            |task| {
                if *task == "domain-1" {
                    Err("dlq unavailable")
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result, Err("dlq unavailable"));
        assert_eq!(applied, vec!["domain-1"]);
    }

    #[test]
    fn jitter_matches_go_bounds_for_one_second_poll() {
        let input = 1_000_000_000;
        assert_eq!(jit_int64_with_addon(input, 0.2, 0), 800_000_000);
        assert_eq!(jit_int64_with_addon(input, 0.2, 399_999_999), 1_199_999_999);
        assert_eq!(jit_int64_with_addon(input, 0.0, 0), input);
        assert_eq!(jit_int64_with_addon(0, 0.2, 0), 0);
    }
}
