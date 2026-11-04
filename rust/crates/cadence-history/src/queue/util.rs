//! Port of `go/service/history/queue/queue_processor_util.go` plus the small
//! `common/backoff` helpers (JitDuration, persistence retry policy) the package uses.
use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;

use rand::Rng;

use super::deps::{QueueError, Time, TaskKey};
use super::domain_filter::DomainFilter;
use super::processing_queue_state::ProcessingQueueState;

/// Go `types.DomainFilter`.
#[derive(Clone, Debug, Default)]
pub struct PersistenceDomainFilter {
    pub domain_ids: Vec<String>,
    pub reverse_match: bool,
}

/// Go `types.ProcessingQueueState`.
#[derive(Clone, Debug, Default)]
pub struct PersistenceProcessingQueueState {
    pub level: i32,
    pub ack_level: i64,
    pub max_level: i64,
    pub domain_filter: PersistenceDomainFilter,
}

pub fn new_transfer_task_key(task_id: i64) -> TaskKey {
    TaskKey::Transfer { task_id }
}

pub fn new_timer_task_key(visibility_timestamp: Time, task_id: i64) -> TaskKey {
    TaskKey::Timer { visibility_timestamp, task_id }
}

fn transfer_id(k: TaskKey) -> i64 {
    match k {
        TaskKey::Transfer { task_id } => task_id,
        other => panic!("expected transfer task key, got {:?}", other),
    }
}

fn timer_nanos(k: TaskKey) -> i64 {
    match k {
        TaskKey::Timer { visibility_timestamp, .. } => visibility_timestamp.unix_nano(),
        other => panic!("expected timer task key, got {:?}", other),
    }
}

pub fn convert_to_persistence_transfer_processing_queue_states(
    states: &[ProcessingQueueState],
) -> Vec<PersistenceProcessingQueueState> {
    states
        .iter()
        .map(|s| PersistenceProcessingQueueState {
            level: s.level(),
            ack_level: transfer_id(s.ack_level()),
            max_level: transfer_id(s.max_level()),
            domain_filter: convert_to_persistence_domain_filter(s.domain_filter()),
        })
        .collect()
}

pub fn convert_from_persistence_transfer_processing_queue_states(
    p_states: &[PersistenceProcessingQueueState],
) -> Vec<ProcessingQueueState> {
    p_states
        .iter()
        .map(|p| {
            ProcessingQueueState::new(
                p.level,
                new_transfer_task_key(p.ack_level),
                new_transfer_task_key(p.max_level),
                convert_from_persistence_domain_filter(&p.domain_filter),
            )
        })
        .collect()
}

pub fn convert_to_persistence_timer_processing_queue_states(
    states: &[ProcessingQueueState],
) -> Vec<PersistenceProcessingQueueState> {
    states
        .iter()
        .map(|s| PersistenceProcessingQueueState {
            level: s.level(),
            ack_level: timer_nanos(s.ack_level()),
            max_level: timer_nanos(s.max_level()),
            domain_filter: convert_to_persistence_domain_filter(s.domain_filter()),
        })
        .collect()
}

pub fn convert_from_persistence_timer_processing_queue_states(
    p_states: &[PersistenceProcessingQueueState],
) -> Vec<ProcessingQueueState> {
    p_states
        .iter()
        .map(|p| {
            ProcessingQueueState::new(
                p.level,
                new_timer_task_key(Time(p.ack_level), 0),
                new_timer_task_key(Time(p.max_level), 0),
                convert_from_persistence_domain_filter(&p.domain_filter),
            )
        })
        .collect()
}

pub fn convert_to_persistence_domain_filter(f: &DomainFilter) -> PersistenceDomainFilter {
    PersistenceDomainFilter { domain_ids: f.domain_ids.iter().cloned().collect(), reverse_match: f.reverse_match }
}

pub fn convert_from_persistence_domain_filter(f: &PersistenceDomainFilter) -> DomainFilter {
    DomainFilter::new(f.domain_ids.iter().cloned().collect::<HashSet<_>>(), f.reverse_match)
}

/// Argument of Go's `validateProcessingQueueStates(…, ackLevel interface{})`.
pub enum AckLevelValue {
    TaskId(i64),
    Time(Time),
    Unknown,
}

pub fn validate_processing_queue_states(p_states: &[PersistenceProcessingQueueState], ack_level: AckLevelValue) -> bool {
    if p_states.is_empty() {
        return false;
    }
    let min_ack = p_states.iter().map(|p| p.ack_level).min().unwrap();
    match ack_level {
        AckLevelValue::TaskId(id) => min_ack == id,
        AckLevelValue::Time(t) => min_ack == t.unix_nano(),
        AckLevelValue::Unknown => false,
    }
}

/// Go `backoff.JitDuration`: random duration in [(1-c)*d, (1+c)*d).
pub fn jit_duration(d: Duration, coefficient: f64) -> Duration {
    assert!((0.0..=1.0).contains(&coefficient), "coefficient cannot be < 0 or > 1");
    let input = d.as_nanos() as i64;
    if coefficient == 0.0 || input == 0 {
        return d;
    }
    let base = (input as f64 * (1.0 - coefficient)) as i64;
    let span = 2 * (input - base);
    if span <= 0 {
        return d;
    }
    let addon = rand::thread_rng().gen_range(0..span);
    Duration::from_nanos((base + addon) as u64)
}

const RETRY_PERSISTENCE_INITIAL: Duration = Duration::from_millis(50);
const RETRY_PERSISTENCE_MAX: Duration = Duration::from_secs(10);
const RETRY_PERSISTENCE_EXPIRATION: Duration = Duration::from_secs(30);

/// Go `backoff.ThrottleRetry` with `common.CreatePersistenceRetryPolicy()` and
/// `persistence.IsBackgroundTransientError` as the retryable predicate.
pub async fn retry_persistence_operation<T, F, Fut>(mut op: F) -> Result<T, QueueError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, QueueError>>,
{
    let start = std::time::Instant::now();
    let mut attempts: i32 = 0;
    let mut prev_err: Option<QueueError> = None;
    loop {
        let err = match op().await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        if !err.is_background_transient() {
            return Err(prev_err.unwrap_or(err));
        }
        let elapsed = start.elapsed();
        // ExponentialRetryPolicy.ComputeNextDelay
        let done = elapsed > RETRY_PERSISTENCE_EXPIRATION;
        let mut next = RETRY_PERSISTENCE_INITIAL.as_nanos() as f64 * 2f64.powi(attempts);
        let mut stop = done || next <= 0.0;
        if !stop {
            next = next.min(RETRY_PERSISTENCE_MAX.as_nanos() as f64);
            let remaining = RETRY_PERSISTENCE_EXPIRATION.saturating_sub(elapsed).as_nanos() as f64;
            next = next.min(remaining);
            if (next as u64) < RETRY_PERSISTENCE_INITIAL.as_nanos() as u64 {
                stop = true;
            }
        }
        if stop {
            return Err(prev_err.unwrap_or(err));
        }
        attempts += 1;
        prev_err = Some(err);
        tokio::time::sleep(Duration::from_nanos(next as u64)).await;
    }
}

/// Go `maximumTransferTaskKey` (transfer_queue_processor.go).
pub const MAXIMUM_TRANSFER_TASK_KEY: TaskKey = TaskKey::Transfer { task_id: i64::MAX };
/// Go `maximumTimerTaskKey` (timer_queue_processor_base.go).
pub const MAXIMUM_TIMER_TASK_KEY: TaskKey = TaskKey::Timer { visibility_timestamp: Time(i64::MAX), task_id: 0 };
