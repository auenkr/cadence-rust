//! Task-list dispatch limiter: Go `tasklist/task_list_limiter.go` backed by the
//! ported `common/clock` reservation wrapper over `x/time/rate`.
//!
//! The reported aggregate rate and the token bucket live behind one mutex. The
//! clock is anchored once to wall Unix nanoseconds plus a monotonic `Instant`
//! delta, matching Go `time.Time` comparisons that carry a monotonic component;
//! sampling `SystemTime` repeatedly could rewind and corrupt reservation state.
//! Queue expiry still uses wall time, because persisted expiry has no monotonic
//! component.
use cadence_common::{
    matching_rate::TaskListRate,
    rate_limiter::{RateLimiter, Reservation, WaitError},
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

/// Go `config.TaskDispatchRPS`.
pub const TASK_DISPATCH_RPS: f64 = 100000.0;
/// Go `config.TaskDispatchRPSTTL`.
pub const TASK_DISPATCH_RPS_TTL_NANOS: i64 = 60_000_000_000;
/// Go `MatchingMinTaskThrottlingBurstSize` default.
pub const MIN_TASK_THROTTLING_BURST_SIZE: i64 = 1;

/// Monotonic-anchored wall clock in Unix nanoseconds.
#[derive(Clone, Copy, Debug)]
pub struct MonotonicClock {
    anchor_wall: i64,
    anchor: Instant,
}
impl MonotonicClock {
    pub fn new() -> Self {
        let anchor_wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as i64)
            .unwrap_or_default();
        Self {
            anchor_wall,
            anchor: Instant::now(),
        }
    }
    pub fn now(&self) -> i64 {
        self.anchor_wall
            .saturating_add(self.anchor.elapsed().as_nanos() as i64)
    }
}
impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

fn real_clock() -> Clock {
    let clock = MonotonicClock::new();
    Arc::new(move || clock.now())
}

struct Limiter {
    rate: TaskListRate,
    bucket: RateLimiter,
}
/// Outcome of awaiting one limiter token, mapped from Go's `quota.Limiter.Wait`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// A token was acquired and consumed.
    Acquired,
    /// Cannot be satisfied (Go `ErrCannotWait`): zero burst or a deadline the
    /// reservation would miss. The reader yields and retries the head.
    Throttled,
    /// The reader stop signal fired (Go `context.Canceled`).
    Stopped,
}
/// Shared per-task-list dispatch limiter. One instance is owned by each native
/// queue owner, exactly like Go's per-`taskListManager` limiter.
pub struct TaskListLimiter {
    clock: Clock,
    ttl: i64,
    /// Go `taskListLimiter.countPartitions`: read at construction and whenever
    /// `tryUpdate` re-snapshots the partition count. The caller is responsible
    /// for Go's `max(1, cfg.NumTasklistWritePartitions(...))` clamp.
    count_partitions: PartitionCount,
    inner: Mutex<Limiter>,
}
/// Live partition-count provider, mirroring Go's `numPartitions func() int`
/// handed to `newTaskListLimiter`.
pub type PartitionCount = Arc<dyn Fn() -> i64 + Send + Sync>;
impl TaskListLimiter {
    /// Build the limiter Go creates in `newTaskListLimiter`: initial value is
    /// `TaskDispatchRPS`, and the backing bucket is sized from the initial
    /// partition-divided rate/burst.
    pub fn new(
        clock: Clock,
        rps: f64,
        ttl: i64,
        min_burst: i64,
        count_partitions: PartitionCount,
    ) -> Self {
        let now = clock();
        let partitions = count_partitions();
        let rate = TaskListRate::new(rps, ttl, min_burst, partitions, now);
        let snapshot = rate.snapshot();
        Self {
            clock,
            ttl,
            count_partitions,
            inner: Mutex::new(Limiter {
                rate,
                bucket: RateLimiter::new(snapshot.limit, snapshot.burst),
            }),
        }
    }
    /// A single-partition limiter, matching the default
    /// `MatchingNumTasklistWritePartitions` of one.
    pub fn with_defaults() -> Self {
        Self::with_partitions(Arc::new(|| 1))
    }
    /// Build a limiter whose partition divisor is re-read live, matching Go's
    /// `countPartitions` func used by `tryUpdate`.
    pub fn with_partitions(count_partitions: PartitionCount) -> Self {
        Self::with_partitions_and_burst(count_partitions, MIN_TASK_THROTTLING_BURST_SIZE)
    }
    /// Go `newTaskListLimiter` reads `matching.minTaskThrottlingBurstSize` once
    /// when the manager's limiter is built, so a live configuration change only
    /// applies to owners created afterwards.
    pub fn with_partitions_and_burst(count_partitions: PartitionCount, min_burst: i64) -> Self {
        Self::new(
            real_clock(),
            TASK_DISPATCH_RPS,
            TASK_DISPATCH_RPS_TTL_NANOS,
            min_burst,
            count_partitions,
        )
    }
    /// Current backing limit, reported by `DescribeTaskList.status.rate_per_second`.
    pub fn limit(&self) -> f64 {
        self.inner.lock().expect("matching limiter lock").bucket.limit()
    }
    /// Current backing burst size (`max(ceil(rate), minTaskThrottlingBurstSize)`,
    /// capped for a zero rate), mirroring the bucket Go builds in `tryUpdate`.
    pub fn burst(&self) -> i64 {
        self.inner.lock().expect("matching limiter lock").bucket.burst()
    }
    /// Current monotonic-anchored time in Unix nanoseconds.
    pub fn now(&self) -> i64 {
        (self.clock)()
    }
    /// Go `taskListLimiter.ReportLimit`. The partition divisor is re-read from
    /// the live provider exactly when Go's `tryUpdate` re-snapshots it.
    /// Return the aggregate value when Go would update its rate-limit gauge.
    pub fn report_limit(&self, rps: f64) -> Option<f64> {
        let clock = self.clock.clone();
        let count_partitions = self.count_partitions.clone();
        let mut inner = self.inner.lock().expect("matching limiter lock");
        let before = inner.rate.snapshot();
        let now = (self.clock)();
        let elapsed = now.saturating_sub(before.last_update);
        let within_ttl_update = elapsed < self.ttl
            && rps <= before.value
            && (before.value - rps).abs() >= 0.001;
        let ttl_elapsed = elapsed >= self.ttl;
        if inner
            .rate
            .report_with_clock(rps, || clock(), || count_partitions())
        {
            let snapshot = inner.rate.snapshot();
            inner.bucket.set_limit_and_burst(snapshot.limit, snapshot.burst);
        }
        let after = inner.rate.snapshot();
        (within_ttl_update || ttl_elapsed).then_some(after.value)
    }
    /// Current partition divisor used by the backing limit.
    pub fn partitions(&self) -> i64 {
        self.inner
            .lock()
            .expect("matching limiter lock")
            .rate
            .snapshot()
            .partitions
    }
    /// Await one limiter token under the reader's dispatch deadline, matching
    /// Go `Ratelimiter.Wait` reachable through `taskMatcher.ratelimit`.
    ///
    /// `deadline` is an absolute Unix-nanosecond instant on the same monotonic
    /// clock. A token acquired here stays consumed even if the subsequent
    /// matcher handoff is cancelled, because Go's `Wait` already returned nil.
    pub async fn wait_until(
        &self,
        mut stop: watch::Receiver<bool>,
        deadline: Option<i64>,
    ) -> WaitOutcome {
        // Go checks ctx.Err() before consuming any token.
        if *stop.borrow() {
            return WaitOutcome::Stopped;
        }
        let reservation = {
            let now = self.now();
            let mut inner = self.inner.lock().expect("matching limiter lock");
            inner.bucket.reserve_wait(now, deadline)
        };
        let reservation = match reservation {
            Ok(reservation) => reservation,
            Err(WaitError::ZeroBurst | WaitError::Deadline) => return WaitOutcome::Throttled,
        };
        let delay = reservation.delay_nanos();
        if delay <= 0 {
            return WaitOutcome::Acquired;
        }
        // Cancellation restores the reservation at the last observed time,
        // without advancing the real clock, matching Go's cancel-at-latest path.
        struct Guard<'a> {
            limiter: &'a TaskListLimiter,
            reservation: Option<Reservation>,
        }
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                if let Some(reservation) = self.reservation.take() {
                    self.limiter
                        .inner
                        .lock()
                        .expect("matching limiter lock")
                        .bucket
                        .cancel(&reservation);
                }
            }
        }
        let mut guard = Guard {
            limiter: self,
            reservation: Some(reservation),
        };
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_nanos(delay as u64)) => {
                // Disarm before returning so the consumed token is not refunded.
                guard.reservation = None;
                WaitOutcome::Acquired
            }
            _ = stop.changed() => WaitOutcome::Stopped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn fake_clock(now: &Arc<AtomicI64>) -> Clock {
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    }
    fn limiter(rps: f64, now: &Arc<AtomicI64>) -> TaskListLimiter {
        TaskListLimiter::new(
            fake_clock(now),
            rps,
            TASK_DISPATCH_RPS_TTL_NANOS,
            MIN_TASK_THROTTLING_BURST_SIZE,
            Arc::new(|| 1),
        )
    }
    #[test]
    fn report_divides_by_live_partition_count() {
        let now = Arc::new(AtomicI64::new(0));
        let partitions = Arc::new(AtomicI64::new(1));
        let count = partitions.clone();
        let limiter = TaskListLimiter::new(
            fake_clock(&now),
            TASK_DISPATCH_RPS,
            TASK_DISPATCH_RPS_TTL_NANOS,
            MIN_TASK_THROTTLING_BURST_SIZE,
            Arc::new(move || count.load(Ordering::SeqCst)),
        );
        assert_eq!(limiter.limit(), TASK_DISPATCH_RPS);
        assert_eq!(limiter.partitions(), 1);
        // Go `tryUpdate` re-snapshots the live partition count on an accepted
        // lower report; the backing limit is the reported rate divided by it.
        partitions.store(4, Ordering::SeqCst);
        limiter.report_limit(8.0);
        assert_eq!(limiter.partitions(), 4);
        assert_eq!(limiter.limit(), 2.0);
    }
    #[test]
    fn report_limit_returns_go_rate_gauge_updates() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = TaskListLimiter::new(
            fake_clock(&now),
            TASK_DISPATCH_RPS,
            TASK_DISPATCH_RPS_TTL_NANOS,
            MIN_TASK_THROTTLING_BURST_SIZE,
            Arc::new(|| 1),
        );
        assert_eq!(limiter.report_limit(8.0), Some(8.0));
        assert_eq!(limiter.report_limit(50.0), None);
        // Refresh the low value just before expiry. Go retains it on the TTL
        // path when the lower report is still recent, while emitting the
        // aggregate gauge again.
        now.store(TASK_DISPATCH_RPS_TTL_NANOS - 1, Ordering::SeqCst);
        assert_eq!(limiter.report_limit(8.0), None);
        now.store(TASK_DISPATCH_RPS_TTL_NANOS + 1, Ordering::SeqCst);
        assert_eq!(limiter.report_limit(50.0), Some(8.0));
    }
    #[test]
    fn default_limit_and_report_divide_by_partitions() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(TASK_DISPATCH_RPS, &now);
        assert_eq!(limiter.limit(), TASK_DISPATCH_RPS);
        // A lower report is accepted immediately.
        limiter.report_limit(2.0);
        assert_eq!(limiter.limit(), 2.0);
        // A higher report inside the TTL is rejected.
        limiter.report_limit(50.0);
        assert_eq!(limiter.limit(), 2.0);
    }
    #[tokio::test]
    async fn zero_burst_reports_throttled_without_consuming() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(TASK_DISPATCH_RPS, &now);
        limiter.report_limit(0.0);
        let (_stop, stopped) = watch::channel(false);
        assert_eq!(
            limiter.wait_until(stopped, None).await,
            WaitOutcome::Throttled
        );
    }
    #[tokio::test]
    async fn wait_paces_after_terminal_burst_until_tokens_replenish() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(TASK_DISPATCH_RPS, &now);
        limiter.report_limit(3.0);
        // burst = ceil(3) = 3 tokens available immediately.
        let (_stop, stopped) = watch::channel(false);
        for _ in 0..3 {
            assert_eq!(
                limiter.wait_until(stopped.clone(), None).await,
                WaitOutcome::Acquired
            );
        }
        // The fourth token needs a positive delay; without an advanced clock
        // there is no deadline so it would sleep. Instead assert the reservation
        // is throttled when a deadline cannot be met.
        assert_eq!(
            limiter.wait_until(stopped, Some(0)).await,
            WaitOutcome::Throttled
        );
    }
    #[tokio::test]
    async fn cancelled_wait_restores_reserved_token() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = Arc::new(limiter(TASK_DISPATCH_RPS, &now));
        limiter.report_limit(1.0);
        let (stop, stopped) = watch::channel(false);
        assert_eq!(
            limiter.wait_until(stopped.clone(), None).await,
            WaitOutcome::Acquired
        );
        // Next token is not immediately available; start waiting then cancel.
        let waiting = tokio::spawn({
            let inner = limiter.clone();
            async move { inner.wait_until(stopped, None).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        stop.send_replace(true);
        assert_eq!(waiting.await.unwrap(), WaitOutcome::Stopped);
        // The cancelled reservation restored its token at the last observed
        // time, so an immediate retry is available again.
        let (_stop2, stopped2) = watch::channel(false);
        assert_eq!(
            limiter.wait_until(stopped2, None).await,
            WaitOutcome::Acquired
        );
    }
    #[tokio::test]
    async fn stopped_signal_prevents_consuming_a_token() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(TASK_DISPATCH_RPS, &now);
        let (_stop, stopped) = watch::channel(true);
        assert_eq!(limiter.wait_until(stopped, None).await, WaitOutcome::Stopped);
        // The still-available burst token was not consumed.
        let (_stop2, stopped2) = watch::channel(false);
        assert_eq!(
            limiter.wait_until(stopped2, None).await,
            WaitOutcome::Acquired
        );
    }

    // Go `TestRatelimitBehavior` ports the `current` subcases (Go `clock.Ratelimiter`
    // through `taskMatcherImpl.ratelimit`). Go sleeps in real time; here the fake
    // clock is advanced explicitly by the same nominal durations, while real waits
    // inside `wait_until` are timed with `Instant`.
    /// Go `TestRatelimitBehavior` granularity: 10 tokens per second, burst 10.
    const GRANULARITY: Duration = Duration::from_millis(100);
    /// Go `TestRatelimitBehavior` precision for elapsed-time checks.
    const PRECISION: Duration = Duration::from_millis(10);
    const RATE_PER_SECOND: f64 = 10.0;

    fn advance(now: &Arc<AtomicI64>, by: Duration) {
        now.fetch_add(by.as_nanos() as i64, Ordering::SeqCst);
    }
    fn deadline_after(now: &Arc<AtomicI64>, after: Duration) -> i64 {
        now.load(Ordering::SeqCst) + after.as_nanos() as i64
    }
    fn assert_elapsed(expected: Duration, actual: Duration) {
        let delta = if actual > expected {
            actual - expected
        } else {
            expected - actual
        };
        assert!(
            delta <= PRECISION,
            "duration should be within {PRECISION:?} of {expected:?}, was {actual:?}"
        );
    }
    /// Go `limiter.Allow()`: takes a token only when one is available right now.
    async fn allow(limiter: &TaskListLimiter, now: &Arc<AtomicI64>) -> bool {
        let (_stop, stopped) = watch::channel(false);
        let deadline = now.load(Ordering::SeqCst);
        limiter.wait_until(stopped, Some(deadline)).await == WaitOutcome::Acquired
    }
    /// Go `for limit.Allow() {}`: drains every token available at the current time.
    async fn drain(limiter: &TaskListLimiter, now: &Arc<AtomicI64>) {
        while allow(limiter, now).await {}
    }
    async fn available_tokens(limiter: &TaskListLimiter, now: &Arc<AtomicI64>) -> usize {
        let mut available = 0;
        while allow(limiter, now).await {
            available += 1;
        }
        available
    }
    #[tokio::test]
    async fn ratelimit_matrix_no_deadline_returns_immediately_if_available() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        advance(&now, GRANULARITY);
        let (_stop, stopped) = watch::channel(false);
        let start = Instant::now();
        assert_eq!(
            limiter.wait_until(stopped, None).await,
            WaitOutcome::Acquired
        );
        assert_elapsed(Duration::ZERO, start.elapsed());
        assert_eq!(available_tokens(&limiter, &now).await, 0);
    }
    #[tokio::test]
    async fn ratelimit_matrix_no_deadline_waits_until_available() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        let (_stop, stopped) = watch::channel(false);
        let start = Instant::now();
        assert_eq!(
            limiter.wait_until(stopped, None).await,
            WaitOutcome::Acquired
        );
        assert_elapsed(GRANULARITY, start.elapsed());
        advance(&now, GRANULARITY);
        assert_eq!(available_tokens(&limiter, &now).await, 0);
    }
    #[tokio::test]
    async fn ratelimit_matrix_canceling_no_deadline_while_waiting_returns_stopped() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(GRANULARITY / 2).await;
            stop.send_replace(true);
        });
        let start = Instant::now();
        assert_eq!(limiter.wait_until(stopped, None).await, WaitOutcome::Stopped);
        assert_elapsed(GRANULARITY / 2, start.elapsed());
        advance(&now, GRANULARITY / 2);
        assert_eq!(available_tokens(&limiter, &now).await, 0);
    }
    #[tokio::test]
    async fn ratelimit_matrix_returns_immediately_if_canceled() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        advance(&now, GRANULARITY);
        let (_stop, stopped) = watch::channel(true);
        let start = Instant::now();
        assert_eq!(limiter.wait_until(stopped, None).await, WaitOutcome::Stopped);
        assert_elapsed(Duration::ZERO, start.elapsed());
        assert_eq!(available_tokens(&limiter, &now).await, 1);
    }
    #[tokio::test]
    async fn ratelimit_matrix_sufficient_deadline_returns_immediately_if_available() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        advance(&now, GRANULARITY);
        let (_stop, stopped) = watch::channel(false);
        let deadline = deadline_after(&now, GRANULARITY * 2);
        let start = Instant::now();
        assert_eq!(
            limiter.wait_until(stopped, Some(deadline)).await,
            WaitOutcome::Acquired
        );
        assert_elapsed(Duration::ZERO, start.elapsed());
        assert_eq!(available_tokens(&limiter, &now).await, 0);
    }
    #[tokio::test]
    async fn ratelimit_matrix_sufficient_deadline_waits() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        let (_stop, stopped) = watch::channel(false);
        let deadline = deadline_after(&now, GRANULARITY * 2);
        let start = Instant::now();
        assert_eq!(
            limiter.wait_until(stopped, Some(deadline)).await,
            WaitOutcome::Acquired
        );
        assert_elapsed(GRANULARITY, start.elapsed());
        advance(&now, GRANULARITY);
        assert_eq!(available_tokens(&limiter, &now).await, 0);
    }
    #[tokio::test]
    async fn ratelimit_matrix_insufficient_deadline_stops_immediately() {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        let (_stop, stopped) = watch::channel(false);
        let deadline = deadline_after(&now, GRANULARITY / 2);
        let start = Instant::now();
        assert_eq!(
            limiter.wait_until(stopped, Some(deadline)).await,
            WaitOutcome::Throttled
        );
        assert_elapsed(Duration::ZERO, start.elapsed());
        assert_eq!(available_tokens(&limiter, &now).await, 0);
    }
    #[tokio::test]
    async fn ratelimit_matrix_canceling_while_waiting_with_deadline_stops_and_returns_unused_tokens()
    {
        let now = Arc::new(AtomicI64::new(0));
        let limiter = limiter(RATE_PER_SECOND, &now);
        drain(&limiter, &now).await;
        // Sufficient deadline, but canceled after half a token recovers.
        let deadline = deadline_after(&now, GRANULARITY * 2);
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(GRANULARITY / 2).await;
            stop.send_replace(true);
        });
        let start = Instant::now();
        assert_eq!(
            limiter.wait_until(stopped, Some(deadline)).await,
            WaitOutcome::Stopped
        );
        assert_elapsed(GRANULARITY / 2, start.elapsed());
        // Cancel returns the unused reservation, so only 0.5 tokens are present.
        advance(&now, GRANULARITY / 2);
        assert!(
            !allow(&limiter, &now).await,
            "does not yet have a full token, only 0.5"
        );
        advance(&now, GRANULARITY / 2);
        assert!(
            allow(&limiter, &now).await,
            "0.5 -> 1 tokens recovered shows 0.5 were available after waiting"
        );
    }
}
