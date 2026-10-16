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
    inner: Mutex<Limiter>,
}
impl TaskListLimiter {
    /// Build the limiter Go creates in `newTaskListLimiter`: initial value is
    /// `TaskDispatchRPS`, and the backing bucket is sized from the initial
    /// partition-divided rate/burst.
    pub fn new(clock: Clock, rps: f64, ttl: i64, min_burst: i64, partitions: i64) -> Self {
        let now = clock();
        let rate = TaskListRate::new(rps, ttl, min_burst, partitions, now);
        let snapshot = rate.snapshot();
        Self {
            clock,
            inner: Mutex::new(Limiter {
                rate,
                bucket: RateLimiter::new(snapshot.limit, snapshot.burst),
            }),
        }
    }
    pub fn with_defaults() -> Self {
        Self::new(
            real_clock(),
            TASK_DISPATCH_RPS,
            TASK_DISPATCH_RPS_TTL_NANOS,
            MIN_TASK_THROTTLING_BURST_SIZE,
            1,
        )
    }
    /// Current backing limit, reported by `DescribeTaskList.status.rate_per_second`.
    pub fn limit(&self) -> f64 {
        self.inner.lock().expect("matching limiter lock").bucket.limit()
    }
    /// Current monotonic-anchored time in Unix nanoseconds.
    pub fn now(&self) -> i64 {
        (self.clock)()
    }
    /// Go `taskListLimiter.ReportLimit`. `partitions` stays at one until real
    /// partition state is wired; do not claim partition scaling from a constant.
    pub fn report_limit(&self, rps: f64) {
        let clock = self.clock.clone();
        let mut inner = self.inner.lock().expect("matching limiter lock");
        if inner.rate.report_with_clock(rps, || clock(), || 1) {
            let snapshot = inner.rate.snapshot();
            inner.bucket.set_limit_and_burst(snapshot.limit, snapshot.burst);
        }
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
            1,
        )
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
}
