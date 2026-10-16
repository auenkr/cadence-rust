//! Go `common/stats` QPS trackers used by the Matching task list manager
//! (`NewEmaFixedWindowQPSTracker`) and reported through `DescribeTaskList`
//! (`newTasksPerSecond`), `LoadBalancerHints.RatePerSecond` and the isolation
//! group metrics (`GroupQPS`).
//!
//! Go runs the EMA fixed-window tracker on a background ticker. Here the same
//! EMA step is applied from an injectable clock, one step per elapsed
//! `bucket_interval`, and arrivals are recorded against the bucket they landed
//! in. For the same arrival times and tick boundaries the reported QPS is the
//! same as the ticker's, without a background task or lock contention on reads.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

/// Unix-nanosecond clock, matching the rest of the matching state.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Wall-clock source, for callers that do not inject a clock.
#[must_use]
pub fn system_clock() -> Clock {
    Arc::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|time| time.as_nanos() as i64)
            .unwrap_or_default()
    })
}

/// Go `emaFixedWindowState`: one EMA accumulator plus the arrivals of the
/// buckets that have not been folded yet.
struct Accumulator {
    /// Go starts every state with `firstBucket: true`, so the first window is
    /// stored directly instead of being averaged against an initial 0.
    first: bool,
    qps: f64,
    /// Index of the next window to fold; earlier windows are already folded.
    next: i64,
    pending: BTreeMap<i64, i64>,
}

impl Default for Accumulator {
    fn default() -> Self {
        Self::starting_at(0)
    }
}

impl Accumulator {
    /// A state whose next window to fold is `bucket`, like a Go state created
    /// in that window: its first fold stores the window directly.
    fn starting_at(bucket: i64) -> Self {
        Self {
            first: true,
            qps: 0.0,
            next: bucket,
            pending: BTreeMap::new(),
        }
    }

    fn add(&mut self, bucket: i64, amount: i64) {
        *self.pending.entry(bucket).or_default() += amount;
    }

    /// Go `emaFixedWindowState.report`: the first window is stored directly,
    /// every later window is `qps*(1-exp) + counter*exp/seconds`.
    fn fold(&mut self, bucket: i64, exp: f64, seconds: f64) {
        let counter = self.pending.remove(&bucket).unwrap_or_default() as f64;
        self.qps = if self.first {
            self.first = false;
            counter / seconds
        } else {
            self.qps * (1.0 - exp) + counter * exp / seconds
        };
    }

    /// Fold every window up to and including `limit`, oldest first, so a gap
    /// applies one EMA step per missed tick exactly like the ticker.
    fn fold_up_to(&mut self, limit: i64, exp: f64, seconds: f64) {
        while self.next <= limit {
            // Windows without arrivals only decay, so a long idle span folds in
            // one step instead of looping once per missed tick.
            let next_arrival = self
                .pending
                .range(self.next..=limit)
                .next()
                .map(|(bucket, _)| *bucket)
                .unwrap_or(limit);
            if next_arrival > self.next {
                let span = (next_arrival - self.next) as f64;
                self.qps = if self.first {
                    self.first = false;
                    0.0
                } else {
                    self.qps * (1.0 - exp).powf(span)
                };
                self.next = next_arrival;
            }
            self.fold(self.next, exp, seconds);
            self.next += 1;
        }
    }
}

#[derive(Default)]
struct Inner {
    root: Accumulator,
    groups: HashMap<String, Accumulator>,
}

/// Go `emaFixedWindowQPSTracker`. `exp` is the EMA weight applied to the newest
/// window (Go passes 0.5 for matching) and `bucket_interval` is
/// `matching.qpsTrackerInterval` (Go default 10s).
pub struct EmaQpsTracker {
    clock: Clock,
    start: i64,
    interval: i64,
    exp: f64,
    state: Mutex<Inner>,
}

impl std::fmt::Debug for EmaQpsTracker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmaQpsTracker")
            .field("interval", &self.interval)
            .field("exp", &self.exp)
            .finish_non_exhaustive()
    }
}

impl EmaQpsTracker {
    /// Go `NewEmaFixedWindowQPSTracker(timeSource, exp, bucketInterval, baseEvent)`.
    #[must_use]
    pub fn new(exp: f64, bucket_interval: Duration, clock: Clock) -> Self {
        let start = clock();
        Self {
            clock,
            start,
            interval: i64::try_from(bucket_interval.as_nanos()).unwrap_or(i64::MAX).max(1),
            exp,
            state: Mutex::new(Inner::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.state.lock().expect("qps tracker lock")
    }

    /// Index of the bucket that is currently open.
    fn bucket(&self) -> i64 {
        ((self.clock)() - self.start).div_euclid(self.interval)
    }

    fn seconds(&self) -> f64 {
        self.interval as f64 / 1e9
    }

    /// Go `ReportCounter`: one arrival with no isolation group.
    pub fn report_counter(&self, amount: i64) {
        let bucket = self.bucket();
        self.lock().root.add(bucket, amount);
    }

    /// Go `ReportGroup`: counts toward the root and toward the group.
    pub fn report_group(&self, group: &str, amount: i64) {
        let bucket = self.bucket();
        let mut state = self.lock();
        state.root.add(bucket, amount);
        state
            .groups
            .entry(group.to_string())
            .or_insert_with(|| Accumulator::starting_at(bucket))
            .add(bucket, amount);
    }

    /// Fold the buckets the clock has completed, as the ticker would have. The
    /// open bucket stays pending, matching `QPS()` reading the last tick value.
    fn advance(&self, state: &mut Inner) {
        let limit = self.bucket() - 1;
        let (exp, seconds) = (self.exp, self.seconds());
        state.root.fold_up_to(limit, exp, seconds);
        for accumulator in state.groups.values_mut() {
            accumulator.fold_up_to(limit, exp, seconds);
        }
    }

    /// Go `QPS`.
    pub fn qps(&self) -> f64 {
        let mut state = self.lock();
        self.advance(&mut state);
        state.root.qps
    }

    /// Go `GroupQPS`: 0 for a group that never reported.
    pub fn group_qps(&self, group: &str) -> f64 {
        let mut state = self.lock();
        self.advance(&mut state);
        state.groups.get(group).map_or(0.0, |accumulator| accumulator.qps)
    }

    /// Go's ticker body `report()`: fold the window that is currently open.
    /// `qps`/`group_qps` fold completed windows from the clock instead, so this
    /// is only the manual hook `common/stats`'s own tests use.
    pub fn report(&self) {
        let bucket = self.bucket();
        let (exp, seconds) = (self.exp, self.seconds());
        let mut state = self.lock();
        state.root.fold(bucket, exp, seconds);
        state.root.next = state.root.next.max(bucket + 1);
        for accumulator in state.groups.values_mut() {
            accumulator.fold(bucket, exp, seconds);
            accumulator.next = accumulator.next.max(bucket + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    const RESOLUTION: f64 = 1e-6;

    fn mocked(now: &Arc<AtomicI64>) -> Clock {
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    }

    fn close(expected: f64, actual: f64) {
        assert!((expected - actual).abs() < RESOLUTION, "expected {expected}, got {actual}");
    }

    /// Go `TestEmaFixedWindowQPSTracker`.
    #[test]
    fn ema_tracker_reports_first_window_then_averages() {
        let now = Arc::new(AtomicI64::new(0));
        let tracker = EmaQpsTracker::new(0.4, Duration::from_secs(1), mocked(&now));
        tracker.report_counter(10);
        tracker.report_counter(20);
        // Nothing is reported before the first tick, exactly like Go's ticker.
        close(0.0, tracker.qps());
        now.store(1_000_000_000, Ordering::SeqCst);
        close(30.0, tracker.qps());
        tracker.report_counter(10);
        now.store(2_000_000_000, Ordering::SeqCst);
        close(22.0, tracker.qps());
    }

    /// Go `TestEmaFixedWindowQPSTracker_Groups`.
    #[test]
    fn ema_tracker_groups() {
        let now = Arc::new(AtomicI64::new(0));
        let tracker = EmaQpsTracker::new(0.5, Duration::from_secs(1), mocked(&now));
        tracker.report_group("foo", 10);
        tracker.report_group("bar", 20);
        tracker.report();
        close(30.0, tracker.qps());
        close(10.0, tracker.group_qps("foo"));
        close(20.0, tracker.group_qps("bar"));
        close(0.0, tracker.group_qps("unknown"));
    }

    /// Go `TestEmaFixedWindowQPSTracker_MultipleReportCycles`.
    #[test]
    fn ema_tracker_multiple_report_cycles() {
        let now = Arc::new(AtomicI64::new(0));
        let tracker = EmaQpsTracker::new(0.5, Duration::from_secs(1), mocked(&now));
        tracker.report_group("foo", 10);
        tracker.report_group("bar", 20);
        tracker.report();
        tracker.report_group("foo", 20);
        tracker.report_group("bar", 40);
        tracker.report();
        close(45.0, tracker.qps());
        close(15.0, tracker.group_qps("foo"));
        close(30.0, tracker.group_qps("bar"));
        close(0.0, tracker.group_qps("unknown"));
    }

    /// A read after several idle buckets folds one bucket per missed tick, so
    /// arrivals are attributed to the window they landed in.
    #[test]
    fn ema_tracker_attributes_arrivals_to_their_bucket() {
        let now = Arc::new(AtomicI64::new(0));
        let tracker = EmaQpsTracker::new(0.5, Duration::from_secs(1), mocked(&now));
        tracker.report_counter(10);
        now.store(2_000_000_000, Ordering::SeqCst);
        tracker.report_counter(30);
        // Bucket 0 (10) is the first window, bucket 1 is empty, and bucket 2 is
        // still open, so nothing from bucket 2 is reported yet.
        close(5.0, tracker.qps());
        now.store(3_000_000_000, Ordering::SeqCst);
        // Bucket 2 (30) folds with the EMA weight.
        close(17.5, tracker.qps());
    }
}
