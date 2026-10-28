//! Byte-exact variant of [`crate::qps::EmaQpsTracker`]: isolation-group keys are
//! Go strings, so `0xff`, `0xfe` and U+FFFD groups never share a QPS series.
//! Math, folding and locking mirror the legacy tracker.

use crate::go_string::GoString;
use crate::qps::Clock;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
    time::Duration,
};

struct Accumulator {
    first: bool,
    qps: f64,
    next: i64,
    pending: BTreeMap<i64, i64>,
}

impl Accumulator {
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

    fn fold(&mut self, bucket: i64, exp: f64, seconds: f64) {
        let counter = self.pending.remove(&bucket).unwrap_or_default() as f64;
        self.qps = if self.first {
            self.first = false;
            counter / seconds
        } else {
            self.qps * (1.0 - exp) + counter * exp / seconds
        };
    }

    fn fold_up_to(&mut self, limit: i64, exp: f64, seconds: f64) {
        while self.next <= limit {
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

struct Inner {
    root: Accumulator,
    groups: HashMap<GoString, Accumulator>,
}

/// Go `emaFixedWindowQPSTracker` with byte-exact isolation-group keys.
pub struct EmaQpsBytesTracker {
    clock: Clock,
    start: i64,
    interval: i64,
    exp: f64,
    state: Mutex<Inner>,
}

impl std::fmt::Debug for EmaQpsBytesTracker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmaQpsBytesTracker")
            .field("interval", &self.interval)
            .field("exp", &self.exp)
            .finish_non_exhaustive()
    }
}

impl EmaQpsBytesTracker {
    #[must_use]
    pub fn new(exp: f64, bucket_interval: Duration, clock: Clock) -> Self {
        let start = clock();
        Self {
            clock,
            start,
            interval: i64::try_from(bucket_interval.as_nanos())
                .unwrap_or(i64::MAX)
                .max(1),
            exp,
            state: Mutex::new(Inner {
                root: Accumulator::starting_at(0),
                groups: HashMap::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.state.lock().expect("qps tracker lock")
    }

    fn bucket(&self) -> i64 {
        ((self.clock)() - self.start).div_euclid(self.interval)
    }

    fn seconds(&self) -> f64 {
        self.interval as f64 / 1e9
    }

    pub fn report_counter(&self, amount: i64) {
        let bucket = self.bucket();
        self.lock().root.add(bucket, amount);
    }

    pub fn report_group(&self, group: &[u8], amount: i64) {
        let bucket = self.bucket();
        let mut state = self.lock();
        state.root.add(bucket, amount);
        state
            .groups
            .entry(GoString::from(group))
            .or_insert_with(|| Accumulator::starting_at(bucket))
            .add(bucket, amount);
    }

    fn advance(&self, state: &mut Inner) {
        let limit = self.bucket() - 1;
        let (exp, seconds) = (self.exp, self.seconds());
        state.root.fold_up_to(limit, exp, seconds);
        for accumulator in state.groups.values_mut() {
            accumulator.fold_up_to(limit, exp, seconds);
        }
    }

    pub fn qps(&self) -> f64 {
        let mut state = self.lock();
        self.advance(&mut state);
        state.root.qps
    }

    /// 0 for a group (by exact bytes) that never reported.
    pub fn group_qps(&self, group: &[u8]) -> f64 {
        let mut state = self.lock();
        self.advance(&mut state);
        state
            .groups
            .get(group)
            .map_or(0.0, |accumulator| accumulator.qps)
    }

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
    use std::sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    };

    fn close(expected: f64, actual: f64) {
        assert!((expected - actual).abs() < 1e-6, "expected {expected}, got {actual}");
    }

    #[test]
    fn byte_distinct_groups_have_separate_qps() {
        let now = Arc::new(AtomicI64::new(0));
        let clock_now = now.clone();
        let tracker = EmaQpsBytesTracker::new(
            0.5,
            Duration::from_secs(1),
            Arc::new(move || clock_now.load(Ordering::SeqCst)),
        );
        tracker.report_group(b"\xff", 10);
        tracker.report_group(b"\xfe", 20);
        tracker.report_group("\u{fffd}".as_bytes(), 40);
        tracker.report();
        close(70.0, tracker.qps());
        close(10.0, tracker.group_qps(b"\xff"));
        close(20.0, tracker.group_qps(b"\xfe"));
        close(40.0, tracker.group_qps("\u{fffd}".as_bytes()));
        close(0.0, tracker.group_qps(b"\xfd"));
    }

    #[test]
    fn matches_legacy_multi_cycle_math() {
        let now = Arc::new(AtomicI64::new(0));
        let clock_now = now.clone();
        let tracker = EmaQpsBytesTracker::new(
            0.5,
            Duration::from_secs(1),
            Arc::new(move || clock_now.load(Ordering::SeqCst)),
        );
        tracker.report_group(b"\xff", 10);
        tracker.report();
        tracker.report_group(b"\xff", 20);
        tracker.report();
        close(15.0, tracker.group_qps(b"\xff"));
        close(15.0, tracker.qps());
    }
}
