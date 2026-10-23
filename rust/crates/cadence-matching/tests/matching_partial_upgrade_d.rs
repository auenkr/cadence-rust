//! Public-API parity tests for the Matching PARTIAL rows in
//! `memory/matching-go-test-map-20261023.md`, batch D.
use cadence_matching::matching_limiter::TaskListLimiter;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

/// Go `time.Second`, the `TaskDispatchRPSTTL` used by TestTaskListLimiter.
const TTL: i64 = 1_000_000_000;
/// Go `time.Millisecond`.
const MS: i64 = 1_000_000;
/// The `defaultRps` constant of TestTaskListLimiter.
const DEFAULT_RPS: f64 = 100.0;

/// The mocked clock and live partition count that Go's table passes to
/// `newTaskListLimiter`. Both start at the Go initial values: time zero and one partition.
struct Harness {
    now: Arc<AtomicI64>,
    partitions: Arc<AtomicI64>,
    limiter: TaskListLimiter,
}

impl Harness {
    fn new() -> Self {
        let now = Arc::new(AtomicI64::new(0));
        let partitions = Arc::new(AtomicI64::new(1));
        let clock_now = now.clone();
        let count = partitions.clone();
        let limiter = TaskListLimiter::new(
            Arc::new(move || clock_now.load(Ordering::SeqCst)),
            DEFAULT_RPS,
            TTL,
            1,
            Arc::new(move || count.load(Ordering::SeqCst)),
        );
        Self {
            now,
            partitions,
            limiter,
        }
    }

    fn advance(&self, nanos: i64) {
        self.now.fetch_add(nanos, Ordering::SeqCst);
    }

    fn set_partitions(&self, count: i64) {
        self.partitions.store(count, Ordering::SeqCst);
    }
}

type Case = (&'static str, fn(&Harness), f64);

// Go: TestTaskListLimiter (task_list_limiter_test.go:15). Every case runs the same clock and
// partition steps as the Go table and checks the backing limit (`Limit()` in Go, `limit()` here).
#[test]
fn task_list_limiter_matches_go_limit_table() {
    let cases: [Case; 12] = [
        ("no update", |_: &Harness| {}, 100.0),
        (
            "take lower",
            |h: &Harness| {
                h.limiter.report_limit(1.0);
            },
            1.0,
        ),
        (
            "take lowest",
            |h: &Harness| {
                h.limiter.report_limit(50.0);
                h.limiter.report_limit(10.0);
            },
            10.0,
        ),
        (
            "ignore higher",
            |h: &Harness| {
                h.limiter.report_limit(DEFAULT_RPS + 1.0);
            },
            100.0,
        ),
        (
            "take higher after ttl",
            |h: &Harness| {
                h.advance(TTL);
                h.limiter.report_limit(101.0);
            },
            101.0,
        ),
        (
            "keep lower if reported within ttl",
            |h: &Harness| {
                h.advance(MS);
                h.limiter.report_limit(DEFAULT_RPS);
                h.advance(TTL - 1);
                h.limiter.report_limit(DEFAULT_RPS + 1.0);
            },
            100.0,
        ),
        (
            "taker higher if lower not recently reported",
            |h: &Harness| {
                h.advance(MS);
                h.limiter.report_limit(DEFAULT_RPS);
                h.advance(TTL + 1);
                h.limiter.report_limit(DEFAULT_RPS + 1.0);
            },
            101.0,
        ),
        (
            "recalculate partitions on ttl expiration - lower limit",
            |h: &Harness| {
                h.advance(TTL);
                h.set_partitions(2);
                h.limiter.report_limit(50.0);
            },
            25.0,
        ),
        (
            "recalculate partitions on ttl expiration - equal limit",
            |h: &Harness| {
                h.advance(TTL);
                h.set_partitions(2);
                h.limiter.report_limit(DEFAULT_RPS);
            },
            50.0,
        ),
        (
            "recalculate partitions on ttl expiration - higher limit",
            |h: &Harness| {
                h.advance(TTL);
                h.set_partitions(2);
                h.limiter.report_limit(DEFAULT_RPS + 2.0);
            },
            51.0,
        ),
        (
            "recalculate partitions on early updates",
            |h: &Harness| {
                h.set_partitions(2);
                h.limiter.report_limit(50.0);
            },
            25.0,
        ),
        (
            "recalculate partitions even when keeping lower limit",
            |h: &Harness| {
                // lastUpdate time
                h.limiter.report_limit(50.0);
                h.advance(1);
                h.limiter.report_limit(50.0);
                h.advance(TTL - 1);
                h.set_partitions(2);
                // 100 is not applied because 50 was received within the TTL, but the TTL
                // since the last update has passed, so the partitions are recalculated.
                h.limiter.report_limit(DEFAULT_RPS);
            },
            25.0,
        ),
    ];
    for (name, steps, expected) in cases {
        let harness = Harness::new();
        steps(&harness);
        assert_eq!(harness.limiter.limit(), expected, "{name}");
    }
}
