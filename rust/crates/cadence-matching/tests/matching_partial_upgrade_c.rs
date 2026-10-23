//! Go parity for `go/service/matching/tasklist/task_reader_test.go:TestGetDispatchTimeout`.
//!
//! Go's `taskReader.getDispatchTimeout` is a method on a private reader. The reader calls
//! `cadence_common::task_isolation::dispatch_timeout` on every dispatch attempt, so the table is
//! checked against that function. `buffers` mirrors the reader's `isolated.len() + 1`, which is 5
//! for the four isolation groups used by the Go test (the default buffer is the fifth).
use cadence_common::task_isolation::dispatch_timeout;

const ASYNC_DISPATCH_TIMEOUT_NANOS: i64 = 3_000_000_000;
const DISPATCH_TIMEOUT_BUFFER_NANOS: i64 = 100_000_000;
const BUFFERS: usize = 5;
/// Go `noIsolationTimeout`.
const NO_ISOLATION: i64 = 0;

#[test]
fn matching_dispatch_timeout_matches_go_get_dispatch_timeout_table() {
    let cases: [(&str, i64, f64, i64); 5] = [
        (
            "default - async dispatch timeout",
            NO_ISOLATION,
            1000.0,
            3_000_000_000,
        ),
        (
            "isolation duration below async dispatch timeout",
            1_000_000_000,
            1000.0,
            1_000_000_000,
        ),
        (
            "isolation duration above async dispatch timeout",
            5_000_000_000,
            1000.0,
            3_000_000_000,
        ),
        (
            "no isolation - low dispatch rps extends timeout",
            NO_ISOLATION,
            0.1,
            50_100_000_000,
        ),
        (
            "with isolation - low dispatch rps extends timeout",
            1_000_000_000,
            0.1,
            50_100_000_000,
        ),
    ];
    for (name, isolation, rps, expected) in cases {
        assert_eq!(
            dispatch_timeout(
                rps,
                BUFFERS,
                ASYNC_DISPATCH_TIMEOUT_NANOS,
                isolation,
                DISPATCH_TIMEOUT_BUFFER_NANOS,
            ),
            expected,
            "{name}"
        );
    }
}
