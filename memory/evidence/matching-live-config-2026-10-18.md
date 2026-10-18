# Matching `maxTaskDeleteBatchSize` live-config parity evidence

Scope: the task-list filtered `matching.maxTaskDeleteBatchSize` path through
Rust durable Matching GC and SQLite only. This is not a Matching completion or
mixed-service regression claim. Source baseline before implementation was
`ae27fbe82`.

## Semantics established from Go

`task_gc.go` acquires its atomic GC lock, samples the signed `int` property,
checks the backlog, then sends the same limit to every `CompleteTasksLessThan`
call. It stops on storage error without advancing the cursor; it advances only
when deleted count is less than the sampled batch. The Go SQLite plugin rejects
zero before executing SQL. The Go persistence store passes any nonzero signed
limit to SQLite. SQLite treats a negative `LIMIT` as unlimited, so a negative
batch deletes all eligible rows and the Go loop repeats indefinitely because
the result cannot be less than a negative value. The negative loop was observed
only with a fake that injects an error after three calls.

## Reproduction and outcomes

Commands ran from `rust/` with `CARGO_BUILD_JOBS=1`; stdout/stderr and exit
codes are kept beside this report.

| Command | Result |
|---|---|
| `python3 ../tests/run-matching-gc-compat.py` | PASS, exit 0; 15 original-Go cases, including batch 1 and 4,294,967,296; bounded zero/negative GC tests and live-value sampling test passed. |
| `cargo test --locked -p cadence-persistence matching_` | PASS, exit 0; 36 passed, 1 ignored, 145 filtered out. |
| `python3 ../tests/run-matching-storage-compat.py` | PASS, exit 0; Go SQL persistence interop and Rust SQLite interop passed; 8 Go blob fixtures verified. Go actual-store SQL cases showed zero rejected without row changes, `-1` deleted all 3 rows, and 4,294,967,296 deleted one row without truncation. |
| `cargo test --locked -p cadence-server --lib matching_rpc_live_gc_batch_config_is_filtered_and_resampled` | PASS, exit 0; 1 passed, 184 filtered. |
| `cargo clippy --locked --workspace --all-targets -- -D warnings` | PASS, exit 0 on the final code. |

Evidence logs: `matching-live-config-gc-rerun.log`,
`matching-live-config-persistence-rerun.log`, and
`matching-live-config-storage-rerun.log` in this directory; server and Clippy
logs are `matching-live-config-server-focused.log` and
`matching-live-config-clippy-final.log`. The SQL assertions
use Go's real `sqlTaskStore` and SQLite plugin; they are independent from the
Rust rusqlite unit oracle.

## Implementation boundary

Rust uses a signed `i64` from the filtered dynamic-config read through GC and
SQLite. Every default durable queue GC invocation uses that provider; the
explicit batch override remains available for focused collector tests. The
provider is sampled only after the GC lock is acquired, and each new attempt
observes updated overlay values. Zero storage errors leave the cursor
unchanged; GC consumes the error like Go. No negative-value clamp, unsigned
saturation, or nonpositive rejection was added.
