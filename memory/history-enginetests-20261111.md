# History unit-test port (branch `history-enginetests`) — 2026-11-11

Final rust-ci (cadence-history, clippy -D warnings + tests): **success**
Run: https://github.com/auenkr/cadence-rust/actions/runs/43072990663 — head `6f956464a5ecc2e2e8d64592a4ec52d53694005a`.
Earlier runs on this branch failed on a test-only type alias (hydrator_tests) and two clippy `needless_update` in executor_tests; both fixed.

## Counts (Rust tests in files; Go tests ported)
| Package | Rust test fns | Notes |
|---|---|---|
| shard (`shard_tests.rs`, `shard_context_tests.rs`) | 11 + 38 = 49 | ~87 Go tests in package; remainder not ported |
| replication batch sizer | 2 | |
| replication task reader | 3 | |
| replication task store | 18 | incl. BoundedAckCache |
| replication DLQ handler | 19 | |
| replication ack manager | 3 | metrics emitter tests live there too |
| replication task fetcher | 5 | Go has 6 |
| replication task hydrator | 7 | table-driven; covers Go's 9 except mutableStateLoader |
| replication task executor | 17 | Go has 19; filterTask tested via `execute` |
| replication task processor | 22 | all 22 Go tests |

## Not done
- engine package (~185 Go tests) — not started (told to skip).
- events, query, workflowcache, failover packages.
- shard: ~38 Go tests not ported.
- Skipped Go tests: budget-manager task-store tests; hydrator `mutableStateLoader` (no equivalent layer in Rust); Go fetcher test that needs mocked clock was re-expressed with a 300ms aggregation interval.

## Deviations
- Real timers, no mocked clock (tokio `start_paused` not used). Metrics emitter start/stop test runs with the real 60s interval.
- Processor DLQ-put-failed test: Go bounds retries with a 2-attempt policy; Rust DLQ policy is unbounded until shutdown, so the test cancels `done` and asserts error plus >=2 attempts.
- Hydrator release-flag assertion adapted for error paths.
- Non-test visibility changes: `determine_replication_latency` (metrics_emitter.rs) and `get_messages` (task_fetcher.rs) now `pub(super)`.

## Rust-vs-Go findings
- No Rust logic bugs found. Queue-state ordering matches Go.
- `ReplicationError` lacks PartialEq/Internal variant, so tests compare via `to_string`/`matches!`.
