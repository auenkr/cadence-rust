# Replication flake investigation — 2026-11-15

Branch: `repl-flake-p2b`  
Validated head: `8677ca04ad0d6506d02dc2de885268c853396de7`  
Base: `origin/master` at `5aa28f9df5f8366f8339bd0abe21943dbb0a6a6a`

## Original failure and retry

- Failed Rust-active-B MySQL run: https://github.com/auenkr/cadence-rust/actions/runs/43647065597 (source/head `3a6a5a44fcb20fe871b6093526d447d01417a0ff`). `GATE FAIL p2-B-continued-history-equal(A->B)`: active A had 35 events, standby B had 0.
- Retry: https://github.com/auenkr/cadence-rust/actions/runs/43648264528 (same source/head), `GATE SUMMARY failed: []`.
- In `tests/gates/run-history-repl-gate.py`, `compare()` polls exact event equality for 300 seconds at 5-second intervals. Therefore this was not a short, immediate assertion racing normal replication catch-up.
- The failed artifact’s standby `DescribeWorkflowExecution` version history had advanced to event 35 (`[25@1, 35@10]`) while `workflow show` returned no history events. New workflow C and the old completed workflow replicated A->B successfully in that same gate. The Rust History log had no replication task apply error or DLQ insertion around the failure. This localizes the observed inconsistency to the Rust History standby’s failover-era history event read/persistence path, rather than the gate’s equality predicate or a brief delay. The available artifact does not establish which lower-level operation lost or hid the event batch.
- Retry Rust binary hashes differed from the failed run because each run was built independently. The retry was not a strict binary-identical replay.

## Code-path comparison

- Gate producer/consumer path is History replication over `GetReplicationMessages`; this is not Kafka publishing. Kafka in this workflow is only the unrelated optional async-workflow lane.
- Rust `TaskAckManager` and Go `TaskAckManager` both read up to max read level, hydrate tasks, shrink oversized responses by moving `LastRetrievedMessageID` back to the final retained task, then ack the prior read level. Rust `TaskProcessor` retries failed history application, writes exhausted failures to DLQ, and returns successfully only after application/DLQ handling. The corresponding Go path in `go/service/history/replication` follows the same structure.
- Rust `HistoryReplicatorImpl::apply_non_start_events_to_current_branch` and Go `applyNonStartEventsToCurrentBranch` both apply history events and then call the transaction manager to update workflow state. No specific parity mismatch was established in the inspected code, and no source behavior was changed without evidence for a targeted fix.

## Repeated mixed-cluster validation

All six dispatched runs used MySQL and `repl_run_tag` to avoid the workflow’s `cancel-in-progress` concurrency key cancelling identical parallel repetitions. Defaults match the recorded lane (`kafka=true`, optional ES/archive/two-history lanes off). Each run used a fresh Actions target directory and printed its cadence-server SHA256.

| Rust History side | Run | Head SHA | Result | `cadence-server` SHA256 |
|---|---|---|---|---|
| A (Go active -> Rust standby) | https://github.com/auenkr/cadence-rust/actions/runs/43677852956 | `8677ca04ad0d6506d02dc2de885268c853396de7` | success; equality OK; `GATE SUMMARY failed: []` | `b15f397cdfd85ab4af1fda91e542e652ab04696bf90f15316a4f6bd7217d05e8` |
| A | https://github.com/auenkr/cadence-rust/actions/runs/43677853040 | `8677ca04ad0d6506d02dc2de885268c853396de7` | success; equality OK; `GATE SUMMARY failed: []` | `16c9d5f645e613da4f64c609a6f646e2aa13745428c5f455b62207c3e74056d2` |
| A | https://github.com/auenkr/cadence-rust/actions/runs/43677853094 | `8677ca04ad0d6506d02dc2de885268c853396de7` | success; equality OK; `GATE SUMMARY failed: []` | `421dc3440b74d0fbdcdcf1462d1086d2289f7ae6e2a3c27aa624858253fbee48` |
| B (Rust active -> Go standby) | https://github.com/auenkr/cadence-rust/actions/runs/43677853413 | `8677ca04ad0d6506d02dc2de885268c853396de7` | success; equality OK; `GATE SUMMARY failed: []` | `61384e7b3fd5a7ba00844a4fc1ed524b3b9b996263642a6c6ac4f5a28123dd27` |
| B | https://github.com/auenkr/cadence-rust/actions/runs/43677853650 | `8677ca04ad0d6506d02dc2de885268c853396de7` | success; equality OK; `GATE SUMMARY failed: []` | `808db1abad2d1bed8b3dfdd03b8eaa2da0d071642a410dc2915cf9f6f1902979` |
| B | https://github.com/auenkr/cadence-rust/actions/runs/43677853256 | `8677ca04ad0d6506d02dc2de885268c853396de7` | success; equality OK; `GATE SUMMARY failed: []` | `e29bd62e03c97f3609f87db15bc04af407689b4d45118c330056fbd1b888dde6` |

## Verdict and limits

Observed root cause category: a transient Rust History history-event persistence/read inconsistency during cross-cluster failover. This is not explained by a too-short gate wait. The six repetitions are 6/6 green, so the failure did not recur, and available failed-run logs do not pinpoint a specific code-level defect. No Rust source change or unit test was made; changing replication behavior without a demonstrated mismatch would be speculative. The equality check remains unchanged. The workflow-only `repl_run_tag` input preserves prior concurrency behavior when blank and permits explicitly tagged identical repetitions to run in parallel.

No `cargo`, `go`, or `docker` commands were run locally. Rust CI was not dispatched because Rust code did not change; every repetition built and exercised the Rust service on Actions.
