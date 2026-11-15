# Replication flake investigation — 2026-11-15

Branch: `repl-flake-p2b`  
Validated head: `8677ca04ad0d6506d02dc2de885268c853396de7`  
Base: `origin/master` at `5aa28f9df5f8366f8339bd0abe21943dbb0a6a6a`

Final branch head after recording this evidence: `740ceca9b230f23d7c2761a6891015ead8154d0e` (documentation-only commit after the six runs).

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

## Follow-up standby-read audit — 2026-11-15

- Working branch `history-standby-read` started at `origin/master` HEAD `23a9a76c4e3d0ea12e977a22f57e31b571c28506`.
- Downloaded the original artifact by its API archive URL after `gh run download` hit the local `/tmp` disk quota. The 81,875-byte artifact contains build/gate logs, JSON `workflow show` outputs, and server logs; it has no SQL dump or other persisted database snapshot. In it `logs/show-B-repl-B-c457c068.json` is literally `[]`. The gate log records `p2-B-continued-history-equal(A->B)` with A at 35 events and B at zero, followed by equal version histories. The Go Frontend log also records `GetWorkflowExecutionHistory` failing with `corrupted history event batch, eventID is not continuous` for this workflow. Thus the evidence points to a branch event sequence/read inconsistency, but does not show which rows were present in MySQL or establish which Rust write/read step caused it.
- Checked Go Frontend `WorkflowHandler.GetWorkflowExecutionHistory` (`go/service/frontend/api/handler.go`): initial and resumed history reads first call History `PollMutableState`; the returned `CurrentBranchToken` is put into the continuation token and passed to `getHistory`/`getRawHistory`. Rust `WorkflowMatchingBackend::get_workflow_execution_history` (`rust/crates/cadence-frontend/src/workflow_matching.rs`) follows the same order and passes the returned `state.branch_token` into the persistence history reader. On long-poll continuation both implementations provide the prior token and version-history item to PollMutableState, then replace it with the returned current token.
- Checked Go and Rust `PollMutableState`: both load mutable state and take the current version-history branch token. Rust's `HistoryEngine::load_mutable_state` uses `get_current_branch_token`; the poll implementation validates the requested version-history item and returns current mutable state. No current-token vs fork-token mismatch was identified.
- Compared Go `historyV2ManagerImpl.readRawHistoryBranch` / `sqlHistoryStore.ReadHistoryBranch` with Rust `read_history_branch` / `store_read` (`go/common/persistence/history_manager.go`, `go/common/persistence/sql/sql_history_store.go`, `rust/crates/cadence-persistence/src/history_reader.rs`). Both traverse ancestor ranges followed by the current branch, cap each range at its exclusive end, carry node/transaction cursor state across pages, skip older transaction rows, and reject decreasing node IDs or a duplicate node ID with a transaction not less than the prior accepted row. Rust MySQL/Postgres readers negate stored SQL transaction IDs after the same `(node_id, txn_id)` ordering used by Go's plugins. No range bound, pagination cursor, or transaction-selection mismatch was found in these paths.
- Compared Go `branchManagerImpl.prepareVersionHistory/createNewBranch` with Rust `BranchManager::prepare_version_history/create_new_branch` and `history_branch::fork`. Both duplicate through the LCA, fork at `LCA event ID + 1`, update the branch token in the new version history, and preserve/clip exclusive ancestor ranges. No demonstrated mismatch found.
- No Rust unit test or implementation change was made: without the failed DB rows or a repeatable state that isolates an operation, a code change would be speculative. To conclusively locate the fault, capture from a reproducing run: workflow/run ID; current and previous branch tokens decoded to tree/branch/ancestor ranges; version-history list/current index; ordered `history_node` rows for each ancestor and current branch (`node_id`, stored and decoded `txn_id`, batch first/last event IDs and versions); and the `ForkHistoryBranch`/`AppendHistoryNodes` requests and successful writes across the conflict/failover. This determines whether event batches were absent, stored under the wrong physical branch/tree/transaction, or present but excluded by range/pagination.

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
