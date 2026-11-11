# History churn flake (two_history) 2026-11-12

Failed runs: 43061124538 (mysql), 43075206911 (cassandra). Symptom: sdk-history-churn-workflows
FAIL (deadline-exceeded) after only ~9 workflows, then every later SDK step timed out.

Root cause (rust-history.log): during churn the SDK retries ExecuteWorkflow with the same workflow id;
the first create hits AlreadyStarted, start flow then calls `create_workflow_execution` a second time
(WorkflowIdReuse) with the SAME snapshot. `EngineStartHost::create_workflow_execution`
(`wiring/assembly/engine_start.rs`) `take()`d the prepared `ClosedTxn`, so the second call failed with
`internal: start transaction not prepared` ("unknown failure detected when creating execution",
repeated 30x per worker). Go reuses the snapshot.

Fix: `ClosedTxn` is `Clone`; create_workflow_execution clones the txn and leaves it in the snapshot
(task ids are re-allocated in `snapshot_of` per attempt).

Validation: see runs below (filled in after CI).

## Validation (head 774c8b07662e2846434c9cc3a1085d7ac7cadda3 for gates; rust-ci head 6bdeae6d8)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43093728568 success (6bdeae6d8).
- Gate concurrency group made per-run (parallel dispatches were cancelling each other).
- two_history grpc gates on 774c8b076: mysql 43095031454, 43095047769, 43095063698; cassandra 43095078439, 43095095217, 43095110281.
  Runs 43095031454, 43095047769, 43095063698, 43095095217 logged `GATE SUMMARY failed: []` (all SDK/churn steps pass); their
  conclusion is "failure" only because actions/upload-artifact hit a GitHub outage ("Failed to CreateArtifact ... <!DOCTYPE").
  Runs 43095078439 and 43095110281: GitHub log service returned truncated logs; no gate summary visible (inconclusive).
