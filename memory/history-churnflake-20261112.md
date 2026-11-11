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
