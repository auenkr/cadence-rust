# Matching raw RunID SQL writer Actions oracle — 2026-10-25

## Implementation and reproducible invocation

Added a manual-only workflow at `.github/workflows/matching-raw-runid-oracle.yml`. It runs on `ubuntu-latest`, has a 90-minute job timeout, cancels superseded runs, checks Go formatting on the Actions runner, and runs only `./service/matching/tasklist` with the overlay oracle test selected. Run it with:

```sh
gh workflow run matching-raw-runid-oracle.yml --ref <branch>
```

The overlay maps an oracle-only helper into the SQL persistence package and an oracle-only test into the matching tasklist package. No production Go files are changed. The helper calls private `newTaskPersistence` with a real `serialization.Parser`; the test constructs the real private `taskWriter`, starts `taskWriterLoop`, and calls its synchronous `appendTask`. Its `TaskManager` adapter delegates `CreateTasks` to the real SQL task store. A `sqlplugin.DB` embedding fake disables TTL, reports one shard, and returns a sentinel error from `BeginTx`.

The test launches its own Go test binary as a subprocess for malformed, empty, and valid RunIDs. Malformed input must terminate the subprocess with a panic stack containing the production SQL task store, `MustParseUUID`, and task writer frames, and without an `ORACLE_APPEND_ACK` line. Empty and valid inputs must return the sentinel SQL transaction-boundary error through the writer's normal append response. Child subprocesses have a 20-second deadline; the Go test itself has a 2-minute timeout.

## Evidence and scope limits

Implementation commit: `2f58dd50f0abfdd17f56e6830055d6d16ffdbd15` (branch `t3/matching-actions-sql-oracle`, pushed to `origin`). Dispatch was attempted with `gh workflow run matching-raw-runid-oracle.yml --ref t3/matching-actions-sql-oracle`. GitHub rejected discovery because the new workflow is not present on the default branch: `HTTP 404: workflow matching-raw-runid-oracle.yml not found on the default branch`. `gh workflow list --all` confirms the workflow is not registered. No run ID, URL, or result exists yet. GitHub requires a `workflow_dispatch` workflow to be available from the default branch; this task explicitly forbids merging to master, so the oracle remains committed and pushed with Actions execution pending default-branch availability. Do not report an observed result before the workflow completes.

This oracle proves the SQL serializer panic and the writer's lack of a normal append acknowledgement under a controlled SQL transaction failure boundary. It does not exercise a real SQL database, transaction commit/rollback, a Matching RPC client observing process death, other SQL drivers, Cassandra UUID parsing, sync-match bypass, or Rust behavior. It does not claim whole-service parity or mixed-cluster validation. The embedded `sqlplugin.DB` has all otherwise-unused methods promoted from its nil interface and is intentionally safe only for the exercised `SupportsTTL`, shard-count, and `BeginTx` calls.
