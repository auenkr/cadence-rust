# Matching raw RunID SQL writer Actions oracle — 2026-10-25

## Implementation and reproducible invocation

Added a manual-only workflow at `.github/workflows/matching-raw-runid-oracle.yml`. It runs on `ubuntu-latest`, has a 90-minute job timeout, cancels superseded runs, and runs only `./service/matching/tasklist` with the overlay oracle test selected. `format_oracle_files` defaults to false; when true, Actions runs `gofmt -w` on both oracle Go files and uploads their `git diff --binary` patch. Formatting and oracle output, their exit codes, exact checked-out commit, and overlay JSON are uploaded even when either check fails. A final gate fails the workflow after evidence upload if formatting or the oracle fails. Run it with:

```sh
gh workflow run matching-raw-runid-oracle.yml --ref <branch>
```

For the repair run use `gh workflow run matching-raw-runid-oracle.yml --ref matching-raw-runid-ci-repair -f format_oracle_files=true`. The formatting patch is then to be applied as a mechanical commit and verified with a fresh dispatch using the default false input. Formatting and Go tests are run only by Actions.

The overlay maps an oracle-only helper into the SQL persistence package and an oracle-only test into the matching tasklist package. No production Go files are changed. The helper calls private `newTaskPersistence` with a real `serialization.Parser`; the test constructs the real private `taskWriter`, starts `taskWriterLoop`, and calls its synchronous `appendTask`. Its `TaskManager` adapter delegates `CreateTasks` to the real SQL task store. A `sqlplugin.DB` embedding fake disables TTL, reports one shard, and returns a sentinel error from `BeginTx`.

The test launches its own Go test binary as a subprocess for malformed, empty, and valid RunIDs. Malformed input must terminate the subprocess with a panic stack containing the production SQL task store, `MustParseUUID`, and task writer frames, and without an `ORACLE_APPEND_ACK` line. Empty and valid inputs must return the sentinel SQL transaction-boundary error through the writer's normal append response. Child subprocesses have a 20-second deadline; the Go test itself has a 2-minute timeout.

## Evidence and scope limits

Oracle source implementation commit: `2f58dd50f0abfdd17f56e6830055d6d16ffdbd15`. CI repair branch: `matching-raw-runid-ci-repair`, pushed to `origin`. Initial dispatch `40219519005` at head `14b91076856d7c81b4e9aeae79e657ef8f8071ef` was superseded by the latest revision. The current format-enabled dispatch is run `40219571304`, head `985fc26d052cbbecd22482438e10f14cfbdc38da`, URL `https://github.com/auenkr/cadence-rust/actions/runs/40219571304`; it was `pending` when last checked. Do not report a Go test result until its evidence artifact is inspected. Once this run finishes, the next stage is to retrieve and inspect its format patch and test evidence, commit the Actions-produced formatting mechanically, then dispatch with the default false input and inspect that evidence too.

This oracle exercises the real Go task writer -> `TaskManager` adapter -> `taskListDB` -> SQL task store path. The transaction boundary is a mock `sqlplugin.DB` whose `BeginTx` returns a sentinel error; it is not a real SQL transaction. It proves the SQL serializer panic and the writer's lack of a normal append acknowledgement under that controlled boundary. It does not exercise transaction commit/rollback, a Matching RPC client observing process death, other SQL drivers, Cassandra UUID parsing, sync-match bypass, or Rust behavior. It does not claim whole-service parity or mixed-cluster validation. The embedded `sqlplugin.DB` has otherwise-unused methods promoted from its nil interface and is intentionally safe only for the exercised `SupportsTTL`, shard-count, and `BeginTx` calls.
