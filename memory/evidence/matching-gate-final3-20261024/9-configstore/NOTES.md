# Lane 9 (configstore) — matching-gate-final3-20261024

Base: origin/master bafd65a77. Binaries: `../binary-shas.txt`.
Command (same flags as gate-final2 lane 9): `tests/gates/run-mysql-lane.sh <artifacts> <rust cadence-matching> --binary <go cadence-server> --matching-configstore --configstore-seed-binary <configstore-seed> --history-transport grpc --evidence-dir <this>/evidence`, under `work-safe`. The wrapper owns the MySQL container (`mysql:8.0.36`, removed after the run; `mysql-container.log` saved in `evidence/`).

## Result: FAILED (run.exit = 1)

The gate-final2 failure is gone. The seed row now decodes: `configstore-seed.log` shows
`seeded 6 entries version=1 encoding=thriftrw`, and the Rust log shows `initialising ConfigStore dynamic config client`
with no `DeserializeBatchEvents` / `unknown ttype 89` error. Master's `strip_thriftrw_preamble` fixes the 0x59 preamble bug.

The new failure is in the existing-SDK step, before the configstore longPoll check:

    tests/go-sdk/mixed_matching.go:84
    panic: workflow-task-retry: timed out waiting for the workflow to finish: context deadline exceeded

The scenarios before it (`activity-retry`, `child`, `workflow-run-retry`) completed. `activity-retry` logged the expected
`Activity error`, and `workflow-task-retry` logged the expected injected `Workflow panic ... injected workflow task failure`
twice, but that workflow never finished. The SDK log is `evidence/go-sdk.log` (the run copy is `l9/run/rust-matching/go-sdk.log`).

## Hypothesis (not verified)

The seed (`tests/run-go-service-baseline.py` ~line 1026) also writes `system.enableTasklistIsolation: true` and
`system.allIsolationGroups: ["zone-a","zone-b"]`. Only Rust reads the configstore. Go Frontend/History/Worker still use the
file config, where isolation is off. The SDK pollers send no isolation group, so Rust may route tasks to a group nobody polls.
That would hang the workflow-task retry, which needs a new decision task. The lane 1 MySQL run used the same SDK
scenarios with file-based config and passed (gate-final2 `1-mysql-single` exit 0).

To settle it: run the same lane with the isolation keys removed from the seed, or check the Rust dispatch logs for isolation-group
routing. I did not change the seed: that changes the scenario under test, and the brief was to rerun, not redesign.

Rust program logic was NOT changed. No probe changes in this lane.
