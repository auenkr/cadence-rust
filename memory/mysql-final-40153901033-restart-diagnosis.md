# MySQL mixed candidate 40153901033 restart failure

Candidate under diagnosis: `27d3ff5f60f0604553824420da0cf61026eda468`.

## Finding

The failure is a harness readiness race, not evidence of a Matching persistence/recovery defect. In `tests/matching-service-compat.cjs`, `waitForRustReady` considered the replacement ready as soon as Go Frontend `DescribeTaskList` succeeded. The mixed run has a secondary Rust Matching node, so Frontend discovery can succeed through that node while the restarted primary has not yet bound its own gRPC listener. The durable restart scenario then calls the primary-only client at `127.0.0.1:17595` and receives ECONNREFUSED.

The archived ZIP `/mnt/storage/service/ci-evidence/mysql-final-40153901033.zip` supports this: `gate/run/rust-matching/rust-server-after-durable-restart.log` contains startup/config messages but no `Cadence internal matching gRPC listening` line; `gate/run/probe.log` records ECONNREFUSED at `durableRestart` line 227. The probe's `finally` sends SIGTERM to the current primary, and its shutdown check replaces the original RPC failure with the SIGTERM exit error. The artifact's Python gate log then shows the Node probe hit its 1200-second timeout. Available logs do not establish exactly which remaining Node handle caused that long process lifetime.

## Fix

`waitForRustReady` now requires both Go Frontend discovery and a successful `ListTaskListPartitions` RPC to the restarted primary's exact gRPC endpoint before returning. The partition/backlog/recovery assertions are unchanged. This also covers the earlier restart callers that use the same helper.

## Validation and handoff

- `node --check tests/matching-service-compat.cjs`: passed.
- `git diff --check`: passed.
- No Cargo, Go, or mixed-cluster command was run locally. The parent owns serialized mixed lanes; rerun candidate 40153901033 with this harness fix when scheduled.
- This diagnosis does not establish Matching service parity or acceptance; the candidate's mixed MySQL run remains failed pending that rerun.
