# Lane 4 (cassandra) — matching-gate-final3-20261024

Base: origin/master bafd65a77. Binary: rust `cadence-matching` sha256 in `../binary-shas.txt`.
Command: `tests/gates/run-cassandra-lane.sh /mnt/storage/service/gate-runs/gatefinal3/l4 <rust-binary> --binary <go cadence-server> --history-transport grpc --evidence-dir <this>/evidence` under `work-safe`.
Same flags as gate-final2 lane 4; the wrapper owns the uniquely named cassandra container.

## Result: FAILED (run.exit = 1)

Failing assertion (`tests/matching-service-compat.cjs:1168`, from `run.log` / `probe.log` of attempt 3):

    AssertionError [ERR_ASSERTION]: DescribeTaskList must report the reported backing rate
    100000 !== 1

The probe polls an activity task list with `taskListMetadata.maxTasksPerSecond = 1`, then calls
`DescribeTaskList(includeTaskListStatus)`. Rust reports `taskListStatus.ratePerSecond = 100000`
(the default limiter value) instead of 1. The same assertion passes in the gate-final2 MySQL lanes
(1-mysql-single, 2-mysql-two-node, 5-thrift-mysql all exit 0), so this looks specific to the Cassandra
lane. That lane runs two Rust nodes with the owner SIGKILL and the SIGTERM drain. Not yet diagnosed.
Suspects to check: which Rust node answers `DescribeTaskList` versus which applied the poll's rate
update (`rust/crates/cadence-matching/src/matching_limiter.rs:138`), and whether the update reaches
the limiter before the describe on this topology. Not a harness typo as far as the probe reads.

Rust program logic was NOT changed.

## Attempts (all kept in this directory)

1. `attempt1-env-failed/`: probe could not start. `Cannot find module '@grpc/grpc-js'`. The worktree
   had `cadence-web` (a submodule) uninitialized. Fixed by `git submodule update --init cadence-web`
   and `npm ci --omit=dev --ignore-scripts` (environment setup, not a code change).
2. `attempt2-probe-owner-id-failed/`: harness bug. The semaphore probe built the gap owner id as
   `<len>:<id>:<uuid>:gap`. Go's owner format (`go/common/semaphore/owner.go`) needs a decimal
   hold id, so Rust correctly returned `INVALID_ARGUMENT: invalid owner id ... invalid hold id "gap"`.
   Fixed in the probe: `:gap` -> `:1` (line 548). The probe's own comment says this acquire must return
   `NO_SLOT`, which the rest of the semaphore section then confirms. This is the single probe change.
3. Attempt 3 (this run, `run.log`, `run.exit`, `evidence/`): semaphore section passes; fails at the
   paced `DescribeTaskList` assertion above.

Cassandra container was removed by the wrapper trap (`cassandra-container.log` saved in `evidence/`).
