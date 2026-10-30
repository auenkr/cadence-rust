# PostgreSQL Matching gate dispatch checkpoint — 2026-10-30

## Frozen candidate CI evidence

Candidate: `caa32882f9a3fd1da4450c8b216607c053996893` on
`origin/t3/matching-dyncfg-reader-audit-fix` at dispatch time. That source branch
later advanced to docs commit `131bdf339dde50bbeaba0312fa9d1969051fce1f`, so
this gate uses a separate immutable ref, not the docs commit.

Rust CI run: https://github.com/auenkr/cadence-rust/actions/runs/41033025800

Downloaded the small `rust-ci-logs` artifact. It records the exact candidate in
`commit.txt`, target `/home/runner/work/_temp/cargo-target-final` in
`target-dir.txt`, and `clippy.exit=0`, `test.exit=0`. `test.log` contains the
whole workspace (not a package-filtered run): 1,829 passed, 28 ignored. This
was fresh runner target evidence, with clippy completed before workspace tests.

## PostgreSQL production validation dispatch

A dedicated immutable ref `t3/postgres-gate-caa32882` points exactly to
`caa32882f9a3fd1da4450c8b216607c053996893`. Before dispatch, the
`matching-production-validation.yml` at that SHA was inspected: manual input
`lane=postgres`; standard `ubuntu-latest`; 90-minute job timeout; per-run fresh
`/tmp/cargo-target-final-${run_id}`; builds a fresh Rust Matching binary; runs
the PostgreSQL wrapper with durable restart, query-standby, partition-list,
and auto-config probes. The mixed-gate step includes Matching restart and
failure recovery. The workflow concurrency group is
`matching-production-validation` with cancel-in-progress.

No queued or in-progress Actions runs existed at dispatch. PostgreSQL gate:
https://github.com/auenkr/cadence-rust/actions/runs/41034380907

At the one allowed inspection, it was `in_progress` at checkout, head SHA
`caa32882f9a3fd1da4450c8b216607c053996893`, run ID `41034380907`. This is a
checkpoint only: no gate exit evidence or binary SHA is available yet. Do not
claim mixed PostgreSQL pass or production readiness until the terminal artifact
shows nested readiness/SDK/restart/recovery exits and the Rust binary SHA256.
The source ref deliberately excludes supplemental commit
`3a1da7b06bdbec4ddaa5858f003f4f986870540c`; therefore this run cannot validate
that supplemental queued-activity crash-recovery case.

## Supplemental activity crash-recovery review (not integrated)

Reviewed `origin/t3/matching-queued-activity-crash-recovery` at
`3a1da7b06bdbec4ddaa5858f003f4f986870540c`; left it untouched and did not move
the frozen candidate.

The scenario has useful explicit checks: exactly one durable activity row
before the owner crash; both Rust members report the same activity partition
owner; the reported owner is one of the two Rust members; SIGKILL; the survivor
reports ownership; the existing Go SDK obtains the expected activity result;
history has exactly one activity completion and one workflow completion; the
activity queue drains; then restarted membership reconverges.

Blocking finding: the new observer call passes `taskType=1`, but the
PostgreSQL-backed `sqliteTaskState` path in `tests/matching-service-compat.cjs`
still has a three-argument signature and calls `durableTaskCount(..., 0, ...)`
for both queued row count and task-list range. PostgreSQL production mode selects
the observer's `sqlite` backend. Thus the activity probe reads `task_type=0`
(decision tasks), so its exact-one queued activity assertion cannot observe the
activity row and will time out or fail. Thread `taskType` through
`sqliteTaskState` to both durableTaskCount calls before integrating this test.
The observer self-check callbacks also ignore extra args and currently do not
assert the task type forwarding, so extend that coverage when repairing it.

No source change to the supplemental commit, no local Go/Cargo/runtime test, and
no integration into the passing candidate were performed here.
