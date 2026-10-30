# Matching startup stage diagnostics — 2026-10-30

## Fresh CI proof

Run [41037584598](https://github.com/auenkr/cadence-rust/actions/runs/41037584598)
completed successfully at head
`3a958668c040b289b495f9b76b2209969db7e2a0` on
`t3/matching-startup-stage-diagnostics`.

Downloaded the small `rust-ci-logs` artifact (178,073 bytes) and inspected its
identity and exit files. `commit.txt` matches the requested SHA; `PKG` was empty
in the Actions log, selecting `--workspace`; the fresh target step selected
`/home/runner/work/_temp/cargo-target-final`, and the Rust cache step was
skipped. `clippy.exit=0` and `test.exit=0`. Workspace test log totals across
57 result groups: 1,829 passed, 0 failed, 28 ignored, 0 measured. These are
Rust workspace tests and do not establish mixed-cluster service readiness.

## PostgreSQL diagnostic gate

The existing ref `t3/matching-startup-stage-diagnostics` was verified to point
at the same exact SHA. The production validation workflow at that commit accepts
`lane=postgres`; no other Actions run was in progress at dispatch time. Run
[41039044942](https://github.com/auenkr/cadence-rust/actions/runs/41039044942)
was dispatched with only `lane=postgres`, at the exact SHA above. Its first
status snapshot (2026-10-30 10:12:38Z) was `in_progress`, at job setup. No later
status was polled in this checkpoint. SDK, restart, and recovery readiness
assertions remain unverified until that gate reaches a terminal state and its
artifact is inspected.

## Previous failure boundary

For [41034380907](https://github.com/auenkr/cadence-rust/actions/runs/41034380907),
the available log ended while opening admission. RingpopReady is proven by the
prior evidence; no watch defect is proven. Do not infer a cause beyond the
recorded stage markers. On a terminal failed diagnostic run, inspect the
artifact's stage markers, nested exit files, and Rust binary SHA256 to locate
the exact startup boundary.

## Next

- Inspect run 41039044942 exactly once after it reaches a terminal state; if it
  is still running, leave this checkpoint pending without polling.
- On failure, use only recorded stage markers, nested exits, and binary SHA256
  to identify the last proven startup stage.
- On success, verify the SDK workflow, restart, and recovery assertions from
  gate evidence before claiming readiness; then record the remaining nine lanes
  as pending. Matching remains incomplete until the parity audit and all
  required mixed Go/Rust validation are complete.
