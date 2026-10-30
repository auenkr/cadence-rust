# Matching readiness trace: corrected current status (2026-10-30)

## Candidate and trace activation

The trace candidate is `a534baae8c921e0ec8109527c6bc3184937b0323` on
`t3/diagnose-matching-gate-failure` (`origin/t3/diagnose-matching-gate-failure`).
The opt-in trace activation through the PostgreSQL production workflow was
reviewed and verified in
[`matching-list-partitions-trace-review-a534baae.md`](matching-list-partitions-trace-review-a534baae.md),
from commit `5c95e8e93`. No extra trace input is needed for that lane.

## Corrections for future diagnosis

- Matching startup completes. The first readiness operation is the
  `ListTaskListPartitions` RPC. This is the point that times out; gRPC health is
  not the readiness check used by this production probe.
- An `admission-complete` trace line only says the admission check returned. It
  does not show that the engine or RPC completed and does not prove the handler
  is stalled in admission.
- `RingpopReady` is emitted as normal readiness state. Ring removals after the
  readiness timeout are part of process teardown; they do not establish an
  earlier spontaneous membership failure.
- The admission state is retained after failed readiness. A timeout does not
  prove the startup admission state was lost or reset.
- Gate output is written to regular files, so pipe backpressure/saturation is
  not a credible explanation for missing trace or progress output.
- The Go cold-cache timeout is an independent three-second cache-owned
  persistence timeout and is the same in Rust. Go also does not pass the
  inbound RPC deadline into its cold domain cache lookup. The missing inbound
  deadline forwarding is therefore not a Rust parity defect. See
  [`matching-list-partitions-domain-cache-deadline-20261030.md`](matching-list-partitions-domain-cache-deadline-20261030.md),
  commit `17b909a6`.

These findings supersede the speculative readings listed in the trace review.
Do not make source changes based on those false leads. The useful next result is
the already-running opt-in trace from the exact candidate below.

## Prior mixed failure artifact: run 41039044942

Run: <https://github.com/auenkr/cadence-rust/actions/runs/41039044942>

- Run head: `3a958668c040b289b495f9b76b2209969db7e2a0`
- Rust Matching binary SHA256: `4b846e47b4fa81457e5ca5df3f532bad9201ccf4d702b5bdcd6e503bbcf640f2`
- Nested `gate/run/run.exit`: `1`
- Nested `gate.exit`: `1`
- Probe result: `probeExitCode: 1`, `runExitCode: 1`,
  `acceptanceEligible: false`; readiness timed out via Go Frontend (20.001s)
  and direct primary gRPC (10.001s).
- Worker readiness marker was observed and Go services remained alive after
  probe failure. SIGTERM teardown exceeded its two-second bound and used
  SIGKILL; this is cleanup evidence after failed readiness, not proof of the
  readiness cause.

The artifact does not contain the opt-in stage trace: the run was on an earlier
startup-diagnostics head, not the trace candidate. It therefore cannot identify
the last `ListTaskListPartitions` handler stage.

## Current trace CI checkpoint

Inspected exactly once on 2026-10-30:

- Run: <https://github.com/auenkr/cadence-rust/actions/runs/41040870450>
- Workflow: `rust-ci`
- Head branch: `t3/diagnose-matching-gate-failure`
- Head SHA: `a534baae8c921e0ec8109527c6bc3184937b0323`
- Observed status: `in_progress`; conclusion is empty.

No further polling was done. Since it is not terminal, there is no clean-target
clippy/test result to verify and the PostgreSQL lane pinned to this immutable
candidate was not dispatched. On the next handoff, inspect this run once. If it
is terminal success, verify its exact-head fresh-target clippy and workspace
test artifacts, then dispatch the PostgreSQL lane for
`t3/diagnose-matching-gate-failure` at the immutable SHA above. If it is still
pending, checkpoint and stop; if it failed, report its logs without speculative
source changes.

## Scope

Documentation/evidence only. No source edits, local Cargo/Go commands,
formatters, runtime probes, or PR actions were performed for this checkpoint.
