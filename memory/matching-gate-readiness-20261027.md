# Matching final acceptance gate readiness — 2026-10-27

Scope: dispatch and artifact-path review only. No gate was dispatched and no
local Cargo, Go, build, test, formatting, or runtime command was run.

## Review pins and conclusion

- Reviewed source head: `5808e205a1b58b2169084b2e46eaea219f8b7cf2`
  (`t3/matching-gate-readiness`).
- Acceptance checklist authority: `memory/matching-acceptance-status-20261027.md`.
- The ten mixed lanes are dispatchable serially on this pin. I found no wrong
  fixture routing or missing dispatch input that blocks starting the final
  sequence. Run every lane against the same final source SHA; if any source
  changes, restart the sequence at the fresh workspace run.
- All mixed workflows use `ubuntu-latest`, 90-minute job limits, per-run fresh
  target paths, cancellation concurrency, and 3-day artifacts. The Rust
  Matching executable SHA is captured in `out/binary.sha256`; the checked-out
  SHA is captured in `out/commit.txt`; gate status and logs are under
  `out/gate/`. PostgreSQL/MySQL/Cassandra wrappers remove their uniquely named
  database containers in EXIT/INT/TERM cleanup and save cleanup logs.

## Exact serial dispatch checklist

Use `gh workflow run` for each command, wait for it to finish, download/review
its artifact, and only then dispatch the next. Set `<final-sha>` to the frozen
source pin (or record the current reviewed SHA if it remains unchanged).

1. `gh workflow run rust-ci.yml --ref <branch> -f clean_target=true` (leave
   `package` empty for workspace clippy then workspace tests). Confirm the run
   checked out `<final-sha>`, used its fresh `RUNNER_TEMP` target, and both
   `clippy.exit` and `test.exit` are zero. Record run URL, SHA, conclusion.
2. `gh workflow run matching-production-validation.yml --ref <branch> -f lane=postgres`
3. `gh workflow run matching-production-validation.yml --ref <branch> -f lane=mysql-probes`
4. `gh workflow run matching-production-validation.yml --ref <branch> -f lane=thrift`
5. `gh workflow run matching-production-validation.yml --ref <branch> -f lane=configstore`
6. `gh workflow run matching-production-validation.yml --ref <branch> -f lane=shard-distributor`
7. `gh workflow run matching-mixed-gate.yml --ref <branch>` (default
   `extra_args` includes restart, failure recovery, and idle check).
8. `gh workflow run matching-cassandra-mixed-gate.yml --ref <branch>` (the
   harness `--cassandra` path forces restart and two-instance failure recovery,
   even though the workflow's default args only spell restart/idle check).
9. `gh workflow run matching-production-validation.yml --ref <branch> -f lane=adaptive`
   (default `adaptive_retry_only=false`; turn it on only if the parent has
   explicitly selected the retry-only subprobe for this acceptance run).
10. `gh workflow run matching-active-offer-gate.yml --ref <branch>`
11. `gh workflow run matching-production-tls-mixed.yml --ref <branch>`

`matching-production-validation.yml` accepts exactly `postgres`,
`mysql-probes`, `thrift`, `configstore`, `shard-distributor`, `adaptive`, and
`adaptive-go-control`; the last is all-Go diagnostic control, not an acceptance
lane. Do not dispatch it as a substitute for adaptive Rust Matching.

## Evidence review per mixed lane

Require artifact `commit.txt == <final-sha>`, successful build and `gate.exit`,
valid `binary.sha256`, and `gate/run/results.json` with
`goMatchingStarted:false`, `rustServices:["matching"]`, and Go peer services
Frontend/History/Worker. Confirm `runExitCode` and `probeExitCode` are zero,
then review actual result rows in `gate/run/probe.log` and the lane-specific
assertion evidence, rather than treating flags or a successful process start
as proof. For SDK/recovery lanes inspect the emitted Go SDK logs,
`matching-shutdown.json`, restart/drain evidence, and two-node recovery rows as
applicable. Confirm the DB container cleanup record exists and reports its
removal. `tests/collect-matching-gate-evidence.py` checks the pinned head,
binary hash, exits, topology and at least one actual scenario result; it keeps
`serviceAcceptance:false`, so it is a collector, not the acceptance decision.

Lane routing/assertions checked:

- PostgreSQL uses `run-postgres-lane.sh`, original Go schema, PostgreSQL GC,
  standby, partitions, durable restart and auto-config probes; shared flags add
  restart and two-node recovery.
- MySQL probes and active-offer use `run-mysql-lane.sh` and the production Rust
  Matching path. Active-offer additionally requires a real Rust poller and
  delivery without persisted fallback.
- Thrift sends Go service Matching calls over TChannel/Thrift and Rust
  Matching's History calls over TChannel; shared flags still exercise restart
  and recovery.
- Configstore seeds Go MySQL `cluster_config` and asserts the seeded long-poll
  expiration. It correctly avoids the mutually incompatible auto-config-hint
  flag.
- Shard-distributor selects `--real-shard-distributor` (Go shard-distributor
  over etcd), not only the in-memory interoperability fixture; harness turns on
  restart/recovery and the real fixture asserts executor TTL expiry.
- Adaptive routes to Rust Matching via production Ringpop and runs the SDK
  scaled-queue scenario. Its default is the full adaptive probe; retry-only is
  optional and is not silently substituted for the full SDK scenario.
- Cassandra wrapper and harness use Go-compatible Cassandra keyspaces; the
  harness forces restart plus two-node recovery and marks only this database
  lane `acceptanceEligible` in its runtime result.
- TLS lane uses the normal MySQL production path with verified matching gRPC
  TLS, restart and failure-recovery flags.

## Source pointers

- Workflow inputs, fresh targets, source/binary capture, lane routing and
  artifact upload: `.github/workflows/matching-production-validation.yml`,
  `.github/workflows/matching-mixed-gate.yml`,
  `.github/workflows/matching-cassandra-mixed-gate.yml`,
  `.github/workflows/matching-active-offer-gate.yml`,
  `.github/workflows/matching-production-tls-mixed.yml`.
- Final clean workspace gate: `.github/workflows/rust-ci.yml` (`clean_target`,
  cache skip, clippy before workspace test, `commit.txt`).
- Runtime option validation, topology and lane side effects:
  `tests/run-go-service-baseline.py` (argument definitions/validation,
  Cassandra and shard-distributor flag expansion, result capture, cleanup).
- DB fixture routing and removal: `tests/gates/run-mysql-lane.sh`,
  `tests/gates/run-postgres-lane.sh`, `tests/gates/run-cassandra-lane.sh`.
- SDK/probe assertions: `tests/matching-service-compat.cjs`,
  `tests/matching-probe-scenarios.cjs`.
- Artifact consistency checks: `tests/collect-matching-gate-evidence.py`.
