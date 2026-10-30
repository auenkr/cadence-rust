# Matching acceptance harness audit — 2026-10-30

## Scope and candidate

This is a source-level audit of the frozen candidate
`0032e7b3341275c028952daa82ba302d9ede0f02` (`t3/matching-failure-coverage-20261030142037`),
not a runtime result. The candidate is the post-Clippy-fix test-only query callback alias; the
Rust CI run `41031374784` is still `in_progress` at this checkpoint. Its head SHA is the exact
candidate SHA above. No PostgreSQL lane was dispatched because the specified prerequisite
(terminal success) has not happened. No artifacts from that pending run were reviewed.

## What the PostgreSQL lane actually wires

`.github/workflows/matching-production-validation.yml` (candidate lines 80–110) sets the
production common flags `--matching-restart --matching-failure-recovery
--matching-idle-check-seconds 3`. For `lane=postgres`, it selects
`tests/gates/run-postgres-lane.sh` and adds `--probe-pg-gc-domain --probe-query-standby
--probe-list-partitions --probe-durable-restart --probe-auto-config-hint`.
The wrapper (lines 37–40) invokes `tests/run-go-service-baseline.py --rust-services matching
--postgres --rust-production-binary <fresh-built-binary>`. That baseline computes Go services
as every supported service except `matching` and starts the Go server with
`--services frontend,history,worker` (baseline lines 362–374, 900–901). Its results record
`goMatchingStarted: false`, `rustServices`, `goServices`, and the active original Go Ringpop
PeerProvider gate (around lines 900–930). Thus the lane's intended topology has no Go Matching.

The PostgreSQL workflow lane is a meaningful mixed-cluster acceptance harness, not an adapter
smoke test:

- Existing Go SDK traffic goes through Go Frontend/History/Worker and Rust Matching. The
  normal mixed mode in `tests/go-sdk/mixed_matching.go` (roughly lines 131–270) asserts echo,
  activity retry, child workflow, workflow-run retry, workflow-task retry, activity timeout,
  signal and cancellation results. Restart recovery uses the same existing SDK in its
  `CADENCE_MIXED_RESTART_WORKFLOW_ID` mode (lines 132–150), then checks workflow completion in
  `matching-service-compat.cjs`.
- Production readiness requires both a public Go Frontend `DescribeTaskList` call and a direct
  call to the exact Rust primary endpoint (`matching-service-compat.cjs` lines 517–561). The
  two-instance recovery branch joins a secondary, compares a 64-key ownership map from both Rust
  members (around lines 643–790), kills the selected owner with SIGKILL, waits for Go Frontend
  to reach the survivor, completes the recovered workflow with the Go SDK, and checks one
  completion, non-regressing task ID range, advancing ack level, increased durable range, and
  zero queued rows (around lines 900–975). It restarts the failed member and checks discovery
  again (around lines 976–989).
- `--matching-restart` creates durable decision work, observes queued task rows and a persisted
  watermark, SIGKILLs primary Matching, starts it again while Go services remain alive, verifies
  readiness/discovery, then resumes the work with the Go SDK and asserts exactly one completion
  (`matching-service-compat.cjs` around lines 1148–1205). The PostgreSQL `--probe-durable-restart`
  flag is translated to `CADENCE_PROBE_DURABLE_RESTART` by the baseline (around lines 1245–1265)
  and runs multi-partition activity backlog recovery in `matching-probe-scenarios.cjs` around
  lines 386–417.
- Production mode always exercises graceful shutdown with a queued durable decision: it sends
  SIGTERM to the actual owning Rust member, requires exit code 0 within 60 seconds, ensures the
  Go services remain alive, restores membership, has the existing SDK consume the preserved task,
  and checks one workflow completion and an empty durable queue (`matching-service-compat.cjs`
  around lines 1259–1327). The final cleanup separately records `matching-shutdown.json` and
  requires orderly primary/secondary exit when the probe body succeeded (around lines
  1770–1810).
- PostgreSQL-specific probes verify PostgreSQL task persistence through Go History and inspect
  pending task rows (`matching-service-compat.cjs` around lines 1726–1748); the workflow's
  `--probe-pg-gc-domain`, query-standby, partition-listing and auto-config flags activate their
  corresponding scenarios.

## Evidence recording and its limits

The workflow records the candidate SHA in `out/commit.txt`, the fresh Matching build result in
`out/build.exit`, the Rust executable SHA256 in `out/binary.sha256`, the wrapper command and
combined log, and the wrapper result in `out/gate.exit` (`matching-production-validation.yml`
lines 49–57 and 105–110). `run-postgres-lane.sh` records `out/gate/lane.exit`; the Python
baseline records `out/gate/run/run.exit`, `results.json` including `probeExitCode`, and the
probe's JSON/log outputs. `upload-artifact` retains `out/` for three days while excluding
generated executables (workflow lines 112–121). These layers let a reviewer verify nested exits
and that the binary hash belongs to the candidate.

These are code paths and expected artifacts, not passing evidence. At this audit checkpoint,
there is no PostgreSQL lane URL, lane conclusion, runtime SHA, or recorded probe assertion result.
The CI status read was exactly:

`https://github.com/auenkr/cadence-rust/actions/runs/41031374784` — head
`0032e7b3341275c028952daa82ba302d9ede0f02`, status `in_progress`, conclusion empty.

## Precise coverage boundaries / remaining checks

1. The PostgreSQL lane is not the broad Go SDK test suite. It runs the purpose-built mixed SDK
   compatibility driver listed above; that is substantive representative workflow coverage,
   but does not claim every Cadence SDK behavior.
2. The PG lane's process-crash recovery assertion targets a queued decision task. The separate
   multi-partition durable-restart probe covers activity backlog across restart, but the lane
   does not kill a Matching owner specifically while an activity task is queued and then prove
   that activity is recovered through the survivor.
3. This lane exercises the original Go Ringpop discovery path. Shard-distributor discovery,
   heartbeat expiry and reassignment belong to the separate `shard-distributor` lane; their
   checks do not run in `lane=postgres`.
4. The SIGTERM scenario proves a persisted decision survives orderly shutdown and subsequent
   SDK completion, while the multi-partition activity scenario proves hard-restart activity
   backlog recovery. It does not assert a SIGTERM drain of a queued activity task specifically.
5. The pending Rust CI run provides no test evidence. If it succeeds, its fresh workspace
   artifacts still need review, then the PostgreSQL lane must be dispatched at this exact SHA
   using an immutable gate branch and its run URL, head SHA, result, nested exits, and binary
   SHA recorded. If it fails, fix only the reported minimum and re-establish the candidate before
   dispatching runtime validation.

No source or test code was changed for this audit.
