# Two-Go-Matching adaptive control: bounded harness limitation (2026-10-27)

## Decision

Do not add an opt-in two-Go-Matching mode in this subtask. The current
diagnostic harness does not support launching, configuring, observing, and
cleaning up two independent Go Matching processes. Extending it safely would
require an orchestration change, rather than a probe-only option, and this
task explicitly excludes a broad rewrite. No production Go/Rust code or
adaptive scenario assertions were changed. No gate was run.

## Source-backed limitation

- `tests/run-go-service-baseline.py:837-838` launches one Go server process
  with the selected service list. In the all-Go adaptive-control path that
  list includes Matching.
- `tests/run-go-service-baseline.py:547-582` allocates one Matching TChannel
  port and one Matching gRPC port. It has no secondary Go Matching listener
  ports; the `RUST_MATCHING_*` secondary ports are only allocated for the
  Rust production failure-recovery path.
- `tests/run-go-service-baseline.py:592` rewrites the development Ringpop
  bootstrap list with the selected frontend/history/worker endpoints for the
  Go Matching production lane. There is no split Go topology/config generation
  for a frontend/history/worker process plus two Matching processes.
- `tests/run-go-service-baseline.py:1003-1004` owns one server child. Its
  `finally` at `1244-1250` terminates and waits for only that child.
- `tests/matching-adaptive-go-control.cjs:18-24` accepts one Matching gRPC
  endpoint, and `:56-62` supplies one direct client to the unchanged adaptive
  scenarios. The existing probe can route among clients, but the harness has
  no second Go endpoint or independent Go process to give it.

Therefore the prior successful Go control establishes the adaptive scenario
under one Go Matching process only. It cannot determine whether the two-instance
Rust diagnostic discrepancy is explained by topology.

## Existing one-instance control evidence

- Actions run: <https://github.com/auenkr/cadence-rust/actions/runs/40473213335>
- Conclusion: success; source/head `fc359c05e16b2abff94b757c7d301986e6b93572`.
- Go server binary SHA256: `53080040b6c7b37998c541ce763107c42e8bb78c33a591528fbc7dceb94aa222`
- Retained artifacts: `/mnt/storage/service/ci-evidence/go-control-40473213335/matching-production-adaptive-go-control/`
- `results.json` says `acceptanceEligible:false`, `goMatchingStarted:true`,
  and lists all four Go services. The adaptive scenario completed its existing
  24-workflow and 576-route assertions. The sole route candidate was
  `127.0.0.1:14110`.
- Full scenario and comparison details: `memory/matching-adaptive-control-40473213335.md`.

This note is a source review, not a new run or new validation evidence.

## Minimal follow-up brief

Add an opt-in `--adaptive-go-control-two-matching` (or equivalent) mode while
keeping the current one-instance mode as default and preserving
`acceptanceEligible:false`. Reuse the exact adaptive scenario and assertions.
The implementation should:

1. Start Go Frontend, History, and Worker without a Go Matching service.
2. Start two independent original-Go Matching server processes with separate
   TChannel, gRPC, and pprof listener ports, shared test persistence and
   dynamic configuration, and a Ringpop bootstrap/ownership topology that
   includes both Matching instances and the other Go services.
3. Wait for both Matching processes and their Ringpop membership/readiness
   before provisioning the scenario. Pass both matching endpoints to the
   existing scenario's `directClients` and `matchingCandidates` inputs without
   changing its budgets or assertions.
4. Record source commit, Go binary SHA256, all process commands, both matching
   endpoints, observed membership/owners, and per-process logs in the existing
   evidence format.
5. Terminate and reap every child on success, failure, timeout, and SIGTERM.
   Keep the default one-instance lane behavior and its evidence schema
   compatible.

The next run must be dispatched by the parent on the existing public Ubuntu
Actions workflow; this subtask did not dispatch a gate.
