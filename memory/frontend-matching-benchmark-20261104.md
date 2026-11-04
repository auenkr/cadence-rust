# Frontend + Matching benchmark

User requested a Go versus Rust benchmark, scoped specifically to Frontend and
Matching. Keep History and system Worker in Go in both arms. No migration work
or service completion claim is part of this benchmark.

Branch: `benchmark-frontend-matching`.
Driver and protocol: `tests/frontend-gate/benchmark/main.go` and
`tests/benchmark/README.md`. Existing Go SDK v1.4.0-rc.3, TChannel, closed-loop
concurrent workflows, one echo activity, payload verification, warmup, raw
successful latency samples, p50/p95/p99, throughput and explicit failures.

CI source: `d9b4b1df3` (resolve full SHA using git). Validation run:
https://github.com/auenkr/cadence-rust/actions/runs/41761570862
Terminal conclusion: success. Workflow formats Go on CI, saves
the patch, vets and builds the driver, records driver SHA256 and source head.
No Cargo or Go build/test/format commands ran locally.

Initial workflow_dispatch returned HTTP 404 because the new workflow is absent
from the default branch. Added a narrowly scoped branch push trigger instead.

Remaining: collect CI result and formatting patch; integrate the workload into
the existing Docker drop-in cluster launcher for serial Go and Rust arms with
fresh equivalent persistence; build optimized Rust on CI; collect per-service
CPU/RSS over measured intervals; repeat alternating arms with the same driver
and infrastructure; retain exact source and binary hashes plus raw results.
The SDK driver alone neither provisions clusters nor collects process metrics.
No performance results or speedup claims exist yet. Runtime service acceptance
remains a separate prerequisite.

## Acceptance checklist

- Run the same existing-SDK workload binary against Go Frontend + Matching and
  Rust Frontend + Matching, holding Go History/system Worker, persistence,
  transport, service count and resource limits constant.
- Use optimized Rust, fresh equivalent database state, alternating arm order
  and repeated measurements. Preserve per-run samples and distinguish a small
  smoke measurement from a full concurrency/repetition study.
- Record every source SHA, binary/image hash, runner/host details, command,
  configuration, exit code and log. Check that no Go Frontend/Matching instance
  exists in the Rust arm.
- Require verified workflow/activity results and zero benchmark errors in both
  arms before making performance claims. Failed runs stay visible in evidence.
- Capture per-service CPU seconds and memory during the measured interval;
  retain raw SDK latency samples, errors and successful throughput.
- Report combined Frontend/Matching end-to-end workflow performance. Do not
  imply the benchmark isolates either service or establishes a fully Rust
  Cadence cluster.
- Verify the report can be reproduced from retained machine-readable inputs,
  with workload mismatches and incomplete/failed pairs rejected.

SDK driver compilation/vet passed in CI run 41761570862 at full source
`d9b4b1df3b4f6b6c20ec3c959933de499e0900a2`. This validates only the
driver preflight, not runtime throughput or service resource consumption.
Two user-requested gpt-6-luna medium-reasoning/default-tier workers implement
paired deployment and metrics/reporting separately; integration remains open.
