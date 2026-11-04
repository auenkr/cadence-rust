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

## Integrated checkpoint

Integration branch: `benchmark-frontend-matching-integration`.
Paired runner and workflow integrated at
`38fa2af71007f5e13ac84f0927735c3544977932`.

- Updated SDK marker driver passed CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41763602522 at
  `d79a33e7c8f63a51316c021ef31308dc1d3c8b31` (driver-only preflight).
- Integrated smoke comparison (100 requests, 10 warmups, concurrency 16,
  one repetition) is running at exact source `38fa2af71007f5e13ac84f0927735c3544977932`:
  https://github.com/auenkr/cadence-rust/actions/runs/41764102198 .
  Last observed status: in progress, benchmark Python checks passed and image
  builds underway. No runtime comparison accepted yet.
- Earlier orchestration worker run
  https://github.com/auenkr/cadence-rust/actions/runs/41762497645 remains in
  progress. Its draft estimated Docker CPU and mislabeled memory; its resource
  outputs cannot establish the corrected benchmark's resource claims.
- Integration fixes cgroup controller parsing (hierarchy IDs are not controller
  names), captures final counters, adds SDK/collector baseline readiness and
  bounded event waits, applies identical service CPU/memory limits, preserves
  timeout command exits and produces one validated report per concurrency.
- Independent audit found stale marker reuse on runner reruns. Cleanup now
  removes old start/end/ready/result/resource files before collector startup;
  cleanup failures propagate. This correction is newer than smoke source 38fa.
- All 19 lightweight Python tests passed locally through
  `/mnt/storage/service/bin/work-safe nice -n 10 ionice -c3 python3 -m unittest discover -s tests/benchmark -p 'test_*.py'`.
  Log and exit evidence live under `memory/evidence/frontend-matching-benchmark-integration/`.
  No local Cargo/Go command ran.

Next: collect smoke terminal artifacts, resolve any runtime problems, dispatch
the corrected-source repeated comparison, inspect every pair's correctness,
identity/config/resource windows and cleanup, then retain and present real
results. No speedup or service resource improvement is claimed yet.

## First measured workflow comparison retained

Run 41762497645 finished successfully at source
`2e7e5f56d65ccdc2c23afa070c9b6486e4ee73fb`. All 30 SDK runs passed
(5 pairs × 3 concurrency levels × 2 arms; 100 measured workflows each).
The current comparer independently validated its raw workflow outputs and
reproduced medians. Go/Rust workflows per second were 25.19/17.72 at concurrency
1, 67.99/50.72 at 16 and 62.68/51.71 at 64. These are descriptive measurements
on that runner, with identical uncapped service defaults in both arms.
Older estimated CPU and incorrectly labeled memory samples are excluded from
the reproduced reports (`resources.status=not_collected`). New explicitly
capped runs and corrected resources must be reported separately.

Full note: `memory/frontend-matching-benchmark-initial-results-41762497645.md`.
Compact raw outputs/configs/hashes/command evidence and reproduced reports:
`memory/evidence/frontend-matching-benchmark-initial-41762497645/`.
Corrected integrated smoke 41764102198 is still building the release Rust image
as last observed; no corrected resource result is accepted yet.

## Resumed checkpoint — 2026-11-04 03:21 UTC

Integration head before this checkpoint: `8f74d83c8`. CI smoke
41764102198 remains in progress: Python validation and Go image passed;
release Rust image is building. No corrected resource measurements accepted.
The existing artifact-verifier delegate is pending completion; do not poll it.
Host load 6.99 exceeds the <=6 start guard, so no new worker or local heavy
work was started. Next: inspect terminal smoke artifacts, integrate the verifier,
then dispatch the repeated capped comparison from the final source head.

## Artifact verifier integrated

Worker commit `5963c15c9` integrated as `154669d0a`. All 33 benchmark
Python tests passed through work-safe (log/exit in the integration evidence
directory). The paired workflow now requires artifact verification against
GITHUB_SHA before acceptance. Smoke run 41764102198 has completed its Rust
release-image build and is building the CLI/SDK driver as last observed.
The smoke source predates the final identity and MySQL inspection hardening,
so it cannot satisfy the new full artifact verifier; the final repeated run
must use the newly integrated workflow. No local Go/Cargo commands were run.

## Corrected runtime smoke accepted — 41764102198

https://github.com/auenkr/cadence-rust/actions/runs/41764102198 succeeded
at source `38fa2af71007f5e13ac84f0927735c3544977932`. Both arms completed
100/100 workflows with zero failures (concurrency16, warmup10, payload1024).
Go/Rust resource collectors captured 41/52 samples with exact CPU counter
deltas consistent with recorded CPU seconds; run/collector/teardown/cleanup
commands all exited zero. Compact raw evidence is in
`memory/evidence/frontend-matching-benchmark-smoke-41764102198/`.
This smoke predates collector ended_at_utc and final deployment identity
evidence; it establishes runtime viability, not full evidence acceptance.
CI-generated driver formatting was applied locally as a patch without
running Go. Dispatch the full comparison from the resulting final head.

## Full corrected comparison dispatched

Run https://github.com/auenkr/cadence-rust/actions/runs/41767896292
from exact head `316c28e4a2638e55cd52603a307fb6d7ffd09bd7`.
Settings: 1000 measured workflows, 100 warmup, five alternating repetitions,
concurrency 1/16/64, payload1024. Standard public ubuntu-latest, 90-minute
job limit, explicit resource caps, strict artifact verification. Status at
dispatch: in progress. No final performance/resource claims accepted yet.

## Full run checkpoint — 2026-11-04 04:15 UTC

Run 41767896292 remains in progress. Benchmark Python checks, Go image,
release Rust image and SDK/CLI builds all passed. Paired workload started
at 03:51:10 UTC; strict evidence verification has not run yet. Leave this
run active; no restart or second benchmark dispatched. Final measurements
remain unaccepted until terminal success and artifact inspection.

## Final corrected evidence accepted

Run41767896292 succeeded, strict CI/local verification passed with no
warnings, all30,000 measured workflows succeeded. Reports reproduced
exactly. Final results and limitations: [final report](frontend-matching-benchmark-final-results-41767896292.md).
Compact raw evidence and standalone charts saved under
`memory/evidence/frontend-matching-benchmark-full-41767896292/`.
The earlier uncapped results remain separate; benchmarking objective is
fulfilled without making any service parity or causal profiling claim.
