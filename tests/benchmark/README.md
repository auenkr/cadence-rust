# Frontend + Matching comparison

Compare only the following deployments:

| Arm | Frontend | Matching | History / system Worker | Persistence |
| --- | --- | --- | --- | --- |
| Go | Go | Go | Go | MySQL 8.0 |
| Rust | Rust | Rust | Go | MySQL 8.0 |

`tests/frontend-gate/benchmark` uses the existing Go SDK over TChannel. It
starts a worker, runs an echo activity, and verifies the result. This measures
combined Frontend and Matching workflow performance, not isolated RPC
throughput.

## Reproduce with GitHub Actions

Run **Frontend + Matching paired benchmark** with `workflow_dispatch` on
`benchmark-frontend-matching`. The bounded defaults are 5 repetitions, 100
measured workflows and 10 warmups per arm and concurrency, 1 KiB payloads, and
concurrency levels 1, 16, and 64. The run builds the Go server image, builds
the Rust image with Cargo's release profile, and builds one Go SDK driver used
for both arms. It requires no local Cargo or Go build.

Each repetition alternates which arm runs first. Each arm gets a new MySQL
container and schema; the complete compose project is removed before the next
arm starts. History and system Worker remain Go in both arms. The Rust arm
asserts that both Frontend and Matching use `cadence-rust:ci`. Services share
the same compose configuration, runner, persistence engine, and transport.

Download the `frontend-matching-paired-benchmark` artifact. It contains each
driver JSON result, the full resolved compose config for each arm, image IDs,
Go and Rust server binary SHA256s, source tree IDs, runner details, workload
parameters, service logs, command/exit records, and per-service resource
samples. A nonzero workflow result or any workload error invalidates that
comparison; do not report a speedup from a failed run. The workflow run URL,
head SHA, and conclusion should be recorded with any published result.

The workload is closed-loop: each client slot starts another workflow after
the prior one finishes. Latency excludes client queue time and cannot describe
open-loop overload. SDK retries are included. Per-service CPU seconds in the
resource artifact are an estimate integrated from sampled Docker CPU
percentages; RSS is the maximum sampled working set. These samples are useful
for diagnosis, but they are not precise enough for strong resource improvement
claims. Do not compare results from different runner classes as controlled
measurements. This workflow is an initial reproducible comparison harness,
not evidence that either implementation is faster; no measurements are
recorded until a successful run completes.
