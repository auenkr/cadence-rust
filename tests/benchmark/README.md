# Frontend + Matching comparison

Compare only these deployments:

| Arm | Frontend | Matching | History / system Worker | Persistence |
| --- | --- | --- | --- | --- |
| go | Go | Go | Go | identical engine/config/schema |
| rust | Rust | Rust | Go | identical engine/config/schema |

The `tests/frontend-gate/benchmark` executable uses the existing Go SDK over
TChannel. Each workflow executes one echo activity and verifies its output.
This exercises Frontend requests and Matching decision/activity dispatch together.
It measures combined service performance, not isolated RPC throughput.

Build using the `Frontend Matching benchmark driver` Actions workflow, never
locally. Download its driver artifact. Register a `benchmark` domain in each
deployment using the existing CLI. With only one deployment active at a time:

```sh
./cadence-benchmark -endpoint 127.0.0.1:7933 -domain benchmark -label go \
  -requests 1000 -warmup 100 -concurrency 16 -payload-bytes 1024 -output go-1.json
./cadence-benchmark -endpoint 127.0.0.1:7933 -domain benchmark -label rust \
  -requests 1000 -warmup 100 -concurrency 16 -payload-bytes 1024 -output rust-1.json
```

Switch deployments between commands; do not point both at the same service.
Use fresh equivalent databases per arm, identical History binaries, service counts,
CPU/memory limits, transport, persistence settings and SDK driver binary. Use a
release Rust binary, not the debug binary from acceptance gates. Record source
commits, binary SHA256s, full configs, host specifications and command/exit/logs.
Prove no Go Frontend/Matching runs in the Rust arm. Correctness gate success is a
prerequisite for interpreting performance; failures must not become speedup claims.

Run at least five repetitions, alternate Go/Rust order, then repeat concurrency
1, 16, 64. Report each run and median throughput and p50/p95/p99 successful
workflow completion latency. JSON preserves successful samples and all errors;
any measured failure exits nonzero. Warmup errors abort measurement. This is a
closed-loop workload, so latency excludes client queue time and cannot describe
open-loop overload. SDK retries are included in end-to-end latency.

CPU-heavy cluster comparisons run on standard public-repository Actions runners.
Capture per-service CPU seconds and RSS alongside each measured interval before
claiming resource improvements. The driver does not yet collect those metrics or
provision either cluster. Do not compare results from dissimilar hosted runners
as if they were controlled measurements. Long low-CPU soaks of prebuilt binaries
run locally in tmux `cadence-rust`, through `work-safe`, nice/ionice and resource
guards specified by AGENTS.md.

No benchmark measurements have been recorded yet.

## Resource collection and comparison tools

Keep one result file per arm and repetition. Do not edit the SDK driver JSON;
the comparer validates the actual label, request count, warmup, concurrency,
payload size, transport and sticky setting. It rejects any failed workflow,
missing successful sample, incomplete run or mismatched Go/Rust pair. Pair
files in the same repetition order (alternate which arm runs first to reduce
time/order bias):

```sh
python3 tests/benchmark/collect_resources.py --duration 65 --interval 1 \
  --containers frontend matching history worker --output go-1.resources.json &
collector=$!
./cadence-benchmark -endpoint 127.0.0.1:7933 -domain benchmark -label go \
  -requests 1000 -warmup 100 -concurrency 16 -payload-bytes 1024 -output go-1.json
wait "$collector"
```

Estimate the sampling duration from a pilot run; use an interval that covers
the complete measured workload while excluding warmup. Repeat this alongside
each driver run and keep both files. Container names accepted by Docker may
include Compose prefixes; run `docker ps --format '{{.Names}}'` to find them.
Use the same service set in both arms. The collector samples all specified
containers at one-second intervals by default. RSS is sampled Docker memory
usage (peak across samples); CPU seconds are an estimate from trapezoidal
integration of sampled CPU percentage, so record the sampling interval and
duration and do not present it as exact cgroup accounting.

For every arm/repetition, retain a metadata JSON document with at least source
commit, Go/Rust service image or binary identifiers and SHA256s, benchmark
driver SHA256, full effective Compose/configuration, persistence engine/schema,
host/runner specifications, service replica counts, CPU/memory limits and
confirmation that the Rust arm has no Go Frontend/Matching. The comparer takes
these documents unchanged and includes them in its JSON output; it does not
invent missing provenance. Use identical Go History/system Worker versions,
persistence and service limits across arms. Performance claims require passing
correctness gates, equivalent metadata, and resource samples for both arms.

Generate the per-run-preserving machine and human reports after all repetitions:

```sh
python3 tests/benchmark/compare.py \
  --go go-1.json go-2.json go-3.json go-4.json go-5.json \
  --rust rust-1.json rust-2.json rust-3.json rust-4.json rust-5.json \
  --go-metadata go-metadata.json --rust-metadata rust-metadata.json \
  --json comparison.json --markdown comparison.md
```

JSON embeds the full original run objects and latency samples, paired per-run
values, median throughput and median per-run p50/p95/p99 latency, plus Rust
minus Go deltas. Markdown summarizes the same results. These are combined
Frontend+Matching SDK workflow measurements, never isolated service RPC
throughput. The driver is closed-loop: latency excludes client queue time and
includes SDK retry time. No benchmark numbers are checked in until they come
from real, reproducible Go and Rust deployments.
