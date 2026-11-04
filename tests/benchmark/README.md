# Frontend + Matching comparison

The comparison command can also emit a self-contained HTML report with charts;
it reads the same comparison JSON and requires no external assets or network:

```sh
python3 tests/benchmark/compare.py --go go-1.json go-2.json --rust rust-1.json rust-2.json \
  --json comparison.json --markdown comparison.md --html comparison.html
```

Open `comparison.html` in a browser to inspect median throughput and latency,
paired per-run throughput variation, deployment metadata, and resource values
when Go and Rust resource samples are comparable. Missing or incomparable
resource collection is shown with its status and reason. Scope and workload
metadata come from the comparison document; the report does not synthesize
measurements.

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

For precise interval alignment, have the benchmark orchestrator create distinct
start and end marker files immediately before and after the driver's measured
phase, excluding warmup. Remove stale marker files before each run. The
collector waits for the start marker, snapshots cumulative counters, samples
until the end marker appears, then takes a final counter snapshot:

```sh
rm -f go-1.start go-1.end
python3 tests/benchmark/collect_resources.py --start-event go-1.start \
  --end-event go-1.end --interval 1 --containers frontend matching history worker \
  --output go-1.resources.json &
collector=$!
# Create go-1.start at measured-workload start and go-1.end when it completes.
./cadence-benchmark ... -output go-1.json
wait "$collector"
```

These markers are an integration contract; the standalone SDK driver does not
currently emit them. A launcher without measured-phase hooks can use the fixed
duration fallback, which is approximate. Use the same service set in both arms.
The collector reads cumulative cgroup v2 or v1 CPU counters for exact CPU
consumption deltas. Memory is peak sampled cgroup usage, includes charged cache,
and is not RSS. Keep sampling interval and duration with the evidence.

```sh
python3 tests/benchmark/collect_resources.py --duration 65 --interval 1 \
  --containers frontend matching history worker --output go-1.resources.json
```

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
  --go-resources go-1.resources.json go-2.resources.json go-3.resources.json go-4.resources.json go-5.resources.json \
  --rust-resources rust-1.resources.json rust-2.resources.json rust-3.resources.json rust-4.resources.json rust-5.resources.json \
  --json comparison.json --markdown comparison.md
```

JSON embeds the full original run objects and latency samples, paired per-run
values, median throughput and median per-run p50/p95/p99 latency, plus Rust
minus Go deltas. Resource files are optional and follow run order; resource
objects can also be embedded in each run as `resources`. Resource deltas are
reported only for complete schema version 2 samples with matching container
sets. Otherwise the report says `not_collected`, `incomplete`, or
`incomparable`. Metrics are CPU seconds, average CPU cores and peak cgroup
memory bytes. Markdown summarizes the same results. These are combined
Frontend+Matching SDK workflow measurements, never isolated service RPC
throughput. The driver is closed-loop: latency excludes client queue time and
includes SDK retry time. No benchmark numbers are checked in until they come
from real, reproducible Go and Rust deployments.
