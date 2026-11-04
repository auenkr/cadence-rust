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
