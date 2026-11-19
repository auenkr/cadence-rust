# eff-fe-metrics-alloc (2026-11-19)

Branch eff-fe-metrics-alloc, code commit 618f35ad7 (base master 75aae40bd).

## Changes (behaviour/metric-neutral)
- `wrappers/metrics_sink.rs`: per-domain tag map built once per Metrics call; per-tasklist tags = clone of it + `tasklist`
  (was: full rebuild + second scan). Same keys/values; MetricEvent API unchanged (owned map still required per event,
  so per-call maps remain; caching across calls would need a service-core API change — not done).
- `handler_workflow.rs` dispatch_with_metadata: redirector reads `&request`; removed the extra whole-request clone
  (continuation still clones, needed for DomainNotActive retry).
- `redirection_runtime.rs` domain_id_from_token: deserialize EncodedToken from `&Value` (no whole-request clone).

## Validation
- rust-ci cadence-frontend: success https://github.com/auenkr/cadence-rust/actions/runs/44475685570
- frontend-mixed-gate: success, failedSteps [] (307 steps), binary sha256 1139ec18d1001d2eb7edc5bf7658a2378d8aeb43d313be5e6f6210b431b52e47
  https://github.com/auenkr/cadence-rust/actions/runs/44475690463

## Benchmark (Frontend, 25/s, mysql_tmpfs=true, dispatched back to back)
before https://github.com/auenkr/cadence-rust/actions/runs/44475695330 (source 75aae40bd)
after  https://github.com/auenkr/cadence-rust/actions/runs/44475700036 (source 618f35ad7)
Go arm p99 < 300ms in both (valid runners).

| run | arm | FE CPU cores | peak RSS (sum) | peak cgroup mem | start p50/p99 ms | completion p50/p99 ms |
|---|---|---|---|---|---|---|
| before | go | 0.170 | 103.0 MiB | 34.6 MiB | 7.19/16.27 | 61.2/122.4 |
| before | rust | 0.117 | 76.6 MiB | 34.3 MiB | 6.74/17.35 | 56.2/108.4 |
| after | go | 0.129 | 103.2 MiB | 35.4 MiB | 3.24/9.14 | 32.8/111.5 |
| after | rust | 0.067 | 70.4 MiB | 28.3 MiB | 2.92/8.76 | 31.1/96.8 |

Rust/Go CPU ratio: before 0.69, after 0.52 (runner noise visible: Go arm itself moved 0.170->0.129).
Verdict: gate-green, no regression; modest CPU improvement (single pair, within noise band — direction positive).
