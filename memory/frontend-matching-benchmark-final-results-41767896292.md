# Verified Frontend + Matching benchmark — 2026-11-04

Run: https://github.com/auenkr/cadence-rust/actions/runs/41767896292
Conclusion: **success**, including strict CI artifact verification; independent local verifier exit **0**, valid=true, no warnings.
Source: `316c28e4a2638e55cd52603a307fb6d7ffd09bd7`.

Five alternating paired repetitions at each concurrency; 1,000 measured workflows and 100 warmups per run, one echo activity, 1,024-byte payload, existing Go SDK over TChannel, sticky execution disabled. All **30,000 measured workflows in 30 runs succeeded**, zero failures. Go History and system Worker remained constant; fresh MySQL per arm/repetition. Each service had 1 CPU / 1,400 MiB and MySQL 1.5 CPU / 1,800 MiB. Rust used the release build. Runner details and resolved Compose configs are retained with raw evidence.

| Concurrency | Go workflows/s | Rust workflows/s | Rust throughput change | Go p95 ms | Rust p95 ms |
|---:|---:|---:|---:|---:|---:|
| 1 | 12.77 | 10.78 | -15.63% | 84.94 | 99.84 |
| 16 | 33.57 | 29.82 | -11.18% | 618.96 | 709.16 |
| 64 | 31.13 | 27.55 | -11.50% | 2600.80 | 3035.88 |

Values are five-run medians; latency is the median of each run's percentile, not a pooled percentile. All three JSON comparison reports reproduced exactly with the current comparer from retained raw inputs.

## Resource interpretation

At concurrency16, median CPU time per 1,000 workflows was 4.069s Go / 5.008s Rust for Frontend and 2.687s Go / 5.045s Rust for Matching. Peak sampled cgroup memory was 39.25/29.27 MiB for Frontend and 35.66/26.80 MiB for Matching. CPU counters are exact deltas over the marker-aligned collection interval; memory is sampled every50ms, includes charged cache, is not RSS and can miss shorter peaks. Baseline/ready handshake and final CPU snapshot are included; interval envelope is checked within1s of SDK timestamps. The collector interval includes small handshake/end-detection overhead. Per-service results at all concurrency levels are in comparison-c*.md/html/json.

Rust had lower throughput and higher latency in this controlled run, used more Frontend/Matching CPU time per fixed work count, and less sampled cgroup memory. The benchmark does not identify the bottleneck: Go History CPU also changed despite its implementation being held constant. Profiling/RPC/database tracing would be needed for causal explanation.

## Reproducibility and provenance

Compact evidence: `memory/evidence/frontend-matching-benchmark-full-41767896292/`.
Full downloaded logs: `/mnt/storage/service/ci-evidence/benchmark-full-41767896292/`.
Raw results, resource samples, resolved configs, immutable identities, source IDs, process inventories, marker files, command exits, CI/local verification, hashes, and JSON/Markdown/HTML reports are retained.

Local verification command (exit0):

```sh
/mnt/storage/service/bin/work-safe nice -n 10 ionice -c3 python3 tests/benchmark/verify_artifacts.py --out memory/evidence/frontend-matching-benchmark-full-41767896292 --expected-head 316c28e4a2638e55cd52603a307fb6d7ffd09bd7
```

Go binary SHA256: `9dccaabe33f0fe7e0b7f436b2a6e12723fd6db228130524195411981a6094cd9`.
Rust binary SHA256: `84878674bb435f323fa2f0dfed9bbd9614e09de899a2a1ff3cac85e5279e9d62`.
SDK driver SHA256: `abcc90a0087d235228f693bdfc02c55d846d3b64c8485f8a88e4de26e824cf19`.
Go tree: `36b5af995abeea69cdca171d449c93c835c84871`.
Rust tree: `33b78ae30a6cdabbae4e3992032658a9f5761608`.

All Go/Cargo builds ran on CI; no Rust source changes were needed for the benchmark. The33 Python checks passed. This is a single standard hosted runner, one closed-loop workload, one persistence backend and short measured intervals. It compares the combined Frontend+Matching deployment; it does not establish separate-service speed, open-loop capacity, broader parity or a fully Rust cluster. Initial uncapped run41762497645 remains separate: changes in runner, caps and workload size prevent direct attribution of differences between runs.
