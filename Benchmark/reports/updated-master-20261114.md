# Updated Cadence SDK benchmark — 2026-11-14

Source: `3c2bf3aaabf160518afde04f75097d2dc2c374a6` (origin/master merged into the benchmark branch).
One activity, existing pinned Go SDK over TChannel, 1 KiB payload, controlled 10ms activity, 128 worker capacity, non-sticky. 10s warmup / 30s measurement, three alternating Go/Rust pairs, fresh MySQL per arm. All-Rust retains the Go system Worker.
Standard ubuntu-latest runners; services 1 CPU/1400MiB each, MySQL 1.5 CPU/1800MiB. SDK worker CPU1, generator CPU0, services/database CPUs2–3. Schema, DB version, shard count and generated configuration derive from the same pinned Go tree and Compose files in every arm.

## CI runs

- **frontend**: https://github.com/auenkr/cadence-rust/actions/runs/43609203848 — success; harness `fd6825302eacc03aaed8f490fc632388d0f645dc`.
- **all**: https://github.com/auenkr/cadence-rust/actions/runs/43609203955 — success; harness `fd6825302eacc03aaed8f490fc632388d0f645dc`.
- **matching**: https://github.com/auenkr/cadence-rust/actions/runs/43609204077 — failure; harness `fd6825302eacc03aaed8f490fc632388d0f645dc`.
- **history**: https://github.com/auenkr/cadence-rust/actions/runs/43609203863 — success; harness `fd6825302eacc03aaed8f490fc632388d0f645dc`.

## Fixed 5 starts/sec

Medians of per-run values, not pooled percentiles. CPU is the sum for Frontend+Matching+History, excluding the Go Worker and MySQL. Memory/RSS are sums of per-container sampled peaks, not simultaneous cluster peaks; summed process RSS can count shared pages repeatedly. Collection includes drain and History audits. All-Go below uses the All-Rust lane baseline; other paired baselines follow.

| Configuration | completions/s | p50 ms | p95 ms | p99 ms | core CPU | core peak MiB | core RSS MiB | correct | errors | valid repetitions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| All Go | 5.00 | 57.96 | 67.98 | 73.13 | 0.22 | 106.91 | 313.93 | 450 | 0 | 3/3 correct |
| Rust Matching only | 5.00 | 49.26 | 56.58 | 59.85 | 0.16 | 98.19 | 276.68 | 450 | 0 | 3/3 correct |
| Rust Frontend only | 5.00 | 62.51 | 72.45 | 78.07 | 0.21 | 97.05 | 272.30 | 450 | 0 | 3/3 correct |
| Rust History only | 5.00 | 122.83 | 654.13 | 931.88 | 0.15 | 90.49 | 267.91 | 450 | 0 | 3/3 correct |
| All Rust (Go Worker) | 5.00 | 72.34 | 75.79 | 78.45 | 0.18 | 67.13 | 188.70 | 450 | 0 | 3/3 correct |

## Paired baseline and per-service resources

| Lane / arm | completions/s | FE CPU / MiB / RSS MiB | Matching CPU / MiB / RSS MiB | History CPU / MiB / RSS MiB |
|---|---:|---|---|---|
| matching / go | 5.00 | 0.04 / 33.25 / 101.13 | 0.02 / 31.35 / 95.74 | 0.10 / 41.64 / 113.09 |
| matching / rust | 5.00 | 0.04 / 33.83 / 101.77 | 0.02 / 21.21 / 56.00 | 0.10 / 42.82 / 118.26 |
| history / go | 5.00 | 0.05 / 33.69 / 101.43 | 0.02 / 31.37 / 95.91 | 0.11 / 42.56 / 116.35 |
| history / rust | 5.00 | 0.05 / 33.66 / 101.19 | 0.02 / 30.68 / 95.52 | 0.08 / 25.22 / 70.38 |
| all / go | 5.00 | 0.06 / 33.79 / 101.84 | 0.03 / 31.07 / 95.43 | 0.13 / 42.05 / 116.84 |
| all / rust | 5.00 | 0.06 / 19.20 / 60.01 | 0.03 / 22.62 / 58.14 | 0.09 / 25.29 / 71.20 |
| frontend / go | 5.00 | 0.05 / 33.61 / 101.56 | 0.03 / 31.53 / 95.65 | 0.14 / 41.91 / 110.66 |
| frontend / rust | 5.00 | 0.05 / 21.42 / 61.68 | 0.03 / 34.10 / 99.05 | 0.14 / 43.09 / 112.55 |

## Capacity and recovery

New bounded smoke tests; no equivalent capacity/recovery measurements existed in the previous SDK smoke or run 41767896292. Capacity steps 5/25/100 starts/s require all three repetitions, zero workflow errors, p99 completion <=1000ms, valid open-loop generation and <=max(2,1% arrivals) outstanding at arrival end. Highest passing tested step is a short-run bound, not proven sustainable capacity.
Recovery blocks MySQL TCP input for 3s, 10s after collector readiness while arrivals continue. Recovery is the end of the first full healthy five-second arrival window after restore (all correct, completion <=1s). Missing recovery windows are censored, not success.

| Lane / arm | highest passing tested starts/s | recovery successes | median recovery seconds | recovery workflow errors |
|---|---:|---:|---:|---:|
| matching / go | 5 | 2/3 | 5.00 | 19 |
| matching / rust | 25 | 3/3 | 5.00 | 0 |
| history / go | 5 | 3/3 | 5.00 | 0 |
| history / rust | none | 3/3 | 6.00 | 0 |
| all / go | 25 | 3/3 | 5.00 | 0 |
| all / rust | 25 | 3/3 | 5.00 | 0 |
| frontend / go | 25 | 3/3 | 5.00 | 0 |
| frontend / rust | 25 | 3/3 | 5.00 | 0 |

All four 100-starts/sec stages reached the 1200s stage timeout (exit 124), with partial ledgers and workflow errors. These are failed overload tests, not measured sustainable capacities. CI success in three lanes means fixed/recovery correctness passed; it does not mean the capacity stages passed.
The Matching lane CI failure is in its All-Go recovery baseline: 19 warmup START_TO_CLOSE timeouts, before measurement/fault injection; only two Go recovery repetitions ran successfully. Rust Matching recovered 3/3 with no workflow errors. This does not establish a Rust Matching recovery regression.
The All-Go 25/s step passed all three targets in the Frontend and All-Rust lanes, missed one target in the Matching lane, and missed all three in the History lane. History-only Rust also missed one 5/s target. This baseline variation prevents ranking cross-runner capacity from the coarse steps.

| Lane | 25/s Go/Rust median p99 ms | 100/s Go/Rust recorded workflow errors |
|---|---:|---:|
| matching | 125.24 / 122.96 | 8911 / 5950 |
| history | 17406.75 / 18948.24 | 3004 / 7537 |
| all | 155.80 / 295.16 | 8981 / 5498 |
| frontend | 169.04 / 193.55 | 8982 / 2996 |

## Comparison to previous results

Previous SDK smoke source `c1bbd3ae71c84a71d5a226a70b0d8b71d021cb5b`: Matching run 43480728156, History run 43479770520; Frontend rerun 43487649192 failed all 50 warmup workflows before measurement. These used the same 5/s, 10s/30s, three-pair one-activity protocol. Hosted runners and compiler/image resolution vary, so deltas are observations rather than isolated causal attribution.

| Isolated service | previous Go/Rust p99 ms | updated Go/Rust p99 ms |
|---|---:|---:|
| matching | 76.77 / 86.11 | 81.91 / 59.85 |
| history | 80.79 / 20073.00 | 613.35 / 931.88 |
| frontend | unavailable (warmup failed) | 75.71 / 78.07 |

Previous completion p50/p95 Go vs Rust (ms): Matching 61.64/70.47 vs 75.69/82.66; History 68.33/76.82 vs 20016.33/20067.09. These are medians of per-run percentiles, retained from the earlier compact reports.

Previous Matching isolated-service CPU Go/Rust 0.0274/0.0338 cores, peak cgroup memory 31.48/19.30 MiB, both 5 completions/s and 450 correct. Previous History CPU 0.1555/0.0659 cores and memory 40.97/26.70 MiB were NOT equal-throughput gains: observed completion rate was 5/1.67 per second and Rust drained later. Previous RSS and recovery: unavailable.

### Older run 41767896292

https://github.com/auenkr/cadence-rust/actions/runs/41767896292 — source `316c28e4a2638e55cd52603a307fb6d7ffd09bd7`, combined Rust Frontend+Matching with Go History/Worker. Closed-loop concurrency 1/16/64, five repeats, 100 warmups and 1000 measured workflows, all 30000 correct. This differs from fixed-arrival isolated-service and All-Rust tests: do not compute cross-protocol speedups.

| concurrency | Go/Rust completions/s | Go/Rust p50 ms | Go/Rust p95 ms | Go/Rust p99 ms |
|---:|---:|---:|---:|---:|
| 1 | 12.77 / 10.78 | 77.63 / 92.24 | 84.94 / 99.84 | 90.34 / 104.10 |
| 16 | 33.57 / 29.82 | 484.45 / 541.93 | 618.96 / 709.16 | 671.86 / 769.26 |
| 64 | 31.13 / 27.55 | 2294.92 / 2623.26 | 2600.80 / 3035.88 | 2884.15 / 3488.81 |

At old concurrency16, per-1000-workflow FE CPU seconds Go/Rust were 4.069/5.008; Matching 2.687/5.045. FE peak cgroup MiB 39.25/29.27; Matching 35.66/26.80. These CPU seconds per fixed work count differ from new average CPU cores. RSS/capacity/recovery comparisons are unavailable.

## Provenance and limits

Generated Go History config confirms **4 History shards**. Matching and History lane identity snapshots both resolve MySQL to image `sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b` (`mysql:8.0`). Service replicas: one each; MySQL: one.

Exact resource/Compose definitions are versioned in `Benchmark/compose.yml` and `Benchmark/run.py` at the harness SHA above; SDK locks in `Benchmark/go.mod` / `go.sum`. Runtime configs use the pinned source Go image template/dynamic defaults. Resolved Compose hashes follow. Rust History uses a standalone launcher; FE/Matching share the server launcher. All binaries are release builds; hashes are read from the actual runtime images.

- **matching**: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`, SDK lock SHA256 `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`; driver SHA256 `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`.
  - go_binary_sha256: `5c3aa8f03ea0c4d7a959044eda01b71597e782f8f442859cbd7ec9686addb099  /usr/local/bin/cadence-server`
  - rust_binary_sha256: `8182da9246d391b099b0dfaf296915a263d4470577a31585099b26763e0d8392  /usr/local/bin/cadence-server`
  - go resolved Compose SHA256: `f53ec0200e8c762d4a95366b1f60f0624c6eee9b951e048c0f3b7cdd8fc4848b`
  - rust resolved Compose SHA256: `c85af75fd1dbf72038b5d4a2e114b88af89e244ee4436b54e6e10d9199b4d204`
- **history**: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`, SDK lock SHA256 `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`; driver SHA256 `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`.
  - go_binary_sha256: `c8662bbb2d49be8b42a5de0100e95327583264a3d34c3c495ba7b44090100e6e  /usr/local/bin/cadence-server`
  - rust_binary_sha256: `31ffce44c72573ac07d13056802c8a81f8b86c5d0060e2051680ea3335f4f5aa  /usr/local/bin/cadence-server`
  - go resolved Compose SHA256: `f53ec0200e8c762d4a95366b1f60f0624c6eee9b951e048c0f3b7cdd8fc4848b`
  - rust resolved Compose SHA256: `fd686618883f39de2eb6150a2fd25289584abfe0361f5216a16b2ce8c0caf621`
- **all**: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`, SDK lock SHA256 `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`; driver SHA256 `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`.
  - go_binary_sha256: `3352660ad0ba04e0f8da923b08f6e79092da9f4954cce87822af1e14ec0f1643  /usr/local/bin/cadence-server`
  - rust_binary_sha256: `98efe2ecdc20c77c3c92f1b3d4cb3a6d6499cecfed1aa11f500f53012e889cff  /usr/local/bin/cadence-server`
  - rust_history_binary_sha256: `ea3351262e78432334ba518d583899cb8ab31743c2c3b68c086c521a3766860e  /usr/local/bin/cadence-server`
  - go resolved Compose SHA256: `f53ec0200e8c762d4a95366b1f60f0624c6eee9b951e048c0f3b7cdd8fc4848b`
  - rust resolved Compose SHA256: `f5614ebe9327e89eeb06d96f770db441d6c0a04845ad3c36e2bb782151d12c1f`
- **frontend**: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`, SDK lock SHA256 `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`; driver SHA256 `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`.
  - go_binary_sha256: `fe323a6ec9025458820d081cec1355feef913e79bdeb35cfb65fdb4f8855a588  /usr/local/bin/cadence-server`
  - rust_binary_sha256: `8182da9246d391b099b0dfaf296915a263d4470577a31585099b26763e0d8392  /usr/local/bin/cadence-server`
  - go resolved Compose SHA256: `f53ec0200e8c762d4a95366b1f60f0624c6eee9b951e048c0f3b7cdd8fc4848b`
  - rust resolved Compose SHA256: `4511271398699498bd4c488fb5383ab9ff01a8dad4c5ef558c0bbe1c04b6558a`

Matching RSS trend is provisional: the memory-growth/soak-slope fix was still in progress at this source pin. No short smoke test establishes a stable RSS slope. CPU and memory improvements require equal verified completion throughput within each paired runner. No multi-workload, long soak, server retry, database query-tail or production-capacity claim. Failed/incomplete arms remain failures. Raw logs/samples are excluded from Git and have three-day CI retention.
