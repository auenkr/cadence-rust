# Final-code benchmark: 77dbaa379 versus 3c2bf3aaa

New source: `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e`. Previous source: `3c2bf3aaabf160518afde04f75097d2dc2c374a6`.
Same pinned SDK/one-activity workflow, 1 KiB payload, 10ms activity, non-sticky, worker capacity 128. Fixed load 5 starts/s; capacity steps 5/25/100; three alternating fresh-MySQL pairs, 10s warmup and 30s measured per arm. Recovery drops MySQL TCP input for 3s, 10s into measurement. One Go system Worker stays in every configuration.
Standard ubuntu-latest, jobs <=90min. Services each 1 CPU/1400MiB; MySQL 1.5 CPU/1800MiB. SDK generator/worker occupy separate CPUs; services+DB share CPUs2–3. No allocator feature is enabled: this historical Alpine benchmark uses **System backed by musl**, not the glibc System used by the normal GNU build. It preserves the previous build for comparability and does not establish glibc allocator behavior.

## Attempts and outcomes

- matching, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43702926802 — success; harness `d0207e990a955df41264cffad5e11818a60e87e7`.
- frontend, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43702926775 — success; harness `d0207e990a955df41264cffad5e11818a60e87e7`.
- history, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43702926854 — success; harness `d0207e990a955df41264cffad5e11818a60e87e7`.
- all, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43702926806 — success; harness `d0207e990a955df41264cffad5e11818a60e87e7`.

## Fixed-load results

Each cell is previous → new. Medians of per-run values; CPU sums Frontend+Matching+History cores. Memory/RSS sum per-container sampled peaks, not simultaneous cluster peaks; summed process RSS can repeat shared pages. Worker/DB excluded. Resources include drain and History audit. All-Go main row uses the All-Rust lane baseline; every paired baseline is shown separately below.

| Configuration | completions/s | p50 ms | p95 ms | p99 ms | CPU cores | memory MiB | RSS MiB | correct | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| All Go | 5.00 → 5.00 | 57.96 → 45.07 | 67.98 → 51.42 | 73.13 → 54.30 | 0.22 → 0.14 | 106.91 → 105.49 | 313.93 → 305.25 | 450 → 450 | 0 → 0 |
| Rust Matching only | 5.00 → 5.00 | 49.26 → 41.00 | 56.58 → 48.94 | 59.85 → 71.95 | 0.16 → 0.12 | 98.19 → 94.63 | 276.68 → 266.41 | 450 → 450 | 0 → 0 |
| Rust Frontend only | 5.00 → 5.00 | 62.51 → 60.32 | 72.45 → 68.52 | 78.07 → 73.39 | 0.21 → 0.17 | 97.05 → 94.92 | 272.30 → 272.29 | 450 → 450 | 0 → 0 |
| Rust History only | 5.00 → 5.00 | 122.83 → 81.29 | 654.13 → 424.14 | 931.88 → 720.03 | 0.15 → 0.15 | 90.49 → 89.28 | 267.91 → 262.75 | 450 → 450 | 0 → 0 |
| All Rust (Go Worker) | 5.00 → 5.00 | 72.34 → 66.31 | 75.79 → 67.96 | 78.45 → 72.02 | 0.18 → 0.11 | 67.13 → 67.70 | 188.70 → 186.36 | 450 → 450 | 0 → 0 |

## Capacity and recovery

Capacity is highest tested rate passing all three repetitions: zero errors, valid generator, completion p99 <=1000ms and at most max(2,1% arrivals) outstanding at the end. It is a short tested bound, not long-term sustainable capacity. An empty capacity means no tested step passed all targets. Recovery measures the END of the first full healthy five-second arrival window after restoration; it cannot be less than 5s. Recovery errors include warmup failures; missing windows are not successes.

| Configuration | passing tested starts/s old → new | recovery repetitions old → new | recovery seconds old → new | recovery errors old → new |
|---|---:|---|---:|---:|
| All Go | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust Matching only | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust Frontend only | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust History only | none → 5 | 3/3 → 3/3 | 6.00 → 6.00 | 0 → 0 |
| All Rust (Go Worker) | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |

## Paired baselines and stage outcomes

Runner-to-runner variation is visible in the All-Go baselines. Compare Rust with its own Go pair before attributing changes to the migration. Retry results are identified above; first failures are retained rather than silently replaced.

| Lane / source | Go fixed completion p99 ms | Go CPU cores | Go memory / RSS MiB | Go passing starts/s |
|---|---:|---:|---:|---:|
| matching / previous | 81.91 | 0.16 | 106.25 / 310.88 | 5 |
| matching / new | 91.22 | 0.12 | 105.62 / 306.37 | 25 |
| frontend / previous | 75.71 | 0.21 | 107.13 / 307.34 | 25 |
| frontend / new | 73.31 | 0.18 | 107.46 / 307.61 | 25 |
| history / previous | 613.35 | 0.18 | 107.62 / 313.03 | 5 |
| history / new | 559.90 | 0.18 | 107.20 / 308.25 | 5 |
| all / previous | 73.13 | 0.22 | 106.91 / 313.93 | 25 |
| all / new | 54.30 | 0.14 | 105.49 / 305.25 | 25 |

| New lane / arm | 25/s passed all targets | 100/s stage exit | 100/s recorded errors |
|---|---|---:|---:|
| matching / go | True | 124 | 8716 |
| matching / rust | True | 124 | 5839 |
| frontend / go | True | 1 | 8967 |
| frontend / rust | True | 1 | 5975 |
| history / go | False | 124 | 8988 |
| history / rust | False | 124 | 5757 |
| all / go | True | 1 | 8483 |
| all / rust | True | 1 | 3313 |

## Rust binary provenance

Source/harness/tree hashes and actual runtime-image executable SHA256s follow. Exact config is versioned at the harness SHA in Benchmark/compose.yml and run.py; Go config/dynamic defaults/schema come from the pinned source. New manifests record the Rust compiler/target. Historical rust:1-alpine and mysql:8.0 tags can move; these comparisons do not isolate compiler or hosted-runner effects.

- previous matching: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `8182da9246d391b099b0dfaf296915a263d4470577a31585099b26763e0d8392  /usr/local/bin/cadence-server`
  - go_binary_sha256: `5c3aa8f03ea0c4d7a959044eda01b71597e782f8f442859cbd7ec9686addb099  /usr/local/bin/cadence-server`
  - driver_sha256: `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
- previous history: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `31ffce44c72573ac07d13056802c8a81f8b86c5d0060e2051680ea3335f4f5aa  /usr/local/bin/cadence-server`
  - go_binary_sha256: `c8662bbb2d49be8b42a5de0100e95327583264a3d34c3c495ba7b44090100e6e  /usr/local/bin/cadence-server`
  - driver_sha256: `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
- previous all: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `98efe2ecdc20c77c3c92f1b3d4cb3a6d6499cecfed1aa11f500f53012e889cff  /usr/local/bin/cadence-server`
  - rust_history_binary_sha256: `ea3351262e78432334ba518d583899cb8ab31743c2c3b68c086c521a3766860e  /usr/local/bin/cadence-server`
  - go_binary_sha256: `3352660ad0ba04e0f8da923b08f6e79092da9f4954cce87822af1e14ec0f1643  /usr/local/bin/cadence-server`
  - driver_sha256: `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
- previous frontend: source `3c2bf3aaabf160518afde04f75097d2dc2c374a6`, harness `fd6825302eacc03aaed8f490fc632388d0f645dc`, Rust tree `2feac60f00c7d0a40411998bf47df5f51008085e`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `8182da9246d391b099b0dfaf296915a263d4470577a31585099b26763e0d8392  /usr/local/bin/cadence-server`
  - go_binary_sha256: `fe323a6ec9025458820d081cec1355feef913e79bdeb35cfb65fdb4f8855a588  /usr/local/bin/cadence-server`
  - driver_sha256: `338bedc2b155e7fe1d0904ee817536a7a57cd73f5a6501ef92a5faea6b2ca7da`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
- new matching: source `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `20aba723e0d0edba2fbe82cba3041c643ccf3cc47f47634aa8a23f073742ffcc  /usr/local/bin/cadence-server`
  - go_binary_sha256: `111981f33e6e3919030c93aa9d9490d61371326d28e30b9f7440e582843f0fd3  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- new history: source `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `5bd8d3ededd1974c7c7c7db5d6b350c753e28fa2b1359a371beb921dedd1bdfa  /usr/local/bin/cadence-server`
  - go_binary_sha256: `63d72102f54dd413977253ad707a779979e10554e9382fbd689f54c5849bb944  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- new all: source `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `82027497179f426282baf8431e3a8af10ab1246758c9a8443ee2be1860d0cad1  /usr/local/bin/cadence-server`
  - rust_history_binary_sha256: `3901d029b8e799259e1c04073e22b9acfcd61a0355ed66095c4e4b1e466931ca  /usr/local/bin/cadence-server`
  - go_binary_sha256: `4518d3846a464b9c12af92c89e686f339debc237f1f76571f71aafb886dda22f  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- new frontend: source `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `20aba723e0d0edba2fbe82cba3041c643ccf3cc47f47634aa8a23f073742ffcc  /usr/local/bin/cadence-server`
  - go_binary_sha256: `e2310f51806f54fd14d9f301628c3953b81302542b2579940fbd6790032c5d63  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5

## Separate older reference: CI 41767896292

https://github.com/auenkr/cadence-rust/actions/runs/41767896292 — source `316c28e4a2638e55cd52603a307fb6d7ffd09bd7`. Combined Rust Frontend+Matching with Go History/Worker; five closed-loop repetitions, concurrency 1/16/64, 100 warmups and 1000 measured workflows. All 30000 measured workflows succeeded. This is a different protocol and configuration, so its throughput is not an open-loop capacity comparison.

| concurrency | Go/Rust workflows/s | Go/Rust p50 ms | Go/Rust p95 ms | Go/Rust p99 ms |
|---:|---:|---:|---:|---:|
| 1 | 12.77 / 10.78 | 77.63 / 92.24 | 84.94 / 99.84 | 90.34 / 104.10 |
| 16 | 33.57 / 29.82 | 484.45 / 541.93 | 618.96 / 709.16 | 671.86 / 769.26 |
| 64 | 31.13 / 27.55 | 2294.92 / 2623.26 | 2600.80 / 3035.88 | 2884.15 / 3488.81 |

At old concurrency16, FE CPU seconds per 1000 workflows Go/Rust were 4.069/5.008, Matching 2.687/5.045; FE memory MiB 39.25/29.27, Matching 35.66/26.80. Older RSS and recovery are unavailable. CPU seconds per fixed work count differ from average CPU cores.

## Limits

No speed/cost claim from incomplete arms or unequal completion throughput. Short RSS samples do not establish a soak slope or that QPS-bucket retention fixes have removed all growth. Matching QPS retention, cancellation-safe reads, pooled Frontend-to-History connections and History batch validation changed together; this test does not isolate their individual effects. Other scenario specifications remain unimplemented. Raw logs/ledgers stay ignored; only compact reports/charts are committed.
