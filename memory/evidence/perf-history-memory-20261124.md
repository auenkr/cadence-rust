# History memory and long-poll cancellation

Branch `perf-history-memory` starts from master `ac5df792cce5116c4b96e3c38f78050d0e9a3182`. Final code head `98047b4fcc5c78e2e8b506be39c84ff94ce11f3a` uses Tokio's default thread stack in all profiles.

- Run 45291978847 sampled total cgroup memory. Its Go → Rust History peaks were 77.4 → 89.2, 98.0 → 114.8, and 111.9 → 137.3 MiB at concurrency 1, 16, and 64. Charged file cache is part of those totals. The resource collector now records independently sampled peaks for total, `memory.stat anon`, and `memory.stat file`. The baseline rerun uses the new collector against unmodified History on the same master base. [Original benchmark](https://github.com/auenkr/cadence-rust/actions/runs/45291978847)
- Standalone History with `jemalloc-default` exports the allocator's prefixed config symbol with background purging, 1-second dirty decay and two arenas. The two-arena default targets the one-CPU benchmark. Deployments can override it with `_RJEM_MALLOC_CONF`. Tokio chooses its worker count from CPU availability, including Linux quotas, and honors explicit `TOKIO_WORKER_THREADS` and `RUST_MIN_STACK` overrides.
- Dropping a long-poll future now releases its subscriber and closes the channel that drives the notifier adapter's relay task, following Go's deferred unsubscribe. The cancellation test checks that the subscription disappears as soon as the future is dropped.

## CI

These runs use the final code head `98047b4fcc5c78e2e8b506be39c84ff94ce11f3a`.

| Check | Run | Conclusion | Rust History binary SHA256 |
|---|---|---|---|
| History Clippy and tests, fresh target | [45374014119](https://github.com/auenkr/cadence-rust/actions/runs/45374014119) | Success. Clippy and all 944 History tests passed. | |
| Workspace Clippy and tests, fresh target | [45374013966](https://github.com/auenkr/cadence-rust/actions/runs/45374013966) | Success. Workspace Clippy and tests passed. | |
| MySQL mixed gate, two History hosts, jemalloc, gRPC | [45356914667](https://github.com/auenkr/cadence-rust/actions/runs/45356914667) | Success. 430 checks, `GATE SUMMARY failed: []`. | `1b6ce016e51bab7bb5c8798866a4c97db5a97aaa5c6b8a526c0e7318d248b676` |
| MySQL mixed gate, two History hosts, jemalloc, TChannel | [45356919332](https://github.com/auenkr/cadence-rust/actions/runs/45356919332) | Success. 430 checks, `GATE SUMMARY failed: []`. | `83253dd9de52e48264a47709219cc93ead14c34f9a514bf346c694591fe3eb51` |
| MySQL soak, two History hosts, jemalloc, 45 min at 4 workflows/s | [45356929391](https://github.com/auenkr/cadence-rust/actions/runs/45356929391) | Success. 35 checks and 10,800 workflows passed. | `0a96565c8c2bc0cd5f45ae3a8f75677c7e2e14e751ebba5e26df8ede1dbcc540` |
| Final paired benchmark | [45366356133](https://github.com/auenkr/cadence-rust/actions/runs/45366356133) | Success. Strict artifact verification passed. | `30af6eee79024fc712e2362017545d8eede03bf1be5316eac6da72035f09f525` |

The [initial debug gate](https://github.com/auenkr/cadence-rust/actions/runs/45343599787) at `ad82d13990838c5fbb395f1414411eae99ef7bb8` failed when both History binaries overflowed their 1 MiB worker stacks during schedule backfill. Debug History SHA256 `37ee7b257661ac165a9c50853e91a82bffb0847995bbda1f2c5f3db4212fc515`. The [TChannel attempt](https://github.com/auenkr/cadence-rust/actions/runs/45350728318) at `57ec600c2786f8f80a4049e0783317c7d224b029` used the same stack and was canceled after the overflow diagnosis. The intermediate fix `dd6ef15ec0ea4cb1cb228ae36554ba4c79f4aea4` restored the default debug stack and passed the [debug](https://github.com/auenkr/cadence-rust/actions/runs/45354892737) and [release](https://github.com/auenkr/cadence-rust/actions/runs/45354892340) gates. The final code keeps the default in both profiles.

The paired benchmark dispatches use `rust_services=frontend,matching,history` with defaults from run 45291978847. Each run uses five paired repetitions, 1,000 workflows per arm at concurrency 1, 16 and 64, 10 warmups, a 1,024-byte payload, and the Go SDK over TChannel.

## Instrumented baseline

Runner Intel Xeon Platinum 8573C, 4 logical CPUs, 15 GiB, Ubuntu 26.04.1. History container limit was 1 CPU and 1,400 MiB. Rust History binary SHA256 `57ff8ddeb0fffcb002b0814ba6fca7399fd59bd403c8f6209cc6db292aaa0f55`. Go server binary SHA256 `4d07c752b3abb1e6dc58d8688b42a6147ca1d44d03a74eb741ec0ec6d08d9eb4`. Head `f1a350cab3bcd5f35aa2ec2f768cf8c310a60548`, success with strict artifact verification. [45343398849](https://github.com/auenkr/cadence-rust/actions/runs/45343398849)

Each cell is Go → Rust, with the median of five per-run peaks for memory and five cumulative CPU deltas for CPU. Read `containers.cadence-fm-benchmark-history-1` in `paired-benchmark/resources-*-c*.json` from each run artifact. Independent memory peaks can occur at different samples. The median file peak rounds to 0.00 MiB at these concurrencies, though individual repetitions had file charges.

| Concurrency | CPU seconds per 1,000 workflows | Peak anon MiB | Peak file MiB | Peak total MiB |
|---:|---:|---:|---:|---:|
| 1 | 18.41 → 7.64 | 71.91 → 88.20 | 0.00 → 0.00 | 74.97 → 91.85 |
| 16 | 15.94 → 9.18 | 89.61 → 113.15 | 0.00 → 0.00 | 95.57 → 119.57 |
| 64 | 49.50 → 10.41 | 103.35 → 130.32 | 0.00 → 0.00 | 110.89 → 135.82 |

Rust History's baseline anonymous-memory gap was 16.29, 23.54 and 26.97 MiB. The excess in this rerun was predominantly anonymous memory.

## Final paired benchmark

Runner AMD EPYC 7763, 4 logical CPUs, 15 GiB, Ubuntu 26.04.1. Go server binary SHA256 `270777d38975006305bed475ae57f11b00722af365c22e7051c11ce6633c96de`. The benchmark completed 30,000 workflows with zero failures. Each cell is Go → Rust.

| Concurrency | CPU seconds per 1,000 workflows | Peak anon MiB | Peak file MiB | Peak total MiB |
|---:|---:|---:|---:|---:|
| 1 | 22.74 → 7.51 | 76.47 → 80.94 | 0.00 → 0.00 | 80.04 → 83.71 |
| 16 | 23.00 → 11.35 | 95.57 → 94.75 | 0.00 → 0.00 | 101.44 → 98.91 |
| 64 | 43.28 → 12.47 | 103.98 → 123.14 | 0.00 → 0.00 | 112.52 → 129.69 |

The final Rust-minus-Go anonymous-memory gaps were +4.46, −0.82 and +19.16 MiB. Rust History used 67.0%, 50.7% and 71.2% less CPU than its paired Go arm. The paired gap narrowed at all three concurrency levels relative to the instrumented baseline. Baseline and final runs landed on different CPU models, so the cross-run change is indicative. The final run keeps Tokio's default stack and measures jemalloc and subscription cleanup together.

## Paired benchmark with the experimental 1 MiB stack

Runner AMD EPYC 7763, 4 logical CPUs, 15 GiB, Ubuntu 26.04.1. Rust History binary SHA256 `d60a585d4b75285db1cb00529aae3476a1f33b880af44f92ae8da09f7d95db41`. Go server binary SHA256 `39a8d24f2d99f69e943f4f92afef4cae6b9ce9e795de4bc4bf8c87d658bc2fea`. Head `ad82d13990838c5fbb395f1414411eae99ef7bb8`, success with strict artifact verification and 30,000 workflows with zero failures. [45343599527](https://github.com/auenkr/cadence-rust/actions/runs/45343599527)

| Concurrency | CPU seconds per 1,000 workflows | Peak anon MiB | Peak file MiB | Peak total MiB |
|---:|---:|---:|---:|---:|
| 1 | 24.79 → 8.56 | 72.04 → 78.25 | 0.00 → 0.00 | 75.57 → 82.61 |
| 16 | 21.79 → 11.50 | 94.07 → 95.06 | 0.00 → 0.00 | 99.94 → 99.33 |
| 64 | 46.45 → 12.57 | 106.57 → 110.15 | 0.00 → 0.00 | 113.34 → 121.13 |

The tuned Rust-minus-Go gap was 6.21, 0.99 and 3.58 MiB of anonymous memory. Rust used 47–73% less History CPU than the Go arm. The baseline and tuned benchmarks used different CPU models, so the change in the paired gaps is indicative. The benchmark applies the allocator, stack and cancellation changes together.

## Soak

The second History host restarted at 1,356 seconds. The untouched host's early and late median RSS went from 115.7 to 142.3 MiB, peaking at 143.4 MiB. Its samples stayed around 142 MiB after the restart. The restarted host's early and late post-recovery medians were 256.8 and 255.2 MiB, with a 257.5 MiB peak. Both passed the soak's RSS checks. The restart-associated plateau shift is visible in `samples.jsonl` in the `history-soak-mysql` artifact. Elapsed load was 2,700 seconds at four workflows per second.
