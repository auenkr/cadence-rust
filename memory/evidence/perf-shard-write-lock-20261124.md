# History shard write gate · 2026-11-24

Branch `perf-shard-write-lock`. Code head `19014df6892a584443e09670a569d682c87077eb`. Parent `ac5df792cce5116c4b96e3c38f78050d0e9a3182`.

## Lock boundary

Go prepares workflow mutations and appends their history in `go/service/history/execution/context.go`. The shard methods in `go/service/history/shard/context.go` then acquire the shard lock to allocate queue task IDs, write mutable state, and update the queue read level. Go's history event and transaction ID allocation also takes the shard lock.

Rust's combined updates previously built and appended the current run's history while holding the same gate that serializes queue writes. Continue-as-new, retry/cron, terminate-and-start, and reset now prepare the current mutation and append its history before acquiring the queue write gate. A short gate acquisition covers current-run history and transaction ID allocation in combined updates, which keeps range renewal serialized with queue writes. The queue write gate still covers both runs' queue task ID allocation in the existing order, task serialization with the final IDs and timer timestamps, range ID capture, the execution write, queue notifications, and immediate read-level advancement. The write error paths retain ownership-loss shutdown and range renewal on an unknown outcome.

Ordinary update and create release the gate before updating in-memory workflow bookkeeping and notifying history long polls. The caller's workflow cache lock protects the in-memory state. The regression test pauses a continue-as-new at the current-run history append, verifies the shard gate can be acquired, then pauses at the combined execution write and verifies the gate is held. Existing shard tests cover the queue read level while a lower task ID is uncommitted.

## Verification

All builds and tests ran on standard GitHub-hosted Linux runners. The Rust CI runs use a clean target and run Clippy before tests.

| Check | Run | Result |
|---|---|---|
| History crate Clippy and tests | [45344205599](https://github.com/auenkr/cadence-rust/actions/runs/45344205599) | Success. 943 History tests passed, including the continue-as-new gate regression. |
| Workspace Clippy and tests | [45344205706](https://github.com/auenkr/cadence-rust/actions/runs/45344205706) | Success. Clippy and test exit codes were zero in `/home/runner/work/_temp/cargo-target-final`. |
| History mixed gate, MySQL, two History hosts | [45344205813](https://github.com/auenkr/cadence-rust/actions/runs/45344205813) | Success. `GATE SUMMARY failed: []`. |
| History mixed gate, Cassandra, two History hosts | [45344205547](https://github.com/auenkr/cadence-rust/actions/runs/45344205547) | Success. `GATE SUMMARY failed: []`. |
| History restart gate, MySQL | [45344203999](https://github.com/auenkr/cadence-rust/actions/runs/45344203999) | Success. 52 checks, 4,500 of 4,500 workflows completed, zero failures. `GATE SUMMARY failed: []`. |
| History swap, MySQL | [45344205618](https://github.com/auenkr/cadence-rust/actions/runs/45344205618) | Success. 47 checks, 36 workflow histories compared, zero differences. `GATE SUMMARY failed: []`. |
| Paired benchmark, code head | [45344205512](https://github.com/auenkr/cadence-rust/actions/runs/45344205512) | Success. All five paired repetitions verified at each concurrency. |
| Paired benchmark, parent control | [45343784415](https://github.com/auenkr/cadence-rust/actions/runs/45343784415) | Success. All five paired repetitions verified at each concurrency. |
| Paired benchmark, requested reference `500dec50a9ccc2660ad6a32ecfa8aa12e262dd74` | [45291978847](https://github.com/auenkr/cadence-rust/actions/runs/45291978847) | Success |

The enforced CI rustfmt checks passed. The informational workspace-wide formatting probe exited 1 on this branch and on the release-profile CI run already merged into its base. [Release-profile CI](https://github.com/auenkr/cadence-rust/actions/runs/45334522594)

### Rust binary SHA256

| Run | cadence-history | cadence-server |
|---|---|---|
| MySQL mixed | `bf9d5b9b96a411d1f27a9139486644640cd4f77998ea5b06214bef05342f044f` | `4e1f78e6e41361895b24287e7b4afe4701ed2977ee574e842ea7ea47c4321d4d` |
| Cassandra mixed | `ee802feb513a14059eb4d4cf0bcfd0d2944f12c2623314ad595f8a7500e1834a` | `a632f3270e4a5f510d9f274dc6f63cd79322f63c268d2cdde4d35134af6da8ef` |
| MySQL restart | `147855bc4b5fce88684afb268bf00ead5a4164ce55e4de93038d20ccff66815b` | `63e5cef9753f9caa80f7709d3040e9266dd4769394ab47cc2007a5389291b2b7` |
| MySQL swap | `abe73754ea2017a2917d2e152d0a0e08bc1d5189552b72d4e9d839cb5495e7ef` | `dec4a0574fa8bfa02b08e0f147cfab235733b753021eb814f34f4c70720fe472` |
| Code-head benchmark | `0359764d499aeadf895ef82fa6648eaa07714c25b205114ab44e8a9bce82609b` | `3f2d061200487af9b9fea004da31ec54d4a6fc381a65fe43e471ea51189a192b` |
| Parent benchmark | `57ff8ddeb0fffcb002b0814ba6fca7399fd59bd403c8f6209cc6db292aaa0f55` | `540f4f83bed4d296b22aa16811ddc219c11eda345e6b32b850db2acb8ed3cc69` |
| Requested reference benchmark | `f549c3c9eb835bcfc15e5586708839770f57c4e3fb0b3feb5c1fa74a48bdf472` | `3d39074f89e03fa5464e5257b34b82524c9b42d3c98ee7d80b810d1142ab7aeb` |

## Benchmark

Both new runs use `rust_services=frontend,matching,history`, 1,000 measured workflows per arm per concurrency level, five repetitions, concurrency 1/16/64, and a 1,024-byte payload. Warmup is 100 workflows versus 10 in the requested reference. The requested reference also predates the fat LTO and single codegen unit release profile. The parent control uses the same warmup and release profile as the change. The system Worker is Go in both benchmark arms.

Each benchmark workflow runs a single echo activity. The benchmark exercises ordinary writes and releases of the shard gate. The regression test and restart workload exercise continue-as-new.

| Concurrency | Run | Go workflows/s | Rust workflows/s | Rust p95 ms | Rust p99 ms | Rust History CPU s / 1,000 workflows |
|---:|---|---:|---:|---:|---:|---:|
| 16 | Requested reference | 41.902 | 60.615 | 354.426 | 394.971 | 13.024 |
| 16 | Parent control | 37.403 | 74.324 | 295.896 | 328.338 | 9.440 |
| 16 | Code head | 41.596 | 62.733 | 344.543 | 378.037 | 10.622 |
| 64 | Requested reference | 20.674 | 57.076 | 1600.292 | 1719.125 | 14.043 |
| 64 | Parent control | 21.682 | 65.052 | 1400.810 | 1466.662 | 11.019 |
| 64 | Code head | 22.915 | 58.518 | 1479.715 | 1603.054 | 11.933 |

The code-head Rust arm gained 3.5% at c16 and 2.5% at c64 in throughput against the requested reference. Its p99 fell 4.3% and 6.8%, respectively. Relative to the parent control, throughput fell 15.6% and 10.0% while p99 rose 15.1% and 9.3%. The requested reference and code head ran on AMD EPYC 7763. The parent control ran on Intel Xeon Platinum 8573C. All three used the same MySQL image and container resource limits.

The earlier fat-LTO benchmark on AMD EPYC 7763, at `3e4e58c0e2052f771aa062c4714faf6ae2994f7e` with 10 warmup workflows, reached 63.844 and 58.635 workflows/s at c16 and c64, with p99 of 389.381 and 1589.166 ms. The code-head results are within 1.8% of its throughput at both levels. Hardware, warmup, and release-profile differences across the comparisons prevent attributing a throughput change to the shorter gate. [Release-profile benchmark](https://github.com/auenkr/cadence-rust/actions/runs/45334544627)
