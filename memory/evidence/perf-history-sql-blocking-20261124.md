# History SQL scheduling experiment, 2026-11-24

The standalone History binary previously used `#[tokio::main]`. Tokio 1.53.3 takes `TOKIO_WORKER_THREADS` when set and otherwise uses `std::thread::available_parallelism()`, falling back to one worker. The paired benchmark gives History a one-CPU Docker quota.

History uses synchronous `mysql` and `postgres` connections. The `mysql_async` runtime in the shared pool serves separate timed domain queries. The History adapter's `block_in_place` hands the Tokio scheduler core to another thread while the caller runs SQL synchronously. Persistence calls commonly borrow transaction inputs. Converting them to `spawn_blocking` would require owned inputs and changes to many callers. An async driver migration would also change connection and cancellation behavior.

The candidate uses an explicit Tokio runtime builder in `rust/crates/cadence-history/src/main.rs`. It sets two workers when Tokio would otherwise use one automatic worker. Explicit `TOKIO_WORKER_THREADS` settings still go through Tokio's normal validation. The SQL adapters, statements, transactions, timeouts and error mapping are unchanged.

- Control source `ac5df792cce5116c4b96e3c38f78050d0e9a3182`.
- Candidate source `0d424f07e5281eee02154dbb62a11763d001cd87` on `perf-history-sql-blocking`.

## Compatibility

| Run | Result |
| --- | --- |
| [History crate clippy and tests](https://github.com/auenkr/cadence-rust/actions/runs/45343382859) | Success, 943 tests passed, fresh target |
| [Workspace clippy and tests](https://github.com/auenkr/cadence-rust/actions/runs/45343382840) | Success, 3,473 tests passed and 29 ignored, fresh target |
| [Workspace rustfmt patch](https://github.com/auenkr/cadence-rust/actions/runs/45347246575) | Success at documentation-only child `00e4af8ea76c91fc8b66641b3ef17965dedd2d83`, no diff for the changed History entry point |
| [MySQL History mixed gate](https://github.com/auenkr/cadence-rust/actions/runs/45343383091) | Success, `two_history=true`, `GATE SUMMARY failed: []` |
| [PostgreSQL History mixed gate](https://github.com/auenkr/cadence-rust/actions/runs/45343382841) | Success, `two_history=true`, `GATE SUMMARY failed: []` |
| [MySQL History restart gate](https://github.com/auenkr/cadence-rust/actions/runs/45343382706) | Success, `GATE SUMMARY failed: []`, 4,500 workflows completed with zero failures in the 1,500 s fault scenario |

MySQL mixed gate binary SHA-256 values:

- `cadence-history` `d1fe1eb9f6dc3657814d55cfd53a313c0d7d7be5ed121366402e0bd1121b64b9`
- `cadence-server` `ea14030416d7f03dc2a08fb4634ec331f6d579d5a00bac819974f99342e78125`

PostgreSQL mixed gate binary SHA-256 values:

- `cadence-history` `e97ba54c74343423090ac756637c315de520b0f936e79fd004e71acdca4c4b0f`
- `cadence-server` `d31ec7ed16e319b80f45857b0b66c7439a6fcb3704fbcaa67a3908225707edbe`

MySQL restart gate binary SHA-256 values:

- `cadence-history` `e68c3c289a84e562ddb2a53c72b67699b086fd22a6c8e3cd0c961634ab542de8`
- `cadence-server` `e36b26fde6de17af435a55e511f19f75bcebf4cbb6d0f398ad277dfce2b6d8f8`

## Benchmark

[Control](https://github.com/auenkr/cadence-rust/actions/runs/45343383057) and [candidate](https://github.com/auenkr/cadence-rust/actions/runs/45343382918) use Rust Frontend, Matching and History with the Go Worker in the Rust arm. Both dispatches use 1,000 measured workflows and 100 warmup workflows per arm, concurrency 1/16/64, five repetitions and a 1,024-byte payload. Both use the current fat-LTO, one-codegen-unit build and the current instrumented harness. Each dispatch pairs an all-Go arm and its Rust arm on one runner.

The [historical reference](https://github.com/auenkr/cadence-rust/actions/runs/45291978847) passed strict artifact verification at `500dec50a9ccc2660ad6a32ecfa8aa12e262dd74`. Its warmup was 10, its release profile had LTO disabled and 16 codegen units, and it predates the benchmark instrumentation. The current control holds the source base, warmup and release profile constant.

Historical Rust-arm medians from the five repetitions:

| Concurrency | History CPU s per 1,000 workflows | Workflows/s | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 9.150 | 25.173 | 29.202 | 99.722 | 107.132 |
| 16 | 13.024 | 60.615 | 265.911 | 354.426 | 394.971 |
| 64 | 14.043 | 57.076 | 1281.291 | 1600.292 | 1719.125 |

The candidate and current control both passed strict artifact verification with zero failures in 30,000 measured workflows each. The candidate used an Intel Xeon Platinum 8370C. The historical reference and current control used AMD EPYC 7763 runners. All three used runner image `ubuntu26 20261116.202.1` and MySQL digest `sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b`.

Candidate Rust-arm medians from the five repetitions:

| Concurrency | History CPU s per 1,000 workflows | Workflows/s | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 8.149 | 25.113 | 25.147 | 100.470 | 104.723 |
| 16 | 9.621 | 70.680 | 226.476 | 304.017 | 336.834 |
| 64 | 11.164 | 62.880 | 1167.603 | 1427.723 | 1496.151 |

Current control Rust-arm medians from the five repetitions:

| Concurrency | History CPU s per 1,000 workflows | Workflows/s | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 7.861 | 24.769 | 27.999 | 99.551 | 102.323 |
| 16 | 11.307 | 62.565 | 259.051 | 347.125 | 375.686 |
| 64 | 12.352 | 57.180 | 1305.757 | 1522.053 | 1585.828 |

Candidate Rust-arm change versus the historical Rust arm:

| Concurrency | History CPU | Workflows/s | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | −10.9% | −0.2% | −13.9% | +0.8% | −2.2% |
| 16 | −26.1% | +16.6% | −14.8% | −14.2% | −14.7% |
| 64 | −20.5% | +10.2% | −8.9% | −10.8% | −13.0% |

Changes from current control to candidate. The ratio columns compare Rust ÷ Go using each run's own Go-arm median.

| Concurrency | Rust History CPU | Go History CPU | History CPU ratio | Rust workflows/s | Go workflows/s | Throughput ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | +3.7% | −19.3% | +28.4% | +1.4% | +8.3% | −6.4% |
| 16 | −14.9% | −18.4% | +4.2% | +13.0% | +13.1% | −0.1% |
| 64 | −9.6% | −9.4% | −0.2% | +10.0% | +9.4% | +0.5% |

At concurrency 1, Rust History CPU rose 3.7% and p99 rose 2.3% versus current control. The concurrent throughput gains closely tracked the all-Go arm. The CPU and tail latency comparison is inconclusive because the runner CPU changed, and the Go-arm ratios do not isolate the worker-count effect. The branch retains the patch for review as an experiment.

Each absolute metric is the median of the five per-repetition values. Extract workflow rate and latency from `paired-benchmark/result-*-rust-c*.json` and the History container's `cpu_seconds` from `paired-benchmark/resources-*-rust-c*.json` in the `frontend-matching-paired-benchmark` artifact. CI recorded matching warmup counts in the run logs and artifact metadata.

| Benchmark | `cadence-history` SHA-256 | `cadence-server` SHA-256 |
| --- | --- | --- |
| Historical reference | `f549c3c9eb835bcfc15e5586708839770f57c4e3fb0b3feb5c1fa74a48bdf472` | `3d39074f89e03fa5464e5257b34b82524c9b42d3c98ee7d80b810d1142ab7aeb` |
| Current control | `c075740b4fc75668a84aeae269708e20a60126b795c33e15ecd720b97516d771` | `3f2d061200487af9b9fea004da31ec54d4a6fc381a65fe43e471ea51189a192b` |
| Candidate | `aa63f0904da1ae2a5ee3194d3620b31ac86cfb542e147f09dc80cb68debd99ea` | `540f4f83bed4d296b22aa16811ddc219c11eda345e6b32b850db2acb8ed3cc69` |
