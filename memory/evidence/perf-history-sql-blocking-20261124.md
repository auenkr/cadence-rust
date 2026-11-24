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
| [MySQL History restart gate](https://github.com/auenkr/cadence-rust/actions/runs/45343382706) | Success, `GATE SUMMARY failed: []`; 4,500 workflows completed, zero failures in the 1,500 s fault scenario |

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

The [historical reference](https://github.com/auenkr/cadence-rust/actions/runs/45291978847) passed strict artifact verification at `500dec50a9ccc2660ad6a32ecfa8aa12e262dd74`. Its warmup was 10, its release profile had LTO disabled and 16 codegen units, and it predates the benchmark instrumentation. The matched control provides the comparison for the worker-count change.

Historical Rust-arm medians from the five repetitions:

| Concurrency | History CPU s per 1,000 workflows | Workflows/s | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 9.150 | 25.173 | 29.202 | 99.722 | 107.132 |
| 16 | 13.024 | 60.615 | 265.911 | 354.426 | 394.971 |
| 64 | 14.043 | 57.076 | 1281.291 | 1600.292 | 1719.125 |

The candidate's strict artifact verification passed with zero failed workflows across all 30,000 measured workflows. Its runner was an Intel Xeon Platinum 8370C and the historical run used an AMD EPYC 7763. Both used runner image `ubuntu26 20261116.202.1` and the same MySQL image digest.

Candidate Rust-arm medians from the five repetitions:

| Concurrency | History CPU s per 1,000 workflows | Workflows/s | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 8.149 | 25.113 | 25.147 | 100.470 | 104.723 |
| 16 | 9.621 | 70.680 | 226.476 | 304.017 | 336.834 |
| 64 | 11.164 | 62.880 | 1167.603 | 1427.723 | 1496.151 |

Candidate benchmark binary SHA-256 values:

- `cadence-history` `aa63f0904da1ae2a5ee3194d3620b31ac86cfb542e147f09dc80cb68debd99ea`
- `cadence-server` `540f4f83bed4d296b22aa16811ddc219c11eda345e6b32b850db2acb8ed3cc69`
