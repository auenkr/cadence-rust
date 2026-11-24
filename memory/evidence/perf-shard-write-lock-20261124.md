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
| Paired benchmark, code head | [45344205512](https://github.com/auenkr/cadence-rust/actions/runs/45344205512) | Pending |
| Paired benchmark, parent control | [45343784415](https://github.com/auenkr/cadence-rust/actions/runs/45343784415) | Pending |
| Paired benchmark, requested reference `500dec50a9ccc2660ad6a32ecfa8aa12e262dd74` | [45291978847](https://github.com/auenkr/cadence-rust/actions/runs/45291978847) | Success |

The enforced CI rustfmt checks passed. The informational workspace-wide formatting probe exited 1 on this branch and on the release-profile CI run already merged into its base. [Release-profile CI](https://github.com/auenkr/cadence-rust/actions/runs/45334522594)

### Rust binary SHA256

| Run | cadence-history | cadence-server |
|---|---|---|
| MySQL mixed | `bf9d5b9b96a411d1f27a9139486644640cd4f77998ea5b06214bef05342f044f` | `4e1f78e6e41361895b24287e7b4afe4701ed2977ee574e842ea7ea47c4321d4d` |
| Cassandra mixed | `ee802feb513a14059eb4d4cf0bcfd0d2944f12c2623314ad595f8a7500e1834a` | `a632f3270e4a5f510d9f274dc6f63cd79322f63c268d2cdde4d35134af6da8ef` |
| MySQL restart | `147855bc4b5fce88684afb268bf00ead5a4164ce55e4de93038d20ccff66815b` | `63e5cef9753f9caa80f7709d3040e9266dd4769394ab47cc2007a5389291b2b7` |
| MySQL swap | `abe73754ea2017a2917d2e152d0a0e08bc1d5189552b72d4e9d839cb5495e7ef` | `dec4a0574fa8bfa02b08e0f147cfab235733b753021eb814f34f4c70720fe472` |

## Benchmark

Both new runs use `rust_services=frontend,matching,history`, 1,000 measured workflows per arm per concurrency level, five repetitions, concurrency 1/16/64, and a 1,024-byte payload. Warmup is 100 workflows versus 10 in the requested reference. The requested reference also predates the fat LTO and single codegen unit release profile. The parent control uses the same warmup and release profile as the change. The system Worker is Go in both benchmark arms.

| Concurrency | Run | Go workflows/s | Rust workflows/s | Rust p95 ms | Rust p99 ms | Rust History CPU s / 1,000 workflows |
|---:|---|---:|---:|---:|---:|---:|
| 16 | Requested reference | 41.902 | 60.615 | 354.426 | 394.971 | 13.024 |
| 64 | Requested reference | 20.674 | 57.076 | 1600.292 | 1719.125 | 14.043 |
