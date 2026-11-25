# History startup and idle allocations

Base `dc642b9f8d52a353f0737ca337a3a6a63e71491d` from `origin/master`. Scope excludes allocator tuning, persistence snapshots and EventCache keys.

- Go defaults both History and Events cache initial capacities to 128 and their maximum counts to 512. Rust exposes the same values in `config.rs`. The runtime maps and ordered indexes are constructed empty in `wiring/assembly/engine_cache.rs` and `events/mod.rs`. Applying the initial capacities to Rust would increase startup allocation.
- The host-level task scheduler is shared by all shards through `HistoryQueueExecProvider::shared`. Its 10,000-task queue limit and 200 async workers match Go defaults. Tokio MPSC allocates linked blocks as messages arrive, so the configured capacity is a backpressure limit. Individual queue wakeup channels have capacity one.
- Shard ack and read levels begin with entries for enabled clusters. The remaining failover and task maps start empty. Page-sized reservation in `queuev2/virtual_slice.rs` happens during reads. Startup shard controller scratch space is proportional to the number of concurrent acquisitions.
- Standalone History uses Tokio's available-parallelism default and honors `TOKIO_WORKER_THREADS`. The benchmark grants each service one CPU. The blocking pool already has a configurable cap, 64 by default, and 2-second idle expiry. See `perf-history-mem-20261124.md` for the earlier blocking pool change.
- TChannel allocates receive payloads after reading the frame header. Its response assembly buffer is allocated from the actual frame length. The History gRPC server uses the shared tonic builder with defaults. Reducing protocol windows or queue limits would change backpressure and requires a measured case.

## CI and measurements

| Run | Head | Purpose | Result |
|---|---|---|---|
| [45547843859](https://github.com/auenkr/cadence-rust/actions/runs/45547843859) | `dc642b9f8` | History Clippy and tests | Success |
| [45547848056](https://github.com/auenkr/cadence-rust/actions/runs/45547848056) | `dc642b9f8` | History mixed gate | Success, History binary SHA256 `60617c97e574592325bf2404f01d7227fdfdd97f0702cfca0382480c3a1d982a` |
| [45547790763](https://github.com/auenkr/cadence-rust/actions/runs/45547790763) | `dc642b9f8` | c1 heap profile, one repetition | Success |
| [45548657226](https://github.com/auenkr/cadence-rust/actions/runs/45548657226) | `dc642b9f8` | Three-arm c1/c16/c64, three repetitions | Success |

The diagnostic dispatch uses `heap_profile=1`. The paired-run harness enables the memory report whenever `BENCH_HEAP_PROFILE=1`, so it collects both allocator counters and live heap dumps even though the workflow has no separate memory-report input. The three-arm dispatch pins the control to master and uses identical Rust source for both Rust arms to measure benchmark noise.

## Synchronous MySQL framing buffers

The early `heap-0000-0031.heap` dump in [45511252648](https://github.com/auenkr/cadence-rust/actions/runs/45511252648) contains six leading 4 MiB stacks symbolized to `mysql::conn::Conn::connect_stream`. Their callers include operational config polling, queue task reads and visibility writes. The next sampled stack was about 0.04 MiB. This predates the current base. The same upstream driver code still runs on master. Its `MySyncFramed::new` allocates two `BytesMut` buffers at `DEFAULT_MAX_ALLOWED_PACKET` (4 MiB) as soon as a connection opens. The earlier buffer-pool environment changes govern a distinct allocation path and cannot change this constructor.

The current c1 diagnostic [45547790763](https://github.com/auenkr/cadence-rust/actions/runs/45547790763) reproduces this on the pinned master. History binary SHA256 `04b5d8135fda367a3c7af3fe9433a948381e3462df9dd0151eee2c139fbe56a9`. In the earliest dump (`heap-0000-0036.heap`), six 4 MiB stacks resolve to `Conn::connect_stream`, followed by a 0.17 MiB workflow state load stack. In the largest dump (`heap-0004-0099.heap`), six framing stack groups account for 32 MiB. The largest remaining stack is `EngineExecutionContext::load_state` (2.13 MiB), followed by three `write_event_to_cache` stacks (0.52–0.54 MiB each). Sizes are raw sampled live bytes per stack group. Symbolization subtracts the ELF image base from recorded return addresses before running `addr2line -fC`. The memory report counted 28–29 threads, including 15 under Tokio's shared async/blocking name and 4–5 under the MySQL domain-runtime name. Profiled cgroup peaks were 70.4 MiB for Go and 93.8 MiB for Rust. The separate unprofiled benchmark is the memory and throughput comparison.

Candidate `d263b752ae926d051ea5a22267be2d6c22c74ac3` vendors the existing synchronous mysql 26.0.1 driver and uses its existing framing constructor with two 4 KiB buffers, using the initial size from the [Go MySQL driver](https://github.com/go-sql-driver/mysql/blob/v1.9.3/buffer.go). Buffer growth, SSL upgrade, and packet limit negotiation use the existing code. Allocation capacity is distinct from anonymous resident bytes, so the 8 MiB per-connection reservation cannot be used to estimate the measured cgroup difference.

| Run | Purpose | Result |
|---|---|---|
| [45550041086](https://github.com/auenkr/cadence-rust/actions/runs/45550041086) | History Clippy and tests, clean target | Success, 953 History tests passed |
| [45550104677](https://github.com/auenkr/cadence-rust/actions/runs/45550104677) | Workspace Clippy and tests, clean target | Success |
| [45550060945](https://github.com/auenkr/cadence-rust/actions/runs/45550060945) | MySQL mixed gate, two History hosts, jemalloc, clean target | Success, no failed workflow checks. History SHA256 `58df5bee1cd03af90b0e366cae87cdde28fe524847efa32cbf259db2f5910ad2` |
| [45550044918](https://github.com/auenkr/cadence-rust/actions/runs/45550044918) | Three-arm c1/c16/c64, three repetitions | Pending |

## Identical-binary benchmark variation

Run [45548657226](https://github.com/auenkr/cadence-rust/actions/runs/45548657226) used History SHA256 `e921fb4503aa784e38bb3ba6b5738e33e74e5f9bd88a840e3ab9eeab81103883` for both Rust arms with zero workflow failures across 27,000 workflows. Memory is the median across three per-run `peak_cgroup_anon_bytes` values for the History container.

| Concurrency | Go anon MiB | Rust control anon MiB | Identical Rust binary anon MiB | Paired throughput ratio median [min, max] |
|---:|---:|---:|---:|---:|
| 1 | 72.68 | 77.72 | 77.04 | 0.996 [0.968, 1.029] |
| 16 | 92.36 | 88.14 | 89.19 | 0.974 [0.969, 0.998] |
| 64 | 102.98 | 99.20 | 103.12 | 1.002 [0.993, 1.017] |

Between identical binaries, c1 peak-anon differences per pair ranged from −1.08 to +3.15 MiB. The A/A run also produced a 2.6% c16 throughput shift in the median paired ratio.

## Three-arm result for 4 KiB framing buffers

Run [45550044918](https://github.com/auenkr/cadence-rust/actions/runs/45550044918), success, source head `d263b752a`, control `dc642b9f8` (master). 27,000 workflows, 0 failed. Rust History SHA256 `e70f47f0af3a4d11459b63d6dc83ae53730c3203ea8d5306687836d6d81014eb`; cadence-server `4b8c1cd73a206d53957a974b4b78d0af259af8ddb4a6a47833b7fb39daacc07f`. Runner: AMD EPYC 9V74, 4 vCPU, 15 GiB RAM, Ubuntu 26.04.1, kernel 7.0.0-1020-azure, mysql:8.0; services 1 CPU each, History limit 1.8 GiB. Memory is History `peak_cgroup_anon_bytes`; each cell is the median across three repetitions, followed by the individual runs.

| c | arm | History anon MiB (med; runs) | wf/s (med; runs) | p50 ms | p95 ms | p99 ms | failed |
|--:|---|---|---|---|---|---|--:|
| 1 | go | 73.48; 71.8, 76.2, 73.5 | 18.8; 22.2, 18.5, 18.8 | 36.78 | 141.53 | 309.66 | 0 |
| 1 | control | 75.18; 76.2, 75.2, 73.2 | 23.7; 24.8, 22.5, 23.7 | 24.79 | 113.96 | 218.50 | 0 |
| 1 | rust | 69.49; 69.5, 72.2, 67.3 | 23.7; 23.7, 24.7, 21.2 | 22.83 | 106.78 | 204.00 | 0 |
| 16 | go | 93.92; 93.5, 93.9, 97.7 | 40.9; 41.3, 40.9, 40.7 | 352.59 | 714.91 | 1083.94 | 0 |
| 16 | control | 88.91; 89.4, 84.3, 88.9 | 53.1; 66.5, 50.3, 53.1 | 246.98 | 558.32 | 754.51 | 0 |
| 16 | rust | 88.76; 85.0, 88.8, 88.9 | 63.3; 57.0, 63.3, 68.5 | 204.58 | 526.49 | 755.16 | 0 |
| 64 | go | 101.06; 98.1, 103.4, 101.1 | 20.5; 19.1, 20.5, 21.6 | 3070.94 | 5698.31 | 5969.88 | 0 |
| 64 | control | 101.62; 95.5, 101.6, 121.7 | 59.6; 64.9, 53.1, 59.6 | 1072.67 | 1750.55 | 1897.51 | 0 |
| 64 | rust | 111.18; 111.2, 112.3, 99.6 | 62.2; 63.2, 62.1, 62.2 | 1054.28 | 1812.03 | 2466.84 | 0 |

- c1: Rust History is 69.5 MiB, against Go 73.5 and control 75.2. That is 4.0 MiB below Go and 5.7 MiB below control, which is outside the −1 to +3 MiB A/A range. c1 throughput matches control at 23.7 wf/s, and latency is equal or slightly lower.
- c16: memory matches control (88.8 against 88.9 MiB). Throughput is 63.3 against 53.1 wf/s, but the control runs vary from 50.3 to 66.5, so this is noise.
- c64: memory is 111.2 MiB against control 101.6 and Go 101.1. The three runs are 111.2, 112.3 and 99.6, and control had a 121.7 outlier, so the c64 comparison is inconclusive. The patch removes a fixed per-connection reservation and should not raise memory. Throughput is within noise (62.2 against 59.6). p99 is higher (2467 against 1898 ms), which is within the spread seen across runs.
- Conclusion: c1 History memory is now below Go with no throughput regression. A c64 memory increase is not ruled out. Only one three-repetition run exists, so a repeat would be needed to settle it.
