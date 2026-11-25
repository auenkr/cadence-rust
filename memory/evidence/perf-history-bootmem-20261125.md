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
