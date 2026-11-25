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
| [45547848056](https://github.com/auenkr/cadence-rust/actions/runs/45547848056) | `dc642b9f8` | History mixed gate | Pending |
| [45547790763](https://github.com/auenkr/cadence-rust/actions/runs/45547790763) | `dc642b9f8` | c1 heap profile, one repetition | Pending |
| [45548657226](https://github.com/auenkr/cadence-rust/actions/runs/45548657226) | `dc642b9f8` | Three-arm c1/c16/c64, three repetitions | Pending |

The diagnostic dispatch uses `heap_profile=1`. The paired-run harness enables the memory report whenever `BENCH_HEAP_PROFILE=1`, so it collects both allocator counters and live heap dumps even though the workflow has no separate memory-report input. The three-arm dispatch pins the control to master and uses identical Rust source for both Rust arms to measure benchmark noise.
