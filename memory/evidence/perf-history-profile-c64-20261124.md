# Rust History CPU profile at concurrency 64 (2026-11-24)

Measurement only; no service code changed. Harness change: `profile=1` input added to `frontend-matching-benchmark.yml`, an opt-in `perf record` hook in `tests/frontend-gate/benchmark/paired-run.py` (rust arm, History container PID tree, via `Benchmark/profile.py`) and a `PROFILE` build arg in `rust/Dockerfile` (frame pointers and unstripped symbols, same fat LTO).

- Run (success): https://github.com/auenkr/cadence-rust/actions/runs/45391986883
- Dispatch: `rust_services=frontend,matching,history concurrency=64 warmup=100 requests=1000 repetitions=1 profile=1`; branch head at dispatch `3760b1ed92c57e35269aa0c5b4c2c00a069f7446`.
- First attempt, run 45378359952, used `line-tables-only` and lost the runner during the fat-LTO build, so it was dropped.
- Rust binaries: cadence-history `d04137ff582deb4642b3c557efab50993e2da59a414651320735de5b38455c13`, cadence-server `3d7869a23efafd98cf2c523af3fbbfb8436b350d7c22bf362f686af955235c20`.
- Runner: 4 vCPU, 15 GiB. History container limit 1 CPU / 1400 MiB. The Rust arm completed 1000/1000 workflows at 65.9 workflows/s (profiling on, so not a benchmark number).
- Sample: 840 samples at 99 Hz over the 15.2 s measurement window = 8.48 CPU-s, i.e. History averaged 0.56 cores. Resolution is about ±3 percentage points per bucket. Raw top lists: `perf-history-profile-c64-20261124-top.txt`.

## Buckets (each sample counted once, leaf-first classification)

| Bucket | % of History CPU |
|---|---:|
| SQL driver / serialization (mysql crate, `SqlBackend`, row decode, stmt meta) | 49.3 |
| Syscalls/IO (kernel TCP send/recv, softirq, scheduler return after socket wait) | 39.3 |
| Allocator (jemalloc malloc, free, realloc) | 7.9 |
| Futex / thread switching | 2.0 |
| memcpy/memcmp/other | 1.1 |
| tokio scheduler | 0.5 |
| Thrift/proto codec | ~3 incl., mostly inside SQL/h2 frames |
| History engine logic (own frames) | under 1 self; about 34 incl. (`update_as_active`/`persist`, which is mostly SQL below it) |

Reading: about 88% of CPU is below a SQL call. Kernel network send for MySQL statements (`inet_sendmsg` 15.5%, `__sys_sendto` 12.6%) is the largest single block. `mysql::Conn::exec_iter` is 18.5% inclusive.

## block_in_place / thread handoff (`history_sql_backend.rs:49` and `:322`)

- 88.2% of samples sit under a `BlockingTask<block_in_place…>`. That is only where tokio moved the worker's run loop to a blocking-pool thread, not a cost. The wrapper frames are not work.
- Handoff machinery: any futex frame 2.0% (wake 1.3%, wait 0.7%), `Spawner::spawn_task` 0.24%, tokio scheduler 0.5%. **Handoff cost is about 2–3% of History CPU.**
- Upper bound: `finish_task_switch` is 5.2% self / 6.0% incl., but most of it is the return from a blocking MySQL socket read, which async would also pay. So the upper bound is about 8%, the likely value 3%.
- Converting SQL calls to async would therefore save at most about 3% CPU, not the 10%+ that would justify the risk. Its value, if any, is latency and threads, not CPU.

## Top 25 self frames (% of samples)

memcpy 7.50; finish_task_switch 5.24; _raw_spin_unlock_irqrestore 4.05; handle_softirqs 2.98; _rjem_malloc 1.31; memcmp 1.31; kernel.kallsyms 0.95; _rjem_sdallocx 0.95; _copy_to_iter 0.71; trace_syscall_enter 0.60; LenEnc::deserialize 0.60; Arc<[Column]>::drop_slow 0.60; HistoryEvent::clone 0.60; ResultSetInfo::into_statement_meta 0.60; __nf_conntrack_find_get 0.60; do_rallocx 0.60; _raw_spin_lock_bh 0.48; nf_nat_inet_fn 0.48; _raw_spin_unlock_irq 0.48; EngineExecutionCache::acquire_in 0.48; sip13 Hasher::write 0.48; Notified::poll_notified 0.48; batch_semaphore::Acquire::poll 0.48; MetricsRegistry::add 0.48; kmem_cache_free 0.48.

## Top inclusive frames (excluding the tokio/hyper/tonic wrapper chain)

syscall entry 39.05; `x64_sys_call` 36.3; `EngineExecutionContext::update_as_active` 33.7; `::persist` 33.2; `write_update` 20.1; `SqlBackend::transaction` 19.6; `mysql::Conn::exec_iter` 18.5; `update_workflow_execution` 18.1 (rate limited / SQL store); `respond_decision_task_completed` 16.9; `record_decision_task_started` 14.9; `inet_sendmsg` 15.5; `handle_softirqs` 13.9; `append_history_nodes` 14.3; `tcp_sendmsg` 13.7; `SqlWorkflowStore::update` 13.5; `prepare_update` 12.7; `__sys_sendto` 12.6; `Conn::write_packet` 12.3; `Conn::_execute` 11.8. HTTP/2 + tonic handling 60 (wrapper only).

## Comparison with the 2026-11-18 flamegraph (`perf-history-flamegraph.md`)

The old profile was open-loop 25 starts/s on the pre-LTO build; this one is closed-loop c64 with fat LTO, cache index, shard write lock and jemalloc tuning. Shares are of each profile's own samples.

| Item | Old | Now |
|---|---:|---:|
| Syscalls inclusive | 38 | 39 |
| `Conn::exec_iter` inclusive | 22 | 18.5 |
| `SqlBackend::transaction` | 21 | 19.6 |
| `update_as_active` / persist | 24 | 33.7 |
| memcpy self | 7.5 | 7.5 |
| memcmp self | 2.1 | 1.3 |
| finish_task_switch self | 4.7 | 5.2 |
| spin_unlock_irqrestore self | 4.7 | 4.1 |
| Allocator | about 4.6 self | 7.9 incl. |
| Metrics registry | 5.3 incl. | under 1 (`MetricsRegistry::add` 0.5 self) |

Shape unchanged: MySQL round trips through the kernel network path dominate. The metrics fix held. The persist share rose because less time goes to everything else at c64 (decision-heavy workload).

Oddity: `__fopen_rb_ca` and `trace_syscall_enter` under History (under 1%) suggest a `/proc` read per tick (the process-metrics collector); worth a look but small.

## Recommendations (ranked)

1. **Cut MySQL round trips and kernel send cost per transaction.** Pipeline or batch statements that already run in the same transaction (history node insert plus execution update plus queue-task inserts) and keep the prepared-statement cache hot (`into_statement_meta`, `Arc<[Column]>::drop_slow`, `LenEnc::deserialize` are visible). Expected gain: 10–15% of History CPU (the 39% syscall bucket shrinks roughly in proportion to statements). Go-parity risk: medium. SQL results and isolation must be identical and statement ordering, row locks and error mapping for `ConditionFailed` must be unchanged. Gate with the persistence parity tests and a mixed MySQL gate.
2. **Remove copies in the row and event path.** `memcpy` is 7.5% self, with `HistoryEvent::clone` and row decode into owned buffers. Use `Bytes` or `Arc` for blobs and avoid cloning events when appending nodes. Expected gain: 4–6%. Go-parity risk: low (internal, no behavior change). Also trim jemalloc traffic (7.9%) as a side effect.
3. **Do not convert the SQL calls to async for CPU.** Handoff is about 2–3% (upper bound 8%), so the conversion is not worth it on this evidence. If done anyway for latency or thread count, expect under 3% CPU. Go-parity risk: medium-high, because a rewrite of the `transaction` boundary (`history_sql_backend.rs:49`, `:322`) touches commit/rollback semantics. Prefer a cheaper alternative: fix the sip13 hashing and `MetricsRegistry::add` costs (about 1% combined, risk low).

Limits: 840 samples from one repetition, so treat percentages below 1% as noise. perf attribution inside the kernel TCP stack under a hypervisor (`hyperv_callback` shows up) may be inflated for network syscalls.
