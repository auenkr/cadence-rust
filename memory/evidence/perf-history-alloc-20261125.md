# perf-history-alloc (2026-11-25)

Allocator: History uses tikv-jemallocator (feature jemalloc-default, used by Benchmark/Dockerfile). Previous conf: background_thread:true,dirty_decay_ms:1000,narenas:2.
Tried (head 5ee2c38c57e5aabfa859a8084a6b9af16907ad65): background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:1,tcache_max:4096.
No large with_capacity/channel preallocations found in cadence-history (only Notifier chan 1000).

Benchmark run: https://github.com/auenkr/cadence-rust/actions/runs/45488268567 (control = master 1693a0222, source = 5ee2c38c5). Median peak cgroup anon MiB (History):
| conc | go | control(master) | candidate |
|---|---|---|---|
| 1 | 73.3 | 81.6 | 79.7 |
| 16 | 92.0 | 97.8 | 96.1 |
| 64 | 103.4 | 113.2 | 113.6 |
Throughput cand/control: 1.008, 0.991, 1.008 (within noise); p50 latency ratio 0.99/0.99/0.97.
Result: ~2 MiB saving at c1/c16, none at c64; Rust still above Go at all concurrencies (+6.4/+4.1/+10.2). Goal NOT met; allocator conf is not the dominant source of the gap.
rust-ci cadence-history: run 45488264383 (see gh). History mixed gate not run (change not goal-meeting).
