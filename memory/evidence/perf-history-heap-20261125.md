# perf-history-heap (2026-11-25)

Added opt-in `CADENCE_MEM_REPORT=1` (cadence-history/src/mem_report.rs): every 10s prints `MEMREPORT` with jemalloc
allocated/active/resident/metadata/mapped/retained, Threads, VmRSS, RssAnon, threads by comm name. Set on the History
service in tests/frontend-gate/benchmark/paired-run.py (Go ignores it). First attempt (Benchmark/compose.yml only) did not
reach the frontend-matching workflow's compose; fixed in afaf00b0e.

Runs: rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/45501287714 (success);
benchmark with report https://github.com/auenkr/cadence-rust/actions/runs/45509163787 (head afaf00b0e59c676af43b43b6bbc024d57ea9a119, success, 2-arm go/rust).

## Stats (Rust History, MiB, successive 10s samples during c1->c64)
allocated 43.5, 61.4, 78.9, 81.0, 105.3, 108.9, 139.9; resident 46.3 -> 153.6; active ~= allocated+1..10;
metadata 2.3->4.4; retained 10-178 (virtual, not RSS). Threads 26-37 (tokio-rt-worker 14 -> 25 incl. blocking pool, 4-5 mysql
pool threads, 1 jemalloc bg thread).
Interpretation: resident ~= allocated (+2-5%), so fragmentation/retention is NOT the cause; no allocator tuning will help.
Thread count is modest (stacks are touched-lazily; ~25 threads ~ <2 MiB). Growth tracks live heap (workflow state/caches under load).

## Per-arm peak cgroup anon MiB (History), this run
| conc | go | rust |
|---|---|---|
| 1 | 74.1 | 75.0 |
| 16 | 91.1 | 91.1 |
| 64 | 105.6 | 101.4 |
Previous run 45488268567 showed rust +4..+10 MiB over Go; this run shows rust within ~+1/0/-4 MiB. Run-to-run noise (~+-5 MiB)
is comparable to the claimed gap, so no single Rust-only consumer was identified and NO behavioural/memory fix was made.
Three-arm control comparison not run (no code change to measure). Candidates for future work if a gap persists: live-heap owners
under load (mutable-state cache default 512 entries, queue task buffers), measured with the heap-profile feature in cadence-server.
