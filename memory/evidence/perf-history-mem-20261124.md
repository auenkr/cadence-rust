# perf-history-mem (2026-11-24)

Findings (no code change needed):
- Tokio worker threads: default = available_parallelism (cgroup-quota aware on Linux); TOKIO_WORKER_THREADS override honored.
- Allocator: jemalloc already tuned (background_thread, dirty_decay_ms:1000, narenas:2) under feature `jemalloc-default`.
- Caches: Rust defaults history.cacheInitialSize=128, eventsCacheInitialSize=128, eventsCacheMaxSize=512 match Go constants.go (128/512); no Rust cache preallocates from them.

Change: cadence-history main.rs runtime `max_blocking_threads(64)` (default 512) and `thread_keep_alive(2s)` (default 10s) to bound retained blocking-thread stacks/arenas. Not a Go-visible behaviour change.

CI: see runs appended below (rust-ci package=cadence-history, history-mixed-gate).

## CI (head 9702f5dfa8dd53f87993746a2e97b375507c8bf9)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/45459989692 — success
- history-mixed-gate (jemalloc): https://github.com/auenkr/cadence-rust/actions/runs/45459997943 — success
- RSS delta not measured (benchmark not rerun).
