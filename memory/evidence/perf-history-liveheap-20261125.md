# perf-history-liveheap (2026-11-25)

## Profiling
Added opt-in `heap-profile` feature to cadence-history (jemalloc `prof:true`, dump every 10s when `CADENCE_HEAP_PROFILE_DIR` set,
files named heap-<seq>-<allocatedMiB>.heap) plus benchmark input `heap_profile=1` (Dockerfile `HEAP_PROFILE` arg, paired-run.py copies
dumps + binary out of the container). Run 45511252648 (head 22da380e0, success) at c1+c64 produced 12 dumps (31 -> 120 MiB allocated).
The workflow's `jeprof` step could not resolve PIE addresses (output unsymbolized); I symbolized peak dump heap-0011-0120.heap by hand
(addr2line with load base = first mapping, leaf-first first cadence frame).

## Result (peak dump, 94 MB sampled in-use)
Attributed cadence frames (42.8 MB): `SyncConnectionPool::checkout` 33.6 MB (everything allocated below Conn::new, i.e. the mysql crate's
global BUFFER_POOL: default 128 buffers, each retained up to 4 MiB; Go's driver pools nothing), EngineExecutionContext::load_state 4.0,
Inner::write_event_to_cache 2.9, completed_run 0.6, rest <0.5 each. Mutable-state/event caches are NOT the main cost.

## Fix (168812c66)
`RUST_MYSQL_BUFFER_POOL_CAP=16`, `RUST_MYSQL_BUFFER_SIZE_CAP=32768` defaulted at start of cadence-history and cadence-server main
(explicit env still wins). rust-ci cadence-history 45518259870 success, cadence-server 45518263721 success.

## Verification
Three-arm benchmark 45518274418 (3 reps) was cancelled after 60 min, stuck in the Rust image build. Re-dispatched 45525993111
(1 rep, c1+c64, control 39aa8be14). Peak-memory numbers: see below if filled.
