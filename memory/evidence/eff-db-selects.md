# eff-db-selects: extra History SELECTs vs Go (2026-11-19)

Branch `eff-db-selects`, head `2fe8d08fb3e3aed70c189e7bf9d98ce38be6de44` (base master `6c7ce1f5ad17fc12d6caeb0c920b7ea1e533b9d2`).

## Attribution (MySQL general log, `sql_log_secs`, Benchmark/collect_resources.py)
Runs 44473947003 (history), 44473951208 (all), 15/s. Rust extra SELECTs per completed workflow vs Go:
| source | Rust | Go | Go mechanism |
|---|---|---|---|
| `transfer_tasks` range read | ~3.0 | 0 | queue v2 + cached queue reader (`enableTransferQueueV2`=true, `transferProcessorCachedQueueReaderMode`=enabled); new tasks injected via NotifyNewTask |
| `timer_tasks` range read | ~2.0 | 0 | same, timer queue v2 |
| `history_node` read | ~4.4 | ~2.2 | shard events cache (`writeEventToCache`, `eventsCache.GetEvent`) |
Mutable-state loads, shard range lock, current_executions reads: equal.

## Fix (this branch)
Shard events cache wired (Go parity): `events/mod.rs` (`ShardEventsCache`, `get_cached`), `engine_events.rs` (write Started / close / ActivityTaskScheduled / StartChildWorkflowExecutionInitiated events to cache; start/completion/activity-scheduled getters read cache first, populate on DB read), `engine_factory.rs` (one cache per shard, `events_cache_max_count`/`events_cache_ttl`, Go defaults 512/1h). Tests: `events_cache_tests` (3).
rust-ci cadence-history: run 44475858012, success.

## Before/after (3 alternating pairs, 15/s, 30s measure, sql_log 25s); Rust numbers, per workflow
| pair | svc | ref | Rust sel/wf | Rust history_node/wf | Rust p50/p99 ms | hist CPU Rust-Go |
|---|---|---|---|---|---|---|
| 1 | history | before 44476056113 | 34.6 | 4.55 | 39.4/55.3 | -37.0% |
| 1 | history | after 44476060504 | 30.9 | 3.23 | 36.6/44.2 | -30.4% |
| 1 | all | before 44476064877 | 30.7 | 4.46 | 43.2/71.5 | -51.6% |
| 1 | all | after 44476068942 | 32.7 | 2.75 | 30.4/44.1 | -47.2% |
| 2 | history | before 44476072614 | 22.0* | 4.32 | 40.2/70.5 | -45.7% |
| 2 | history | after 44476076226 | 33.0 | 2.99 | 32.9/41.4 | -39.9% |
| 2 | all | before 44476079874 | 34.4 | 4.47 | 44.0/64.3 | -55.9% |
| 2 | all | after 44476084166 | 29.9 | 2.98 | 30.2/36.9 | -47.6% |
| 3 | history | before 44476089493 | 30.4 | 4.17 | 32.7/41.0 | -41.5% |
| 3 | history | after 44476093844 | 32.7 | 2.67 | 32.6/40.0 | -40.2% |
| 3 | all | before 44476097858 | 31.1 | 4.35 | 42.8/465.5 | n/a |
| 3 | all | after 44476101657 | 29.9 | 2.87 | 34.8/44.0 | -39.0% |
Go history_node/wf in the same runs: 2.06-2.30. Rust transfer/timer task reads unchanged (2.98/1.99).
*Total sel/wf is noisy: many Execute rows log binary args and normalize to unclassifiable text; per-table counts (history_node, transfer/timer) are reliable.
Result: history_node reads 4.39 -> 2.92 avg (Go 2.2). Remaining gap ~0.7 (other getters, e.g. child-initiated/signal paths, not yet cache-first).
Rust History CPU is still below Go in every run; whether it changed from before to after is not clear (run-to-run noise is large). p50/p99 are equal or better.

## Not fixed (follow-up): queue v2 + cached reader (~5 SELECT/wf)
Rust `wiring/assembly/queues.rs::build_queue_processors` always builds v1 processors; `enable_*_queue_v2` is not read. Parity needs `queuev2::{transfer,timer}_queue_factory` wiring (adapters exist in `shard_adapters.rs`) with cached reader mode. Deferred: large change, v2 queue-state persistence was flagged lossy (history-crossreview-opus-20261113 item 12) and v2 has little gate coverage.

## Gates (branch head 2fe8d08fb)
| gate | run | result |
|---|---|---|
| history-mixed-gate mysql/grpc two_history | 44476119714 | success; `GATE SUMMARY failed: []` |
| history-mixed-gate postgres/tchannel two_history | 44476124287 | success; `GATE SUMMARY failed: []` |
