# eff-matching-v2 (2026-11-20/21)
Branch eff-matching-v2 from origin/master 119df57fc. Change commit 10c29bcb8f3ee054f872a891c40d9ebdcad912d3.

## Profile (run 44785418793, master 119df57, matching, 25/s, 60s, 2 pairs, profile=1; Rust arm folded stacks merged, 2 files)
Rust Matching samples are diffuse (no single hotspot). Percent of all samples.

Top 15 self time: memcpy 7.82; sip Hasher::write 0.81; jemalloc sdallocx 0.77; __rust_dealloc 0.69; __memcg_slab_post_alloc_hook 0.62;
RawVec<EventTypeV2> drop 0.59 (symbol aliasing); __raw_spin_lock_irqsave 0.58; BytesMut::put_slice 0.57; Mutex<h2 Inner>::lock 0.56;
__inet_lookup_established 0.54; memcmp 0.48; rb_erase 0.47; h2 decode_frame 0.46; tokio park_internal 0.45; hashbrown RawTable 0.45.

Top inclusive (app frames): gRPC server H2Stream 26.4; RawStatusService (matching_native_status) 21.0; MatchingApiServer call 11.7;
NativeTaskDispatcher poll_with_lifecycle 11.6; poll_for_decision_task 10.8; add_decision_task 7.4-7.5; NativeMatchingService 8.7/7.1;
delivery_loop 7.9; task-local header BTreeMap futures 7.2/6.2; MatchingHistoryClient::retry (RecordDecisionTaskStarted) 6.0;
NativeMatchingEngine::add_task 5.5; RingpopNode swim/ping 3.1-3.4; NativeQueueRegistry::get_with_deadline 3.3;
owner_work (spawn_blocking per lookup) 1.8-2.4; HistogramTagSink::record 2.0; GoRollupSink::record 1.6; MigrationMetricSink::record dealloc 0.5.
Syscall/TCP write ~27 (kernel, not reducible in Matching code).
memcpy callers: poll_for_decision_task 1.26, RawStatusService 0.61, engine poll closure 0.60, service 0.59, History client retry 0.58.

## Change
`matching_registry_native.rs`: `get_with_deadline` now tries `get_cached_fast` (registry state lock, not stopped, owner exists and not stopped,
ownership check ok -> handle) before `owner_work`/`spawn_blocking`. Same result as `get_blocking`'s cached branch; anything else (stopped,
missing owner, ownership error) falls through to the unchanged slow path. No persistence, timing or Go-visible change.
Not done (parity/risk or <1% each): metrics event tag allocation, task-local header BTreeMap, ringpop ping.

## Validation (all success)
- rust-ci cadence-matching: https://github.com/auenkr/cadence-rust/actions/runs/44789758141
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44791099830
- Matching mixed gate: https://github.com/auenkr/cadence-rust/actions/runs/44791105310
- frontend-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44791111111 (`GATE SUMMARY failed: []`)

## Benchmark (matching, 25/s, 60s, 3 pairs, mysql_tmpfs, sql_log_secs=60)
The benchmark workflow takes one source_ref, so before/after were dispatched back to back, but they landed on DIFFERENT CPU models again.
Matching container CPU seconds (mean of 3 pairs):
| run | code | runner | Go CPU s | Rust CPU s | Rust/Go | Go p99 ms | Rust p99 ms |
|---|---|---|---|---|---|---|---|
| 44789763436 | before (119df57) | Xeon 8370C | 5.56 | 5.56 | 1.00 | 163-579 | 157-191 |
| 44789769010 | after (10c29bc) | Xeon 6975P-C | 3.82 | 2.74 | 0.72 | 106-111 | 104-108 |
Rust/Go is normalised inside each run, but the runners differ by a CPU generation, so the 1.00 -> 0.72 change is NOT a proven effect of the code change.
Honest verdict: the change is correct and removes a blocking-pool hop per lookup (~2% inclusive in the profile), but a same-runner A/B is still missing.
Rerun pairs until both land on the same CPU model before claiming a gain.
