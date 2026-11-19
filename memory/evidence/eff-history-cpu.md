# eff-history-cpu (2026-11-19) — PAUSED, before/after incomplete

Branch `eff-history-cpu` from master 56b58c6c1. Code commit f742f5c171503bbe7f6edb2f3504730355a81d61.

## Profiles (master, 25/s, profile=1)
History lane https://github.com/auenkr/cadence-rust/actions/runs/44427016079 (success);
all-Rust https://github.com/auenkr/cadence-rust/actions/runs/44427020610 (success).
History: kernel ~45% of samples (MySQL net path). User space diffuse: mysql client (query/execute/
named_params parse ~9%, other worker's area), h2 server framing 4.7%, MetricsRegistry::add ~1.5%
(already optimised), MatchingDomainCache::by_id deep clone of MatchingDomainMetadata 1.1%
(callers: EngineExecutionContext::load_state/new, task_allocator, verify_task_version,
get_domain_name), FileDynamicClient::get_value 0.6% (to_lowercase alloc + hashing).

## Fixes (no behaviour change)
- cadence-matching matching_domain_cache: CacheState.by_id stores Arc<MatchingDomainMetadata>;
  new get_domain_by_id_shared (same read-through); get_domain_name / cached_domain_name /
  exists-check no longer deep-clone. Unit test shared_lookup_reuses_entry_until_refresh_publishes_new_snapshot.
- cadence-history wiring/resources/domain_cache.rs: memoize engine/task DomainCacheEntry per id,
  invalidated by Arc::ptr_eq when refresh publishes a new snapshot Arc.
- dynconfig FileDynamicClient::get_value: skip to_lowercase alloc for already-lowercase keys.

## Validation
- rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/44435772082 success
- rust-ci cadence-matching https://github.com/auenkr/cadence-rust/actions/runs/44435777616 success
- history-mixed-gate mysql/grpc two_history=true https://github.com/auenkr/cadence-rust/actions/runs/44439068463
  success, `GATE SUMMARY failed: []`
- Benchmark after (branch, 25/s, 120s, 3 pairs) https://github.com/auenkr/cadence-rust/actions/runs/44435828123 success (not yet analysed)
- Benchmark before (master 56b58c6c1 full SHA) https://github.com/auenkr/cadence-rust/actions/runs/44439042842 in progress at pause
  (first attempt 44435824291 failed: source_ref must be a full SHA).

## Next steps
1. Download both benchmark artifacts, tabulate History CPU s / RSS / p50 / p99 per pair.
2. Expected gain small (<few %): Rust History CPU already ~0.5x Go; remainder is DB round trips.
3. Optional further: metrics observe() double key build/lock; mysql named-params parse per exec (SQL worker).
