# eff-history-cpu (2026-11-19)

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

## Before/after (History lane, 25 starts/s, 30 s warmup, 120 s, 3 alternating pairs)
Before = master 56b58c6c1 (full SHA source_ref) https://github.com/auenkr/cadence-rust/actions/runs/44468110967 (success)
  (earlier baseline 44439042842 invalid: Go generator exit 1, Rust p50 4-23 s, overloaded runner).
After = branch code https://github.com/auenkr/cadence-rust/actions/runs/44435828123 (success).
History container CPU seconds / peak summed RSS / completion latency ms (correct measured samples, 3000 each).

| arm | before CPU s | before RSS MiB | before p50 | before p99 | after CPU s | after RSS MiB | after p50 | after p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 00-rust | 43.4 | 134 | 59.5 | 221.9 | 26.1 | 132 | 32.9 | 202.2 |
| 00-go | 85.7 | 191 | 122.4 | 21076.1 | 42.8 | 168 | 39.4 | 113.0 |
| 01-rust | 43.7 | 136 | 59.5 | 212.4 | 26.4 | 136 | 32.8 | 197.0 |
| 01-go | 86.1 | 195 | 118.0 | 23759.4 | 43.8 | 170 | 40.0 | 115.2 |
| 02-rust | 43.4 | 133 | 58.8 | 216.8 | 26.1 | 136 | 32.8 | 201.1 |
| 02-go | 87.5 | 198 | 162.9 | 23360.3 | 43.3 | 170 | 39.5 | 116.6 |

Interpretation: absolute numbers differ by runner — the unchanged Go arm also halved (85-87 s -> 43 s) and
the before-run Go arms were saturated (p99 21-24 s), so before/after absolute deltas are runner variance,
not this change. Within-run Rust/Go CPU ratio: before 0.50-0.51 (Go saturated, not comparable),
after 0.60-0.61. Result: no measurable CPU gain attributable to the fix (domain-cache clone was ~1% of samples,
below runner noise). Rust History stays below Go on CPU and RSS; p99 tail (~200 ms vs Go ~115 ms) unchanged.
Next ideas: tail latency (not CPU) is the real gap; mysql named-params parse per exec (SQL worker).
