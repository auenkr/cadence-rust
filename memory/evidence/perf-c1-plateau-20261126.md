# perf-c1-plateau (2026-11-26) - source audit only, no fix, no CI run

Branch `perf-c1-plateau` (base origin/master). No product code change; no benchmark dispatched.

## Audit
- grep of rust/ for 100 ms constants on start->decision->complete path: matching native dispatch 100 ms fallback wake (same shape as Go matcher, enables Notify before claim), min_timeout test sleeps, ES visibility retry (not on path). No Rust-only 100 ms poll found.
- History transfer queue (v1 `transfer_queue_processor_base.rs`) and queue v2 `virtual_queue.rs::load_and_submit_tasks` match Go line by line (permit before slice check, same re-notify rules, one limiter per shard/category, 20 rps burst 20, 50 ms spacing).
- Existing traces (perf-c1-trace-20261125, perf-c1-burst-20261125) already attribute the plateau to ~50 ms limiter waits: two per slow workflow = ~100 ms. Rust waits on ~21% of permits vs Go ~1-2% because Rust runs ~10-15% more workflows/s with burstier per-shard arrivals (5% inter-arrival gaps <5 ms); not extra permits (2.93 vs 3.00 per workflow).
## Conclusion
No fixed 100 ms wait exists; the plateau is the shared default 20 rps limiter being hit more by the faster Rust path. Removing it would change Go-parity limiter behavior, so no change was made. Open lead not tested: why Rust shows sub-5 ms inter-arrival gaps (possible extra has_more_tasks re-notify on cached readers); needs a traced run comparing per-load task counts.
