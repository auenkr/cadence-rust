# History execution cache: indexed access order (perf-history-cache-index)

Code head: `ee68bce892e2c61572020757ed1a528babd924bb` (`engine_cache.rs` only). Base: origin/master 4e42a6750.

## Change
`rust/crates/cadence-history/src/wiring/assembly/engine_cache.rs`: the cache map is now `CacheMap` with two `BTreeMap` indexes
(`by_created` keyed `(created, seq)`, `by_access` keyed `(last_access, seq)`) beside the `HashMap`.
- Expiry: walks `by_created` only while `now > created + ttl` (ordered, stops at the first live entry); pinned entries (lock Arc strong_count > 1) are skipped. ttl==0 disables expiry.
- Eviction: first unpinned entry in `by_access` (least recently accessed; ties by insertion/touch seq) instead of a full `min_by_key` scan.
- Update-on-access: `touch()` re-keys the access index on hit (previously an AtomicI64 store).
- Semantics kept: TTL, capacity (`max(1)`), pin refcount, "capacity fully occupied with pinned elements" error, `BaseCache*` metrics, release-after-error path untouched.
Tests added (`evict_tests`): eviction order and touch, tie-break, pinned skipped (evict+expiry), TTL boundary and ttl=0, all-pinned-full fails then recovers, release after error unpins and frees capacity; existing real-context capacity/TTL test kept.
Note: first push failed clippy (`clear` dead code); fixed by `#[cfg(test)]` (ee68bce89).

## CI evidence (all on head ee68bce89)
| Check | Run | Result |
|---|---|---|
| rust-ci package=cadence-history (clippy + tests) | https://github.com/auenkr/cadence-rust/actions/runs/45335166594 | success |
| History mixed gate mysql, two_history=true | https://github.com/auenkr/cadence-rust/actions/runs/45335170483 | success |
| History mixed gate cassandra, two_history=true | https://github.com/auenkr/cadence-rust/actions/runs/45335175187 | success |
| History restart gate mysql (1st) | https://github.com/auenkr/cadence-rust/actions/runs/45335178976 | FAILED: `GATE SUMMARY failed: ['soak-driver-exit','zero-failed-workflows','all-started-completed']`, 1 of 4500 workflows failed, all s1-s4 scenarios ran; log shows "no matching hosts in the membership ring" task retries during faults |
| History restart gate mysql (rerun, same branch) | https://github.com/auenkr/cadence-rust/actions/runs/45340870941 | success |
| History restart gate mysql (master baseline) | https://github.com/auenkr/cadence-rust/actions/runs/45340876454 | success |
| Paired benchmark rust_services=frontend,matching,history | https://github.com/auenkr/cadence-rust/actions/runs/45335182651 | success, strict verify exit 0 |

The first restart-gate failure was not reproduced on the rerun (passed) and master also passed; one failed workflow out of 4500 during kill-9/outage faults. Not proven unrelated to the change, but the cache is not involved in ring membership; treat as an intermittent flake and watch.
Rust binaries (benchmark): cadence-server sha256 `3d39074f89e03fa5464e5257b34b82524c9b42d3c98ee7d80b810d1142ab7aeb` (unchanged vs baseline), cadence-history `ec0a0fe3c8c8302a18c0a7294361a1f9d5cc82d5879a527fae747eac2cd66f6f` (baseline `f549c3c9eb83…`).

## Benchmark vs baseline run 45291978847 (head 500dec50a, inputs identical: defaults + rust_services=frontend,matching,history)
CAUTION: runner CPU differs (baseline AMD EPYC 7763, new EPYC 9V45), so absolute numbers are not comparable across runs; the Go arm in the same run is the control.

| Conc | Baseline Go -> Rust wf/s | New Go -> Rust wf/s | Rust/Go ratio base -> new |
|---:|---|---|---|
| 1 | 22.67 -> 25.17 | 25.34 -> 26.42 | 1.11 -> 1.04 |
| 16 | 41.90 -> 60.62 | 75.09 -> 103.88 | 1.45 -> 1.38 |
| 64 | 20.67 -> 57.08 | 31.41 -> 69.89 | 2.76 -> 2.23 |

History CPU s/run (Go -> Rust) and Rust/Go ratio:
| Conc | Baseline | New |
|---:|---|---|
| 1 | 22.38 -> 9.15 (0.41) | 11.78 -> 5.47 (0.46) |
| 16 | 20.22 -> 13.02 (0.64) | 10.39 -> 7.67 (0.74) |
| 64 | 46.02 -> 14.04 (0.31) | 30.55 -> 9.41 (0.31) |

Conclusion: no measurable History CPU or throughput gain attributable to this change; ratios move within the noise of a different runner CPU (c1/c16 slightly worse ratio, c64 unchanged). The workload has few cached workflows (cache never near capacity), so the full scans were already cheap; the index is a complexity fix (O(log n) vs O(n) per miss) that matters only at large cache sizes. A same-runner A/B would be needed to resolve differences under ~10%.
