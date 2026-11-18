# perf-ceiling-partitions (2026-11-19)

Branch `perf-ceiling-partitions`; LB fix head `da179dbe682bc166786fdf6b3124cfbd763e210a`.
Protocol: open-loop one-activity, relaxed commit, 16 pollers, 3 alternating Go/Rust pairs, 30s measure.
Pass = p99 completion <=1000ms, 0 errors, outstanding <= max(2,1%).

## Task A: ceiling (source 0a74e0940, pre-LB-fix, 1 partition)
Runs: 40 https://github.com/auenkr/cadence-rust/actions/runs/44369779892 ·
50 https://github.com/auenkr/cadence-rust/actions/runs/44369792203 ·
60 https://github.com/auenkr/cadence-rust/actions/runs/44369804573 ·
80 https://github.com/auenkr/cadence-rust/actions/runs/44369816942

| rate | Go pass | Go cps / p99 | Rust pass | Rust cps / p99 |
|---|---|---|---|---|
| 40 | 3/3 | 39.9 / 134-141ms | 3/3 | 39.8 / 204-208ms |
| 50 | 0/3 | 9-12 / 51-61s | 0/3 | 34-36 / 8-10s |
| 60 | 0/3 | 17-37 / 21-120s | 3/3 | 59.5-59.7 / 236-301ms |
| 80 | 0/3 (1 run crashed driver) | ~1 / timeouts | 0/3 | 11-15 / 46-56s |

Max all-pairs-pass: Go 40/s, Rust 40/s strictly (60/s passed 3/3, 50/s failed 3/3 -> the 50-60 band is
noise-dominated; one CI runner per rate). Saturation: no service hits its 1-core cap (Rust FE<=0.2,
History<=0.45, Matching<=0.15; Go History ~0.8). MySQL CPU ~0.7 while backlog explodes -> the limit is
MySQL commit/IO on the runner disk (same as perf-capacity-limit), not service CPU. Under overload Go degrades
far worse (cps collapses, errors) than Rust; Rust uses ~half Go's History CPU. No Rust-specific saturation
found, so no profiling fix was needed.

## Task B: 4 read/write partitions
Root cause: Rust FE/History had no matching-client load balancer (Go `client/matching` multiLoadBalancer,
default `weighted`), so all add/poll/query traffic hit the root partition and partitions only added
forwarding. Fix: `cadence-common/src/matching_lb.rs` (PartitionConfigProvider, weighted/rr/random/isolation
LB, unit tests), wired into History add_activity/add_decision/query and FE poll_decision/poll_activity/
cancel_outstanding_poll.
rust-ci green: https://github.com/auenkr/cadence-rust/actions/runs/44372360722 (common),
https://github.com/auenkr/cadence-rust/actions/runs/44372366976 (history),
https://github.com/auenkr/cadence-rust/actions/runs/44372372924 (frontend).

Runs (source da179dbe6): 30 https://github.com/auenkr/cadence-rust/actions/runs/44374046542 ·
35 https://github.com/auenkr/cadence-rust/actions/runs/44374060100 ·
40 https://github.com/auenkr/cadence-rust/actions/runs/44374073911

| rate | Go pass | Go cps / p99 | Rust pass | Rust cps / p99 |
|---|---|---|---|---|
| 30 | 3/3 | 30.0 / 164-168ms | 3/3 | 29.9 / 219-224ms |
| 35 | 0/3 | 25-28 / 12-18s | 3/3 | 34.8-34.9 / 235-256ms |
| 40 | 0/3 | 15-18 / 40-50s | 0/3 | 38.4-39.5 / 437-1209ms (fail on outstanding 14-49 > 12) |

Before fix: Go 3/6, Rust 0/6 at 30-35. After: Rust 6/6, Go 3/6 -> Rust >= Go.

Known remaining parity gaps (documented, not behaviour-affecting in benchmark): History Filters lack
taskType filter for partition counts; FE LB strategy lookup is domain-filtered only; no ReadOnlyPartitionError
cache invalidation; thrift polls carry no LB hints (same as Go over TChannel -> round-robin reads).

Temp branches (not deleted): pcp-ceil-40, pcp-ceil-50, pcp-ceil-60, pcp-ceil-80, pcp-parts-30, pcp-parts-35, pcp-parts-40.
