# Matching scaler parity fixes (2026-10-23)

Branch `matching-scaler-fix-20261023` (from matching-isolation-scaler-tests-20261023). Not merged.

## 1. Trailing partition describe in production
`Scaler::tick` split into `tick_prepare` / `tick_finish`; new async
`adjust_read_partitions_async` fetches missing trailing partitions between them.
`scaler_update` (matching_registry.rs) now describes each trailing partition via
`describe_scaled_partition` (DescribeTaskList RPC to the owner when membership is
set, local otherwise; same request fields as the isolation path). A failed describe
yields `None` -> loop breaks, as Go (`break` on error, adaptive_scaler.go:293-297).
`...production_drain_describes_trailing_partitions` un-ignored; it now drives the
async path with a fake in-process describe source.

## 2. ensure_minimum_groups two-pointer pass
Ported Go's single sort + lo/hi pointers (isolation_balancer.go:290-344). One safety
exit if a move makes no progress (Go would loop forever).

Hand derivation of the new test (min=2). P0={}, P1={k:0.5}, P2={g1..g6 = 1,2,3,4,5,7},
P3={h1..h4 = 10..13}. Go sort by (count,size): [P0,P1,P3,P2]; lo=0,hi=3(P2).
- need 2, avail 4. Move1 from=22,to=0, diff=|22-2s|: g6->8 minimal -> g6. from=15,to=7.
- Move2 diff=|8-2s|: g4->0 -> g4. P0={g6,g4}, P2={g1,g2,g3,g5}.
- lo=1 (P1 len1), hi still P2 (len4>2): need1 avail2; from=11,to=0.5, |10.5-2s|: g5->0.5 min.
  P1={k,g5}, P2={g1,g2,g3}. lo advances to 3 = hi, stop.
Old re-sorting loop: after the first batch it re-sorted, picked P3 (4 groups, size 46 > P2's 11)
as donor and moved h4 to P1 instead -> differs.

## CI evidence
(see below)
