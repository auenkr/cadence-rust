# eff-matching (2026-11-20)
Branch eff-matching, change commits e191d6e72 + 378f96503 (head 378f96503eca54f6975c24748a5403acdee7d972), base a075a66fb.

## Profile (run 44761746372, matching, 25/s, profile=1, 3 pairs, EPYC 7763)
Rust matching samples are sparse (~600/pair). Matching-crate hotspots (inclusive):
`MatchingHistoryPeerResolver::shard_id` -> `owner()` rebuilt `Ring::new` (100 points/host, format! + fingerprint32 + sort) on every
History call (2.4% of samples, top self-cost frame in cadence_matching). Other: RingpopNode::ping 1.9%, domain cache by_id_shared ~0.6%, memcpy 7.4% (diffuse).

## Change
`rust/crates/cadence-matching/src/matching_history_peer_resolver.rs`: `owner()` reuses an `Arc<Ring>` cached per resolver, keyed on the exact
host list of the current membership snapshot (rebuilt whenever the list differs, so refreshes stay visible). Placement results identical; no persistence/timing/Go-visible change.

## Validation (all success)
- rust-ci cadence-matching: https://github.com/auenkr/cadence-rust/actions/runs/44769373820 (first attempt 44768610073 failed clippy type_complexity, fixed)
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44770624967
- Matching mixed gate: https://github.com/auenkr/cadence-rust/actions/runs/44770630962
- frontend-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44770636940 (`GATE SUMMARY failed: []`)

## Benchmark (matching, 25/s, 60s, 3 pairs, mysql_tmpfs, sql_log_secs=60). Runner CPU differed per run, so CPU is NOT comparable across rows.
| run | code | runner | Go CPU s | Rust CPU s | Rust/Go CPU | Go RSS | Rust RSS | Go p99 | Rust p99 |
|---|---|---|---|---|---|---|---|---|---|
| 44761746372 | before, profile=1 | EPYC 7763 | 6.40 | 6.18 | 0.97 | 100M | 61M | 2924 | 2796 |
| 44770663986 | before | Xeon 8573C | 4.45 | 3.71 | 0.83 | 100M | 61M | 113.6 | 113.7 |
| 44770643661 | after (378f965) | EPYC 9V45 | 4.75 | 5.00 | 1.05 | 99M | 61M | 116.5 | 113.6 |
SELECT counts per table Rust/Go ~1.00 in all runs (task_lists/tasks 0.99; cluster_config 0.76 unchanged from before).

## Verdict
The ring cache removes a real per-call hotspot, but the benchmark cannot show the gain: before/after landed on different CPU models and run-to-run
CPU variance (3.7-5.0 s) exceeds the ~2% saving. Matching Rust/Go CPU was 0.83-1.05 across runners (earlier 0.74 reading was also runner-dependent). Needs same-runner A/B for a measurable number.
