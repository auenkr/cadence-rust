# Matching CPU: remove unconditional per-RPC trace prints (2026-11-18)

Branch `perf-matching-cpu`, fix commit `dfee2e84ac1729f7dc68a6d1295acb3435d0d128`
(base master `36e8a1e3175ad76ed77bc9ec9bddadf3241a4d78`).

Finding: every Matching RPC wrote eprintln lines (MATCH/POLL/ADD/QUEUE/HIST/FE-TRACE);
TCHAN-TRACE and NATIVE-TRACE also `{:?}`-formatted the whole request (up to 3000 chars)
on every TChannel/native call. Go has no equivalent output. Removed, no logic change.
Idle timers reviewed: Matching background loops are >=500ms on an in-memory store,
and the 100ms re-check only applies to the no-reader path. No busy polling found.
No idle-CPU measurement or profile was run.

## Validation
- rust-ci (cadence-matching): https://github.com/auenkr/cadence-rust/actions/runs/44321958963 — success, head dfee2e84.
- Benchmark harness `173f9053797595b3e9b441f54635419e15c7edbb` (sdk-matching-benchmark.yml), fixed 5 starts/s,
  10s warmup + 30s, 3 alternating fresh-MySQL pairs, 0 errors in all arms.
  - before (source 36e8a1e31): https://github.com/auenkr/cadence-rust/actions/runs/44321744301 — success
  - after (source dfee2e84, ref bench-perf-matching-cpu): https://github.com/auenkr/cadence-rust/actions/runs/44322024068 — success
- This workflow lane is fixed-rate only: no recovery stage was run here.

## Matching container CPU (average cores) and completion p99 (ms), per pair Go / Rust

| run | pair | Go cores | Rust cores | Rust/Go | Go p99 | Rust p99 |
|---|---|---:|---:|---:|---:|---:|
| before | 00 | 0.0187 | 0.0189 | 1.01 | 51.3 | 52.2 |
| before | 01 | 0.0167 | 0.0184 | 1.10 | 49.7 | 52.2 |
| before | 02 | 0.0172 | 0.0178 | 1.03 | 48.3 | 52.0 |
| after  | 00 | 0.0308 | 0.0245 | 0.80 | 80.4 | 80.7 |
| after  | 01 | 0.0303 | 0.0235 | 0.78 | 82.6 | 77.4 |
| after  | 02 | 0.0299 | 0.0248 | 0.83 | 81.1 | 78.1 |

Runners differ a lot between the two runs (Go 0.017 vs 0.030 cores), so compare Rust/Go inside each run:
before median 1.03 (Rust used more CPU than Go), after median 0.80 (Rust used less CPU than Go).
p99 before: Rust about 1-4ms above Go. After: Rust at or below Go in all 3 pairs.
At 5/s the absolute difference is small; the 25/s and closed-loop gaps were not re-measured here.
