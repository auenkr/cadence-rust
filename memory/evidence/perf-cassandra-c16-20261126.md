# Cassandra c=16 gap investigation (2026-11-26)

Branch `perf-cassandra-c16`. Run [45753659254](https://github.com/auenkr/cadence-rust/actions/runs/45753659254) (success), source head `e360ba70a2585241b0d29f947ac3bd03f98103b6`, workflow `frontend-matching-benchmark.yml` database=cassandra, default capacity c=1,16,64, shape=echo, 1000 requests, **2 reps only** (not 3 runs x 4). Runner: AMD EPYC 9V74, 2 cores/4 threads, 15 GiB, Ubuntu 26.04.1 (same HW spec as the earlier Cassandra evidence). Rust cadence-server SHA256 `e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725`, cadence-history `9bda3c570ee7fb4e222d5b1bcb33d1ed113fddf92f840f6ee942dd2b4db43804`.

## Change (measurement only)
`tests/frontend-gate/benchmark/paired-run.py`: for `--database cassandra`, `docker exec <cassandra> nodetool tablestats -F json` is saved before and after each measurement (`cass-<rep>-<arm>-c<N>-{before,after}.log` in the artifact). No new workflow input. Per-table op counts = delta of `local_read_count`/`local_write_count` (single node, so these are CQL ops incl. each batch mutation per table; `system.paxos` writes = LWT phases).

## Per-table ops (sum of 2 reps; 1000+10 workflows per rep per cell, so divide by ~2020)
| table | c=16 Go R/W | c=16 Rust R/W | c=64 Go R/W | c=64 Rust R/W |
|--|--|--|--|--|
| system.paxos (LWT) | 16 / 42600 | 16 / 42267 | 4 / 41346 | 4 / 43023 |
| cadence.executions | 14593 / 12208 | 14490 / 12273 | 15195 / 12225 | 15595 / 12298 |
| cadence.history_node | 6200 / 12192 | 6200 / 12258 | 6200 / 12216 | 6200 / 12292 |
| cadence.tasks | 4014 / 2041 | 3657 / 1862 | 3132 / 1599 | 4093 / 2080 |
| cadence.history_tree | 0 / 2020 | 0 / 2020 | 0 / 2020 | 0 / 2020 |
| visibility (8 tables, each) | 0 / ~2130-2150 | 0 / ~2120-2125 | 0 / ~2190 | 0 / ~2120-2133 |
| cadence.cluster_config | 161 / 0 | 158 / 0 | 142 / 0 | 130 / 0 |

c=1 is likewise identical (executions 14193/12148 vs 14172/12144; paxos W 36411 vs 36396). Full tables regenerate from the artifact with the script logic above.

## Findings
- Rust issues the **same CQL work per workflow as Go**: per-table read/write counts and LWT (paxos) counts match within <3% (task-table counts vary run to run with matching timing, in both directions). No extra reads, LWTs, batches or visibility writes were found, so no CQL-volume discrepancy exists to fix. Session config also matches Go (LOCAL_QUORUM, LOCAL_SERIAL, NumConns=maxConns default 2, token-aware round robin, 10 s timeout, prepared statements).
- The c=16 gap did **not reproduce**: wf/s (successful/elapsed), per rep: c=16 go 73.8/66.1, rust 75.1/69.4, control 69.2/74.4; c=64 go 81.1/70.5, rust 77.7/80.8, control 80.6/86.3; c=1 go 23.7/21.4, rust 24.0/24.1. Rust >= Go at every concurrency; A/A (control vs rust) spread (~5 wf/s at c=16) is as large as any Go/Rust difference.
- Conclusion: the earlier c=16 deficit (Go 59.4 vs Rust 49.5, run 45748842656 on EPYC 7763) is most plausibly runner/CPU-throttled-Cassandra noise (that run's control arm, the same binary as Rust, was equally low) not a Rust persistence inefficiency. No Rust code change made (strict parity). With only 2 reps this is not a CI-grade claim; a 3x4 rerun would be needed for CIs.
