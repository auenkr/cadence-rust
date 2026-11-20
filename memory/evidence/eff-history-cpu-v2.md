# eff-history-cpu-v2 (2026-11-20)

Question: why Rust History CPU vs Go went 0.55 (11-19) -> 0.71 (bench-rerun-20261120), RSS 0.83 -> 1.02.
All benchmarks: sdk-matching-benchmark.yml, service=history, 25/s, warmup 10 s + 60 s, mysql_tmpfs=true, sql_log_secs=60.
"CPU"/"RSS" = History container only. SELECTs from per-arm `select_by_table` (Benchmark/agg.py, fixed here).

## 1. agg.py fix
`Benchmark/agg.py` (now in repo) used the top-N general-log `rows` list, which can omit tables (made executions/map
tables look 1.47-1.50x). Now: per-arm `select_by_table`, mean over arms that have it. Re-run on 44625313998:
executions 1.00, map tables 1.00, history_node 1.25 (the real gap fixed by 13a74d4a9).

## 2. Queue v2 on vs off (source e6b344173d6f705f9c55a3419690ad969899bbe0)
New workflow input `dynamic_config_extra` (both arms; replaces same top-level keys in development.yaml).

| Variant | Run | Go CPU s | Rust CPU s | Rust/Go CPU | Go RSS | Rust RSS | Go p99 ms | Rust p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| v2 on (3 pairs) | https://github.com/auenkr/cadence-rust/actions/runs/44644440768 | 27.07 | 16.46 | 0.61 | 162 | 156 | 120 | 104 |
| v2 off (pairs 01,02) | https://github.com/auenkr/cadence-rust/actions/runs/44644453475 | 38.0 / 37.9 | 22.6 / 21.0 | 0.57 | 159 / 163 | 145 / 150 | 4220 / 3417 | 388 / 300 |
| v2 on + perf profile (1 pair) | https://github.com/auenkr/cadence-rust/actions/runs/44644458223 | 26.91 | 19.32 | 0.72 | 153 | 152 | 125 | 110 |

v2-off run "failed" only at report.py: Rust arm 00 had 1/1500 activity START_TO_CLOSE timeout (Go arm 00 p99 13.3 s);
metrics above computed from raw resources/samples. v1 queues are heavily degraded for both at this load.

Verdict: v2 does not make Rust slower: it cuts Rust CPU ~22 -> ~16.5 s, but Go more (~38 -> ~27 s), so the ratio moves
0.57 -> ~0.6. v2-off ratio (0.57) reproduces the 11-19 value (0.55, pre-v2 default). Rust RSS +~8 MiB with v2.
Go absolute CPU varies 21-30 s between runners at identical settings; single-run ratios swing +-0.1.

## 3. Profile hotspots (run 44644458223) and fixes (commit a400d03779cb89b51779e438ed423d4d1104fffd)
- queuev2 `append_or_merge_slice -> try_merge_with_virtual_slice -> merge_by_range`: ~7% of History samples.
  Rust cloned ALL pending tasks of both slices and rebuilt a new tracker each merge (O(pending) per poll; plus
  get_domain_id String + SipHash per task). Go `mergeVirtualSlicesByRange` reuses `left.pendingTaskTracker` and adds only
  right's tasks. Fix: take left's tracker (merge runs under the virtual-queue lock, old slices are replaced), add right's tasks.
- events cache `put`: ~4.5%. Every put swept the whole map for TTL and rebuilt a HashSet of all keys; every hit/put did
  `VecDeque::retain` (O(n)); size mode summed all weights. Go lru: no sweep, expiry checked on Get, evict from LRU back.
  Fix: BTreeMap access-seq LRU + running total weight; expiry on read only (Go). Unit test lru_order_tests.
- queue v2 defaults (poll backoff 5 s, max poll 1 min transfer / 5 min timer, max virtual queues 2, cached reader enabled)
  match Go dynamic config defaults.

## 4. Validation (head a400d03779cb89b51779e438ed423d4d1104fffd)
| Check | Run | Result |
|---|---|---|
| rust-ci cadence-history | https://github.com/auenkr/cadence-rust/actions/runs/44650479752 | success |
| History mixed gate MySQL | https://github.com/auenkr/cadence-rust/actions/runs/44650484083 | success, GATE SUMMARY failed: [] |
| History mixed gate Cassandra | https://github.com/auenkr/cadence-rust/actions/runs/44650488363 | success, GATE SUMMARY failed: [] |
| History restart gate (mysql) | https://github.com/auenkr/cadence-rust/actions/runs/44650493096 | success |

## 5. Before/after benchmark (dispatched together, 3 pairs each)
| | Run | Rust bin sha256 | Go CPU | Rust CPU | CPU ratio | Go RSS | Rust RSS | RSS ratio | p50 G/R | p99 G/R |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| before e6b344173 | https://github.com/auenkr/cadence-rust/actions/runs/44650513488 | da28f61d…7610f (cache) | 21.32 | 12.68 | 0.60 | 154 | 155 | 1.01 | 35.2 / 27.3 | 111.1 / 100.9 |
| after a400d0377 | https://github.com/auenkr/cadence-rust/actions/runs/44650509198 | a21bb5be16da796f3b7884d15442ac48458647bd3607cde398f52579b361e635 (fresh) | 29.75 | 17.00 | 0.57 | 155 | 149 | 0.97 | 69.9 / 37.7 | 196.2 / 107.0 |

Per-table SELECTs: Go == Rust (1.00) for executions, current_executions, shards, history_node and all map tables in both runs;
Rust lower for cluster_config (0.75), task_lists, tasks.

The two runs landed on runners of different speed (Go CPU 21 vs 30 s), so only ratios compare: CPU 0.60 -> 0.57,
RSS 1.01 -> 0.97. Improvement is within run-to-run noise (~+-0.1); fixes remove O(pending)/O(cache) per-op work that
grows with backlog, so they matter more under load than at 25/s.

Verdict: the 0.55 -> 0.71 move was mostly Go benefitting more from queue v2 (plus runner noise), not a Rust regression.
Two Go-parity hot-path fixes applied; all gates green.
