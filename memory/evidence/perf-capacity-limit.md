# Capacity limiter study: Go vs Rust (2026-11-18)

Branch perf-capacity-limit (harness head 3b497bd4e3d6362e2cbd384da46b60ae16e5a086, source = same tree, master 4327f6e67 + harness).
Protocol as memory/evidence/perf-capacity.md: all core services (Rust arm = Rust FE+Matching+History, Go system Worker),
one-activity, 30s measure, 16 SDK pollers, 3 fresh-DB alternating pairs per step, services 1 CPU, MySQL 1.5 CPU.
Pass = 0 errors, p99 <=1s, <=max(2,1%) outstanding. Variants are identical for both arms.

New instrumentation (collect_resources.py / suite.py): InnoDB log/data fsyncs, log waits, row-lock waits,
perf-schema redo/data file IO wait, COMMIT count+wait (avg_commit_ms), lock time, max `tasks` rows per type
(Matching DB backlog), max `transfer_tasks`/`timer_tasks` per shard (History queue lag). Knobs: workflow inputs
mysql_args, history_shards, dynamic_config_extra (env MYSQL_ARGS, NUM_HISTORY_SHARDS, DYNAMIC_CONFIG_EXTRA; MYSQL_TMPFS).

## Runs (all conclusion success; capacity misses are results)
| Variant | 30,35/s | 40/s |
|---|---|---|
| (a) baseline | https://github.com/auenkr/cadence-rust/actions/runs/44357733488 | https://github.com/auenkr/cadence-rust/actions/runs/44357740681 |
| (b) innodb_flush_log_at_trx_commit=2, sync_binlog=0 | https://github.com/auenkr/cadence-rust/actions/runs/44357747092 | https://github.com/auenkr/cadence-rust/actions/runs/44357754117 |
| (c) numHistoryShards=16 | https://github.com/auenkr/cadence-rust/actions/runs/44357761975 | https://github.com/auenkr/cadence-rust/actions/runs/44357768877 |
| (d) matching.numTasklistRead/WritePartitions=4 | https://github.com/auenkr/cadence-rust/actions/runs/44357775656 | https://github.com/auenkr/cadence-rust/actions/runs/44357782109 |

## Pairs passing (of 3) and completions/s
| Variant | Go 30 | Go 35 | Go 40 | Rust 30 | Rust 35 | Rust 40 |
|---|---|---|---|---|---|---|
| baseline | 0 (19-30/s) | 0 (19-24) | 0 (24-27) | 0 (26-29) | 0 (27-28) | 0 (26-28) |
| fsync relaxed | **3** (29.9) | 0 (24-27) | **3** (39.9) | **3** (29.9) | **3** (34.8) | **3** (39.8) |
| 16 shards | **3** | 0 (26-29) | 2 | 1 | 0 | 0 (22-34) |
| 4 partitions | 1 | 2 | 0 (11-13) | 0 | 0 | 0 (20-22) |

Max rate where all 3 pairs pass: baseline Go <30, Rust <30 (25 both in the earlier study); fsync relaxed Go 30
(passes 40 on another runner, fails 35), Rust >=40 (highest tested, a lower bound); 16 shards Go 30, Rust <30;
4 partitions Go <30, Rust <30.

## Limiter: MySQL commit/redo-log fsync latency on the runner disk (environmental)
- Relaxing durability is the only variant that lifts both: decision schedule-to-start p99 drops from 1.5-12s to 55-120ms,
  DB decision backlog disappears, avg COMMIT 0.5-3 ms -> 0.08-0.45 ms. CPU unchanged and unsaturated (History <=0.76, MySQL <=0.94 of 1.5).
- Baseline backlog is in decision tasks only (DB `tasks` type 0 up to ~480 rows for Go); activity backlog ~0.
- The Go fsync-relaxed 35/s fail (commit 1.1 ms, run 44357747092) while 40/s passed on another runner (0.2 ms) shows
  runner disk variance, which explains the large run-to-run spread in the earlier study.
- 16 shards helps Go at 30 slightly but not reliably; partitions do not help (they add forwarding and made 40/s worse for both).
  Neither is the limiter.
- Rust per-commit latency is lower than Go (0.5 vs 1.7-3 ms at baseline) but it also fails, so the shared ceiling is
  the disk's sync throughput, not a Rust default. No Rust-side limit found, so there is no Rust code change and no rust-ci run.

## Benchmark config change
Workflow input mysql_args now defaults to `--innodb-flush-log-at-trx-commit=2 --sync-binlog=0` (both arms; `none` = MySQL defaults)
so capacity reflects services rather than the runner disk. Under that config: Go max all-pass 30 (40 on a faster disk), Rust >=40.

## Follow-ups
- Re-run fsync-relaxed at 40,50,60 to find the real service ceiling (Rust is a lower bound at 40).
- With 4 partitions Rust had higher decision schedule-to-start than Go at 30-35/s (Go passed 3/6, Rust 0/6) but Rust was better at 40/s.
  Partitioning is not the default config; check Rust partition forwarding vs Go when it matters.
