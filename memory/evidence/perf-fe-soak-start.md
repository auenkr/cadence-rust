# Frontend SELECT parity and StartWorkflowExecution soak

Branch `perf-fe-soak-start` from master `a7fb6797d9dfc423bc20af503192587285b2ecc2`. The baseline soak used `e8952d5e5d9484c0466a2cfa1001bcba95fe5da8`, which changes only the workflow to save `lscpu` output for each lane.

## SELECTs per 60 seconds

- Run the `sdk-matching-benchmark.yml` workflow with `service=frontend`, `rate=25`, `duration=60`, `warmup=10`, `repetitions=3`, `mysql_tmpfs=true`, `sql_log_secs=60`, `fresh_build=true`, and the full `source_ref`. Download `sdk-frontend-benchmark` into a directory named for the run ID and pass its grandparent to `python3 Benchmark/agg.py`. The table uses the mean of the per-arm `mysql_general_log.select_by_table` counts.
- Master baseline, source and workflow head `a7fb6797d9dfc423bc20af503192587285b2ecc2`, **success**, Rust binary SHA256 `c6cde404566c2cb550b2f9fafc1f48359f6884eb7c279292c94a5da74131c829`. `agg.py` reported AMD EPYC 7763 and 54.55 Go MySQL CPU seconds. Completion p99 excluded from comparisons. https://github.com/auenkr/cadence-rust/actions/runs/44681288887
- Baseline rerun, source `a7fb6797d9dfc423bc20af503192587285b2ecc2`, workflow head `9e3aafeec0956c558682002cf7b92a58d370ba1d`, **success**, Rust binary SHA256 `d2ac42686dcbcd8dfaab8202c9861a0b2b2d1c96e3c01adeb881020023fda5be`. `agg.py` reported Intel Xeon Platinum 8573C, Go/Rust MySQL CPU 39.37/37.11 seconds, and median completion p99 148.8/120.8 ms. All six arms completed 1,500 workflows with zero errors. https://github.com/auenkr/cadence-rust/actions/runs/44685439005
- Candidate, source and workflow head `7a871a4cc089f20cca0fa54445ff45d5be02d58c`, **success**, Rust binary SHA256 `31fd89bf963887b3c9c690bbc6e8b06e4ffe0b97df156f46b4a03c8699b21e51`. `agg.py` reported AMD EPYC 9V74, Go/Rust MySQL CPU 37.89/37.56 seconds and median completion p99 117.5/118.8 ms. 1,500/1,500 workflows correct per arm, zero errors. https://github.com/auenkr/cadence-rust/actions/runs/44682430142

Go constructs its operational dynamic config client in `go/common/dynamicconfig/dynamicconfigfx/fx.go`. The client reads `cluster_config` row type 2 every two seconds by default, independent of row type 1 (global isolation groups, 30 seconds). `NewConfigStoreClient` starts polling, then `common/resource/resource_impl.go` calls `Start`, which starts a second poller on each client. This predicts 60 operational plus 4 isolation reads per service per minute, or 256 with four Go services. Master Rust Frontend already has the row-type-1 client but has one poller and keeps operational config in memory. That predicts 192 + 2 reads for the mixed arm even after the isolation merge.

The candidate binds Frontend operational Admin operations to the existing Go-format config-store mechanism, loads row type 2 before serving, and refreshes both stores at the Go cadence. It preserves operational cache state across normal config reloads and publishes persisted operational changes after a refresh, matching Go's cached reads. A persistence regression verifies the row type, stale snapshot rejection, cache publication, namespace isolation and restore.

The degraded baseline reproduced the predicted config totals on all three pairs (Go 256, Rust-Frontend arm 194). `domain_metadata` matched at 24 in each arm, and `domains` was 30 versus 24, exactly 20% lower. The Go-arm SQL log identifies 24 paginated domain scans. The mixed arm's metadata reads are 18 from Go services and 6 prepared reads from Rust. The other table differences above 20% were `task_lists` (447 versus 268) and `tasks` (435 versus 256). The underlying Go Matching `GetTasks` counts track durable `INSERT INTO tasks` counts (Go 398/410/409, Rust-Frontend arm 309/323/50), so this measures how often Matching falls back to persistence after attempting a synchronous match. Both arms completed all 1,500 workflows per pair. `Query NULL` is the collector's bucket for SELECTs without a parsed table.

The Xeon master rerun confirmed `cluster_config` at 256 versus 194 (−24.2%), `domain_metadata` at 24 each, and `domains` at 30 versus 24 (−20%). `task_lists` and `tasks` were within one mean SELECT. The remaining table over 20% was `timer_tasks`: Go History issued 3 prepared SELECTs across three pairs versus 10 in the mixed arms; the separate collector probes matched at 12 per minute. History is Go in both lanes, and the timer queries are too sparse to establish a Frontend parity gap. All other table ratios were within 2%.

| Candidate table | Go SELECTs per 60s | Rust-Frontend arm | Rust/Go |
|---|---:|---:|---:|
| cluster_config | 256 | 256 | 1.00 |
| domains | 30 | 24 | 0.80 |
| domain_metadata | 24 | 24 | 1.00 |
| task_lists | 41 | 81 | 1.95 |
| tasks | 29 | 69 | 2.34 |
| timer_tasks (prepared) | 1.33 | 2 | 1.50 |

The candidate's `task_lists` and `tasks` differences come from pair 01, which recorded 119 durable task inserts and 119 additional SELECTs on each table. The other two pairs match within one SELECT. The offset reverses direction from the degraded baseline. Prepared `timer_tasks` differed by two queries across all three pairs (4 vs 6). The collector's separate `cadence.timer_tasks` probe matched at 12 per arm. Other tables were within 2%.

Candidate pair 01's Start RPC p99 was 337.7 ms versus 16.6 ms. Its other pairs were 17.8 versus 17.9 ms and 14.3 versus 18.1 ms. All 32 candidate measurement starts slower than 100 ms were scheduled in the same two seconds (06:04:12–13 UTC). At the same time the Go History logs show three workflow-context locks held for over one second, the slower workflows spent hundreds of ms waiting for decisions, and the MySQL container briefly rose from roughly 0.55 to 1.0 CPU seconds per second. The Frontend stayed below 0.2 CPU seconds per second. That arm had also begun warmup before Go History acquired its shards (06:03:11.962 UTC). The logs establish shared downstream work during the burst but do not isolate the trigger.

## Frontend soak

Use `frontend-soak.yml` at rate 10/s for 70 minutes, with `lanes=["go-baseline","rust-fe"]` and the scheduled midpoint Frontend restart. These lanes isolate Frontend with Go Matching, History, and Worker on both sides. Save `summary.json`, `runner-cpu.txt`, and `rust-binary.sha256` from the lane artifacts and check the `soak-verdict` artifact independently of the workflow conclusion.

- Baseline repeat 1. https://github.com/auenkr/cadence-rust/actions/runs/44681376673
- Baseline repeat 2. https://github.com/auenkr/cadence-rust/actions/runs/44681388177
- Candidate repeat, head `7a871a4cc089f20cca0fa54445ff45d5be02d58c`. https://github.com/auenkr/cadence-rust/actions/runs/44682455258
- Superseded during build to capture runner CPU. https://github.com/auenkr/cadence-rust/actions/runs/44681304320 and https://github.com/auenkr/cadence-rust/actions/runs/44681317804

## Candidate validation

- Workspace Rust CI, `clean_target=true`, head `7a871a4cc089f20cca0fa54445ff45d5be02d58c`, **success**. Fresh target selection, Clippy followed by workspace tests, and PostgreSQL pool wakeup check passed. https://github.com/auenkr/cadence-rust/actions/runs/44682391895
- MySQL Frontend mixed gate, same head, **success** with `GATE SUMMARY failed: []`. Fresh Rust binary SHA256 `0254e8918d6cdfdc873ff59933d579ed1e9b9b96dc363a630a18fafa1327e9c1`. https://github.com/auenkr/cadence-rust/actions/runs/44682405787
- The formatting job on the first source commit cancelled the run. The CI-reported rustfmt hunks were applied before the clean-target run. https://github.com/auenkr/cadence-rust/actions/runs/44682086971
