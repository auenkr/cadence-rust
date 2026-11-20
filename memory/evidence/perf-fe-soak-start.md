# Frontend SELECT parity and StartWorkflowExecution soak

Branch `perf-fe-soak-start` from master `a7fb6797d9dfc423bc20af503192587285b2ecc2`. The baseline soak used `e8952d5e5d9484c0466a2cfa1001bcba95fe5da8`, which changes only the workflow to save `lscpu` output for each lane.

## SELECTs per 60 seconds

- Run the `sdk-matching-benchmark.yml` workflow with `service=frontend`, `rate=25`, `duration=60`, `warmup=10`, `repetitions=3`, `mysql_tmpfs=true`, `sql_log_secs=60`, `fresh_build=true`, and the full `source_ref`. Download `sdk-frontend-benchmark` into a directory named for the run ID and pass its grandparent to `python3 Benchmark/agg.py`. The table uses the mean of the per-arm `mysql_general_log.select_by_table` counts.
- Master baseline, source and workflow head `a7fb6797d9dfc423bc20af503192587285b2ecc2`. https://github.com/auenkr/cadence-rust/actions/runs/44681288887
- Candidate, source and workflow head `7a871a4cc089f20cca0fa54445ff45d5be02d58c`. https://github.com/auenkr/cadence-rust/actions/runs/44682430142

Go constructs its operational dynamic config client in `go/common/dynamicconfig/dynamicconfigfx/fx.go`. The client reads `cluster_config` row type 2 every two seconds by default, independent of row type 1 (global isolation groups, 30 seconds). `NewConfigStoreClient` starts polling, then `common/resource/resource_impl.go` calls `Start`, which starts a second poller on each client. This predicts 60 operational plus 4 isolation reads per service per minute, or 256 with four Go services. Master Rust Frontend already has the row-type-1 client but has one poller and keeps operational config in memory. That predicts 192 + 2 reads for the mixed arm even after the isolation merge.

The candidate binds Frontend operational Admin operations to the existing Go-format config-store mechanism, loads row type 2 before serving, and refreshes both stores at the Go cadence. It preserves operational cache state across normal config reloads and publishes persisted operational changes after a refresh, matching Go's cached reads. A persistence regression verifies the row type, stale snapshot rejection, cache publication, namespace isolation and restore.

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
