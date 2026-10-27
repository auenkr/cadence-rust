# Matching adaptive scaler QPS source audit (2026-10-27)

Scope: bounded source comparison of Go adaptive scaling/QPS accounting and Rust `cadence-matching` at source SHA `7d072c254` (tag `matching-retry-trace-20261027-v1`). No production edits or runtime gates were made. Parent owns mixed trace gate `40469125507`; this audit did not dispatch or poll it. No Cargo/Go commands were run.

## Source map

- Go scaler: `go/service/matching/tasklist/adaptive_scaler.go`
  - `run` checks both feature flags, collects metrics, resets sustained conditions on collection failure, calculates writes and read drain: lines 140-214.
  - `calculateWritePartitionCount`: thresholds and sustains at 231-258; `getNumberOfPartitions` at 428-441.
  - `assumeEvenQPS`: root `DescribeTaskList(true).TaskListStatus.NewTasksPerSecond * len(WritePartitions)`: lines 317-331.
  - Isolation path: describes each configured read partition, sums per-partition root QPS and per-group QPS: lines 333-396.
- Go QPS tracker: `go/common/stats/stats.go`
  - manager creates EMA tracker with exp 0.5 and configured bucket interval: `go/service/matching/tasklist/task_list_manager.go:229`.
  - global ticker/report, first bucket and subsequent EMA: `stats.go:76-103, 166-189`.
  - `ReportGroup` increments both root and group; `ReportCounter` root only: `stats.go:111-129`.
  - `taskListManagerImpl.AddTask` reports only when `ForwardedFrom == ""`, before sync match or persistence: `task_list_manager.go:544-568`.
  - `DescribeTaskList` exposes tracker root and group QPS: `task_list_manager.go:770-803`.
- Rust scaler: `rust/crates/cadence-matching/src/matching_registry.rs`
  - Root queue scaler loop resolves dynamic config each interval: lines 1674-1730.
  - `scaler_update` gates flags, snapshots partition config, sums per-read-partition metrics for isolation mode, else multiplies root tracker by write count: lines 1817-1960 (notably 1898).
  - Child partition describes read child queue QPS and group metrics: lines 1731-1806.
- Rust QPS: `rust/crates/cadence-common/src/qps.rs`
  - Fixed-window EMA accumulation and folding: lines 32-105, 112-205.
  - root/group accounting (`report_group` increments both): lines 164-176.
- Rust arrival entry: `rust/crates/cadence-matching/src/grpc_matching.rs`
  - `report_arrival` reports exactly one task to root and optional isolation group: lines 96-110.
  - Decision/activity AddTask only call it for non-forwarded requests: around lines 2350-2357 and 2574-2581.

## Findings

No concrete source divergence found in the requested adaptive QPS/accounting scope.

- Root-only mode deliberately uses `root QPS * current write partition count` in both languages (`assumeEvenQPS` vs `scaler_update`). Thus at 1 RPS and 1 current write partition, 1 RPS is expected; with 102 writes the estimate becomes 102 RPS and with 434 writes it becomes 434 RPS. Those observations alone do not prove runaway growth: the scaler thresholds also scale with current count (`n * upscaleRPS`), so the estimate at 1 RPS is below the default 200 RPS-per-partition threshold. Tuned thresholds or a transient EMA estimate can change that comparison.
- Isolation assignment mode instead sums measured QPS from every current read partition in both implementations. It does not additionally multiply the root estimate by write count.
- AddTask arrival is counted at request arrival before synchronous matching or persistence; failed sync attempts still represent one root arrival. Forwarded child Adds are excluded to avoid counting the same logical submission again. Rust mirrors this root/forwarded condition.
- `ReportGroup` counts one event in root and in that group's accumulator; it does not double count at root. The scaler's isolation path sums partition root QPS once, while group QPS is used only for isolation balancing.
- EMA reports only on the fixed tracker interval. Arrivals stay pending until a completed tick; first folded bucket is direct `count / seconds`, later buckets use the configured EMA. Rust's clock-driven folding uses the same first-window and later-window formula and retains open-bucket arrivals until the boundary. No arrival/completion accounting is present: QPS is AddTask arrival rate, not task completion rate.
- Live scaler config is re-read by Rust's periodic provider on each iteration and values are used during the following tick. Partition config is read once at tick start, then updated after scale decisions in both implementations. No sourced read/write transition divergence was identified.

Prior failed mixed run `40465991711`'s 24 workflows scaling `1 -> 102 -> 434` with `1 RPS / 1s` config cannot be adjudicated from terminal counts. The key runtime observation is the exact sampled `NewTasksPerSecond` at each tick, write count used by that same tick, active dynamic config (especially upscale RPS, sustain duration, and interval), and whether isolation assignment was enabled. Under root-only default formula, compare `root_qps * write_count` against `write_count * upscale_rps`; sustained upscale requires the former to exceed the latter for the configured sustain. A sample with ~1 root QPS and thresholds below 1 RPS/partition could legitimately scale; a sample showing growth while the recorded estimate stayed below the recorded threshold continuously would discriminate an implementation/runtime issue from the documented formula. Preserve the 60s SDK path, all 24 workflow completions, and routing assertions during any gate interpretation.

## Disposition

No fix proposed; source audit only. Parent gate remains authoritative for runtime behavior. This note records no service-completion or full parity claim.
