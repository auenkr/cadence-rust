# parity-domain-v1 (2026-11-20)

Branch `parity-domain-v1` (from master 658f873fc).

## 1. Frontend domain/cluster_config SELECTs (bench-fe-lane)
Source data: per-statement counts in run 44656247009 `0*-*-resources.json` (`mysql_general_log`, 60 s window).
The counts were the same in all 3 pairs:

| statement | Go (4 Go services) | Rust FE arm (3 Go + Rust FE) |
|---|---:|---:|
| `SELECT notification_version FROM domain_metadata` | 24 | 18 |
| `SELECT ... FROM domains WHERE shard_id=? ORDER BY id LIMIT ?` (list) | 24 | 18 |
| `SELECT ... FROM cluster_config WHERE row_type = ?` | 256 | 191–193 |

Exactly one service's share was missing, so the Rust Frontend issued **zero** of these queries. This was not
caching. It was a parity bug:
- **Domain cache (fixed).** Go Frontend owns `DefaultDomainCache` (`common/resource/resource_impl.go`): it refreshes every
  `DomainCacheRefreshInterval` = 10 s (1 s min, metadata read first, stops at notification version >= metadata version).
  In a Frontend-only Rust process (`launcher.rs`, run_frontend && !run_matching), the domain store was loaded
  once at startup and then only filled on misses (`insert_matching_metadata_if_missing`). Domain updates or failovers
  made by another Frontend/worker/replication were therefore never seen (stale active cluster, config, isolation groups).
  Fix: `start_frontend_domain_refresh` runs the existing Go-parity `MatchingDomainCache` (same refreshLoop, interval, min interval,
  metadata-first order and notification-version check) with a callback that feeds `InMemoryDomainStore::sync_matching_metadata`.
  When Matching runs in the same process, its cache already feeds the store, so the new refresher is not started.
- **cluster_config (documented, not fixed here).** In Go every service polls `cluster_config` through the configstore
  client (isolation-group store, `createConfigStoreOrDefault`; plus dynamic config when `client: configstore`). The Rust
  Frontend keeps global isolation groups in memory (`admin.rs` `get/update_global_isolation_groups` ->
  `InMemoryDomainStore`): it never reads or writes `cluster_config`. Global isolation-group updates via the Rust FE
  are therefore not persisted or shared, and updates written elsewhere are not visible. This is a missing feature port
  (configstore client + MySQL/Postgres/Cassandra row_type GlobalIsolationGroupConfig), not a timing difference. It is
  follow-up work.

## 2. v1 (queue v2 off) "activity START_TO_CLOSE timeout" (run 44644453475)
- The failure was really the *workflow* ExecutionStartToClose timeout (driver: 1 min). Workflow `00-rust-measure-000001494`
  had activity/decision schedule-to-start = null.
- The same pattern was in every Rust arm of that run. Go had none:
  00-rust max 60.0 s (1499: 59.5 s, 1494: timed out), 01-rust max 42.4 s, 02-rust max 48.3 s, against Rust p99 0.3–0.4 s.
  Go max was within about 2% of its own p99.
  For 1499, `decision_schedule_to_start_ms = [59453, 5]`: the **first decision transfer task** of one of the last workflows
  waited for the v1 transfer `maxPollInterval` (~60 s).
- Root cause (Rust bug): Go's `taskNotifier` notifies the queue *before* `updateMaxReadLevelLocked`, but does so inside the shard lock.
  The v1 reader's `getImmediateTaskMaxReadLevel` takes `s.RLock()`, so it blocks until the writer has raised the level.
  Rust's reader read `immediate_task_max_read_level` without the task-write gate. If it ran between the notify and the
  update, it saw `read_level == max_read_level` and went back to waiting for a notification. With no further writes
  (end of load), the task waited for the max-poll timer.
- Fix: `queue::deps::ShardContext::wait_in_flight_task_writes` (default no-op). `RealQueueShardContext` acquires and releases
  `ShardContext::lock_task_writes()`. The v1 transfer processor calls it before reading the max read level, which matches
  Go's RLock wait. Write and notify order are unchanged, so v2 is not affected.

## Validation
Code head 714e0f6d0296b4f958e92da4861b7ad8acb923d0 (all runs below on branch parity-domain-v1, all success):
- rust-ci cadence-matching (item 1): https://github.com/auenkr/cadence-rust/actions/runs/44660597856
- rust-ci cadence-history (item 2): https://github.com/auenkr/cadence-rust/actions/runs/44661355008
- frontend-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44661419352 (`GATE SUMMARY failed: []`)
- history-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44661423242 (`GATE SUMMARY failed: []`)
- history-restartgate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44661427533 (`GATE SUMMARY failed: []`)
- History benchmark, v2 off (rate 25, 60 s, warmup 10, 3 pairs, mysql_tmpfs, same inputs as 44644453475):
  | run | Rust p99/max ms per pair | Go p99/max ms per pair | incorrect |
  |---|---|---|---|
  | https://github.com/auenkr/cadence-rust/actions/runs/44667221232 | 200/223, 199/237, 203/233 | 521/647, 1327/1463, 2950/4182 | 0 |
  | https://github.com/auenkr/cadence-rust/actions/runs/44667224821 | 198/233, 201/267, 203/233 | 512/727, 1142/1303, 2886/3189 | 0 |
  Before the fix (44644453475), every Rust arm had a 42–60 s tail at the end of load. After the fix, max is within ~35% of p99 in all 6 arms.
  Both runs used EPYC 7763 runners (degraded MySQL; see bench-fe-lane.md), so do not use the Go-vs-Rust p99 ratio from them.
  The tail result does not depend on that.
  (Dispatches 44661393258/44661397572 used the default low rate of 150 workflows and are not used here.)

## Verdict
1. Domain cache: real deviation (the Frontend-only process had no periodic refresh), fixed. cluster_config: the Rust FE does not port the
   configstore isolation-group store at all (in-memory only). This is a functional gap, recorded as follow-up, not changed here.
2. v1 timeout: real Rust bug (lost transfer-task wakeup, read-level race), fixed parity-safely, confirmed by 2 reruns.
