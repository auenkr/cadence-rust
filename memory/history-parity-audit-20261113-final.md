# History final parity audit (2026-11-13), branch history-finalaudit, base 26e4e3e73

Method and limits: package-level mapping of `go/service/history/**` to `rust/crates/cadence-history/src/**`, carried forward from
`history-parity-audit-20261112-v2.md` (per-function scan, 234 renamed/private-helper misses, all manually classified) and
`history-p2rest-20261112.md`. No new line-by-line re-read of every Go file was done in this pass; no new P0/P1 gap was found, so no
Rust logic was changed. No cargo/go/docker ran locally. DONE = Go behaviour present and validated by CI/mixed gate.

| Go package | Rust pointer | Status |
|---|---|---|
| `.` (service.go, factory) | `service.rs`, `launcher.rs`, `main.rs` | DONE |
| `common` | `common.rs` | DONE |
| `config` | `config.rs`, `config_collection.rs`, `openfeature_dynamic.rs` | DONE |
| `constants` | `constants.rs` | DONE |
| `decision` | `decision/` (task_handler.rs dispatch, checker.rs) | DONE (handler fn names differ) |
| `engine`, `engine/engineimpl` | `engine/`, `wiring/assembly/engine_*.rs` | DONE; P2 repair-by-rebuild on checksum mismatch not on engine path |
| `events` | `events/` | DONE |
| `execution` | `execution/`, `mutable_state*.rs` | DONE; `startTransactionHandleDecisionFailover` ported in history-decfailover |
| `failover` | `failover/`, `wiring/assembly/failover.rs` | DONE |
| `handler`, `wrappers/{grpc,thrift,ratelimited}` | `handler/`, `wrappers/` | DONE |
| `lookup` | `lookup.rs` | DONE |
| `ndc` | `ndc/` | DONE; P2 notifier classification of NDC `write_failed`/conflict-resolve/CreateHistoryTasks |
| `query` | `query/` | DONE |
| `queue`, `queuev2` | `queue/`, `queuev2/` | DONE |
| `replication` | `replication/` | DONE |
| `reset` | `reset/` | DONE |
| `resource` | `wiring/`, `service.rs` | DONE |
| `shard` | `shard.rs`, `shard_context.rs` | DONE |
| `task` | `task/` | DONE |
| `taskdlq` | `taskdlq/` | DONE |
| `workflow` (context/util/errors) | `mutable_state.rs`, `engine_cache.rs` | DONE |
| `workflowcache` | `workflowcache/` | DONE |

## P0 / P1
None known open.

## P2 (listed, not fixed)
- Checksum mismatch: detection/metrics/logs only, no repair-by-rebuild on the engine path.
- Task notifier classification for NDC `write_failed`, conflict resolve, CreateHistoryTasks.
- Non-blob JSON tag mismatches (HTTP/JSON surfaces only); real S3/GCS network clients need credentials.
- Test volume gaps in engine/events/query/queue; stale `TODO(link)` comments.

## Failure / restart / recovery coverage
Existing gate (`tests/gates/run-frontend-gate.py`): two_history kill+restart of host 2 during SDK churn (`history-churn`), Rust Frontend
kill/restart with signal/query/complete, scenario phase (retry/cron timeout timers, idempotency, archival, bad-binary reset), Kafka and
ES restarts. Missing before this branch: kill of every History host, kill of the primary host, DB restart, and verification of in-flight
timers/activities/decisions after recovery.

Added: `FE_MODE=recover` driver (`tests/frontend-gate/recover.go`) + `recovery phase` in the harness, run by every
`history-mixed-gate.yml` combo. Per round the SDK holds an in-flight 30s timer workflow, a 20s activity (with retry policy), an
activity whose start-to-close timeout must fire after recovery, a signal workflow (one signal before, one after), a started but
unanswered raw decision (must time out then be completed), and a workflow started during the failure.
Rounds: `killall` (SIGKILL all History hosts, restart), `dbrestart` (`docker restart` of the MySQL/Postgres/Cassandra container),
`killprimary` (two_history only: kill host 1, host 2 takes over shards, restart host 1). Steps are named `recover-<round>-*`
and `sdk-recover-*`; logs in artifact `run/sdk-recover.log`.

## Evidence
Head SHA (gated): 5f7bb28ef22ce852cd42fae482871f32209ae91d (origin/master 26e4e3e73 already an ancestor). Rust binary SHA256 (mysql/grpc/two_history=true run):
cadence-server 1eb0d414e533f3a62c0d95d8a1175acf8b7e7f50ba0d93d3154faa4191c45789, cadence-history b4aa0f1e6a6a06ec0fb577da3bc8cbe1cd0a177377c07e9b78062d30f552cc62
(each run uploads its own `rust-binaries.sha256` in artifact `history-mixed-gate-<db>-<outbound>`).
rust-ci (clippy + workspace tests): https://github.com/auenkr/cadence-rust/actions/runs/43385294461 success.

## Gap found and fixed by the new recovery round (P1)
First gate pass (head 708fdc3a4): postgres and cassandra green, all four MySQL combos FAILED. `killall` passed, but after `dbrestart`
Rust History logged `CodecError { IO error: Broken pipe }` for ~50 min and no workflow completed (`sdk-recover-driver-timeout`; archival and
rate-limiter steps failed as knock-on). Cause: `cadence-persistence/src/history_sql_backend.rs` returned connections broken by a DB restart to the
pool. Go `database/sql` drops connections that report `driver.ErrBadConn`; Matching already did (`matching_mysql.rs ConnectionLease.discard`).
Fix (5f7bb28ef): `MySqlTx` wrapper + `ConnectionLease::discard_flag()` poison the lease on `is_connectivity_error()`.
Two clippy errors from master were also fixed (`matching/src/grpc.rs` unreachable_code, `history/src/task/task_util.rs` needless_bool).

## Gate runs on 5f7bb28ef (all `GATE SUMMARY failed: []`)
| db | outbound | two_history | run |
|---|---|---|---|
| mysql | grpc | true | https://github.com/auenkr/cadence-rust/actions/runs/43387070567 |
| mysql | grpc | false | https://github.com/auenkr/cadence-rust/actions/runs/43387073967 |
| mysql | tchannel | true | https://github.com/auenkr/cadence-rust/actions/runs/43387077498 |
| mysql | tchannel | false | https://github.com/auenkr/cadence-rust/actions/runs/43387081460 |
| postgres | grpc | true | https://github.com/auenkr/cadence-rust/actions/runs/43387085040 |
| postgres | grpc | false | https://github.com/auenkr/cadence-rust/actions/runs/43387088807 |
| postgres | tchannel | true | https://github.com/auenkr/cadence-rust/actions/runs/43387092336 |
| postgres | tchannel | false | https://github.com/auenkr/cadence-rust/actions/runs/43387096285 |
| cassandra | grpc | true | https://github.com/auenkr/cadence-rust/actions/runs/43387100686 |
| cassandra | grpc | false | https://github.com/auenkr/cadence-rust/actions/runs/43387104331 |
| cassandra | tchannel | true | https://github.com/auenkr/cadence-rust/actions/runs/43387108919 |
| cassandra | tchannel | false | https://github.com/auenkr/cadence-rust/actions/runs/43389357186 (re-dispatched after an HTTP 500 on dispatch) |
Round coverage: `killall`, `dbrestart` (all DBs), `killprimary` (two_history=true only).
