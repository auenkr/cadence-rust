# Matching gate run 3 (2026-10-23): Cassandra, shard-distributor fixture, real shard-distributor, Thrift

Branch `matching-gate-run3-20261023` (from origin/master 67f95cb78). Gate runs used the harness at the
commits listed in `git log`; the Cassandra run used the harness at `a3dd01500` + `51ed69333`-era fixes
(only the persistence-target guard at that point). Later harness edits only touched shard-distributor,
Thrift and domain-registration paths. The Rust binary and Go server never changed during the run.

## Binaries (built once, `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-final`, one worktree)

See `binary-shas.txt`.

| Binary | sha256 |
| --- | --- |
| `cadence-matching` (Rust, debug) | `7dc23ec456f73478d61f4682f1a2231bab35a6dde4add15b36709c5968f1cf45` |
| `cadence-server` (Go) | `620e012a2a4d9f5e438ca2d67cde0835cb6deced55489ef4d1d7222e53e4f615` |
| `shard-distributor-real` (Go launcher) | `7d017389fe72959fd4499f6a6862e259dcd488745b11f3499e3db5a93c490879` |

Build: `python3 tests/run-go-service-baseline.py --rust-services matching --build-only --artifacts-dir .../g6/build`
(cargo via `cargo-safe`, Go build via the harness). Exit 0. Note: do NOT wrap `--build-only` or
`--real-shard-distributor` runs without `CADENCE_SD_REAL_LAUNCHER` in an outer `flock /tmp/cargo.lock`:
the harness takes the same lock itself and self-deadlocks (this cost ~50 min once).

## Results (real exit codes)

All four lanes: `NODE_PATH=/mnt/storage/service/node-grpc/node_modules`, run as
`flock /tmp/cargo.lock work-safe <cmd>`, exit code recorded in `exit-codes.txt` and each lane's `run.exit`.

| Lane | Command (abridged) | Exit | probe result |
| --- | --- | --- | --- |
| Cassandra | `tests/gates/run-cassandra-lane.sh g6/cass bin/cadence-matching --binary build/cadence-server --history-transport grpc` | 0 | `passed`, `acceptanceEligible: true` |
| Shard-distributor (fixture) | `--rust-services matching --shard-distributor` | 0 | `preliminary-passed` |
| Shard-distributor (real, etcd) | `--rust-services matching --real-shard-distributor` (`CADENCE_SD_REAL_LAUNCHER=bin/shard-distributor-real`) | 0 | `preliminary-passed` |
| Thrift (MySQL) | `tests/gates/run-mysql-lane.sh g6/thrift bin/cadence-matching --binary ... --matching-transport thrift --history-transport grpc` | 0 | `preliminary-passed`, `matchingTransport: thrift` |

Per scenario (each lane's final probe JSON, see `<lane>/probe.log` last line): idle eviction and recovery
passed; graceful SIGTERM drain with durable task rows (1 row before, 0 after recovery) passed; SIGKILL
restart recovery passed; query dispatch passed; isolation-group dispatch with CancelOutstandingPoll
passed; native partition config passed; existing Go SDK samples (`go-sdk.log`) passed. Cassandra also
passed two-node ring, typed non-owner ABORTED, owner SIGKILL takeover, semaphore owner/token rows and
domain pagination. Fixture lane `shardDistributor` evidence: heartbeat before/after SIGKILL and SIGTERM,
ownership reassigned, executor stopped on drain (`ExecutorStatusDRAINING`), SD restart re-registration.
Real lane evidence: `executor-expired-by-ttl` after SIGKILL (6099 ms), plus the same heartbeat,
reassignment, drain and SD-restart checks against the real Go leader loop and etcd.

`goSchemaOnly` for the Thrift/MySQL lane: no missing Go tables, no private tables.

These are lane gates only; they do not replace the service parity audit.

## Bugs found

Harness bugs (all fixed, tests only; no Rust production code changed):

1. Fresh worktree has no `cadence-web` submodule; probe needs `NODE_PATH` for `@grpc/grpc-js`.
2. Production drain check rejected the Cassandra gate (no SQLite/MySQL/PostgreSQL env): added `cassandraGate`.
3. Fixture SD lane: assignment went to the freshest executor, flapping between the two Rust nodes and
   reloading task lists (DescribeTaskList reported the 100000 default rate). Now pinned to the executor
   whose heartbeat metadata `grpc` port is the probe's primary.
4. Real SD lane (never run before): the real leader loop picks owners, so the probe asks `GetShardOwner`,
   waits for that executor to report READY and sends direct Matching RPCs to its gRPC port. Go skips the
   SD ownership check for short-lived task lists (`errIfShardOwnershipLost`, hash-ring owner), so on a typed
   `not owned by this host` ABORTED the probe follows `OwnedBy` and retries.
5. Real SD expiry check waited for every fresh executor, including the live secondary; now only the
   SIGKILLed primary's executor.
6. Thrift lane: Thrift `DescribeTaskListResponse` has no `partitionConfig` (Go's thrift mapper drops it),
   so the probe reads it through Matching's gRPC `DescribeTaskList` when `CADENCE_MATCHING_TRANSPORT=thrift`.
7. Go-side race (not Rust): worker `cadence-system` registration vs probe `RegisterDomain` on MySQL
   (`Failed to update domain metadata. <>1 rows affected`). Probe retries.
8. Fixture SD lane first attempt: `production idle probe poll was not registered` once (cause: bug 3's
   executor flapping); not seen after the pin. Diagnostics kept in the error message.

Also: `tests/gates/run-mysql-lane.sh` added (limited container `--memory 1800m --cpus 1.5 --pids-limit 2048`,
removed in an EXIT trap); `CADENCE_SD_REAL_LAUNCHER` lets a gate hold the cargo lock for its whole run;
`tests/__pycache__/*.pyc` untracked and ignored.

Real Rust bugs found by these four lanes: none. The Rust `not owned by this host` ABORTED for short-lived
lists matches Go's ring fallback.

## Known weaknesses

- Fixture lane `after-sigkill-restart` heartbeat check accepts stale (<10 s) executors from before the kill.
- Cassandra lane was run once after the fixes; the later probe edits were not re-run on Cassandra.
- Debug binary, not release.

`failed-attempts/` has the probe log and exit code of each failed attempt, in order.
