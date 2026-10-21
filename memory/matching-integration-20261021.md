# Matching integration branch (2026-10-21)

Branch `t3/matching-integration-20261021`, worktree
`/home/ubuntu/.t3/worktrees/cadence-rust/matching-integration-20261021`.
Root checkout (`/home/ubuntu/github/auenkr/cadence-rust`) user WIP was not touched.

## Candidate inputs (heads at integration time)

| candidate | head used | notes |
|---|---|---|
| telemetry + MySQL gate | `864dcdbe9` (`matching-telemetry-mysql-candidate`) | base; StatsD/M3 + MySQL production evidence |
| PostgreSQL | `e3cfca7a6` (`codex/matching-postgres-parity`) | brief said `a8db27ab1`; branch moved on, includes `a8db27ab1` |
| Cassandra | `82dc7e32a` (`t3/matching-cassandra-protocol`) | brief said `3495c39d5`; branch moved on, includes it |
| SQL multi-DB fix | `695198be0` | already present in Cassandra branch as cherry-pick `ed9254721` (`git cherry` shows `-`) |

Note: `tests/matching-mysql-gate.cjs` is a library the mixed runner loads, not a
standalone runner. The MySQL mixed run is `tests/run-go-service-baseline.py --mysql`.

## Commits

- `a4eb44ce3` merge PostgreSQL candidate. Conflicts: `matching_partition.rs` (take
  PG trait-object imports), `matching_registry.rs` (whitespace only),
  both harnesses resolved by porting PG deltas onto telemetry harnesses.
- `589bbef08` merge Cassandra candidate. Harness conflicts resolved by taking the
  Cassandra side, then restoring telemetry-only pieces the auto-merge dropped:
  MySQL `awaitPersistedTask` helper, `restartState`, the three `mainDecision /
  mainActivity / final` watermark reads, restart `durableWatermarkBeforeKill`,
  shutdown evidence in the `finally` block, and MySQL/PG branches of the
  task observer (`sqliteTaskState` dispatches to `durableTaskCount`).
- `b8b76e8d4` regenerate `Cargo.lock` (merged deps `scylla` patch, openssl, etc.).
  `cargo update` ran with network access; `--locked` could not pass before this.
- Submodules `go/idls` (bfbf273) and `cadence-web` (be95f6ff) initialised.
  `cadence-web/node_modules` is a read-only symlink to the root checkout's install.

## Compile fixes (cadence-persistence)

The Cassandra branch never compiled on its own (its semaphore trait gained
`get_semaphore_metadata`, SQL stores did not implement it, and `semaphores.rs`
had a duplicate `release_semaphore_token`). Fixed:

- duplicate `release_semaphore_token` removed (kept the documented copy);
- MySQL/PostgreSQL/SQLite stores delegate `get_semaphore_metadata` to
  `SqlUnsupportedSemaphorePersistence` (same as their other semaphore methods);
- manual redacting `Debug` impls for the three SQL task stores (trait supertrait);
  `#[async_trait]` must stay on the `SemaphorePersistence` impl, not the Debug impl;
- Cassandra UDT field `r#type` renamed to `type_` (`#[scylla(rename = "type")]`
  keeps the wire name; scylla derive panicked on raw identifiers);
- `scylla::value::Row` has `columns` (not `first()`) in scylla-cql-core 1.9;
- dead `task_udt` and unused `HashMap` import removed;
- interop test calls pass the `now_nanos` argument added by the PG/telemetry lineage.

`cargo check -p cadence-persistence --locked --all-targets` after these: exit 0,
log `memory/evidence/matching-integration-20261021/persistence-check/`
(see the evidence directory for the exact log and exit file).

## Still open

- Full `cargo clippy --workspace --all-targets --locked -- -D warnings` and
  `cargo test` (persistence, server): see `memory/evidence/matching-integration-20261021/rust-gate/`.
- Mixed MySQL gate: `tests/run-go-service-baseline.py --rust-matching --matching-restart
  --rust-production-binary <integration release binary> --binary <frozen Go> --mysql
  --history-transport grpc`, with `CADENCE_MYSQL_DOCKER_CONTAINER=cadence-mysql-test`.
  Not yet run.
- PostgreSQL and Cassandra mixed gates: not run in this pass.
