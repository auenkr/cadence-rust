# Matching source integration plan (2026-10-20)

This is a read-only integration review. No candidate branch was merged, no
root-worktree file was edited, and no build or service was run.

## Observed checkpoints and worktree state

- Root `master` is `7e22bd05f`; its working tree has user changes in
  `AGENTS.md`, `matching_registry.rs`, and `matching_runtime.rs`, plus an
  untracked `matching_shard_processor.rs`.
- Latest MySQL lane is `b5e6ba875` (`codex/matching-task-store-abstraction`).
  It descends from the validated MySQL source checkpoint and adds the later
  production MySQL gate fixes/evidence. Its worktree has an untracked
  `artifacts/` directory; do not stage its copied binaries as source.
- Telemetry candidate is `d645c1630` (`matching-telemetry-mysql-candidate`),
  with five uncommitted compiler/ownership adjustments in
  `grpc_matching.rs`, `matching_metrics_prometheus.rs`, `matching_reader.rs`,
  `matching_rpc_tests.rs`, and `matching_runtime.rs`.
- Runtime owner checkpoint is `0e22a68af` (`t3/matching-runtime-owner`).
- PostgreSQL source checkpoint is `532301c36` (`codex/matching-postgres-parity`).
- Cassandra/semaphore checkpoint is `60f0efcf8` (`t3/matching-semaphore-durable`),
  with seven uncommitted changes to the Cassandra adapter, shared domain
  metadata, the three SQL task-store adapters, queue, and task-store trait.

All candidate heads are outside root's current `master` history. The root
working tree is shared user state: leave `AGENTS.md` and its Matching source
changes intact. Before final integration, the owner should preserve those
changes in a checkpoint or explicitly carry them as a patch into a clean
integration worktree. Do not reset or wholesale-checkout the shared root files.

## Commit graph and overlap evidence

- MySQL `b5e6ba875` and telemetry `d645c1630` share `4bc9873b9`. Their changed
  path sets after that base have **zero overlaps**. The MySQL-only tail is
  focused on gate harness/evidence; telemetry contains the Metrics source and
  oracle work plus the earlier validated MySQL merge. This is the cleanest
  pair to combine first, with MySQL's newer gate evidence retained.
- MySQL and PostgreSQL share `55c4ae9ad`; their later path overlap is only
  `tests/matching-service-compat.cjs` and `tests/run-go-service-baseline.py`.
  Preserve both `--mysql` and `--postgres` configuration, persistence probes,
  and backend-specific evidence in those files.
- PostgreSQL and Cassandra share `55c4ae9ad`; their post-base path overlap is
  eight files: persistence `Cargo.toml`, `lib.rs`, `matching_domain_metadata.rs`,
  `matching_postgres.rs`; server `matching_domain_cache.rs` and
  `matching_task_store.rs`; and the two mixed-service harness files. These
  overlaps reflect the Cassandra branch's current shared-trait/domain-source
  changes, so PostgreSQL must be adapted to the final trait shape.
- Telemetry and PostgreSQL share `55c4ae9ad`; their post-base overlap is
  `matching_domain_cache.rs` and the two mixed-service harness files. Retain
  the PostgreSQL domain source wiring while applying telemetry instrumentation.
- Telemetry and Cassandra share `55c4ae9ad`; they overlap in twelve server
  files, including `grpc_matching.rs`, `matching_dispatch.rs`, `matching_reader.rs`,
  `matching_registry.rs`, `matching_runtime.rs`, plus both mixed-service
  harnesses. This is the largest manual reconciliation area.
- Cassandra's current uncommitted `create_tasks_with_domains` signature adds
  `now_nanos`; this changes `MatchingTaskStore`, `DurableTaskQueue`, and every
  backend adapter. Do not integrate PostgreSQL adapter code against the old
  signature and then overwrite it with Cassandra changes. Freeze this shared
  trait contract first, then adapt PostgreSQL/MySQL/SQLite implementations
  consistently.

## Recommended additive integration sequence

1. Create/use an isolated integration worktree based on the current root
   checkpoint, after the root owner has preserved the user edits. Keep the
   shared root worktree untouched during conflict resolution.
2. Bring in the validated MySQL checkpoint `b5e6ba875` as the production
   persistence base. Retain its corrected gate, acknowledgements/GC evidence,
   and persistence adapter code.
3. Apply the root's uncommitted owner/runtime source deliberately. The root
   already has `qps_for_task_list_name` and `stop_task_list_name` additions
   equivalent to the older implementations in the candidates, so keep one
   implementation. Keep the root `matching_shard_processor.rs` version with
   its injected clock and focused owner tests; the candidate copies have the
   older `Instant`-only version. In `matching_runtime.rs`, retain the
   domain-cache hookup to `Matching` exactly once per constructor; inspect the
   duplicate `with_domain_cache` line in the current root working diff before
   integrating instead of silently dropping the user change. Preserve the
   AGENTS.md wording change.
4. Freeze the Cassandra shared persistence contract after its current source
   audit. Merge/adapt Cassandra semaphore persistence, its async/blocking
   boundary and task-store signature before porting the PostgreSQL adapter to
   that final trait. Keep SQL semaphore operations explicitly unsupported as
   Go does. Avoid copying Cassandra's current uncommitted persistence files
   over the newer MySQL or PostgreSQL code; reconcile method signatures and
   domain metadata changes file by file.
5. Apply PostgreSQL store/domain-source changes from `532301c36` against the
   frozen shared persistence API. Preserve the `sslmode=allow` plaintext-first
   semantics, CA/client certificate precedence, Go domain DataBlob decoding,
   and PostgreSQL handoff harness. Merge its two overlapping probe files with
   the MySQL gate rather than choosing either version wholesale.
6. Integrate telemetry source from `d645c1630` and its five current working
   adjustments after backend/runtime shapes have settled. It is source-wide
   across RPC, queue, reader, runtime, and metric exporters. Preserve telemetry
   oracle fixtures and port-specific tests while resolving against the
   owner/runtime edits and Cassandra async boundary.
7. Bring in only missing two-host/runtime-owner harness fixes from
   `0e22a68af`; the root has already recreated core registry/processor work,
   while MySQL/telemetry candidate histories also contain older copies. Avoid
   merging a second stale copy of the same core implementation.
8. Once the shared validation lane opens, run strict Clippy, focused owner,
   persistence, and metrics tests, then production build. Run separate actual
   MySQL and PostgreSQL mixed-service gates against their respective Go
   schemas, then Cassandra failure/restart tests. Record source commit hashes
   with each result. No service migration is complete based on source merges
   or harness syntax checks alone.

## Hot files to resolve manually

- `matching_registry.rs`, `matching_runtime.rs`, and
  `matching_shard_processor.rs`: root user changes collide with all source
  candidates; retain the root's semantic additions and add only missing
  backend/telemetry wiring.
- `cadence-persistence/Cargo.toml`, `matching_task_store.rs`, `matching_queue.rs`, `matching_domain_metadata.rs`,
  `matching_mysql.rs`, `matching_postgres.rs`, `matching_cassandra.rs`, and
  `matching_sqlite.rs`: Cassandra changes the common persistence contract and
  backend boundaries; merge by trait method and Go behavior rather than file
  replacement.
- `grpc_matching.rs`, `matching_dispatch.rs`, `matching_reader.rs`, and
  `matching_rpc_tests.rs`: telemetry and Cassandra change overlapping async
  call paths. Preserve RPC ownership, task timing, and error behavior while
  adding metric context.
- `tests/matching-service-compat.cjs` and
  `tests/run-go-service-baseline.py`: retain both SQL backend flags and their
  probes, MySQL GC evidence, PostgreSQL Go-consumption handoff, and production
  topology metadata.
- `AGENTS.md`: retain root user edits verbatim.

This plan is about combining the current Matching candidates. It does not
authorize starting History or another service migration.
