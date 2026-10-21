# Matching SQL multi database routing audit (2026-10-21)

## Go behavior traced

Source inspected in this worktree after merging `e219c4730` (merge commit
`6bd475f4f`). Relevant implementation:

- `go/common/persistence/sql/sqldriver/connections.go`: `CreateDBConnections`
  creates one SQL connection per ordered `MultipleDatabasesConfig` entry and
  preserves the full driver shard array. It requires `NumShards > 1` and an
  exact entry count match.
- `go/common/config/config.go`: `SQL.SplitMultipleDatabases` copies shared SQL
  settings to each endpoint, sets per-endpoint `NumShards=1`, and overrides
  only user/password/database/address.
- `go/common/persistence/sql/sqlplugin/dbSharding.go`:
  task DB shard is `farm.Hash32([]byte(domainID + "_" + tasklistName)) %
  NumShards`; domain ID alone is used for domain-level sharding generally.
- `go/common/persistence/sql/sql_task_store.go`: task store selects the DB shard
  from domain ID and task-list name for every task-list operation.
- `go/common/persistence/sql/sql_domain_store.go` and its tests use
  `sqlplugin.DbDefaultShard` for domain metadata operations. With multiple DBs,
  that resolves to endpoint index zero, so Rust metadata routing must remain on
  pool zero even when task writes route to other endpoints.
- MySQL and PostgreSQL adapters share each endpoint's TLS, connect attributes,
  connection limits, and lifetime from the parent SQL config. The endpoint list
  itself carries credentials, DB name, and connect address.
- `go/common/config/persistence.go` also validates that multiple SQL DB config
  is enabled only in the advanced-visibility-only configuration mode. The SQL
  factory and stores nevertheless implement the ordered shard routing above.

## Rust changes

- `rust/crates/cadence-server/src/matching_task_store.rs` now builds the full
  MySQL and PostgreSQL endpoint lists from the existing config field shapes,
  validates Go's count and empty endpoint constraints, and propagates shared
  SQL/TLS settings to all endpoints. Single DB behavior stays on the existing
  path. Existing Cassandra SQL semaphore adapter behavior is untouched.
- `rust/crates/cadence-persistence/src/matching_mysql.rs` now opens one pool
  per ordered MySQL endpoint, applies inherited SQL settings to each pool,
  validates task schema on every database, and selects pools by the existing
  Go-compatible task-list shard hash. Domain metadata continues to share the
  first pool, matching `DbDefaultShard`.
- PostgreSQL's existing vector pool selector is now reached from production
  construction; its existing metadata source uses pool zero. TLS and SQL
  connect-attribute behavior remains inherited per shard.
- Added focused Rust source tests for SQL endpoint config validation and MySQL
  logical-to-physical shard selection. No Cargo or Go test/build was run due to
  the PG-owned validation lane constraint.

## Required validation before accepting this source

Run after explicit lane release, with all outputs and actual exit statuses
preserved:

1. Focused server config tests for `matching_task_store` and persistence
   adapter tests covering PostgreSQL and MySQL shard selection/connection
   settings.
2. Strict locked workspace lint and a production server build on the exact
   committed source SHA.
3. Real mixed Go/Rust SQL tests with two independently named databases and
   distinct credentials/addresses. Seed task-list keys that hash to each shard;
   prove each endpoint receives only its expected rows; prove domain metadata
   always reads/writes endpoint zero.
4. Verify TLS CA/client certificate and inline cert behavior, per-shard
   connect attributes, max connection/idle/lifetime behavior, first and later
   endpoint connection errors, restart/recovery, and `now_nanos` task expiry
   parity. Run the SQL semaphore adapter check to confirm SQL remains explicitly
   unsupported.
5. Preserve command lines, source hash, logs, exit files, sanitized config and
   Go oracle evidence in `memory/evidence/` before claiming parity acceptance.

`git diff --check` passed. No runtime or mixed-service parity claim is made.
