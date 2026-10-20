# Matching PostgreSQL runtime parity audit (2026-10-20)

Scope: source-only audit against the pinned Go implementation and current Rust
Matching runtime at `55c4ae9ad`. No compile, lockfile resolution, database,
SDK, restart, or recovery validation was performed. The MySQL candidate remains
untouched in its own worktree.

## Current Rust state

- `cadence-persistence::matching_postgres::PostgresTaskStore` implements
  Matching task CRUD over Go's existing PostgreSQL `tasks` and `task_lists`
  tables and validates their columns and primary keys. It does not create or
  migrate schema.
- Its connection options accept `sslmode=disable`, `connect_timeout`,
  `application_name`, and `options`. Any TLS sslmode or TLS file parameter is
  currently rejected. The adapter has no Go `TLSConfig` input.
- `cadence-server::matching_task_store::open_configured_default_store` only
  routes `sqlite` and `mysql`. Consequently `pluginName: postgres` can parse as
  config but cannot start Rust Matching through the configured production
  persistence path. Existing PostgreSQL task adapter code is not production
  runtime integration.
- Domain metadata source implementations exist for SQLite and MySQL only.
  PostgreSQL does not supply `metadata_version`, `list_domains`, or `get_domain`
  to the Matching cache. The cache needs these to hydrate domain UUID/name,
  status, task-list settings and replication configuration from Go's durable
  `domains.data` payload. Task-store availability alone is insufficient.

## Go contracts verified in source

- `go/common/persistence/sql/sqlplugin/postgres/plugin.go` creates standard
  `database/sql` pools using `maxConns`, `maxIdleConns`, and
  `maxConnLifetime`; it installs snake_case struct mapping. PostgreSQL's
  default/admin database name is `postgres` only when the SQL config's name is
  empty, but general Go config validation requires a database name for this
  non-SQLite plugin. Thus Rust Matching's production default-store path should
  follow validated config and require a nonempty name.
- Go `Persistence.FillDefaults` changes `nShards: 0` to 1. It does not
  normalize negative values. The current factory's `if n_shards == 0 { 1 }`
  matches this default; negative values must be rejected. For a non-sharded
  database Go's SQL driver ignores `nShards` for physical connections, while
  the task-list row still uses the configured logical shard count as the
  routing/hash modulus. Sharded DB mode requires `nShards > 1` and exactly
  that many `multipleDatabasesConfig` entries.
- `go/common/config/persistence.go` permits `UseMultipleDatabases` only for
  advanced-visibility-only config, not ordinary default/visibility stores.
  A Rust Matching service opening the Go default store should reject this
  shape rather than attempt routing; it should also not silently claim support
  based on the adapter's lower-level multi-pool capability.
- `registerTLSConfig` in the Go PostgreSQL plugin sets `sslmode=disable` when
  TLS is absent/disabled. When TLS is enabled it uses configured `sslMode`,
  defaulting to `require`, and sets `sslrootcert`, `sslkey`, and `sslcert` from
  `caFile`, `keyFile`, and `certFile`. It then applies `connectAttributes`,
  which override these parameters by key. Rust TLS must preserve that
  precedence and lib/pq's mode semantics; mapping only a CA file while
  ignoring `verify-ca`, `verify-full`, client cert/key, or overrides is not
  parity. PostgreSQL's Go plugin does not use MySQL-only `caFiles`,
  `enableHostVerification`, or `requireClientAuth` fields.
- Go schema `go/schema/postgres/cadence/schema.sql` stores durable domain
  metadata in `domains` (`shard_id` default 54321, 16-byte `id`, `name`, raw
  ThriftRW `data`/`data_encoding`, `is_global`) and one global
  `domain_metadata.notification_version` row. The Rust metadata reader should
  preserve the raw Go nested DataBlob and use the same paging identity contract
  as Go (`id > token`, ascending id, page-size limit); this repository's
  existing decoder is the shared source of domain payload semantics.

## Required next implementation slice

1. Add PostgreSQL conversion and production selection in
   `open_configured_default_store`, preserving Go defaulting and rejecting
   invalid/multi-database default-store configuration.
2. Implement PostgreSQL TLS through a connector supported by the selected
   synchronous PostgreSQL client. Reproduce the effective Go DSN precedence
   (TLS defaults first, explicit connect attributes last), support the Go
   `sslmode` variants and CA/client identity files, and fail startup on invalid
   credentials/certificates. Do not substitute `NoTls` for enabled TLS.
3. Implement a read-only PostgreSQL domain metadata source on the same bounded
   pool as task operations (or enforce a shared connection budget). Wire it to
   the existing cache refresh and cache-miss path. Ensure metadata reads count
   against configured `maxConns` as in Go's datastore pool.
4. Exercise configured Rust Matching against Go PostgreSQL schema with Go
   Frontend/History/Worker, then SDK workflow, graceful restart/recovery, task
   lease, and GC checks. Record exact setup and results before calling this
   service complete.

## Explicit remaining gaps

PostgreSQL TLS, PostgreSQL domain metadata reads/cache integration, configured
runtime startup, and end-to-end mixed Go/Rust validation remain incomplete.
Multi-database persistence is not appropriate for the default store under Go's
configuration validation and remains unsupported for Matching. Cassandra and
other NoSQL stores remain outside this SQL-only Matching work slice.
