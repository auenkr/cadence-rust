# Matching PostgreSQL runtime parity implementation and validation status (2026-10-20)

Scope: source-only implementation against pinned Go behavior from `55c4ae9ad`.
No compile, lockfile resolution, database, SDK, restart, or recovery validation
was performed because the build lane is reserved. The MySQL candidate remains
untouched in its own worktree.

## Current Rust state

- `cadence-persistence::matching_postgres::PostgresTaskStore` implements
  Matching task CRUD over Go's existing PostgreSQL `tasks` and `task_lists`
  tables and validates their columns and primary keys. It does not create or
  migrate schema.
- `open_configured_default_store` now routes `postgres` to the task adapter,
  passes Go pool/shard/encoding settings, and attaches a read-only PostgreSQL
  domain source. Zero `nShards` defaults to one, negative values fail, and
  multiple-database default-store config is rejected.
- The PostgreSQL domain source reads notification version and Go domain rows,
  validates UUIDs, preserves Go's page token order, decodes the existing
  nested ThriftRW DataBlob, and shares the task pool so reads count against the
  same `maxConns`. Cache source timeout errors are retryable like Go PostgreSQL
  persistence failures.
- PostgreSQL TLS settings now flow from Go-shaped `TlsConfig`. TLS defaults to
  `require` when enabled; supported modes include `disable`, `allow`, `prefer`,
  `require`, `verify-ca`, and `verify-full`. Since the synchronous PostgreSQL
  driver has no `allow` mode, the adapter tries plaintext first and retries
  over TLS only when PostgreSQL reports the specific `SSL off` pg_hba failure.
  CA/client cert/key fields and `connectAttributes` precedence are applied by
  the adapter. `verify-ca`
  suppresses only OpenSSL hostname-mismatch verification while retaining CA
  chain checks.
- `tests/run-go-service-baseline.py --postgres` emits Go and Rust configs
  against the same PostgreSQL databases, uses Go `--auto-setup` to seed schema,
  runs Rust Matching with Go Frontend/History/Worker and the production restart
  probe, then starts all Go services and runs an SDK workflow probe. PostgreSQL
  queue row inspection uses `psql`; this harness source has not been executed.

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

## Required validation slice

1. Resolve and review PostgreSQL/OpenSSL dependency versions, compile the
   persistence/server crates, and run targeted adapter/cache tests after the
   build lane is released. The workspace Cargo.lock is intentionally not
   updated in this source-only change.
2. Exercise configured Rust Matching against Go PostgreSQL schema with Go
   Frontend/History/Worker, then SDK workflow, graceful restart/recovery, task
   lease, and GC checks. Record exact setup and results before calling this
   service complete.
3. Verify the Go-only probe reopens and reads the same Go domain row, the
   Rust-written task-list row, and the completed workflow history after Rust
   exits. This does not assert Go consumption of a deliberately queued pending
   task row.

## Explicit remaining gaps

Compilation and all runtime behavior remain unverified. PostgreSQL TLS and
domain source code are implemented but must be exercised against real server
certificates and Go-seeded metadata. The SQL harness expects the caller to
provision PostgreSQL. It does not yet prove Go Matching consumed a Rust-produced
pending queue item. Multi-database persistence is not appropriate for the
default store under Go's configuration validation and remains unsupported for
Matching. Cassandra and other NoSQL stores remain outside this SQL-only slice.

## Reproducible validation command (pending build lane)

```sh
CADENCE_POSTGRES_ADDR=127.0.0.1:5432 \
CADENCE_POSTGRES_USER=postgres \
CADENCE_POSTGRES_PASSWORD=cadence \
CADENCE_POSTGRES_DATABASE=cadence \
CADENCE_POSTGRES_VISIBILITY_DATABASE=cadence_visibility \
python3 tests/run-go-service-baseline.py \
  --postgres --rust-matching --matching-restart \
  --rust-production-binary /path/to/cadence-rust \
  --artifacts-dir /tmp/cadence-matching-postgres
```

For TLS validation, also set `CADENCE_POSTGRES_SSLMODE` and, as needed,
`CADENCE_POSTGRES_CA_FILE`, `CADENCE_POSTGRES_CERT_FILE`, and
`CADENCE_POSTGRES_KEY_FILE`. Adapter tests and the mixed Go/Rust run must pass
before this audit can be treated as parity evidence.

## Source checkpoint update (2026-10-20, still unexecuted)

- Tightened the Rust PostgreSQL adapter's `sslmode=allow` unit coverage. It now
  asserts that configuration selects plaintext first, keeps a TLS `require`
  retry configuration, and that `connectAttributes.sslmode` overrides the TLS
  block as Go applies it. The connection pool retries TLS only when the
  plaintext attempt returns PostgreSQL's explicit `SSL off` authentication
  rejection; all other errors are returned without a TLS retry.
- Extended the PostgreSQL mixed-service source harness to leave a new decision
  task pending after Rust Matching has persisted it through the real Go
  History service. It records a domain-scoped PostgreSQL row count before
  shutdown, starts original Go Frontend/History/Matching/Worker against that
  schema, and has the existing Go SDK worker consume and complete that task.
  The verifier then requires exactly one completion with the expected result.
  This closes the prior *harness-source* gap; it is not runtime evidence.
- `node --check` passed for both changed JavaScript probes, `python3 -m py_compile`
  and `git diff --check` passed for the runner, and `rustfmt --check` passed for
  the Rust adapter source. No Cargo, Go, Docker, PostgreSQL, or mixed-service
  command was run in this source-only lane. The mixed PostgreSQL test remains
  unexecuted and must be run in the shared validation lane before accepting
  backend parity.

## Status checkpoint (2026-10-21, WIP, mixed PostgreSQL gate NOT passed)

- HEAD chain: ba3a84444 (observer task-type) -> d7a2f1c20 (range expectations) -> 732979b5d/2cc47c373 (verifier block + payload decode) -> d133439452 (observer timeout 120s).
- Go lib/pq v1.10.2 ssl.go confirmed: require (+existing sslrootcert => verify-ca), verify-ca, verify-full, disable only; allow/prefer rejected. Rust TLS mode mapping (58d1d1e32) matches.
- Rust tree unchanged since a8db27ab1; frozen binary sha256 4fbc0eb3...c40 in cadence-rust-evidence (built from a8db27ab1).
- Gate attempts 3-7 failed on harness bugs now fixed: missing evidence path, stale range expectation (Go DescribeTaskList leases and advances range_id), raw vs JSON-decoded SDK result, observer timeout under load.
- Attempt8 failed: go build -p 1 of cadence-server timed out after 1200s (host load ~7-8 on 2 CPUs from another worktree's cargo/rustc). No Go services were started in attempt8.
- Remaining: rerun gate when host is idle; full clippy/persistence tests logs; SIGKILL/SIGTERM and pending-task exactly-once evidence; cleanup of cadence-rust-matching-pg-parity-20261021 container after evidence.
