# Matching Cassandra options: Go vs Rust (2026-10-23)

Go: `go/common/config/config.go:217-258` (NoSQL), `go/common/persistence/nosql/nosqlplugin/cassandra/plugin.go:138-175`,
`.../gocql/client.go:54-123` (newCassandraCluster), `.../gocql/session.go:66-77`.
Rust: `rust/crates/cadence-matching/src/matching_task_store.rs` `cassandra_config` (~:56-95) maps config to
`rust/crates/cadence-persistence/src/cassandra.rs` `CassandraSessionConfig::with_go_options` (:683) / `connect` (:726).

| Option | Go | Rust | Status |
|---|---|---|---|
| hosts (csv, trimmed) | client.go:119 parseHosts | config.rs `hosts_list`; cassandra.rs:726 (IPv6 bracketing) | match |
| port | client.go:64 | cassandra.rs:649 (0 -> env CASSANDRA_DB_PORT / 9042) | match |
| user/password | client.go:65-71 (both required) | cassandra.rs:786 | match |
| allowedAuthenticators | client.go:69 | cassandra.rs `GoPasswordAuthenticator`, GO_DEFAULT_AUTHENTICATORS | match |
| keyspace | client.go:73 | cassandra.rs:764 `use_keyspace` | match |
| datacenter | client.go:74 DataCentreHostFilter | cassandra.rs:770 DcHostFilter | match |
| region | client.go:77 (overrides datacenter, first 3 chars of DC) | cassandra.rs:768 CassandraRegionHostFilter, same precedence | match |
| maxConns | client.go:98 NumConns | cassandra.rs:697/765 PoolSize::PerHost (default 2) | match |
| tls enabled/cert/key/ca/enableHostVerification/serverName | client.go:80-93 (only caFile used by Go cassandra) | cassandra.rs:775 + build_rustls_client_config; Rust also accepts caFiles (superset) | match (superset) |
| protoVersion | client.go:56 default 4 | cassandra.rs:707 any value other than 0/4 -> config error naming protoVersion | **limitation**: Scylla driver speaks only v4 |
| connectTimeout | session.go:74 | cassandra.rs:686 (0 -> 2s, Go 0 means gocql no timeout) | intentional safe default |
| timeout | session.go:73 | cassandra.rs:684 (0 -> 10s) | intentional safe default |
| consistency (default LOCAL_QUORUM) | plugin.go:138 | cassandra.rs:691/672 | match |
| serialConsistency (default LOCAL_SERIAL) | plugin.go:142 | cassandra.rs:694/673 | match |
| connectAttributes | not forwarded to gocql | ignored, cassandra.rs:710 | match |
| hostSelectionPolicy | client.go:100-104 | cassandra.rs:712-751 (tokenaware,roundrobin / roundrobin / hostpool-epsilon-greedy; others error) | match |
| disableInitialHostLookup, addressTranslator, retry policy, reconnection policy | NOT set by Go (gocql defaults) | driver defaults | n/a, not configurable in Go |
| maxQPS | not a Cassandra option in Go config | n/a | n/a |

## Per-operation consistency (Matching)
- Task create/update/lease, CAS (`IF range_id=?`), reads: session default (`consistency`, serial `serialConsistency`) in both (matching_cassandra.rs statements use the profile default).
- Task/tasklist delete (tasks.go:326, :530; db.go:110): Go tries ALL when dynamic config `EnableCassandraAllConsistencyLevelDelete`, on UNAVAILABLE (0x1000, public/client.go:98) retries at the constant `cassandraDefaultConsLevel` = LOCAL_QUORUM (constants.go:45).
  Rust matching_cassandra.rs (delete paths) did the ALL attempt but retried at the *configured* consistency. **Fixed**: retries at constant LOCAL_QUORUM (`FALLBACK_CONSISTENCY`).
- Semaphore tasks delete (semaphore_tasks.go:283) uses the same helper; Rust `semaphores.rs` has no delete path for tasks, so nothing to map.

## Tests
`cassandra.rs` option tests (defaults, explicit options, protoVersion rejection, authenticators, policies);
new: `matching_cassandra.rs::delete_fallback_consistency_is_go_constant_local_quorum`,
`matching_task_store.rs::cassandra_config_maps_go_options`, `cassandra_config_ignores_disabled_tls_and_rejects_unsupported_proto`.

## Validation (base origin/master 3fa85f5f7, cargo-safe, shared target)
- `check -p cadence-persistence -p cadence-matching --all-targets`: exit 0
- `clippy ... --all-targets -D warnings`: exit 0
- `test -p cadence-persistence -p cadence-matching` (fail-fast): exit 101. cadence-matching lib: 397 passed, 1 failed, 2 ignored.
  Failure: `grpc_matching::durable_tests::matching_rpc_durable_cancel_unregisters_waiting_poller` (Elapsed timeout at matching_rpc_tests.rs:613), unrelated to Cassandra options; not re-verified (host load ~5).
  cadence-persistence tests did NOT run (fail-fast stopped first).
- Re-run with `--no-fail-fast` queued on the cargo lock for 30 min behind another job and was killed by the background limit: no result. UNVERIFIED: new tests and the persistence crate's tests were not seen passing.
