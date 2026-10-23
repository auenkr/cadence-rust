# Matching Cassandra CQL protocol version parity — 2026-10-21

## Go contract traced

- `go/common/config/loader.go` loads YAML with `uconfig.Expand(os.LookupEnv)` before populating `config.NoSQL`. The Docker Cassandra template puts `CASSANDRA_PROTO_VERSION` directly in each `protoVersion`, defaulting it to `4`.
- `go/common/persistence/nosql/nosqlplugin/cassandra/plugin.go:toGoCqlConfig` fills `protoVersion == 0` from `environment.GetCassandraProtoVersion`; that getter uses `CASSANDRA_PROTO_VERSION`, default `4`, and `strconv.Atoi` without range validation.
- `go/common/persistence/nosql/nosqlplugin/cassandra/gocql/client.go:newCassandraCluster` converts a still-zero value to `4`, then passes `cluster.ProtoVersion` unchanged to gocql. Therefore this service path pins v4 by default; it does not auto-negotiate. Explicit nonzero versions are passed through. Go's configured value is not validated against a server-version matrix before connect.
- Resolved gocql is `github.com/gocql/gocql v0.0.0-20211015133455-b225f9b53fa1`. Its frame implementation recognizes protocol versions 1–5. This does not mean every feature is valid for every server; server compatibility depends on the selected protocol and query features.

## Rust path and current limitation

- `rust/crates/cadence-server/src/config.rs` already parses Go's `protoVersion` into `NosqlConfig.proto_version`.
- `rust/crates/cadence-server/src/matching_task_store.rs:cassandra_config` resolves zero from `CASSANDRA_PROTO_VERSION`, then 4, and passes the result to `CassandraSessionOptions`.
- `rust/crates/cadence-persistence/src/cassandra.rs:CassandraSessionConfig::with_go_options` accepts zero or 4 and rejects every other configured version. `connect` passes no version option to `SessionBuilder`.
- The vendored `rust/vendor/scylla-1.9.0` source hardcodes one CQL protocol version in `network/connection.rs` startup options (`DEFAULT_CQL_PROTOCOL_VERSION`). The available driver path therefore cannot honor gocql's explicit v1–v5 selection. Current v4 default works with the Rust driver's fixed v4; explicit Go settings other than v4 fail clearly at startup rather than being silently rewritten.
- No alternate CQL driver source is already present in the Cargo registry. No build, dependency fetch, or service run was done for this source-only task.

## Wire work required for broader parity

A driver extension cannot be implemented by changing `SessionBuilder` configuration alone. The Go gocql source changes the frame header layout by protocol: v1/v2 use an 8-byte header and 1-byte stream ID; v3+ use a 9-byte header and 2-byte stream ID. Other version-dependent codecs include startup flags/handshake (v5 beta and wider flags), query parameter flags and trailing fields, result metadata, prepared-result metadata, schema-change event payloads, and protocol-specific value/feature handling (for example unset values at v4+). Those choices affect request encoding and response decoding across all connection paths, not just Matching's CQL statements.

Implementing this safely requires a version parameter through connection establishment, framing, stream allocation, every request/response codec and event parser, then Go-compatible golden wire fixtures and server-version interoperability for each supported protocol. The vendored driver currently has a fixed-v4 startup path, so this is substantial driver work; a partial header-only change would corrupt RPC framing or parse responses incorrectly. This task leaves that larger codec change unresolved.

## Source change and evidence status

The session setup error now names the actual limitation (“only speaks CQL protocol v4”) instead of suggesting that arbitrary versions are negotiated. A source regression covers v4 acceptance and explicit rejection for versions 2, 3, 5, and 6, ensuring unsupported values are not coerced. These source tests were intentionally not run per task constraints. The separate Go config/env/default trace above is static source inspection, not runtime validation.

Remaining work: implement or select a CQL driver with configurable v1–v5 wire codecs and test against Cassandra server versions; then remove the non-v4 startup rejection only when each selected protocol is actually supported end to end. Matching Cassandra protocol parity is not complete.
