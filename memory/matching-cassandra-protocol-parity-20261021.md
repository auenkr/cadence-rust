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

## Bounded recheck against final acceptance audit — 2026-10-27

The protocol gap above is **still unresolved**; it was not superseded by
`matching-final-acceptance-audit-20261026.md` or its ledger. Those audits discuss
the Cassandra mixed lane and other service requirements but do not trace the
`protoVersion` deployment setting or the driver's protocol selection. Their
“no new concrete production mismatch” finding therefore does not close this
separately recorded configuration mismatch.

The supported, reachable mismatch is specifically protocol v2/v3 selection:

- Go's production NoSQL config has `ProtoVersion int` tagged `yaml:"protoVersion"`
  (`go/common/config/config.go:240-241`). The normal Cassandra template exposes
  `CASSANDRA_PROTO_VERSION` in that field and defaults it to 4
  (`go/docker/config_template.yaml:23,31`). Config loading expands environment
  variables (`go/common/config/loader.go`); when zero remains,
  `toGoCqlConfig` reads `CASSANDRA_PROTO_VERSION`
  (`go/common/persistence/nosql/nosqlplugin/cassandra/plugin.go:123-124`),
  whose getter defaults to 4 and parses an integer without rewriting it
  (`go/environment/env.go:186-194`). `newCassandraCluster` assigns that value
  to `cluster.ProtoVersion` (`go/common/persistence/nosql/nosqlplugin/cassandra/gocql/client.go:56-59`).
- The pinned gocql version is
  `v0.0.0-20211015133455-b225f9b53fa1` (`go/go.mod:20`). Its
  `ClusterConfig.ProtoVersion` comment says to use known versions 2, 3, or 4
  (`/home/auenkr/go/pkg/mod/github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1/cluster.go:41-48`);
  its frame codec has version-specific framing and codecs in `frame.go`.
  Thus YAML `protoVersion: 2` / `3`, or the corresponding environment setting
  through the defaulted config, is a supported Go deployment choice for a
  cluster that requires that protocol.
- Rust parses the same YAML field (`rust/crates/cadence-service-core/src/config.rs:705-706`)
  and resolves zero from the same environment variable, defaulting to 4
  (`rust/crates/cadence-matching/src/matching_task_store.rs:58-66`). But
  `CassandraSessionConfig::with_go_options` rejects every explicit value other
  than 4 (`rust/crates/cadence-persistence/src/cassandra.rs:751-752`), and the
  vendored Scylla driver's startup path uses its fixed
  `DEFAULT_CQL_PROTOCOL_VERSION` (`rust/vendor/scylla-1.9.0/src/network/connection.rs:2274`).
  Therefore a supported Go v2/v3 deployment fails during Rust Matching startup.

The ordinary default-v4 configuration and the Cassandra 4.1 mixed acceptance
lane (`memory/matching-cassandra-acceptance-20261024.md`) establish v4 behavior
only. They do not establish v2/v3 parity. Conversely, do not broaden this
finding to v1 or v5: the pinned gocql `ClusterConfig` documents 2/3/4 as the
known versions to configure, and no supported Matching deployment using v1/v5
was identified in the traced configuration. No current server matrix was
established here, so this note does not claim that any particular server
version requires v2 or v3; it records the concrete supported configuration
surface and its observable Rust startup rejection.

Minimal required port: Matching's Cassandra client must be able to select and
use v2 and v3 end to end when configured, including the version-dependent
frame layout and request/response codecs already identified above, with
protocol-specific compatibility evidence before accepting those settings.
Merely accepting the option or changing startup negotiation is insufficient.
Until then, preserve the explicit rejection and mark this parity item open;
do not silently waive it based on v4 acceptance. This recheck was static source
inspection only; no Cargo, Go, formatting, CI, or mixed-service command was
run.
