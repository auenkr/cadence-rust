# Matching Cassandra domain cache UUID normalization — 2026-10-27

## Change

Branch `t3/cassandra-uuid-cache-parity`; implementation head validated by CI:
`e2f2bd5cd1af34a83db938496a95783b6f4403a9`.

The domain metadata source now owns cache key normalization. SQL and default
sources retain identity keys. The Cassandra source mirrors the pinned Go
gocql `ParseUUID` behavior: ASCII hex, case insensitive, hyphens skipped only
at even decoded-digit offsets, and exactly 32 decoded hex digits. Cassandra
domain IDs are canonicalized as pages enter the cache, for cache lookups, and
for persistence cache-miss lookups. This lets the same cache resolver supply
the domain label to Matching metrics and persistence limiter context when Go
would resolve alternate valid spellings.

Malformed Cassandra IDs on the cache-miss persistence path are forwarded to the
existing source validator; this preserves its backend error class and
precedence. The cache-only telemetry lookup returns no label for malformed
spellings. RPC validation and handler error ordering are untouched. SQL
malformed UUID panic behavior remains in the existing SQL handler guard and is
not changed here.

Regressions in `matching_domain_cache.rs` prove uppercase/unhyphenated
Cassandra spellings resolve to a domain stored with uppercase canonical
spelling, invalid spellings do not resolve, and a non-Cassandra source keeps
exact UUID cache keys.

## Source trace

- Go's domain metadata Cassandra binding uses the pinned gocql UUID parser;
  see `go/go.mod` and `go/go.sum` for
  `github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1`. The parser rules
  are also captured in `memory/matching-sql-malformed-domain-uuid-design.md`.
- Rust cache normalization is implemented in
  `rust/crates/cadence-matching/src/matching_domain_cache.rs` on
  `DomainMetadataSource`, its Cassandra implementation, and `gocql_uuid_string`.
- `MatchingDomainCache::cached_domain_name` is used by the RPC metric resolver
  in `grpc_matching.rs` and by the persistence limiter resolver in
  `matching_persistence_limiter.rs`.
- The separate RPC quota path calls `Matching::domain_name` and the strict
  History domain lookup. That parser is intentionally unchanged.

## CI evidence

- Workflow: `rust-ci.yml`, package `cadence-matching`, `clean_target=true`.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40463522678
- Exact head SHA: `e2f2bd5cd1af34a83db938496a95783b6f4403a9`.
- Conclusion: `success`; CI log artifacts report `clippy.exit=0`, `fmt.exit=0`,
  and `test.exit=0`. The fresh target was
  `/home/runner/work/_temp/cargo-target-final`; `rust-ci-logs` confirms both
  UUID cache regression tests passed and the Matching test binary reported
  726 passed, 0 failed, 3 ignored.
- An earlier run on `8939c6e4caf8a2a7273f504776a5ca7e62a23faa` was cancelled
  after preserving source validation errors and was superseded by the
  successful run above.
- No local Cargo, Go compilation, or runtime validation was run. No mixed
  cluster gate was dispatched; parent-owned mixed Cassandra and SQL oracle
  evidence remains required for service acceptance.

## Scope and residual risk

Only the Matching domain cache source and cache tests changed. No CQL protocol
dependency or broad Cassandra migration was needed. This change is source and
focused unit CI validated; it does not replace the parent’s serial mixed
Go/Rust backend and recovery gates.
