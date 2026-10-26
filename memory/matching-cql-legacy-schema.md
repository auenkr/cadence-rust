# Cassandra 2.x CQL metadata fallback

## Go compatibility contract

The pinned Go driver is `github.com/gocql/gocql` at
`v0.0.0-20211015133455-b225f9b53fa1` (local module source:
`/home/auenkr/go/pkg/mod/github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1/metadata.go`).
Its schema compatibility path uses:

- `system.schema_keyspaces(durable_writes, strategy_class, strategy_options)`;
  `strategy_options` is JSON and the class must be combined with it as the
  modern replication map's `class` entry.
- `system.schema_columnfamilies` for table names and validators including
  `key_validator`, `comparator`, and `default_validator`.
- `system.schema_columns` for column names, `component_index`, `validator`,
  and (with protocol v2+) `type`.
- V1 compatibility derives partition/clustering keys from key validators,
  comparators, key aliases, and column aliases because `type` is unavailable.

Go oracle run `40411377210` established that Cassandra 2.1.22 and 2.2 are
supported backends; the CQL oracle also covered Cassandra 3.11.17.

## Rust work in this change

`rust/vendor/scylla-1.9.0/src/cluster/metadata/fetching.rs` now has pure helpers
for decoding legacy keyspace replication options into the existing strategy
decoder and mapping scalar legacy marshal validator names to CQL types. Unit
tests cover NetworkTopologyStrategy option decoding, representative scalar
validators, and unknown validator rejection.

## Required live callsite integration still outstanding

The live fetch remains modern-only. In `ControlConnection::query_keyspaces`
(around line 759), the first keyspace query is hardcoded to
`system_schema.keyspaces`. A compatible fallback must be selected only when
that query reports Cassandra's missing-table/invalid error; peer discovery in
`system.peers` / `system.local` remains valid and should not be changed.

The subsequent schema branch (around lines 775-835) also calls
`query_user_defined_types`, `query_tables_schema`, `query_tables`, and
`query_views`, all modern `system_schema` readers. Those callsites need legacy
reader counterparts for `system.schema_usertypes` (where available),
`system.schema_columnfamilies`, and `system.schema_columns`. Legacy readers
must assemble keyspace-scoped table schemas, convert validators to CQL types,
preserve partition/clustering key positions, derive reversed clustering order,
and retain token-aware partition keys. Cassandra 2.x has no materialized
views, so its view result can be empty. Do not suppress metadata errors or
fall back to an empty keyspace/table map.

This change is deliberately not a functioning fallback yet: the isolated
decoders/tests are groundwork, and no Rust/Go build or rustfmt was run locally
per the CI-only command requirement. CI evidence must be added after a focused
Actions run from the committed branch.
