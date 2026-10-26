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

## Completed in the keyspace fallback increment

`ControlConnection::query_keyspaces` now first reads the modern
`system_schema.keyspaces` rows. If and only if that query yields the driver's
existing missing-table/column `DbError::Invalid` shape, it retries using
`system.schema_keyspaces` and converts `strategy_class` plus JSON
`strategy_options` into the modern replication map. It retains
`durable_writes`, and the converted values pass through the existing common
strategy decoder. Errors unrelated to missing modern schema are returned
unchanged. `keyspace_metadata_query` and the row conversion have direct unit
coverage in the vendored scylla crate.

CI now checks rustfmt on this file and explicitly runs the vendored scylla
library's metadata fetching tests. This prevents a package-filtered
`cadence-persistence` workflow from being mistaken for vendored driver test
coverage.

## Live reader integration status (2026-10-27)

The keyspace fetch identifies legacy mode only when the modern keyspace query
returns the recognized missing-table/column `DbError::Invalid`; unrelated
errors are propagated. That mode now selects `system.schema_usertypes` and
`system.schema_columns` in full schema loading. Legacy validators are parsed
recursively for scalar marshal types, `ListType`, `SetType`, `MapType`, and
`ReversedType`; the existing table assembly consumes column kind and component
position and resolves UDT references using the legacy UDT rows. `schema_columnfamilies`
supplies table names, and Cassandra 2.x materialized-view metadata is empty.

Remaining gaps: Cassandra 2.1 protocol v1 omits `system.schema_columns.type`;
the Go driver derives key columns from `key_validator`, `comparator`, aliases,
and `default_validator`. Rust does not yet implement that alias/comparator
derivation. Legacy column kinds/positions therefore cover protocol-v2+ schema
rows, not every protocol-v1 response. Marshal families outside scalar,
list/set/map, and reversed wrappers (including tuples, composites, and nested
UDT marshal encodings) are not yet decoded. Cassandra2 end-to-end behavior and
mixed-cluster SDK compatibility still require the parent CI/gate pass.

Focused direct tests cover recursive collection/reversed validator parsing;
the prior scalar and keyspace tests remain. No local Cargo, Go, or rustfmt
commands were used. Dispatch was attempted after push against
`t3/cassandra-legacy-metadata`; GitHub returned HTTP 422:
`Workflow does not have 'workflow_dispatch' trigger` for workflow `394409098`.
The branch YAML does contain `workflow_dispatch`; the dispatch API rejected
the request, so no run URL, CI conclusion, or compilation evidence exists for
this commit. Parent validation remains required.

## CI evidence

- Commit: `bd07d81254e5fee01a3b0493b30232098cc9355c`.
- Push run: https://github.com/auenkr/cadence-rust/actions/runs/40412443565
  (matching head SHA, event `push`, conclusion `failure`, zero jobs). The
  workflow push filter only covers `master` and `matching-*`, so this assigned
  `t3/legacy-keyspace-fallback` branch got no validation jobs.
- Dispatch for this ref was rejected by GitHub with HTTP 422: the workflow at
  the default branch does not have a `workflow_dispatch` trigger.
- Earlier helper run `40411987955` belongs to `03171a099` and failed. It is
  retained as prior evidence, not attributed to this implementation.

The focused CI run has not executed; do not treat this increment as validated.

## Current metadata-reader increment

- Code is pushed to `t3/legacy-schema-metadata-1` and
  `matching-legacy-schema-metadata`.
- The Rust CI workflow currently has dispatch support only in the feature
  branch file, not in the default branch workflow definition. A dispatch for
  this ref was rejected with HTTP 422 (`Workflow does not have
  'workflow_dispatch' trigger`). Push runs observed for the `t3/*` branch
  ended with zero jobs and are not validation evidence. The attempted
  `matching-*` push did not produce a run in the workflow run list.
- No Cargo, Go, or rustfmt command was run locally. No successful CI result is
  available for this increment.
