# memory/

Working memory for agents on the cadence-rust monorepo. Read these before
starting a task; update `progress.md` when something substantial lands.

- `project-goals.md` — what the Rust rewrite must (and must not) do.
- `monorepo-layout.md` — where things live after the Go→`go/` move.
- `api-compat.md` — how SDK/wire compatibility is defined and checked.
- `progress.md` — chronological log of substantial changes.
- `migration-inventory.md` — concrete RPC adapters and remaining migration scope.
- `service-map.md` — service definitions, listeners and implementation boundaries.
- `resume.md` — latest runtime, validation boundaries, lessons and next actions.
- `port-ledger-summary.md` — Go source symbol coverage and review backlog.
- `port-map.json` — reviewed Go-to-Rust mappings; partial means incomplete.

Read `resume.md` first when continuing the rewrite. Passing SDK examples and
running listeners do not establish complete Go behavior. Keep unfinished changes
and validation of the running binary distinct from validation of source changes.

- `matching-storage-next.md` — durable Matching storage port, validation and queue/History RPC continuation.

### Legacy Cassandra UDT metadata follow-up (2026-10-27)

On `t3/legacy-udt-metadata`, `rust/vendor/scylla-1.9.0/src/cluster/metadata/fetching.rs` now converts legacy `system.schema_usertypes.field_types` marshal encodings before the normal CQL parser. It decodes `UserType(keyspace, hex-name, hex-field:marshal-type...)` references, validates embedded field encodings recursively, supports `FrozenType` and `TupleType`, and lets existing topological sorting see qualified UDT references. Added parser/assembly ordering coverage based on Cadence `task_list_partition`, `task_list_partition_config`, and `task_list` definitions in `go/schema/cassandra/cadence/schema.cql`.

Pinned Go source evidence: `$GOMODCACHE/github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1/helpers.go` `getCassandraBaseType` maps both `DateType` and `TimestampType` to `TypeTimestamp`; `SimpleDateType` is date. Rust legacy `DateType` mapping was corrected accordingly. No Cargo, Go, or rustfmt command was run locally, per CI-only validation requirement. `git diff --check` passed. Focused vendor tests remain unvalidated and must be run by parent on Actions.
