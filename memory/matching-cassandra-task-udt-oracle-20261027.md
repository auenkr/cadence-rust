# Matching Cassandra task UDT protocol oracle — 2026-10-27

## Contract and source evidence

Extended the pinned Go backend oracle to create a task-shaped custom UDT and
table, prepare/insert a row, read it back, compare all six fields, and drop the
table/type. The field layout follows `go/schema/cassandra/cadence/schema.cql`
lines 276–283: `domain_id uuid`, `workflow_id text`, `run_id uuid`,
`schedule_id bigint`, `created_time timestamp`, and
`partition_config map<text,text>`. Protocol v2 expects the exact pinned gocql
`ErrorUDTUnavailable`; v3 and v4 require a complete successful field roundtrip.

Pinned source is
`/home/auenkr/go/pkg/mod/github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1/marshal.go`:
line 30 defines the exact error text, “UDT are not available on protocols less
than 3, please update config”; lines 155–157 return it when marshalling a
custom `UserType` below protocol 3; lines 259–261 do the same while
unmarshalling. This confirms that protocol v2 task UDT success is not a
compatibility requirement for this Go client.

The Rust example now uses the public `CassandraTaskUdt` for the same six-field
prepared insert/read roundtrip. This is source coverage only. The existing Rust
session configuration rejects protocols 2 and 3 before the example reaches the
UDT path. The Rust comparison gate remains opt-in and was not dispatched; do
not count this edit or Go success as Rust protocol parity.

## Validation and evidence

No local Go, Cargo, Rust, or rustfmt commands were run. `git diff --check` was
clean before commit. Go matrix run
https://github.com/auenkr/cadence-rust/actions/runs/40416310576 was dispatched
with `compare_rust=false` at head
`74e5fbe1cda804cca7c9bc17b8d392d1a74947c8`. On first inspection it was queued,
with protocol 2 running setup and protocol 3/4 queued; conclusion is pending.
The matrix keeps protocol 2 / Cassandra
2.1.22 and protocol 3 / Cassandra 3.11.17 range-delete expectations, adds
protocol 4 / Cassandra 4.1.7 task UDT success, and preserves all scalar queue
CAS/count/batch/error/range assertions.

## Review update (integration branch t3/cassandra-udt-oracle)

- Run 40416310576 (head 74e5fbe1c) was still `in_progress` at inspection; the
  v2 (Cassandra 2.1.22) and v3 (3.11.17) jobs FAILED in 0–1 s at the
  "Run exact pinned Go gocql protocol oracle" step with empty output and no
  oracle.log artifact. Cause: the step's first command
  `test -z "$(gofmt -d main.go)"` failed (set -e) because the new
  `matchingTask` composite literal keys were not gofmt-aligned. This is NOT
  evidence of a UDT/protocol failure; the oracle never ran. v4 was pending.
  Run is not accepted as terminal evidence.
- Fixed by aligning the literal keys (whitespace only, not verified with gofmt
  locally; no Go/Cargo/rustfmt run). Needs a fresh oracle dispatch
  (`compare_rust=false`) to get real v2/v3/v4 results; none dispatched here.
- Merged origin/t3/matching-cassandra-udt-oracle (clean) onto
  dc84a0690 (legacy UDT metadata mismatch rejection).
- Unresolved acceptance boundaries: task UDT Go roundtrip on protocols 3/4 and
  the v2 ErrorUDTUnavailable expectation unproven; Rust probe parity
  (compare_rust) unrun and Rust rejects protocols 2/3 so v3 UDT parity is
  impossible by config; legacy system.schema_usertypes / schema_columns reader
  vs pinned gocql was not re-audited against live Cassandra 2.1/3.x output.
  A stray `step("task-udt-prepare-insert")` precedes the insert without a
  separate prepare; cosmetic.
