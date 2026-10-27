# Native Matching decision response encoder — 2026-10-28

Updated `rust/crates/cadence-matching/src/thrift_matching_native.rs` to encode
the decision response graphs already handled by the production
`thrift_matching.rs::decision` adapter:

- Single workflow query: raw `GoString` query type and raw payload bytes.
- Query map: raw key bytes and query values; query byte identity is not run
  through UTF-8 validation. Empty query map is emitted as the production
  adapter emits it.
- Transient scheduled/started decision events: event IDs, version/task IDs,
  event time, supported decision attributes, task list and raw identity/request
  IDs. Other populated event variants return the legacy explicit error.
- Auto-config hint fields.
- The separate partition metadata helper encodes raw key/host bytes and keeps
  both fields present, including empty values, matching `partition`.
- Scalar counters, attempt normalization, optional-field presence and
  nanosecond timestamps retain the legacy decision response mapping.

Added focused tests for invalid raw query types, payloads and query-map keys,
transient event identities and unsupported event variants, auto-config hints,
zero attempt/backlog values, and raw/empty partition metadata. Existing request
adapter tests remain in place. Tests compare the exact Thrift field shape and
bytes because the production helper is private.

Remaining limits: legacy decision Thrift has no fields for the newer native
partition-config or load-balancer-hints response values; those values are
outside the reachable `decision` mapping and remain omitted. The native adapter
is still isolated and is not wired into production dispatch. No claim is made
for complete Matching service parity. CI compilation and test validation are
pending the parent agent; this task ran no Cargo, Go, or rustfmt locally.
