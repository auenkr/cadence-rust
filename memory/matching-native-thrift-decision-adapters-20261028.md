# Native Thrift decision adapters — 2026-10-28

Parent review corrected the missing FromValue import, raw empty default,
absent enum defaults, wrong-type timeout skipping, response fields incorrectly
assumed to be boxed, an incorrect poll-domain assertion, and redundant struct
updates. Unsupported partition/load-balancer response graphs now also return
an explicit error rather than silently disappearing. The adapters remain
isolated pending complete handlers and full response coverage. CI is pending.

Added `thrift_matching_native.rs` as an isolated, non-production adapter slice for `MatchingService::AddDecisionTask` and `PollForDecisionTask`; registered the module in `lib.rs`. It decodes into the generated `cadence_proto::native::{matching,api}` graph and uses raw `GoString` fields. No public protobuf `String` conversion or production dispatch change is involved.

The request field IDs and semantics were cross-checked against `thrift_matching.rs` and `cadence-thrift`'s `PollForDecisionTaskRequest` converter: AddDecisionTask uses domain 10, execution 20, task list 30, schedule ID 40, timeout 50, source 59, forwarded-from 60, and partition map 70. Poll's outer request uses nested API request 20 plus domain 10, poller ID 15, forwarded-from 30, and isolation group 40; nested API poll request uses domain 10, task list 20, identity 30, checksum 40. TaskList kind follows the existing Go/legacy adapter offset (`wire + 1`); timeout seconds become a protobuf Duration. Correctly typed scalar fields are used and wrong-type duplicates skipped.

Raw strings preserve invalid `ff`/`fe`, NUL, and map keys/values. Map insertion is last-wins for duplicate byte-identical keys. The scalar string selector skips wrong-typed duplicates and retains the latest correctly typed value, matching generated Go `FromWire` behavior. The adapter currently normalizes only these requested fields; it does not provide a general Thrift schema normalizer. The existing `Struct::get` returns the first duplicate, so the native string/map helpers explicitly scan from the end.

The response encoder preserves token/branch bytes, workflow/run IDs, workflow type, and workflow task-list name/base name, along with scalar fields and nanosecond timestamps using the legacy adapter's wrapping arithmetic. It returns an explicit unsupported-graph error if query/query map, transient decision event graph, or auto-config hint is present. It does not claim complete response graph coverage. Response task-list/ execution fields use generated native boxed message shape; verify generated field boxing and signatures with CI before integration.

Tests added for invalid bytes versus text-preserving model boundary (raw byte assertions), embedded NUL identifiers, duplicate raw map keys, scalar wrong-type skipping, nested absent/default values, and poll identity/list decoding. No Cargo, Go, or rustfmt command was run locally per task instructions. Compilation/parity validation remains for parent-owned CI.
