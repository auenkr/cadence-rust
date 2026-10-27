# Native Matching Thrift response encoders — 2026-10-28

New `rust/crates/cadence-matching/src/thrift_matching_native_responses.rs`
(parent must register `mod`; no Cargo/rustfmt/Go run locally, CI pending).
Public infallible encoders returning `Struct`:
`poll_for_activity_task_response`, `query_workflow_response`,
`describe_task_list_response`, `get_task_lists_by_domain_response`,
`list_task_list_partitions_response`.

Source: legacy `thrift_matching.rs` `activity`, `description`, `partition`,
and the `invoke` arms for QueryWorkflow/GetTaskListsByDomain/ListTaskListPartitions;
shapes from `go/proto/internal/uber/cadence/matching/v1/service.proto`.
Strings/map keys are emitted from `GoString` bytes unchanged; timestamps are
unix nanos, durations whole seconds as i32 (legacy truncation), enums
`value-1` when >0, payload presence = Some (empty stays empty bytes).

Limits: native `partition_config`, `load_balancer_hints` (activity), and
QueryWorkflow/Describe `partition_config` have no legacy Thrift field and are
omitted, as in the legacy adapter. Not decision-response (handled in
thrift_matching_native.rs); other RPCs are void. Compile risks: native type
names (`n_api::TaskIdBlock`, `PollerInfo`, `QueryRejected`, `Header.fields`
as `HashMap<GoString, Payload>`) assumed from the generator; `GoString::from(Vec<u8>)`
and `From<&[u8]>`-free usage; map-order is HashMap-iteration (as Go).

Parent registered the module in lib.rs and the CI formatting list. Static comparison confirms the encoder field IDs and ordinary timestamp/duration conversions match existing thrift_matching.rs. Pinned proto names are available from initialized IDLs. Compilation and Go differential validation remain pending.
