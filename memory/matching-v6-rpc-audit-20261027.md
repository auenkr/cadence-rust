# Matching v6 RPC audit (2026-10-27, read-only source audit)

Candidate: tag `matching-acceptance-20261027-v6`, SHA `3055d0c6c0c8932c14e12714b498b82569cfe0b5`.
Method: source reading only (no Cargo/Go/CI). Sources: Go `go/service/matching/handler/handler.go`,
`go/service/matching/wrappers/thrift/thrift_handler_generated.go`, `go/proto/internal/uber/cadence/matching/v1/service.proto:44-102`;
Rust `rust/crates/cadence-matching/src/grpc_matching.rs`, `thrift_matching.rs`, `tchannel.rs`. Reconciled with
`memory/matching-final-rpc-acceptance-matrix-20261025.md`. Prior RPC thread not recovered (scope/time); the matrix was used instead.
Scope limit: this is an inventory/structure audit. Per-branch error-precedence parity was NOT re-traced here; it relies on the matrix's test evidence.

## Method inventory

| # | Method | Go handler.go | Native gRPC (Rust grpc_matching.rs) | Thrift Go wrapper | Rust Thrift (thrift_matching.rs `invoke`) |
|---|---|---|---|---|---|
| 1 | PollForDecisionTask | :252 | fn at :1902 | :50 | :315 |
| 2 | PollForActivityTask | :212 | :2172 | :45 | :339 |
| 3 | AddDecisionTask | :179 | :2386 | :20 | :365 |
| 4 | AddActivityTask | :146 | :2630 | :15 | :386 |
| 5 | QueryWorkflow | :293 | :2846 | :55 | :428 |
| 6 | RespondQueryTaskCompleted | :326 | :3704 | :60 | :452 |
| 7 | CancelOutstandingPoll | :354 | :3623 | :25 | :467 |
| 8 | DescribeTaskList | :382 | :3840 | :30 | :484 |
| 9 | ListTaskListPartitions | :411 | :3220 | :40 | :540 |
| 10 | GetTaskListsByDomain | :441 | :3158 | :35 | :507 |
| 11 | UpdateTaskListPartitionConfig | :470 | :3392 | none | none (correct) |
| 12 | RefreshTaskListPartitionConfig | :498 | :3536 | none | none (correct) |
| 13 | AcquireSemaphore | :526 | :3278 | none | none (correct) |
| 14 | Health (Meta) | :106 | `MetaApi::health` :4040, waits for start, `ok:true`, "matching good" | n/a | `dispatch_inner` Meta branch, `thrift_matching.rs:726` (ok=true, msg bytes "matching good") |

Result: 13/13 proto RPCs plus Health present in Rust gRPC; 10/10 Go Thrift methods plus Meta::health present in Rust Thrift; unknown MatchingService procedures return Unimplemented / "Unknown Matching procedure". Rust Thrift correctly exposes no procedure for methods 11-13 (test `matching_thrift_has_no_partition_config_procedure`). No missing methods found.

## Behavior / test links
Per-RPC behavior and test names are in the 2026-10-25 matrix rows (unchanged, still accurate as to method set). Additional Thrift-layer facts verified in source:
- Error mapping `application_error` (`thrift_matching.rs:~575-665`): selects declared exception from gRPC status details type URL, falls back by code (InvalidArgument->BadRequest, ResourceExhausted->ServiceBusy, NotFound->EntityNotExists if declared, else InternalService); fills required fields for DomainNotActive/EntityNotExists/ServiceBusy/TaskListNotOwnedByHost; captured panic is returned as a non-declared transport error (TChannel Unexpected). Matches the comments citing Go thrift mapper.
- Poll over Thrift: timeout is `deadline - 1s` capped by `poll_expiration`; on expiry returns an EMPTY successful response (`thrift_matching.rs:~746-760`). Go engine also returns an empty response on poll expiry, so shape is consistent; the 1s margin/cap is Rust-specific (see U2).
- AddDecisionTask/AddActivityTask/CancelOutstandingPoll map Thrift enum + 1 into proto enum (`wrapping_add(1)`), absent -> 0 (INVALID), matching the Go mapper's INVALID handling.

## Confirmed bugs
None confirmed from source inspection in this pass: no missing method, no missing Thrift procedure, no extra Thrift procedure.

## Uncertainties (not confirmed bugs)
- U1: Thrift request arg absent (`args.get(1)==None`) is treated as an empty struct (`thrift_matching.rs:~732`); Go thriftrw would reject or nil-handle a missing required argument. No test pairing found; exotic client only.
- U2: Thrift poll budget (`deadline-1s`, `poll_expiration` min) vs Go context deadline use; confirm against Go `engine.go` poll expiry plus long-poll tests 538-539 in the matrix. The gRPC path has its own tested deadline rules.
- U3: Optional-payload handling in Thrift decoding (`nested(...)?`, `string(s,N)`) maps absent nested request to `None`; Go mapper maps nil to nil. The gRPC-side nil-dereference ordering (matrix rows PollForActivityTask, RespondQueryTaskCompleted) is tested, but the same cases through Thrift are not paired with tests.
- U4: AddActivityTask `activity_task_dispatch_info` with a non-struct value returns InvalidArgument; Go thriftrw would fail decode before the handler (error shape could differ: BadRequest vs transport error).
- Historical/diagnostic exclusions carried from matrix: `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` is `#[ignore]` (dev-only in-memory constructor, no Go equivalent; `memory/matching-ignored-rpc-audit-20261025.md`); the `matching_history_tchannel` mixed-wire test is `#[ignore]` by design.
- Evidence gap (unchanged from matrix): mixed Go/Rust runs do not cover typed-error/optional-field/cancellation matrices for each RPC; End-to-end Thrift error-shape probes from Go History/Frontend callers against Rust Matching remain limited to the scenarios named in the matrix.

## Bounded closure
Method-surface parity (native + Thrift + Meta health) is closed at this SHA. Remaining open items are evidence, not code: U1-U4 need paired Thrift-side tests or a Go oracle probe if the parent wants to close them; no service-complete claim is made or implied.
