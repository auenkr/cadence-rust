# Matching Thrift audit U1-U4 resolution (2026-10-27)

Source-only; no Cargo/Go run. Edited only `rust/crates/cadence-matching/src/thrift_matching.rs` (not committed; CI unrun).

## U1 CONFIRMED + fixed
Go `MatchingService_*_Args.FromWire` (`go/.gen/go/matching/matching.go:13627`) skips field 1 when absent OR wrong wire type; arg stays nil, no decode error.
Mapper `ToMatching*Request(nil)` returns nil (`go/common/types/mapper/thrift/matching.go:257`); handler getters are nil-safe, then the engine:
- panics on nil (plain error via `CapturePanic`): `PollForDecisionTask` `engine.go:677` (`req.PollRequest`), `PollForActivityTask` :894, `AddDecisionTask` ~:526 (`request.Execution`), `AddActivityTask` :609, `DescribeTaskList` :1139 (`request.DescRequest`).
- nil-safe (== empty request): QueryWorkflow, CancelOutstandingPoll, ListTaskListPartitions (`getAllPartitions` getters), GetTaskListsByDomain, RespondQueryTaskCompleted (`GetTaskID`).
Rust before: absent -> empty struct (differs for the 5 panicking methods); wrong type -> BadRequest (differs for all).
Rust now (`dispatch_inner`): absent/wrong-typed arg -> `NIL_DEREFERENCE_MESSAGE` transport error (TChannel 0x05) for the 5 methods; empty request for the rest.
Residual: Go runs the host rate limiter (and for polls the context-timeout check) before the engine panic; Rust returns the panic before the private `check_rpc_quota` (in grpc_matching.rs, not owned). Differs only when throttled with a nil arg.

## U2 CONFIRMED EQUAL (not changed)
Go `task_list_manager.go:62,708` `returnEmptyTaskTimeBudget=1s` child context; Rust `deadline-1s` capped by poll_expiration returns empty response. Not re-traced further.

## U3 no change
Absent nested request: Go nil-safe getters; Rust uses defaults (covered by existing gRPC nil-ordering tests). Nil top-level covered by U1.

## U4 PARTIALLY fixed
Generated Go skips any field with wrong wire type (leaves zero/nil). Fixed for `AddActivityTask` field 80 non-struct (now treated as absent, was InvalidArgument).
NOT fixed (needs generic lenient decoder in `cadence-thrift/value.rs` / `cadence-api` FromThrift, outside ownership): all other wrong-typed scalar/nested fields (`opt_*` getters return `Error::Invalid` -> BadRequest where Go would skip). Exotic clients only; no SDK emits these.

## Tests
`matching_thrift_nil_request_argument_follows_go_from_wire` in thrift_matching.rs. CI: `gh workflow run rust-ci.yml --ref <branch> -f package=cadence-matching`.
