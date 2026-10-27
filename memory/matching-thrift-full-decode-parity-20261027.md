# Matching Thrift full decode parity: U1 ordering + U4 wrong-typed fields (2026-10-27)

Source-only, nothing compiled or run (no local Cargo/Go). Not committed/staged. CI unrun.
Continues `matching-thrift-audit-resolution-20261027.md` (its U1 "residual" and U4 "NOT fixed" are closed here).

## Files
- `rust/crates/cadence-matching/src/thrift_matching.rs` (nil-arg dispatch, `normalize`, tests)
- `rust/crates/cadence-matching/src/grpc_matching.rs` (new `Matching::thrift_nil_request`; `check_rpc_quota` stays private)
- NEW `rust/crates/cadence-matching/src/thrift_matching_schema.rs` (generated, `#[path]`-included from thrift_matching.rs, no lib.rs change)
- NEW `tests/gates/gen-matching-thrift-schema.py` (generator: `python3 -I tests/gates/gen-matching-thrift-schema.py .`)

## U1: nil / wrong-type top-level arg ordering
Go `handler.go`, all methods: `domainName(request.GetDomainUUID())` (nil-safe -> "") -> `newHandlerContext` -> `startProfiling(&h.startWG)` (waits for `Start()`, request counter + timer) -> forwarded counter -> host limiter (`workerRateLimiter` for AddActivity/AddDecision/Poll*/RespondQuery/CancelOutstandingPoll, `userRateLimiter` for the rest) -> polls: `ValidateLongPollContextTimeoutIsSet` -> engine.
Engine dereferences the nil top-level request first thing in 5 methods (`engine.go`: PollForDecisionTask `req.PollRequest` ~677, PollForActivityTask ~894, AddDecisionTask `request.Execution` ~526, AddActivityTask ~609, DescribeTaskList `request.DescRequest` ~1139). So a throttled nil request is ServiceBusy, an unthrottled one is the plain runtime error (`CapturePanic`, TChannel 0x05). The engine's own `IsValidContext`/min-deadline check is after the dereference, so it is NOT run for nil; the handler's timeout-set check always passes (a TChannel call carries a TTL).
The other 5 (QueryWorkflow, CancelOutstandingPoll, ListTaskListPartitions, GetTaskListsByDomain, RespondQueryTaskCompleted) are nil-safe and run as an empty request through the normal Rust gRPC handler (quota + metrics included).
Rust now: `dispatch_inner` waits `wait_started()` for every MatchingService method (Go startWG; before only Meta waited), and a nil/wrong-type arg for the 5 panicking methods calls `Matching::thrift_nil_request(method, MatchingRpc)`: metrics scope (`matching_request`, request counter + timer, no failure counter since the Go panic skips `handleErr`), `check_rpc_quota` with the empty domain (user/worker class from `MatchingRpc::policy`), then the nil-dereference error.

## U4: wrong-typed known fields
Go generated `FromWire`: `if field.Value.Type() == wire.TX { ... }` per known field; a different wire type (i16/i32/i64 mixes, bool, string vs struct ...) is skipped without error; later duplicates overwrite; `_List_X_Read` / `_Map_K_V_Read` return `nil,nil` if the declared element/key/value type differs. No reachable struct has a required field (checked, all `is required` errors are exception types).
Rust `Struct::opt_*` widen (i16/i8->i32, i32/i16->i64) or reject, and `cadence-thrift`/`cadence-api` are shared with persistence and later services, so they are unchanged. Instead `thrift_matching::normalize(struct, &Struct)` runs once at the Matching boundary (after the nil handling, before `invoke`) using the generated schema of all 78 structs reachable from the ten Matching request structs (incl. HistoryEvent graph under ActivityTaskDispatchInfo): drops wire-type mismatches, container element mismatches, keeps last duplicate, recurses into nested structs / list / map struct values. Schema was cross-checked: per-struct field count equals the number of `case N:` in the Go `FromWire`.
Request struct per method: Poll*/Add*/QueryWorkflow/RespondQueryTaskCompleted/CancelOutstandingPoll/DescribeTaskList/ListTaskListPartitions -> `matching.<Method>Request`; GetTaskListsByDomain -> `shared.GetTaskListsByDomainRequest` (matching.thrift uses the shared type).
Also changed in the previous step: wrong-typed AddActivityTask field 80 is treated as absent.

Known remaining difference (not fixed, low value): Go decodes strings without UTF-8 validation while Rust `from_utf8` errors on invalid bytes; empty list/map element types are not carried by the Rust `Value` (equivalent result: empty).

## Tests (all in thrift_matching.rs, CI only)
- `matching_thrift_nil_request_argument_follows_go_from_wire` (5 panicking + nil-safe, absent and wrong-type)
- `matching_thrift_nil_request_runs_metrics_and_host_quota_before_engine_panic` (throttled+nil via user and worker host limit `matching.rps`/`matching.workerrps`=1; counts request metric)
- `matching_thrift_nil_request_waits_for_startup_like_go_start_profiling`
- `matching_thrift_wrong_wire_type_fields_are_skipped_like_go_from_wire` (i32 on i64, i64 on i32, i16 on enum, nested)
- `matching_thrift_wrong_type_known_fields_cover_every_request_struct` (ten structs + nested ones)
- `matching_thrift_container_and_nested_wrong_types_follow_go_readers`
Run: `gh workflow run rust-ci.yml --ref <branch> -f package=cadence-matching`. Risk to watch in the first CI run: the throttle test assumes a burst of 1 for a global rps of 1 (domain "" bucket uses the default domain rps); and the whole schema file needs rustfmt/clippy clean (`#[rustfmt::skip]`, `#![allow(clippy::all)]` added).
No Go oracle overlay required: behaviour is derived directly from the generated decoder source.
