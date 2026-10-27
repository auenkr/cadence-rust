# Matching Thrift decoder review (read-only, 2026-10-27)

Scope: uncommitted `thrift_matching.rs` (normalize, nil-request path), `grpc_matching.rs::thrift_nil_request`,
generated `thrift_matching_schema.rs`, `tests/gates/gen-matching-thrift-schema.py`, against Go
`go/.gen/go/{shared,matching}` `FromWire` and `go/service/matching/handler/{handler,engine}.go`.
No Cargo/Go run; static audit only.

## Confirmed blockers (fix before the source pin)

1. **Test references a struct absent from the schema (CI failure).**
   `matching_thrift_wrong_type_known_fields_cover_every_request_struct` has
   `("shared.WorkflowQueryResult", 10, Value::I64(1))`. `WorkflowQueryResult` is not reachable from any
   Matching request (Go `RespondQueryTaskCompletedRequest.QueryResult` is `[]byte`) and is not in
   `thrift_matching_schema.rs`. `normalize` returns `value.clone()` for unknown names, so the field is
   not removed and the assertion fails. Fix: delete that case. (Other cases checked one by one against
   the schema: ids and wrong types are correct.)

2. **Container type mismatch must clear an earlier duplicate (code and test both wrong).**
   Go: `v.PartitionConfig, err = _Map_String_String_Read(...)` assigns the returned nil, so a later
   duplicate field 70 with a bad key/value type overwrites an earlier valid map with nil. Same for
   `_List_*_Read` (ResetPoints.Points, RetryPolicy non-retriable list). Rust `continue`s before the
   `fields.retain`, so the earlier map survives.
   Counterexample: `AddDecisionTaskRequest {70: map<string,string>{k:v}, 70: map<i32,string>{1:v}}`
   -> Go PartitionConfig == nil; Rust keeps `{k:v}`.
   `matching_thrift_container_and_nested_wrong_types_follow_go_readers` encodes the wrong behaviour
   (`assert_eq!(out.fields, vec![(70, good_map), (10, text("d"))])`). Fix: on container mismatch do
   `fields.retain(|(e,_)| e != id); continue;` and expect `vec![(10, text("d"))]`.
   Scalar / string / struct wrong wire type correctly keeps the earlier value (Go skips the case).

3. **Args field 1 picks the first duplicate, Go picks the last struct.**
   `dispatch_inner` uses `args.get(1)` (`Struct::get` = first match). Go `Args.FromWire` loops all
   fields and each `TStruct` field 1 reassigns; wrong-typed ones are skipped.
   Counterexamples: args `[(1,I32), (1,Struct(req))]` -> Go decodes req, Rust takes the I32 and takes the
   nil-deref path; args `[(1,A),(1,B)]` -> Go uses B, Rust A. Fix: select the last
   `Value::Struct` among `args.fields` with id 1 (None if none). Low real-world likelihood but
   the stated goal is FromWire parity and the fix is a one-liner.

## Verified correct

- Required-field errors: no struct reachable from the Matching requests has a required field (all
  "is required" hits are exception types), so skipping a wrong-typed nested struct never turns into a
  Go decode error. No missed error path.
- Schema completeness: for every one of the 85 generated structs the number of `case` arms in the
  Go `FromWire` equals the number of generated entries (scripted comparison); wire types come from the
  same `field.Value.Type() == wire.X` lines. Container readers: 6 `map<string,string|binary>`, 1
  `list<ResetPointInfo>`, 1 `list<binary>`; matches Go `_Map_*_Read`/`_List_*_Read` checks. Generator is
  deterministic (sorted), regeneration-safe; its assert guards nested containers.
- Nil-request ordering matches Go: `h.domainName("")` -> `newHandlerContext` -> `startProfiling`
  (startWG wait + request counter) -> host limiter (Poll/Add*: worker; Describe: user) -> poll timeout
  check (passes, TChannel always has a deadline) -> engine deref (`req.PollRequest`,
  `request.DescRequest.GetTaskListType()`, Add* `request.Execution` path) -> CapturePanic returns the
  plain runtime error, `handleErr` skipped so no failure counter, latency deferred still recorded
  (wrapper returns Ok so `timer.finish(false)`, no failure metric). Throttled nil request is ServiceBusy,
  not panic: right.
- `async { check?; Ok(()) }` should infer `E = Status` from the `Err(status) => status` arm; low compile
  risk. If CI reports "type annotations needed", write `Ok::<(), Status>(())`.
- Rust empty list/map: `Value` has no declared element type, so an EMPTY container whose declared
  element type is wrong (Go: nil) is kept as empty. Observable only as nil-vs-empty map/list in
  PartitionConfig/non-retriable lists; no behavioural difference found downstream. Record as a known
  nil-vs-empty nuance, not a blocker.
- Map duplicate keys: Go last wins; verify downstream `HashMap` collect is last-wins (not checked here).
- Quota test: limit 1 with sequential calls is flaky only if >1s elapses between calls on the runner;
  low risk.

## Invalid UTF-8 limitation

Rust getters (`opt_string`/`req_string`, `thrift_matching.rs:200`) reject invalid UTF-8 with
`InvalidArgument`/BadRequest; Go accepts arbitrary bytes in `string`. Scope: only affects Thrift string
fields (domain UUID, task list name, identities, workflow ID/run ID, forwardedFrom, map keys/values)
carrying non-UTF-8 bytes. Official Go/Java/Cadence SDKs and server-internal callers always send valid
UTF-8 (Go would then fail later in persistence/JSON for most of these), so SDK behaviour is not broken.
Do NOT switch to lossy conversion (it would silently alias distinct IDs). Parity would need the String
-> bytes plumbing through the engine; that is a logic change, so this stays a documented limitation
until the user decides. Not claimed as accepted divergence.

## Other notes

- `dispatch_inner` now awaits `wait_started()` before the unknown-service/validation fall-through only
  for known MatchingService methods (Meta already waited); unknown procedures error before it. OK.
- Nil request handling covers Poll*/Add*/DescribeTaskList only; other methods read through nil-safe getters,
  consistent with handler bodies (not re-audited).
