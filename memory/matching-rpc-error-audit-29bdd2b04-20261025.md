# Matching RPC error/validation audit — candidate 29bdd2b04 (read-only source audit)

Method: source-only compare of `rust/crates/cadence-matching/src/grpc_matching.rs` (+ `thrift_matching.rs` code map:
InvalidArgument->BadRequestError, other->InternalServiceError) against Go `go/service/matching/handler/{handler,engine,context}.go`,
`tasklist/identifier.go`, `semaphore/identifier.go`. No cargo/CI/Go run. Oracle: Go `handleErr` (context.go) wraps every
untyped error as `InternalServiceError{err.Error()}`; run 40171379726 oracle confirms malformed `/__cadence_sys//0` => Internal.
Stale mock/private-test gaps intentionally not reopened.

## Concrete mismatches (Go behaviour => Rust behaviour at 29bdd2b04)
1. **Malformed reserved task-list name type (high).** Go `tasklist.NewIdentifier` returns untyped `fmt.Errorf` => `InternalServiceError`
   in AddDecision/AddActivity/Query/Update/Refresh/CancelOutstandingPoll (raw message `invalid partitioned task list name X`).
   Rust helper `task_list_name()` (grpc_matching.rs:129) maps to `Status::invalid_argument` => **BadRequestError**. Only Describe (3664) and
   ListPartitions (3055) were fixed to `Status::internal`. Callers to fix: 1241,1387,1419,1502,1795,2034,2231,2455,2715,3246,3374,3461.
   Fix is likely just changing the helper to `Status::internal`.
2. **Poll message/precedence (med).** Go poll wraps: `couldn't create new decision|activity tasklist <err>` (Internal) and checks identifier
   before anything domain-related; Rust polls resolve `domain_name()?` (unknown domain error) before the name check (grpc_matching.rs ~1780/2020).
3. **RespondQueryTaskCompleted (high).** Go: only `deliverQueryResult(taskID)` => `EntityNotExistsError("query task not found, or already expired")`;
   domain-cache result and TaskList are ignored. Rust (3530-3600) returns BadRequest for absent nested `Request`, absent `TaskList`,
   unknown domain (`domain_name()?`), and token/task-id mismatch ("TaskToken does not match request.").
4. **Nil nested payload (med).** Go getters are nil-safe, an absent TaskList/PollRequest/DescRequest is treated as name "" and processed
   (poll waits to deadline, Update/Refresh proceed on root ""). Rust returns BadRequest "Request is not set."/"TaskList is not set."
   (lines 1776,1792,2015,2031,2278,2502,2680,2713,3219,3364,3459,3541,3581) and rejects empty name in Update/Refresh (3221,3366). Outer-nil Go panics
   are unserializable and out of scope. Decision for owner: wire-visible only via absent optional Thrift fields.
5. **Update ordering (low).** Go order: domain name => adaptive scaler => kind != Normal => PartitionConfig nil => identifier => non-root.
   Rust adds the "TaskList is not set" check before the scaler check and does parse after config (order OK otherwise).

## Verified matching Go (no action)
Semaphore BadRequest (empty domain/name, bucket<0, bad owner), `ErrSemaphoreDisabled`, long-poll missing deadline BadRequest,
Update/Refresh BadRequest texts, Describe/ListPartitions Internal for bad names, sticky-unavailable, kind mapping 2|3.

## Limits
Source-only; not executed. Line numbers refer to `git show 29bdd2b04:rust/crates/cadence-matching/src/grpc_matching.rs`. Go PollForDecisionTask
unknown-domain behaviour in task-list manager creation (item 2) is inferred, not oracle-verified.

## Implementation (branch t3/matching-go-error-parity, based on master 0bef6d183) — status: UNVALIDATED, CI pending
- Finding 1: `task_list_name()` now `Status::internal` (all callers are Go `NewIdentifier` equivalents; Go `handleErr` wraps raw error as Internal).
  Polls use `poll_task_list_name()` => `couldn't create new decision|activity tasklist <err>`, checked before the domain lookup (Go order).
- Finding 4 (proven by Go getters, `engine.go` poll/cancel/update/refresh): absent decision-poll `Request`, absent `TaskList` (polls, Update,
  Refresh, Cancel) is the empty-named list instead of BadRequest. NOT changed (unproven / Go may panic): activity poll with absent
  `PollRequest` (Go derefs `request.TaskListMetadata` => runtime panic), Add*/Query absent TaskList/Execution/Request, Cancel/Refresh unknown domain
  (Go ignores domain-cache errors there). New Go oracle `tests/go-matching-optional-payload-oracle_test.go` logs `GO_OPTIONAL_ORACLE` lines for these
  cases; read them from the Go oracle step before closing the open items.
- Finding 5: Update scaler check now first (TaskList-not-set pre-check removed).
- Finding 3 (RespondQueryTaskCompleted) intentionally untouched (other worker).
- Tests: `go_malformed_reserved_task_list_name_is_internal_for_every_rpc`; refresh-missing-tasklist test updated.

## Oracle results (Go run 40175488464, source 50d9dad0; lines in memory/evidence/matching-optional-payload-go-oracle-40175488464/)
Proven: malformed => Internal (decision poll wrapped, activity poll bare); refresh-malformed Internal; absent decision/activity-empty polls, Add* absent
TaskList/Execution, Cancel/Refresh absent TaskList => nil error; Update absent list + `{}` config => BadRequest "read partitions < 1", absent config =>
BadRequest "Task list partition config is not set in the request."; activity poll absent PollRequest => plain `runtime error: invalid memory address or
nil pointer dereference`. Unknown-domain results are synthetic mock errors ("oracle unknown domain") and NOT used as contracts. Query absent payload: not yet covered.
Implemented in b6d6b1b2 (CI 40175927357 pending): captured-panic helper accepts the nil-deref message; Add* nil-safe; refresh test renamed to _is_internal.
