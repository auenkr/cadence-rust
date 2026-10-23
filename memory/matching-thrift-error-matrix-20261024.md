# Matching Thrift (TChannel) error/exception parity matrix (2026-10-24)

Scope: how each Matching error kind reaches the TChannel/Thrift wire. Compares Go exception types and fields against Rust. Companion to the gRPC-only matrix `matching-error-matrix-20261023.md`.

Go sources:
- Wire IDL: `go/idls/thrift/matching.thrift` (`MatchingService` throws lists, field IDs), `shared.thrift` (exception field IDs, required/optional).
- Handler error switch: `go/service/matching/handler/context.go` `handleErr` (typed errors pass through; default becomes `InternalServiceError{Message: err.Error()}`). ShardOwnershipLostError is not in the switch, so it becomes Internal.
- Thrift wrapper: `go/service/matching/wrappers/thrift/thrift_handler_generated.go` (every return goes through `mapper.FromError`).
- Thrift mappers: `go/common/types/mapper/thrift/errors.go`, `shared.go` (From/To functions).
- Producers: `go/service/matching/handler/{handler,engine,context}.go`, `go/service/matching/tasklist/{task_list_manager,task_writer}.go`.

Rust sources:
- Method exception table: `rust/crates/cadence-thrift/src/service.rs` (generated from the IDL).
- Status to exception mapping: `rust/crates/cadence-matching/src/thrift_matching.rs` `application_error`.
- Producers: `matching_dispatch.rs`, `matching_query.rs`, `grpc_matching.rs`, `grpc.rs`, `matching_membership.rs`, `matching_writer.rs`, `matching_host_limiter.rs`.

Mapping rule on the Rust side: a status's typed detail, when its name is declared on that method, selects the exception. Otherwise the gRPC code selects it: InvalidArgument to BadRequest, ResourceExhausted to ServiceBusy, NotFound to EntityNotExists (when declared), everything else to InternalServiceError.

Field IDs checked against IDL: EntityNotExists 1/2/3/4, DomainNotActive 1-5, ServiceBusy 1/2, TaskListNotOwnedByHost 1/2/3, RemoteSyncMatched **10**, StickyWorkerUnavailable 1, QueryFailed 1, LimitExceeded 1, BadRequest 1, InternalService 1. The method table in `service.rs` matches the IDL throws lists for all 10 MatchingService methods.

## Matrix

| # | Error kind | Go exception (fields) | Rust exception (fields) | Go producer | Rust producer | Match? |
|---|---|---|---|---|---|---|
| 1 | BadRequestError | `BadRequestError{1 message}`, passthrough in handleErr | `BadRequestError{1 message}` via InvalidArgument fallback | handler.go:240 "Context timeout is not set.", invalid task list names | `grpc_matching.rs` InvalidArgument sites | MATCH |
| 2 | InternalServiceError | `InternalServiceError{1 message}`, default branch `err.Error()` | `InternalServiceError{1 message}` via Internal fallback | any unmapped error (context.go default) | any `Status::internal` | MATCH (kind; message text not byte-compared) |
| 3 | ServiceBusyError | `ServiceBusyError{1 message, 2 reason}`; reason always set (`&t.Reason`) | `ServiceBusyError{1 message, 2 reason}`; reason always set | handler.go:54 "Matching host rps exceeded" (reason ""); task_writer.go:184 "task queue is full and timed out waiting to enqueue" | `matching_host_limiter.rs` / grpc_matching.rs:1612; `matching_writer.rs` `full()` (reason "") | MATCH |
| 4 | LimitExceededError | `LimitExceededError{1 message}`, declared on 5 methods | Same fields; never produced by Matching | not produced on Matching path (only mapper/proto and history) | not produced; untyped ResourceExhausted maps to ServiceBusy in both | MATCH (unreachable) |
| 5 | EntityNotExistsError | `EntityNotExistsError{1 message, 2 currentCluster, 3 activeCluster, 4 activeClusters(required)}` | Same IDs; empty defaults filled for untyped NotFound | engine.go:1103 "query task not found, or already expired" | `matching_query.rs:157` Status::not_found, mapped via NotFound fallback | MATCH |
| 6 | DomainNotActiveError | `DomainNotActiveError{1 message, 2 domainName, 3 currentCluster, 4 activeCluster, 5 activeClusters}` | Same IDs when detail present; fields 2-5 filled with defaults | not produced by Matching handler (history-side errors consumed internally, engine.go:812/964) | not produced by Matching (`matching_history.rs` consumes it internally) | MATCH (unreachable on Matching wire) |
| 7 | RemoteSyncMatchedError | `RemoteSyncMatchedError{10 message}` (field 10, not 1). Declared on AddDecisionTask(6) and AddActivityTask(6) | Before: `Status::unavailable` with **no detail**, so mapped to InternalServiceError(2). After: detail `uber.cadence.shared.v1.RemoteSyncMatchedError`, mapped to RemoteSyncMatchedError with message at field 10 | task_list_manager.go:162 `errRemoteSyncMatchFailed` | `matching_dispatch.rs` `accept_forwarded_backlog` (two sites) | **DIFF, fixed** |
| 8 | StickyWorkerUnavailableError | `StickyWorkerUnavailableError{1 message}`. Declared on AddDecisionTask(7) and QueryWorkflow(7) | Same. Detail present (`sticky_unavailable`) | engine.go:129 `_stickyPollerUnavailableError` | `grpc_matching.rs:124` `sticky_unavailable` | MATCH |
| 9 | TaskListNotOwnedByHostError | `TaskListNotOwnedByHostError{1 ownedByIdentity, 2 myIdentity, 3 tasklistName}` (all required) | Same IDs from detail. Without a decodable detail, only field 1 is set, so 2 and 3 are missing (see residual) | engine.go:1607; mapper proto ABORTED with detail | `matching_membership.rs:228`, decoded at line 313 | MATCH (residual below) |
| 10 | QueryFailedError | `QueryFailedError{1 message}` = worker error verbatim (engine.go:1083). Declared on QueryWorkflow(4) | Before: `InvalidArgument` with **no detail** and a `"Query failed."` fallback, mapped to BadRequestError(1). After: `QueryFailedError` detail, message verbatim | engine.go:1083 | `matching_query.rs` `query_outcome` | **DIFF, fixed** |
| 11 | AccessDeniedError | Not declared on any MatchingService method | Not declared; no Matching producer | none | none | MATCH (N/A) |
| 12 | ShardOwnershipLostError | Not declared on Matching methods. Not in handleErr switch, so becomes InternalServiceError(`err.Error()`) | Aborted + detail not declared, falls back to InternalServiceError(status message) | (history-side, reached via Matching calls) | `matching_history.rs` consumed internally | MATCH (kind; message text not compared) |
| 13 | ClientVersionNotSupportedError | Returned from QueryWorkflow strong query (engine.go:1073) but **not declared** in QueryWorkflow throws | FailedPrecondition + detail (`query_outcome`), not declared, so mapped to InternalServiceError(2) | engine.go:1073 `SupportsConsistentQuery` | `matching_query.rs` `query_outcome` | **DIFF, not fixed (unverified).** Go sends an undeclared error, which thriftrw/yarpc may encode as a transport-level error rather than an exception. Needs a live Go client capture. |
| 14 | Unknown query completed type | `InternalServiceError{"unknown query completed type"}` (engine.go:1085) | Any non-Answered result becomes QueryFailedError | engine.go:1085 | `query_outcome` | **DIFF, edge, not fixed.** Only reachable with an out-of-range enum value on the wire. |
| 15 | Deprecated-domain errors | No deprecated-domain check in `go/service/matching` (deprecation is handled in frontend) | None | none | none | MATCH (N/A on Matching) |
| 16 | Other typed errors not in Matching IDL (ReadOnlyPartition, SemaphoreNotOwnedByHost, CurrentBranchChanged, RetryTaskV2, EventAlreadyStarted, WorkflowExecutionAlreadyStarted) | Passed through handleErr, but undeclared on Matching methods | Undeclared, so mapped to InternalServiceError | none on Thrift path | none on Thrift path | MATCH (N/A) |

Totals: 16 rows. DIFF found 4 (rows 7, 10 fixed; rows 13 and 14 not fixed). MATCH or N/A 12.

## Fixes (this branch)

1. `matching_dispatch.rs`: new `remote_sync_match_failed()` builds `Unavailable` with the `RemoteSyncMatchedError` detail (shared type URL, same as the Go gRPC mapper). Used by both `accept_forwarded_backlog` failure sites.
2. `matching_query.rs` `query_outcome`: failed query uses `QueryFailedError` detail with the worker message verbatim. Removes the `"Query failed."` fallback, which Go does not have.

Tests (`thrift_matching.rs`): `matching_thrift_forwarded_sync_miss_and_failed_query_use_go_exception_types` checks row 7 (RemoteSyncMatched on AddDecisionTask, field 10) and row 10 (QueryFailed on QueryWorkflow, field 4, message "boom").

## Residual (not fixed)

- Row 9: a `TaskListNotOwnedByHostError` with an undecodable detail keeps only field 1. Go never sends that shape, so this only affects corrupt detail bytes.
- Row 13: undeclared typed error on QueryWorkflow. Needs wire capture from a Go client before deciding whether Rust should emit a transport-level error.
- Row 14: out-of-range `QueryResultType` on the wire.
- Message text differs in some Internal and ShardOwnershipLost cases (kind matches; not byte-compared).
- Unit tests only. No cargo was run locally (project rule); verification is GitHub Actions.
