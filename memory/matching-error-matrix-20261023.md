# Matching error-path parity matrix (2026-10-23)

Go: `go/service/matching/handler/{handler,engine,context}.go`, `go/common/types/mapper/proto/errors.go`.
Rust: `rust/crates/cadence-matching/src/`. Scope: static comparison plus unit tests; Thrift mapping not re-audited line by line (thrift_matching.rs reuses the same status types).

Go `handleErr` (context.go:83): typed errors pass through; anything else becomes `InternalServiceError{err.Error()}`.

| Condition | Go | Rust | Match? |
|---|---|---|---|
| Host rps exceeded | ServiceBusy "Matching host rps exceeded" (handler.go:54) | ResourceExhausted + ServiceBusyError detail (`check_rpc_quota`, matching_rpc_tests quota test) | yes |
| Persistence throttled | SQL convertCommonErrors -> ServiceBusy | `queue_status` ResourceExhausted+ServiceBusy (matching_dispatch.rs:448) | yes |
| Persistence timeout / ConditionFailed / other / task list stopped | not typed -> handleErr default Internal | `queue_status` -> Internal (test at matching_dispatch.rs:1415) | yes |
| Task list not owned by host / membership change | TaskListNotOwnedByHostError, gRPC Aborted + shared.v1 detail (engine.go:1607, errors.go) | Aborted + TaskListNotOwnedByHostError detail (matching_membership.rs:228) | yes |
| Membership lookup failure | wrapped error -> Internal | Internal "failed to lookup task list owner" (matching_membership.rs:215) | yes |
| Long poll without deadline | BadRequest "Context timeout is not set." (handler.go:240) | InvalidArgument same message (grpc_matching.rs poll sites; test exists) | yes |
| Long poll with remaining deadline <10ms / expired | `IsValidContext` returns context.DeadlineExceeded (util.go:386, engine.go:700/903) -> Internal "context deadline exceeded" | **was missing**; now `check_poll_context` -> Internal "context deadline exceeded" | **MISMATCH, fixed** |
| Poll cancelled / no task | ErrNoTasks / errPumpClosed -> empty response, nil error (engine.go:722) | empty response (grpc_matching.rs:1814 etc.) | yes |
| RecordDecisionTaskStarted: EntityNotExists / AlreadyCompleted / EventAlreadyStarted | drop task, continue poll (engine.go:828) | handled in matching_history.rs:44 classification | yes |
| RecordDecisionTaskStarted: DomainNotActive | refresh workflow tasks, drop, continue (engine.go:812) | matching_history.rs:47 | yes |
| Other history errors | task.Finish(err), continue poll (retry) | same classification (matching_history.rs:106) | yes |
| Forwarded parent ServiceBusy | forwarder.handleErr -> errForwarderSlowDown | `ForwardError::classify` (matching_forward.rs:20) | yes |
| Invalid task list name | BadRequest / Internal wrap | InvalidArgument (`task_list_name`) | yes (code), message not byte-compared |
| Invalid partition-config update | BadRequest messages engine.go:1255-1300 | InvalidArgument (grpc_matching.rs:2994-3135) | yes |
| Query task not found | EntityNotExists "query task not found, or already expired" (engine.go:1103) | NotFound | yes |

Mismatches found: 1 (poll near-expired context), fixed with unit test `matching_long_polls_with_nearly_expired_deadline_return_internal_like_go`.
Not covered (not verified): per-line Thrift exception field comparison; domain-deprecated handling beyond DomainNotActive.

CI run: (filled below)
