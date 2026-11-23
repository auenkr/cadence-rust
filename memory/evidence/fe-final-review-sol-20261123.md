# Frontend parity review, 2026-11-23

Reviewed origin/master at `851749ff74d8c81b9953c3dbf93cb043400d5f31` by tracing Go and Rust production call paths. Findings are source-derived. No Cargo, Go, or Docker commands were run. Pinot visibility was excluded.

| Severity | Count |
| --- | ---: |
| High | 3 |
| Medium | 4 |
| Low | 1 |

## High — Go and Rust use incompatible domain replication queue payloads

- Go: `go/service/frontend/admin/handler.go:976`, `go/common/domain/replication_queue.go:65`, `go/common/domain/replication_queue.go:167`, `go/common/codec/version0_thriftrw.go:44`, `go/common/codec/version0_thriftrw.go:64`.
- Rust: `rust/crates/cadence-persistence/src/domains.rs:261`, `rust/crates/cadence-service-core/src/admin.rs:1579`, `rust/crates/cadence-service-core/src/admin.rs:1886`.
- Go prefixes each persisted task with the ThriftRW version byte `0x59` and requires that byte on decode. Rust publishes the unframed Thrift struct and feeds stored rows directly to a struct decoder. A Rust Frontend taking over a queue containing Go writes fails at the first Go-framed row. Rollback to Go likewise fails on Rust-written rows. Domain DLQ reads and merges share the Rust decoder assumption.
- Suggested fix — Use the Go versioned codec for new writes and all queue and DLQ read paths. Account for previously written unframed Rust rows during transition. Exercise both directions with Go-generated payload fixtures and a persisted cursor.

## High — Workflow RPCs ignore the Go outbound transport selector

- Go: `go/common/rpc/params.go:76`, `go/client/clientfactory.go:117`, `go/client/clientfactory.go:148`.
- Rust: `rust/crates/cadence-server/src/main.rs:75`, `rust/crates/cadence-frontend/src/workflow_backend.rs:1218`, `rust/crates/cadence-frontend/src/workflow_backend.rs:372`.
- Go selects a gRPC or TChannel stub and matching named membership port for both History and Matching. The installed Rust public workflow backend always serializes Thrift and calls TChannel. A mixed cluster configured to route those calls through the gRPC endpoints still receives Rust workflow calls on TChannel, and the RPCs fail if those endpoints are the only reachable ones.
- Suggested fix — Select the outbound adapter and corresponding membership port together using the configured Go transport rule. Exercise real frontend-to-History starts and frontend-to-Matching polls with both settings.

## High — History continuation drops the version-history guard on failover

- Go: `go/service/frontend/api/handler.go:1903`, `go/service/frontend/api/handler.go:1981`, `go/service/history/engine/engineimpl/poll_mutable_state.go:191`.
- Rust: `rust/crates/cadence-frontend/src/workflow_matching.rs:863`, `rust/crates/cadence-frontend/src/workflow_matching.rs:974`.
- Go sends the continuation token's last version-history item back to History. History can reject a stale item after a branch switch with `CurrentBranchChangedError`. Rust saves the item in the token but sends only the branch token, so Go History can initialize the missing item from the new branch and the resumed poll can continue using the old event cursor against that branch.
- Suggested fix — Encode the token's version-history item in field 50 of the resumed `PollMutableState` request and preserve Go's status mapping for a branch change. Verify the behavior by resuming an open history poll across a branch switch.

## Medium — Frontend History retries differ for typed service failures and owner changes

- Go: `go/service/frontend/service.go:101`, `go/common/resource/resource_impl.go:296`, `go/common/util.go:54`, `go/common/util.go:159`, `go/common/util.go:294`, `go/common/util.go:311`, `go/client/history/client.go:1160`.
- Rust: `rust/crates/cadence-frontend/src/workflow_backend.rs:36`, `rust/crates/cadence-frontend/src/workflow_backend.rs:147`, `rust/crates/cadence-frontend/src/workflow_backend.rs:2156`, `rust/crates/cadence-frontend/src/workflow_matching.rs:884`.
- Rust retries every typed `ServiceBusyError`, including the workflow-ID rate-limit reason that Go returns immediately. It returns typed internal errors immediately, where Go can recover by retrying. For ownership loss Go immediately follows the owner from the exception, while Rust sleeps and re-resolves membership, which can still point to the old owner. Rust also uses a 250 ms linear backoff and 25 s budget where Go uses the 50 ms exponential policy with a 10 s cap and 30 s expiration. The history paging path issues `PollMutableState` directly and bypasses even the Rust retry loop.
- Suggested fix — Port the Frontend retry predicate and policy, including caller deadline handling. Separate immediate owner redirects from backoff retries and use the common path for history polling.

## Medium — Nested schedule and domain operations re-enter public wrappers

- Go: `go/service/frontend/api/handler_schedule.go:286`, `go/service/frontend/api/handler_schedule.go:661`, `go/service/frontend/api/domain_handlers.go:185`, `go/service/frontend/service.go:154`.
- Rust: `rust/crates/cadence-service-core/src/schedule_flow.rs:371`, `rust/crates/cadence-service-core/src/schedule_flow.rs:409`, `rust/crates/cadence-service-core/src/schedule_flow.rs:572`, `rust/crates/cadence-service-core/src/yarpc.rs:268`, `rust/crates/cadence-frontend/src/handler_workflow.rs:921`, `rust/crates/cadence-frontend/src/handler_workflow.rs:1072`.
- Go calls the underlying handler for the scheduler workflow start and the visibility reads. In Rust these calls enter the full workflow dispatch again. For example, a `CreateSchedule` caller can pass the outer quota and then be throttled on the synthetic `StartWorkflowExecution`. A schedule list served locally under Go can invoke a second redirection decision for `ListOpenWorkflowExecutions` under Rust. The Thrift `DeleteDomain` visibility check can also be rejected by the nested workflow's authorization or quota after passing domain authorization.
- Suggested fix — Expose a local handler entry point for nested calls. Keep handler validation and backend routing for the nested operation, with public authorization, rate limiting, version checking, and cluster redirection applied at the outer API boundary.

## Medium — Worker poll quota waits do not reserve in Go's stage order

- Go: `go/service/frontend/wrappers/ratelimited/ratelimit.go:65`, `go/common/quotas/multistageratelimiter.go:114`.
- Rust: `rust/crates/cadence-frontend/src/wrappers/quota_policy.rs:111`, `rust/crates/cadence-frontend/src/wrappers/quota_policy.rs:176`, `rust/crates/cadence-frontend/src/wrappers/quota_policy.rs:215`.
- With `frontend.maxWorkerPollDelay` enabled, Go waits and consumes a task-list token, then a domain token, then a global token. If a subsequent stage times out, prior tokens stay consumed. Rust repeatedly tries an all-or-nothing reservation, returning held collection reservations if a later stage rejects. With a depleted global quota and tokens remaining at earlier stages, identical failed polls leave different capacity for later polls.
- Suggested fix — Add the Go stage-ordered wait path for worker polling, including consumption and caller-deadline semantics. Keep the immediate admission path for other calls.

## Medium — Async Kafka ignores configured TLS server names

- Go: `go/common/asyncworkflow/queue/kafka/config.go:52`, `go/common/config/tls.go:73`.
- Rust: `rust/crates/cadence-frontend/src/async_kafka.rs:12`, `rust/crates/cadence-frontend/src/async_kafka.rs:103`, `rust/crates/cadence-frontend/src/async_kafka.rs:237`.
- Go uses `tls.serverName` for the TLS handshake. Rust deserializes it but the broker connection does not apply it. With host verification enabled, a broker dialed through an address outside the certificate's names can accept Go async workflow publishes and reject Rust publishes using the same config.
- Suggested fix — Pass the configured override through to broker TLS server-name selection and test a broker alias with a certificate covering the configured name.

## Low — RemoveTask reports a different error for an omitted task type

- Go: `go/service/frontend/admin/handler.go:308`.
- Rust: `rust/crates/cadence-service-core/src/yarpc.rs:2816`, `rust/crates/cadence-service-core/src/admin.rs:2221`.
- For a Thrift request with a valid shard and no `Type`, Go returns `BadRequestError("Request is nil.")`. Rust returns `BadRequestError("TaskType is not set on request.")`. If the shard is also invalid, Rust's shard check wins, while Go checks the missing type before delegating to History.
- Suggested fix — Validate Thrift task-type presence before shard validation and use Go's `ErrRequestNotSet` message.
