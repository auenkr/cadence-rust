# Matching History full-wrapper correction — 2026-10-19

The earlier timeout audit traced only the raw redirect client and is incomplete for production.

Production chain: Matching RecordTaskStarted attempt/retry → resource retryable History client → factory timeout wrapper → raw History ownership redirect client → transport (with metering/error injection inside factory).

- `go/client/clientfactory.go:109-145`: NewHistoryClient uses NewHistoryClientWithTimeout and wraps the raw History client in timeoutwrapper.NewHistoryClient.
- `go/client/wrappers/timeout/timeout.go:43-44`: HistoryDefaultTimeout is 30 seconds. Generated History methods create a context using this timeout, bounded by any earlier caller deadline.
- `go/common/resource/resource_impl.go:289-300`: resource wraps History in retryable.NewHistoryClient with CreateHistoryServiceRetryPolicy and service retry classifier.
- `go/client/wrappers/retryable/history_generated.go`: DescribeWorkflowExecution, GetMutableState, RefreshWorkflowTasks and Record methods use throttleRetry.Do. Do not remove retries based on the raw redirect client alone.
- `go/service/matching/service.go:72`: classifier is common.IsServiceTransientError. Exact policy, classification and nesting are being audited by Luna.
- `go/service/matching/handler/engine.go:1471-1528`: Record calls have an additional Matching per-attempt timeout/retry. Default one-second RecordTaskStartedTimeout differs from the factory default 30-second History timeout.
- `go/client/history/client.go:1160-1188`: raw redirect only checks context and follows typed ownership loss.

Routing round r6 completed source-only in `/home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b`. Four files contain typed redirects, resolver/Go-fixture comparison and query deadline. No Cargo verification, no acceptance. Its unbounded nonstarted-call assumptions need the wrapper correction before acceptance. Runtime discovered-peer injection remains open.

Refresh cancellation source fix is owned by cache_luna; production cache integration by recover_workers. outbound_luna retains the only heavy build grant for SWIM, reusing its existing target. Matching remains incomplete; no later-service migration authorized by parity gates yet. Root AGENTS.md is the user's uncommitted edit and must remain untouched.

## Follow-up audit and work queue

History resource retry policy is initial 50ms, max 10s, expiration 30s (`common/util.go:54-56`). `WithContextExpiration` is absent from generated retryable History construction: expiration prevents starting further retries rather than cutting off an active call. Each factory timeout-wrapper invocation creates its own 30s context. Retry backoff applies the separate ServiceBusy throttle policy; `ThrottleRetry.Do` may return the previous error on terminal subsequent failure, exhausted backoff, or cancellation during wait. Keep this separate from Matching Record retry (250ms initial / 1s max / five attempts, no expiration, per-attempt timeout).

Active source correction task r7: `node:delegated-task:command%3Amcp%3A6a1e03e1-ed6f-4ea2-b645-7d7964f28ed9%3Adelegate-task%3Amatching-history-full-wrappers-luna-20261019-r7`, worktree `/home/ubuntu/.t3/worktrees/cadence-rust/t3-04a5b54d`, Luna standard tier; no build grant.

Refresh source candidates: `7c5d9d2e8` then child `7344efc4b` in matching-domain-metadata; compilation/tests pending. Updated tests exercise inactive-domain delivery branch cancellation/deadline with refresh drop and durable rewrite assertions. Not integrated yet.

SWIM bounded component review found fixed-period scheduling differs from pinned Go gossip.go adaptive cadence, and abort-only sync Stop differs from Go waiting for in-flight protocol loop. outbound_luna will record these as open gaps and freeze the currently scoped component after real probe/lint. Original focused server run: 54 passed, one test failed because 5ns timeouts are below Tokio timer-wheel resolution and timer task had not polled. Corrected 5ms focused timer test reported passed; final complete focused run remains pending. No Matching completion claim.
