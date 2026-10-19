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
