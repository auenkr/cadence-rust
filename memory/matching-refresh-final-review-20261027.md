# Matching refresh final review — 2026-10-27

Reviewed committed `94276d20fe09cc8dc79b678e3791533349caacad` as a read-only review. No source changes, Cargo/Go/rustfmt commands, CI dispatch, or runtime validation were performed. This report does not claim a runtime pass; CI run `40535623193` is owned by the parent and was still running at review time.

## Verdict

No confirmed correctness bug found in the reviewed refresh persistence-error classification, retry/cancellation behavior, deadline forwarding, or failover callback test assertions.

The Rust behavior matches the Go control flow reviewed here:

- `matching_partition.rs:168-184`: a refresh executes the first store read even if already cancelled, then retries transient errors through `persistence_until`; cancellation during a wait returns the latest/previous persistence error, not a context cancellation error. It updates/clears the in-memory cache only after the read succeeds. This follows `go/common/backoff/retry.go:221-270` (`Do` stores `prevErr`, performs an attempt before examining `ctx.Done`, and returns `prevErr` when cancellation interrupts backoff) and `go/service/matching/tasklist/task_list_manager.go:425-447` (`RefreshTaskListPartitionConfig` returns the retry result and updates the cache only on success).
- `matching_partition.rs:16-43,175-179`: persistence `NotFound` becomes gRPC NotFound, throttling becomes ResourceExhausted with typed `ServiceBusyError`, and other persistence failures become Internal without typed details. This matches Go `handler/context.go:83-147`: entity-not-found and service-busy errors pass through, while uncategorized persistence failures become `InternalServiceError`. `go/service/matching/handler/engine.go:1281-1311` returns the manager refresh error unchanged; `go/service/matching/handler/handler.go:498-522` applies `handleErr`.
- `matching_registry.rs:2298-2332`: config-bearing notifications push immediately, while nil-config reload parses the child task list to its root before reading persistence. A dropped caller sets the cancellation flag observed by the blocking retry. This is consistent with Go’s immediate non-nil cache update and nil-config root reload in `task_list_manager.go:425-447`; the cancellation addition does not alter the operation’s first-attempt/error behavior.
- `grpc_matching.rs:3553-3600` captures the incoming gRPC deadline before consuming request data and forwards it to the registry reload. This constrains server-side retry waits to the caller’s deadline while retaining Go’s behavior of attempting the first read.

## Test review and limitations

- `matching_partition_cancellation_tests.rs` exercises caller-drop cancellation of the detached update retry and an RPC refresh with already-expired timeout across permanent, timeout, and throttled backend classifications. It checks one attempted read, unchanged cache, live queue after refresh failure, and expected status/detail classes. This is focused regression coverage, not broad handler or transport interoperability coverage.
- `matching_failover_callback_tests.rs:47-101` uses the actual `MatchingRuntime::start_domain_cache` callback wiring. Its three filtered cases are consistent with `matching_runtime.rs:811-823`: local domain, active-active global domain, and stale failover version do not release; the later non-active-active global version advance does. The active-active fixture correctly makes `active_clusters_by_cluster_attribute` nonempty, the predicate used by `DomainReplicationConfiguration::is_active_active` (`cadence-api/src/domain.rs:11-21`). The catch-up case establishes version 10 and verifies no release; the accepted case advances to 13 and checks the blocked poll releases while the same owner queue remains.
- The failover test’s negative assertions rely on 20 ms timeouts, so they are timing-sensitive regression checks rather than deterministic proof that no delayed release can occur. The test also calls `runtime.shutdown()` only on success; a panic before that point can leave the cache maintenance thread running until test-process teardown. These are test robustness weaknesses, not demonstrated product defects or false assertions.

## Scope classification

No product blocker identified in the reviewed code. The cancellation test does not model a transient error followed by cancellation specifically at the retry wait, nor does this review validate the full SDK/service workflow; those are coverage limits. No Cargo/Go test or runtime result is inferred from source inspection.
