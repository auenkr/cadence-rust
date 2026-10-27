# Review: Matching cold-cache metric domain resolution (2026-10-27)

Reviewed source commit `c2e631b4383a2b61313654d47588da727f058bdb` (branch head `005b7849fbab2db3d4cd1b499831ec222626ac81`). Focused Actions run `40472811597` was reported passed; no additional CI/runtime validation was run as part of this review.

## Findings

No concrete blockers found within the requested scope.

Go `go/service/matching/handler/handler.go:152-218, 258-388, 476-532, 559-565` calls `domainName` before constructing its handler context and its worker/user rate-limit checks. `domainName` uses `domainCache.GetDomainName` and maps returned lookup errors to the empty name. The Go cache miss path checks persistence and refreshes before retrying the ID (`go/common/cache/domainCache.go`, `GetDomainName` / `getDomainByID`).

Rust `rust/crates/cadence-matching/src/grpc_matching.rs:463-490` wires the metric resolver to `MatchingDomainCache::get_domain_name`, awaits it through `spawn_blocking`, then flattens both join failures and returned lookup errors to `None`. The request metric is emitted before quota checks in the changed handlers. All 12 production call sites were updated: poll decision/activity, add decision/activity, query workflow, acquire semaphore (including context-timeout metric), update/refresh task-list partition config, cancel poll, respond query completion, and describe task list. `matching_domain_cache.rs:712-733, 768-875` performs cache hit/read-through behavior; on misses this may do the existence lookup plus a refresh scan, matching the Go cache flow. Returned persistence/cache errors stay swallowed for telemetry.

Cancellation note: cancelling a request stops awaiting the blocking task but cannot abort an already-running `spawn_blocking` closure. It may therefore finish its bounded synchronous metadata lookup/cache refresh after the RPC is gone. The lookup uses the cache's three-second persistence bound (`matching_domain_cache.rs:24`) and Go's matching handler uses `context.Background()` for this lookup, so this is not a behavior divergence found in scope.

## Limits / integration recommendation

The added Rust regression test uses an injected resolver and verifies metric attribution, not a real cold persistence cache, Go SDK workflow, or mixed Go/Rust cluster. CI pass `40472811597` is focused crate validation only. Parent integration owner should separately verify cold domain creation immediately before an RPC against the real cache/persistence adapter and confirm domain tagging before quota rejection; broader Matching mixed-cluster gates remain necessary for service completion.

## Integrated harness review (2026-10-27)

Reviewed the integrated optional probe in `tests/run-go-service-baseline.py` and `tests/matching-service-compat.cjs` at repository head `79b97f26a`. Source-only review; no Cargo/Go build, runtime gate, or Actions run was performed.

One concrete blocker: `run-go-service-baseline.py:529-531` configures `matching.domainworkerrps` to `0`, but `matching_quota.rs:233-247` treats any nonpositive domain rate as a fallback to `matching.workerrps` (the default unlimited rate). Therefore `matching-service-compat.cjs:547-548` cannot validly expect this setting to reject `AddDecisionTask`; it should be admitted and may create a durable task. To test a rejection instead, configure a positive rate of `1` and launch two `AddDecisionTask` calls concurrently for that domain, asserting one is rejected with `RESOURCE_EXHAUSTED`. The bucket's one-token initial burst admits one call and rejects the other; concurrent calls avoid timing the first call's backend completion against the one-second refill.

Other reviewed assertions are source-consistent: the probe runs after `waitForRustReady`, sends through the exact primary Rust gRPC client, and uses `AddDecisionTask` (the worker quota class). `matching_request` emits the canonical request counter before entering the future where `check_rpc_quota` rejects, and receives `rpc-caller` plus `grpc` from the request metadata. The `domain` and `tasklist` labels match request-tag names; Prometheus sanitizes values by replacing non-ASCII-alphanumeric/underscore characters with `_`, matching the validator. The 1.1-second delay follows readiness; production cache startup performs its initial scan synchronously, and cold ID lookup does a read-through plus refresh, so the late registered domain can resolve before quota rejection. The evidence file is written under the supplied `rust-matching` directory and the validator reads that same path.

Validation limit: these are source-level conclusions only. The integrated probe has not been run in Actions or against a mixed cluster, and its current zero-rate rejection claim is invalid until corrected.
