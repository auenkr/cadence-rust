# Review: Matching cold-cache metric domain resolution (2026-10-27)

Reviewed source commit `c2e631b4383a2b61313654d47588da727f058bdb` (branch head `005b7849fbab2db3d4cd1b499831ec222626ac81`). Focused Actions run `40472811597` was reported passed; no additional CI/runtime validation was run as part of this review.

## Findings

No concrete blockers found within the requested scope.

Go `go/service/matching/handler/handler.go:152-218, 258-388, 476-532, 559-565` calls `domainName` before constructing its handler context and its worker/user rate-limit checks. `domainName` uses `domainCache.GetDomainName` and maps returned lookup errors to the empty name. The Go cache miss path checks persistence and refreshes before retrying the ID (`go/common/cache/domainCache.go`, `GetDomainName` / `getDomainByID`).

Rust `rust/crates/cadence-matching/src/grpc_matching.rs:463-490` wires the metric resolver to `MatchingDomainCache::get_domain_name`, awaits it through `spawn_blocking`, then flattens both join failures and returned lookup errors to `None`. The request metric is emitted before quota checks in the changed handlers. All 12 production call sites were updated: poll decision/activity, add decision/activity, query workflow, acquire semaphore (including context-timeout metric), update/refresh task-list partition config, cancel poll, respond query completion, and describe task list. `matching_domain_cache.rs:712-733, 768-875` performs cache hit/read-through behavior; on misses this may do the existence lookup plus a refresh scan, matching the Go cache flow. Returned persistence/cache errors stay swallowed for telemetry.

Cancellation note: cancelling a request stops awaiting the blocking task but cannot abort an already-running `spawn_blocking` closure. It may therefore finish its bounded synchronous metadata lookup/cache refresh after the RPC is gone. The lookup uses the cache's three-second persistence bound (`matching_domain_cache.rs:24`) and Go's matching handler uses `context.Background()` for this lookup, so this is not a behavior divergence found in scope.

## Limits / integration recommendation

The added Rust regression test uses an injected resolver and verifies metric attribution, not a real cold persistence cache, Go SDK workflow, or mixed Go/Rust cluster. CI pass `40472811597` is focused crate validation only. Parent integration owner should separately verify cold domain creation immediately before an RPC against the real cache/persistence adapter and confirm domain tagging before quota rejection; broader Matching mixed-cluster gates remain necessary for service completion.
