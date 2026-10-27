# Matching cold canonical domain metric attribution (2026-10-27)

## Source trace

Go `go/service/matching/handler/handler.go` resolves `h.domainName(request.DomainUUID)` before creating the handler metric context and before worker quota checks (for example `AddDecisionTask` and `AddActivityTask`, lines 152 and 218). `h.domainName` calls `domainCache.GetDomainName` and swallows lookup errors to an empty string (lines 559-565). In `go/common/cache/domainCache.go`, `GetDomainName` calls `getDomainByID`; a miss checks persistence, takes the refresh lock, refreshes, and retries the exact raw ID (lines 400-409 and 625-676).

Rust production startup does perform a synchronous initial domain-cache scan before constructing the runtime handler (`matching_runtime.rs`, `start_domain_cache`, and `MatchingDomainCache::start`). That does not cover domains created after Matching starts and before its periodic refresh. Go handles those through its miss read-through; Rust previously wired `metric_domain_name` to `cached_domain_name`, which only reads the snapshot (`grpc_matching.rs` `with_domain_cache`; `matching_domain_cache.rs` `cached_domain_name`). The inbound request metric is emitted before `check_rpc_quota` in the affected handler methods (`MatchingMetrics::matching_request` records at entry). Thus a newly registered, canonical ID received during that window had no `domain` tag in Rust's request metric while Go tagged it with the domain name. The cache UUID oracle is not used to support this finding; this is the exact canonical ID path.

## Change

The Matching handler's metric-name resolver now uses the cache's Go-compatible `get_domain_name` read-through lookup, run via `tokio::task::spawn_blocking`. Lookup errors remain swallowed for metrics, as in Go. This occurs before the request metric and quota check, so a successful cold lookup also publishes refreshed metadata through the existing cache callback before quota evaluation. Cache hits and cache misses both use the blocking pool when the production resolver is installed; there is no synchronous persistence work on a Tokio executor thread. The snapshot-only `cached_domain_name` API remains available for callers that require nonblocking telemetry.

A focused test records the emitted `AddDecisionTask` request metric and asserts its canonical cold-domain `domain` tag. The test uses an injected read-through resolver to model the cache result; it is a handler metric regression, not a live Go SDK or mixed-cluster oracle.

## Validation and limits

`git diff --check` passed before commit. No Cargo, Go, build, formatting, or test command was run locally, per project CI policy. The parent owns runtime gates. Run the focused `cadence-matching` Actions workflow on the pushed exact head and record its URL, SHA, and conclusion here. The existing Rust metadata cache read timeout remains its configured three seconds; this change does not alter it. The original Go cache lookup runs with `context.Background()` on this path, so persistence timeout parity is an existing bounded-cache difference and is not changed here.
