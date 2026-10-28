# Matching native host admission update (2026-10-28)

Status: implementation source updated; **unverified**. No Cargo, rustfmt, Go, build, test, or check command was run locally. Parent CI owns validation.

## Changes

- `MatchingRpcQuota` retains its existing `allow(MatchingRpc, Option<&str>)` API and adds `allow_go_string`. Per-domain bucket identity is now `(RpcClass, GoString)`, so raw byte names such as `tenant-\xff`, `tenant-\xfe`, and UTF-8 `tenant-\xef\xbf\xbd` remain separate. Dynamic config filters remain Go-compatible UTF-8 domain names; invalid UTF-8 names receive their own bucket and use the host-rate fallback. Host user/worker buckets, lazy dynamic-rate refresh, reservation return ordering and control RPC exemptions are shared with the old path.
- Native host admission passes raw `GoString` through quota without UTF-8 conversion or rejecting the request. Existing domain-name resolution and its error behavior are unchanged.
- Native host startup state now has starting/running/draining transitions. `begin_shutdown()` is callable by runtime/launcher code, wakes startup waiters, and rejects later RPC entry. Registry cached-owner behavior remains in the registry and is not modified here.
- Added bounded source tests for byte-domain quota separation and startup/drain gate transitions. These tests are not run here.

## Go policy reference

`go/service/matching/handler/handler.go` constructs separate user and worker multi-stage limiters with a host dynamic limiter and domain fallback collection. Worker task/poll methods and user query/describe/config methods reject over limit; `RespondQueryTaskCompleted`, `CancelOutstandingPoll`, and `RefreshTaskListPartitionConfig` call `Allow` for accounting but accept over-limit calls. The existing Rust `MatchingRpc::policy` classifications remain intact.

## Remaining host gap

The host metric callbacks still accept only the RPC method (and status on failure), so they cannot represent all Go metric tags/context. This change does not claim metric parity. Native request context propagation was not broadened because it would require churn across the per-RPC service interface.

## Validation/evidence

No test or binary evidence is available from this change. Parent CI run previously reported by task owner: https://github.com/auenkr/cadence-rust/actions/runs/40719459374 (pending at task start; outcome not checked here). Matching remains incomplete; this note does not claim service completion.

Parent registered native semaphore request module for CI. CI 40719459374 failed on native manager passing owned GoString parameters to borrowed store methods and missing Debug on the native semaphore limiter wrapper. Corrected the borrowed parameters, added a wrapper Debug implementation and removed unused copied poison-test hooks. These source fixes are unverified pending the next CI run.

Fresh six-crate CI https://github.com/auenkr/cadence-rust/actions/runs/40720317784 at 8b6fcdac0 (source SHA pinned by workflow), clean target and runner formatting enabled. Conclusion pending. Includes service-core quota changes.
