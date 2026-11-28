# Gap audit: visibility limiters/sampling and throttled logger (2026-11-28)

Branch `gap-visibility-throttlelog`, code head `12b58c97368af924c1d4fd25743965ee9d46e173` (this evidence file is committed on top).

| Item | Go | Rust | Status |
|---|---|---|---|
| Priority token bucket (`NewFullPriorityTokenBucket`, `GetToken`, refill/overflow) | go/common/tokenbucket/tb.go | rust/crates/cadence-common/src/priority_token_bucket.rs | **Ported (new)**, 6 unit tests |
| Per-domain bucket factory (`NewDomainToBucketMap`) | go/common/persistence/wrappers/sampled/tokenbucketfactory.go | same file, `DomainToBucketMap` | **Ported (new)** |
| History write sampling (open: 1 prio; closed/upsert: 2 prio, Completed=low) | wrappers/sampled/visibility_manager.go:88-155, :292; wired at persistence/client/factory.go:574 | cadence-history/src/wiring/assembly/queue_deps.rs (`VisibilitySampling`, wired in `TransferDepsImpl::new` and the 3 record fns) | **Ported (new)**; config keys already existed (config.rs:313-316: `system.enableVisibilitySampling` false, `history.historyVisibilityOpenMaxQPS`/`ClosedMaxQPS` 300). Enabled flag read once at construction, like Go. |
| Frontend list sampling (`frontend.visibilityListMaxQPS`, default 10, ServiceBusy "Persistence Max QPS Reached for List Operations.") | visibility_manager.go:300 | cadence-frontend/src/workflow_visibility.rs `ListSampler` | Existed; **rebased onto the Go bucket** (was a continuous-refill approximation) |
| Sampling order: DB store -> errorinject -> ratelimited -> sampled -> metered | factory.go:566-585 | n/a: Rust visibility store has no ratelimited/errorinject wrappers in history path | Order differs; not changed |
| `PersistenceSampledCounter` metric and "Request ... is sampled" info log | visibility_manager.go:99-107 | not emitted | **Gap** (observability only) |
| `VisibilityArchivalQueryMaxQPS` | constants.go:1597 | not used in Rust either; Go has no reader beyond the key | No action |
| Throttled logger type (`NewThrottledLogger`) | go/common/log/throttle.go | matching: cadence-matching/src/matching_throttled_logger.rs; frontend: handler_workflow.rs `allow_throttled_log` (key `frontend.throttledLogRPS`, 20) | Matching and frontend present at poll/GetHistory sites |
| History throttled logger call sites | history/decision/handler.go:97, engine/engineimpl/history_engine.go:189, shard/controller.go:144,167 | `history.throttledLogRPS` config only (config.rs:469); no use | **Gap**, log-only; deferred to the History service |
| cache/budget.go:274 throttled logger | go/common/cache/budget.go | no Rust equivalent checked | Not checked |

## CI (rust-ci.yml, `package=` runs, head 12b58c973)
- cadence-common: https://github.com/auenkr/cadence-rust/actions/runs/46112861698 success
- cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/46112863912 success
- cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/46112866305 success

Earlier failures fixed along the way: rustfmt (46109732413) and clippy `unnecessary_map_or` (46110561861).

Caveats: history sampling is unit-tested at limiter level only; no mixed-cluster run with sampling enabled.
