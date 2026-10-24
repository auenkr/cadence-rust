# matching-nodiff-b-20261024: last four proposed accepted differences + TChannel TLS

Source: `memory/matching-open-gaps-20261024-v2.md` section 4b (on `origin/matching-open-gaps-v2-20261024`).
Directive: no accepted differences. Rust must match Go behaviour. Keep API-neutral. Do not merge.

## Status table

| # | test | file | status in this branch |
|---|------|------|-----------------------|
| 1 | `setup_executor_with_empty_config_installs_noop_executor` | `tests/matching_engine_go_tests_c.rs` | un-ignored, asserts `ShardExecutorSetup::Noop` and NotFound lookups. Launcher wires the noop policy when namespaces are empty. |
| 2 | `shard_distributor_onboarded_partial_percentage_is_engine_wide` | `tests/matching_engine_go_tests_c.rs` | un-ignored, asserts `shard_distributor_onboarded(0|1|100)`. Runtime enable closures use the predicate. |
| 3 | `go_handle_err_passes_typed_errors_through_unchanged` | `tests/matching_partial_upgrade_a.rs` | **STILL IGNORED (open)**. See "Item 3" below. |
| 4 | `go_handler_start_calls_engine_start_once_and_stop_calls_engine_stop_once` | `tests/matching_partial_upgrade_a.rs` | un-ignored against new `matching_handler::Handler`. Production start/stop wiring is partial, see below. |
| TLS | TChannel TLS question | `tests/matching_tchannel_tls_guard.rs` (new) | new source-guard tests. Go and Rust both plaintext. |

## Go evidence (read on this branch's base e7285da4f)

- `go/service/matching/handler/engine.go:221` `setupExecutor`: with `len(Namespaces)==0`, installs `executorclient.NewNoopExecutor` and a `taskListsFactory`. With more than one namespace, `getValidatedShardDistributorConfig` calls `Fatal`. `reportTTL` defaults to `_defaultSDReportTTL` unless the single namespace sets `TTLReport`.
- `engine.go:281` `shardDistributorOnboarded()` = `percentageOnboarded.Value() > 0`. This is the executor's `Enabled` predicate (heartbeat gate), so it is engine-wide.
- `engine.go:1703` `isExcludedFromShardDistributor` = `membership.TaskListExcludedFromShardDistributor(name, uint64(pct), excludeShortLived)`. That is `(excludeShortLived && uuid) || hash%100 >= pct`. This is a separate, per-task-list rule. Rust `task_list_excluded_from_shard_distributor` already matches it.
- `engine.go:1626` ownership: non-excluded task lists call `executor.GetShardProcess`. `ErrShardProcessNotFound` becomes `NotOwnedByHost "not known"`. Excluded task lists go through the membership lookup.
- `handler/handler.go:95` `Start` = `engine.Start()` then `startWG.Done()`. `Stop` = `engine.Stop()`.
- `handler/context.go:83` `handleErr`: 14 typed errors are returned unchanged with a counter bump. Default is `InternalServiceError{Message: err.Error()}`.
- `common/rpc/factory.go:63` TChannel uses `tchannel.NewChannelTransport(ListenAddr)` with no credentials. `grpc.InboundCredentials` applies only to gRPC. `yarpchttp.InboundTLSConfiguration` applies only to HTTP. `common/config/config.go:140` documents `rpc.tls` as "only on gRPC port". TChannel outbounds use `createDialer` only for the gRPC transport.

## Changes

1. `matching_shard_routing.rs`: new `pub fn shard_distributor_onboarded(i64) -> bool` (`> 0`). Used by `matching_runtime.rs` for the executor and spectator `enabled` closures. This replaces the inline `percentage() > 0`.
2. `matching_shard_distributor.rs`: `NoopShardExecutor` (lookups return `NotFound`, `has_local` false). `ShardExecutorSetup {Noop, Live{report_ttl_nanos}}` and `setup_shard_executor(config)`, which mirrors Go `setupExecutor` and `getValidatedShardDistributorConfig`.
3. `matching_runtime.rs`: `with_noop_shard_distributor(membership, pct, exclude)`. It installs a `QueueOwnershipPolicy` whose eligibility is Go's `isExcludedFromShardDistributor` and whose local-processor check is always false. It also installs the ownership check with a NotFound lookup.
4. `launcher.rs`: the percentage and exclude closures moved above the shard-distributor branch. The live branch now passes `.clone()`. A new `else` branch calls `with_noop_shard_distributor` when `run_matching` and namespaces are empty and Ringpop membership exists.
5. `matching_handler.rs` (new, `pub mod`): `EngineLifecycle` trait and `Handler<E>`. `start()` and `stop()` each call the engine once and return whether they did.

## Behaviour impact to verify in CI

- Default deployments (no namespaces, percentage 0) now install a policy and an ownership check. With percentage 0 every task list is excluded, so the policy is inert. The check falls through to `membership.check_task_list`. Go's `errIfShardOwnershipLost` also does a membership lookup for excluded lists, so this should match Go. CI must confirm no existing test assumes the old unset-hook path.
- Namespaces empty and percentage above 0: Go returns NotOwnedByHost for non-excluded task lists. Rust now does the same through the noop policy. This is an intentional behaviour change toward Go.

## Item 3: `handleErr` (OPEN, not done in this pass)

Go has one `handleErr` that every RPC passes through. Rust has no unified error type. Errors are mapped per call site, for example `queue_status` in `matching_dispatch.rs:448` and `semaphore_status` in `grpc_matching.rs:167`. A faithful port needs a single matching error enum or one central status classifier covering every RPC, plus the 14-type identity table. That is a cross-cutting refactor that cannot be checked by reading alone. It is not done here. The test stays `#[ignore]` with its reason. Next step: introduce `handle_err` over one error enum and route all RPCs through it.

## Item 4: production wiring (partial)

`Handler` is the Go seam, and its once-only semantics are tested. Production still uses `with_start_pending` and `mark_started` in `launcher.rs`, and `MatchingRuntime::shutdown` (async) for stop. The `EngineLifecycle` trait is sync, so wiring stop needs either a sync stop path or an async trait. Not done here.

## TLS

Go has no TLS on TChannel (see Go evidence). Rust `tchannel.rs` has no TLS reference, and the launcher binds the Matching TChannel port with plain `TcpListener::bind`. The guard test `matching_tchannel_tls_guard.rs` checks both facts from source, following the `include_str!` pattern already used in `matching_dyncfg_defaults_tests.rs`. It is a static guard, not a socket test.

## Verification

- No local cargo (per directive). Verification is GitHub Actions `rust-ci.yml` with `package=cadence-matching`, then a whole-workspace run on the final head.
- Head SHA and run IDs are recorded below after the runs.

## CI evidence

- Code head `af4c30001` (after merge of origin/master 69a3f72e5 and the lib.rs attribute fix).
- `cadence-matching` package run: `40067793834`, success.
- Whole workspace run: `40067992637`, success.
- Earlier red runs, all clippy `result_large_err` from the attribute fix: `40067400086`, `40067667306`.
- Item 3 (`handleErr`) and the production wiring of item 4 remain open; see sections above.
