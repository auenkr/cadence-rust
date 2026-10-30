# Query-dispatch hang review (2026-10-30)

## Scope and CI evidence

- Reviewed frozen source `0032e7b3341275c028952daa82ba302d9ede0f02`.
- CI run [41031374784](https://github.com/auenkr/cadence-rust/actions/runs/41031374784), head `0032e7b3341275c028952daa82ba302d9ede0f02`, was terminal when inspected at 2026-10-30 09:18:34 UTC. It ran 09:09:35–09:17:58 UTC (8m23s); test step ran 09:13:43–09:17:53 UTC (4m10s). Clippy and formatting passed.
- CI test step completed with 986 passed, 1 failed, 2 ignored, in 39.07s. The sole failure was `matching_dyncfg_defaults_tests::reader_defaults_match_go_table`: `matching.numTasklistWritePartitions: no Rust reader found`. This does not point to a hang in the reviewed tests. No Rust/Go/Cargo commands were run locally.

## Hang/deadlock audit

- `grpc_matching.rs` test-only `injected_query_dispatch_fault` clones the callback while holding `query_dispatch_fault`'s `std::sync::Mutex`, then the temporary guard ends at the statement. The callback invocation occurs after that guard is dropped.
- In production query dispatch, `query_workflow` calls `register_guarded` before the injected fault boundary. This registers a oneshot waiter in `waiting`, then the registry's `QueryIndex` entry. The test callback briefly takes its separate captured-task-ID `std::sync::Mutex`, stores the ID, and returns an error synchronously; there is no await in the callback.
- Returning through `?` drops the local `Registration`. `withdraw_generation` takes and releases `waiting`, removes the matching-generation index entry (the index lock is not nested under `waiting`), then retains the pending queue without this task. `QueryIndex::deliver` likewise clones under the index lock and releases it before taking `waiting`, so the reviewed paths do not create an inverse nested-lock order.
- The test's subsequent late result lookup is synchronous and must return `NotFound`; its pending-offer assertion waits at most 20ms for `queue.queries.next()` and expects timeout. The query RPC itself has a five-second poll expiration, but the injected error exits before that wait.
- `refresh_rpc_propagates_engine_error_and_recovers_after_fault_clears` checks an immediate engine fault, clears it, retries the same refresh, and shuts down the registry; no blocked store, retry sleep, or unbounded receive is configured in this fixture.
- `startup_failures_stop_provider_and_start_is_idempotent` injects a synchronous subscribe error, asserts `start -> subscribe -> stop`, clears it, and exercises repeated start/shutdown. It contains no wait or timeout that could mask a deadlock.

## Conclusion

No concrete unbounded wait or deadlock risk was found in the requested query-dispatch, refresh-error, or startup-abort paths. Frozen source was left unchanged. The PostgreSQL gate was not scheduled because the specified CI run failed.
