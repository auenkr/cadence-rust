# Matching admission watch/lock trace (2026-10-30)

## Scope and evidence

Read-only source audit of candidate `3a958668c040b289b495f9b76b2209969db7e2a0`
(`t3/matching-startup-stage-diagnostics`) against PostgreSQL mixed-gate run
[41034380907](https://github.com/auenkr/cadence-rust/actions/runs/41034380907).
The retained Rust log ends at `Matching startup: opening admission` (2,462 bytes);
fd 1/2 are regular file-backed in the harness according to
[`matching-startup-stream-diagnostic-20261030.md`](matching-startup-stream-diagnostic-20261030.md).
No build, test, formatter, runtime, CI, or gate was run for this audit.

## Exact admission path

1. `launcher.rs` creates the production handler with `with_start_pending()`
   before serving, setting its `started` watch to `false` (around line 1525).
   The handler is published to the gRPC and Thrift paths while membership
   starts so those RPCs can wait for readiness.
2. `Matching::wait_started` (`grpc_matching.rs:1115-1121`) subscribes a local
   receiver and awaits `Receiver::wait_for(|ready| *ready)`. The Thrift
   handlers (`thrift_matching.rs:847,872`) and Meta health
   (`grpc_matching.rs:4094`) use this same gate. Tokio 1.53.3 is locked in
   `rust/Cargo.lock`; its `watch.rs::wait_for_inner` puts the value read guard
   in a lexical block, then calls `changed_impl(...).await` after that block.
   Thus a false-state guard is dropped before the await. On success it returns
   a `Ref` only after observing true; Matching immediately discards the result
   of `wait_for` in `wait_started`.
3. After Ringpop/membership startup returns, launcher lines 1619-1624 log the
   combined boundary, call `matching_service.mark_started()`, then
   `runtime.mark_started()`, then log `admission opened`. The first setter is
   `watch::Sender<bool>::send_replace(true)` (`grpc_matching.rs:1112-1114`):
   synchronous, no application mutex and no await.
4. `MatchingRuntime::mark_started` (`matching_runtime.rs:425-429`) only checks
   its optional native host and delegates to `NativeMatchingHost::mark_started`
   (`matching_host_native.rs:247-256`). That uses
   `watch::Sender<u8>::send_if_modified` for the `0 -> 1` transition. Native
   RPC `enter` awaits `check_started` before admission (`matching_service_native.rs:68-74`);
   its receiver uses `wait_for` (`matching_host_native.rs:283-292`). Native
   `wait_for_startup` holds a successful `Ref` only for its final synchronous
   state comparison, then drops it as the function returns; no await occurs
   while that guard is live. There is no `std::sync::Mutex` recursion or await
   in the setter/delegation path.
5. On graceful shutdown, `MatchingRuntime::drain` performs membership eviction
   and drain delay before setting the handler's shutdown flag and native host
   watch value to `2` (`matching_runtime.rs:1024-1044`,
   `matching_host_native.rs:258-262`). Native waiters then return unavailable;
   the legacy started watch remains true through drain while listeners are
   torn down. This path does not hold a watch borrow while calling either
   setter.

## Finding

No watch-guard lifetime, synchronous mutex recursion, or setter-side await
explains a stall at this boundary in the inspected source. In particular,
health waiters do not retain a false-state `watch::Ref` over `wait_for`'s
pending await, and neither admission setter takes a source-level mutex. The
retained marker is before both setters, so it does not identify which setter
(or the immediately following marker write) failed to complete. The log ending
there alone does not prove a source deadlock. No production behavior change or
regression test is justified by this audit; readiness and discovery remain
unchanged.

## Next discriminating trace

On the next reproduction, emit separate begin/complete stage events around
`matching_service.mark_started()` and `runtime.mark_started()`, followed by the
existing `admission opened` event. Preserve the exact Rust/probe logs, SIGTERM
observation, shutdown result, and fd 1/2 targets. If one setter has a begin but
no completion, capture the Rust thread stack at that point; a blocked `write(2)`
would distinguish diagnostic output I/O from the watch update. If both setters
complete but the RPC still waits, trace the listener's request path and the
native `enter -> check_started -> admit` transition. This makes the next
observation local to the first boundary still missing, without changing the
readiness predicate.
