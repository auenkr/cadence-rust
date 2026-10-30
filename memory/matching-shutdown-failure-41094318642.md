# Matching readiness failure and SIGTERM shutdown diagnosis

Run: https://github.com/auenkr/cadence-rust/actions/runs/41094318642  
Head: `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`  
Conclusion: mixed Go/Rust validation failed during Matching readiness; the subsequent Rust shutdown also exceeded the harness grace period. Source diagnosis only; no build, test, or runtime was run for this investigation.

## Retained evidence

- `matching-shutdown.json` records SIGTERM at Unix ns `1793379880110000000` and SIGKILL at `1793379900115000000`, 20.005 seconds later. The exit code is null and `childSignal` is SIGKILL.
- `matching-service-compat.cjs:1747-1767` writes the SIGTERM record in `finally`, sends SIGTERM, waits 20 seconds for `exited`, and only then sends SIGKILL. The readiness exception is thrown before this `finally` (`:562` onward), so cleanup still runs. The harness did not stop Rust before cleanup.
- `probe.log` says readiness failed first: Frontend readiness was cancelled and the direct Rust gRPC probe timed out waiting for an LB pick. Its body reports the subsequent shutdown deadline failure.
- The retained `rust-server-before-restart.log` ends with a `ListTaskListPartitions` `engine-begin` marker. It contains no signal-received or shutdown-stage markers. A preceding request reaches `engine-complete`. This does not locate the post-signal wait.
- The direct probe's transport artifact reports TCP connect success, gRPC channel state `1` (CONNECTING), and a 10.005 second `Deadline exceeded ... Waiting for LB pick`. This is evidence for the readiness failure, not evidence about shutdown.

## Source path and Go comparison

- `launcher.rs:155` installs a Unix SIGTERM receiver. During service it wins a `tokio::select!` alongside `serving.join_next()` and the fatal-writer channel (`:1729-1745`). There is no logging at signal receipt.
- On signal, the launcher sequentially awaits `MatchingMembershipLifecycle::withdraw()`, `MatchingRuntime::shutdown()`, and `stop_after_drain()` before aborting/joining the listener `JoinSet` (`launcher.rs:1750-1785`). Thus a stuck membership, runtime, or provider-cleanup future prevents listener teardown and process exit. `serving.abort_all()` is not reached until those awaits finish.
- Matching gRPC uses `tonic::transport::Server::serve_with_incoming` (`grpc_matching.rs:4105-4175`), with no graceful-shutdown future. The serving tasks are aborted by the launcher only after subsystem shutdown. This is an ownership/order difference from relying on a tonic graceful drain, but the retained artifact cannot show that an in-flight RPC caused this 20 second hang.
- Membership withdrawal calls `provider.self_evict()` in an owned task and awaits its `JoinHandle` (`matching_membership_lifecycle.rs:157-169`). Ringpop self-eviction gathers direct PING tasks and awaits the `JoinSet` (`ringpop_provider.rs:720-784`). Provider teardown then awaits monitor, healer, and `node.destroy_and_wait()` (`matching_ringpop_provider.rs:405-427`); `stop_and_wait()` itself awaits probe/rate tasks and then transition/reverse tasks (`ringpop_provider.rs:782-899`). These are concrete awaited shutdown joins, but the retained run has no per-stage marker or stack showing which one was pending.
- After withdrawal, runtime shutdown drains, signals and joins maintenance, shuts down registries, waits for writers, stops semaphores, unregisters/stops the domain cache, and propagates join errors (`matching_runtime.rs:1027-1182`). These are additional waits before serving tasks are aborted.
- Go Matching `Service.Stop` evicts self, sleeps `ShutdownDrainDuration()`, closes `stopC`, then stops the handler and base resource (`go/service/matching/service.go:139-159`). Rust's runtime documents `matching.shutdownDrainDuration` as call-time dynamic config with fallback zero (`matching_runtime.rs:1213-1227`), but this run did not retain the resolved value. Therefore the artifact does **not** establish that the 20 seconds were an expected configured drain. The 20.005 seconds matches the harness timeout exactly, not an observed drain interval.

## Diagnosis

The exact blocking await is **not established**. There are several source-level joins on the shutdown path, and the terminal log has no signal or teardown markers to distinguish membership self-eviction, Ringpop task joins, registry/writer shutdown, or another cleanup wait. There is no source-backed basis to call this a specific deadlock. Likewise, without the resolved dynamic-config value, the evidence cannot prove the configured drain duration for this process; the harness deadline itself is exactly 20 seconds and the normal source fallback is zero.

No production change is justified by this artifact alone. A decisive rerun should retain timestamped enter/exit markers at SIGTERM receipt and around each launcher await above, record the resolved drain duration, and capture task/thread stacks if one stage exceeds the harness deadline. Keep the existing rule that readiness failure enters cleanup and SIGKILL is sent only after the 20-second wait. The Go reference ordering to preserve is eviction, configured drain, handler stop, then resource stop.
