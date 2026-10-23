# P1-4 and P1-6 notes (2026-10-24)

Base: `origin/master` @ `e9b8e9a0e`. Branch: `matching-p1-4-6-20261024`. Source of the items: `memory/matching-open-gaps-20261024.md` (on `origin/matching-open-gaps-20261024`).

No cargo was run locally. Verification is the `rust-ci.yml` run recorded at the bottom of this file.

## P1-4: adaptive scaler Start/Stop handle

**Status before:** STILL OPEN. The ignored gap test `matching_adaptive_scaler_lifecycle_start_stop_is_idempotent_gap` (`matching_scaler_go_tests.rs`) had no body. The scaler had no start/stop handle; the lifecycle lived in `Owner.scaler: Mutex<Option<AbortHandle>>` in `matching_registry.rs`.

**Go reference:** `go/service/matching/tasklist/adaptive_scaler.go:114-132`. `Start` runs the periodic loop only from the Initialized state (CAS). `Stop` cancels and waits only from the Started state. Both are idempotent, and a stopped daemon never restarts. Test: `go/service/matching/tasklist/adaptive_scaler_test.go:87` (`TestAdaptiveScalerLifecycle`).

**Change:**
- `matching_scaler.rs`: new `ScalerDaemon` with `start(future)`, `stop()`, `is_started()`. Status is `Initialized | Started(AbortHandle) | Stopped`. `start` spawns only from Initialized. `stop` aborts only from Started, then sets Stopped.
- `matching_registry.rs`: `Owner.scaler` is now a `ScalerDaemon`. `abort_scaler` is removed. `finish` and `Drop` call `scaler.stop()`. The spawn site calls `owner.scaler.start(...)`. Same spawned loop and weak-state wiring as before.
- `matching_scaler_go_tests.rs`: the ignored gap is replaced by `matching_adaptive_scaler_lifecycle_start_stop_is_idempotent`. It asserts one loop per start, idempotent stop, and no restart after stop.

**Difference kept on purpose:** Go's `Stop` waits for the loop to exit (`wg.Wait`). Rust aborts the task and does not wait. Nothing in the scaler awaits the loop's exit, so the abort is enough for the lifecycle. Recorded as an accepted difference.

**Not done:**
- No scaler gate lane was added. The Go `enableAdaptiveScaler` gate lane is still not located (P1-4 "gate lane" half). Stays open or document as not exercised.

## P1-6: liveness ticker unload window

**Status before:** STILL OPEN. The existing `liveness_event_loop_*` tests call `sweep_idle` directly, not the timer.

**Go reference:** `go/service/matching/liveness/liveness.go:73` (ticker at `ttl/2`), `:93-95` (fire shutdown when inactive), `:108` (`alive = now-lastUpdate < ttl`). So the unload happens at the first tick where `now - last >= ttl`. That is the second TTL/2 tick, at TTL after the last mark.

**Change (test only, no production code change):** `liveness_idle_pump_unloads_owner_between_ttl_and_one_and_a_half_ttl` in `matching_registry.rs`. It creates an owner with `TTL = 500ms` and runs the production `maintain_until` pump on real time, with the idle check interval from `with_idle_tasklist_check_interval`. It then asserts:
- unload is observed no earlier than TTL after the `get` call,
- and no later than 1.5·TTL (750ms),
- and the owner is removed from the registry map.

The `started` instant is taken before `get`, and the liveness clock starts at `registry_now()` inside `get`. So the lower bound holds, and the upper bound allows 250ms of scheduling slack.

**Not done:** the test is real-time, so under heavy load it could be flaky at the upper bound. If CI shows flakes, move it to a paused-time form (this needs the liveness clock to follow tokio time, which it does not today).

## Verification

- CI run: see below (filled after the run).

## CI attempt log

- Run `40031735890` on `aadd8f7e7` failed in clippy (`clippy::manual_async_fn` at `matching_scaler_go_tests.rs:1066`, the test helper `periodic`). Test job was skipped. Fixed by making the helper a plain `async fn`. No other diagnostics were reported.
