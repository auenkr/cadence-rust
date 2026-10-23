# Matching liveness idle-pump test: paused-time conversion (2026-10-24)

Branch: `matching-liveness-paused-20261024` (from `origin/matching-p1-4-6-20261024` @ 5bdca97a3)
Test: `liveness_idle_pump_unloads_owner_between_ttl_and_one_and_a_half_ttl`
(`rust/crates/cadence-matching/src/matching_registry.rs`, ~line 3099)

## Outcome: NOT converted to paused time (blocked by production time source)

The task asked to convert the test to `#[tokio::test(start_paused = true)]` only if the
production pump can be driven by tokio paused time. It cannot, so per the task instructions
production code and the test are unchanged.

### Finding

The `maintain_until` pump splits its time sources in two:

- Scheduling uses `tokio::time::Instant` / `tokio::time::sleep_until(deadline)`
  (`matching_registry.rs` ~2059-2110). Paused time *does* drive this.
- Liveness decisions use wall-clock nanos from `registry_now()`, which is
  `std::time::SystemTime::now()` (`matching_registry.rs:17-22`). Every call site feeds it:
  - the owner's `Liveness::new(registry_now(), ttl)` at creation (~line 1125),
  - the test's own `registry.get(key, 0, registry_now())` (~line 3105),
  - the pump's `sweep_idle(registry_now(), true)` and `flush(registry_now(), ...)` (~2103-2104).
- `Liveness::is_alive(now)` (`rust/crates/cadence-common/src/liveness.rs:17-19`) is a pure
  comparison of `now - last_event < ttl`. It never reads a clock itself.

Consequence under `start_paused = true`: tokio auto-advances virtual time while the runtime is
idle, so the pump wakes at virtual TTL/2 steps, but `registry_now()` barely moves in real time.
`is_alive(wall_now)` stays true, the owner is never unloaded, and the test's loop panics with
"owner still loaded after 3 TTL" after about 1.5 virtual seconds. Paused time therefore makes
the test fail deterministically rather than fixing the flake.

### Why not widened or patched

- Making the test deterministic would need an injectable clock for `registry_now()` (or
  `Liveness`), which is a production change. The task forbids that.
- The `sweep_idle` unit tests (lines ~3050-3185) already cover the same TTL semantics with
  explicit nanos and no real time, so the window logic is covered deterministically elsewhere.
  Only the end-to-end "real pump timing" assertion is real-time dependent.

### Options for a follow-up (not done here)

1. Inject a clock into the registry (`fn(&self) -> i64` defaulting to `registry_now`) and use it
   for `Liveness` and `sweep_idle`, so the pump test can advance both clocks together under
   paused time. Production logic is unchanged, but this is a production-code change and needs
   sign-off.
2. Keep the real-time test and relax the timing band on loaded CI runners. This weakens the
   assertion the task asked to keep.

## Verification

- No cargo was run locally (per instruction). Verification is the GitHub Actions run below.
- Worktree setup: `git worktree add .../wt/livepause -b matching-liveness-paused-20261024 origin/matching-p1-4-6-20261024`;
  `git submodule update --init go/idls` succeeded (go/idls @ bfbf273b4).
- Tokio dev-dependency already has `test-util` (`rust/crates/cadence-matching/Cargo.toml`:
  `tokio = { version = "1", features = ["full", "test-util"] }`), so no Cargo.toml change is needed
  for the paused-time option once the clock blocker is removed.
- CI run (baseline, unchanged test and code, head efabe9ce8):
  https://github.com/auenkr/cadence-rust/actions/runs/40032742709 — conclusion `success`
  (`rust-ci.yml`, `package=cadence-matching`, job `clippy-test`).
  Log shows `matching_registry::tests::liveness_idle_pump_unloads_owner_between_ttl_and_one_and_a_half_ttl ... ok`
  and `567 passed; 0 failed; 9 ignored` for the crate unit tests.
  This only shows the real-time test passes on this runner today; it does not show it is
  flake-free, which is the original concern.
