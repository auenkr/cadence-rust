# Ringpop state transition scheduler

This slice adds `cadence_common::ringpop_state_transitions`, a deterministic
core matching `swim/state_transitions.go` in the pinned Ringpop v0.10.0 source.
Its caller supplies a monotonic nanosecond timestamp and calls `advance`; it
returns actions containing the captured address and incarnation. Defaults are
5 seconds suspect, 24 hours faulty, and 1 minute tombstone. A configured zero
selects the default like `util.SelectDuration`; negative durations are retained
and become due at the current timestamp.

The scheduler ignores disabled/local scheduling, ignores a repeated state for
an address while retaining the original captured subject, replaces a different
state timer, cancels/removes entries, clears entries on disable, and makes
enable/disable idempotent. Fired entries remain in the map, as Go's map does;
they fire once. Replacement gets a new generation so delayed consumers can
reject stale actions.

Concurrency boundary: Go's `Timer.Stop` cannot retract a callback that is
already executing. This core serializes `schedule`, `cancel`, `disable`, and
`advance` through the caller and does not model an already-running callback
racing a cancellation. A runtime adapter may check `is_current(address,
generation)` to discard an obsolete timer wakeup before it calls `advance`.
Once `advance` emits an action, the adapter must apply it atomically while it
owns the scheduler lock; cancel or disable cannot retroactively discard that
fired action, matching Go's already-running callback boundary. No autonomous
clock, task, or production wiring is included.

Validation passed on 2026-10-19:

- `python3 tests/run-ringpop-state-transitions-oracle.py` — the test-only
  overlay ran against an untouched copy of pinned Ringpop v0.10.0; Go produced
  the trace and the focused Rust test compared its own output to that JSON.
- The runner's `cargo test --locked -p cadence-common
  ringpop_state_transitions --lib` passed all 3 focused tests.
- `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-common --all-targets --
  -D warnings` passed.
- `git diff --check` passed.

The clean end-to-end log is `/tmp/ringpop-state-transitions-success.log` (exit
code 0). Earlier runner attempts exposed and fixed a test variable shadow, a
Go/Rust deadline window mismatch, and a shell working-directory mistake; those
failed logs remain under `/tmp/ringpop-state-transitions-*.log` for diagnosis.

The Go oracle also exercises two due timers in one mock-clock advance: one
suspect timer due at 5 seconds and one faulty timer due at 10 seconds. The Go
callback for the first timer changes membership and schedules the next
faulty-to-tombstone timer for 15 seconds; the 10-second trace intentionally
stops before that callback. The Rust core emits the first two ordered actions.
Scheduling the cascaded timer after applying a membership action belongs to a
future Node/runtime adapter, so this core does not claim full state lifecycle
parity.
