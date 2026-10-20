# Matching shard-distributor candidate gate evidence

Source revision: `cd68692cee64a35f8f53ad4c7e932efa9252af80`, rebased on accepted root `160fcc6f3afaa8db0692faf69d3283788456bf83`.

The final serialized run used `tests/run-shard-distributor-review-gates.sh` in tmux session `cadence-rust`, window `matching-shard-distributor-review`, with `CARGO_BUILD_JOBS=1` and `CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target`. `gates.exit` is `0`. It ran library test compilation with `--no-run`, the focused Rust shard-distributor tests (7 passed), config tests (12 passed), strict `cadence-server` Clippy, and pinned Go Matching resolver tests (all passed).

`first-attempt.*` records the initial unit-test failure caused by checking an asynchronous processor stop before it completed (exit 101). The pinned Go executor confirms assignment updates remove the processor synchronously but invoke Stop asynchronously. The candidate test now awaits an explicit Notify sent by completion of the fake stop future. `second-attempt.*` records the successful Rust test gates followed by strict Clippy failures (exit 101); the final source fixes three equivalent style lints, with no behavior changes.

The Rust suite covers fake-Tonic spectator snapshot/fallback/shutdown behavior and executor heartbeat/assignment/drain behavior. The Go suite covers the existing resolver. This is standalone component evidence only; it does not establish actual Rust executor/spectator compatibility with a running Go shard-manager service, Matching runtime integration, mixed cluster behavior, failure/restart/recovery, or Matching service parity.

No credentials, TLS private keys, databases, or runtime data are included. `source.sha256.before` and `source.sha256.after` include the candidate source files, `Cargo.lock`, gate script, Go resolver oracle source/tests, pinned Go executor source, and the warmed `cadence-server` binary.
