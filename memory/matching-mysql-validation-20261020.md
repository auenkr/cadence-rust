# Matching MySQL adapter validation (2026-10-20)

Source branch: `codex/matching-task-store-abstraction`  
Validated commit: `6d0f77b05 Release MySQL timed query slots before completion`

The candidate passed strict workspace Clippy and live Go → Rust → Go interoperability against a disposable MySQL 8.0.36 instance. The runner uses Go's real v8 schema and SQL plugin, Go persistence to seed domain/task rows, Rust adapter tests to read and mutate them, and Go persistence to verify the resulting rows.

Reproduce from the repository root:

```sh
CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target \
CARGO_BUILD_JOBS=1 cargo clippy --locked --workspace --all-targets -- -D warnings

CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target \
CARGO_BUILD_JOBS=1 python3 tests/run-matching-mysql-interop.py
```

Both commands were run in tmux session `cadence-rust`. The second command owns and removes its uniquely named MySQL container in a `finally` block.

Results:

- Strict workspace Clippy: passed with warnings denied.
- MySQL timeout integration tests: 7 passed, including connect failure, handshake cancellation, slow result drain, stalled socket reads, pool slot release, and per-operation read timeout behavior.
- MySQL TLS integration test: 1 passed.
- Go MySQL persistence seed phase: passed.
- Rust MySQL adapter interop test: 1 passed.
- Go MySQL persistence verification phase: passed.
- Container cleanup: confirmed; no `cadence-matching-mysql-*` container remained.

Detailed logs: `memory/evidence/matching-mysql-validation-20261020/attempt-14/`.

Earlier attempts exposed and corrected two test harness issues: successful timed queries signaled before dropping their reserved pool slot, and the injected Go test did not blank-import the real MySQL SQL plugin. The final candidate explicitly releases the slot before reporting success and verifies that a second query succeeds with a one-connection limit.
