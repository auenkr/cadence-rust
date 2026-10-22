# Verify: t3/matching-integration-20261021 (base head f17c25d95)

Date: 2026-10-22/23. All cargo runs wrapped in `flock /tmp/cargo.lock`, `-j2`, from `rust/`.

| Command | Result |
|---|---|
| `cargo test -j2 --workspace` (on f17c25d, before the fix below) | Ran to completion incl. all doc-tests; 0 FAILED/panicked in log. Exit code not captured (tmux window was reaped); completion inferred from final doc-test output. |
| `cargo clippy -j2 --workspace --all-targets -- -D warnings` (with the fix below) | `Finished`, no warnings/errors. Exit code not captured (wrapper orphaned); success inferred from `Finished` and no diagnostics. |
| `cargo test -j2 -p cadence-persistence` (with the fix below) | EXIT=0, 240 passed, 0 failed, 12 ignored |

## Fix
`rust/crates/cadence-persistence/src/matching_domain_metadata.rs`: removed redundant `drop(put);` (no behavior change).

## Caveat
Workspace test was not re-run after the one-line fix; only cadence-persistence tests were.
