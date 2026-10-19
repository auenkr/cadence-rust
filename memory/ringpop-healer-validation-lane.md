# Matching Ringpop healer validation lane

Prepared on 2026-10-19 after rebasing the healer/audit chain onto accepted
root `8b414dd19` (rebased tip `b11c4ae36`). The pre-rebase candidate remains
available as `archive/swim-healer-before-root-rebase-0d1debef8`. Rebase replayed
the seven commits after `b1a7f52e6`; it made no implementation edits beyond
the healer chain itself.

The frozen serialized runner is
`tests/run-ringpop-healer-validation.sh`. It requires the existing `cadence-rust`
tmux session, a clean committed tree, and the sole warmed target directory
`rust/target`; it exports `CARGO_BUILD_JOBS=1` and that target path. It performs
the pinned Go oracle in comparison-only mode, followed sequentially by Rust
healer fixture, live partition convergence, reverse full-sync convergence,
shared-TChannel provider, memberlist regression, discovery regression, and
strict `cadence-common`/`cadence-server` Clippy commands. It captures separate
logs and per-command exit codes, plus candidate diff hashes before and after.
Its output defaults to a timestamped directory under
`memory/evidence/ringpop-healer-validation-*`.

The runner has not been started. `bash -n` and `git diff --check` passed while
preparing it; no Go/Rust build, test, or Clippy command was run.

Coverage gap surfaced while assembling the requested test list: Rust has no
blocked-JOIN/healer-stop regression. The Go oracle harness exercises
`stopWaitsForActiveHeal`, but the Rust `go_healer_expected_trace` test only
asserts that fixture field and does not prove Rust shutdown semantics. The
runner records this as missing coverage and returns failure instead of
misreporting a green lane. Add the Rust behavioral test before treating the
requested combined lane as complete.
