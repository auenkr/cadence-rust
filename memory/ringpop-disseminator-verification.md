# Ringpop dissemination verification

Applied the candidate `6c71b6774e55f439bc3647387fb544694277226d` and repair
`187f4aeff2564eb9a6f7029842d054c5980ce1c7` in that order to the assigned
verification worktree. The repair commits sender-address counter bumps and
local-member source fields for receiver full sync. Strict Clippy found one
candidate warning: `issue_receiver` takes eight protocol/local-state inputs.
Added a narrow `#[allow(clippy::too_many_arguments)]` on that method; no logic
or signature changed.

## Confirmed checks (2026-10-18)

All checks used `CARGO_BUILD_JOBS=1` and the worktree's isolated
`rust/target` directory, run in a unique `cadence-rust` tmux window.

- `cargo test -p cadence-common --test ringpop_disseminator -- --nocapture`:
  exit 0, 2 passed.
- `cargo clippy -p cadence-common --all-targets -- -D warnings`: exit 0.
- `cargo test -p cadence-common --test ringpop_member_oracle -- --nocapture`:
  exit 0, 1 passed.
- `python3 tests/run-ringpop-wire-compat.py`: exit 0; Go wire fixture oracle
  passed and 11 Rust wire tests passed.

The earlier run used an overly narrow cargo test name filter and selected zero
tests; it is superseded by the explicit integration target run above. Its first
strict Clippy attempt found the argument-count warning, which was fixed and
then rerun successfully.

## Source review and remaining question

Pinned upstream source is
`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/swim/disseminator.go`.
It sets `log10 = math.Log(10)` and computes
`pFactor * int(math.Ceil(math.Log(float64(numPingable+1))/log10))`. Rust uses
`(n as f64).log10().ceil()` after saturating `numPingable + 1`. Both express
ceiling log base ten, but their floating point evaluation paths can round
slightly differently near exact powers of ten. This focused run did not
establish exhaustive Go/Rust boundary equivalence, so do not claim it.

Upstream `swim/member.go` defines a `Change` with source/source-incarnation and
subject address/incarnation/status/labels fields. `populateSubject` copies the
member's address, incarnation, status, and labels; `populateSource` copies the
local member's address and incarnation. Full-sync changes should therefore use
the local node as source and each synchronized member as subject.

The focused checks do not establish full dissemination differential parity.
The separate Go oracle fixture work remains pending; no fixture was generated
or parity claim made in this task.

## Combined final validation, standard-21 (2026-10-18)

Validated the requested five-commit stack in combined tree
`0655292c3df79528c4781cf8ecf155539029a3e8` in the assigned isolated worktree.
This supersedes the pending statements above for this stack. All commands ran
in tmux session `cadence-rust`, window `standard-21`, with
`CARGO_BUILD_JOBS=1`. Disk-backed command logs and exit markers are under
`/tmp/cadence-standard-21-0655292/`.

- Original pinned Go generation: `python3
  tests/generate-ringpop-member-primitives.py` passed twice. The generated
  member primitive and dissemination fixtures each matched the checked-in
  files after both runs (`member-cmp1`, `disseminator-cmp1`, `member-cmp2`,
  `disseminator-cmp2` all exit 0), so there is no fixture drift.
- `python3 tests/run-ringpop-wire-compat.py`: exit 0; Go wire oracle passed
  and all 11 Rust wire tests passed.
- `cargo test --locked --manifest-path rust/Cargo.toml -p cadence-common
  --test ringpop_disseminator -- --nocapture`: exit 0, 2 passed.
- `cargo test --locked --manifest-path rust/Cargo.toml -p cadence-common
  --test ringpop_disseminator_oracle -- --nocapture`: exit 0, 1 passed.
  This consumer checks actual-Go vectors for factor-derived maxP at pingable
  counts 0, 1, 8, 9, 10, 98, 99, 100, and 999, plus factors 0 and 1 at one
  pingable member, along with queue/callback/filter/full-sync behavior.
- `cargo test --locked --manifest-path rust/Cargo.toml -p cadence-common
  --test ringpop_member_oracle -- --nocapture`: exit 0, 1 passed.
- `cargo clippy --locked --manifest-path rust/Cargo.toml -p cadence-common
  --all-targets -- -D warnings`: exit 0.

This confirms bounded deterministic dissemination parity for the checked
fixtures and listed maxP boundary vectors. It is not evidence that production
SWIM is implemented or wired into the server, nor does it claim exhaustive
floating-point equivalence for every possible pingable count.
