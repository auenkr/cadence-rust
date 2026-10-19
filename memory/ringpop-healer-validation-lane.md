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
healer fixture, blocked-JOIN shutdown, live partition convergence, reverse
full-sync convergence, shared-TChannel provider, memberlist regression,
discovery regression, and strict `cadence-common`/`cadence-server` Clippy
commands. It captures separate
logs and per-command exit codes, plus candidate diff hashes before and after.
Its output defaults to a timestamped directory under
`memory/evidence/ringpop-healer-validation-*`.

`matching_ringpop_provider::tests::stop_waits_for_active_healer_join_response`
uses a test-only Notify trigger to start the provider's real healer pass
deterministically. A real TChannel listener acknowledges receipt of JOIN, then
holds its response on a Notify barrier while the test polls provider shutdown
once and proves it remains pending. Releasing the response must let stop finish
within a bound; the test then destroys/joins both peer listeners. This avoids
sleep-based race assertions and makes Rust behavior observable independently
of the Go fixture. The test and runner have not been executed.

Initial frozen runner attempt 1 ran in `cadence-rust` at
`memory/evidence/ringpop-healer-validation-20261019T215112Z`. The Go oracle
comparison and both selected `cadence-common` memberlist tests passed. All
`cadence-server` test targets and Clippy failed to compile because three
`ringpop_provider.rs` call sites referred to an undefined `wall_clock_millis`
(lines 1222, 1232, and 2183 at that revision). The runner repeated the same
compile error for multiple filters; the captured logs are retained as the
initial failure record. A module-local helper now supplies Unix wall-clock
milliseconds and is also used by the normal successful JOIN path, matching Go's
`UnixMilli` timestamp intent. The serialized runner now has a single
`cadence-server --no-run` compile gate; dependent test filters and combined
Clippy are recorded as skipped if that gate fails, while independent common
Clippy/memberlist checks still execute.

Attempt 1 was not validation success. The corrected source and runner need a
fresh full lane run; no tests of the corrected source have been executed yet.
