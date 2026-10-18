# Matching test artifact selection fix — 2026-10-18

The mixed Matching runner previously let `matching-service-compat.cjs` scan
`target/debug/deps` and choose the first executable named `matching_delivery-*`.
That could select a stale test binary when a target directory held multiple
hashes. The runner now requests Cargo JSON messages for its existing
`cargo test --no-run` prebuild, selects the sole `compiler-artifact` whose
target is the `matching_delivery` test, checks that Cargo-reported path exists
and is executable, then passes that exact path in
`CADENCE_MATCHING_DELIVERY_TEST_EXECUTABLE`. The CJS probe requires and checks
that explicit path; it no longer guesses from the directory. Zero or multiple
Cargo matches fail with a message pointing to `rust-build.log`.

## Verification

Ran in tmux session `cadence-rust` with the existing Go mixed server binary,
existing Node dependency tree, and existing `t3-ce1e72cb/rust/target`; no Go
Matching service was started. `CARGO_BUILD_JOBS=1` was set. A temporary
executable dummy named `matching_delivery-0000000000000000` that exits 99 was
placed beside the real Cargo binary for the duration of the run. The CJS probe
launched Cargo's reported
`matching_delivery-c8468cdca4f9fb2f`, and the mixed process-restart test
passed. The dummy was removed by the tmux command's exit trap.

Evidence is retained outside Git at
`~/.cache/cadence-rust-test-artifacts/matching-artifact-selection-fix/`:

* `restart-pass.exit` is `0` and `restart-pass/results.json` reports a passing
  mixed Go Frontend/History/Worker with Rust Matching run.
* `restart-pass/rust-build.log` contains the JSON compiler artifact and
  confirms the selected executable path.
* `restart-pass/probe.log` reports a process restart pass: one pending task row
  existed before SIGKILL, the second Rust Matching process reopened the same
  SQLite queue and listener, the existing Go SDK completed the workflow and
  activity, and History exposed exactly one completion. The Go services
  remained alive after the probe.
* `restart-pass/results.json` records the Go server alive after probe and
  `goMatchingStarted: false`.

Python bytecode compilation, `node --check`, and `git diff --check` passed.
This is harness artifact-selection evidence and one mixed restart regression;
it does not complete Matching's parity audit or establish whole-service
replacement. No production code changed.
