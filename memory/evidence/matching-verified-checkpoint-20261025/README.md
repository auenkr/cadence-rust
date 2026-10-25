# Verified Matching validation checkpoint

These are inspected artifacts, not a Matching completion sign-off.

- `workspace.json`: fresh uncached whole-workspace Clippy then tests passed in
  run [40217841072](https://github.com/auenkr/cadence-rust/actions/runs/40217841072)
  on `6af476941031193317fdd6c44070e9785e6384b9`. Both exits are 0; target path
  is `/home/runner/work/_temp/cargo-target-final`. Separate fmt exit is 0;
  query/TLS/workspace format exits are 1. Do not claim those checks passed.
- `shard-distributor.json`: mixed Go Frontend/History/Worker with Rust Matching
  passed in run [40218022772](https://github.com/auenkr/cadence-rust/actions/runs/40218022772)
  on `a5a8e2a14638efcc2d7f6cc5bd6ec027271978dd`. Build/gate exits are 0.
  The captured primary port 24141 uniquely identifies the killed executor.
  Go's real leader removed it and released assignments after 5128ms; the probe
  then observed a replacement heartbeat, reassigned ownership, and completed
  the persisted workflow exactly once. Shard-manager restart and SIGTERM drain
  recovery also passed. The probe retains `preliminary-passed` and
  `acceptanceEligible:false`; this is scenario evidence, not full sign-off.

The source difference between these pins is the shard test selection/diagnostic
fix and documentation; Rust implementation is unchanged. Record the pins as
tested, rather than calling them one exact-SHA final gate. Subsequent production
changes still require fresh workspace and serial mixed validation. Remaining
raw SQL RunID fatal-error parity is under executable Go oracle investigation.
