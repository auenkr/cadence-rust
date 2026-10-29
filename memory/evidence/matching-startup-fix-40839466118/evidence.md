# Diagnostic Matching startup CI evidence

- Run: https://github.com/auenkr/cadence-rust/actions/runs/40839466118
- Workflow: `rust-ci`; branch: `t3/matching-startup-fix`
- Exact head: `12c3063ea91e0dc5abd552a79140a14690edb8bd`
- Status/conclusion: `completed` / `failure`
- Job: `clippy-test`, conclusion `failure` (job `123332063004`)
- Rust artifact `commit.txt`: `12c3063ea91e0dc5abd552a79140a14690edb8bd`
- `matching-runtime-format.exit`: `1`
- `workspace-format.exit`: `1`
- `query-format.exit`: `0`
- `tls-format.exit`: `0`
- Failed step: `Check changed Matching runtime formatting`; log shows a rustfmt-only change at `crates/cadence-matching/src/matching_runtime.rs:925`, wrapping the startup `eprintln!` across lines.
- Clippy, Formatting, and Test steps were skipped after the formatting failure. This run is not source-validation evidence and did not establish the prior runtime startup cause.
- Downloaded original artifacts: `/mnt/storage/service/ci-evidence/matching-startup-fix-40839466118/`
- Decision: no workspace retry dispatched because this selected run failed; no PostgreSQL dispatch.
- Frozen ref preserved: `origin/matching-native-final-20261029` = `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`.
- Pending commits `overlay9d3a342d5` and `PG9498ee5e1` await separate review, remain untested here, and have no proven causal relationship to the prior startup failure.
