# Approved Matching TLS integration candidate — 2026-10-25

Approved code commit: `e3c79b3014e80e98f1c1d0a18604588d5c068929`  
Merge commit: `357e36567032f4da2a929d86614e12b5cf7a43e6`  
Source reviewer: independently approved, per parent confirmation.  
Matching remains incomplete; this integration does not constitute service
sign-off.

## Fresh workspace evidence

Run: https://github.com/auenkr/cadence-rust/actions/runs/40164484146  
Exact tested source: `e3c79b3014e80e98f1c1d0a18604588d5c068929`  
Conclusion: success; fresh target `/home/runner/work/_temp/cargo-target-final`,
cache step skipped.

Artifact exit markers confirm strict workspace Clippy before workspace tests;
both exited 0. The TLS-touched-file rustfmt check exited 0, and its uploaded
patch is empty (no formatting diff). Workspace-wide formatting advisory exited
1 and remains a limitation. The separate `fmt.exit=0` is another configured
formatting step; it does not override the workspace advisory. Compact identities,
markers, TLS regression-test output and format patch are in this directory.
Full workflow ZIPs are on disk at `/mnt/storage/service/ci-evidence/run-40164484146/`.

The exact commit was tested before merge; the merge contains the same Rust
source, with the approved TLS workflow and tests preserved. The metrics
artifact's SHA is for the metrics oracle executable, not a Matching binary.

## Candidate and first serial lane

The stable branch `t3/matching-validation-tls-e3c79b3-20261025` is pinned to
the documented integrated candidate commit. Its exact full SHA is the branch
tip at creation and must match the first mixed workflow's `headSha`. The sole
first gate is `matching-http-tls-interop.yml`, which uses `ubuntu-latest`, a
90-minute job timeout, three-day artifact retention, and cancel-in-progress
concurrency. Do not dispatch a second lane until this run completes and its
artifacts are reviewed. Fresh equivalent workspace evidence above means no
manual workspace dispatch is needed. A push to master may trigger automatic
`rust-ci`; record it if present rather than dispatching a duplicate.
