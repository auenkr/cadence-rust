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

The stable branch `t3/matching-validation-tls-e3c79b3-20261025` is pinned at
`4b5d90458471c36ab8064bb69c695854ffc23de5`, including the workflow parser
correction. The first dispatch attempt was rejected before creating a run
(HTTP 422: `runner.temp` is unavailable in job-level `env`). The workflow now
selects its fresh target in a step using `$RUNNER_TEMP` and `$GITHUB_ENV`; YAML
parsing and `git diff --check` passed. The sole first gate is
`matching-http-tls-interop.yml`, which uses `ubuntu-latest`, a 90-minute job
timeout, three-day artifact retention, and cancel-in-progress concurrency. Do
not dispatch a second lane until this run completes and its artifacts are
reviewed. Fresh equivalent workspace evidence above means no manual workspace
dispatch is needed. Master push started automatic workspace run
40165304531; record its result without dispatching a duplicate workspace run.

First lane run: https://github.com/auenkr/cadence-rust/actions/runs/40165433074
failed during the build on exact branch head
`4b5d90458471c36ab8064bb69c695854ffc23de5`; Cargo required updating
`rust/Cargo.lock` while `--locked` was set, exit 101. The interop step was
skipped, so this is not a TLS compatibility failure. See
`memory/evidence/matching-http-tls-interop-failure-40165433074/README.md`.

Workflow correction removes `--locked` and saves any generated lockfile diff
in the gate artifact without changing TLS source or assertions. The retry uses
immutable ref `t3/matching-validation-tls-e3c79b3-r1-20261025` at
`e787524851b06f0f9aab007ae645c5d9cf8c85e5`. Retry run
https://github.com/auenkr/cadence-rust/actions/runs/40165758848 is
`in_progress` on that exact head. Do not dispatch the next lane until the retry
completes and its artifacts are reviewed.

Before retry dispatch, the active-run query was empty; automatic master
workspace run 40165304531 had completed successfully on
`389c36527908c20d461576d0868a22ece2519f69`. No duplicate workspace run or
second mixed lane was started.

Independent review approved branch `t3/http-tls-lockfix-20261025` at
`204c2414f3707577f5bce921c82f8fcc72b056c7`; it is merged as
`1e537b135ad6919cfcb44ad09fbc7fe57f4957fd`. The change applies the CI lockfile
delta, restores `--locked`, records the lock diff against HEAD, and makes the
unsupported NoSQL negative startup probe deterministic. It changes no TLS
production code or TLS assertions. The retry uses stable candidate branch
`t3/matching-validation-http-lockfix-20261025`; its exact SHA and dispatch
status are in the final validation plan.
