# Admin DescribeWorkflowExecution History proxy CI evidence — 2026-11-02

Branch: `t3/admin-history-proxy-ci`

Final pushed HEAD: `4a46f97dd4f72732a27c67a52cfb828a5344597c`

## Changes

- Rebased the admin History proxy change onto current `origin/master` (`fbb2c1084`).
- Applied rustfmt's required split method-chain layout in `cadence-service-core/src/admin.rs`.
- Added an e2e endpoint regression asserting `/api/v1/admin/workflows/describe` rejects a request without an execution.
- Existing e2e coverage continues to verify successful local embedded describe fallback.

## CI

- Package-filtered run (`package=cadence-frontend`): https://github.com/auenkr/cadence-rust/actions/runs/41499024728 — head `4a46f97dd4f72732a27c67a52cfb828a5344597c`, conclusion `success`.
- Full run: https://github.com/auenkr/cadence-rust/actions/runs/41499945985 — head `4a46f97dd4f72732a27c67a52cfb828a5344597c`, conclusion `success`.

Both required CI gates passed on the final pushed commit. An earlier filtered run failed because the workflow formatter auto-commit restored the unformatted one-line chain; the formatting was corrected, pushed, and both gates passed afterward. No local Cargo or rustfmt commands were run.
