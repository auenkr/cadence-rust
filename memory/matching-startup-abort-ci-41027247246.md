# Startup-abort candidate CI checkpoint — 2026-10-30

Candidate: `t3/matching-startup-abort-regression` at
`17ca0308e22973bfc9216e362a679596189907be`.

## Rust CI result

Fresh uncached workspace run:
<https://github.com/auenkr/cadence-rust/actions/runs/41027247246>

- Workflow: `rust-ci`; dispatched with `package` unset and `clean_target=true`.
- Actions head branch/SHA: `t3/matching-startup-abort-regression` /
  `17ca0308e22973bfc9216e362a679596189907be`.
- Target: `/home/runner/work/_temp/cargo-target-final` (fresh per-run target;
  Cargo cache step skipped).
- Clippy: success (`cargo clippy --workspace --all-targets -- -D warnings`).
- Format checks: success.
- Workspace tests: failure; 983 passed, 2 failed, 2 ignored.
- `matching_launch_native::tests::configured_grpc_semaphore_slow_start_deadlines_cleanup_and_retry`
  failed because its initial RPC returned
  `DefaultDomainCache encounter case where domain exists but cannot be loaded`.
- `matching_runtime_native::tests::manager_config_binder_keeps_constructed_domain_name_and_live_values`
  failed with the same cache-load error.

Both failing test files are unchanged from the candidate's parent commit. The
candidate diff is limited to the semaphore parity tests and candidate memory.
The CI log does not identify a source-level cause or prove these failures are
caused by the candidate, so no candidate code was changed and no rerun was
dispatched on this evidence alone.

## Mixed gate disposition

The PostgreSQL mixed gate was not dispatched. The reviewed acceptance plan
requires a successful fresh uncached Rust workspace test run before dispatch;
this run did not pass. No claim is made about SDK compatibility, Go Matching
absence, restart/recovery, or a Rust binary hash for this candidate.

Next action: investigate the two test failures or establish that they are
transient with a separately identified fresh CI run on this exact SHA. Only
after a successful workspace proof, dispatch
`matching-production-validation.yml` with `lane=postgres`, then verify the
Actions head SHA, artifact `out/commit.txt`, all required zero exit files,
SDK workflow/restart/recovery evidence, Go/Rust topology, binary SHA256, and
database cleanup as specified in
`memory/matching-startup-abort-review-20261030.md` on the review branch.
