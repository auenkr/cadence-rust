# Matching CI evidence checkpoint (2026-10-30)

## Frozen source and run state

Source under review is `1c1c6e94d0d7384f89f09920a1452e58643b02e` on `origin/t3/matching-failure-coverage-20261030142037` (remote docs head `3ad26eb6b64ecb7a96456efcb5c6f7dd9e361f6d`). Do not change Rust/Go source while validating it.

Successful previous workspace run: [41028655670](https://github.com/auenkr/cadence-rust/actions/runs/41028655670), workflow `rust-ci`, branch `t3/matching-domain-cache-ci-fix`, exact head `22c695d65af367a0b49b605877f72ec33fa2beb8`, conclusion `success`. Its `rust-ci-logs` artifact was downloaded and inspected. `commit.txt` repeats that SHA; `target-dir.txt` is `/home/runner/work/_temp/cargo-target-final`. Workflow step 30 selected this dedicated target and the shared-cache step was skipped. `clippy.exit` and `test.exit` both contain `0`; clippy log ends successfully. Workflow source at that SHA maps empty `package` to `--workspace` for both clippy and tests, and defaults package to empty. The logs compile/check all workspace crates and enumerate all workspace test executables. Thus this is verified fresh-target full-workspace evidence for 22c, not evidence for frozen source 1c.

The downloaded Rust test log contains 57 test harness result lines: 1,827 passed, 0 failed, 30 ignored, 0 measured, 0 filtered. This is the complete workspace test total from the run; it is not a Matching-service parity completion claim. The evidence artifacts were small (`rust-ci-logs` 178,129 bytes plus four small Go/metrics artifacts).

## Ignored query/refresh case reconciliation

Reviewing the source diff from 22c to frozen 1c confirms the two old ignored stubs were removed from `matching_engine_go_tests_b.rs` (`query_workflow_failed_to_dispatch_query_task_is_error`) and `matching_engine_go_tests_c.rs` (`refresh_task_list_partition_config_manager_error_is_propagated`). Their cases are now exercised by tests in `matching_partition_cancellation_tests.rs`:

- `query_workflow_dispatch_failure_returns_error_and_cleans_pending_query` injects an error at the query-dispatch boundary after waiter registration, verifies the RPC error, checks the waiter has been removed and no query remains offered, then shuts down the registry.
- `refresh_rpc_propagates_engine_error_and_recovers_after_fault_clears` injects the refresh failure, checks the public RPC error and absence of partial durable state, clears the test fault, and verifies the same refresh succeeds.

The query seam is `#[cfg(test)]`; refresh uses the existing `#[cfg(test)] engine_fault` hook. This source review reconciles the old ignores with the replacements but does not constitute a CI pass at 1c. Other 30 ignored outcomes in run 410286 remain unrelated suite ignores (mixed-service/database opt-ins and other explicitly documented parity gaps).

## Final workspace run and mixed gate

Run [41029943649](https://github.com/auenkr/cadence-rust/actions/runs/41029943649) was dispatched on `t3/matching-failure-coverage-20261030142037` at exact head `1c1c6e94d0d7384f89f09920a1452e58643b02e`. At checkpoint, the single status inspection reported `in_progress`; no final artifacts or conclusion are available yet. Do not infer a pass from dispatch or started steps.

Next action after this checkpoint: inspect run 41029943649 once. If terminal-successful, inspect its fresh-target workspace artifacts and exact head, confirm there is no active mixed gate, then dispatch only the PostgreSQL lane of `matching-production-validation.yml` pinned to exact source 1c. Record the Rust binary SHA256 plus nested SDK/readiness/restart/recovery outcomes from gate artifacts before drawing any conclusion. If run 41029943649 is still in progress, checkpoint and stop without polling or starting a gate. If it fails, diagnose with its logs and propose the smallest necessary fix; do not modify source absent a proven CI defect.
