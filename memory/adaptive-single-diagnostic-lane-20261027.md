# Adaptive single-Matching diagnostic lane (2026-10-27)

The `adaptive-single-diagnostic` option in `.github/workflows/matching-production-validation.yml` runs the adaptive scaler SDK/load/routing probe with `--matching-restart` and `--matching-idle-check-seconds 3`, but intentionally omits `--matching-failure-recovery`. The existing `adaptive` acceptance lane is unchanged and still enables two-instance failure recovery.

Topology was checked in `tests/matching-service-compat.cjs`: `failureRecoveryMode` is enabled only by `CADENCE_MATCHING_FAILURE_RECOVERY=1`; `startSecondaryRust()` is reached only inside `if (productionMode && failureRecoveryMode)`. Therefore this lane starts one Rust Matching process at a time. Its restart scenario stops and starts that same primary process. The mixed lane continues to use Go Frontend, History, and Worker with no Go Matching.

The workflow writes a `diagnostic-only` lane description to `out/lane.txt`, includes the full executable wrapper invocation in `out/command.txt`, and names the artifact `matching-production-adaptive-single-diagnostic`.

Exact parent dispatch command after this commit is pushed:

```sh
gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref t3/matching-single-lane -f lane=adaptive-single-diagnostic -f adaptive_retry_only=false
```

No validation gate was dispatched or polled by this change; the parent owns serial mixed-runtime gates. `git diff --check` passed. Cargo, Go, build, test, formatting, and runtime commands were not run locally.
