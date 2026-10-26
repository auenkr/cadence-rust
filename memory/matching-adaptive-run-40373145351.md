# Matching adaptive production lane fixture diagnosis — 2026-10-26

## Run and evidence

- Failed Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40373145351
- Run head: `eecb6bef8b5b5c6c36e3e9c7336fa01f451a3f97` (`t3/matching-pin-eecb6bef8`), workflow `Matching production validation`, lane `adaptive`.
- Diagnosis/fix source base: current `origin/master` at `a54661c279846f7eaea46f7f5b3568a09ef0febe`, which includes approved live queue routing and writer shutdown guard coverage descended from `10692b2d5`. The failed run predates those tests.
- Downloaded artifact: `/mnt/storage/service/ci-evidence/40373145351/matching-production-adaptive` (428 KB retained after removing the 116 MB Go `cadence-server` executable). Its `commit.txt`, `binary.sha256`, `build.exit`, `gate.exit`, `gate/run/results.json`, `gate/run/probe.log`, dynamic config, SDK restart/recovery logs, and container log record the run evidence. Download used `TMPDIR=/mnt/storage/service/ci-evidence/40373145351/download-tmp`.
- Rust build exit: `0`.
- Rust Matching binary SHA256: `cf4ad43a7030ba74510a76af1ac4aaceecf67151c100c757ed8778b40e37ed3f`.
- Gate exit: `1`; Go baseline probe exit: `1` (`results.json`, `probe.log`). Failure stage was the adaptive probe, before Rust/Go response comparison: `tests/matching-probe-scenarios.cjs:242`, called from `run-go-service-baseline.py`.
- Exact error: `AssertionError [ERR_ASSERTION]: adaptive scaler did not increase write partitions (1 -> 0)`. The probe waited up to 45 seconds for partition configuration to advance and then failed.

## Go-to-Rust trace and root cause

Go `go/service/matching/tasklist/adaptive_scaler.go:147-149` returns without running unless both `EnableAdaptiveScaler()` and `EnableGetNumberOfPartitionsFromCache()` are true. The latter defaults to false in `go/common/dynamicconfig/dynamicproperties/constants.go:4883-4887`. Rust has the same two-condition guard: `rust/crates/cadence-matching/src/matching_registry.rs:1833` and reads the cache flag with false default in `grpc_matching.rs`.

The adaptive lane fixture in `tests/run-go-service-baseline.py` set `matching.enableAdaptiveScaler=true` and low test thresholds, but omitted `matching.enableGetNumberOfPartitionsFromCache=true`. Therefore the run did not exercise scaler production behavior; it exposed a fixture configuration error, not evidence of a production mismatch. The earlier rejected ListPartitions repair (`ca64`) is unrelated and must not be repeated.

## Fix

The fixture now enables `matching.enableGetNumberOfPartitionsFromCache=true`, scoped to `production-adaptive-scaler-probe` along with the other adaptive settings. No production code or API logic changed. The fixture comment records the Go prerequisite. Existing adaptive scale count/version checks, static `ListTaskListPartitions` one-partition assertions, and the 24 Go SDK completion assertions remain unchanged.

## Limits / reproducible status

This failure run completed its earlier Go SDK restart and recovery checks, but failed before the adaptive scale assertions could pass, before static listing checks in that scenario, and before its 24 post-scale SDK completions. The preserved SDK logs show only the earlier crash/restart/idle recovery workflows completing. The run's configured binary and its hash are preserved as historical evidence; the binary itself was removed from the downloaded copy to keep the artifact small.

No local Cargo, Go, formatting, or runtime command was run. No mixed gate was dispatched. This fixture correction has not yet been rerun; the failed run cannot validate it. Current master also contains later approved live queue routing and shutdown guard tests absent from the failed run, so this historical result says nothing about those checks. A fresh adaptive mixed-cluster gate is still required to claim the scale/listing/24-completion behavior passes, but this task intentionally did not dispatch one.
