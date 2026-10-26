# Adaptive mixed run 40375459588 — failed

Run: https://github.com/auenkr/cadence-rust/actions/runs/40375459588

Source: `eba0b437df9096d44b82be91af542d164c5c5123`.
CI binary SHA256: `ce65db111892697fa110ebb66061779a0bcc8338aab9f8ee5cdcddb87f8f5d3f  /tmp/cargo-target-final-40375459588/debug/cadence-matching`.

Artifact exit records:
- `build.exit`: `0`
- `gate.exit`: `1`
- `gate/lane.exit`: `1`
- `gate/run/run.exit`: `1`

The probe failed at its SDK subprocess assertion. The SDK log reports `Completed 24 adaptive scaler load workflows`, then its later `workflow-task-retry` scenario hit the shared context deadline. The source has one 60s context in `tests/go-sdk/main.go`; `runMixedMatchingSDK` uses it for all 24 sequential `GetWorkflow(...).Get(ctx)` calls and every subsequent serial workflow scenario. The subprocess in `tests/matching-probe-scenarios.cjs` already has a 180s hard timeout. In the artifact, the SDK worker starts at `18:15:22.078`; the injected workflow-task panics are logged at `18:16:12.164` and `18:16:12.409`, roughly 50s into the suite and with at most 10s left on the shared context. The context expires and the worker stops at `18:16:22`. This confirms the shared suite budget cut off the retry assertion. It does not prove that the retry would have completed: the `Decision task not found` SDK log records have no workflow ID, so they cannot be tied to this retry or distinguished from stale responses in other scenarios. The later History transfer-queue connection timeout at `18:16:24.219` occurred after the probe shut down Matching, and is teardown fallout, not evidence of the pre-timeout delivery path.

The pinned source `eba0b437...` already includes commit `cc147f32f`'s Matching fix: once a task is claimed, the poll waits under the caller deadline while the long-poll window only bounds waiting for a task. The run's dynamic config has no `matching.longPollExpirationInterval` override for this SDK task list. Thus the previously documented 2s claimed-task long-poll divergence is not an explanation for this run. The SDK code still preserves the normal echo, activity retry, child, workflow-run retry, workflow-task retry, timeout, signal, and cancellation assertions.

Harness correction on branch `t3/adaptive-sdk-retry-timeout`: mixed Matching mode uses a 150s shared SDK context, leaving 30s before the existing 180s subprocess cap; standalone SDK mode keeps its 60s budget. This is based on the observed suite consuming about 50s before the workflow-task retry and on its additional sequential checks. It addresses the confirmed premature suite deadline, not a claimed Matching parity fix. The exact workflow-task retry remains unverified because the requested follow-up excludes mixed-gate dispatch and local Go/runtime execution. No Cargo, Go, format, or runtime command was run for this correction.

The adaptive scale/version/static-listing assertions precede this call in source, but no completed adaptive scenario result was emitted. Subsequent JavaScript per-load terminal-history checks and the live routing phase were not reached. Do not count the full adaptive lane as passed.

Downloaded artifact root: `/mnt/storage/service/ci-evidence/40375459588`. Original log SHA256:

- `gate/run/probe.log`: `862a590325ce2d0beb2bd8d6c4446bd82484a56a9b5f154a593d357350beac83`
- `gate/run/rust-matching/adaptive-scaler-sdk.log`: `2999799c381d7132448010fc40d93c5273fdc6cece184560d7af7d4d52ee1998`
