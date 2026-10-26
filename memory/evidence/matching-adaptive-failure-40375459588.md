# Adaptive mixed run 40375459588 — failed

Run: https://github.com/auenkr/cadence-rust/actions/runs/40375459588

Source: `eba0b437df9096d44b82be91af542d164c5c5123`.
CI binary SHA256: `ce65db111892697fa110ebb66061779a0bcc8338aab9f8ee5cdcddb87f8f5d3f  /tmp/cargo-target-final-40375459588/debug/cadence-matching`.

Artifact exit records:
- `build.exit`: `0`
- `gate.exit`: `1`
- `gate/lane.exit`: `1`
- `gate/run/run.exit`: `1`

The probe failed at its SDK subprocess assertion. The SDK log reports `Completed 24 adaptive scaler load workflows`, then its later `workflow-task-retry` scenario hit the shared context deadline. Decision-task-not-found messages and injected workflow-task failures precede that timeout. This does not establish the cause as a fixture or production defect; diagnosis is assigned to Luna.

The adaptive scale/version/static-listing assertions precede this call in source, but no completed adaptive scenario result was emitted. Subsequent JavaScript per-load terminal-history checks and the live routing phase were not reached. Do not count the full adaptive lane as passed.

Downloaded artifact root: `/mnt/storage/service/ci-evidence/40375459588`. Original log SHA256:

- `gate/run/probe.log`: `862a590325ce2d0beb2bd8d6c4446bd82484a56a9b5f154a593d357350beac83`
- `gate/run/rust-matching/adaptive-scaler-sdk.log`: `2999799c381d7132448010fc40d93c5273fdc6cece184560d7af7d4d52ee1998`
