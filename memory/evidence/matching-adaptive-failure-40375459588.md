# Adaptive mixed run 40375459588 — failed

Run: https://github.com/auenkr/cadence-rust/actions/runs/40375459588

Source: `eba0b437df9096d44b82be91af542d164c5c5123`.
CI binary SHA256: `ce65db111892697fa110ebb66061779a0bcc8338aab9f8ee5cdcddb87f8f5d3f  /tmp/cargo-target-final-40375459588/debug/cadence-matching`.

Artifact exit records:
- `build.exit`: `0`
- `gate.exit`: `1`
- `gate/lane.exit`: `1`
- `gate/run/run.exit`: `1`

The probe failed at its SDK subprocess assertion. The SDK log reports `Completed 24 adaptive scaler load workflows`, then its later `workflow-task-retry` scenario hit the shared context deadline. The Go driver in `tests/go-sdk/main.go` gave the entire SDK subprocess only 60 seconds, even though the adaptive scenario first waits for 24 workflows and then runs more SDK retry, child, timeout, and signal workflows on the scaled task list. The logged workflow-task retry began at 18:16:12 UTC and the driver context expired at 18:16:22 UTC. Two injected workflow-task panics occurred in that 10-second window, so there was no budget for the required later successful retry attempt. This is a fixture-level overall timeout defect; it does not show a Rust Matching retry defect.

The server log also contains History timer/transfer queue failures such as `context deadline exceeded while waiting for connections to become ready` for the activity-retry and workflow-task-retry executions. These show pressure from the shared mixed fixture while the adaptive task-list config was growing (History logged write/read partition counts 90 and then 338). They are not evidence that Rust Matching rejected the retry task: the final workflow-task retry panic is an intentional Go SDK fault injection, and the enclosing Go SDK context expired immediately afterward. Preserve these logs as a secondary signal to recheck in the next permitted adaptive run.

Fix committed in the current branch: when `CADENCE_MIXED_MATCHING_LOAD_IDS` is set, the compatibility driver now uses a three-minute overall context while leaving each workflow's execution timeouts and all 24-load, scale/version, static-listing, and live-routing assertions unchanged. Ordinary SDK runs retain the one-minute context. No mixed gate is launched in this change; it must wait until the parent integrates the retry pin.

The adaptive scale/version/static-listing assertions precede this call in source, but no completed adaptive scenario result was emitted. Subsequent JavaScript per-load terminal-history checks and the live routing phase were not reached. Do not count the full adaptive lane as passed.

Downloaded artifact root: `/mnt/storage/service/ci-evidence/40375459588`. Original log SHA256:

- `gate/run/probe.log`: `862a590325ce2d0beb2bd8d6c4446bd82484a56a9b5f154a593d357350beac83`
- `gate/run/rust-matching/adaptive-scaler-sdk.log`: `2999799c381d7132448010fc40d93c5273fdc6cece184560d7af7d4d52ee1998`
