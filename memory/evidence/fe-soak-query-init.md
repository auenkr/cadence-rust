# FE soak query "init" FAIL, root cause and fix (2026-11-21)

## Root cause (harness, not a server bug)
Rust soak run 44813614531, workflow `...-0020994` (index/10 per second = started at ~2099 s). The Frontend was killed at t=2101 and listening again at t=2105 (steps.json). The workflow's 2 s timer fired and its activity was dispatched to the Go worker in that window.
History log (timer-queue-processor-v2): `Activity timed out`, `activity-timer-type: START_TO_CLOSE`, at 21:31:43. So the activity was started, but its completion/response was lost in the Frontend kill. The driver activity had StartToClose = 60 s, so the workflow stalled about 60 s; the SDK also logged `Workflow has unhandled signals [wake]`.
The workflow reads `wake` only after the activity, so query kept answering "init" and the driver's 90 s retry gave up. Not Rust-specific: an in-flight activity across a Frontend kill hits the Go Frontend the same way (Go lanes 44366018885, 44681388177, 44682455258).
Earlier note (2026-11-19) attributing it to query semantics was only part of the story; the init retries themselves are normal, the 90 s stall is the lost activity.

## Fix
`tests/frontend-soak/main.go`: activity `StartToCloseTimeout` 60 s -> 10 s, plus server-side `RetryPolicy` (1 s initial, x2, max 5 s, 2 min expiry, 5 attempts). A lost activity retries in ~12 s, well inside the 90 s query retry. No check was weakened: every workflow must still complete, 0 failures allowed.

## Validation
Only the Go test driver changed (no Rust code), so rust-ci / frontend-mixed-gate were not rerun.
frontend-soak (rate=10, 70 min, lanes go-baseline + rust-fe, restart at 2100 s) on branch fe-soak-query-init, head a53394794d17d35dcf723601d2ffb0d34f8c4b23:
https://github.com/auenkr/cadence-rust/actions/runs/44826841276 conclusion success, **RUST OVERALL: PASS** (all criteria PASS).
Go 42000/42000/0 failed, Rust 42000/42000/0 failed. Start p99 Go 21 / Rust 7 ms; query p99 46 / 25 ms; e2e p99 4124 / 4063 ms; query init workflows 20982 / 21049.
