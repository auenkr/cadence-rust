# Async restart message loss (2026-11-14)

Failing run: https://github.com/auenkr/cadence-rust/actions/runs/43599084228 (mysql, head f7a35e30) - step
`sdk-async-frontend-restart-no-lost-messages` lost `...-fe-restart-22` (restart requested at i=20).
Service logs were not in the artifact (kafka-container.log exceeded quota on download), so the exact lost message path could not be proven.

## Analysis (code comparison)
- Rust publisher (`rust/crates/cadence-frontend/src/async_kafka.rs::send`) replies only after rskafka `produce` resolves (acks=all), same as Go's sync producer. No loss path on publish.
- Consumer is the Go Worker (Rust Worker is skipped); it is not touched by the Frontend restart.
- Go consumer (`go/common/asyncworkflow/queue/consumer/default_consumer.go`) calls Frontend with `CreateFrontendServiceRetryPolicy` (expiration 15s); on failure it Nacks, and the Kafka Nack publishes to the DLQ (`consumer_impl.go`). Harness restart = kill, 3s sleep, restart, so an in-flight message hitting an outage >15s is DLQ'd. This is Go behaviour, unchanged (no logic change). Likely cause of the single flake, unconfirmed.
- Harness change only: `tests/gates/run-frontend-gate.py` now prints Go consumer failure lines after the async phase.

## Stability re-runs (branch fe-async-restart-loss, head e9fb140c, mysql, kafka=true)
- https://github.com/auenkr/cadence-rust/actions/runs/43602030188 success, GATE SUMMARY failed: []
- https://github.com/auenkr/cadence-rust/actions/runs/43602958675 success, GATE SUMMARY failed: []
- https://github.com/auenkr/cadence-rust/actions/runs/43604069064 success, GATE SUMMARY failed: []
(Two earlier dispatches were cancelled by the workflow concurrency group; runs were serialized.)
