# Durable Matching reader adaptive probe trace (2026-10-27)

This change adds opt-in tracing at the durable backlog reader boundary. It is limited to task lists whose parsed root is `production-adaptive-scaler-probe`, and requires `CADENCE_MATCHING_ADAPTIVE_TRACE=production-adaptive-scaler-probe`. Other queue traffic and normal production runs stay silent.

Source: `rust/crates/cadence-matching/src/matching_reader.rs` (`adaptive_reader_trace`, `reader_partition_parts`, reader head dispatch, forwarder call, and `reader_durable_completion`). Events include root/child partition, workflow ID, run ID, schedule ID, task ID, head begin/end, forward elapsed time and exact `ForwardError` debug/status (or deadline exceeded), inferred parent destination, outer dispatch time remaining, and the fixed 2000 ms child forwarding deadline. Successful forwarding reports whether durable completion/acknowledgement and GC succeeded. The Go `MustOffer` two-second forwarding context is preserved; no dispatch, retry, persistence, or deadline behavior was intentionally changed.

Existing `grpc_matching.rs::adaptive_scaler_trace` already traces synchronous/gRPC delivery and filters the adaptive root. This fills the distinct durable reader/backlog path.

Validation: no local Cargo, Go, build, test, format, or runtime commands were run, per task instructions. Focused `rust-ci.yml` cadence-matching Actions validation is to be recorded here after it completes. No mixed-cluster gate was started; the parent owns runtime validation. This instrumentation does not establish Matching service parity or successful SDK delivery.

Focused CI iteration 1: [run 40477718267](https://github.com/auenkr/cadence-rust/actions/runs/40477718267), head `1011c7f1c6c87b7cbd6a3326b0faca612dddf696`, conclusion `failure`. Clippy found that `Attempt` lacked `Debug` for the end-event field. Added a derive only; this does not alter dispatch behavior. A follow-up focused run is pending.

Focused CI iteration 2: [run 40478127638](https://github.com/auenkr/cadence-rust/actions/runs/40478127638), head `632bd31bf9c46b9f71df4ac22ee21002835bbab8`, conclusion `failure`. Clippy passed; matching test suite had 729 passed, 1 failed, 2 ignored. The failure is `grpc_matching::cold_domain_metric_tests::add_decision_task_swallows_ordinary_domain_resolver_failure`: assertion at `matching_cold_domain_metric_tests.rs:192` received `NotFound`, expected `InvalidArgument`.

Focused CI iteration 3 reran the same head: [run 40478904879](https://github.com/auenkr/cadence-rust/actions/runs/40478904879), conclusion `failure`; the same single cold-domain resolver test failed with 729 passed, 1 failed, and 2 ignored. This does not implicate reader instrumentation, but full cadence-matching test validation is not green. The instrumentation compiled and clippy passed on iteration 2; no service runtime or mixed cluster was exercised.
