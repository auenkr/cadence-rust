# Durable Matching reader adaptive probe trace (2026-10-27)

This change adds opt-in tracing at the durable backlog reader boundary. It is limited to task lists whose parsed root is `production-adaptive-scaler-probe`, and requires `CADENCE_MATCHING_ADAPTIVE_TRACE=production-adaptive-scaler-probe`. Other queue traffic and normal production runs stay silent.

Source: `rust/crates/cadence-matching/src/matching_reader.rs` (`adaptive_reader_trace`, `reader_partition_parts`, reader head dispatch, forwarder call, and `reader_durable_completion`). Events include root/child partition, workflow ID, run ID, schedule ID, task ID, head begin/end, forward elapsed time and exact `ForwardError` debug/status (or deadline exceeded), inferred parent destination, outer dispatch time remaining, and the fixed 2000 ms child forwarding deadline. Successful forwarding reports whether durable completion/acknowledgement and GC succeeded. The Go `MustOffer` two-second forwarding context is preserved; no dispatch, retry, persistence, or deadline behavior was intentionally changed.

Existing `grpc_matching.rs::adaptive_scaler_trace` already traces synchronous/gRPC delivery and filters the adaptive root. This fills the distinct durable reader/backlog path.

Validation: no local Cargo, Go, build, test, format, or runtime commands were run, per task instructions. Focused `rust-ci.yml` cadence-matching Actions validation is to be recorded here after it completes. No mixed-cluster gate was started; the parent owns runtime validation. This instrumentation does not establish Matching service parity or successful SDK delivery.
