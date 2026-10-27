# Matching adaptive probe tracing

For `production-adaptive-scaler-probe`, Rust Matching now has opt-in boundary tracing to distinguish scaler decisions from workflow-task delivery stalls. Enable it with `CADENCE_MATCHING_ADAPTIVE_TRACE=production-adaptive-scaler-probe` in the Matching process environment.

Trace source pointers:

- `rust/crates/cadence-matching/src/grpc_matching.rs`: `adaptive_scaler_trace`; decision sync attempt/outcome, forwarding elapsed/status/ack, durable enqueue elapsed/status, and poll wait/result elapsed/token presence plus workflow/run/started-event identifiers. Add and forwarding records carry workflow ID, run ID, and schedule ID.
- `rust/crates/cadence-matching/src/matching_registry.rs`: `scaler_update` emits tick timestamp, observed QPS, current read/write partition counts, proposed write count, and isolation assignment.

The selector matches the probe root and parsed child partitions. Tracing is disabled unless the exact environment value above is set. No timeout or scaler algorithm behavior is changed. Focused CI evidence is recorded after dispatch; mixed runtime validation remains parent-owned.

Focused CI dispatch: [rust-ci run 40476193408](https://github.com/auenkr/cadence-rust/actions/runs/40476193408), package `cadence-matching`, head `6c0ed7c769e770c81946720a9bfc417a63111d4b`. Last observed status was `in_progress`; no conclusion is available yet.
