# Native Matching scaler (2026-10-28)

Added `matching_scaler_native.rs`: raw GoString partition/group maps, sustained scaling and isolation balancing, owner-scoped loop, cache update and partition descriptions. The starter takes live configuration and a byte-native remote describe callback; launcher must wire both, and register the module. The callback must route to the actual partition owner and return QPS, emptiness, write membership and raw group poller/QPS data. Scaler telemetry is not yet wired. Verify ownership replacement/cancellation races, remote error handling, and Go parity with mixed-cluster recovery tests. The configuration provider must supply raw isolation group bytes separately from the legacy string settings.

No Cargo, formatting, Go, CI, or mixed-cluster validation was run in this worker; parent owns integration and CI. Tests added here are unverified.
