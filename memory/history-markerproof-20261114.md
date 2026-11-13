# History failover-marker cross-host proof (2026-11-14) — INCOMPLETE

Branch history-markerproof. Commit adds info logs only (no logic change):
- send: `failover-marker-send: NotifyFailoverMarkers to owning history host` (target_host, target_grpc, tokens, domains) in failover_client.rs
- recv: `failover-marker-recv: NotifyFailoverMarkers applied on this history host` (shard_ids, domain_id, failover_version) in handler_impl.rs
rust-ci cadence-history run dispatched: https://github.com/auenkr/cadence-rust/actions/runs/43484951832 (result not yet recorded here).

## Why the assertion is NOT added yet
- history-mixed-gate (two_history=true) runs run-frontend-gate.py: a SINGLE cluster. Failover markers exist only for a *graceful* failover of a global domain between two clusters (pending-active). A single-cluster gate cannot trigger them.
- run-frontend-two-cluster-gate.py has two clusters, but Rust History is one host there and its failover step is a non-graceful `domain update --active_cluster` (no markers).
- Needed: a two-cluster gate with two Rust History hosts on the cluster that gets `--failover_type grace`, then grep rust-history*.log for send/recv with different hosts. Go baseline: Go logs for this path are not emitted at info level, so no Go log comparison exists; comparison would use the Go-side metrics/behaviour only.
Gates 12-combo/two_history runs were not dispatched because they cannot exercise the path.
