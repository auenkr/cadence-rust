# Frontend residual gaps (branch t3/fe-residual-gaps, 2026-11-04)

Head SHA validated: ded5a24795acf0264c5ef4f5730c804e13769012

## CI / gate runs (all on ded5a24)
- rust-ci (clippy + workspace tests): https://github.com/auenkr/cadence-rust/actions/runs/41769877646 — success
- frontend-mixed-gate mysql (single cluster, includes admin probe): https://github.com/auenkr/cadence-rust/actions/runs/41768785863 — success, `GATE SUMMARY failed: []`; Rust binary sha256 19a97c6069ed6c4df9be83678f7d51d46d84cc09d3ae70a24dad7f16faa41628
- frontend-mixed-gate postgres: https://github.com/auenkr/cadence-rust/actions/runs/41769881366 — success
- frontend-mixed-gate two_cluster (earlier head 7ee36f5; lane uses a different script, does not run the admin probe): https://github.com/auenkr/cadence-rust/actions/runs/41766428047 — success

## Per gap
1. Decision-task times in ns end to end (workflow_matching.rs, thrift convert, proto convert, in-memory executions): done; unit tests added; gate green. Activity-poll ms path left unchanged.
2. RegisterDomain typed not-found: `DomainReadError` + `get_domain_typed` on the persistence trait (all four backends); `exists_for_register` matches NotFound. Test added.
3. Failover watcher GetFailoverInfo: ported (log-only, 1s bound, error logged and continued). Routed via the single `history_admin_channel`, not Go's per-domain peer resolver (logging only).
4. Gate coverage: tests/gates/admin_probe.py (called from run-frontend-gate.py, grpcio-tools installed in the workflow). Passing assertions through Rust Frontend against Go History:
   - GetCrossClusterTasks empty cluster -> INVALID_ARGUMENT "Cluster name is not set."
   - GetCrossClusterTasks with cluster -> INTERNAL "The cross-cluster feature has been deprecated." (Go History returns a value-typed BadRequestError, which its gRPC mapper does not convert)
   - ReapplyEvents: missing domain/execution/workflowId messages match Go; unknown domain -> NOT_FOUND; bad batch -> INTERNAL DeserializeBatchEvents (Go History).
   Production fixes found by the probe: Go messages, History tonic status -> typed errors (`internal()` mapping), ReapplyEvents History proxy (HTTP/admin.rs path used by gRPC admin). The Thrift/yarpc ReapplyEvents path was not separately probed.
