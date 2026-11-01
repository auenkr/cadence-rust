# Frontend continuation checkpoint

Restored after context handoff on 2026-11-01. Master was clean and synchronized with origin at `6a5c3d7d6`. User dashboard remains preserved. Frontend is incomplete; Rust Matching remains the matching service for all Frontend validation. No local Cargo, rustfmt, Go validation or cluster execution was performed.

## Integrated evidence

- Replication wire fixtures: code head `1bc05691ab778e96e289a39e173104f29618b762`, package CI https://github.com/auenkr/cadence-rust/actions/runs/41330541065 succeeded. Independent raw-byte goldens cover only DomainTaskAttributes and ReplicationTaskInfo; other fixtures use the project codec.
- Ringpop listener lifecycle: code head `c6c011c3b4fc8573b3419032760944ff9b23d861`, package CI https://github.com/auenkr/cadence-rust/actions/runs/41331347247 succeeded. Actual listener handle is aborted/joined during cleanup, with bound-port regression coverage.
- Integrated workspace CI https://github.com/auenkr/cadence-rust/actions/runs/41331730633 succeeded at `f5d2151f9dd0aa0237fda69072cb2a451eefb609`. This is not final clean-target or mixed-cluster acceptance.

## Running work to preserve

Mixed gate https://github.com/auenkr/cadence-rust/actions/runs/41331739932 remains in progress, checked at approximately 17:39 UTC. Workflow and Rust source head are both `f5d2151f9dd0aa0237fda69072cb2a451eefb609`. Fresh Rust and Go builds succeeded; runtime Gate step is active. Retrieve terminal artifacts, binary SHA256, process survival, SDK and recovery results when finished. Do not start a duplicate gate or infer compatibility from the successful build.

Two existing Luna medium/default delegates retain ownership; do not poll them or start a third:

- Async gRPC routing, task suffix `fd99446e-44ff-4326-ba6f-111bc0bc2dbe`: grpc.rs and memory only. Hook async Start/SignalWithStart with wrappers, metadata/deadline/error and fallback tests. This does not supply a production async publisher.
- Configured visibility and final transport state, task suffix `60537aee-9465-4795-8397-7aa24f4d97f5`: launcher.rs, matching_task_store.rs and SQL visibility readers. Select configured visibility role without task schema validation; preserve default-store task/history/domain routing and Matching-only startup. Separately defer HTTP/gRPC/TChannel state snapshots until persisted manager, stores and workflow hook are installed.

## Next bounded gaps

Direct JSON workflow handlers in service-core handlers.rs bypass the shared hook. YARPC dispatch_with_headers already consults workflow_api (confirmed again during restoration); it needs final state installation, not a duplicate dispatcher. Preserve shutdown middleware, body/error semantics, caller headers, deadlines, GET query conversion and no-hook behavior when implementing JSON dispatch.

Production async publisher remains absent. Port Go domain producer selection, Kafka configuration and acknowledged publication; never substitute an in-memory success. Admin replication still needs pinned protobuf conversions, shared endpoints and real History shard routing/retry behavior. Correct Go mapper paths and narrowing/oneof semantics are recorded in frontend-transport-followups-20261101.md.

After workers finish: review, integrate and record exact CI heads/results; clean up only ancestry-merged, clean, idle branches/worktrees. After the current mixed gate terminates, use actual failures to prioritize fixes and run the next pinned candidate once ready. Final service acceptance still requires a fresh uncached Clippy-then-workspace-test gate and mixed SDK/service/restart/recovery evidence with binary SHA256.
