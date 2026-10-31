# Matching PostgreSQL gate progress (2026-10-31)

Branch `t3/matching-overlay-reentrancy-fix`. Matching is NOT complete; 0/10 mixed lanes accepted.

Fixed this session (all verified by gate progress, evidence under ci-evidence/g-<run>):
- Overlay mutex reentrancy deadlock (lock hoisting), rust-ci 41118272139 passed.
- prost omitted default map values; added `wire::message_map` (cadence-proto), rust-ci 41123206198 passed.
- Harness: crash-recovery expects 2 decision tasks; seed worker exit race relaxed.
- Harness: durable-restart scenario uses fixed list `probe-durable-restart-root` with a 3-partition dynamic-config constraint (Go getAllPartitions reads dynamic config, not adaptive config). Run 41128468137 failed there before this fix.

Open failure (runs 41126816223, 41129774119, 41131219584, PostgreSQL lane):
`tests/matching-service-compat.cjs` isolation step: zone-a poll returns empty; diagnostic in 41131219584 shows the zone-b poller (pollB) received the task, so a task added with partitionConfig isolation-group=zone-a leaked to zone-b in Rust Matching. Passed once (41128468137), so it is timing dependent. Next: inspect the Rust isolation selector (`matching_runtime_native.rs`, `matching_registry_native.rs` sync offer) against Go getIsolationGroupForTask and the 2s poller window.
Other threads dispatch gates concurrently on this repo and cancel each other's runs via the concurrency group.

Still pending: remaining lanes, fresh package-unset workspace CI, parity audit.
