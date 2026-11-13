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

## Update: gate mode added, proof still NOT obtained (sub-agent run, branch history-markerproof)
New opt-in input `two_history_graceful` on frontend-mixed-gate.yml (needs two_cluster=true rust_history=true rust_history_side=active) -> `--two-history-graceful` in tests/gates/run-frontend-two-cluster-gate.py. Topology: Go cluster A (active, all Go), cluster B = Go FE/Matching/Worker + TWO Rust History hosts (rust-history, rust-history-2). Domain registered active=cluster0; 4 workflows on A; `domain update --active_cluster cluster1 --failover_type grace --failover_timeout_seconds 30` on B (graceful must be requested on the TO-BE-ACTIVE cluster: Go rejects it on the to-be-passive one, "Cannot start the graceful failover from a to-be-passive cluster", run 43485348186); asserts failover complete on A+B, grep send/recv logs, workflow start+signal on B replicated to A.
Findings by run:
- rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/43485361173 success; plain two-cluster gate (no new input, side active) .../43485354354 success (no regression, head 9c4f220).
- Attempt with Rust History on standby A (two hosts on A, update on A): .../43491015355 and primary-A variant .../43493070454 failed: Rust Frontend gRPC admin `get_domain_replication_messages` (rust/crates/cadence-matching/src/grpc_admin.rs:~918) is a stub returning "domain replication queue not enabled for cluster", so a Rust-Frontend cluster cannot be the domain-replication SOURCE; Go worker on B logs that error. Real Frontend gap (separate from History), not fixed here (no production changes).
- Final topology run (head 99fa8f79): https://github.com/auenkr/cadence-rust/actions/runs/43494792030 -> failover completes on A and B, workflows continue on B and replicate to A, both Rust History hosts alive, BUT `GATE SUMMARY failed: ['grace-marker-send-cross-host', 'grace-marker-recv-on-other-host']`: zero `failover-marker-send`/`-recv` lines in rust-history*.log (Go A log only shows "Marker notifier ... Started"). Either Go A never emitted FailoverMarker replication tasks (condition at go/service/history/engine/engineimpl/register_domain_failover_callback.go:239: needs domain failoverNotificationVersion >= shard notification version and previousClusterName==current) or the Rust FailoverMarker replication-task -> AddingPendingFailoverMarker -> coordinator path did not reach failover_client; 30s timeout may also have completed the failover first. Next step: log at info in Rust replication processor when a FailoverMarker task is received (deps.rs/task_hydrator) and in the coordinator, then re-run with a longer timeout (e.g. 300) and Go A debug logging.
Gate step assertions are in place and fail honestly; no pass claimed.

## RESOLVED (2026-11-14): cross-host failover-marker proof PASSES on mysql
Root causes found:
1. Rust History defaults to RUST_LOG=warn, so none of the info logs printed (earlier "0 markers" was a logging artefact). Gate now starts graceful-mode Rust History with RUST_LOG=info.
2. REAL PARITY BUG: Go `DomainCacheEntry.IsActiveIn` returns false when `IsDomainPendingActive()` (failoverEndTime != nil); Rust `domain_is_active_in` (wiring/assembly/failover.rs) lacked this, so `AddingPendingFailoverMarker` dropped every marker as "domain is active in current cluster" on the to-be-active cluster. Fixed exactly as Go (head dabee2bb).
Also added info logs along the receive path: failover-marker-task-received, -dropped-stale-on-add, -pending-added, -coordinator-notify/-receive; gate failover timeout 300s.
Evidence (all branch history-markerproof, head dabee2bbb73adbe70f058a23d00c27154c729b5b):
- Graceful gate (two_cluster, rust_history, side=active, two_history_graceful, mysql): https://github.com/auenkr/cadence-rust/actions/runs/43499788904 success, `GATE SUMMARY failed: []`; steps grace-marker-send-cross-host (sends=4 cross=2; e.g. `failover-marker-send: ... target_host=127.0.0.1:40177 target_grpc=127.0.0.1:45089 tokens=1`) and grace-marker-recv-on-other-host (recv hosts=['rust-history-2'] count=4) both ok; failover completed on A and B, post-failover workflow start/signal on B replicated to A.
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43499782628 success (rust-check 43499783384 success).
- Regression, plain two-cluster gate with Rust History: standby side https://github.com/auenkr/cadence-rust/actions/runs/43500937883 success; active side .../43502445809 success (43500933422 was cancelled by the shared concurrency group, not a failure).
- Failed progression runs: 43496182781 (no logs: RUST_LOG), 43498016479 (markers received but dropped stale -> parity bug).
postgres/cassandra: not run — the two-cluster lane (tests/gates/run-frontend-two-cluster-lane.sh) is MySQL-only; no such lane exists for those databases.
Note: Rust Frontend get_domain_replication_messages gRPC stub left untouched per user.
