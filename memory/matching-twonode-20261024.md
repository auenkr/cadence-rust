# Matching two-node + restart/failover gate (2026-10-24)
Result: PASS on GitHub Actions (workflow .github/workflows/matching-mixed-gate.yml; runs on push to branch matching-twonode-20261024 or workflow_dispatch; gh workflow run needs the file on master, so the push trigger was used).
Run: https://github.com/auenkr/cadence-rust/actions/runs/40071731050 (commit 95ba26b852ee, fresh runner build, no shared-cache stale risk).
Command: tests/gates/run-mysql-lane.sh out/gate <cadence-matching> --matching-restart --matching-failure-recovery --matching-idle-check-seconds 3
Exit codes: cargo-build 0, gate 0, lane.exit 0, run.exit 0, matching-shutdown.json exitCode 0 (SIGTERM).
Binary sha256 (debug cadence-matching): 6e7c89d62612e93a90d3fa8497911eb31ef3533eac229d3f4fb55da8fbcab161
Evidence: memory/evidence/matching-twonode-20261024/run1/ (probe.log, results, per-scenario SDK logs, production-failure-recovery-checkpoints.json showing identical ring ownership maps on both Rust nodes).
Covered: two Rust Matching nodes + Go frontend/history/worker on MySQL 8.0.36; idle eviction and recovery; graceful drain (SIGTERM, queued 1 -> 0, new pid, 1 completion); SIGKILL restart (1 durable row, new pid, 1 completion); crash recovery; typed non-owner ownership error decoded by Go; query/isolation/native partition dispatch; GC + domain metadata.
Root cause of the 20261023 failures: (a) primary Rust exited 1 at SIGTERM because the shutdown final-ack persist hit lost task-list ownership (range ID mismatch); already fixed in base by tolerate_lost_ownership (matching_registry.rs), matching Go, whose Stop never fails on lease loss. (b) ECONNREFUSED / "Decision task not found" / hang in runs b and c came from old frozen binaries under host load 9-13; not reproducible on an unloaded runner with current code. No new Rust change was needed.
Caveats: harness reports acceptanceEligible=false by design (Cassandra gate is the acceptance lane). Single run only; flakiness not measured.
Local note: go/idls submodule was empty in this worktree (copied from another worktree for a local build); local builds were abandoned per coordinator instruction.
