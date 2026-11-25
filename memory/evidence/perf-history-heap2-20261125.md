# perf-history-heap2 (2026-11-25)
Review: load_state (EngineMutableState::from_snapshot) keeps the decoded snapshot plus decoded structs (pc_exec, pc_acts, pc_children,
timers, version_histories cloned from the envelope); removing those needs a wide refactor of the mutable-state write path, judged too risky for
identical behaviour. Events cache already stores Arc<HistoryEvent>, default max count 512 = Go. Only the cache cloned each EventKey twice
(map key + LRU index); change: share one Arc<EventKey> (events/mod.rs). Behaviour identical.
CI: rust-ci cadence-history 45540826469 success; history-mixed-gate 45541735032 success (head = branch head at push).
Three-arm benchmark 45541739794 (3 reps, control 9ce0ba1a1) success. History peak_cgroup_anon MiB (mean of 3):
| conc | go | control | candidate |
|---|---|---|---|
| 1 | 72.0 | 76.4 | 77.8 |
| 16 | 91.4 | 89.8 | 90.5 |
| 64 | 104.3 | 103.2 | 101.0 |
Throughput cand/control median 1.018 / 1.012 / 1.017 (within noise). Memory delta (+1.4, +0.7, -2.2 MiB) is within noise: NOT a resolvable
reduction. Rust History is ~at parity with Go (c64 -3.3 MiB, c1 +5.8). Conclusion: change is harmless but gives no measurable win; do not claim improvement.
Real remaining lever would be dropping the duplicated persistence snapshot in EngineMutableState (large refactor).
