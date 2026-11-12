# history-p2rest (2026-11-12)
Branch `history-p2rest`. Ported: load-time checksum verify + invalidateBefore (`engine_checksum.rs`), DataInconsistentCounter + call sites, HistoryExecutionCache metrics, task notifier persistence-error notify + shadow-mode drop log.
Gaps: no repair-by-rebuild on engine path; NDC/conflict-resolve/CreateHistoryTasks notifier classification.
CI (rust-ci, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/43231286909 green.
Mixed gates: see below.
