# history-p2rest (2026-11-12)
Branch `history-p2rest`. Ported: load-time checksum verify + invalidateBefore (`engine_checksum.rs`), DataInconsistentCounter + call sites, HistoryExecutionCache metrics, task notifier persistence-error notify + shadow-mode drop log.
Gaps: no repair-by-rebuild on engine path; NDC/conflict-resolve/CreateHistoryTasks notifier classification.
CI (rust-ci, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/43231286909 green.
Mixed gates: see below.
Head SHA (gated): 5fc2e30662c9cd608c7d14ee946e27000eb5593d (includes merge of origin/master).
Full workspace rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/43232267645 success.
Gates (all `GATE SUMMARY failed: []`):
- mysql/tchannel/two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43232168661
- cassandra/grpc/two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43232173600
- postgres/grpc/two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43232180482
Note: three earlier gate runs dispatched without inputs were cancelled.

## Post-merge master validation

Master 87a4f4c71 (2026-11-12): 43234482463: success 43234489096: success 43234495897: success 43234501514: success 43234507870: success 43234513968: success 43234520493: success 43234527283: success 43234533943: success 43234539568: success 43234546786: success 43234553451: success 43234559010: success 

Work paused here by user request (2026-11-12). Next: final audit w/ failure-restart-recovery evidence, Codex+Opus cross-review, dashboard update, FE soak fix.
