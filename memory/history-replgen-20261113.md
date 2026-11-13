# history-replgen (2026-11-13)
Head SHA gated: 4b2ee2ffe2b5d56ad766616647a527476e8f51d1 (master merged). rust-ci 43407015440 (c287522) success.

| Item | Status |
|---|---|
| HistoryReplicationTask + SyncActivityTask generation on active close, continue-as-new merge | DONE (+3 unit tests) |
| Cache eviction double lock (entry pinned via lock Arc cloned under map mutex) | DONE (no unit test) |
| Opus P2 7 completedID in size-limit fail | DONE |
| Opus P2 8 is_conflict_error | DONE |
| Opus P2 10 active-domain check in decision Scheduled/Started/Failed/Completed | DONE |
| Opus P2 11 DecisionTaskFailed closed -> WorkflowAlreadyCompleted | DONE |
| Opus P2 9 active-active DomainNotActive form | OPEN |
| Codex 8 ShardOwnershipLost empty owner | OPEN |
| Gate check for category-3 rows | NOT DONE: harness has no global (multi-cluster) domain lane; single-cluster gate never emits them |

Notes: NotifyNewReplicationTasks (preemptive hydration cache) skipped; reader hydrates from DB.

history-mixed-gate runs (all success, 12 combos): 43408777601 43408781711 43408786585 43408790742 43408794665 43408799203 43408803370 43408807524 43408811889 43408816305 43408820547 43408824699 (https://github.com/auenkr/cadence-rust/actions/runs/<id>). I relied on job conclusion=success; did not separately inspect each `GATE SUMMARY failed: []` line.

## Post-merge master validation

Master f9e9319d3 (2026-11-13), all 13 success; 12 gate logs show GATE SUMMARY failed: []. Runs: 43411112948 43411117187 43411122253 43411126791 43411131399 43411135347 43411139854 43411145257 43411149511 43411154046 43411157884 43411162837 43411166710 
