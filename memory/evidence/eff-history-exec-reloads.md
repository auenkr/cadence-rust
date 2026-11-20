# eff-history-exec-reloads (2026-11-20)

## Finding
- The `executions` 1.47x / map tables 1.50x ratio in bench-rerun-20261120.md (History lane) is an **aggregation artifact**:
  raw per-arm `select_by_table` in run 44625313998 shows executions ~9230-9250 and every `*_info_maps`/`buffered_events`
  ~1746-1752 for BOTH Go and Rust in all 3 arms (ratio 1.00). Go arm 00's top-300 `rows` list lacks the map rows, which skewed the averaged table.
  Execution-cache behavior (Go `makeReleaseFunc` clear on err/forceClear, LRU+TTL, lock-per-update) already matches; no reload fix needed.
- Real gap: `history_node` Rust 7485-7500 vs Go 5990-6000 per arm = exactly +1 read per workflow (1500 workflows/arm).
  Cause: `push_event` wrote WorkflowExecutionStarted to the shard events cache while `exec.workflow_id` was still empty
  (new_for_start), so `GetStartEvent` missed and read history_node. Go writes the start event after
  `ReplicateWorkflowExecutionStartedEvent` (real workflow id).

## Fix (commit 13a74d4a9)
- engine_start.rs: `write_event_to_cache(&event)` after exec info is populated; engine_events.rs: push_event skips caching
  the start event. Unit test `start_event_is_cached_under_real_workflow_id`.

## Validation (head 13a74d4a990f79a87fc0ca941ef2ef35ae29fcb4)
| Check | Run | Result |
|---|---|---|
| rust-ci cadence-history | https://github.com/auenkr/cadence-rust/actions/runs/44638180285 | success |
| History mixed gate MySQL | https://github.com/auenkr/cadence-rust/actions/runs/44638187320 | success, GATE SUMMARY failed: [] |
| History mixed gate Cassandra | https://github.com/auenkr/cadence-rust/actions/runs/44638198920 | success, GATE SUMMARY failed: [] |
| History restart gate (mysql, v2 default on) | https://github.com/auenkr/cadence-rust/actions/runs/44638204541 | success |
| History benchmark after (25/s, 60s x3, tmpfs, sql_log 60) | https://github.com/auenkr/cadence-rust/actions/runs/44638241327 | success (fresh build) |
| Baseline rebench dispatch | 44638246443 | failed at checkout (short source_ref); before = run 44625313998 (same Rust code) |

## Per-arm SELECTs (Go / Rust)
| Table | before 44625313998 | after 44638241327 |
|---|---|---|
| history_node | 5990/7485, 5981/7485, 6000/7500 (1.25x) | 5997/5985, 6000/5983, 6000/5986 (1.00x) |
| executions | 9238/9232, 9223/9232, 9252/9252 | 9259/9229, 9251/9226, 9254/9231 |
| activity_info_maps | 1749/1747, 1746/1747, 1752/1752 | 1751/1747, 1751/1746, 1753/1747 |

Verdict: History SELECTs now at Go parity for executions, map tables and history_node. Follow-up: fix agg.py per-table averaging (use select_by_table).
