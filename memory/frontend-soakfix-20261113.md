# Frontend soak fix (2026-11-13) — PARTIAL: failures fixed, tail latency still FAILS

Branch `frontend-soakfix` (from master 7d0e189c4). Not merged.

## Cause of the "query returns init" failures (fixed)
- Go `WorkflowHandler.QueryWorkflow` (go/service/frontend/api/handler.go:2643) only validates and forwards to History. The Rust Frontend validation (handler_workflow.rs) matches; no Frontend divergence found.
- Rust History `engine/query_workflow.rs` was read against Go `QueryWorkflow`; no divergence found. It is not a line-by-line audit of every helper.
- The failures came from the History bug in memory/history-soak-20261113.md: a timer task scheduled a decision while one was pending, the task retried forever, the `wake` signal was never processed, and the query was answered from state before the signal ("init"). Fix: commit 1f59dcd34 (from branch history-soakfix, guard `schedule_decision` with `has_pending_decision`, Go ScheduleDecision), cherry-picked here as a143c88bc. I did not write that fix; it needs its own review/merge from history-soakfix.
- Evidence: soak 3000 started / 3000 completed / 0 failed, no panics. Before the fix, the 2026-11-04 pilot had 17/300 failed (memory/evidence/fe-soak-20261104.md).

## Runs (head SHAs on branch frontend-soakfix)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43447923581 — head a143c88bca511fab1f5cb935b885f20fa8919b83 — success.
- Frontend soak (rate 5, 10 min): https://github.com/auenkr/cadence-rust/actions/runs/43447917811 — head a143c88bc — workflow conclusion "success" but the `compare` step crashed (KeyError 'panic_in_logs'), so that green is not a pass.
- Harness bug: the Soak step copied the gate's `summary.json` over the analysis `summary.json`. Fixed in 4b68f9824 (gate summary saved as gate-summary.json).
- Frontend soak re-run: https://github.com/auenkr/cadence-rust/actions/runs/43454535209 — head 4b68f9824004650aebcfaac3fdf9b3df35c3ccc2 — conclusion success (the compare job does not fail the run); verdict.md says **RUST OVERALL: FAIL**.

## Verdict table of run 43454535209 (Rust FE+Matching vs Go baseline, 10 min, rate 5)
PASS: no panics; 3000/3000 completed, 0 failed (Go the same); e2e p50 3163 vs 3093 ms; query p50/p95; start p50.
FAIL:
- e2e p95 14045 ms vs Go 4099 ms; p99 23071 vs 6552 ms (the real open problem).
- query "init" retries: 1712 workflows / 4137 attempts vs Go 1548 / 2121 (Go also hits "init" retries, so some init is a normal race; Rust retries about 2x as often per workflow, consistent with slower signal-to-decision handling).
- start p95 103 vs 11 ms, p99 270 vs 15 ms; query p99 131 vs 57 ms.
- fe/mt RSS and fd slopes: not meaningful for a 10-minute run (window includes startup and the Frontend restart; fe slopes n/a). Needs the full 70-minute run.

## Open: tail latency root cause NOT found
- Only Frontend and Matching are Rust in this soak (History and Worker are Go), so the e2e tail comes from Rust Frontend or Matching, not History.
- Checked and found equivalent to Go: Frontend GetWorkflowExecutionHistory long-poll flow (workflow_matching.rs vs handler.go getHistory/queryHistory), Matching task reader notify-driven pump (matching_reader.rs vs task_reader.go).
- Not checked: Matching partition forwarding/poller selection, Matching sync-match vs backlog behaviour with 2 decision pollers and sticky disabled (driver uses DisableStickyExecution), Frontend tchannel connection handling (Rust Frontend fds 128 vs Go 27 at the end).
- The retained artifact logs are only the last 300 lines per service, with TRACE output on, so they do not show the delays. Next step: add a per-stage latency breakdown to the driver (start, signal, query attempts, result) and keep full Rust Matching/Frontend logs, or run mixed lanes (Go FE + Rust Matching, Rust FE + Go Matching) to isolate the service.
