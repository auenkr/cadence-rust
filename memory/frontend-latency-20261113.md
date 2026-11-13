# Frontend soak tail latency (2026-11-13) — fixed in Rust Matching

Branch `frontend-latency`. Head SHA of validated code: 8ee8a44538bde6e29734c7a00d19926059ec9dd5.

## Symptom
Rust Frontend + Rust Matching (Go History/Worker) soak: e2e p95 14.0s vs Go 4.1s, p99 23.1s vs 6.6s,
query "init" retries 4137 vs 2121 (baseline run 43454535209). Reproduced with Go Frontend + Rust
Matching (lane `rust-mt`), so the cause is in Matching, not Frontend. Extra delay = ~10s decision
StartToClose timeout: a decision was started in History but never delivered.

## Root cause (Matching, `rust/crates/cadence-matching`)
`matching_engine_poll_native.rs` decision poll did `select! { biased; query = queue.queries.next() => ..., result = dispatcher.poll_decision_with_poller(...) }`.
The query arm stays registered while the dispatcher worker has already claimed a task and is inside
`RecordDecisionTaskStarted`. When a query offer arrived, the biased query arm won, dropped the poll
future, the oneshot receiver closed, and `delivery_loop` hit "Poll cancelled during start": the History
RPC was aborted mid-flight (History had often started the decision) and the task was rewritten, so the
decision sat until the 10s timeout. In run 43484374903: 39,528 of 243,532 begin lines were dropped.
Go: `getTask` yields a task OR a query, never both (go/service/matching/tasklist/task_list_manager.go,
matcher.go), and RecordDecisionTaskStarted runs on the request context
(go/service/matching/handler/engine.go pollLoop ~698-870, recordDecisionTaskStarted ~1475).

## Fixes (all Matching; confirms the orchestrator hint about dropped RecordDecisionTaskStarted)
1. `PollClaim` (flag + Notify) shared via `NativePollLifecycle.claim`; `QueryChannel::next_unless_claimed`
   stops accepting query offers (and deregisters as a query waiter) once a task is claimed.
2. CancelOutstandingPoll / failover / blocked-poller release end only the GetTask wait, never a claimed
   task (Go pollerCtx semantics) — `poll_with_lifecycle` + engine selects.
3. Trace line `NATIVE-TRACE poll receiver dropped during start` for any remaining drops.

## Evidence
- rust-ci cadence-matching green: run 43501099395 (head 8ee8a44).
- Soak run 43502282747 (head 8ee8a44, lanes go-baseline+rust, rate 5, 42000 workflows, verdict.md):
  e2e p50/p95/p99 rust 3108/4113/4141ms vs go 3096/4104/4135ms (PASS); 42000/42000 complete, 0 errors;
  RecordDecisionTaskStarted begin/end 203887/203885, 2 dropped receivers; start/query latency PASS;
  fe rss/fds PASS, mt fds PASS. Overall verdict still FAIL on two criteria:
  - mt_rss_mb_per_h rust 6.92 vs go 1.04 (limit go+5): marginal RSS slope, to investigate separately.
  - query "init" none: Go also fails it (go 28524 attempts/20992 wf vs rust 26418/21094) — criterion is
    not discriminating vs Go; rust is not worse.
- Earlier runs: 43465391047 (mixed lanes), 43470070249 (STAGE data), 43479408047 (cancel/failover fix only,
  still FAIL), 43484374903 (released-guard fix, still FAIL: 39.5k drops).
- Mixed gate: frontend-mixed-gate.yml run 43514600864 (head 8ee8a44) conclusion success.

## Open
- rust-fe lane (Rust Frontend + Go Matching): 100% query timeouts, Connection reset on CancelOutstandingPoll to Go Matching — not
  investigated (harness or tchannel interop); not needed for the default Rust+Rust verdict.
- mt RSS slope marginal fail.
