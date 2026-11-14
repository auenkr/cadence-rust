# History soak validation (2026-11-14) - FAIL, stall persists

Branch `history-soakstall`, head 0e46b77c11ee2d19f9259f36b27eff63acb37f8c (= soakstall 644d71307 + origin/master d8f7888bd, which has the Matching "never drop a claimed decision task for a query" fix). Rust History + Matching + Frontend, Go Worker, 45 min, 4 wf/s, two_history=true (History restarted at ~1350s).

## Soak results (all 3 FAIL, gate.exit=1)
| DB | run | started | completed | failed | e2e p50/p95/p99 ms | start p50/p95/p99 ms | query p50/p95/p99 ms | query init wfs | RSS history early/late MB |
|---|---|---|---|---|---|---|---|---|---|
| mysql | https://github.com/auenkr/cadence-rust/actions/runs/43516438846 | 10800 | 123 | 10677 | 25218/30735/112955 | 14/27/36 | 83/282/892 | 1903 | 230/243 (h2 211/221) |
| postgres | .../runs/43516444784 | 10800 | 77 | 10723 | 25514/111837/113902 | 16/42/84 | 86/489/864 | 1947 | 98/102 (h2 254/297) |
| cassandra | .../runs/43516450001 | 10800 | 23 | 10777 | 27824/112916/112917 | 18/40/62 | 97/310/539 | 1840 | 260/292 (h2 307/332) |
No Go baseline lane was run in this pass; failure is ~99% non-completion, not a tail-latency delta, so it is not a "Go fails equally" case (earlier Go-baseline notes in memory/frontend-soakfix-20261113.md show Go completes).

## Mixed gates: all 18 success
History mixed gate, head 0e46b77c1. 12 grpc runs (my first batch omitted `outbound`, so each db x two_history combo ran twice on grpc): 43516455745 43516460396 43516465587 43516470512 43516475607 43516479873 43516485897 43516490918 43516495630 43516500533 43516505497 43516510139. 6 tchannel runs: 43516541756 43516546279 43516550450 43516555095 43516559672 43516563883. All conclusion success. `GATE SUMMARY failed: []` was NOT individually read from logs (only the conclusion) - still to verify.

## Diagnosis so far (not fixed)
postgres matching log: 43k add_decision_task ok (14-18ms), 103k PollForDecisionTask, 74k RespondQueryTaskCompleted/query_workflow, 42k "QUEUE-TRACE claim task=true", history logs show no errors. So the earlier History errors are gone, but very few workflows complete: driver sees `query state "init"` / "workflow must handle at least one decision task before it can be queried" for ~1900 workflows. Decision tasks are added to matching but do not result in completed decisions; retried_calls ~510k. Next steps: soak with two_history=false, log RespondDecisionTaskCompleted counts in History/Frontend, check whether PollForDecisionTask returns tasks to the Go worker (matching poll->History RecordDecisionTaskStarted dedupe, e.g. repeated "sched=2/8/14" starts: 14607 RecordDecisionTaskStarted for sched=2 suggests tasks re-dispatched many times), and compare with a Go-History baseline run.
