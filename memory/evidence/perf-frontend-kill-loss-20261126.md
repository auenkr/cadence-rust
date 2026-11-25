# kill_frontend lost workflow: Rust bug or not? (2026-11-26)

Verdict: **not a Rust bug.** Go loses the same way at the same rate. No Rust code changed.

Runs (workflow_dispatch of recovery-timing.yml on branch perf-frontend-kill-loss, `scenarios=kill_frontend database=mysql repetitions=10`, 5 parallel runs = 50 kill events per arm):
45719235056 (failure: 1 Go lost), 45719239816, 45719245498, 45719250507, 45719256241 (all success).
Head = commit "Recovery harness: dump event history of lost workflows" on perf-frontend-kill-loss.

| arm | workflows | lost (this batch) | lost (earlier pooled run 45708672523, 30 events) | events lost / events |
|---|---|---|---|---|
| go | 7218 | 1 | 0 | 1 / 80 |
| rust | 5380 | 0 | 1 | 1 / 80 |

Fisher exact on events (1/80 vs 1/80): p = 1.0. On this batch alone Go 1/50 vs Rust 0/50: p = 1.0. No evidence of a difference.

Lost Go workflow `ad4d0727-7ff1-41c9-ada8-323345806493` (run 45719235056, rep 05, arm go). History dump (new `-lost-dir` driver flag):
1 Started, 2 DecisionTaskScheduled, 3 DecisionTaskStarted (+9 ms), 4 DecisionTaskCompleted, 5 ActivityTaskScheduled, 6 ActivityTaskStarted (+30 ms after start), 7 WorkflowExecutionTimedOut at +60 s.
The step lost is the activity task: it was handed to the worker's poller (ActivityTaskStarted) but the completion response never arrived (the frontend was SIGKILLed with the RespondActivityTaskCompleted in flight, and the SDK's retry did not land before the activity was gone). The activity StartToClose (60 s) equals the workflow timeout, so the workflow times out. This is the documented Cadence behavior (a started-but-unreported task is only recovered by its timeout). Same signature as the earlier Rust loss (START_TO_CLOSE at 60 s, started ~47 ms before kill).

Conclusion: lost-in-flight at SIGKILL is an inherent race of the driver (activity timeout == execution timeout, ~1 in 80 kills), present in both arms. Not a Rust parity gap; no fix needed. Harness change kept: `-lost-dir` in tests/frontend-gate/recovery/main.go dumps lost-workflow histories into `out/recovery/lost-<rep>-<arm>/`.
Suggestion (optional): lower the driver's activity StartToClose below the execution timeout if the loss should be reported as recovered rather than failing the job.
