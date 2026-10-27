# Isolated adaptive retry failure

Run https://github.com/auenkr/cadence-rust/actions/runs/40465991711 failed
on c268c99cde16c4b12e7ab7edd048c13443329ad3. Build exit0, gate exit1.
Rust Matching SHA256 b906aa69f3bb682db6edf1764a56e31f91245a79e00b50aa147bfcbc879290c2.
The isolated retry started with59.951 seconds remaining and failed after59.9s.
History retained initial WorkflowExecutionStarted, DecisionTaskScheduled,
DecisionTaskStarted and deliberate DecisionTaskFailed. No completion occurred.
No durable retry Scheduled event does not prove History never scheduled a
retry: original Go transient decision attempts can remain unpersisted until
completion. Source/boundary diagnosis remains required. This rules out only
prior full-suite scenarios consuming this isolated process's deadline.
The full SDK suite and24 terminal-history/routing assertions were not reached.
Topology records Go Frontend/History/Worker, Rust Matching and no Go Matching;
acceptanceEligible:false is retained. Database cleanup output is retained.
A Luna implementation worker now owns the exact retry/delivery boundary.
