# PostgreSQL acceptance: reproducible failure

Run https://github.com/auenkr/cadence-rust/actions/runs/40083288791 failed for
source 9d56f05679395aae90dc0c3161668defcd44208b. Fresh CI target, binary SHA256
in binary.sha256. Both Go baseline and Rust build completed; mixed gate failed.
This repeats run 40080485161's pre-crash queued-row failure, before any crash.

History diagnostic has WorkflowExecutionStarted and DecisionTaskScheduled only;
no DecisionTaskStarted. DescribeTaskList reports no pollers, backlog=0,
readLevel=ackLevel=100000, block=200001..300000. Observer returned queued_rows=0,
range_id=2 throughout 20s. This does not establish where dispatch stalled or
whether a persistence error occurred. Next investigation must inspect Go
transfer scheduling and Rust AddDecisionTask ingestion/ownership, not weaken the
queued-before-crash assertion. Full transient artifacts are downloaded under
/tmp/matching-production-40083288791. No local Cargo command ran.
