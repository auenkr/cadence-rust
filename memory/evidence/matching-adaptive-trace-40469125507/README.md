# Adaptive boundary trace, diagnostic run 40469125507

Run https://github.com/auenkr/cadence-rust/actions/runs/40469125507 failed
on immutable diagnostic tag `matching-retry-trace-20261027-v1`, source
`7d072c25422c165c92e6912d4210de7b8bb741a4`. Build exit0, gate exit1.
Rust Matching SHA256:
`c347383fda712d9c1da14c96a63a61116fc2240cf42e2064e207dd1b3f9e42e0`.
This run adds instrumentation to candidate v3 production code; it is diagnostic,
not final service acceptance. Existing budgets and assertions were unchanged.

## Actual result

- Isolated retry completed in37.769s with22.185s suite budget left. Retained
  history contains WorkflowExecutionCompleted and two injected worker panics.
- The full SDK process observed all24 initial load workflows complete, then
  activity-retry, child, workflow-run-retry and workflow-task-retry completed.
  Full-suite workflow-task-retry took23.434s.
- The gate then failed in activity-timeout: its history contains the activity
  timeout and a subsequent DecisionTaskScheduled, but that decision had not
  completed by the SDK wait deadline. The final24-terminal-history checks and
  explicit live-read-partition routing phase were not reached.
- Trace proves retry adds reached Matching and polls returned their task tokens.
  A general claim that retry adds never reached Matching is contradicted here.

## New delivery boundary to investigate

The activity-timeout workflow is
`production-adaptive-scaler-probe-activity-timeout`, run
`227957da-d0c6-4d30-b77f-577edf0d381d`. Its schedule7 task reached partition47
at1793087511132634988 and was enqueued at1793087511964945194. Forward attempts
reached partition2 repeatedly, without a successful token-return trace before
the SDK failure. Trace does not alone establish a Go/Rust divergence.

Scaler samples grew90->383 at qps382.5, then stayed383 at zero measured QPS.
The source audit found Go also uses root-QPS-times-write-count without isolation;
growth counts alone are not proof of a scaler defect. Runtime logs also contain
many partition-config refresh RPC timeout warnings. Compare actual refresh
fanout, concurrency and queue lifecycle with Go before proposing a fix.

Raw downloaded artifacts remain under
`/mnt/storage/service/ci-evidence/40469125507/matching-production-adaptive/`.
This directory retains source/binary identity, actual exits/topology, full SDK
logs, and Matching boundary lines; original acceptanceEligible:false is preserved.

Next independent candidate-v3 lane: Thrift mixed40469948997, running at dispatch.
Only one mixed gate is active at a time.
