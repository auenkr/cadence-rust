# Frontend mixed gate (2026-11-01)

Workflow: `.github/workflows/frontend-mixed-gate.yml` (dispatch with `-f rust_ref=<ref>`), harness
`tests/gates/run-frontend-gate.py` + `tests/gates/run-frontend-mysql-lane.sh`, SDK driver `tests/frontend-gate/`.
Topology: Go History+Worker (one Go process, `--services history,worker`), Rust Matching process and Rust Frontend
process (both `cadence-server start --services ...`, fresh CI target dir, SHA256 in artifact `binary.sha256`), MySQL 8.0.36,
no Go Frontend/Matching. Steps: CLI domain register/describe over TChannel and gRPC, Go SDK echo (start/complete/describe-closed),
signal/query/describe-open/list-open workflow, SIGKILL+restart of the Rust Frontend mid-workflow, list-closed, CLI list.

## Runs (gate head 57ec76d1d)
- 41286555561 / 41286805977: harness bugs (IDL submodule missing for rustsrc checkout) then fe-integ failed to build (`Uuid::now_v7` missing, cadence-service-core, since fixed on fe-integ).
- 41287057648, 41288545434: rust_ref=master (head 57ec76d1d), conclusion failure (gate steps below). Binary sha256 6ef0ad96...39f.
- https://github.com/auenkr/cadence-rust/actions/runs/41289546603: rust_ref=fe-integ (head 22a7a159a), conclusion failure.
  Binary sha256 1768bf11ed6b5320565e0d30a49a02467886f50356053b07ba1c632feb28a4e3. Same step results as master.

## Passing (Rust Frontend)
Listens on both TChannel and gRPC; CLI `domain register` and `domain describe` over tchannel and grpc; SDK DescribeDomain,
worker start, StartWorkflow, echo workflow completes, DescribeWorkflowExecution on a closed run, Signal accepted,
ListClosedWorkflow, CLI `workflow list --open` over gRPC and TChannel (exit 0), Frontend SIGKILL + restart listens again.

## Real gaps (failing steps, exact text)
1. `QueryWorkflow` returns nil result and nil error (SDK `client.QueryWorkflow`). The SDK worker logs repeatedly
   `Failed to process decision task ... "error":"invalid query decision task"` (30 lines in gate.log): the decision/query task the
   Rust path delivers is not a valid query task for the Go SDK.
2. `DescribeWorkflowExecution` of the still-open signal/query workflow reports a close status ("open workflow reports closed").
3. `ListOpenWorkflowExecutions` never lists the open execution within 30s (list-open).
4. After the Frontend restart: SignalWorkflow fails `WorkflowExecutionAlreadyCompletedError{Message: Workflow execution already completed.}`
   (the workflow had already closed, by failure 1/2 chain) and the run ends with `TimeoutType: START_TO_CLOSE`; so restart recovery
   is not proven yet (the in-flight workflow did not survive to the restart point).
5. Ringpop: Rust Matching logs `Matching Ringpop JOIN attempt N completed: successes=[] failures=["127.0.0.1:<rust frontend tchannel port>"]`
   (all ten attempts) and Go History died with `failed to start peer provider ... bootstrap ringpop: join duration of 32.27s exceeded max 30s`
   (logs/go-history-worker.log line ~161). The Rust Frontend does not answer Ringpop join on its TChannel port.
Artifact `frontend-mixed-gate` (3-day retention) has gate.log, steps.json, per-process logs under logs/.

## Driver lifetime protection

Source audit after master sync: SDK main has a ten-minute request context, but the Python harness waited indefinitely for driver exit and did not register that subprocess for its final cleanup. Added `--sdk-timeout-seconds` (positive integer, default 900), an explicit failed timeout step and driver process-group termination; the driver is now tracked alongside services for exception cleanup. The outer limit allows SDK context expiry and shutdown before diagnosing a hang. This changes only gate supervision, not service behavior. Verified Python syntax using ast.parse and `git diff --check`; no local Go/Cargo or cluster run. Actual mixed execution remains required on Actions. The already-live gate 41327590981 predates this change and was left running.

## Integrated baseline result: 41327590981 FAILED

Run: https://github.com/auenkr/cadence-rust/actions/runs/41327590981. Artifact downloaded to /tmp/frontend-mixed-41327590981. Both Rust source and gate head were b967e1442cfc431c1d8253df17044755febeb685. Fresh Rust binary SHA256: `663b7defbd17249b9a500f0303d7d63126df8a245c61f054ac6447b153b87691`.

Domain CLI operations over both transports, SDK echo completion, describe-closed, signal acceptance and listening after Frontend restart passed individual assertions. These do not establish mixed compatibility: Rust Matching failed Ringpop bootstrap against the Frontend seed (ten unsuccessful JOIN attempts; 35.98s exceeded max 30s); Go History exited fatally after 30.81s bootstrap timeout at 16:48:07 UTC. This gate predates the later service-survival assertions, so its reported SDK successes cannot be used as proof of interaction with healthy Go History and Rust Matching.

Failed assertions: query returned nil result and nil error; describe-open reported closed; list-open could not find execution within 30 seconds; signal-after-restart and signal-done returned WorkflowExecutionAlreadyCompletedError; completion after restart returned START_TO_CLOSE timeout; SDK driver exit 1. The prior query/open/restart defects persist in this baseline. Fix Frontend membership startup, then rerun the current harness with service-survival assertions and investigate remaining workflow failures against healthy services. No new mixed run was dispatched while the Ringpop implementation remains unintegrated.
