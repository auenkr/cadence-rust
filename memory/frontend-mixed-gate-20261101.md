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
