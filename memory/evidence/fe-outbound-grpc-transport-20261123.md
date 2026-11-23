# Frontend outbound gRPC transport (system.enableGRPCOutbound) - 2026-11-23

Branch `fe-outbound-grpc-transport`, head b6faf67bea445104090b76012b479b22cd1b1130.

## Implemented
- `rust/crates/cadence-frontend/src/grpc_outbound.rs`: transport selection (default gRPC, TChannel when config selects it),
  gRPC address = TChannel host with the gRPC port (env CADENCE_{HISTORY,MATCHING}_GRPC_PORT, then config grpc_port, then 7834/7835).
- 19 methods over gRPC: History Start, SignalWithStart, Signal, Terminate, RequestCancel, Reset, QueryWorkflow,
  ResetStickyTaskList, RefreshWorkflowTasks, RecordActivityTaskHeartbeat, RespondActivityTask{Completed,Failed,Canceled},
  RespondDecisionTaskFailed; Matching CancelOutstandingPoll, RespondQueryTaskCompleted, DescribeTaskList,
  ListTaskListPartitions, GetTaskListsByDomain. Unmapped methods fall back to TChannel.
- `frontend_remote_grpc::error_from_status_typed`: Go `ToError` parity (AlreadyStarted, ShardOwnershipLost, ServiceBusy,
  LimitExceeded, AlreadyCompleted, QueryFailed, CancellationAlreadyRequested, DomainAlreadyExists, ClientVersion, AccessDenied).
- Root-cause fix: inbound `rpc-service`/`rpc-caller`/... headers are no longer forwarded (Go History rejected
  `cadence-frontend` as service name -> StartWorkflowExecution failures).
- Tests: transport selection, address, supported(), typed error decoder.

## CI
- rust-ci cadence-frontend (format_all): https://github.com/auenkr/cadence-rust/actions/runs/45205208598 success
- frontend-mixed-gate (default grpc outbound): https://github.com/auenkr/cadence-rust/actions/runs/45205201809 success, head b6faf67be

## Remaining
- Not yet over gRPC (still TChannel): History PollMutableState, RespondDecisionTaskCompleted, DescribeWorkflowExecution,
  RatelimitUpdate; Matching PollForDecisionTask/PollForActivityTask.
- No per-method mapping round-trip tests beyond the existing converter tests.
- Late-teardown `CancelOutstandingPoll transport error` warnings appear when matching stops (benign).
