# History decision parity (history-gap-decision3), 2026-11-06
Branch history-gap-decision3, code head ba65e607c.
CI (rust-ci, workflow_dispatch): cadence-history success https://github.com/auenkr/cadence-rust/actions/runs/42220071139 ; cadence-api success https://github.com/auenkr/cadence-rust/actions/runs/42221059615 (head ba65e607c).
Fixed: typed errors (BadRequest/Internal/EntityNotExists/EventAlreadyStarted/AlreadyCompleted/Other), ContinueAsNew ASCP inheritance + domain lookup, start-child check order, cancel-timer validation, handler error text, cron parentDomainName/ASCP/last completion result, AcquireSemaphore decision (api/thrift/proto).
Remaining gaps: ASCP not stored on WorkflowExecutionStartedEventAttributes (passed as parameter instead); thrift/proto converter crates only validated via cadence-history/api dependency builds; handler.rs lines ~172 and token errors stay BadRequest.
