# Frontend MySQL gate extras — 2026-11-02

Branch: `t3/frontend-gate-extras`. No local Cargo, Go, rustfmt, builds or runtime gates.

Read the IPv6 and parity audit notes. The requested final-gate note is absent on
master; read its exact contents with `git show origin/fe-final:memory/frontend-final-gate-20261102.md`.

Adds named steps to the existing Python-owned MySQL lane and SDK driver:
- Real gRPC DescribeDomain over `[::1]:<port>` after restarting the Rust Frontend
  with `bindOnIP: ::1`. Only unavailable IPv6 loopback skips, with logged
  `SKIPPED ipv6 unavailable`; RPC/startup failures fail the gate.
- SDK parent waits for an EchoWorkflow child (including its activity) and asserts
  its returned value before completing.
- Real Go scheduler backfills two annual historical fires while the SDK worker
  remains alive; closed visibility and DescribeWorkflowExecution must report
  completed target executions before deleting the schedule.
- DescribeTaskList, ListTaskListPartitions, GetTaskListsByDomain,
  RefreshWorkflowTasks, ResetStickyTaskList, RequestCancelWorkflowExecution and
  TerminateWorkflowExecution assertions through the existing SDK's TChannel client.

CI attempts:
- https://github.com/auenkr/cadence-rust/actions/runs/41420891439 — cancelled as
  superseded, head `fccc99865` (the scheduler spec was corrected before testing).
- https://github.com/auenkr/cadence-rust/actions/runs/41420976855 — in progress,
  head `4792b98880170c63361a478667d86891a8355b9f`. Fresh dedicated runner target;
  binary SHA256 and final conclusion to be recorded after artifact inspection.

No parity row is upgraded before green runtime evidence is available.

First attempt concluded failure (binary
`58e6e895a730199079bfb0f04c9cb5efc371c6177e2a8888370a23963ec1d104`).
IPv6 gRPC (not skipped), child completion, task-list APIs, reset sticky, cancel
and terminate passed. RefreshWorkflowTasks failed `DomainID is empty`: backend
read domainUUID although handler deliberately emits Go's domainUIID. Fixed the
field selector. Populated gRPC visibility exposed nanoseconds being treated as
local-engine milliseconds; production list responses now use an explicit nanos
converter, with regression coverage retaining local-engine milliseconds.

Backfill exposed that current master lacked fe-final's production schedule wiring:
`create_schedule` used the local in-memory execution store. Reused precisely the
two-file schedule implementation from commit `55a6ec215`, leaving newer TLS state
intact. This is existing migration work recovered for the failing check, not a
new scheduler implementation. SDK registered workflow type is
`main.ScheduledWorkflow`; use that exact type for scheduler actions. Go cluster
Frontend rpcAddress now uses explicit IPv4 during the main lane (IPv6 is tested
separately), avoiding localhost resolution ambiguity.
