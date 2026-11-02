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
- https://github.com/auenkr/cadence-rust/actions/runs/41420976855 — failure on head `4792b98880170c63361a478667d86891a8355b9f`; exposed RefreshWorkflowTasks domain-field, timestamp-unit and missing production schedule wiring defects, all fixed in later commits.



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

Final validation on source head `63412d6281edbeff0755f947ce9ac39807d523c7`:
- Clean uncached workspace CI: https://github.com/auenkr/cadence-rust/actions/runs/41422827735 — success. Fresh dedicated target, clippy then workspace tests, formatting check passed.
- Mixed MySQL gate: https://github.com/auenkr/cadence-rust/actions/runs/41422833412 — success. Rust Frontend and Matching with Go History/Worker, no Go Frontend; 46/46 named JSON assertions passed. Retained exact evidence in `memory/frontend-gate-extras-41422833412-steps.json`.
- Mixed gate Rust binary SHA256: `7264f455fdc361ae3fdf9b5de73fc2f99990a288c8f4df52489b19d4c148f2cc` (fresh CI target). Real IPv6 gRPC ran successfully; child SDK workflow and activity completed; schedule backfill executions were visible as completed; requested task-list, refresh, cancel, terminate and sticky reset APIs passed.
- That run’s formatting patch was committed and passed the final-head workspace format/clippy/test gate below.

Final-head evidence:
- Fresh uncached workspace CI (same final head `5644052674ad0294bb10662a8fe8c67446193e3b`): https://github.com/auenkr/cadence-rust/actions/runs/41424261789 — success (formatting, workspace clippy then tests).
- Final-head MySQL mixed gate: https://github.com/auenkr/cadence-rust/actions/runs/41427827989 — **success**, head `5644052674ad0294bb10662a8fe8c67446193e3b`, `rust_ref=t3/frontend-gate-extras`, 51/51 named steps true. Rust binary SHA256 `e6cd5037ffb45cf4897c81a5d40d005207e12b6e9f149de7b4dc67efe803db85` from fresh `/tmp/cadence-cargo-target-fe-41427827989`. Exact steps retained in `frontend-gate-extras-41427827989-steps.json`. IPv6 gRPC, child workflow completion, backfill run closure and requested API checks all passed.

The two intervening mixed attempts used the default Rust ref `fe-integ` by mistake;
their captured commit files identify source head `115f5b4b56969603648a828f01758ad1e73bdf70`,
not this gate branch. They are retained as dispatch evidence and are superseded by
the correctly dispatched final-head run 41427827989.
- Run 41424268026 — failure, binary SHA256 `d068f36e208320f9a677fe3b7c4770dcbe9ae2d6f8f3aeecab2ebc34c05b2781`, 25/34 checks.
- Run 41425876411 — failure, binary SHA256 `1619092278254401211139b4ad7e79b64b5f9b0e2329772880985d48076bf494`, 25/34 checks.
- Their exact step arrays remain in `frontend-gate-extras-41424268026-steps.json` and `frontend-gate-extras-41425876411-steps.json`.
