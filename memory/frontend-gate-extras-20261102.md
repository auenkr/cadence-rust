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

Final validation on source head `63412d6281edbeff0755f947ce9ac39807d523c7`:
- Clean uncached workspace CI: https://github.com/auenkr/cadence-rust/actions/runs/41422827735 — success. Fresh dedicated target, clippy then workspace tests, formatting check passed.
- Mixed MySQL gate: https://github.com/auenkr/cadence-rust/actions/runs/41422833412 — success. Rust Frontend and Matching with Go History/Worker, no Go Frontend; 46/46 named JSON assertions passed. Retained exact evidence in `memory/frontend-gate-extras-41422833412-steps.json`.
- Mixed gate Rust binary SHA256: `7264f455fdc361ae3fdf9b5de73fc2f99990a288c8f4df52489b19d4c148f2cc` (fresh CI target). Real IPv6 gRPC ran successfully; child SDK workflow and activity completed; schedule backfill executions were visible as completed; requested task-list, refresh, cancel, terminate and sticky reset APIs passed.
- CI formatting artifact from run 41422827735 is applied as a formatting-only patch in `workflow_backend.rs` and `cadence-proto/src/convert/mod.rs`; this patch remains to be committed and final-head CI rerun.

Final-commit validation follow-up (head `d08287ffecebbd7b4f09ddeeadfaa54a2f464702`):
- Fresh uncached full workspace CI https://github.com/auenkr/cadence-rust/actions/runs/41424261789 — success (clippy, formatting, workspace tests).
- Mixed run https://github.com/auenkr/cadence-rust/actions/runs/41424268026 — failure. Binary SHA256 `d068f36e208320f9a677fe3b7c4770dcbe9ae2d6f8f3aeecab2ebc34c05b2781`. 25/34 gate assertions passed. SDK workflow starts failed because Go History returned EntityNotExists for the registered domain UUID; schedule create/backfill RPCs passed but completion polling hit the same domain error. The IPv6 gRPC probe was refused. Captured `rust.yaml` had `bindOnIP: "::1"`; runtime log said the restarted Frontend listened on `0.0.0.0`.
- Retry https://github.com/auenkr/cadence-rust/actions/runs/41425876411 — same failure pattern. Binary SHA256 `1619092278254401211139b4ad7e79b64b5f9b0e2329772880985d48076bf494`; 25/34 assertions passed. Exact step arrays retained in `memory/frontend-gate-extras-41424268026-steps.json` and `memory/frontend-gate-extras-41425876411-steps.json`.
- The earlier source-head mixed run 41422833412 was green (46/46), with binary SHA256 `7264f455fdc361ae3fdf9b5de73fc2f99990a288c8f4df52489b19d4c148f2cc`. Final-head runtime regressions are not explained by the formatting-only Rust changes; do not claim final-head mixed validation as green. Specific unresolved evidence is domain metadata visibility to Go History during SDK starts and Frontend IPv6 bind selection on restart. No local Cargo, Rust, Go, or gate commands were run.
