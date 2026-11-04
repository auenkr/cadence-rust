# History execution port (`history-port-exec-a`)

Branch from `origin/master`: `history-port-exec-a`.

This is an in-progress port log. No Cargo command has been run on the
development host. Validation is delegated to GitHub Actions.

## Go-to-Rust function/file map

| Go source | Rust location | Coverage/status |
|---|---|---|
| `mutable_state_builder_methods_timer.go` | `src/execution/timer.rs` | Start, replicate start, fire, cancel, cancel-failed, pending lookup, user timer update/delete, fired-event removal. Task generation and exact timer-event index persistence remain dependent on the history task/persistence contracts. |
| `mutable_state_builder_methods_child_workflow.go` | `src/execution/child_workflow.rs` | Pending lookup/delete, initiated/replicated initiated, started/replicated started, terminal cleanup helpers for failed/completed/failed/canceled/timed-out/terminated. Full event constructors, cache writes, domain-cache resolution, and child task generation remain. |
| `mutable_state_builder_methods_cancellation.go` | `src/execution/cancellation.rs` | Workflow cancel request state, external cancel initiation, replication and pending cleanup. Local constructors for all external failure/success transitions and task generation remain. |
| `mutable_state_builder_methods_signal.go` | `src/execution/signal.rs` | Signal-request IDs, workflow signaled count/event, external signal initiation, success/failure pending cleanup. Event constructors and transfer-task generation remain. |
| `mutable_state_builder_methods_completed.go` | `src/execution/workflow_close.rs` | Close status/state for completed, failed, canceled, timed out, terminated, continued-as-new; completion event lookup. Close task generation and full error semantics remain. |
| `mutable_state_builder_methods_started.go` | `src/execution/workflow_started.rs` | Workflow start event and core execution state fields. Continue-as-new start attribute transformation and start task generation remain. |
| `mutable_state_builder_methods_timedout.go` | `src/execution/workflow_close.rs` | Timeout close-state application is shared with other terminal event types. Go timeout constructor/task behavior remains. |
| `mutable_state_builder_methods_decision.go` | `src/mutable_state.rs` / H3a3 worker | Excluded per task instruction; decision Add* work belongs to concurrent decision worker. |
| `mutable_state_builder_methods_activity.go` | `src/mutable_state_activity.rs` | Existing activity module from H3a4; not modified here. |
| `mutable_state_builder_methods_semaphore.go` | — | Not ported. |
| `mutable_state_builder.go` | `src/mutable_state.rs` | Existing foundation; not modified here except module registration was not needed. Transaction lifecycle, buffering, task/checksum buckets remain incomplete in foundation. |
| `mutable_state_decision_task_manager.go` | — | Not ported; decision manager/task generation dependencies are absent from current Rust crate. |
| `mutable_state_task_generator.go` | — | Not ported; task IDs, shard allocation, domain cache, and durable history task integration are not yet available in current crate contracts. |
| `mutable_state_task_refresher.go` | — | Not ported. |
| `checksum.go` (`workflow_checksum`) | — | Not ported; Go checksum payload Thrift encoding and generated checksum schema are not yet represented in Rust. |
| `retry.go` | `src/execution/retry.rs` | Retry eligibility, non-retriable reasons, overflow/cap and injected jitter calculation. |
| `mutable_state_util.go` | `src/execution/mutable_state_util.rs` | Reset-point selection/trimming and child domain fallback helpers. Decision scheduling and typed persistence conversions remain. |
| `history_builder.go` | `src/execution/history_builder.rs` | Event ID/time/version assignment and event buffer. Full Go event-constructor surface remains. |
| `state_builder.go` | `src/execution/state_builder.rs` | Ordered replicated event application for currently ported transitions. State rebuild from persistence, transient batches, and all other event kinds remain. |
| `context.go` | `src/execution/context.rs` | Workflow key identity only. Persistence load/create/update/conflict resolution, locks, and event reapply are not ported. |
| `context_util.go` | — | Not ported; metrics/logging and size-stat emitters need Rust metrics/log interfaces. |

Other non-test execution files from the requested `ls` inventory (cache,
integrity, state rebuilder, timer sequence, workflow, workflow execution util,
workflow repairer, mocks excluded) were not ported in this progress slice.

## Commits

- `7cf547b0a` timer mutable-state transitions
- `ce1adb644` child workflow, signal, and retry transitions
- `83fc7b46b` cancellation, markers, and search attributes
- `f60ad5459` workflow start and child terminal transitions
- `8d5c1725f` execution event replay state builder
- `f266fc031` history builder and execution utility primitives
- `57b1f19c9` expanded transitions and terminal child handlers
- `6329b2407` preserved headers and cancellation identity
- `94deff872` carried external cancellation request identifiers

## CI evidence

- Requested-crate workflow dispatched against head `f266fc031d4a7454368ff6d178a64895c6495f28`:
  https://github.com/auenkr/cadence-rust/actions/runs/41878835114
- At the time of this note, the run was queued. Final green CI on the final
  branch head has not yet been recorded.
- CI against `94deff87212a4d6f596aefc4289c4a5dc351788a` failed to compile:
  https://github.com/auenkr/cadence-rust/actions/runs/41880868975
  The failure was private enum imports (`EventType`, `ParentClosePolicy`) and
  one wrong timer attribute field shorthand. These are corrected in the next
  commit; rerun the requested-crate workflow against that head.
- CI against `03073a2c973ff5549ee0d658fed04dcc1d3ce8b0` compiled, then failed
  Clippy on one long child-start signature and a clone of a `Copy` event type:
  https://github.com/auenkr/cadence-rust/actions/runs/41882590713
  The method now has a narrow `too_many_arguments` allowance matching the Go
  argument shape, and the unnecessary clone is removed.

This note must be updated with the final head SHA, run URL/conclusion, and a
complete parity checklist before the work is considered done.

## Continuation by implementation worker

Worktree: `/mnt/storage/service/wt-exec-sol`; main checkout detached before
attaching the existing branch here. Added `integrity.rs` (both resurrected
entity scans with paginated error propagation), `timer_sequence.rs` (user and
all four activity timer orderings, expiry, mask conversion and generation),
and `workflow.rs` (vector-clock conflict resolution, revive, suppress,
buffered-event flushing and termination). These use explicit context/reader/
task sink traits; concrete context integration remains pending and must not
be represented as completed service parity. No local Cargo commands.

Additional continuation modules: `checksum.rs` implements payload-v1 binary
encoding (including 0x59 preamble) and IEEE CRC32; `mutable_state_task_generator.rs`
and `mutable_state_task_refresher.rs` implement task generation/refresh policy;
`state_rebuilder.rs`, `workflow_repairer.rs`, `context_util.rs`, `semaphore.rs`,
`cache.rs`, and `mutable_state_decision_task_manager.rs` implement their Go
control flow against explicit context/state interfaces. Concrete adapters and
existing partial modules remain incomplete; this is not a parity sign-off.
CI https://github.com/auenkr/cadence-rust/actions/runs/41886354555 at
`74b125584` failed compilation on optional first-decision backoff fields;
fixed by defaulting absent values to zero, matching Go getters.

## Resume checkpoint 2026-11-05

Continued in `/mnt/storage/service/wt-exec-sol` after fetch and worktree verification.
CI https://github.com/auenkr/cadence-rust/actions/runs/41887402910
(head d0b3cbc94) failed: missing failed-decision request ID API field and
non-exhaustive continue-as-new initiator. Reset ID is now explicitly required
from the state adapter, rather than silently dropping it. Unknown initiators
follow the Go invalid-initiator error path.

`context.go` -> `context.rs`: load/checksum-repair/start-transaction loop, history
append, create/update/task-only updates, continue-as-new snapshot orchestration,
conflict resolution, request validation, signal reapplication routing and error
cleanup now have Rust policy implementations. Concrete shard persistence retry,
notification, and mutable-state adapters remain required. This is not complete
execution parity. No local Cargo commands or integration tests run.
