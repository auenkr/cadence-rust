# Active Matching queue / History delivery work

## Queue registry checkpoint (2026-10-15)

Added `matching_registry::QueueRegistry` and QueueHandle. Lease acquisition and
registration share one blocking worker; concurrent requests get one owner.
Append and notification share a worker so RPC cancellation does not skip wakeup.
Ack flush visits all owners despite individual errors. Shutdown stops handles;
unload checks pointer identity so old teardown cannot remove a replacement.
Four registry tests plus seven delivery/History tests passed:
`/tmp/cadence-registry-unload-tests.log` (11 passed). Strict workspace all-target lint passed;
`/tmp/cadence-registry-unload.exit` = 0.
This is an independent lifecycle component, not a running Matching replacement.
RPC integration, configured ack timer, domain cache, ownership checks and idle
unload still remain. Go reference: task_list_registry.go and task_reader.go.

Before AddActivity RPC wiring, port/test a source-domain SQL edge: original
`sql_task_store.go:CreateTasks` writes each row with v.Data.DomainID while fencing
TaskListInfo.DomainID. AddActivity engine supplies SourceDomainUUID as TaskInfo
DomainID. Current Rust create_tasks assumes the queue domain for every row.
Preserve pinned Go behavior; do not silently normalize source to target domain.


## Verified delivery checkpoint (2026-10-15)

All 21 compatibility stages passed using the unchanged original Go server:
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-dispatch-verified/results.json`
(outer exit 0). This includes SQLite/queue Go differential fixtures, strict
workspace clippy, SDK checks, and an 11-event workflow with Rust queue delivery
through actual Go History and completion through Go Frontend. Final cancellation
worker checks passed: 7 focused tests and strict workspace all-target clippy
(`/tmp/cadence-matching-dispatch-last-check.exit` = 0).

The listener still uses the embedded engine. Next: independent Matching queue
registry and RPC integration, then route Go Frontend/History to Rust Matching.
Queries, forwarding, ownership, domain lifecycle, throttling, semaphores, internal
transport and production membership remain. Matching is NOT complete.


Continuation after durable SQLite checkpoint 90e14cfac (pushed).

New matching_queue::DurableTaskQueue owns range ID allocation, bounded read scans,
in-flight claims and ack recovery, expiry and rewrite-before-ack. Six focused
Rust tests and original-Go reader/expiry differential fixtures PASSED:
/tmp/cadence-matching-queue-verified.log, exit 0. Focused persistence clippy also
PASSED /tmp/cadence-matching-queue-clippy.log. Added queue stage to full runner,
and a full final runner is in progress on this source tree.

Go details: maxReadLevel advances even when append fails; do not reuse task IDs.
Reader scans max 10 bounded ranges and signals again after a loaded batch. Expiry
only applies strictly after a positive timestamp (not equality/epoch/negative).
Read-back Go TaskInfo does not retain schedule-to-start timeout; failure rewrite
uses fresh creation time and zero timeout (Go zero-time expiry sentinel), then
acks the original only after successful rewrite. If rewrite persistence fails,
original row stays recoverable. Task GC and ack persistence are separate actions.

New matching_history::MatchingHistoryClient makes real typed History RPCs with
YARPC metadata, per-call timeout (Go default 1s supplied by caller), Go retry
backoff 250ms/1s max/5 retry delays, stable request UUID across retries. It maps
History started responses into matching decision/activity responses and SDK
JSON tokens. Go terminal-error interpretation requires BOTH matching gRPC code
and first Any detail's full Cadence type name; untyped status codes retry. Three
focused retry/metadata/cancellation/classification tests PASSED:
/tmp/cadence-matching-history-unit.log, exit 0. cargo check passed too.

Live Go History test is building/running in tmux cadence-rust:matching-go-history:
/tmp/cadence-matching-delivery-build.log (Rust integration test build), then
/tmp/cadence-matching-go-history.log, artifacts in /tmp/cadence-matching-go-history,
outer /tmp/cadence-matching-go-history.exit. Initial run hit the known Go new-domain
cache race at StartWorkflowExecution. The harness now retries only that exact
provisioning error with the SAME start request ID. Final rerun PASSED:
/tmp/cadence-matching-go-history2/results.json, probe.log and outer exit 0.
Full final regression is running in cadence-rust:matching-delivery-full,
/tmp/cadence-matching-delivery-full.log and artifacts/results.json. It includes
a final original-Go service delivery stage (builds its own Go server).

This uses tests/run-go-service-baseline.py --matching-delivery with original Go
four-role SQLite server. Node creates workflow via Go frontend; Rust ignored
integration test queues a task in its own SQLite DB, calls actual Go History,
acks only on success and returns encoded matching response. Node uses Rust tokens
with Go frontend to schedule/complete activity and workflow. Rust queue/test
process reopens per decision/activity delivery, exercising range/recovery. The
probe checks payload/header/heartbeat, branch token, workflow type/domain and
11-event history. Original Go Matching is still running but receives NO polls
for this task list. This is a real queue/History boundary check, NOT replacement
of the Matching RPC listener or a full mixed-service topology.

No Matching handler or persistence selection is wired to these new components.
Needed next: queue controller/delivery ownership independent of dropped RPC
futures, typed terminal-error handling + inactive-domain refresh, long polls,
sync match, query channels, config/poller state, membership/forwarding/isolation,
rate limiting and semaphores, then actual Go frontend routed to Rust Matching.
Queue claim cancellation/rewrite must be owned by a background controller or guard;
a dropped RPC future must not strand its in-flight claim. Do not silently switch
running default server to this partial engine.

Work is uncommitted after 90e14cfac. Files: matching_queue.rs, matching_history.rs,
matching_delivery.rs integration test, matching-queue fixtures/oracle/runner,
matching-history-compat.cjs, changes to service baseline runner and Cargo exports.
Finish Go-live checks, strict workspace lint and full compatibility runner, record
results, then commit/push. Read root AGENTS.md after compaction.

## Ongoing cancellation worker (not yet fully validated)

matching_dispatch.rs is now exported and focused tests are compiling in
cadence-rust:matching-dispatch-unit, logs /tmp/cadence-matching-dispatch-unit.log
and outer .exit. It owns claims in background delivery workers, so dropping a
poll future triggers rewrite-before-ack. It supports decision/activity delivery,
terminal typed drops and inactive-domain RefreshWorkflowTasks-before-drop.
Tests cover poll abort, retry rewrite/redelivery, terminal drop without rewrite,
and empty long-poll expiration. RESULT PENDING. No RPC listener uses it yet.

matching_history now exports StartErrorKind and refresh_workflow_tasks; this
small addition happened after the full runner's workspace/clippy phase. The
full runner's Go server is still linking before its Go-backed probe. Its result
must not be presented as final verification of the later dispatcher work.
Integration matching_delivery.rs currently uses queue/client directly; after
focused dispatcher tests pass, route that test through TaskDispatcher and rerun
Go-backed delivery to verify the cancellation-safe consumer end-to-end.

## Cancellation worker focused result

Four TaskDispatcher focused tests PASSED, exit 0:
/tmp/cadence-matching-dispatch-unit.log. Poll abort rewrites the claimed row in
background; retryable failure redelivers a new ID; typed terminal errors drop
without rewrite; empty long polls expire. History refresh now follows Go's
special case: typed EntityNotExists is treated as successful refresh, while other
errors (including already-completed) propagate.

Live integration test now uses TaskDispatcher public decision/activity polling,
not direct client calls. Full final regression is running as:
`python3 tests/run-compatibility.py --go-server-bin /tmp/cadence-go-mixed/cadence-server --artifacts-dir /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-dispatch-final`
in cadence-rust:matching-dispatch-final, outer log
/tmp/cadence-matching-dispatch-final.log and .exit. RESULT PENDING.

Previous full runner passed all Rust/differential/gRPC/SDK stages but its final
Go server rebuild stalled in linking while a Rust link competed for IO. Cancelled
only that Go linker; its parent cleaned up, and the runner reports failure of
that BUILD step, not a functional probe. Go source is unchanged since the already
validated /tmp/cadence-go-mixed/cadence-server build. New --go-server-bin reuses
it for boundary tests; fresh Go builds omit DWARF/debug symbols for lower overhead.
Older copied Rust test binaries in /tmp were moved to
/home/ubuntu/.cache/cadence-rust-test-artifacts with symlinks preserving artifact
paths, freeing RAM-backed /tmp space. No active service was moved or stopped.
