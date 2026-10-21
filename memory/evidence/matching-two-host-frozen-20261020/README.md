# Frozen two-host Matching recovery run

Command (from the `matching-dns-provider` worktree, inside tmux session `cadence-rust`):

```sh
NODE_PATH=/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules \
python3 tests/run-matching-production-failure-recovery.py \
  --rust-production-binary "$PWD/artifacts/frozen-production/cadence-server-679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa" \
  --artifacts-dir "$PWD/memory/evidence/matching-two-host-frozen-20261020/run"
```

The frozen binary SHA-256 is `679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa`. The Go Frontend, History and Worker ran with two Rust Matching members; no Go Matching process was started. The runner used gRPC for Matching-to-History and an idle check interval of three seconds. It compiled only the Go baseline server.

The run exited 1 in the later isolation compatibility section. After successful recovery and restart assertions, its direct Matching `AddActivityTask` call received `ABORTED` for task list `isolated-4752eac5-8b3f-40e8-ae93-9f4abc6b88e7`, with owner `127.0.0.1:60123` and responding host `127.0.0.1:33281`. See `run/rust-matching/production-failure-recovery-checkpoints.json` and `run/rust-matching/production-typed-ownership-go.log`.

The failure was initially mistaken for Go Frontend retry exhaustion. Inspection of the stack and harness showed it was instead a direct test call: the isolation section replaces its client with a direct primary Matching gRPC client, then sends `AddActivityTask` for an arbitrary task-list name which Ringpop assigned to the secondary. This path bypasses Go Frontend routing. The dedicated public non-owner and Go typed-error checks had already passed. The harness now selects a task-list name whose owner agrees across both Rust Ringpop views and targets that owner for its direct isolation-only calls; the SDK and workflow calls remain through Go Frontend. This correction is pending rerun evidence.

Passed checkpoints before that later failure:

- Both Rust Matching instances returned the same Ringpop ownership map for all 64 probes, including keys assigned to both instances.
- Production idle eviction released the long poll, and the existing Go SDK completed its recovery workflow exactly once.
- A request sent directly to the non-owner returned `ABORTED`; the pinned Go Matching oracle decoded the typed ownership error and verified owner, self and task-list fields. Its output is `run/rust-matching/production-typed-ownership-go.log`.
- The owner-crash recovery SDK workflow completed once; the persisted range advanced from 1 to 2, acknowledgement advanced from 0 to 1, and queued rows returned to zero. These details are in the checkpoint JSON.

The revised harness cleanup attempted both Rust Matching children independently. The outer runner stopped the Go services. A post-run process query for the Go baseline runner, compatibility probe, cadence-server services and Go typed oracle returned no processes; this was recorded in `process-cleanup.txt`. Full output is in `run.log`, `run/probe.log` and the service logs. `exit` records the expected failing gate status `1`.

## Follow-up owner and range diagnostics

The harness fixes are committed on the active worktree branch through `9a2f08487`: resolve the isolation task list against both Rust membership views, retry while a restarted member's direct gRPC listener becomes available, use a full host-and-port for the selected direct client, validate multi-owner task ID block alignment and the persisted range lease, and retain the exact first `1..100000` expectation for a single-owner run. Poll failures are converted to handled outcomes so they do not bypass cleanup.

Three fresh reruns narrowed the remaining issues. `owner-rerun2/` passed the two-view ownership lookup, direct non-owner typed Go oracle, idle eviction, SDK recovery, owner crash recovery, and persisted range/ack checks. It then observed `startId=100001` for a new generic workflow queue. That is a valid second lease: Go `rangeIDToTaskIDBlock` maps range 2 to `100001..200000`, while SQL `LeaseTaskList` increments the range ID. The revised check preserves the 100000 block size and alignment, and verifies the reported end ID exactly matches the persisted `task_lists.range_id`.

`owner-rerun3/` used that range validation and reached the isolated direct Matching call, but failed because the selected raw gRPC port was passed without its host to `MatchingAPI`. Commit `da6e3d2c0` fixes the test address and asserts the full host:port shape. That run's artifacts are outside this directory at `/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-interop-20261020/matching-two-host-frozen-owner-rerun3/`.

`owner-rerun4/` used frozen binary `679e0b…4254aa` plus the same already-built Go baseline server, with no Rust build. It passed the direct typed Go ownership oracle, idle + SDK recovery, owner-crash + SDK recovery, ownership checks and the reported-versus-persisted task ID range check. It then failed because the Go Frontend `DescribeTaskList` response lacked recent poller `service-probe` immediately after returning that workflow's decision task. The test has not yet identified which Matching host handled the poll and which manager the Frontend queried. Rust `cadence_common::poller::Manager::end` retains a completed poller in its five-minute history, and Go `poller.manager.EndPoll` refreshes its history cache similarly; therefore this remains an owner/routing diagnosis, not evidence that the retention implementation is absent. Commit `9a2f08487` records both Rust members' direct owner view and direct task-list descriptions when this assertion fails. That diagnostic has not yet run.

The rerun4 result is in `owner-rerun4/run/probe.log` and its `exit` file. The post-run process audit found no cadence services, Go runner, compatibility probe or typed oracle processes. The runner reused Go executable SHA-256 `d0d0bbada5bf18c87111144c482265b47d159f978d35e0cda733625c60eed543`; the frozen Rust binary checksum remains in `frozen-binary.sha256`.

## Source-only poller diagnosis and parity recheck

The failed assertion is after `PollForDecisionTask` has already returned the
started workflow decision task to Go Frontend. The next `DescribeTaskList` goes
through Go Frontend, which routes through its Matching client; the assertion
does not pin that call to the host that served the poll. The Rust `Manager::end`
and Go `poller.manager.EndPoll` both move a completed poller's identity into a
five-minute history cache, but each cache belongs to its task-list manager.
Consequently, the current evidence does not prove whether this is an owner
selection/manager-lifetime issue or a Rust history retention defect.

The already-committed diagnostic at `9a2f08487` writes
`production-poller-owner-diagnostics.json` on this assertion failure. It
records the public Frontend poller list and task ID block, then asks both live
Rust gRPC endpoints for their reported owner and direct task-list poller list.
The artifact is not present in rerun4 because that diagnostic was added after
that run. Do not weaken the assertion. If only one direct manager reports
`service-probe`, fix the harness to pin its retention check to that owner while
keeping the public path assertion as a separate routing check. If neither does,
trace whether the workflow poll request landed on another manager instance or
whether the Rust history manager lost the entry while Go's matching owner
retained it before changing implementation.

Current Go-source comparison also corrects the previous domain callback note:
`engine.go:1707-1785` uses `ManagersByDomainID`, which enumerates loaded
managers, then releases normal decision/activity and sticky decision pollers
for newly notified global non-active-active domains. It does not enumerate
persisted task-list rows or create managers. Rust now tracks the global
notification watermark in the cache callback and releases the same loaded
manager kinds on eligible updates. Ordinary config changes do not trigger
release or generic manager unload.

The source adds a two-host partition regression: add/remove a remote read
partition, verify the remote manager receives both changes, send an older
version and verify it is ignored, verify write-only IDs do not create managers,
then move ownership and verify the new owner receives the update. The Go source
notifies the union of old/new read IDs only; Rust now follows that behavior and
keeps errors best-effort. The regression is queued, not run. The poller harness
now resolves the task-list owner, checks `service-probe` directly on that same
owner, records the direct and Frontend identities, and retains the separate
public Frontend assertion. No Cargo, Go, cluster, or Docker process was run.
