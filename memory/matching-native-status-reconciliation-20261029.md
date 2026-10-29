# Native Matching task-list status reconciliation (2026-10-29)

## Conclusion

At source pin `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`, the ignored test `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` is not evidence of a current configured-native Matching parity gap. It is a legacy/non-durable `Matching::new(AppState::default())` fixture. The configured native `DescribeTaskList` path unconditionally returns a `TaskIdBlock` and computes `empty` from the native durable queue status. No mismatch was found in that native path.

This conclusion is scoped: it establishes the status fields for the native queue control path and shows the ignored test is about a different fixture. It does not establish equivalent status reporting for any legacy Go-backed/non-durable handler path.

## Source trace at the requested pin

- `rust/crates/cadence-matching/src/matching_rpc_tests.rs:1610-1631` (`git show bf9f3d9:...`): `non_durable_fixture()` builds `Matching::new(AppState::default())`, creates a domain, and does not configure the native queue registry/store. `non_durable_describe` is an RPC test helper.
- `rust/crates/cadence-matching/src/matching_rpc_tests.rs:1807-1821`: the named test is ignored with the stale rationale “non-durable status has no ack manager”; it requests status and expects `empty` and a present task ID block.
- `rust/crates/cadence-matching/src/matching_engine_control_native.rs:109-161`: native `describe_task_list` calls `queue.queue.status()` when status is requested, then constructs `TaskListStatus` with `task_id_block: Some(TaskIdBlock { start_id, end_id })` and `empty` from that tuple.
- `rust/crates/cadence-persistence/src/matching_native_queue.rs:117-127, 241-267`: `DurableTaskQueueBytes` owns an `AckManager`; `open` seeds its ack level from persisted metadata and initializes range state.
- `rust/crates/cadence-persistence/src/matching_native_queue.rs:518-535`: empty scans advance read level and, when no outstanding gap exists, ack level as well. `:537-552` registers delivered tasks as read/in-flight; `:554-565` completes/acks them. `:608-625` persists the ack level; `:697-702` returns read, ack, backlog, current task-ID block and `empty = (ack == max_read_level)`.
- `rust/crates/cadence-matching/src/matching_registry_native.rs:1399-1408` opens `DurableTaskQueueBytes` for native queue ownership; `:363-369` shows the queue handle consumed by native control.
- Older durable-handler RPC coverage is `rust/crates/cadence-matching/src/matching_rpc_tests.rs:1465-1543`, `matching_rpc_durable_describe_tracks_activity_poller_and_queue_status`: it asserts read/ack are zero, `empty` is true, and the task ID block is `[1,100]` for an untouched legacy durable queue. This fixture imports QueueRegistry and SqliteTaskStore, not NativeQueueRegistry / DurableTaskQueueBytes; its passing assertions do not establish native handler execution.

## Go semantics

- `go/service/matching/tasklist/task_list_manager.go:767-805`: `taskListManagerImpl.DescribeTaskList` reads `taskAckManager.GetReadLevel()`, `GetAckLevel()`, and `GetBacklogCount()`, builds the block with `rangeIDToTaskIDBlock(c.db.RangeID(), c.config.RangeSize)`, and defines `Empty` as `GetAckLevel() == taskWriter.GetMaxReadLevel()`.
- `go/common/messaging/ackManager.go:120-154`: getters expose read level, contiguous ack level and backlog count. `ReadItem`/`AckItem` (same file) update those levels as tasks are delivered and completed.
- `go/service/matching/tasklist/task_writer.go:188-190`: `GetMaxReadLevel` returns the writer’s maximum read boundary.
- `go/service/matching/tasklist/task_list_manager.go:983-989`: range ID and configured range size become the task ID block.
- `go/service/matching/tasklist/task_list_manager_test.go:319-450`: active Go status cases assert the untouched queue has the initial block and `Empty: true`; a completed/read case asserts read level 10, ack level 4, backlog 6 and `Empty: false`.

The native implementation expresses the same untouched-empty invariant as `ack == max_read_level`, and reports the native lease range as the block. The cited Go logic clarifies the intended semantics; it does not imply that the Rust non-durable fixture is itself native-configured.

## Same-pin evidence inspected

Evidence directory: `/mnt/storage/service/ci-evidence/matching-native-workspace-40807471166/`.

- `workflow.log` checks out exactly `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` (checkout lines around 100-114).
- Rust test output records the active `matching_rpc_durable_describe_tracks_activity_poller_and_queue_status` as `... ok` (around line 2489).
- The same output explicitly records the target non-durable test as `ignored, parity gap: ...` (around line 2517); it was not executed. Therefore this run corroborates the older durable-handler behavior but provides no executed Rust result for the ignored fixture.
- The Go oracle evidence under `matching-describe-tasklist-go-oracle/` has `source-sha.txt` equal to the pin and `go-oracle.exit` equal to 0. Its `command.txt` runs malformed/optional-payload Go tests, not the status case, so it is not direct Go status test evidence.

No Rust test was run locally for this review. I used only source inspection and already-present pinned CI output; no Cargo, rustfmt, Go, workflow, or production/test edits were made.

## Parent evidence qualification

The production native source constructs both fields, and no source mismatch is proved. However the cited matching_rpc_tests fixture uses the older QueueRegistry/Matching handler; it is not a native RPC regression. The native control test loaded_descriptions_keep_raw_names_and_optional_status checks isolation metrics but does not explicitly assert task_id_block/empty. Preserve this exact assertion gap until a native test or actual mixed status output supplies it. Do not label the legacy fixture as native execution.

## Direct native assertion added

Extended loaded_descriptions_keep_raw_names_and_optional_status to assert untouched read/ack levels0, empty=true and TaskIdBlock[1,100] through NativeMatchingEngine::describe_task_list. This closes the assertion-source gap once CI executes it; it is currently unvalidated and is after the frozen bf9f3d938 candidate. No runtime behavior changed. The original pin remains unchanged; consolidate with the readiness fix before freezing replacement acceptance source. No local Cargo/Go/rustfmt validation ran.
