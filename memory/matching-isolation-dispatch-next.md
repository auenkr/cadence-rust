# Matching isolation-aware backlog dispatch (2026-10-16, in progress)

Following verified reporting checkpoint5484f8ac8, implements group-aware durable
reader dispatch. No service role is complete.

## Implemented

- `cadence-common::task_isolation`: Go getIsolationGroupForTask policy +
  taskReader.getDispatchTimeout. Actual Go overlay oracle produces17 selection
  decisions and18 timing outcomes, checked in as matching-isolation.json.
  Initial Go regeneration and Rust fixture comparison passed.
- Reader has one bounded buffer, dispatch head and rendezvous per configured
  nonempty group, plus the shared buffer; independently dispatched groups share
  the queue-owner rate limiter. Selection is reevaluated before admission and
  on each retry. Timeout accounts for ALL buffers and remaining isolation age.
- Known group pollers select their group and shared handoffs; unknown/empty
  pollers only select shared handoffs. Dispatcher supplies lifecycle's group.
- Registry installs snapshot group sets (enable + nonsticky at creation), live
  duration/window/drain/poller/partition checks. Go age cutoff50ms, default
  isolation duration200ms, recency2s. Drained/error/unknown/unpolled/unassigned/
  aged tasks fall back to shared delivery. Sticky always shared.
- Tests: independent group buffers, unknown-group fallback, timeout reselection
  with unchanged durable task ID, policy provider drain/recency/sticky behavior.
- Mixed integration fixture enables configured groups through the existing
  test-only config source. Probe schedules a real Go History activity, queues its
  reference with groupA through Matching, verifies A receives it while B stays
  waiting/cancels, then completes it through Go Frontend.
- Combined runner adds matching-isolation-differential (now30 stages).

## Validation

Initial85 server tests passed before final timeout/provider additions. Clippy
found a complex selector type; resolved via IsolationSelector alias.
Full runner running in tmux cadence-rust:isolation-full:
`/tmp/opencode/isolation-dispatch-full.log` + `.exit`, artifact dir
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-isolation-dispatch-full`.
Additional provider test was added during initial workspace build; confirm it
appears in workspace-tests.log or run fresh focused checks/full runner.

## Remaining

- Live isolation enable toggle should unload/recreate owners like Go
  shouldReload. Not implemented yet.
- Production global drain state is still the existing in-memory admin state,
  not a configstore watcher. Production domain metadata/config loading is open.
- Partition assignment uses existing executions.partition_config metadata; full
  native persisted partition version/config/cache/scaling still open.
- Sync matching and forwarding, passive/query-only behavior, membership,
  standalone lifecycle, semaphores still open.
- Existing newInternalTask partition metadata rewrite/metrics not ported here.

Keep all behavioral limitations explicit; this is not isolation gate completion.

## Mixed-service wire discovery

First full run: workspace tests (87 server tests), strict lint and29/30 stages
passed; mixed probe failed at the first Describe through Go Frontend:
protobufjs "Cannot read properties of null (reading newTasksPerSecond)".
Reason: prost omits map-message value fields equal to Default. Pinned Go gogo
decoder leaves such values nil, reencodes nil, protobufjs rejects the null object.
Go itself emits explicit empty submessages for nonnil zero metrics.

Fix: custom `cadence-proto::task_list_status::TaskListStatus` wire implementation,
selected via tonic-build extern_path and reexported at the same API path. Encodes
metric entries with optional Some(message), forcing tag2 length0 for zero
metrics. Other fields/tags unchanged, map duplicate/default decoding supported.
Unit exact-byte and roundtrip test passed. No upstream IDL/generated edits.
Second initial build exposed missing api::TaskListStatus reexport; fixed.

Fresh full runner started: `/tmp/opencode/isolation-wire-full.log` / `.exit`,
artifacts `.../matching-isolation-dispatch-wire-full`. Must confirm full pass.

That run stopped on strict clone-on-copy lint; fixed. Latest run is
`/tmp/opencode/isolation-final.log` / `.exit`, artifacts
`.../matching-isolation-dispatch-final`. Current frozen workspace separately
passed401 tests (87 server tests), both wire regression tests, and workspace
all-target Clippy with -D warnings. Full runner now in end-to-end stages. No
runtime code changes during this run; added second wire regression test only.

Final outcome: all30 stages PASSED; `/tmp/opencode/isolation-final.exit=0`.
Mixed probe explicitly reports isolationDispatch passed, and existing Go SDK
passed. Separately frozen workspace401 tests and strict all-target Clippy pass.

## Follow-up: isolation enable-toggle owner reload

Committed backlog checkpoint:63f374479. Next increment installs should_reload
closure on the reader, comparing live domain-scoped enable config to the owner
snapshot. AddDecision/AddActivity stop the owner before writing and return
shutdown error; decision/activity GetTask stop the owner and return empty poll,
as Go AddTask/GetTask do. Normal stop waker releases all reader loops and
blocked polls; next lookup cleans up the stopped owner and acquires a new lease.
Changes to the configured group list alone do not reload Go owners, so do not
compare the live list for reload.

Three focused policy/reload tests passed, including toggles both directions,
durable row retention across owner replacement, empty decision/activity polls.
Fresh full runner in tmux cadence-rust:reload-full:
`/tmp/opencode/isolation-reload-full.log` / `.exit`, artifacts
`.../matching-isolation-reload-full`. Still needs final outcome recorded.

Reload final: all30 stages PASSED, exit marker0;403 workspace tests incl89
server tests and strict all-target lint pass. Go SDK and mixed probe passed.
