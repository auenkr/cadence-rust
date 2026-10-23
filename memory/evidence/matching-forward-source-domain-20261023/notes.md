# Matching forwarder source domain (activity) — 2026-10-23

## Go reference
- `go/service/matching/tasklist/forwarder.go:154-168` (ForwardTask, activity arm):
  `DomainUUID = fwdr.taskListID.GetDomainID()` (list domain),
  `SourceDomainUUID = task.Event.DomainID` (task's own domain).
- `forwarder.go:139-153` (decision arm): `DomainUUID = task.Event.DomainID`
  (task's own domain), no SourceDomainUUID.
- `go/service/matching/tasklist/forwarder_test.go:110-133` (TestForwardActivityTask):
  asserts `DomainUUID == list domain`, `SourceDomainUUID == taskInfo.DomainID`.
- `go/common/persistence/sql/sql_task_store.go:527-528`: pinned SQL GetTasks sets
  `TaskInfo.DomainID = request.DomainID` (queue domain) on every read. Backlog
  forwards (`task_reader.go:500` → `matcher.go:376`) therefore carry the queue
  domain as the source.
- Go sync-offer forward (`matcher.go:219-222`) uses the in-memory add-request task,
  whose DomainID is the real source domain. Rust has no call site for this path
  (`forward_backlog_task` is called only by the reader and `accept_forwarded_backlog`).

## Rust change
- `rust/crates/cadence-matching/src/matching_dispatch.rs` `ForwardState::forward_backlog_task`:
  new `source_domain: &str` parameter (Go `task.Event.DomainID`).
  - decision: `AddDecisionTaskRequest.domain_id = source_domain` (Go `forwarder.go:142`).
  - activity: `AddActivityTaskRequest.domain_id = domain_id` (list domain, Go `:156`),
    `source_domain_id = source_domain` (Go `:157`).
  - parent Dispatcher arm passes `source_domain` to `accept_forwarded_backlog`.
- `matching_registry.rs` (backlog forward closure) and `accept_forwarded_backlog`
  pass the queue domain for both → persisted-read behaviour is unchanged and
  matches pinned Go SQL GetTasks.
- Test `matching_forward_activity_task_fields_match_go_forwarder`: `#[ignore]` removed,
  caller supplies the source domain.
- Not changed: `AllocatedTask`/`TaskData` (adding a source field would change the
  Go task blob or divergence from SQL reads).

## Open (not fixed here)
- CORRECTED 2026-10-23 (see `memory/matching-sync-forward-trace-20261023.md`): the Go
  sync-offer forward (`matcher.go:219-222`) IS wired in Rust. `sync_match_chain`
  (`grpc_matching.rs:1358`) is called from `add_decision_task` (`:2200`) and
  `add_activity_task` (`:2415`), and its remote branch sets the Go fields
  (`source` = the task's own domain; `SourceDomainUUID` is reachable from RPC). A test pins
  those fields.
- Remaining metric-only gap: a parent ServiceBusy on sync forward does not increment
  `SyncMatchForwardTaskThrottleErrorPerTasklist` (Go `matcher.go:230-231`).

## Evidence (worktree /mnt/storage/service/github/wt/fwdsrc/rust, CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared)
- Prerequisite: `git submodule update --init go/idls` (IDL missing in fresh worktree;
  first attempts exit 101 with "Cadence IDL missing", logs overwritten).
- `cargo-safe test -p cadence-matching` → `/tmp/fwd.exit` = 0
  (442 passed, 0 failed, 2 ignored: unrelated mixed Go/Rust wire gates).
- `cargo-safe clippy -p cadence-matching --all-targets -- -D warnings` → `/tmp/fwd-clippy.exit` = 0.
