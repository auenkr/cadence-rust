# History gap: queue exec task rows + archiver client (branch `history-gap-queueexecrows`, 2026-11-07)

## Item 7 (done): full task blobs
- `SqlHistoryTask` (wiring/persistence/queues.rs) now keeps the full decoded `pc::TransferTask` / `pc::TimerTask`
  (`transfer_task()` / `timer_task()`); `queue::deps::PersistenceTask` got a default `as_any()` hook (SqlHistoryTask returns Some).
- `TaskInfoAdapter` (assembly/queue_exec.rs) builds the typed `task::deps::*Task` (by task type, Go field mapping:
  cancel/signal/start-child `initiated_id` = ScheduleID, timer `attempt`/`schedule_attempt` = ScheduleAttempt) and its
  `as_any()` returns the typed task so executors' `downcast_ref::<ActivityTask>()` etc. work. New default method
  `task::deps::PersistenceTask::wrapper_any()` returns the adapter itself (used by the task filter).
- Unknown task types keep `typed = None`, so executors report the unexpected-task error like Go.
- Gap: `OriginalTaskList` / `OriginalTaskListKind` are not on the typed Activity/Decision structs (the `define_ptask!` macro has no such fields), so `get_original_task_list*` return empty (only used for matching tags/metrics).

## Item 6 (client ported, backends are seams): archiver client
- `assembly/archiver_client.rs` ports Go `service/worker/archiver/client.go` (note: the Go client lives under service/worker, not history):
  inline attempt per target (separate history/visibility rate limiters, throttle error), remaining targets signalled to
  `cadence-archival-<rand(numWorkflows)>` with a 300ms timeout and the request-rate limiter. Unit tests included.
- Wired into `CleanupDepsImpl::archive` (returns `HistoryArchivedInline`) and `TransferDepsImpl::archive_visibility`
  (inline=true, visibility time limit) in queue_deps.rs.
- NOT done (TODO(link)): cadence-common archivers are read-only, so `InlineArchivers` (filestore/s3/gcs `Archive`) and
  `ArchivalSignaler` (frontend SignalWithStart + the archival system workflow in the Rust worker) default to explicit errors.
  Archival-enabled domains therefore still retry and never lose history. Rates use Go defaults (not read from dynamic config yet).

## Evidence
