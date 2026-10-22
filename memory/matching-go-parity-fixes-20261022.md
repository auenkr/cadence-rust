# Matching Go-parity fixes: items 4, 5, 6, 8 (2026-10-22)

Implements the Rust changes in `memory/matching-audit-verification-20261022.md`
(from `origin/audit/matching-verify-20261022`) on top of
`origin/t3/matching-integration-20261021` (c32a31e91). Branch
`matching-go-parity-fixes-20261022`, commit `da1c307b2`.

Note: the audit's line numbers are for `a0953f121`; the base is ~214 commits
later, so every site was re-located by content, not by line.

## Changes

- **Item 4 (CancelOutstandingPoll)**
  - `cancel_outstanding_poll` returns empty OK when no poll matched; runs
    `durable_queue` (owner check + load) when the durable backend is active.
  - `register_poll` no longer cancels an earlier poll with the same poller ID
    (Go `poller/manager.go` StartPoll overwrites).
  - `PollGuard::drop` removes the key unconditionally (Go EndPoll quirk).
  - Cancelled decision/activity polls return an empty response with
    `auto_config_hint` instead of `Status::cancelled`.
- **Item 5 (QueryWorkflow with no poller)**
  - Root and sticky lists no longer fail fast with `cannot find parent`; they
    wait on the local query channel until the deadline (existing
    `force_local` path).
  - Deadline elapse returns `Internal("context deadline exceeded")`.
  - Non-root deadline-before-forward returns the same Internal.
  - Test `matching_rpc_query_without_poller_fails_fast` replaced by
    `matching_rpc_query_without_poller_waits_for_deadline`.
- **Item 6 (ForwarderMaxChildrenPerNode)**
  - `.max(1)` at the three read sites (forward-degree provider, add/poll
    path, query path). `degree <= 0` disable branches removed.
  - `matching_rpc_forwarding_disabled_keeps_parent_backlog_untouched` no longer
    covers `forwarderMaxChildrenPerNode=0` (now clamps to 1, forwards); new
    `matching_rpc_forwarder_max_children_below_one_clamps_to_one`.
- **Item 8 (error codes)**
  - Shutdown `task list shutting down` → `Internal` (grpc_matching add paths,
    `matching_writer::stopped`).
  - Writer non-sticky `ConditionFailed` and other raw errors → `Internal`;
    sticky branch unchanged. `retryable` flags unchanged.
  - Writer ctx deadline → `Internal` (message unchanged).
  - `queue_status`: SQL `Backend{kind: Throttled}` → `ResourceExhausted` +
    `ServiceBusyError` detail; everything else → `Internal`.

## Tests added

- `matching_rpc_cancel_without_matching_poll_returns_ok`
- `matching_rpc_duplicate_poller_id_replaces_entry_without_cancelling_first`
- `matching_rpc_cancel_outstanding_poll_checks_task_list_ownership`
- `matching_rpc_forwarder_max_children_below_one_clamps_to_one`
- `matching_rpc_query_without_poller_waits_for_deadline`
- `queue_status_maps_throttling_to_service_busy_and_other_errors_to_internal`
- Updated expectations: durable cancel, cancelled-poll tests, writer
  DeadlineExceeded/Unavailable → Internal, isolation-toggle add → Internal.

## Verification status

**Not yet verified at commit time.** Host has 2 CPUs and another worktree's
`cargo test -p cadence-server` (t3-4dab8e54) ran for 1h30m+, so the run was
queued. See the exit-code log in the final report of the implementing agent.

## Not covered (remaining audit items)

- `grpc_matching.rs` poll-level `Matching poll deadline exceeded.` (audit item 8
  candidate, unverified in Go).
- `matching_dispatch.rs` `Matching delivery worker stopped.` (unverified).
- `matching_partition.rs` and `matching_registry.rs` `Status::unavailable(error)`
  sites for storage errors (not in audit scope; likely the same Go
  `handleErr` Internal mapping, not checked).
- Query `force_local` queues without a waiting poller (audit item 5.4): unchanged.
- `poll_expiration` fallback for missing query deadline (audit item 5.5): kept.
- Frontend mapping of these codes not inspected.
