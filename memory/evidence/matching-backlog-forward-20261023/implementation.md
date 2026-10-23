# Backlog forward-to-parent implementation (2026-10-23)

Base: origin/matching-candidate-20261023 at 4f5fb6722.
Worktree: /mnt/storage/service/github/wt/backlog2.
Branch: matching-backlog-forward-v3-20261023.
Reference WIP: 6676a9684, 57e2e4dc4, 078f586c8. These were cherry-picked, reviewed and corrected. Imported old evidence files were removed; they are not validation of this branch.

## Behavior

Go oracle: service/matching/tasklist/{task_reader,matcher,forwarder,task_list_manager}.go. In particular matcher.MustOffer's local-first wait, local/send versus outstanding-token selection, two-second child context, token release, local fallback until that same deadline after any error, and Finish(nil) after successful forwarding.

Non-root normal readers now call the parent AddDecisionTask/AddActivityTask path with DbBacklog source and ForwardedFrom. Root forwarded backlog offers wait for a poller; forwarded failures do not create a parent durable row. Successful forwarding completes the child row and invokes the normal GC path. Sync and backlog forward requests share the same outstanding-add generation and dynamic rate limiter. Cancellation drops the in-flight request and releases its token while retaining the unacknowledged child row.

Wire kinds convert internal normal=0 to protobuf normal=1. Remote calls send the existing YARPC gRPC metadata. Source domain for a persisted SQL backlog task is its task-list domain, matching Go sqlTaskStore.GetTasks (sql_task_store.go:527-536); the SQL blob does not retain the original cross-domain source or schedule-to-start duration. Forwarded schedule-to-start timeout stays zero, as both Go SQL GetTasks and NoSQL toTaskInfo leave that field at its zero value; stored expiry remains for reader expiry filtering.

ForwardError distinguishes no-parent, sticky-kind, invalid task-list type, slow-down, and remote status. Slow-down classification decodes the first typed ServiceBusyError detail rather than searching arbitrary status bytes. Parent resolution failure remains a remote error. Remote busy and canceled calls emit forward latency; a locally rejected rate limit does not.

The local rendezvous/forward-token selection withdraws the local offer under its queue lock before forwarding, so a concurrent local claim cannot also be forwarded.

## Tests added

- Reader forwarded success acknowledges its durable row and emits async-forward metrics.
- Local match after slow-down emits the Go local-after-forward-failed metric.
- Zero outstanding-forward tokens leaves dispatch available to a local poller.
- Remote error retains durable row until local match.
- Local head-start prevents forwarding.
- Forward errors retry only after the original two-second child context expires.
- Stop cancels a hanging forward, releases its token and retains the durable row.
- Two-partition RPC test enqueues a durable child task, polls only root, checks delivery/child ack and absence of root persistence.
- Remote transport test checks request fields, protobuf task-list kind, YARPC metadata, typed busy error classification and shared rate consumption.

No Go source changes. This branch's tests do not establish multi-host mixed Go/Rust cluster acceptance or overall Matching completion.
