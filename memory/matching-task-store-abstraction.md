# Matching task-store abstraction source slice

Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction`
Branch: `codex/matching-task-store-abstraction`, based on `c6cbdfb6e`.
This is a source-only slice. No build or test was run.

The new persistence trait is `cadence_persistence::matching_task_store::MatchingTaskStore`. It covers the operations called by `DurableTaskQueue` and is implemented by `SqliteTaskStore`. The concrete SQLite inherent methods remain available, preserving existing external callers and SQLite error variants. `DurableTaskQueue` stores `Arc<dyn MatchingTaskStore>`; existing `Arc<SqliteTaskStore>` call sites coerce at the queue constructor boundary. Matching writer and reader consume the queue interface and do not need direct storage types.

The trait deliberately preserves these contracts already implemented by SQLite and consumed by the queue:

- Lease returns a task-list record and uses the range fence; task-list updates are fenced by the acquired range.
- Batch creation receives source domains per task while fencing the target task list. Inserts and range verification remain one backend transaction.
- Reads return the ordered Go interval `(read_level, max_read_level]`, bounded by signed `batch_size`. Expired rows remain visible to the queue reader, which owns expiration behavior.
- Size counts task IDs strictly above the ack level; single deletion is not lease-fenced; bounded GC deletes IDs at or below the inclusive ack ID; task-list deletion checks the expected range.

The trait currently returns `matching_sqlite::Result` to preserve existing queue error behavior and avoid adding a speculative error variant before a driver adapter exists. Before implementing SQL drivers, normalize backend failures into a store-level error type while re-exporting legacy `matching_sqlite` names for compatibility. Do not collapse conditional lease errors into transient/database errors.

The runtime/registry constructor types remain SQLite-specific in this checkpoint by request from the runtime worker, who is freezing that candidate for production validation. This does not block the persistence abstraction: current queue construction accepts concrete SQLite `Arc` values through trait-object coercion. Generalizing runtime selection and registry ownership is the next coordinated integration slice before a MySQL/PostgreSQL adapter can be wired into production.
