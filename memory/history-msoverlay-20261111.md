# History changed-record mutation overlay — 2026-11-11

## Done

- Added explicit typed mutation to persistence-request conversion and typed snapshot conversion in `rust/crates/cadence-history/src/execution/mutable_state_bridge.rs`.
- Converts activity, timer, child, request-cancel and signal keyed upserts/deletes; signal-request IDs, workflow request rows, tasks, versions and buffered-event mutation state are carried as well.
- Changed records merge recognized fields into the prior Thrift struct, preserving unknown fields. Existing activity scheduled/started event blobs and encodings, task-list kind and failure category are retained from the loaded record.
- Maps typed timer `task_status` to persistence `task_id`; activity heartbeat details/time are overlaid separately while untouched heartbeat rows remain from the snapshot.
- Added record overlay/reload coverage for all five families, unknown activity fields, persistence-only activity fields and timer task ID mapping.
- Reviewed actual Go `CloseTransactionAsMutation` and Rust `SqlWorkflowStore::{apply_snapshot_tx,apply_mutation_tx,maps}`: snapshots reset keyed maps; SQL mutations apply keyed deletes/upserts, and SQL ignores the optional rewrite fields. Workflow request rows remain part of the mutation contract.

## Gaps / semantics

- `history_contract::WorkflowMutation` currently represents rewrite rows as `Vec`, so it cannot preserve Go's nil-versus-non-nil empty rewrite distinction. The Rust SQL mutation writer does not consume rewrite vectors and applies keyed deletes/upserts instead. A final-key deletion is represented explicitly as a keyed delete. Backends that implement rewrite sampling need a future presence-capable contract field before Go rewrite sampling can be reproduced.
- Typed activity records do not contain the original scheduled/started event encodings/blobs or task-list kind/failure category. Existing rows are preserved from the loaded envelope; newly created activities consequently have empty persistence-only fields until the typed model/creation path supplies them.
- No mixed-cluster service gate is claimed by this persistence-boundary change.

## Validation evidence

- Local checks: `git diff --check` only. No Cargo, rustfmt or Go command was run locally.
- CI run URL: pending.
- Code head SHA: pending.
- Conclusion: pending.
