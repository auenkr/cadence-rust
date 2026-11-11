# History transaction prepare / commit — 2026-11-11

Branch: `history-mstxn`

## Done

- Added policy-aware `TypedMutableState::prepare_close_mutation` and `prepare_close_snapshot`. They prepare from a clone, so append/encoding failures leave source event buffers, generated tasks, and `next_event_id_in_db` available for retry.
- Active preparation flushes buffered events and generates activity/user timer tasks with the supplied time. Passive preparation does not flush buffered events or run those active generators.
- Kept `next_event_id_in_db` as the mutation condition independently from `execution_info.next_event_id`.
- Added `TransactionState` for `OwnedNdcMutableState`, converting prepared typed mutations/snapshots through the existing mutable-state bridge. NDC close now returns an immutable prepared snapshot.
- Added a transaction commit hook; `WorkflowContext` and the NDC new-workflow transaction manager clear transaction deltas only after persistence reports success. Failed store calls retain the source state until the context's existing error discard path.
- Added tests for passive buffered event retention, failure during workflow-request encoding preserving events and CAS, and request payload validation.
- Left `engine_close.rs` and `execution/state_builder.rs` unchanged.

## Gaps / audit limits

- Typed start/reset replay currently adds workflow-request IDs with empty payloads. Preparation rejects these instead of silently losing deduplication information. A faithful encoder for Go's request type/version row is not present in the typed replay layer, so these request-producing paths remain blocked pending that port.
- Go's persistence task blob schema keeps allocated task IDs in the task wrapper/SQL row; `sqlblobs::{transfer_task_to_struct,timer_task_to_struct}` do not encode a task ID field. This change does not invent a non-Go Thrift field. Allocation-once consistency still needs fixture coverage at the actual shard allocation/persistence boundary.
- The tests exercise preparation failure before persistence and source-state recovery, not an injected SQL/history append failure against a live store. Existing callers discard failed contexts according to the context error path.
- No broad service parity claim is made.

## CI evidence

- Successful run: https://github.com/auenkr/cadence-rust/actions/runs/42952105050
- Validated code head: `6e1a3b94ddcba023705f03f9aa2a3279613939af`
- Conclusion: success (Clippy, formatting and cadence-history tests).
- Earlier run: https://github.com/auenkr/cadence-rust/actions/runs/42951604196 — failed Clippy with `needless_option_as_deref`; fixed in the validated head above.
- Local checks: `git diff --check` only. No Cargo, rustfmt, or Go command was run locally.
