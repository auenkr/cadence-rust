# Matching native byte-preserving task store interface — 2026-10-28

Added `matching_native_task_store.rs` as an additive persistence contract. It
mirrors every operation and the default config-store behavior of the existing
`MatchingTaskStore`, using `TaskListKeyBytes`, `TaskListRecordBytes`,
`AllocatedTaskBytes`, `TaskDataBytes`, and `TaskListDataBytes` throughout.

`TaskListKeyBytes` hashes and compares the UUID, original `GoString` name bytes,
and persistence task type. The native error enum retains
`SqlRunIdBytesError.raw` and mirrors backend retry classification. Explicit
legacy-model conversions are available: valid legacy to native conversions are
infallible; native to legacy conversions are fallible on invalid UTF-8. No
lossy or alias namespace conversion is used.

Tests cover `ff`, `fe`, and the UTF-8 replacement rune as distinct keys,
domain/task-type key separation, and legacy model conversion identity including
absent versus present-empty optional partition containers.

This is only the typed contract prerequisite. **No persistence backend
implements `MatchingTaskStoreBytes` yet.** It is now registered
in `lib.rs`. Integration review corrected `AllocatedTask` to import from
`matching_task_store` and moved test-only blob imports into the test module.
CI validation is pending. No Cargo, Go, or
rustfmt command was run locally.
