# Cassandra Matching parity corrections — 2026-10-28

Source review: matching-native-cassandra-source-review-20261028.md.

Implemented unconditional ALL consistency for DeleteTaskList, with code-only UNAVAILABLE fallback to LOCAL_QUORUM, in both legacy and native stores. The dynamic flag continues to apply to range deletes only.

Changed the shared task insertion predicate to skip positive expiry with less than one whole second remaining, matching Go nosql_task_store.go. This prevents insertion without TTL. Added legacy and native batch tests proving the lease fence still runs when the task is skipped and the exact one-second boundary writes TTL 1.

Driver error classification now maps request timeout, broken connection and write timeout to Timeout, overload to Throttled, and only UNAVAILABLE with Go's actionable message patterns to DbUnavailable. Other errors remain Internal; consistency fallback deliberately still accepts every UNAVAILABLE. Tests cover the category boundaries.

Not yet addressed: Go operation-specific prefixes in persistence error messages; driver error display text also differs between drivers. No production integration claim. No local Cargo, Go or rustfmt ran; git diff --check passed. CI evidence will be recorded in the rolling integration checkpoint.

Follow-up source change: added Go operation-specific error prefixes and timeout wording to task-list reads, leases (including their initial SELECT), updates, task creation/reads/deletes and both range completion methods. Both legacy and native paths use the same formatter. Added regression assertions for timeout, delete and each completion operation. Driver-specific error detail strings remain driver-generated; no claim of byte-identical diagnostics across Go/Rust drivers. These follow-up edits are not included in CI pin 9ef92bd87.
