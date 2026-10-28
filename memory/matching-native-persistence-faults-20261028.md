# Native Matching persistence fault injection (2026-10-28)

## Scope

Added `MatchingTaskStoreBytes` error injection in `rust/crates/cadence-matching/src/matching_fault_injection_native.rs`, privately registered and re-exported by `matching_fault_injection.rs` as `inject_task_store_bytes_errors`. The wrapper preserves the native byte-string contract and delegates `fetch_latest_config` unchanged. Its operation names, 30-second startup grace, rate-zero identity pass-through, random selection/forwarding rules, fake error mapping, logging text and sink behavior reuse the parent module helpers and match the legacy task-store wrapper.

The wrapper calls the native backend with the original key/task arguments, without UTF-8 conversion. Injected errors are `MatchingTaskStoreBytesError::Legacy(fake.to_task_store_error())`; calls selected for forwarding execute first, but still return the fake error. Tests added for `0xff` and `0xfe` keys during startup grace and deterministic stop/forward cases with legacy errors and operation logs.

## Validation and limits

Per the task instruction, no local Cargo, rustfmt, Go build, test, or check commands were run. Tests and compilation are therefore **unverified** and should be run by the parent through CI. Source inspection confirms all `MatchingTaskStoreBytes` methods are implemented and config fetch is explicitly delegated. No launcher, crate-root, or workflow wiring was changed.
