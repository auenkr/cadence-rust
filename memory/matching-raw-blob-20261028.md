# Matching raw-byte blob codec work — 2026-10-28

Implemented additive byte-preserving SQL matching blob models and codecs in
`rust/crates/cadence-persistence/src/matching_blob.rs`:

- `TaskDataBytes` and `TaskListDataBytes` carry Go Thrift string values as
  `cadence_common::go_string::GoString` for workflow IDs, partition map keys
  and values, and isolation group names.
- `encode_task_bytes`, `decode_task_bytes`, `encode_task_list_bytes`, and
  `decode_task_list_bytes` preserve arbitrary string bytes. The UTF-8 APIs
  remain as they were. Explicit `TryFrom` conversions reject malformed UTF-8;
  conversion back to byte forms is lossless.
- Runtime-only `raw_run_id` is separate from persisted UUID bytes. The raw
  model stores it as `GoString`, but encoding rejects unresolved values just as
  the existing SQL blob encoder does. SQL and Cassandra UUID behavior was not
  changed, and UUID bytes are never treated as Go strings.
- Optional maps/lists retain absent versus present-empty state. Map decoding
  uses the same `BTreeMap` collection semantics as the legacy codec (later
  duplicate entries replace earlier values).

Added source regression tests for `ff`, `fe`, UTF-8 replacement rune, embedded
NUL, distinct raw map entries, absent/empty partition containers, and valid
UTF-8 byte-identical encoding and decode conversion through old and new APIs.

The task request named Go `sqlblobs.thrift`, but no matching file exists in this
checkout (`rg --files | rg -i 'sql.?blobs|blob.*thrift'` returned no paths).
The implementation follows the field IDs and value types encoded by the
existing Rust codec, which documents those Go blob fields. No Cargo, rustfmt,
Go build, or tests were run locally per instruction. This is source-only work;
it does not claim production store/registry integration. Parent owns CI and
commit handling.
