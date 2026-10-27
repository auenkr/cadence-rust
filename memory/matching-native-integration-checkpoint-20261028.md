# Native Matching integration checkpoint — 2026-10-28

Resumed the existing Matching completion task. Exact Go raw-byte behavior remains required.

Committed and pushed `c51012f9e`:
- Registered `matching_native_task_store` in persistence lib.rs.
- Fixed the contract import: AllocatedTask lives in matching_task_store, not matching_blob.
- Moved test-only blob imports under cfg(test).
- Fixed an unclosed tuple in native decision response query-map encoding, which prevented rustfmt and compilation in the previous CI run.
- Included native persistence and dynamic config files in the CI formatting patch list.

Previous CI: https://github.com/auenkr/cadence-rust/actions/runs/40583437678
Head: baf2da6a9cd7ca55209b8924ce28d9d262bbaba3
Conclusion: failure during rustfmt syntax parsing, before compilation.

Replacement CI: https://github.com/auenkr/cadence-rust/actions/runs/40583973417
Head: c51012f9e (resolve full SHA from git)
Conclusion: in progress at checkpoint; not a pass.
Selection: cadence-proto, cadence-common, cadence-api, cadence-persistence, cadence-matching.
Inputs: clean_target=true, format_matching_parity_files=true.

No Cargo, Go, or rustfmt commands ran locally. git diff --check passed.
Pre-existing uncommitted dynamic_config.rs and matching-native-config-filters-20261028.md were left untouched and are excluded from this CI pin.

Remaining: native backend implementations, handler/queue/History/token adoption, production tonic/Thrift registration, full native response graphs, invalid-byte mixed Go oracles, final workspace and mixed gates at one verified source pin. The scheduled continuation must remain enabled while Matching is incomplete.

## Integrated backend checkpoint

Commit b243fd999 contains the SQLite/MySQL/PostgreSQL native stores, dynamic config byte helpers, activity-scheduled dispatch adapter, retained raw lease error messages, and runner-generated formatting patch. Each remains unverified. New CI: https://github.com/auenkr/cadence-rust/actions/runs/40584900096 at head b243fd999 (resolve full SHA with git), fresh uncached target, five affected crates, format_matching_parity_files=true. Pending at dispatch.

Previous replacement 40583973417 concluded failure: clippy chunks_exact_to_as_chunks in native_wire_edges.rs. That lint was fixed in b243fd999. No Go or Cargo command ran locally. The pinned IDL submodule was initialized with git only to inspect source field definitions.

Run 40584900096 concluded failure in clippy: four PostgreSQL ignored-test lease calls omitted the kind argument; native.rs had a second chunks_exact_to_as_chunks lint. Corrected both and applied the runner formatting patch. Added and registered the five non-decision native Thrift response encoders; production dispatch is unchanged. Next CI required.

Run https://github.com/auenkr/cadence-rust/actions/runs/40585602320 validates 69981a19d (response encoders and prior CI fixes). Pending at the Cassandra/queue integration checkpoint. It does not include the newly registered native queue or Cassandra backend.

## Queue and Cassandra CI pin

Commit 102f1c077 registers the full native durable queue and raw Cassandra backend. Fresh uncached five-crate CI dispatched at https://github.com/auenkr/cadence-rust/actions/runs/40585964293 (head 102f1c077; conclusion pending). Supersedes prior in-progress 40585602320 via workflow cancel-in-progress concurrency. No local Cargo, Go or formatting executed. Native History start-client worker remains active. Further worker starts deferred because the host load was 6.86 (>6 resource guard), despite available RAM 3.2 GiB. Next source review should compare native queue behavior and RawCql UDT/collection serialization to legacy and Go. No production-native handler adoption or service completion claimed.

## Native History client and clippy correction checkpoint

Run 40585964293 at 102f1c077 concluded failure in clippy. Reported only lints: duplicated allow(dead_code) on all three native adapter modules, test-module ordering in the decision adapter, and field_reassign_with_default in Cassandra/Postgres tests. Removed duplicate inner attributes, moved the test module to the end, and initialized test fields directly. Applied the CI-generated formatting patch locally without invoking rustfmt.

Integrated the additive native gRPC History task-start client and raw workflow/domain peer hashing. Parent corrected the lossy timeout key, added a native byte-aware timeout provider seam, and preserved malformed persisted-UUID errors. TChannel raw History still unsupported; native runtime/provider adoption remains pending. Next CI pin required; no Matching completion claim.
