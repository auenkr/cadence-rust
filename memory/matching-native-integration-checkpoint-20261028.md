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

Native History and clippy-correction commit a04f6ab98 is under fresh five-crate CI: https://github.com/auenkr/cadence-rust/actions/runs/40586873102 (head a04f6ab98; conclusion pending at dispatch). All checks remain runner-only.

Run 40586873102 at a04f6ab98 concluded failure in clippy: unused mut in Cassandra test, three clone_on_copy fields and a useless test conversion in the native History client, and large_enum_variant after storing both the typed client and Channel. Removed mut/clones/conversion and boxed the typed gRPC client. Added a source-domain row assertion following native queue review. Native TChannel implementation and Cassandra source review remain active. Runner-produced formatting patch applied except the actively edited native History file. No local Cargo/Go/rustfmt ran.

Latest five-crate fresh-target CI: https://github.com/auenkr/cadence-rust/actions/runs/40587756818 at dfcb1883da3e93c62725fd6d93afca759ef36036. Verified Actions headSha equals pushed origin/master. Conclusion pending. Host load reached 11.13, so no replacement worker was started for the completed native queue review. Existing TChannel implementation and Cassandra review remain active.

## Confirmed five-crate pass and Cassandra corrections

Run https://github.com/auenkr/cadence-rust/actions/runs/40587756818 concluded **success**, head dfcb1883da3e93c62725fd6d93afca759ef36036. Fresh target, clippy followed by tests for cadence-proto/common/api/persistence/matching. This excludes the subsequent native TChannel commit 462dcad29 and the Cassandra fixes below. Applied its formatting patch without local rustfmt.

The Cassandra source review confirmed unconditional ALL-to-LOCAL_QUORUM task-list deletion, subsecond task insertion, and backend category mismatches. Corrected both legacy/native deletion paths, their shared expiry predicate, and shared driver error classification. Added recorder regressions for subsecond skip with lease fence preserved, the one-second TTL boundary, flag-off deletion/fallback, actionable UNAVAILABLE messages, overload, client/write timeout and broken connections. Range deletion remains flag-controlled. Go's operation-specific error-message prefixes remain an explicit follow-up; the new classifier corrects categories only. No local Cargo, Go or formatting ran. Fresh CI required for these changes and the native TChannel integration. Production native queue/handler adoption and final mixed gates remain open; Matching is not complete.

Fresh five-crate validation dispatched at https://github.com/auenkr/cadence-rust/actions/runs/40589162143, verified head 9ef92bd87a139740060c8ae2889759382abba0ef, clean_target=true and format_matching_parity_files=true. Pending; no pass claimed. This includes the native TChannel source and Cassandra corrections. Supplemental matching-dependency-oracles.yml dispatched at the same source pin for the real Go raw-byte TChannel peer and Postgres TLS cases; run URL must be recorded once resolved.

The supplemental oracle run is https://github.com/auenkr/cadence-rust/actions/runs/40589249708, verified head 9ef92bd87a139740060c8ae2889759382abba0ef. Both this and 40589162143 remain in progress at the subsequent Cassandra error-prefix checkpoint. Operation-specific error prefixes are now implemented in source, including lease SELECT and distinct CompleteTask/CompleteTasksLessThan wording. Regression tests added; not yet validated. Do not cancel the running earlier-pin checks merely to include this follow-up.

Two low-effort Sonnet 5.5 workers now own only new matching_writer_native.rs and matching_reader_native.rs, respectively, plus their notes. Parent owns registration and CI. No production adoption claimed. Native fatal writer callback must preserve SqlRunIdBytesError and must be wired through the host fatal enum later; reader isolation/selection keys must remain GoString bytes. Both workers were told not to invoke local Cargo/Go/rustfmt and not to stage/commit/push. Load was 4.71 and available RAM 3.6GiB before launch.

Native writer worker completed. Parent registered matching_writer_native and corrected the source-domain lookup and cancellation-test race. Its typed fatal callback is not yet connected to the host enum, because the production registry still constructs the legacy writer. Native reader work remains active. Current earlier-pin CI reached tests (clippy passed); dependency oracle still runs. Wait for those results before dispatching a replacement to avoid losing evidence.

## Reader/sync registration and validated Cassandra/TChannel source pin

Run https://github.com/auenkr/cadence-rust/actions/runs/40589162143 concluded **success** at 9ef92bd87a139740060c8ae2889759382abba0ef: fresh five-crate clippy and tests. Includes Cassandra consistency/expiry/category corrections and native TChannel source. Excludes subsequent operation error context and native writer/reader/sync additions. Applied runner formatting patch without local rustfmt.

Dependency oracle https://github.com/auenkr/cadence-rust/actions/runs/40589249708 concluded **failure** at the same head: History selector used matching_history_tchannel::tests but the actual module is typed_codec_tests, resulting in zero selected tests. Gate rejected the empty run. Fixed selector; no History runtime pass is claimed. Ten live PostgreSQL TLS cases each executed and passed, and cleanup succeeded. Compact results and executable SHA256s retained in memory/evidence/matching-native-dependency-40589249708.txt. Corrected oracle rerun required.

Registered native reader and native synchronous matcher. Both preserve GoString group keys and task bytes, and remain additive pending registry adoption. Reader tests include duplicate/out-of-order invariant fatal and callback/shutdown ordering. Sync tests cover raw group isolation, cancellation, handoff ack and deadlines. Added both files to CI formatting list. New native writer gets the same result_large_err allowance as the legacy module. No local Cargo/Go/rustfmt commands executed.

Latest registered-foundation source pin: 22ede2f1b. Fresh five-crate clippy/tests: https://github.com/auenkr/cadence-rust/actions/runs/40590414225. Corrected real Go TChannel/PostgreSQL TLS oracle: https://github.com/auenkr/cadence-rust/actions/runs/40590418298. Both pending at dispatch, no pass claimed. Includes native writer/reader/sync registration and Go Cassandra operation error context. Production native registry/handler adoption remains open.

Native partition worker completed: registered matching_partition_native and additive persistence_bytes[_until] retry helper. Raw groups use GoString and partition errors retain a raw message/kind. Parent renamed the optional lossy tonic conversion to to_diagnostic_status; native wire integration must not use it. Legacy retry interface unchanged. Work remains uncompiled pending next CI source pin. Native query worker currently owns native_model.rs changes; excluded from this commit until it reports completion. Active earlier-pin checks 40590414225 and 40590418298 remain pending at this checkpoint.
