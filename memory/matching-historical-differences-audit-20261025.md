# Matching historical difference reconciliation — 2026-10-25

Source-only reconciliation on candidate production tree `27d3ff5f60f0604553824420da0cf61026eda468` (checkout `4b26f1ef166ebcdcb15a2b86c04316e7bb0a0524`; production source matches candidate). Reviewed the v4 parity audit, both open-gap lists, completion audit, Go test map, and nodiff A/B notes against current Go and Rust source. No Cargo/Go commands, tests, CI dispatch, mixed gate, or production edits were made.

## Historical proposed accepted differences

`matching-open-gaps-20261024-v2.md` calls these proposals, not approved differences. Nodiff A/B subsequently records the first four fixes and the last two behavior fixes. Current-source reconciliation:

| Candidate | Current-source finding | Disposition |
|---|---|---|
| Cancelled semaphore context | Go `go/service/matching/handler/handler.go` checks `ctx.Err()` and maps it to Internal; Rust `grpc_matching.rs:168+` checks cancellation/deadline, and `matching_rpc_tests.rs:1651+` exercises cancelled context effects. | Resolved at the in-process observable contract. A cancelled network caller cannot consume a response in either transport. |
| Return non-owned task-list managers | Rust `matching_registry.rs` now returns the names from `unload_non_owned`; RPC discards the internal return. The owner/cache effect is exercised by `shut_down_tasklists_not_owned_unloads_only_moved_owner`. | Resolved. No public API difference. |
| Cancelled RefreshWorkflowTasks | Go engine returns History error; Rust `matching_history.rs:632+` races caller completion and propagates `Cancelled`, production dispatch passes the poller's closed signal (`matching_dispatch.rs:1474+`). | Resolved for caller-visible outcome and abandonment; Rust may avoid an outbound call when caller is already gone, a traffic-only difference documented in nodiff A. |
| Nil execution to History | Go production callers build execution from a non-nil task event (`engine.go:825, 977`; `tasklist/task.go:148`). Rust forwards absent/empty run IDs as empty strings; persisted production tasks carry the event fields. | Mock-only nil shape; no reachable production behavior difference established. |
| Empty shard-executor config | Rust `matching_shard_distributor.rs:507+` explicitly installs `Noop` for zero namespaces, matching Go `handler/engine.go:221+`. | Resolved. |
| Engine-wide shard-distributor onboarding | Go `shard_distributor_onboarded` checks percentage > 0 (`handler/engine.go:281`); Rust uses the same percentage-only predicate (`matching_shard_routing.rs:308+`) separately from per-task-list exclusion. | Resolved; older proposed difference was based on the mistaken conflation of two predicates. |
| Generic Go `handleErr` identity | Go's centralized wrapper is an internal abstraction. Rust maps statuses at RPC boundaries; current request is wire compatibility, not identical internal error-object identity. | Architecture-only candidate. No externally observable mismatch demonstrated by this API-shape difference. |
| Handler Start/Stop wrapper | Rust has `matching_handler::Handler`/`EngineLifecycle`, while production lifecycle also uses launcher/runtime lifecycle. Go wrapper object identity is not a wire contract. | Architecture-only candidate. Audit does not infer completion from the wrapper test; use production launcher lifecycle/restart evidence. |

The first four rows' implementation/evidence details are in `memory/evidence/matching-nodiff-a-20261024/notes.md`; the latter four are in `memory/evidence/matching-nodiff-b-20261024/notes.md`. Current-source pointers above rechecked those dispositions at this candidate.

## Other historical entries checked

- **Fault-injection key (old P1-9): stale finding, not a gap.** Go defines `MatchingErrorInjectionRate` as `matching.errorInjectionRate` in `go/common/dynamicconfig/dynamicproperties/constants.go:2514-2519`, reads it in `go/client/clientfactory.go:183-185`, and applies it to the Matching client. Rust reads the same key in `cadence-matching/src/launcher.rs:767-769`; the injection implementation documents the same Matching-client wrapper in `matching_fault_injection.rs`. The v4 claim that Go has no such key was incorrect. Preserve both Go keys and mappings.
- **Liveness (old P1-6): no distinct behavior difference established.** Go `go/service/matching/liveness/liveness.go:58-92` ticks at TTL/2 and shuts down after `IsAlive` becomes false. Rust `matching_registry.rs:1480+`, `2171+` performs its periodic idle sweep and checks the same liveness condition. The production-pump test `liveness_idle_pump_unloads_owner_between_ttl_and_one_and_a_half_ttl` is present. Scheduling mechanism differs, but both unload in the same TTL-based window; existing test/evidence, not the earlier proposed “accepted difference,” should govern.
- **Raw task batch size (old ignored-test concern): no established gap from this audit.** Rust has an explicit raw nonpositive-value test at `matching_reader.rs:1125+`; actual reader config is stored as `NonZeroU32` with fallback at the production config boundary. Go reads its configured value in `go/service/matching/tasklist/task_reader.go:109,300`. This audit did not trace every production call site for unusual nonpositive config, so it does not claim full edge-case parity or promote the mock accessor expectation into a production bug.
- **TChannel TLS: not a difference.** Go TChannel construction has no TLS options (documented in nodiff B and `matching-tls-dyncfg-20261023/notes.md:88-90`); Rust TChannel remains plaintext.
- **gRPC TLS enabled without server cert/key: verified open behavior difference.** The TLS notes at `memory/evidence/matching-tls-dyncfg-20261023/notes.md:120-125` compare Go startup/handshake behavior with Rust startup failure. The user identified this as a separate active fix owned by another worker; this audit records it and does not duplicate or alter that work.

## Result and limits

No additional production behavior gap was established among the historical proposed accepted differences reviewed here. The gRPC TLS missing/lone certificate startup behavior is a verified open gap already assigned elsewhere. This is a narrow source reconciliation, not a full Matching parity sign-off and not evidence for mixed-service runtime behavior. The earlier historical open-gap counts and notes must not be treated as current status without their source revisions being reconciled.

## Parent follow-up: nonpositive task batch size

Checked primary Go `service/matching/tasklist/task_reader.go`: both
`newTaskReader` (lines108-116) and `getTaskBatchWithRange` (lines300-308)
explicitly replace nonpositive raw values with the property default. Rust
`matching_reader.rs` keeps raw getters separate from `buffer_batch_size` and
`read_batch_size`, and its active tests cover raw0/-5 plus fallback behavior.
This resolves the limited raw-value concern in the source review above; raw
accessor preservation is not a claim that production should request zero rows.
