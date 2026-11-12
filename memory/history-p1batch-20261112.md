# history-p1batch (2026-11-12)

Branch `history-p1batch` from origin/master e607ced11. No cargo/go/docker run locally; CI only.

## Old P1 rows (audit 20261112 section 2) re-checked against current Rust
| Row | Status | Pointer |
|---|---|---|
| 3 task-DLQ processor | DONE | `wiring/assembly/engine_factory.rs` `build_dlq_processor`, `start/stop_dlq_processor` delegate to `taskdlq::Processor` |
| 4 archival workflow signal client | DONE | `wiring/assembly/archiver_wiring.rs FrontendArchivalSignaler` (SignalWithStart) behind `archiver_client.rs ArchivalSignaler` |
| 5 semaphores on replay | DONE | `execution/state_builder.rs` `replicate_semaphore_acquire_initiated/acquired`, `execution/semaphore.rs`, `wiring/assembly/engine_semaphore.rs` |
| 6 failover marker persistence | DONE | `wiring/assembly/engine_start.rs EngineFailoverHost::replicate_failover_markers` (allocate ids, `create_failover_markers_with_range`) |
| 7 request type/version in WorkflowRequest | DONE | `WorkflowRequest.request_type/version` set in `mutable_state_methods_{started,signal,cancellation,decision}.rs`; decoded in `ndc_context_hooks.rs request_type` (stale header comment only) |
| 12 ACSP blob decode in NDC `vector_clock_inputs` | DONE | `wiring/assembly/ndc_state.rs vector_clock_inputs` (+ unit test) |
No OPEN rows, so no new port was needed.

## Archival JSON tags
Compared every `json` tag of `go/common/types` structs with the Rust serde names (`rust/crates/cadence-api/src/service_types.rs`, python scan,
both directions). Only history-blob mismatch: `WorkflowExecutionStartedEventAttributes` Go `firstScheduleTimeNano` (a `*time.Time`, so an
RFC3339Nano string) vs Rust `firstScheduledTimeNano` (i64 nanos, thrift name, kept for HTTP/thrift JSON).
Fix: `cadence-common cloud_archiver/write.rs go_history_batch` (now `pub`) renames the key and writes Go `time.Time` JSON; applied in
`archiver_wiring.rs HistoryNodeBatchSource` read_page/read_all so the filestore path and cloud path both emit Go format. Test:
`go_history_batch_uses_go_first_schedule_time_tag_and_rfc3339`.
Other non-history mismatches (not in archived blobs, left): `RequestCancelWorkflowExecutionRequest.firstExecutionRunID`,
`WorkflowExecutionInfo.parentInitiatedID`, `UpdateDomainRequest`/`SignalWithStart` request shapes.

## CI evidence
(filled in below)
