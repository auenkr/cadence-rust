# Matching AddDecision/AddActivity RunID raw pass-through (UNVERIFIED, no CI run)

Scope: Go Add handlers pass `WorkflowExecution.RunID` as a raw string; sync match works with malformed/empty values.
Rust previously uuid-parsed eagerly in grpc_matching.rs (decision ~2282, activity ~2506) and returned InvalidArgument.

Change: `TaskData` (rust/crates/cadence-persistence/src/matching_blob.rs) gets `#[serde(skip)] raw_run_id: Option<String>`
(runtime-only; thrift blob schema untouched, decode sets None). `with_request_run_id` stores valid UUIDs in `run_id` bytes as before,
anything else in `raw_run_id`. `run_id_string()` is used by readers (history RecordTaskStarted execution, dispatch logging,
forward-to-parent, in-process forward). `for_persistence()` runs in `registry::enqueue` (async persist boundary).

Persistence behaviour:
- empty -> nil UUID bytes (matches Go SQL). NOT matched for Cassandra: gocql ParseUUID rejects empty; Rust stores nil UUID there (divergence, unverified).
- malformed non-empty -> `InvalidArgument("Run UUID is not valid.")` at enqueue. Go SQL: writer goroutine panics (process crash) after the handler
  already queued; Cassandra: marshalUUID error. DELIBERATE DIVERGENCE: not reproducing the crash. Not exact parity.
- Ordering: the early RunID validation is removed, so other validations (domain, task list, sticky) now precede any RunID error.

Tests added: matching_blob unit tests (raw never hits wire; empty->nil; malformed rejected). No handler-level sync-match
regression yet (needs CI-compiled harness); no compilation done locally. Four full TaskData literals updated for the new field.
Next: dispatch `rust-ci.yml -f package=cadence-persistence` then cadence-matching once parent allows.
