# Matching AddDecision/AddActivity raw RunID (PATCH REVIEWED; UNVERIFIED: no local or CI validation)

Scope: Go Add handlers never validate `WorkflowExecution.RunID`; Rust eagerly uuid-parsed it (grpc_matching.rs decision/activity add) and
returned InvalidArgument. Base: master a453ac12e (not 70a7eebb; absent-execution changes there are untouched, so integration should be
a trivial merge around the removed `let run = ...` lines).

Design (rust/crates/cadence-persistence/src/matching_blob.rs): `TaskData.raw_run_id: Option<String>` (`#[serde(skip)]`, runtime only,
blob schema unchanged, decode sets None). `with_request_run_id(s)` ALWAYS stores the raw string (valid/noncanonical/malformed/empty) and
sets `run_id` bytes only as a best-effort parse for in-process readers. `run_id_string()` (raw first) feeds history RecordTaskStarted execution,
dispatch logging, both forward-to-parent paths. Backend-specific conversion lives in the stores, NOT common enqueue:
- SQL (sqlite/mysql/postgres create_tasks call `for_sql()` before `encode_task`): "" -> `None` (Go MustParseUUID("") returns nil slice;
  thriftrw omits nil optional binary, readback `UUID.String()` of len!=16 is "" -> absent, NOT the nil UUID); other forms via uuid::parse_str
  (google/uuid Parse accepts 36/urn/braces/32-hex); invalid -> `StorageError::BackendConfiguration` (no panic). `encode_task` debug_asserts raw is None.
- Cassandra (`task_literal`): `cassandra_run_id()` implements gocql `ParseUUID` (hyphen skipped at even nibble offsets, exactly 32 hex, empty rejected).

Go source evidence (read directly, source-only inference, never executed): sql_task_store.go:444 `MustParseUUID(v.Data.RunID)` inside CreateTasks;
uuid.go:39 panics via google/uuid MustParse; task_writer.go:113 `go w.taskWriterLoop()` with no recover (CapturePanic exists only in adaptive_scaler/
task_list_manager) => malformed RunID on SQL async persist panics an unrecovered goroutine = process crash. Handler returned success first.
gocql marshalUUID(string) -> ParseUUID (module gocql@v0.0.0-20211015133455-b225f9b53fa1 uuid.go:64).

Implementation review follow-up (branch `t3/matching-runid-raw-tests`):
- History-backed decision Add -> poller test is present in `matching_rpc_tests.rs`. It sends malformed, empty, uppercase and compact valid strings through AddDecisionTask, asserts exact raw string at the mocked History RecordDecisionTaskStarted RPC, and asserts no durable row for sync-matched tasks.
- `matching_queue.rs` advances `max_read_level` after a durable store error, then `MatchingTaskStoreError::BackendConfiguration` is non-transient under `matching_retry.rs`; this malformed SQL task fails once and is returned to the Matching handler (no task-list retry/stop). This is source trace only, not runtime evidence.
- Replaced `encode_task`'s release-mode `assert!` with a checked `Result`; SQLite/MySQL/PostgreSQL map any accidental unresolved raw value to BackendConfiguration. SQL call sites still resolve first through `for_sql`. Blob field/schema layout is unchanged.
- Corrected Matching GetTasks accounting to count an in-memory raw RunID by UTF-8 byte length, otherwise canonical persisted UUID strings as 36 bytes. This mirrors Go string length accounting.
- Durable malformed decision Add now requires an error, so the assertion cannot pass if Add unexpectedly succeeds.

Remaining divergences / unverified:
1. Activity sync-match test added (`matching_rpc_activity_sync_match_passes_raw_run_id_to_history_without_persisting`, mocked History RecordActivityTaskStarted, asserts raw string + no durable row). UNCOMPILED/UNRUN: needs CI (`rust-ci.yml -f package=cadence-matching`, after `cadence-persistence`).
2. Malformed non-empty RunID SQL behavior in Go remains a source-only inference: `MustParseUUID` panic occurs in the async task-writer goroutine with no recovery visible in the inspected writer. Rust instead returns a permanent persistence BackendConfiguration error to Add; it does not reproduce process crash.
3. Cassandra error type/mapping for rejected UUID not compared with Go's wrapped persistence error; Rust uses BackendConfiguration.
4. `run_id` best-effort bytes for valid noncanonical forms are canonical; readers needing the string use `run_id_string()` (raw). The queue-persisted row
   is the parsed 16 bytes as in Go (readback canonical lowercase).
5. SQL malformed RunID does not emulate Go's inferred fatal process panic; this divergence is retained as a Rust persistence error and needs an explicit product decision if strict crash parity is required.

Tests added: matching_blob (raw kept for all forms; SQL empty absent + malformed error + uppercase; gocql parser edge cases; unresolved raw encode is fallible),
matching_forward_preserves_raw_request_run_ids_like_go, matching_rpc_add_decision_* and history-backed sync-match regression. All remain uncompiled/unrun.
No CI was dispatched by this task. Next validation should dispatch `rust-ci.yml -f package=cadence-persistence`, then `cadence-matching`, on explicit authorization.
