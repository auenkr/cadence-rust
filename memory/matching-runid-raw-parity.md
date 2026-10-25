# Matching AddDecision/AddActivity raw RunID (UNVERIFIED: nothing compiled, no CI)

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

Remaining divergences (exact):
1. Malformed non-empty RunID on SQL: Go crashes the process after acking the add; Rust returns a persistence error from the writer (not InvalidArgument).
   Whether Rust's writer treats that error as retryable/fatal for the task list is NOT verified.
2. Validation ordering: the early RunID error is gone, so domain/task-list/sticky checks now come first (matches Go, which has none).
3. Cassandra error type/mapping for rejected UUID not compared with Go's wrapped persistence error; Rust uses BackendConfiguration.
4. `run_id` best-effort bytes for valid noncanonical forms are canonical; readers needing the string use `run_id_string()` (raw). The queue-persisted row
   is the parsed 16 bytes as in Go (readback canonical lowercase).
5. No real history-backed sync-match RPC regression: coverage is forward-to-parent raw pass-through (matching_dispatch test), durable SQLite add tests
   (matching_rpc_tests) and blob/Cassandra parser unit tests. A sync-match-to-poller test through MatchingHistoryClient mock was not written.

Tests added: matching_blob (raw kept for all forms; SQL empty absent + malformed error + uppercase; gocql parser edge cases),
matching_forward_preserves_raw_request_run_ids_like_go, matching_rpc_add_decision_* (2). All written without compiling.
Next (when parent allows): `rust-ci.yml -f package=cadence-persistence`, then `cadence-matching`.
