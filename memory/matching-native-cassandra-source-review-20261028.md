# Matching native Cassandra store: read-only source review (2026-10-28)

Scope: `rust/crates/cadence-persistence/src/matching_cassandra_native.rs` (N),
`matching_cassandra.rs` (L), `matching_cassandra_exec.rs` (E), against Go
`common/persistence/nosql/nosql_task_store.go` (G1), `nosqlplugin/cassandra/tasks.go` (G2),
`tasks_cql.go`, `gocql/public/client.go`, gocql `marshal.go`/`uuid.go`
(`gocql@v0.0.0-20211015133455-b225f9b53fa1`), and `scylla-cql-core-1.9.0`
serialization (`writers.rs`, `value.rs`). Source reading only: no Cargo, Go, build or
runtime claims. Reviewed tree: master at `f49a3339a`.

## Confirmed findings

### 1. `DeleteTaskList` is flag-gated in Rust, unconditional in Go (medium)

- Go `tasks.go:318-340`: the query is built with `.Consistency(cassandraAllConslevel)` (line
  326) with no dynamic-config check. On an UNAVAILABLE error (`IsCassandraConsistencyError`,
  code `0x1000`) it retries at `cassandraDefaultConsLevel` (LOCAL_QUORUM). Only
  `executeWithConsistencyAll` (range deletes, `db.go:110-122`) checks
  `EnableCassandraAllConsistencyLevelDelete`.
- Rust N `delete_task_list` (`matching_cassandra_native.rs:619-637`) and L
  (`matching_cassandra.rs:673`) take the ALL-then-fallback path only when
  `all_consistency_delete()` is true. With the flag off (the default) they send the LWT
  DELETE at session-default consistency, with no fallback.
- The L test `go_delete_task_list_statement_values_and_consistency` encodes the wrong
  expectation ("Flag off: default consistency").
- Impact: consistency and availability differ from Go when the flag is off. The delete runs
  at the default level, not ALL, and never gets the LOCAL_QUORUM retry.
- Minimal fix: in both `delete_task_list` impls, always run the ALL attempt, then retry at
  `FALLBACK_CONSISTENCY` on `is_unavailable()`. Do not consult the flag. Keep the flag for
  `range_delete_tasks` and `native_range_delete`. Fix the test.

### 2. Sub-second remaining expiry: Go skips the task, Rust inserts it with no TTL (low-medium)

- Go `nosql_task_store.go:313-324`: for non-zero `Expiry`,
  `ttl = int(Expiry.Sub(now).Seconds())`. The task is inserted only when `ttl > 0`;
  otherwise it is logged as expired and skipped. So `0 < remaining < 1s` is skipped.
- Rust N `create_tasks_with_domains` (`:512-521`) skips only when
  `expiry_not_passed()` is false (`expiry <= now`, L `:371-373`). For `0 < remaining < 1s`
  `ttl_seconds()` (L `:360-366`) returns `None` (`seconds > 0` fails), so the row is
  inserted with no `USING TTL` and never expires. L `create_tasks_with_domains` has the
  same gap.
- Fresh tasks are unaffected. `at_creation` in `matching_native_queue.rs:49-57` gives
  either `now + N*1e9`, so `ttl >= 1`, or `GO_ZERO_TIME_NANOS` (no TTL), matching Go.
  Only a re-added task with a stored non-zero expiry hits this.
- Minimal fix: in `create_tasks_with_domains`, skip when
  `expiry_nanos > 0 && (expiry_nanos - now_nanos) / 1_000_000_000 <= 0`, same condition as
  Go's `ttl > 0`. Keep the `GO_ZERO_TIME_NANOS` case as no-TTL. Apply to L and N.

### 3. Backend error classification is narrower than Go `convertCommonErrors` (low)

- Go `utils.go:40-66` maps, in order:
  - `ErrNotFound` to EntityNotExists
  - timeout to `TimeoutError`: `context.DeadlineExceeded`, `ErrTimeoutNoResponse`,
    `ErrConnectionClosed` and `*RequestErrWriteTimeout` (`client.go:31-43`)
  - overloaded `0x1001` to `ServiceBusy`
  - DBUnavailable only when code is UNAVAILABLE **and** the message contains
    "Cannot perform LWT operation" or "cannot achieve consistency level"
    (`client.go:60-77`)
  - everything else to `InternalServiceError`, formatted `"%v operation failed. Error: %v"`
- Rust `Store::internal` (L `:79-90`) produces only `DbUnavailable` (any UNAVAILABLE, no
  message check) or `Internal`. `BackendErrorKind::Timeout` and `Throttled` are never
  produced for Cassandra, and the operation-name prefix is absent.
- All of these kinds are `is_transient()`, so retry behaviour is unchanged. Impact is the
  error class and message text visible to callers and metrics.
- Minimal fix: classify `ExecutionError` variants (write timeout, overloaded,
  request-timeout/connection-closed) and add the message check on UNAVAILABLE. Optional:
  prefix with the operation name. Note that `is_unavailable()` (E `:79-91`) must stay
  message-agnostic for the ALL-delete fallback (code `0x1000` only), so add a separate
  predicate for DBUnavailable classification.

## Checked and correct (no finding)

- **Raw text bytes.** `RawCql::Text` writes `writer.set_value(bytes)` verbatim, so empty is
  a 0-length value and not null (N `:88-96`), matching gocql `marshalVarchar`. Non-UTF-8
  bytes are not validated or rewritten client-side; a real server rejects them. Expected
  server rejection is surfaced as `Internal` backend error. Every identifier on the native
  path uses `RawCql::text` (`key.name`, UDT `name`, `workflow_id`, `partition_config`
  keys/values, isolation groups, error message bytes). No `String`/lossy conversion on
  write. The read path is `String` from server-validated UTF-8, which is lossless.
- **Cell framing.** UDT: `into_value_builder` plus one `make_sub_writer` per
  server-declared field in order (length-prefixed int32); a missing field is `set_null` (-1)
  and an unknown field is a hard error (N `:98-126`). List/set/map: int32 count then
  length-prefixed elements (N `:127-159`), correct for CQL v4+. Null vs empty:
  `Null` -> -1, `Some(empty map)` -> count 0, `None` -> -1. This matches gocql nil vs
  non-nil semantics for `partition_config` and `isolation_groups`.
- **Bind order and types.** Checked against `tasks_cql.go`:
  - `INSERT_TASK` and `INSERT_TASK_LIST` take 5 key values (domain, name, type, row, id)
    plus the UDT plus `created_time`
  - `UPDATE_LIST*` take `range_id, UDT, last_updated_time, 5 key values, IF range_id`
  - the range fence takes `range, ts, 5 key values, range`
  - `SELECT_TASKS` takes the key, `read_level`, `max_read_level`, then LIMIT as i32
  - Scalar types (i32 `type`/`kind`/`num_*`, i64 `ack_level`/`version`/`schedule_id`,
    Uuid, `DateTime<Utc>`) agree with the UDT/column types, and the driver enforces exact
    type checks.
  - The UDT field order is by name against the server definition, so Go's
    `templateTaskListType`/`templateTaskType` field set is preserved.
  - Binding the whole UDT as one `?` instead of inline `{f: ?, ...}` is wire-equivalent.
- **LWT fence and batch atomicity.** `RANGE_FENCE` is `UPDATE ... IF range_id = ?`, last
  in a `BatchType::Logged` batch (E `:207`), all in one partition. This matches Go
  `InsertTasks`. The TTL list update is a 2-statement logged batch (insert with
  `USING TTL 86400` plus `UPDATE ... USING TTL 86400 ... IF range_id`), matching Go
  `UpdateTaskListWithTTL`. The `1 | 2` kind test equals Go `TaskListKindHasTTL`
  (Sticky, Ephemeral). `lwt_outcome` reads `[applied]` from column 0 and `range_id` by
  name, defaulting to 0 for null, as Go's `previous["range_id"].(int64)` does.
- **Lease messages (nosql, not SQL).** Renew failure uses the caller's `previous_range` as
  haveRangeID and the space after the comma (N `:419-426`); the insert race uses
  `INITIAL_RANGE_ID`; the update race uses the incremented range; kind stored is the
  current row's, returned kind is the request's. All match G1 `:111-176`. The SQL
  helper's difference is correctly not used.
- **Malformed RunID.** `cassandra_run_id` mirrors gocql `ParseUUID` (hyphens at even nibble
  offsets, exactly 32 hex digits, empty rejected). An error is returned as
  `BackendConfiguration` before any statement is sent (N `:225-233`). Go fails the
  marshal inside the batch, so nothing is written.
- **TTL cap and rounding.** Cap `157_680_000`, integer-second truncation, and
  `created_time = now` all match.

## Divergences judged benign or unverified (not bugs)

- `partitions()` (N `:164-183`) writes `Some(empty map)` as an empty collection; Go
  `fromTaskListPartitions` writes null for an empty map. It reads back identically
  (`toTaskListPartitions` returns nil for len 0); only the stored blob bytes differ.
  Whether Matching ever builds `Some(empty)` is not verified.
- Go recomputes `num_read_partitions`/`num_write_partitions` as `len(map)` on write; Rust
  writes the struct fields. Equal when callers keep them consistent. Not verified for the
  adaptive-partition builder.
- `get_tasks` uses `LIMIT ?` where Go uses driver paging with an early break (same first N
  rows). The `read_level > max_read_level` short-circuit is absent (the query returns
  empty). `batch_size <= 0` returns empty where Go would return all rows; callers do not
  pass 0.
- Reading a null `run_id` yields `None`, where Go's gocql yields the zero-UUID string.
  Unreachable for Go- or Rust-written rows, since a RunID is always written.
- A null `task` or `task_list` column makes decode fail (non-Option UDT types), where Go
  would produce an empty row or panic. No code path creates such rows.
- Unprepared statements with non-empty bound values are prepared on the fly by the driver
  (including inside a batch). Whether LWT result-metadata decoding behaves correctly on
  that path is runtime-only and unverified here; the CI/gate evidence should confirm it.
