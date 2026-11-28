# Audit: History execution READ path (2026-11-28)

Branch `audit-exec-read-path`. Go refs under `go/`, Rust under `rust/crates/`.

| # | Function | Go | Rust | Verdict |
|---|----------|----|------|---------|
| 1 | cache get_or_create_current | execution/cache.go:144 | cadence-history/src/execution/cache.rs:48 | EQUAL |
| 2 | cache get_and_create | cache.go:174 | cache.rs:54 | EQUAL |
| 3 | cache get_or_create(+ForBackground/WithTimeout) | cache.go:217-263 | cache.rs:69-76 | EQUAL |
| 4 | getOrCreateInternal (disabled, miss, put, lock fail release) | cache.go:265 | cache.rs:77 | EQUAL |
| 5 | validateWorkflowExecutionInfo | cache.go:307 | cache.rs:90 | EQUAL |
| 6 | makeReleaseFunc (CAS, clear on err/forceClear/panic) | cache.go:340 | cache.rs:37 | EQUAL |
| 7 | getCurrentExecutionWithRetry | cache.go:368 | engine_ndc.rs (SqlExecutionCacheBackend::current_execution_with_retry) | DIVERGENT -> FIXED (Rust retried 5 attempts and never retried Timeout; Go: IsTransientError incl. TimeoutError, 50ms x2 max 10s, 30s expiry, util.go:50-52,151). Go ThrottleRetry ServiceBusy throttle policy not ported (open) |
| 8 | LoadWorkflowExecutionWithTaskVersion (retry 3, repair, stats, flushBeforeReady) | context.go:350 | execution/context.rs:105 | EQUAL (Rust adds `active_failover_version` hook; default None, so no behavior change; Go does this inside StartTransaction) |
| 9 | Clear / SetHistorySize / Load default version -24 | context.go:305,333,433 | context.rs:101,102,104 | EQUAL |
| 10 | getWorkflowExecutionWithRetry | context.go:1227 | engine_ndc.rs SqlWorkflowContextStore::get_workflow_with_retry (~581) | DIVERGENT -> FIXED (same retry fix via retry_transient_read; range id read per attempt = Go shard.GetWorkflowExecution; Go shard-closed log gating not ported, logging only) |
| 11 | executionManager.GetWorkflowExecution deserialization | common/persistence/execution_manager.go:100 | history_execution_cassandra.rs:94, sql_workflow_store.rs:955 | NOT FULLY AUDITED (Go GetWorkflowExecution deserializes activity/child/buffered/execInfo/versionHistories, checksum fallback from ChecksumData; Rust sql_workflow_store.rs:841-853 builds checksum; per-field not diffed) |
| 12 | ReadHistoryBranch range selection / token serialize | persistence/history_manager.go:353-560 | cadence-persistence/src/history_reader.rs:313-405 | EQUAL |
| 13 | Paging token JSON field names | history_manager.go:44 | history_reader.rs:205 | EQUAL |
| 14 | ErrCorruptedHistory (type+message) | history_manager.go:81 | history_reader.rs:469 | EQUAL |
| 15 | per-batch checks (version/ID, stale skip, continuity) | history_manager.go:495-545 | history_reader.rs:448-478 | EQUAL |
| 16 | Empty blob data -> zero events -> "empty events" DataInconsistency | serializer.go:138 | history_reader.rs decode_batch | DIVERGENT -> FIXED |
| 17 | Malformed NextPageToken error class (raw json err, not BadRequest) | history_manager.go:~395 | history_reader.rs:335 | DIVERGENT -> FIXED (now Internal) |
| 18 | JSON-encoded history batches decode | serializer.go deserialize | history_reader.rs decode_batch (thriftrw only) | DIVERGENT, NOT FIXED (needs Go JSON-tag parity for HistoryEvent; Go writers emit thriftrw only) |

Also spot-checked, NOT diffed line by line: mutable_state_builder.Load (go :342-381), cache eviction (go cache.go:113-122: TTL, Pin, MaxCount, MaxSize; Rust cache.rs delegates to shared pinned cache), Cassandra/SQL ReadHistoryBranch store paging.

Not covered (honest scope limit): mutable_state_builder.Load, cache eviction/TTL internals (shared `cache` crate), DeserializeExecutionInfo/activity/child/buffered/version-history deserializers, SQL/Cassandra store paging.

Counts: EQUAL 12, DIVERGENT 5 (FIXED 4, open 1), NOT FULLY AUDITED 4 (deserialization, Load, cache eviction, store paging).

Fixes: cadence-persistence/src/history_reader.rs (decode_batch empty data; token parse error -> Internal), tests `empty_batch_blob_is_data_inconsistency`, `malformed_page_token_is_not_bad_request`.

CI evidence: see section below (filled after runs).

Fix 3: cadence-history/src/wiring/persistence/engine_ndc.rs: retry_transient now 30s-expiry/10s-max backoff; new retry_transient_read retries Timeout (Go IsTransientError) for get/current/append; test `read_path_retries_timeouts_like_go_is_transient_error`.

CI on head bb2e81eb2 (dispatched; conclusions pending at time of writing, still queued):
- rust-ci cadence-persistence https://github.com/auenkr/cadence-rust/actions/runs/46108822393
- rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/46108824890
- history-mixed-gate https://github.com/auenkr/cadence-rust/actions/runs/46108827054
Earlier runs on fc77c7763 (46108569335/46108576248/46108579516) were cancelled while still queued, superseded by the fix.
