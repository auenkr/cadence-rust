# Matching metrics BLOCKED rows (2026-10-23)

## (a) ListDomain payload size
Go: go/common/persistence/metered.go:102-170 (ListDomainsResponse, GetDomainResponse, DomainInfo, DomainConfig, DomainReplicationConfig ByteSize).
Rust: cadence-matching/src/matching_persistence.rs `get_domain_byte_size`/`list_domains_byte_size`.
64-bit Sizeof: ListDomainsResponse 48, GetDomainResponse 88, DomainInfo 80, DomainConfig 120, AsyncWorkflowConfiguration 48.
Hand simulation of Go metered_test.go:154 (535): 48+3 + 88 + (88+103+191+14). Config 191 = 120+3+3 (URIs) + 48 (async sizeof) + 4+4+3 (async strings, blob) + 6 (isolation key+name).
Earlier Rust used 32 for the envelope and 128+36 per domain: both were wrong; the empty-list test now expects 48.
Rust differs from the Go vector only in that sub-structs are never nil: empty entry = 336 (88+80+168), page = 783.
Caveat: Go counts `len(key)` for isolation groups; Rust has a Vec, so key is assumed == partition name (true for thrift-decoded configs).

## (b) is_retry
Go: backoff.ThrottleRetry.attempt stores retryCount in ctx (go/common/backoff/retry.go:217-270); metered *_generated.go tags IsRetryTag(retryCount > 0). Tag is present on every persistence call (0 without context).
Rust: tokio task-local `PERSISTENCE_RETRY_COUNT` (matching_persistence_limiter.rs); `retry_write` in matching_writer.rs scopes each attempt with its attempt index; record_call reads it. Test is_retry_tag_follows_go_retry_count_context.
Limitation: Go ThrottleRetry sites in task_list_manager/task_reader/task_completer are not all mirrored by a Rust retry loop with the same context; only the writer loop propagates the count. Persistence calls executed in spawn_blocking would not see the task-local.

## (c) Rollups / migration
Go: scope.go:55-160 emit metricRollupName on root scope when migration allows; defs.go:3353-3373. Persistence per-domain defs have rollups.
Rust: record_call emits persistence_requests_rollup, persistence_latency_rollup, persistence_errors_rollup and persistence_errors_<class>_rollup (no tags) for per-domain calls. Zero MigrationConfig: counters emit; latency timer emits; persistence_latency_ns_rollup histogram suppressed (config.go:125-126). Test per_domain_task_calls_emit_go_rollups.
Not done: configurable MigrationConfig; persistence_empty_response rollup; rollups of non-persistence defs (cadence_requests_rollup etc.) are outside this change.

## CI
rust-ci run https://github.com/auenkr/cadence-rust/actions/runs/40015945604 sha 877bbc44e0c8de47318995909c59fb176b9f474e conclusion success (clippy + workspace tests; try 3 after two fixes: clippy lint, then test expectations).
