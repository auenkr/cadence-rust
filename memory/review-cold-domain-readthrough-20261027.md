# Cold domain read-through review (2026-10-27)

Scope: review of `grpc_matching.rs::domain_id_by_name` and `with_domain_cache`,
`matching_domain_cache.rs::SourceClassified`, and
`matching_cold_domain_metric_tests.rs`; comparison against Go domain-cache,
handler, and proto error conversion. No Cargo, Go, runtime, or gate commands
were run. Focused CI remains pending; this review does not claim a green run.

## Findings

No concrete blockers found in the requested scope.

- `grpc_matching.rs::domain_id_by_name` does a blocking Go-style cache lookup,
  maps empty names to InvalidArgument (`Domain name is empty`), maps classified
  NotFound to `domain_not_found`, classified BadRequest to InvalidArgument,
  and source/internal or exists-but-not-loaded errors to Internal. The message
  for exists-but-not-loaded matches Go's fixed message; the cache's diagnostic
  identity suffix is not exposed on the wire.
- Go references: `go/common/cache/domainCache.go` (`GetDomain`, `GetDomainID`,
  `getDomain`, `checkDomainExists`; around lines 356-390, 388-402, 590-622,
  539-550) and `go/common/types/mapper/proto/errors.go`
  (`fromEntityNotExistsError`, `fromBadRequestError`; around lines 293-307).
  Go checks persistence on cold miss, refreshes under the refresh lock, then
  returns the fixed InternalServiceError if a confirmed domain remains absent
  from cache. The Rust `get_domain`/`refresh_after_miss` sequence mirrors this.
- Cold discoveries are inserted into the matching metadata view before the
  UUID is returned; the cold lookup test checks both source lookup and
  `matching.domain_name`. The ID read-through used for inbound metrics also
  publishes metadata. `metric_domain_name` resumes a panic from `spawn_blocking`
  so `matching_panic::capture` sees it; ordinary resolver misses remain
  swallowed, consistent with Go `handlerImpl.domainName`.
- The typed NotFound status uses the `EntityNotExistsError` detail with empty
  fields, matching Go proto conversion. The test's `google.rpc.Status` decoder
  models field 3 (`details`) and the `expect_err` assertion verifies error
  results. Empty-name and resolver panic/miss cases are also asserted.

## Limits

This is a source review only. It does not establish CI success, SDK behavior,
or full Matching service parity. The focused CI run is pending and remains the
validation source of truth.
