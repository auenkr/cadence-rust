# matching-semaphore-params-20261024: Go parity for semaphore manager params and page tokens

Status: code written, not yet verified by CI at the time of this note (see Verification).

Note on paths: the two tests live in `rust/crates/cadence-matching/src/matching_semaphore_parity_tests.rs`
(mounted from `matching_semaphore.rs` via `#[path]`), not `tests/`.

## 1. NewManager param validation (Go semaphore_manager_test.go:136)

Go refs: `go/service/matching/semaphore/semaphore_manager.go:100-127` (`validateParams`, `NewManager`),
`go/service/matching/semaphore/identifier.go:73-82` (`Identifier.validate`).

| Go check | Rust | Status |
|---|---|---|
| ID: DomainID empty / SemaphoreName empty / Bucket < 0 | `Identifier::validate` (shared by `Identifier::new` and `Manager::new`) | exact, same messages |
| Tokens nil | store is `Arc<dyn SemaphorePersistence>`, required by type | type-enforced, no runtime case |
| Logger nil | Rust `Manager` has no logger field | not representable, no case |
| IdleTTL <= 0 | `Manager::new` -> `BadRequest("ManagerParams.IdleTTL must be positive")` | exact |
| OnStopFn nil | `on_stop: OnStop` required (was `Option`) | type-enforced, no runtime case |
| TimeSource nil | Rust uses `now_nanos()`; no injectable clock | not representable, no case |

Behaviour changes:
- `Manager::new` now returns `Result<Manager, ManagerError>`; invalid input is `BadRequest`, no manager built.
- `Registry::with_idle_ttl` no longer clamps to 1ns; a non-positive TTL fails `get_or_create` with BadRequest,
  as Go's config-driven `IdleTTL` does. An existing manager is returned without re-validating (as Go).
- `Liveness::new` no longer receives a `.max(1)`-clamped value (validation precedes it).

## 2. Scan page tokens (Go semaphore_manager_test.go:180)

Go refs: `semaphore_manager.go:426-466` (`loadTokenOwnership` paging loop, stops on empty token),
`semaphore_manager.go:23` `scanPageSize = 2*MaxSemaphoreBucketSize+1`,
`go/common/persistence/data_manager_interfaces.go:1596-1613` (request/response with `PageSize`, `NextPageToken`),
`go/common/persistence/nosql/nosql_semaphore_token_store.go:143-170` (paged select).
`go/common/persistence/semaphore_metadata_manager.go:42` `MaxSemaphoreBucketSize = 250`.

Rust had no page token at all: `SemaphorePersistence::scan_semaphore_bucket` returned a whole `Vec`.
Changed the contract (scope note: this goes beyond `matching_semaphore.rs`, because page threading cannot
exist without the persistence seam carrying the token):
- `cadence-persistence/src/semaphores.rs`: new `SemaphoreBucketPage { rows, next_page_token }`
  (Go `ScanSemaphoreBucketResponse`); trait method takes `page_size: usize, next_page_token: Option<&[u8]>`.
  In-memory store pages by row offset, token = 8-byte big-endian offset. `page_size == 0` and malformed tokens
  are `AcquireError::Invalid` (Rust-side guard; Go's gocql has no equivalent rejection, and the manager never sends 0).
- `cadence-persistence/src/cassandra.rs`: `query_single_page` with a `PagingState`, mirroring
  `matching_domain_metadata.rs` (the existing paging precedent). Previously `query_unpaged`.
- SQL forwarders (sqlite/mysql/postgres) and the rate-limited wrapper forward the new args.
- `cadence-matching/src/matching_semaphore.rs`: `SCAN_PAGE_SIZE = 2*250+1` (Go `scanPageSize`);
  `Manager::scan_bucket` follows tokens to the end, stopping on a missing or empty token (Go `len == 0`).
  Rows are accumulated in order and processed by the unchanged `load` logic.

Not changed: the Rust `load` treats a token row with a holder as "remove from free" in the loop; Go instead
deletes held slots after the loop. Both produce the same free set for the tested inputs; not part of these two tests.

## Tests

- `matching_semaphore_new_manager_validates_params` (un-ignored): five cases mirroring Go's table
  (ID domain, ID name, ID bucket, TTL zero, TTL negative), each asserting `BadRequest`.
- `matching_semaphore_scan_threads_page_tokens` (un-ignored): three pages; asserts the first call has no
  token, later calls carry `page-N` (as Go `expectScan`), three scans total, free = {1,3}, held = {owner-x:2, owner-y:4}.
- `ScriptStore` gained `with_pages`; `new` is a single page, so existing tests are unchanged.
- `matching_engine_go_tests_d.rs` `SemaphoreStore` returns one page with no token.

## Verification

No local cargo (per instructions). Verification is GitHub Actions `rust-ci.yml`:
- package run `cadence-matching` on `matching-semaphore-params-20261024`: pending (see final report for run id/result)
- whole-workspace run on the final head: pending
