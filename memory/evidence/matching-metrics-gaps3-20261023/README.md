# Matching metrics gaps 3 (2026-10-23)

Branch `matching-metrics-gaps3-20261023` from origin/master `ddd0d59b6`; code commit `a525d6e29` (code unchanged since; this file adds evidence). Go services not involved; unit tests via the recording sink only, no mixed gate.

## Changes
- `HistogramSpec::{GoPersistenceLatency,GoResponseRowSize,GoResponsePayloadSize}` + bounds fns (defs.go PersistenceLatencyBuckets 55 duration buckets 1ms..60s; ResponseRowSizeBuckets 0,1..65536; ResponsePayloadSizeBuckets 0,1kB..512MB). Prom, statsd, M3 exporters handle them (value histograms carry no histogram_* tags, as tally fixed buckets).
- `persistence_latency_histogram_per_host` and manual `persistence_latency_histogram` now use GoPersistenceLatency.
- `persistence_response_row_size` (non-empty) / `persistence_response_payload_size` (always) with operation + task_category=none, for GetTasks and ListDomain.
- `BackendErrorKind::{DbUnavailable,BadRequest}`; Cassandra `internal()` maps gocql Unavailable to DbUnavailable (transient, like Go IsTransientError). Limiter emits `persistence_errors_db_unavailable[_per_domain]` (+ persistence_errors) and `persistence_errors_bad_request[_per_domain]`.
- Domain wrapper (GetMetadata/ListDomain/GetDomain) is now fully metered (requests, latency, histograms, errors); `DomainErrorClass` on DomainMetadataSourceError (NotFound, BadRequest for bad identity).
- Semaphore wrapper: Go has no metered semaphore manager (only defs.go mentions), so no change.

## Commands / exit codes (cwd rust/, private CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-metrics3)
| step | exit | note |
|---|---|---|
| check1-3 (shared target) | 101,101,101 | invalid: stale artifacts, see below |
| check4 | 0 | shared target, after touch |
| clippy -p matching,service-core,persistence --all-targets -D warnings (clippy3) | 0 | private target |
| test -p cadence-matching -p cadence-service-core --no-fail-fast (test3) | 101 | cadence-matching lib 397 passed, 1 failed, 2 ignored; service-core 96 + 27 passed |
| failing test alone (test-diag) | 0 | durable_cancel_unregisters_waiting_poller passes alone; known timing flake (2s timeout under parallel load), unrelated |

New tests passed: persistence_histogram_contract_tests (2), empty_rows_and_host_latency..., non_empty_list_emits_row_size_and_db_unavailable_and_bad_request_classes, domain_wrapper_calls_use_overall_family_with_task_category_none.

## Build finding
Worktrees sharing cargo-target-shared collide: cargo hashes workspace-member paths relative to the workspace root, so two worktrees reuse artifact names and a build links stale crates from the other worktree (test2.log: E0027 on code not in this branch; test.log/check1-3 errors for symbols that exist). Use a per-worktree CARGO_TARGET_DIR. Earlier shared-dir logs (check*, clippy.log, clippy2.log, test.log, test2.log) are not valid evidence.

## Remaining gaps
- ByteSize is modeled for GetTasks (exact amd64 constants) and approximated for ListDomain (config/replication parts not counted).
- Only GetTasks and ListDomain emit row/payload metrics; Go's other countable methods are not Matching callers.
- Per-domain-wrapper DBUnavailable for domain sources is not classified (only the task store Cassandra path).
- No mixed Go/Rust gate run.
