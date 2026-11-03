# Frontend: Kafka + archival gates (Postgres, Cassandra), ES mapping, retry/visibility tests (2026-11-03)

Branch `frontend-kafka-archival-es`, final head `49f3ec988e53750e86ecaa73713bc4e460dfc619` (plus this evidence commit).

| Check | Run | Head | Result |
|---|---|---|---|
| rust-ci (clippy + workspace tests, fresh target) | https://github.com/auenkr/cadence-rust/actions/runs/41572812796 | 49f3ec988 | success |
| Frontend mixed gate, Postgres, kafka=true | https://github.com/auenkr/cadence-rust/actions/runs/41573628343 | 49f3ec988 | success, 112 steps, 0 failed |
| Frontend mixed gate, Cassandra, kafka=true | https://github.com/auenkr/cadence-rust/actions/runs/41573623671 | 49f3ec988 | success, 112 steps, 0 failed |

Rust cadence-server SHA256: Postgres `1959ef144dbbeb44d1bd269949bb5a108c7d799d7c0909df8a737b7142839a2e`, Cassandra `4097c1824e69baa002e50f92f0eefcd43fa877f861d587e8d20a9ab3aa7bbb7d`.

Both lanes ran the Kafka async-workflow steps (`kafka-broker-stopped/restarted`, `sdk-async-*`) and all 15 `archival-*` steps (start, list-archived, paged, go-visibility-wrote-archive, update-uri-rejected, disable, domain-not-configured).

## Bugs found and fixed
- Cassandra gate (earlier run 41566906769) failed: after `cli-admin-async-config-update-grpc` Go ListDomains failed with "Invalid binary encoding version". Cause: `matching_domain_cassandra.rs::serialize` wrote domain UDT blobs (async_workflow_config, bad_binaries, isolation_groups, active_clusters_config) without Go's ThriftRW 0x59 version byte. Fixed with `finish_blob` (0x59 before optional snappy) + unit test `domain_blobs_carry_go_thriftrw_version_preamble`.
- Frontend reads `system.enableReadFromClosedExecutionV2` once at visibility-store construction (Go client/factory.go) and passes it to Cassandra visibility.
- CI-only compile/clippy/fmt fixes: open `IndexedValueType` enum arm, clippy field-reassign in test, rustfmt diffs.

## New tests
ES mapping (`es_mapping.rs`, fake HTTP server: v7 / v6 request shape, 404 -> create index -> retry, 500, unacknowledged, create failure, unsupported version, no advanced visibility); handler_admin `add_search_attribute_updates_es_mapping_and_surfaces_failures`; History retry loop (5 tokio tests in `workflow_backend.rs`); Cassandra visibility row decoding (3 tests).

## Limits
ES/OpenSearch mapping update is proven only against a fake HTTP server; no gate runs a real Elasticsearch/OpenSearch, so `ListWorkflowExecutions`/Scan/Count against advanced visibility remain out of scope. Kafka was exercised on Postgres and Cassandra with one History instance (two_history not run here).
