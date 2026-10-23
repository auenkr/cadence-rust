# Matching `dynamicconfig.client: configstore` (P0-3 follow-up)

Branch `matching-configstore-dyncfg-20261023` (from `matching-tls-dyncfg-20261023`).

## Implemented
- `cadence-persistence/src/config_store.rs`: Go `config_store_client.go` read path.
  ThriftRW / thriftrw_snappy `DynamicConfigBlob` decode (wrong-typed fields skipped as Go's
  generated decoder), `ClientConfig` (+ Go defaults, 2ns timeouts), `ConfigStoreClient`
  (`new` = first `update`, failure => caller falls back to NOP; not-found row keeps cache;
  fetch error keeps old snapshot; fetch longer than FetchTimeout is "timeout error on fetch").
- `MatchingTaskStore::fetch_latest_config` for SQLite, MySQL, PostgreSQL
  (`SELECT ... FROM cluster_config WHERE row_type=? ORDER BY version LIMIT 1`, stored version
  is negated, sign restored) and Cassandra (`version` DESC clustering key, `LIMIT 1`).
- Launcher: `configstore` builds the client from the default datastore, loads the first
  snapshot before serving; creation error => warning + NOP (Go fx behavior).
  `MatchingRuntime::with_dynamic_config_store` polls every `pollInterval`.
- `DynamicConfigOverlay::from_config_store` feeds the same property resolution as filebased.

## Not ported / differences
- `UpdateValue`/`RestoreValue` (write path, updateRetryAttempts/UpdateTimeout) are admin APIs,
  not used by Matching; not ported.
- JSON-encoded config blobs (`json`/unknown encodings) return an error; Go writes only thriftrw.
- The config-store connection reuses the Matching default-store pool code (not a separate DB handle).
- Value filter matching uses the existing shared overlay semantics, not Go configstore `matchFilters`
  (which requires `len(filters)` >= value filters and compares decoded values).
- Not run locally; no live MySQL/Postgres/Cassandra test.

## CI
(filled after run)
