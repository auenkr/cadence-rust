# Frontend Admin dynamic config / search attribute parity (2026-11-03)

Branch `frontend-admin-dynconfig` (from master). Evidence: CI run https://github.com/auenkr/cadence-rust/actions/runs/41531769629 (full workspace clippy+test, head 2b98dc61d6d5d8f754fe1bca06b30cdf28de572c, conclusion success on rerun of the failed job; first attempt failed only on a Go proxy download flake in the raw-string oracle step).

## Ported (Go: admin/handler.go + configstore/config_store_client.go)
- `cadence-persistence/src/dynamic_config_keys.rs`: all 506 KeyName/type entries generated from `dynamicproperties/constants.go` (script: parse `KeyName:` inside the Int/Bool/Float/String/Duration/Map/List key tables).
- `DynamicConfigOverlay::admin_get/update/restore/list` (service-core admin.rs): unknown key -> Internal "invalid dynamic config key name", empty name -> BadRequest "Request is nil.", `validateKeyDataBlobPair` messages, Go `matchFilters`, fallback = last filterless value, `NotFoundError` = EntityNotExists "unable to find key", restore ErrInvalidFilters, empty value list deletes entry, list of empty/unknown name returns all.
- Durable path: `StoreBinding` (fetch/write), `config_store::encode_snapshot` (byte-identical to Go for the seeded blob), `sqlite_insert_config` (negated version, duplicate = ConditionFailed), version+1 insert, refetch+retry on condition failure.
- All transports (axum JSON, YARPC Thrift, frontend Handler) route through these methods. AddSearchAttribute persists via the same path (failure only logged, like Go).

## Deliberately left out
- MySQL/PostgreSQL/Cassandra config-row inserts and binding a store in the production Frontend launcher (only SQLite writer + tests). Rows stay `partial`.
- Separate operational config store and its audit log (in-memory namespace only).
- Elasticsearch mapping update in AddSearchAttribute; GetCrossClusterTasks/Respond* (need cross-cluster History work, a different audit item).
- Go returns defaults when no snapshot is loaded; Rust has no default table, returns NotFound.
