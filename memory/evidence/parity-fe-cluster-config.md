# parity-fe-cluster-config (2026-11-20)

Follow-up from parity-domain-v1: the Rust Frontend kept global isolation groups only in memory and never used `cluster_config`.

## Change
- New `cadence-persistence/src/isolation_group_store.rs`: a port of Go `configstore.configStoreClient` for config type
  `GlobalIsolationGroupConfig` (row_type 1), key `system.defaultIsolationGroupConfigStoreManagerGlobalMapping`.
  - Initial fetch at construction; if it fails, the store is not created and Get/Update return BadRequest "global isolation group drain is not supported in this cluster".
  - Background poll every `system.isolationGroupStateRefreshInterval` (default 30s). Retries come from `system.isolationGroupStateUpdateRetryAttempts` (default 2),
    with `system.IsolationGroupStateFetchTimeout` and `UpdateTimeout` (default 30s). The client config is checked the same way as Go's `validateClientConfig` and falls back to `defaultConfigValues`.
  - Update writes version cached+1 as a thriftrw DynamicConfigBlob. On ConditionFailed it refetches and retries, then returns "ran out of retry attempts on update".
    If the call outlives UpdateTimeout it returns "timeout error on update".
  - Mappers match Go `MapUpdateGlobalIsolationGroupsRequest` (`[{"Name":..,"State":n}]`, sorted by name) and `MapDynamicConfigResponse`.
    NotFound gives an empty response, and other errors are prefixed with "failed to get global isolation groups from datastore:".
  - SQL reuses the existing Go-identical MySQL/Postgres `cluster_config` select/insert (`MatchingTaskStore::{fetch_latest_config,insert_config}`).
- `InMemoryDomainStore`: `attach_global_isolation_store`, `get_/set_global_isolation_groups`. `global_isolation_groups()` (used by the FE drain check)
  reads the store when one is attached. Without one (embedded/in-memory deployments) the old in-memory behaviour is unchanged.
- service-core admin handlers return Go's errors (BadRequest / InternalServiceError). The write runs in spawn_blocking.
- matching launcher (shared FE launcher): when `run_frontend` and persistence stores are configured, the store is created and polled
  (Go `createConfigStoreOrDefault`).
- Unit tests: persist and share across two clients, poll visibility, version-conflict retry, nil update gives an empty config, mapper errors, client config fallback.

## Validation (head 4c3fe563624d7ce6c13ab4b3a4bce28e3a486b86)
- rust-ci workspace (clippy + tests): https://github.com/auenkr/cadence-rust/actions/runs/44673654139 success
- frontend-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44673658930 `GATE SUMMARY failed: []`, binary sha256 b33e2e401a9fbab0b0b00d7ed077a1d48806199f0dfc111bfb4dbbfd8293940d
- frontend-mixed-gate Postgres: https://github.com/auenkr/cadence-rust/actions/runs/44673668535 `GATE SUMMARY failed: []`, binary sha256 5961a1f9e266277052218661cc50697bb322c736e4e343d7d78994a74bc26383
- Earlier failing attempts (rustfmt, non-exhaustive enum, clippy) fixed: 44672427941, 44672616181, 44673121130, gates 44672473223/44672484532 (build failed).
- Benchmark cluster_config SELECT-rate comparison (agg.py select_by_table): NOT run. Expected now: about 1 row_type-1 SELECT per 30s per FE, same as Go.

## Gaps
- Rust Matching standalone still keeps global isolation groups in memory. Go creates this store in every service; this change ports it for the Frontend only.
- The gates only exercise GetGlobalIsolationGroups. There is no mixed Rust-FE update → Go-service read test.
