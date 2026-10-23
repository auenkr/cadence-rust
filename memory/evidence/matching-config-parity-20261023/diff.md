# Matching config / dynamic-config parity: Go vs Rust diff list (2026-10-23)

Branch `matching-config-parity-20261023`. Rust paths relative to `rust/crates/`, Go paths relative to `go/`.
Status: **fixed** = changed on this branch with unit tests; **documented** = deliberately not ported, behavior stated.

## 1. Dynamic-config providers (`common/dynamicconfig/dynamicconfigfx/fx.go:69-130`)

| # | Go | Rust before | Rust now | Status |
|---|---|---|---|---|
| 1.1 | Client names matched exactly (`fx.go:90-103`: `configstore`, `filebased`, `openfeature`) | `eq_ignore_ascii_case("filebased")`; any other name aborted startup (`launcher.rs:520-525` old) | `Config::dynamic_config_provider()` (`cadence-service-core/src/config.rs`): exact match | fixed |
| 1.2 | Unknown client name: `res == nil` -> `NewNopClient()` (`fx.go:121-123`) | startup error | `Nop`, logs "initialising NOP dynamic config client" | fixed |
| 1.3 | Empty client -> legacy `dynamicConfigClient` file-based (`fx.go:73,86-88`); missing legacy stanza fails to read -> Nop | same (Nop when absent) | same | unchanged |
| 1.4 | `configstore`: `ClientConfig{pollInterval,updateRetryAttempts,FetchTimeout,UpdateTimeout}` (`configstore/config/config.go`), invalid -> `defaultConfigValues` 10s/1/2ns/2ns (`config_store_client.go:66-71,108-111`), min poll 2s, retries >= 0, timeouts > 0 (`:604-621`); polls default datastore rows; creation failure -> Nop (`fx.go:123-127`) | not parsed | parsed + validated + defaulted (`ConfigStoreClientConfig`); runtime refuses to start with a message stating what Go does (no config-store client in Rust; falling back to Nop would silently diverge from a Go cluster that reads rows) | documented |
| 1.5 | `openfeature`: `{providerName, provider(yaml node)}` (`openfeatureclient/config`); provider plugin registered on start (`fx.go:103-119`) | not parsed | parsed; runtime refuses to start with explicit message (no provider plugins ported) | documented |
| 1.6 | filebased `pollInterval` is a `time.Duration` (YAML int = nanoseconds), min 5s (`file_based_client.go:42,373-375`) | String only: integer YAML failed to parse | integer accepted (`de_duration_string`), min 5s (already in launcher) | fixed |
| 1.7 | filebased: missing file at start -> error -> Nop (`:68-78`); later invalid update keeps old values and logs (`:82-90`); mtime-gated reload (`:249-255`) | same behavior (`matching_runtime.rs:374-381`) | unchanged | verified by reading |
| 1.8 | Relative path is `rootDir + "/" + path` (`fx.go:156-161`) | `process_root.join` | unchanged | same |

## 2. `matching.enableTaskInfoLogByDomainID` (`service/matching/handler/engine.go:1588-1598`)

Go: `emitInfoOrDebugLog` logs Info when `system.enableDebugMode` (read once at config build, `config/config.go:217`, default false) **and** `matching.enableTaskInfoLogByDomainID` (filter `domainID`, default false, per call) are true, else Debug.
Rust: new `matching_task_info_log.rs`; installed in `launcher.rs` after dynamic config loads. Info events printed to stderr; Debug events dropped (Rust has no debug level). New reader `cadence_persistence::dynamic_config::boolean_domain_id` for the `domainID` filter.
Call sites ported (Go engine.go line -> Rust):
- 414 AcquireSemaphore "Received AcquireSemaphore" -> `grpc_matching.rs` acquire_semaphore (fields wf-id, wf-run-id, wf-domain-id, wf-schedule-id=hold id)
- 522, 605 "Received AddDecisionTask/AddActivityTask" -> `log_received_add_task` (wf-task-list-name, wf-id, wf-run-id, wf-domain-id, wf-task-list-type, wf-schedule-id, wf-task-list-kind in Go enum numbering = proto-1)
- 550, 633 "Emitting tasklist counter on decision/activity task" -> `log_tasklist_counter` (wf-task-list-name, taskListBaseName)
- 813/965 "Decision task dropped because domain is not active" (Go uses the decision text for both) and 837/990 "Duplicated decision/activity task" -> `matching_dispatch.rs` delivery loop `log_task_event` (wf-domain-id, wf-id, wf-run-id, wf-task-list-name, wf-schedule-id, queue-task-id, + error on duplicates)
Not ported: none of the 10 Go call sites is skipped. Filter domain on poll-path events is the task's own domain, tag `wf-domain-id` the poll's domain, as in Go.
Status: fixed (unit-tested filter/enable logic; fields assembled in handlers, not asserted by test).

## 3. Error injection

`matching.errorInjectionRate` (`MatchingErrorInjectionRate`, constants.go:5424) is **not** the persistence key at `common/persistence/client/factory.go:197-199`. That site reads `ErrorInjectionRate()` = `system.persistenceErrorInjectionRate` (`persistence/config.go:66`). Two separate injectors apply to Matching:

| Key | Go site | Applies to Matching | Rust |
|---|---|---|---|
| `system.persistenceErrorInjectionRate` | `factory.go:195-199` wraps the TaskManager (inside rate limiter, outside nothing else); also wraps other managers Matching builds (domain, config) | yes, task store | `matching_fault_injection::InjectingTaskStore`, wrapped before the QPS limiter in `launcher.rs`; 30s startup grace (`errorinjectors/utils.go:generateFakeError`); fake set {ServiceBusy, Internal, Timeout, Unhandled}; Timeout/Unhandled forwarded 50%; always returns fake error; logs "Injected fake persistence error" with `store-operation`, `error`, `bool`, `store-error` |
| `matching.errorInjectionRate` | `client/clientfactory.go:183-185` wraps the Matching client (`errorinjectors/matching_generated.go`); no startup grace | yes, Matching->Matching forwarding client | `inject_client_call` at the poll forward (decision/activity) and the backlog add forward in `matching_dispatch.rs`; rate read once at startup |

Both rates are read once at construction, as in Go (`GetFloat64Property(...)()` called when the factory builds the object).
Gaps: Go also injects on the domain/config managers and on every other Matching-client call (QueryWorkflow/DescribeTaskList cross-node, partition config RPCs) and on the History client via `history.errorInjectionRate`; Rust injects the task store and the two forward paths only. History-client injection is History's key, not Matching's. Domain-store injection not done (**documented**). Rust maps Unhandled to `BackendErrorKind::Permanent` and fake Timeout (client) to gRPC DEADLINE_EXCEEDED; wire-error identity on a Go peer not compared.

## 4. Static config (`common/config`)

| # | Go | Rust before | Rust now | Status |
|---|---|---|---|---|
| 4.1 | Env expansion = go.uber.org/config v1.4 `expand.go`: `${K}`, `${K:default}` (default = everything after first `:`), `""` = empty, `$K` with `[A-Za-z_][A-Za-z0-9_]*`, `$$` -> `$`, unset without default is an error `default is empty for "K"` (`expand.go:55-84,124-253`) | `${K:-d}` special-cased (Go yields `-d`), unset -> empty string, `$$` unchanged, `$1` expanded, unterminated `${` swallowed rest | `expand_with` implements Go rules | fixed |
| 4.2 | Expansion runs after YAML merge which strips comments (`config.go:85-95`) | comments expanded | text after a comment `#` is not expanded (per-line, quote aware; block-scalar content with ` #` treated as comment: edge case, **documented**) | fixed |
| 4.3 | Layered load `base.yaml`, `<env>.yaml`, `<env>_<zone>.yaml`, later wins, none found -> error; env default `development`, dir default `config` (`loader.go:67-146`); env vars `CADENCE_CONFIG_DIR/ENVIRONMENT/AVAILABILITY_ZONE(+legacy misspelling)/ROOT` (`loader.go:32-42`) | single `--config` file | `--config <dir>` + `--env/--zone` (env vars `CADENCE_ENVIRONMENT`, `CADENCE_AVAILABILITY_ZONE`, `CADENCE_AVAILABILTY_ZONE`) -> `Config::from_layers`; maps deep-merged, scalars/sequences replaced. `CADENCE_CONFIG_DIR`/`CADENCE_ROOT` not read (no flag default); file `--config` unchanged | fixed (partial) |
| 4.4 | `validate:"nonzero"` on `Persistence.DefaultStore`, `NumHistoryShards` (`config.go:172,188`) | only `< 0` rejected | `PersistenceConfig::validate_go` rejects zero/empty (called in `launcher.rs` start and `check-config`) | fixed |
| 4.5 | `Persistence.Validate` (`persistence.go:75-176`): visibility or advancedVisibility store required, missing datastore, exactly one of NoSQL/ShardedNoSQL/SQL, SQL databaseName/connectAddr (not sqlite), multi-DB rules, ShardedNoSQL defaultShard | none | ported (`validate_go`, `validate_sql`) incl. `advancedVisibilityStore`, `cassandra`, `shardedNosql` keys | fixed |
| 4.6 | ShardedNoSQL history-shard-mapping / tasklist-shard validation (`persistence.go:149-175`) | n/a | not ported (Rust Matching has no sharded NoSQL store) | documented |
| 4.7 | `FillDefaults`: `cassandra` -> `nosql` with plugin `cassandra`; SQL `nShards` 0 -> 1; SQL `encodingType`/`decodingTypes` default ThriftRW (`persistence.go:46-72`) | none | cassandra promotion and nShards default done; encoding types are not modeled in `SqlConfig` | fixed (partial) |
| 4.8 | `Authorization.Validate` (oauth+noop both enabled, MaxJwtTTL > 0, creds) (`authorization.go:66-90`) | Authorization not modeled | not applicable to Matching (Frontend only) | documented |
| 4.9 | `PublicClient` default HostPort/transport (`config.go:564-572`) | not modeled | Matching has no public client use here | documented |
| 4.10 | `ClusterGroupMetadata.Validate/FillDefaults` | present (`validate()`, `fill_defaults()`) | unchanged | same |

## Tests (unit)
`cadence-persistence`: `dynamic_config::tests::domain_id_filtered_boolean_and_plain_float_readers`.
`cadence-service-core` `config::tests`: `env_expansion_follows_go_uber_config`, `env_expansion_skips_yaml_comments_but_not_quoted_hashes`, `layered_config_merges_base_env_and_zone_like_go_loader`, `dynamic_config_provider_selection_matches_dynamicconfigfx`, `persistence_validation_follows_go_validate`.
`cadence-matching`: `matching_fault_injection::tests::*` (4), `matching_task_info_log::tests::info_requires_debug_mode_and_domain_id_flag_otherwise_dropped`.
No runtime (mixed-cluster) evidence: unit tests only.
