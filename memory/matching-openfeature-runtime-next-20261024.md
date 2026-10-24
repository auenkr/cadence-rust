# Matching OpenFeature runtime reads — next independent implementation

This remains required even after provider polling and Unleash evaluation are complete. Current launcher registers the provider but Matching consumes the snapshot overlay, so no deployment is OpenFeature-compatible yet.

Primary source pointers:
- service-core/admin.rs DynamicConfigOverlay (normal/operational HashMaps) and property_values(&self,name)->&[DynamicConfigValue].
- cadence-persistence/dynamic_config.rs typed integer/boolean/float/duration/string/string_list helpers and task_list_* variants. They implement ordered file/configstore selection and type-specific fallback.
- Matching modules have 57 property_values sites; matching_config.rs has property-specific defaults and wrappers that also accept slices. Some closures read dynamic properties on every call; others intentionally read once at startup (e.g. client error injection).
- service-core/openfeature/client.rs OpenFeatureClient typed getters already accept flag name, typed default and FlattenedContext. Duration parsing and unsupported admin mutations follow Go's OpenFeature client.
- Go common/dynamicconfig/collection.go and typed generated property functions supply domainName, taskListName, taskType, clusterName etc as filters. OpenFeature context has these attributes but no invented targetingKey.

Implementation requirement: introduce an explicit typed runtime provider/read abstraction, preserving the key and typed default through to evaluation. Keep snapshot selection behavior unchanged for file/configstore. Do not poll unfiltered flags into a snapshot: it cannot represent arbitrary filter-specific evaluations and defaults. Do not insert magic sentinel DynamicConfigValues or hide provider identity in filters. The persistence crate can own a small provider trait to avoid a dependency cycle; service-core owns the OpenFeature adapter. Refactor consuming Matching accessors as needed rather than silently deref-coercing a read wrapper back to a slice and losing the key.

Verification: fake FeatureProvider must record actual key, type/default and attributes; assert a production Matching runtime property changes on the next read without reloading a file; assert per-domain/tasklist/taskType isolation, failure/type-mismatch fallback, and shutdown. Inventory every current property consumer, including startup-only reads and custom map/list/cluster lookups. CI Cargo only. Record package and fresh whole-workspace source-SHA/runURL/conclusion. This task does not implement Unleash strategies; that is a separate worker.

## Implementation checkpoint (2026-10-24)

The saved WIP was reviewed and brought onto `t3/matching-live-reader` from `a69736bcd`; it was not merged to master. Its persistence-owned provider trait and service-core OpenFeature adapter are retained, with a typed `PropertyValues` read wrapper added. `DynamicConfigOverlay::property_values` now returns this wrapper so consumers retain the flag key, snapshot values and optional runtime evaluator. All typed dynamic-config helpers accept both old slices and the wrapper. Without an OpenFeature runtime evaluator, ordered file/config-store selection stays on the original snapshot path. With OpenFeature configured, the read sends the real key, typed default, filters and configured cluster name to the provider.

The 57 Matching source call sites of `property_values(` are all now routed through typed helpers, including startup-only reads, persistence limiter reads, task info by domain ID, task-list scoped partition/writer/QPS properties, map/list values and system properties. The two snapshot loops that would have dropped the key (launcher booleans/integers and shutdown drain duration) now use typed helpers. The grpc partition/QPS callbacks keep the wrapper alive under the overlay lock. There are no remaining production Matching `property_values(...).iter()/to_vec()` paths; remaining direct snapshot inspection is test-only.

`OpenFeatureRuntimeProvider` now passes `clusterName` from Matching config, while filter helpers provide `domainName`, `taskListName`, `taskType`, or `domainID` according to the Go reader used. The recording fake provider test asserts the real flag key and `Int(7)` default, exact Go duration default string (`5m0s`), domain/task-list/task-type/cluster attributes, different evaluations for two domain/task-list/type combinations, a changed value on the next call, provider-error and type-mismatch fallback, and shutdown on deregistration.

CI evidence (all `workflow_dispatch` on the public repository; Cargo was not run on the dev host):
- Matching package clippy/tests: source `b6c6a851c7f97aaf819fdd1002887ceb7922595e`, [run 40085211370](https://github.com/auenkr/cadence-rust/actions/runs/40085211370), conclusion `success`.
- Service-core package clippy/tests, including the recording fake test: source `b6c6a851c7f97aaf819fdd1002887ceb7922595e`, [run 40085435460](https://github.com/auenkr/cadence-rust/actions/runs/40085435460), conclusion `success`.
- Fresh uncached workspace clippy followed by workspace tests (`clean_target=true`): source `b6c6a851c7f97aaf819fdd1002887ceb7922595e`, [run 40085599877](https://github.com/auenkr/cadence-rust/actions/runs/40085599877), conclusion `success`.

Formatting note: a CI `cargo fmt --all -- --check` attempt was not clean; it reports existing workspace formatting drift beginning in unchanged `cadence-api` files. The final configured rust-ci gate validates clippy and tests. Formatting cleanliness for this change is not established.

No Matching production property consumer remains unwired within the audited 57 source call sites. Mixed-cluster and broader Matching parity/recovery gates remain separate service-completion work; this checkpoint only verifies the reader implementation and workspace build/tests. This branch is pushed at `b6c6a851c`; no changes are merged to master.
