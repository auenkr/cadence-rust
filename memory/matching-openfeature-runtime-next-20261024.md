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

## Implementation progress (2026-10-24)

Source branch: `t3/matching-runtime-provider-reads`.

### Runtime integration

- Persistence owns `RuntimePropertyProvider`, `RuntimePropertyType`, `RuntimePropertyRead`, and owned `PropertyValues`. `PropertyValues` has no `Deref`; snapshot inspection requires an explicit `snapshot_values()` call. Owning ordered values lets callbacks release overlay locks while retaining the property key and provider handle.
- The Matching launcher attaches `OpenFeatureRuntimeProvider` only for the OpenFeature dynamic-config source. File and config-store sources keep their ordered snapshot path. No OpenFeature polling/snapshot overlay was added.
- Typed integer, boolean, float, duration, string, list, and map evaluations preserve the property type and default. Duration uses `OpenFeatureClient::get_duration_value`, returning nanoseconds and preserving parse/type errors for default fallback.
- Startup-only `system.enableGRPCOutbound` and `system.enableDebugMode` selectors read through the runtime handle. Without a runtime provider they retain their prior last-decodable unfiltered bool/int selection behavior.
- Two task-list callbacks that copied raw slices now carry the owned `PropertyValues` handle. Persistence quota readers use typed getters and do not bypass an empty snapshot when a live provider is installed.

### Consumer audit

- Audited the 57 pre-existing Matching `property_values` call sites in `grpc_matching.rs`, `launcher.rs`, `matching_runtime.rs`, `matching_task_info_log.rs`, `matching_throttled_logger.rs`, `matching_persistence_limiter.rs`, and the Matching RPC test module. Production typed reads route through persistence accessors; test-only snapshot inspection explicitly calls `snapshot_values()`.
- Filter construction covers `domainName`, `domainID`, `taskListName`, and persistence `taskType` (decision `0`, activity `1`) at the corresponding typed getters. Matching's Go config has no cluster-filtered property getter and no map-valued runtime property consumer. `system.allIsolationGroups` is the custom list consumer and uses the list-typed OpenFeature path. The adapter supports typed map defaults as well.
- File/config-store ordering and fallback selection remain unchanged in typed snapshot readers. Startup selectors preserve their specialized `next_back` behavior for the last decodable unfiltered bool/int. Intentional admin/test snapshot inspection is explicit.

### Tests and CI

- Added a `FeatureProvider` integration test reached through the real OpenFeature client, runtime adapter, and Matching accessors. It records flag names, typed defaults, and flattened attributes; checks live domain changes, domain/task-list/task-type isolation, the isolation-group list, provider failures, type-mismatch fallback, and shutdown.
- Repo-wide `cargo fmt --all -- --check` reports existing formatting debt, so CI keeps that check advisory and non-mutating, matching `origin/master`. A hard `rustfmt --check` step checks changed runtime source files before Clippy/tests; it does not rewrite the checkout.
- After merging current `origin/master`, Matching package CI passed on source SHA `bb08503dc3eded7704410736ba70b86825c5248c`: https://github.com/auenkr/cadence-rust/actions/runs/40086417167 (`cadence-matching`, fresh dedicated target, changed-source formatting check, Clippy, tests).
- Fresh whole-workspace CI passed on source SHA `bb08503dc3eded7704410736ba70b86825c5248c`: https://github.com/auenkr/cadence-rust/actions/runs/40086744572 (uncached dedicated target, workspace Clippy then workspace tests).

### Reconciliation with `t3/matching-live-reader`

Read-only comparison used final branch HEAD `4444c5208bc254ab13b5df581e2362abb8431399`; its reported fresh workspace run validated code SHA `b6c6a851c7f97aaf819fdd1002887ceb7922595e` at https://github.com/auenkr/cadence-rust/actions/runs/40085599877. That run is evidence for the other branch only; this branch is validated independently after reconciliation.

- Kept this branch's explicit `RuntimePropertyType` and owned `PropertyValues` without `Deref`, as required. The other branch infers getter type from JSON and borrows its snapshot; its `Deref` was not copied. This branch clones the ordered snapshot into the wrapper so task-list callbacks can release the overlay lock and retain the key/provider handle.
- Integrated its missing `clusterName` context: the launcher passes configured current-cluster metadata to the runtime adapter, which adds `clusterName` without overwriting an explicit attribute. The production-path FeatureProvider test asserts that context.
- Integrated its Go duration-default coverage without type inference: the explicit `Duration` branch calls `get_duration_value`; the actual provider test asserts `5m0s` is sent as the OpenFeature string default for a 5 minute duration.
- The completed branch's client-level recorder test was not copied. This branch's existing Matching accessor integration test already drives a registered FeatureProvider through the actual client and adapter; it now asserts cluster and duration details in that same test, avoiding a duplicate test harness.
- Its `matching_runtime` borrowed-lock changes are equivalent in filter/key semantics; this branch keeps owned wrappers so provider identity survives snapshot copies. Its launcher bool/int accessors use the generic snapshot selection; this branch retains the prior specialized last-decodable-unfiltered fallback for file/config-store startup selectors.
- Custom map-valued runtime consumers were not found in the Go Matching source. `system.allIsolationGroups` is the custom list consumer and is exercised through the adapter with live list changes. No unfiltered map snapshot or synthetic filter value was introduced.
