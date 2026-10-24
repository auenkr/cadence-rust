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
