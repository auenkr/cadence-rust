# Native configured Matching store selection — 2026-10-28

ConfiguredMatchingStores now exposes `native_task_store` alongside the legacy view. SQLite, MySQL, PostgreSQL and Cassandra constructors clone the same concrete backend Arc into both interfaces; they do not open a second database, connection pool or Cassandra session. TLS/sharding/encoding/config validation remains in the existing constructor. This makes the byte-preserving backend available to native runtime construction, which remains outstanding. The legacy launcher still consumes its legacy field.

Added a configured SQLite regression proving leases written via either view appear through the other, plus a raw-byte queue lease through the native view. Source inspection and `git diff --check` only; CI required. Native error-injection and QPS wrapper installation in the launcher remain necessary before production adoption; exposing the raw field does not establish parity.
