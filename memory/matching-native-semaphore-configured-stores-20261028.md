# Native semaphore configured store selection — 2026-10-28

Added open_configured_semaphore_stores returning legacy and native trait handles over one CassandraSemaphorePersistence and one configured session/keyspace. Existing open_configured_semaphore_store delegates and returns the legacy handle, preserving its public API. SQL selects the same unsupported adapter for both contracts. Ambiguous SQL+NoSQL configuration now fails consistently with task-store construction. No separate production memory database is introduced.

Added limit_semaphore_store_bytes with the same seven operation names, shared limiter budget and Throttled errors as the legacy semaphore wrapper. The raw domain/name/owner parameters and returned rows pass directly through. Invalid domain bytes are omitted only from rejection metric tags because the sink is UTF-8; they remain unchanged in persistence requests. The launcher must retain both factory handles, wrap each with the same limiter and pass the native handle to the native semaphore service; that attachment is not yet done.

Registered the completed host assembler for CI. Its note documents missing byte-valued quota/metrics/lifecycle integration; registration does not make it a complete production host.

CI https://github.com/auenkr/cadence-rust/actions/runs/40718207876 at 01d862f51cfe31c2fcb9cc5079cda6992f9af030 failed compiling Cassandra paging: Option<Arc<[u8]>> must convert to the native contract Option<Vec<u8>>. The active Cassandra source review owns that file. No local Cargo/Go/formatting/test ran.
