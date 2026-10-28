# Native RPC and query forwarding checkpoint — 2026-10-28

All thirteen native service trait methods are implemented in source, including required semaphore dispatch through a host integration callback. This does not establish a constructed production host: concrete lifecycle/quota/metrics/poll/add/query contexts and a native semaphore runtime remain required before launcher attachment. The registered service is checked by CI; no legacy protobuf conversion fallback was added.

Added NativeQueryParent implementations for local and remote native adapters. Local errors retain original typed status metadata; remote calls use native protobuf, deadlines and YARPC metadata, returning parent Status errors for existing ServiceBusy slowdown handling. Extended the bounded scripted adapter test with raw query request/reply bytes and typed ServiceBusy slowdown. Existing grpc_matching query forwarding has no client fault wrapper, so none was invented for this method.

Integrated/corrected the concrete control context and semaphore identifier/owner primitives; their notes list bounds. Added Go strconv.Quote byte behavior for owner errors using the same Go 1.25 printable tables as Matching panic quoting. No local Cargo, Go or formatter ran.

CI https://github.com/auenkr/cadence-rust/actions/runs/40689024481 at `8cec170a52a7de7fe66c1b98615e07b69dc4da1e` concluded failure: missing writer-live closure type annotation and a non-Send query future retaining a MutexGuard across await. Corrected the writer types and used a lexical lock scope for the forwarding decision; no handoff policy change intended. Applied runner formatting excluding edited runtime/query/service/forwarding/lib files. Next fresh CI must validate this source. No overall pass or service completion claimed.

Next: construct host contexts and wire runtime/launcher lifecycle; finish semaphore persistence/manager; attach scaler/isolation/metrics and forwarding/refresh callbacks; pass fresh workspace clippy/tests and mixed Go SDK, failure/restart/recovery gates with exact source and binary hashes. Matching remains current and incomplete.

Fresh five-crate runner CI https://github.com/auenkr/cadence-rust/actions/runs/40692116369 is dispatched at `2d91ca2e12bc14409cddddb772062f55ffb88b31`, clean_target=true and runner formatting enabled. Pending, not a pass. Next three Luna assignments target semaphore byte store/SQL unsupported behavior, native semaphore manager, and concrete poll/add/query contexts; parent retains launcher/host assembly and CI.
