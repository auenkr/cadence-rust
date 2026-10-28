# Native Matching late router attachment — 2026-10-28

Implemented shared live router storage on NativeQueueRegistry, shared by its clones. Runtime::with_shard_router publishes the discovered router to the native registry, while retaining existing legacy attachment. MatchingShardRouter is cloneable with shared peer cache and live property callbacks.

Native control owner lookup reads the registry router for each call. Native host binds its add/query context to that same registry; standalone engine context construction retains its prior optional-router fallback. Registry parent forwarding reads the shared slot at each resolution. Scaler partition describe reads the registry router with its previous supplied router as fallback. All read locks are released before asynchronous owner lookup.

Added regression source checking that a registry clone created before attachment observes both late router installation and replacement. This closes the frozen startup-router gap in these consumers; it does not activate the production native launcher or complete async ownership admission. No local Cargo, formatting, build or tests were run. git diff --check passed; CI validation is still required for this source.

Production gaps still include installing the real native registry, acyclic local parent service builder, partition refresher, scaler and observation callbacks in the launcher, and proving configured shard-executor ownership behavior. Then full workspace and mixed Go/Rust SDK/failure/recovery evidence and parity audit are required.
