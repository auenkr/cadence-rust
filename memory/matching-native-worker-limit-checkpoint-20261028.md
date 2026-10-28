# Matching checkpoint after worker usage limit — 2026-10-28

The native semaphore manager worker terminated on its provider usage limit. It left `rust/crates/cadence-matching/src/matching_semaphore_manager_native.rs` (656 lines at recovery). Preserve this as an unfinished, unregistered draft, not an implementation completion. It has no worker completion review, tests or compilation evidence. Review raw error representation, startup/stop behavior, owner fencing, deadline handling and registry replacement before adoption. No replacement provider was selected.

Fresh five-crate CI https://github.com/auenkr/cadence-rust/actions/runs/40692116369 at source `2d91ca2e12bc14409cddddb772062f55ffb88b31` concluded **failure** in clippy: the `forward_degree` closure in `matching_runtime_native.rs` did not implement sufficiently general reference lifetimes. Added explicit `&GoString`, `&GoString`, `i32` argument types following the compiler suggestion. This correction requires a fresh CI run; no local Cargo, formatting or Go validation was run.

All thirteen native RPC trait methods exist in source, and native gRPC/Thrift attachment seams exist. The launcher still needs a complete native integration provider and runtime lifecycle assembly. Semaphore persistence/manager integration, production scaler/observers/forwarding, fresh workspace validation, and mixed Go/Rust SDK/failure/restart/recovery evidence remain outstanding. Matching is incomplete; no work on the next service is authorized by this checkpoint.

The semaphore persistence and engine context delegates were still outstanding when recovering the failed manager. Their shared-checkout files must not be treated as reviewed or completed solely because they are present.

Fresh five-crate CI dispatched: https://github.com/auenkr/cadence-rust/actions/runs/40700177421 at `c03ba2c9ebcf17097cac86618205689b40d5c17d`, clean target and runner formatting enabled. Initial status queued; conclusion pending. The manager draft remains unregistered and therefore is outside this compile gate.
