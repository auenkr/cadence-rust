# Matching domain cache RPC error parity (2026-10-27)

Commit: `ef53987ee1176a4a4bfb5b321c9e376590fbccec` (`fix(matching): preserve domain cache error types`).

`Matching::domain_id_by_name` now preserves the cache's typed error classification. Empty names map to InvalidArgument (`Domain name is empty`); source NotFound maps to NotFound with the `EntityNotExistsError` detail; source BadRequest maps to InvalidArgument; untyped and other classified source errors map to Internal. An existence check followed by a cache refresh that still cannot load the domain maps to Go's exact InternalServiceError message without appending the requested identity. Stopped cache remains Internal. The blocking-task panic resumption and no-cache in-memory fallback are unchanged.

Go evidence: `go/service/matching/handler/context.go` passes through `BadRequestError` and `EntityNotExistsError`, while its default branch returns `InternalServiceError{Message: err.Error()}`. `go/common/cache/domainCache.go` gives empty name and the exists-but-not-loaded message. `go/common/types/mapper/proto/errors.go` maps entity missing to NotFound with the API detail and bad request to InvalidArgument.

Focused Rust regressions were added in `matching_cold_domain_metric_tests.rs` for empty name, typed source NotFound (including detail URL), and an untyped internal source failure. They exercise the real `ListTaskListPartitions` handler and the production cache. No local Cargo, Go, formatting, build, test, or runtime command was run.

Actions: [rust-ci run 40482240725](https://github.com/auenkr/cadence-rust/actions/runs/40482240725), dispatched for `cadence-matching` on the commit above. Conclusion is pending; it was not polled per task instruction.
