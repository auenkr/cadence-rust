# Matching OpenFeature configuration parity work (2026-10-24)

## Branch review

Reviewed `origin/matching-openfeature-20261024` at `fe5e6c810`. That tip contains documentation/evidence only; its prior commit `2d398db9d` contains a reusable Rust port of the OpenFeature client semantics, provider registry, and Unleash provider config validation. The code was inspected before reuse. The branch's own notes correctly identify that Unleash's evaluation engine and Matching's filter-aware reader integration were still missing.

## Go behavior inspected

`go/common/dynamicconfig/dynamicconfigfx/fx.go:103-119` selects a dynamicconfig client and adds lifecycle hooks: `RegisterProvider(ctx, providerName, providerConfig)` on start and `DeregisterProvider()` on stop. The OpenFeature client evaluates typed values using the caller's defaults and maps filter values to targetless evaluation-context attributes. `go/common/dynamicconfig/openfeatureprovider/provider.go` is a named constructor registry. Go's server links Unleash by blank import; `unleash/init.go` registers it during process initialization. `unleash.go` decodes `provider:` settings, requires URL and appName, reads optional bootstrap data, then creates the SDK provider.

## Current implementation in this branch

- Reused the prior branch's `cadence-service-core::openfeature` client, provider registry, and Unleash config work, including its unit tests.
- Added launcher-side Unleash plugin registration and OpenFeature RegisterProvider/DeregisterProvider lifecycle ownership. Provider startup errors propagate instead of silently switching to no-op.
- OpenFeature remains **incomplete** and must not be described as parity-complete. The Rust Unleash constructor currently stops after config/bootstrap validation because the Go `unleash-client-go` engine is not ported. More fundamentally, Matching production readers still consume the legacy snapshot overlay; they do not call OpenFeature per lookup with domainName/taskListName/taskType/clusterName filters. Registration alone must not be mistaken for working feature evaluations.
- Other arbitrary Go OpenFeature plugins are not dynamically loadable in Rust; the Rust binary must statically register a Rust constructor. This branch registers only Unleash.
- Provider startup timeout/context cancellation is not yet equivalent to Go's fx OnStart context.

## Validation

Pending GitHub Actions runs for focused `cadence-matching` and a fresh clean-target workspace clippy + tests. Record run URLs, exact head SHA, and conclusions below before claiming validation.
