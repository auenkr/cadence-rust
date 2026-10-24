# OpenFeature dynamic config in Rust Matching — notes (2026-10-24)

Branch: `matching-openfeature-20261024` (from `origin/master` e7285da4f). Not merged.

## Status: PARTIAL PORT (foundation only)

Matching still bails on `dynamicconfig.client: openfeature`
(`rust/crates/cadence-matching/src/launcher.rs`, the `DynamicConfigProvider::OpenFeature` arm).
That is deliberate: wiring a client that Matching's readers never consult would
silently serve defaults, which is worse than the explicit bail.

| Piece | Go source | Rust | State |
|---|---|---|---|
| Client semantics (getters, `translateErr`, no-op default, write ops) | `openfeatureclient/client.go` | `cadence-service-core/src/openfeature/client.rs` | ported, unit tests mirror `client_test.go` |
| RegisterProvider / DeregisterProvider (registerOnce, refcount, shutdown) | `client.go:105-166` | `client.rs` `register_provider` / `deregister_provider` | ported |
| Provider contract + plugin registry | `openfeatureprovider/provider.go` | `openfeature/provider.rs` | ported, tests mirror `provider_test.go` |
| Unleash config decode / validation / bootstrap read | `unleash/init.go`, `unleash/unleash.go` `newProvider` | `openfeature/unleash.rs` | ported up to the engine |
| Unleash polling, strategies, constraints, variants, payloads | `unleash-client-go` v4.5.0 + `go-sdk-contrib/providers/unleash` | — | NOT PORTED |
| Matching reader wiring (per-call evaluation) | `dynamicconfigfx/fx.go:103-119`, `common/dynamicconfig/config.go` Collection | — | NOT PORTED |
| Launcher wiring | `dynamicconfigfx/fx.go` | — | NOT PORTED (bail kept) |

## Go behaviour confirmed from source (the parity contract)

- `translateErr` maps only `FLAG_NOT_FOUND` to `dynamicconfig.NotFoundError`. Any other code returns the error
  wrapped as `error code: CODE: message`. Every getter returns the caller's default on error.
- The go-sdk noop default (no provider installed, or after shutdown) returns the default with **no** error.
- memprovider: missing flag → `FLAG_NOT_FOUND`; disabled flag → `GENERAL: flag is disabled`; wrong type → `TYPE_MISMATCH`.
- **The unleash contrib provider never emits `FLAG_NOT_FOUND`.** `BooleanEvaluation` for a missing flag
  returns `IsEnabled(flag, WithFallback(default))` with no error. So in Go, a missing unleash flag yields
  the default with no error, and `NotFoundError` is never produced by unleash. Verified in
  `go-sdk-contrib/providers/unleash@v0.1.1-alpha/pkg/provider.go`.
- Contrib `IntEvaluation` / `FloatEvaluation` accept string payloads and parse them; `StringEvaluation` uses `fmt.Sprint`.
  Not yet ported (engine gap).
- `GetDurationValue` sends `time.Duration.String()` as the default string; `go_duration_string` mirrors Go's formatting.
- `toEvalContext` attaches each filter as an attribute keyed by the filter name (`domainName`, `taskListName`,
  `taskType`, `clusterName`) with no targeting key. Go's Collection always adds `clusterName` (`toFilterMap`).
- `yaml.Node.Decode` on a nil node returns nil, so a missing `provider:` block gives the zero config and then `requires url`.
- Go's `RegisterProvider` keeps the first outcome forever (`registerOnce`). A failed first registration
  is returned to every later caller.

## Known deviations / open decisions

1. **Init failure installs no provider.** Go's `SetProviderWithContextAndWait` may install the provider even
   when Init fails. Rust does not install it, so evaluation returns the default with no error. Unobservable in
   production, because a failed RegisterProvider fails fx start. Not verified against go-sdk internals.
2. **Duration type.** `get_duration_value` returns nanoseconds as `i64` (Go `time.Duration`), which allows negatives.
3. **Unleash `ListValue` / `Client` types.** Go returns `[]*types.DynamicConfigEntry`. Rust returns `Vec<Value>`.
   It always errors, so no entries are ever built.
4. **Startup timeout.** Go's RegisterProvider takes an fx OnStart ctx for the init timeout. Rust has no such
   parameter yet. Must be addressed when wiring the launcher.

## Next steps (required before the feature works)

1. Port the unleash-client-go evaluation engine: feature JSON, repository, polling (ETag), bootstrap storage,
   default / userWithId / gradualRollout* / flexibleRollout / remoteAddress / applicationHostname strategies,
   constraints (IN, NOT_IN, STR_*, NUM_*, DATE_*, SEMVER_*), segments, variants with weights/stickiness/payloads,
   and the contrib provider's type conversions. Port each unit test as well.
2. Refactor Matching's dynamic-config readers (~80 `property_values` call sites, `grpc_matching.rs` has 44) to
   evaluate per call with the filter context (domainName, taskListName, taskType, clusterName), instead of
   reading a snapshot slice.
3. Wire the launcher arm to `register_provider` with the Go startup ordering and timeout.
4. Mixed-cluster and failure tests against Go services (outside this PR).

## CI

See the run log section below (filled in after each dispatch).

## CI log

- `40067696233` cadence-service-core on `2d398db9d`: clippy failed (test compile: `Debug` for `dyn FeatureProvider`).
- `40067854462` cadence-service-core on `8be416d2f`: clippy failed (`approx_constant` on `3.14` fixture).
- `40067944129` cadence-service-core on `fce70363c`: **success**.
- `40068048379` whole workspace on `fce70363c`: failed in `cadence-matching` `matching_reader` test (`pump_falls_back_to_default_for_unknown_isolation_group`, left 2 right 1). Same `matching_reader` tests also failed on base `e7285da4f` (`40064723413`, `40066185225`) with different tests failing per run: pre-existing nondeterminism, not caused by this branch.
- Merged `origin/master` (4424a3b4d) into the branch, head `6f9934010`.
- `40069171737` whole workspace on `6f9934010`: **success** (final head is green).
