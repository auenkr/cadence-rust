# Frontend dynamic config parity audit (2026-11-21)

Source: `go/service/frontend/config/config.go` NewConfig (92 `dynamicproperties.*` ids; 76 distinct keys incl. DomainConfig)
vs `rust/crates/cadence-frontend/src/config.rs` DYNAMIC_PROPERTY_KEYS, `cadence-persistence/src/dynamic_config_keys.rs`
and call sites in cadence-frontend / cadence-service-core.

## Result
- Key names: all Go frontend keys are registered in Rust (name match checked by script against constants.go KeyName).
- Read in Rust code: all except four keys (below).
- Defaults/filters spot-checked (limit.*Length = 1000/domain, maxIDWarnLength=128, minRetentionDays=1,
  maxBadBinaries=10, visibilityListMaxQPS=10, persistenceMaxQPS=128, blobSize/historySize limits): match Go.
  No wrong default or filter found.

## Unread keys (not ported; no behaviour impact)
| key | Go use | decision |
|---|---|---|
| frontend.esVisibilityListMaxQPS | Go marks it "deprecated: never read" | nothing to port |
| frontend.pinotOptimizedQueryColumns | Pinot visibility only; Rust has no Pinot | n/a |
| frontend.throttledLogRPS | rate-limits the throttled logger (logging only) | no Rust throttled logger; not a limit |
| system.enableLogCustomerQueryParameter | log-only flag in persistence factory | logging only |

## Conclusion
No code change was warranted ("no logic change beyond matching Go"). CI/gate below run on the evidence-only head.
The audit was grep/script based; not every call-site default of all 76 keys was verified individually.

## CI
Head SHA: 81e343ee02c9148a4a5ec36448524d084ccd2e53 (branch parity-fe-dynconfig; frontend-mixed-gate has no source_ref input, dispatched on the branch ref).
- rust-ci.yml package=cadence-frontend: success https://github.com/auenkr/cadence-rust/actions/runs/44848168074
- frontend-mixed-gate.yml database=mysql: success https://github.com/auenkr/cadence-rust/actions/runs/44848184635
