# Frontend leftovers — 2026-11-14

Branch: `frontend-leftovers-20261114`, based on origin/master `b5d5d358a`.
No local Cargo, Go, or Docker commands are used.

## DLQ bad-type probe: incorrect oracle, not a Frontend regression

`go/service/frontend/admin/handler.go:GetDLQReplicationMessages` validates the
request/nonempty task list and forwards to History. Go History
`go/service/history/handler/handler.go:1761-1833` groups by workflow, obtains an
engine, and logs/suppresses both engine lookup and task-fetch errors. The engine
(`engine/engineimpl/get_replication_messages.go`) returns `unsupported task type`
for type 9, but the handler returns OK with no tasks. Rust Frontend correctly
forwards the Go History response. Corrected `tests/gates/admin_probe.py` to expect
OK. No production behavior was changed to satisfy the old oracle.

The purported earlier pass, run [43521611638](https://github.com/auenkr/cadence-rust/actions/runs/43521611638),
used the two-cluster Cassandra lane (head `d9eb3d7fc`), which does not execute
`admin_probe.py`. It therefore did not validate this assertion. Failure
[43544072003](https://github.com/auenkr/cadence-rust/actions/runs/43544072003)
was the single-cluster gate, where the probe actually ran.

## File-based frontend.failoverCoolDown: already wired

`cadence-matching/src/launcher.rs` loads the selected file-based provider via
`DynamicConfigOverlay::from_file` into `state.admin.dynamic_config` before service
startup. `cadence-service-core/src/handlers.rs:domain_policy` reads the exact Go
key `frontend.failoverCoolDown` with the domain filter. HTTP, gRPC, Thrift and
admin domain update paths use this policy. The Go development file supplies 1s;
Rust's fallback 60s is used only without a matching configured value. Added a
regression test loading an actual YAML file, asserting a 2s domain override and
1s fallback. There is no missing key mapping to fix. This investigation does not
prove the earlier Cassandra isolation-group failure was a cooldown issue; see
`frontend-isogroup-cass-20261114.md` for its persistence-error investigation.

## Validation in progress

Source head: `ef06ae9b97d2e9024ac88e1c2dee005bf56ad789`.
- [rust-ci service-core, clean target](https://github.com/auenkr/cadence-rust/actions/runs/43547221809)
- [Frontend mixed mysql, kafka=false](https://github.com/auenkr/cadence-rust/actions/runs/43547225372)
- [Frontend soak, Go baseline + Rust FE/Go Matching, 20min at 10/s](https://github.com/auenkr/cadence-rust/actions/runs/43547229168)

Cassandra and active Rust Frontend two-cluster validation follow the initial gate.

## Mixed gate results

All below concluded **success**, with `GATE SUMMARY failed: []` in the run log.
The first run used `ef06ae9b97d2e9024ac88e1c2dee005bf56ad789`; the others used
`8de10bd46` (same source plus this investigation note).

| Lane | Run | Rust binary SHA256 |
|---|---|---|
| mysql single cluster, kafka=false | [43547225372](https://github.com/auenkr/cadence-rust/actions/runs/43547225372) | `79a0e2b73f224560793deed1c9f2b4317f09334d3b4b68afcd3c29b25dc68479` |
| cassandra single cluster, kafka=false | [43548086254](https://github.com/auenkr/cadence-rust/actions/runs/43548086254) | `583f96011d3873d6ab059a9402b67799a28ddebafece4074a5f0d95b3e63be4d` |
| mysql two_cluster=true, rust_frontend_active=true, kafka=false | [43548089039](https://github.com/auenkr/cadence-rust/actions/runs/43548089039) | `b03771c44216daebde3c7eb33a8843653c9884abf2574bcbb5cd63f349ef9b94` |
| cassandra two_cluster=true, rust_frontend_active=true, kafka=false | [43548691333](https://github.com/auenkr/cadence-rust/actions/runs/43548691333) | `412ef3422b1091e56cb2b330f6d675e2d53adcbdc20584f475b8a6e0fe59379d` |

The active Rust Frontend lanes exercise domain updates, isolation groups, and
replication. The Cassandra isolation-group failure did not recur.

Service-core clean-target clippy/tests [43547221809](https://github.com/auenkr/cadence-rust/actions/runs/43547221809)
concluded **success**, including the file-based cooldown regression test.
Fresh full-workspace clippy then tests [43548694169](https://github.com/auenkr/cadence-rust/actions/runs/43548694169)
on `8de10bd46` is in progress.
