# UpdateDomain continuation (2026-10-15)

Verified UpdateDomain controller checkpoint:

- UpdateDomain now has separate optional configuration/replication patch DTOs.
  Thrift false/zero/empty-string presence survives decoding; protobuf masks no
  longer prefill stale state or reject all failover updates as UNIMPLEMENTED.
- New cadence-persistence::domain_update prepares a cloned candidate, classifies
  local/global configuration/failover paths and validates before one store write.
  Data merges, explicit emitMetric=false, retention validation, bad-binary merge/
  limit/deletion and incoming attribute versions follow the traced Go handlers.
- Store update and FailoverDomain share the controller candidate logic, preserving
  config versions, update timestamps, previous failover version/end time and AP
  legacy FailoverHistory data. Audit publication remains local/development storage.
- Actual Go UpdateDomain is exercised with mocked persistence/replicator, fixed
  clock, zero cooldown, min retention 1, max binaries 10, history limit 5, and DISABLED archival.
  34 shared cases compare errors, response, persisted state, write count and
  version/end-time metadata. They PASS against Rust. In particular Go's binary-
  only merge returns changed response without writing; preserve that behavior.
- Full 15-stage runner PASSED: 291 workspace tests, strict clippy, all Go
  differential suites, copied-binary standalone/config checks, public/internal
  gRPC and existing Go SDK TChannel. Evidence:
  /tmp/cadence-domain-update-compat-final/results.json and outer exit file 0.
  Includes masked updates, false/omitted fields, merges, atomic rollback,
  mixed-request rejection, AA promotion/partial updates and graceful-AA rejection.
  Test servers cleaned up; user development server not restarted.
- The first parallel build timed out after 1200s under high memory/IO pressure;
  confirmed all compiler processes terminal, then serial validation passed.
  Combined runner now defaults CARGO_BUILD_JOBS=1 with caller override supported.

Important unfinished behavior (do not claim all domain controllers complete):

- Go default cooldown is 1 minute (frontend.failoverCoolDown), applying to global
  configuration and failover requests. Runtime currently does NOT enforce it.
  The oracle explicitly uses zero. Next port must use real dynamic policy/defaults
  and configure compatibility runners' development overrides explicitly.
- Dynamic min retention/max bad binary/failover-history bounds are not evaluated;
  candidate helper currently uses Go defaults 1/10/5.
- Archival update/provider validation/state machines remain partial. The oracle's
  archival metadata is disabled and its inputs exclude archival update cases.
  Root Go dev YAML enables archival, which Rust still does not fully implement.
- Graceful metadata/validation exists, but promotion callbacks, notification
  versions, pending failover timers/queues and distributed fencing do not.
- Full Go primary registration/delete/deprecate restrictions, required data keys,
  config layering, auth, UUID-v7 durable audit IDs and domain replication remain.
- Thrift empty-container presence and full JSON serialization edge compatibility
  need broader review; zero/false patch values are now retained.

Source anchors: common/domain/handler.go UpdateDomain (438), updateLocalDomain
(900), updateGlobalDomainConfiguration (727), handleFailoverRequest (487),
updateDomainConfiguration (1612), mergeDomainData/mergeBadBinaries (1407),
validateGracefulFailover (1720); common/types/shared.go IsAConfigUpdateRequest /
IsAFailoverRequest (8272); mapper/proto/api.go ToUpdateDomainRequest (4422);
mapper/thrift/shared.go ToUpdateDomainRequest (6535).

The final source audit corrected the legacy history default bound to 5 and
added a Go case for trimming plus HTML/Unicode separator escaping. The final
runner above validates that correction; all controller cases now total 34.
