# matching P0-1 / P1-9 — backlog forward source domain, errorInjectionRate alias

Branch: `matching-p0-1-backlog-domain-20261024` (based on origin/master).
Verification: GitHub Actions only (no local cargo, per task instruction).

## P0-1: backlog forward source domain

### Go behavior (source of truth)

- `go/service/matching/tasklist/forwarder.go:116-160` `ForwardTask`:
  - Decision: `DomainUUID = task.Event.DomainID`.
  - Activity: `DomainUUID = taskListID.GetDomainID()` (list domain),
    `SourceDomainUUID = task.Event.DomainID`.
- `go/service/matching/handler/engine.go:644` (AddActivityTask accept):
  `taskInfo.DomainID = request.GetSourceDomainUUID()`.
- `go/service/matching/handler/engine.go:568` (AddDecisionTask accept):
  `taskInfo.DomainID = domainID = request.GetDomainUUID()`.
  So on both task types the accepted task's `DomainID` is the wire source domain.
- `go/common/persistence/sql/sql_task_store.go:503-535` `GetTasks`:
  every returned `TaskInfo.DomainID = request.DomainID` (the list domain), not
  the row's domain column. `go/service/matching/tasklist/db.go:178-181` passes the
  list domain. So a backlog task read back from persistence carries the list
  domain as its `DomainID`, and Go's backlog forward sends the list domain as the
  source domain.

### Rust findings

- `rust/crates/cadence-matching/src/matching_registry.rs:1381` (persisted backlog
  forward): `forward_backlog_task(task, &domain, &domain, ...)` where `domain` is
  the list domain. This **matches** Go SQL `GetTasks`. `TaskData` has no domain
  field (`cadence-persistence/src/matching_blob.rs:8-15`), and the read-back
  `TaskInfo` does not carry the workflow domain either, so nothing is invented
  here. **No change.**
- `rust/crates/cadence-matching/src/matching_dispatch.rs` `accept_forwarded_backlog`
  (second hop, when the local offer fails and the partition is not root): the
  call passed `&self.domain_id, &self.domain_id`. Go's second hop uses the accepted
  task's `DomainID`, which is the wire source domain (see engine.go:644/568). For
  a cross-domain activity, Rust sent the partition's list domain as
  `SourceDomainUUID` instead of the source. **Fixed**: source argument is now
  `&source_domain.to_string()` (the parsed wire value; `domain_id` stays the
  partition list domain, matching Go `taskListID.GetDomainID()` for activity).

### Test

- `matching_accepted_backlog_second_hop_keeps_source_domain` in
  `matching_dispatch.rs` (tests module). Accepts a forwarded activity task with
  list domain `Uuid(1)` and source domain `Uuid(5)` via `accept_forwarded_backlog`
  with no poller (local wait zero, so the offer fails immediately) and asserts the
  second-hop `AddActivityTaskRequest` has `domain_id == list` and
  `source_domain_id == source`. Passes on the fixed branch (CI run 40031131240).
  Pre-fix failure is inferred from the code (the old call sent the list domain as
  source), not run against the old code.

## P1-9: `matching.errorInjectionRate` alias in launcher.rs

- **Not dropped.** Go has this key: `go/common/dynamicconfig/dynamicproperties/constants.go:2514-2515`
  (`MatchingErrorInjectionRate`, `KeyName: matching.errorInjectionRate`), and it is read
  by `go/client/clientfactory.go:183`. It is not Rust-only, so the instruction's
  condition (drop only if Go has no such key) is not met. `launcher.rs:705-707`
  is unchanged.
- Consistent with the existing memory note `memory/matching-dyncfg-key-matrix-20261023.md:73`.

## CI

- Workflow: `rust-ci.yml`, `workflow_dispatch` with `package=cadence-matching`.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40031131240 (workflow_dispatch, `package=cadence-matching`, branch `matching-p0-1-backlog-domain-20261024`, head 83cca6f09). Conclusion: success. `cadence-matching` lib tests: 566 passed, 0 failed, 10 ignored; the new test passed.
- A push-triggered run (40031123970) was cancelled by the concurrency group; the dispatch run is the evidence.
