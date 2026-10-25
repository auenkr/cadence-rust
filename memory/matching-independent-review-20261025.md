# Independent Matching candidate core review — 2026-10-25

Candidate reviewed: `27d3ff5f60f0604553824420da0cf61026eda468`. This checkout is parent `af0609dbe676fe09dae62be3735ed73fd12745c0`; the candidate evidence README pins the reviewed source. Review was source and recorded-evidence only. No production edits, Cargo/Go commands, or new gates were run.

## Requirement to evidence

| Requirement | Primary source and candidate evidence | Assessment / remaining proof |
|---|---|---|
| Matching RPC surface and errors | Go `go/service/matching/handler/{handler.go,engine.go}`; Rust `grpc_matching.rs`; internal RPC test suites and package/workspace CI recorded in `memory/evidence/matching-final-candidate-20261024/README.md` | Broad exercised surface; CI proves tests on the pinned head, not exhaustive parity. |
| Durable task lifecycle, matching, ack/read/write, forward and recovery | Go `service/matching/tasklist/`; Rust `matching_reader.rs`, `matching_writer.rs`, `matching_ack_manager.rs`, `matching_forwarder.rs`, `matching_task_list_manager.rs`; candidate PostgreSQL gate and recorded SDK/restart evidence | Candidate PostgreSQL SDK/two-node recovery gate passed (run 40108645569); active-offer/zero-insert probe was pending in README and later user-reported passed. Preserve that result as owner-reported until its run/evidence is attached. Other mixed backend gates were pending at README snapshot. |
| Matcher / poller cancellation | Go `handler/engine.go:1109-1130` cancels by poller ID and returns RPC success; Rust `CancelOutstandingPoll` similarly resolves manager, calls `CancelPoller`, and returns success. Rust reader tests validate raw nonpositive config values separately from validated read defaults (`matching_reader.rs:1107+`). | No concrete observable mismatch found in these inspected paths. Exact lost-poller races and backend-specific behavior need mixed/runtime evidence, not inference from unit tests. |
| Semaphore admission and cancellation | Go `handler/engine.go:399+`, shutdown ownership check and `handler.go:550`; Rust `grpc_matching.rs` plus `matching_rpc_tests.rs:1610+` | Rust tests cover shutdown rejection and canceled-context Internal status while confirming no token was consumed. These close prior candidate concerns at test level; rerun/current mixed-backend result was not attached to README. Caller-cancelled network RPC response is inherently dropped by transport, so status test only validates in-process handler semantics. |
| Isolation, scaler, configuration and discovery | Rust isolation/scaler suites; owner-order oracle artifact `workspace/cadence-matching-owner-order.json`; Go handler/partition code; candidate startup-remote-refresh test and CI README | Owner-order fixture and fresh workspace CI are recorded. Candidate README lists configstore and real shard-distributor as pending; those remain concrete acceptance evidence gaps. |
| Transports and service interaction | Rust gRPC/Thrift/TChannel suites; Go Matching handlers and transport wrappers; README runs | PostgreSQL passed; Thrift/TChannel was still running at snapshot. Cassandra, MySQL extended probes, configstore, shard distributor were pending. No claim of transport-wide completion from focused tests. |
| Ignored Go-port tests | Current source scan: one ignored partition owner test relies on scripted mock lookup ordering (`matching_engine_go_tests.rs:271-272`); legacy non-durable Describe status test explicitly concerns `Matching::new` (`matching_rpc_tests.rs:1056`); other ignores include mixed gates and Go mock-only seams. | Do not count all ignored tests as production gaps. Owner-order behavior now has a separate oracle artifact, but review did not inspect its generator/output deeply enough to certify post-membership-change parity. The non-durable helper is not evidence of a supported production source gap. |

## Concrete unresolved blockers

1. **Acceptance lanes remain incomplete/binding absent at the recorded snapshot.** Candidate README lists Cassandra, MySQL extended probes, configstore and real shard distributor as pending; Thrift/TChannel was running. Attach completed run URLs, pinned head and Rust binary SHA256 for every required lane. Owner-reported active-offer success should likewise be linked to its reproducible artifact. Do not turn PostgreSQL + unit CI into all-backend/service signoff.
2. **Post-membership-change partition owner order is not established by the inspected evidence.** The ignored Go test at `rust/crates/cadence-matching/tests/matching_engine_go_tests.rs:271-272` uses scripted owner lookup and cannot prove production ring behavior. Evidence needed: identical membership and partition fixture through Go and Rust, ordered owner hosts before and after membership change, with the oracle artifact/run bound to this candidate.
3. **Some context/cancellation and failure branches still lack a demonstrated external contract.** Examples include canceled `RefreshWorkflowTasks` (`matching_engine_go_tests_c.rs:351`) and ignored injected manager errors. These are not called bugs here: mock-only injection is not a production failure, and the exact caller/transport cancellation visibility has to be observed against Go before asserting a wire difference. A real failing dependency boundary and matched Go/Rust outcome/side-effect observation would close the question.

## Scope and conclusion

Reviewed actual Go handler/engine anchors for `AcquireSemaphore`, `CancelOutstandingPoll`, and task-list description, Rust semaphore status/cancellation tests, the raw-versus-validated task batch-size accessor tests, ignored-test reasons, prior completion audit corrections, and the candidate README/evidence inventory. This is a core parity review, not a full service signoff. No additional concrete production correctness bug was proven. The candidate is not established as fully accepted because several required mixed-cluster lanes and the named owner-order evidence remain unclosed or unlinked in the reviewed record.

## Parent evidence reconciliation

The candidate workspace artifact preserved at
`memory/evidence/matching-final-candidate-20261024/workspace/cadence-matching-owner-order.json`
contains ordered activity/decision responses for baseline, add, remove,
identity-replacement, address-replacement and empty-ring scenarios, each for
root, child and empty task-list requests. Run40108643448 passed the actual Go
handler/resolver versus Rust handler comparison on pinned27d3ff5f. The oracle
feeds snapshots through each production refresh policy (including address-only
change detection). This directly supplies the post-membership-change evidence
requested by item2; the ignored scripted mock test is not the evidence used.

`refresh_workflow_tasks_canceled_context_is_propagated` in
matching_engine_go_tests_c.rs is active, not ignored. It checks a real in-process
gRPC History cancellation status and pre-cancelled caller no-request behavior.
Typed EntityNotExists/AlreadyCompleted refresh integration placeholders remain;
matching-refresh-failure-coverage-20261025 is adding their actual wire-boundary
coverage. Production classification is already active in matching_history.rs,
but that is not substituted for the requested client-path tests.
