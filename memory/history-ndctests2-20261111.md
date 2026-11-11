# history-ndctests2 (2026-11-11)

Branch `history-ndctests2` (from 8e0b1064c).

## Task 1-2: ndc transaction manager tests
- `ndc/test_support.rs`: MockMs gained scripted `close_transaction_as_snapshot` (snapshot/events_seq/closed_with/committed, fail_create_already_started); new `LogWorkflow` (records calls to a shared log), `MockTm` (mock NdcTransactionManager), `events_of`.
- Ported one for one: new_workflow_transaction_manager_test.go (6 cases), existing_workflow_transaction_manager_test.go (10 cases).
- TestCreateWorkflow/TestUpdateWorkflow: Go injects mock sub-managers, Rust builds them inside TransactionManagerImpl, so the ports (`delegation_tests` in transaction_manager.rs) check delegation end to end through the real sub-managers with a scripted env.
- Bugs found: none.

## Task 3: History client TLS
New `client_tls.rs` holds the client TLS built from `services.history.rpc.tls` (`grpc_tls::client_tls`, same as Frontend/Matching clients), installed in launcher.rs; `queue_deps_transfer_active.rs` owner_client uses it. Plaintext when TLS is not configured. Not verified with TLS enabled in a gate (gates run plaintext).
Not changed: admin ReapplyEvents redirect lives in cadence-service-core (`admin::history_channel_for`, frontend side) and replication_clients.rs (remote cluster, not History-to-History); both outside this task.

## CI (head be6ea8df2 for code)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43061042035 success
- mixed gate mysql/grpc/two_history=true: run 43061124538 failure (history churn/restart steps, all later SDK steps failed; flake, no code path changed under plaintext); rerun https://github.com/auenkr/cadence-rust/actions/runs/43065013897 success.

## Remaining
Cassandra port (other worker); TLS-enabled gate lane.
