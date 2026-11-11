# History mixed gate lanes (branch history-gate-lanes, 2026-11-11)

Final code head: `34700b3cdbbe2a4deefa61a50689f5605e5100d7`. Workflow: `history-mixed-gate.yml`.

| Lane | Run | Head | Conclusion |
|---|---|---|---|
| rust-ci (cadence-history) | https://github.com/auenkr/cadence-rust/actions/runs/43041357163 | 34700b3 | success |
| a) mysql / grpc / two_history=true | https://github.com/auenkr/cadence-rust/actions/runs/43041363915 | 34700b3 | success |
| b) mysql / tchannel / false | https://github.com/auenkr/cadence-rust/actions/runs/43041370530 | 34700b3 | success |
| c) postgres / grpc / false | https://github.com/auenkr/cadence-rust/actions/runs/43041375990 | 34700b3 | success |
| e) postgres / grpc / true | https://github.com/auenkr/cadence-rust/actions/runs/43041382870 | 34700b3 | success |
| mysql / grpc / false (baseline) | https://github.com/auenkr/cadence-rust/actions/runs/43041388668 | 34700b3 | success |
| d) cassandra / grpc / false | https://github.com/auenkr/cadence-rust/actions/runs/43039513853 | e9b08be | failure (not fixed, large port) |
| f) cassandra / grpc / true | https://github.com/auenkr/cadence-rust/actions/runs/43039519979 | e9b08be | failure (not fixed, large port) |

## Root causes and fixes
1. Admin ReapplyEvents "Shard N is not owned by this host" (two_history): `admin_replication.rs` now routes to the shard owner and retries on ownership errors (Go executeWithRedirect).
2. Postgres lanes "Cannot start a runtime from within a runtime": the sync `postgres` crate was used inside a tokio runtime. Shard store, `SqlBackend::transaction` and pool checkout now run through `matching_postgres::outside_runtime`.
3. Visibility upsert: non-nop `UpsertWorkflowExecution` on SQL visibility maps to BadRequest "Operation is not supported" like Go (`IsNopUpsertWorkflowRequest` keeps the CadenceChangeVersion no-op). Behavior otherwise matches Go; I did not prove this was the root cause of the logged errors.
4. two_history `sdk-complete-child` failures ("Failed to start child workflow execution ... Shard N is not owned by this host"): the transfer-active deps only used the local shard registry. All history-client calls (start, record child completed, request cancel, signal, remove signal, schedule decision, terminate) now go to the owner host over gRPC (`queue_deps_transfer_active.rs`, new `cadence_proto::convert::start_out`).

## Open gap: Cassandra (lanes d, f)
Rust History has no Cassandra execution, shard, task or visibility stores. `open_configured_default_store` yields `history_sql_backend: None` and `HistoryEnv::build` fails with "History requires a MySQL or PostgreSQL default store". A full Cassandra persistence port is needed; not attempted here.

Other remaining limits: remote forwarding uses plaintext gRPC (no History TLS client config); parent close policy worker and workflow resetter deps are still unported (see file header of queue_deps_transfer_active.rs).
