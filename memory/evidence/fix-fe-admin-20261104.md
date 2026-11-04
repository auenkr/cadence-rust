# Fix FE admin C10-C15 (2026-11-04)
Branch t3/fix-fe-admin, head 1f11b8b0d7a5d0396c0a13d3a70b2ad66a07cf5f (code); evidence commit follows.
- C10 resend bounds mapped from start_event/end_event; C11 Thrift queue task types mapped Go(2,3,4,6)->proto(1..4), else absent; C12 replicationTasksInfo (field 40) added to model/Thrift/service-core; C13 data_blob_out preserves encoding; C14 isolation state no offset; C15 ReapplyEvents keeps blob encoding.
- Unit tests: cadence-thrift convert.rs (queue types, DLQ info), cadence-matching grpc_admin.rs fix_fe_admin_tests.
- rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/41748226186 success
- frontend-mixed-gate (two_cluster=true): https://github.com/auenkr/cadence-rust/actions/runs/41749380193 success
- Note: Thrift DLQ domain-queue path emits empty replication_tasks_info (no source data).
