# Matching Cassandra persistence interop

`python3 tests/run-matching-cassandra-interop.py` performs a Go seed, a Rust
Cassandra persistence/restart probe, and a Go verification against one existing
Cadence Cassandra keyspace using the pinned Go schema. It requires Cassandra to
be running with the `tasks`, `task`, and `task_list` schema already applied. It
does not create or migrate a keyspace.

Required environment:

```sh
CADENCE_CASSANDRA_CONTACT_POINTS=127.0.0.1
CADENCE_CASSANDRA_KEYSPACE=cadence
```

Optional variables: `CADENCE_CASSANDRA_PORT`, `CADENCE_CASSANDRA_USER`,
`CADENCE_CASSANDRA_PASSWORD`, `CADENCE_CASSANDRA_TEST_DOMAIN_ID`,
`CADENCE_CASSANDRA_TEST_TASK_LIST`, and `CADENCE_CASSANDRA_ARTIFACTS`. The
runner deletes rows for only its configured domain/task-list partition before
seeding; use a dedicated test domain/list. It stores command lines, combined
logs and exit codes under `artifacts/matching-cassandra-interop` by default.

The Rust probe is ignored during ordinary tests and is selected explicitly by
the runner. It reads Go's task-list sentinel, acquires a fenced lease, inserts a
task, closes and reconnects its driver session, then reads the persisted task.
Go verifies the resulting range ID, UDT task fields, and task ID. This focused
probe is persistence evidence only; it does not replace the mixed Go
Frontend/History/Worker plus Rust Matching SDK workflow, failure, service
restart, and recovery acceptance gate.
