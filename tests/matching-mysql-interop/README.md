# Go MySQL and Rust Matching persistence interop

`../run-matching-mysql-interop.py` is an opt-in mixed-language harness. It
starts a disposable `mysql:8.0.36` container, initializes the exact schema at
`go/schema/mysql/v8/cadence/schema.sql`, and exercises this sequence:

1. Go's SQL `TaskStore`, wrapped by the real `persistence.TaskManager`, writes
   a leased task list and tasks. Go's SQL `DomainStore` writes two domain rows
   and notification versions. One domain and one task use `thriftrw_snappy`
   while their peers use `thriftrw`.
2. The ignored Rust integration test reads those Go rows, checks UUID paging,
   domain notification versions, task paging/count, stale-range rollback,
   transaction fencing under concurrent lease renewals, expiry metadata, and
   restart durability. It renews the Go queue and writes a task for Go to read.
3. Go `TaskManager` reads the Rust-updated lease/task rows and performs the
   range-fenced delete for the expired Rust task list.

Run from the repository root with:

```sh
python3 tests/run-matching-mysql-interop.py
```

The script requires Docker and Go. After the workspace lockfile has been
resolved, it runs the MySQL DSN unit tests, ignored domain-query cancellation
tests (including max-connections recovery), and a TLS query with Go timeout
options against the same disposable MySQL 8 server before the Go -> Rust -> Go
interop phases. `tests/run-matching-mysql-timeout-validation.sh` is the
serialized entry point: it resolves `rust/Cargo.lock` once, runs the locked
workspace check, tests the vendored driver's timeout unit tests from a
temporary isolated copy, and then invokes this MySQL 8 / Go interop harness.
Run that entry point only in the authorized build lane. It removes only the
temporary MySQL container and temporary vendor test copy it creates; it does
not inspect, stop, or alter other containers.

The expiry portion validates the persisted deadline and the task manager's
fenced delete operation; it does not run Matching's service-level expiry scan.
That remains a mixed-service runtime test, alongside worker restart/recovery.
The harness has not been executed yet, so it is a proposed reproducible test,
not parity evidence.
