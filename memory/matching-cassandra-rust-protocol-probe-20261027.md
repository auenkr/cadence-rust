# Matching Cassandra Rust protocol probe — 2026-10-27

## Scope and implementation

The parent Go CQL integration/oracle branch was already the current branch's
`HEAD` (`bbf4518 Integrate passing Go CQL protocol backend oracle`), so there
was nothing to merge. Added
`rust/crates/cadence-persistence/examples/matching_cassandra_protocol_probe.rs`
and an opt-in Rust comparison path to
`.github/workflows/matching-cassandra-protocol-oracle.yml`. The probe uses the
public `cadence_persistence::cassandra::{CassandraSessionConfig,
CassandraSessionOptions}` configuration and the `Session` returned by
`connect()`. `CASSANDRA_PROTO_VERSION` is mandatory and only values 2 or 3 are
accepted; it is passed directly as `proto_version`, with no fallback.

The workflow's `compare_rust` dispatch input defaults to `false`. With it
enabled, both exact Go-oracle servers are reused in isolated matrix jobs and
the workflow captures Rust command, source SHA, exit code, server/protocol
contract, log, and the built example SHA256. Rust backend execution has **not**
been dispatched. The source currently rejects protocol 2 and 3 in
`CassandraSessionConfig::with_go_options`; an opt-in run should currently fail
at that configuration boundary, as intended, until the driver/persistence
port supports those protocols. Do not count the Go-only workflow passing as
Rust evidence.

Operations preserved from the Go oracle: schema create/drop and results,
prepared CAS enqueue and `[applied]` result, prepared bounded range SELECT and
decoded values, prepared count/result assertion, logged batch insert, original
strict range DELETE, point deletes, and missing-column server error. On the
Cassandra 2.1/protocol 2 matrix leg, only an `Invalid operator` failure from
the strict-range DELETE itself satisfies the expected rejection. On the
Cassandra 3.11/protocol 3 leg it must succeed. The missing-column query accepts
only an undefined-column server error. All connection, configuration,
preparation, result decoding, and unrelated server errors remain failures.

## Pending acceptance

- No Rust or Go code, Cargo command, rustfmt, or backend run was executed on
  this host. Only source reads and `git diff`/status review are allowed here.
- CI must compile/check formatting and run the example. Since current source
  rejects the selected versions, backend acceptance must wait for the actual
  protocol integration. Do not weaken/remove strict range DELETE or expected
  errors to make the probe pass.
- After integration, dispatch the workflow with `compare_rust=true`; require
  protocol 2/Cassandra 2.1.22 to reach operations and report the scoped range
  DELETE rejection, and protocol 3/Cassandra 3.11.17 to pass strict range
  DELETE. Inspect per-leg artifacts and record workflow URL, source/head SHA,
  exit codes, and Rust binary hashes here.
- This probe is driver interoperability evidence only; it is not a Matching
  mixed-service gate or service parity completion.
