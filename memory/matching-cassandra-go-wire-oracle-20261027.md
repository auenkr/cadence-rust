# Matching Cassandra Go wire oracle — 2026-10-27

## Scope

Added `go/tools/matching-cassandra-protocol-oracle` and the independent
`.github/workflows/matching-cassandra-protocol-oracle.yml` workflow. The runner
uses `github.com/gocql/gocql v0.0.0-20211015133455-b225f9b53fa1` from `go/go.mod`
and sets `ClusterConfig.ProtoVersion` explicitly. CI runs protocol v2 against
the Cassandra 2.1.22 image and v3 against 3.11.17. Both are real database
containers with the project resource limits (1.5 CPU, 1800 MiB memory and
swap, 2048 pids) on `ubuntu-latest`.

The runner exercises the Matching Cassandra queue's CQL shape against a real
server: CAS enqueue, bound SELECT/result decoding, automatic prepare/execute
for bound statements, count, logged batch, strict range delete, point delete, and server
query errors. DDL requests produce schema-change results and exercise the
control connection's schema event subscription. `gocql.UnsetValue` is
intentionally never supplied: it is a v4+ query parameter feature and cannot
be used to claim v2/v3 compatibility. The workflow artifact records the image,
protocol, head SHA, command result and server log. This is backend runtime
evidence for the pinned Go client; it is not Rust or service-gate evidence, and
does not claim that Go's success proves Rust support.

The initial Actions runs showed Cassandra 2.1/2.2 reject `>` and `>=` in a
primary-key DELETE with `Invalid operator`, while the Matching queue range
SELECT succeeds. The v2 job preserves the strict `>` range DELETE and asserts
that exact backend rejection before verifying point-delete support. The v3
job runs the same strict-range operation against Cassandra 3.11.17 and must
succeed. Run [40411377210](https://github.com/auenkr/cadence-rust/actions/runs/40411377210)
passed at head `b7272bc68a811e93cfd159487ce62d715a22af62`:

- protocol v2 / Cassandra 2.1.22 reached every tested operation and emitted
  the expected `Invalid operator > for PRIMARY KEY part message_id`, then
  passed point-delete, server-error and schema operations;
- protocol v3 / Cassandra 3.11.17 passed the same operations including the
  original strict-range DELETE;
- both ran using the exact pinned Go gocql version and reported
  `unset_value=not-used`.

The earlier attempt [40410868673](https://github.com/auenkr/cadence-rust/actions/runs/40410868673)
failed when the harness initially treated the Cassandra 2.2 range-DELETE
rejection as a test failure. This passing evidence separates wire protocol
acceptance from server CQL semantics; it does not claim Rust driver support or
a Matching service gate.

## Wire contract to carry into Rust driver work

The pinned source's `frame.go` has a version-dependent header implementation:

| CQL protocol | Header bytes | Stream encoding | Length offset |
| --- | ---: | --- | ---: |
| v2 | 8 | signed 8-bit | 4 |
| v3 | 9 | signed 16-bit big-endian | 5 |

The frame opcode is at byte 3 in v2 and byte 4 in v3. `newFramer` chooses the
header size at protocol v3, while request serializers branch on protocol for
query parameters, result/prepared metadata and schema-change event bodies. A
v2/v3 fixture or implementation must keep those codec branches aligned with
the header; a header-only adjustment is insufficient. v5 beta flags, v1 and
v4+ unset semantics are outside this oracle's asserted scope.

## Validation state

No Go, Cargo, rustfmt, build, or runtime validation was run on the development
host. The focused Actions workflow is the authorized place for Go execution.
After push, record its run URL, commit SHA, and conclusion here before using
its output as evidence. The matrix is deliberately separate from
`rust-ci.yml`, does not build Rust, and does not dispatch a mixed-cluster gate.

This task did not modify Cassandra driver or persistence implementation files.
