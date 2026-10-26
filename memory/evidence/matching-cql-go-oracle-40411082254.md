# Go protocol oracle 40411082254

Run: https://github.com/auenkr/cadence-rust/actions/runs/40411082254
Conclusion: success. Both artifacts record source
`06d4979185aadd18c0f118e54ca939e10f76b6bc` and oracle.exit=0.
Downloaded to `/mnt/storage/service/ci-evidence/40411082254/{v2,v3}`.

Pinned gocql completed schema create/drop, CAS enqueue, range SELECT, count,
batch, point DELETE and an expected server-query error using protocol2 against
Cassandra2.1.22 and protocol3 against Cassandra2.2.19. This proves these Go client
operations work on the named backends/protocols; it is not a Rust result or
Matching mixed-service/SDK result.

Important limit: prior runs 40410393145 and 40410868673 failed range DELETE on
those old servers (`Invalid operator >` / `Invalid operator >=`). The passing
source removes range DELETE and uses point DELETE. Do not claim range DELETE
coverage or full Matching persistence parity from the green run. Protocol3 on
a server supporting the required Matching schema/CQL operations and expected
legacy-server error behavior still need appropriate coverage. Event-subscription
is printed as an operation by the oracle, but the output alone does not assert
that a specific event was received and decoded.

Rust v2/v3 transport and body codec integration remains incomplete. Keep the
existing startup rejection until the configured path is implemented and tested.
No local Cargo, Go or rustfmt command ran.
