# CQL header correction evidence

Run https://github.com/auenkr/cadence-rust/actions/runs/40412871046 succeeded on source `e7657429bb43171568eeff10d1b33a8b4c96c541`.

Downloaded artifact: `/mnt/storage/service/ci-evidence/40412871046/cql-header-foundation`. The actual vendored scylla-cql header unit suite ran in fresh `/tmp/cql-header-target-40412871046`: four passed, zero failed; 28 other tests were filtered out. Cases cover signed stream boundaries, fragmented reading and direction/flags, custom-payload version restriction, and negative wire lengths. This verifies the missing BufMut import correction compiles for that suite. It does not prove live transport/body/session or backend compatibility.

The protocol backend workflow is being prepared to build its Rust probe into a fresh target before starting Cassandra, retain build/format exits and binary SHA256, execute that exact binary without Cargo while Cassandra runs, and serialize protocol matrix lanes. No backend run is claimed; persistence still rejects protocols 2/3.
