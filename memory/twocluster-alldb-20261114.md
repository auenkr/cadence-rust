# Two-cluster gate: postgres + cassandra (2026-11-14)
Branch twocluster-alldb, head 5e486319f19b59db0baa3ff555dd724e71aeeee4.
Changes: run-frontend-two-cluster-gate.py gets --database (persistence yaml for SQL/Cassandra, --zone, Go cassandra configs with cadence_a*/cadence_b* keyspaces);
new tests/gates/run-frontend-two-cluster-lane-db.sh (one container hosts both clusters; 1800m/1.5cpu, Cassandra heap 1G); mysql lane unchanged;
frontend-mixed-gate.yml routes two_cluster non-mysql to the new lane, rust_history_side added to concurrency group. No Rust changes needed.
All runs success + `GATE SUMMARY failed: []` (https://github.com/auenkr/cadence-rust/actions/runs/<id>):
- postgres active 43503885609; standby 43503891148; graceful 43503897021
- cassandra active 43503901966; standby 43503907314; graceful 43503913739
- mysql active (regression) 43503918598
