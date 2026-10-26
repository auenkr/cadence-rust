# Configured-protocol Go reference matrix

Run https://github.com/auenkr/cadence-rust/actions/runs/40413206359 succeeded at `494b0061de21609fec8892406887ccebc3df31bb`, with `compare_rust=false`. All three artifacts embed that source and oracle exit 0. Artifacts are retained under `/mnt/storage/service/ci-evidence/40413206359`.

| Protocol | Server | Strict range delete | oracle.log SHA256 |
| --- | --- | --- | --- |
| 2 | Cassandra 2.1.22 | Expected Invalid operator rejection | `49e39d2bf6b63b784a22a2b928ed86ad3e6b837dc43919c6ba69d5144976840a` |
| 3 | Cassandra 3.11.17 | Succeeded | `41a0b33b0e8279e4922aa033847ef540f93b94e21e0e6af832ec7bcbef87e6e6` |
| 4 | Cassandra 4.1.7 | Succeeded | `70099b44ef7d8ee814b9ede4b89f0383581f6859095103cde1fe33e68612e0fd` |

The pinned gocql probe completed schema DDL, CAS, prepared select/count, batch, range/point deletion, and error operations. The subscription marker does not independently prove an event was received/decoded. This is Go reference evidence only; Rust protocols 2/3 remain rejected pending integration and actual comparison.

Separately, formatting run 40413747814 failed before compilation on the legacy keyspace fallback syntax. Parent commit `3ee83cfd7` removes the erroneous comma and uses synchronous `map_ok` for successful stream values; formatting retry is https://github.com/auenkr/cadence-rust/actions/runs/40413890934. No successful metadata compilation is claimed yet.
