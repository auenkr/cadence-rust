# Docker drop-in: Rust Frontend+Matching images (2026-11-03)

Branch `t3/rust-docker-drop-in`, head 05090f8ce (+ this evidence commit).
Passing run: https://github.com/auenkr/cadence-rust/actions/runs/41648791873 (success, SHA 05090f8ce).

- Compose: tests/docker-drop-in/docker-compose.split.yml; Rust used for frontend+matching by changing only `image:`; history/worker stay Go.
- Go SDK smoke (tests/frontend-gate driver): exit 0, 30 steps ok, 0 FAIL.
- Go image: sha256:c45ff561741d... size 165,767,743 B. Rust image: sha256:aeb9453eb79f... size 265,612,385 B.
- Rust binary sha256: 66a93d208c17f539a5cf5217b9b9407bc78cfc7ac21d1eff2b368a76a32298cf.
- Image config diff vs Go: only added `CADENCE_SCHEMA_HELPER=cadence-server-go` env.

Bugs fixed on the way: null YAML values as zero values; shard-distributor falls back to hash ring without client hostPort; Frontend routed Matching to its own IP -> now routed by task list on the Ringpop matching ring.

Failed runs: 41629744767, 41630064878, 41630414487, 41631357775, 41636944950, 41642837130.

Known gaps: --auto-setup shells out to the Go binary (`cadence-server-go update-schema`); default domain not created; Cassandra variant not run; temporary push trigger in docker-drop-in.yml must be removed after merge.
