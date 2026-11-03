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

## Update: gaps closed (head dfbbc4be1 + this evidence commit)
- shard-distributor test: Go (`server.go` creates the spectator only when a ShardDistributor client exists; >1 namespace is fatal only then) accepts namespaces without client hostPort, so the CODE was right and the TEST was wrong. Test now asserts valid-but-inactive without client, active with client.
- Auto-setup no longer needs the Go server: `rust/docker/cadence-auto-setup.sh` (cadence-sql-tool / cadence-cassandra-tool from the Go base image: create db/keyspace, setup-schema 0.0, update-schema for cadence + visibility; mysql/postgres/cassandra). Default domain "default" registered via `cadence` CLI once the local frontend serves (Go creates it in schema.Update; CLI registration is equivalent in effect: retention 7d, "Example Domain"). Go binary is replaced, not kept.
- docker-drop-in.yml has a mysql + cassandra matrix (workflow_dispatch only; compose services limited to 1800m / 1.5 cpus / 2048 pids; tests/docker-drop-in/docker-compose.cassandra.yml).
- Rust binary sha256: 087cd844265dfdda32ab397dadda2109dbdcd44e0db7f5a5de5701b4bfa28995.
- Runs (all on t3/rust-docker-drop-in, head dfbbc4be1, conclusion success):
  - rust-ci https://github.com/auenkr/cadence-rust/actions/runs/41683731108
  - docker-drop-in (mysql + cassandra) https://github.com/auenkr/cadence-rust/actions/runs/41685249906
  - frontend-mixed-gate mysql kafka two_history https://github.com/auenkr/cadence-rust/actions/runs/41691545289
  - frontend-mixed-gate postgres two_cluster https://github.com/auenkr/cadence-rust/actions/runs/41693547469
- Not verified from artifacts: default-domain creation result (script logs to /tmp/default-domain.log in the container); the SDK smoke itself registers `smoke` domain.
