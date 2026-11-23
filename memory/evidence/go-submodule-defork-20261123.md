# go/ -> upstream submodule + go-overlay (branch go-submodule-defork)

Head SHA validated: 39dd1e9810b6fecae070bdb29888ad6030667433 (history runs on 560bdd200; only workflow-file fixes since).

## Layout
- `go/` = git submodule of https://github.com/cadence-workflow/cadence.git pinned to bb4833c865311f2e22eb6dfe73cad52b53dbaf4b (its `idls` is a nested submodule, init with `--recursive`).
- `go-overlay/` = files we added to the Go tree (same relative paths) + `go-mod.patch` (go.mod/go.sum and two matching test files modified: service/matching/handler/engine_integration_test.go, service/matching/tasklist/task_list_registry_test.go).
  Files: cmd/{tchannel-json-test-server,thrifte2e,thrift-fixtures}, common/dynamicconfig/file_based_oracle_test.go, common/membership/hostinfo_differential_test.go, common/persistence/serialization/sqlblobs_golden_test.go, common/types/mapper/thrift/history_tchannel_wire_oracle_test.go, tests/{configstore-seed,matching-cassandra-interop,matching-error-oracle,shard-distributor-interoperability,shard-distributor-real}, tools/{cadence-rust-history-peer,matching-metrics-{emission,m3,prometheus,statsd}-oracle}.
- `scripts/go-overlay.sh`: copies overlay into go/, applies patch idempotently, sets GOFLAGS=-buildvcs=false in CI.
- Workflows: checkout `submodules: recursive` (or `git submodule update --init --recursive go`) then `scripts/go-overlay.sh`. git commands inside go/ now use `git -C $GITHUB_WORKSPACE`. Benchmark cache keys use HEAD:go and HEAD:go-overlay.
- Removed fork leftovers: CODEOWNERS, dco.yml, ISSUE_TEMPLATE, PR templates, and 10 unused upstream workflows (unreferenced elsewhere).

## CI results (all on branch go-submodule-defork)
| Run | Result |
|---|---|
| rust-ci https://github.com/auenkr/cadence-rust/actions/runs/45130652301 | success |
| matching-mixed-gate (mysql) .../45129223548, cassandra .../45129227601 | success |
| frontend-mixed-gate mysql/postgres/cassandra .../45130664034, 45130673644, 45130682026 | success, `GATE SUMMARY failed: []` |
| history-mixed-gate mysql/postgres/cassandra .../45129257924, 45129265627, 45129273774 | success, `GATE SUMMARY failed: []` |
| matching-raw-runid-oracle .../45130659975 | success |
| matching-dependency-oracles .../45130656222 | FAIL, pre-existing: `cargo test --locked` "cannot update the lock file rust/Cargo.lock"; master run .../45130647967 fails identically. Not caused by this restructure (needs a Cargo.lock refresh, separate task). |

Not run: docker-hub-publish (per instructions).
