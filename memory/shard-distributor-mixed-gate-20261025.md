# Shard-distributor mixed gate failure: executor target selection

Investigation against master base `6af476941031193317fdd6c44070e9785e6384b9` and the retained artifact at `/mnt/storage/service/ci-evidence/l4/matching-production-shard-distributor` (run `40198127804`).

## Finding

The retained timeline suggests ambiguous test target selection; the exact metadata values were not retained, so the root cause is not yet confirmed. In `tests/matching-service-compat.cjs`, the SIGKILL path selected executors only when `Metadata.grpc` equaled the configured port as a bare string, then silently fell back to every fresh executor when this returned no IDs. That fallback can treat a still-running secondary executor as if it belonged to the killed primary. The retained gate failure explicitly reports `d14d4cd0-d794-454a-a7b6-2f1f7e05526f` as lingering. The gate timeline shows executor `202f3fba…` disappearing after the kill while `d14d4cd0…` continued heartbeating until cleanup, consistent with that mistaken target. A new gate must confirm the selected identity and actual expiry behavior.

This is not a TTL assertion adjustment: TTL expiry and assignment cleanup remain required for the explicitly selected primary executor. The primary must now match fresh executor metadata (`grpc` as a bare port or as the port suffix of `host:port`); zero or multiple matches fail immediately and persist the observed executor IDs, metadata, and heartbeat timestamps. The shard assignment helper uses the same identity rule and no longer falls back to another executor. If expiry itself times out, it persists selected and lingering executor metadata, heartbeats, and lingering assignments next to the test work directory.

## Source evidence

Pinned module source: `/home/auenkr/go/pkg/mod/github.com/cadence-workflow/shard-manager@v0.0.0-20260610143419-4bef35311802/service/sharddistributor/store/etcd/executorstore/etcdstore.go`, `GetState` around lines 291–312 copies `executorData.Metadata` into `store.HeartbeatState.Metadata`. `.../service/sharddistributor/store/etcd/executorstore/common/parse.go` parses etcd metadata keys directly into `ParsedExecutorData.Metadata` without transforming values. Thus metadata mismatch must be diagnosed from actual stats and cannot justify targeting all executors.

The retained gate's `gate/run/probe.log` says the real leader loop did not expire `d14d4cd0-d794-454a-a7b6-2f1f7e05526f`. The corresponding retained `results.json` explicitly records `goMatchingStarted: false`, Rust Matching as the running Matching service, and Go Frontend/History/Worker. Prior notes that characterized this artifact as a Go-only baseline were incorrect; it was a mixed Rust Matching / Go peer-services gate.

## Validation and remaining evidence

`node --check tests/matching-service-compat.cjs` and `git diff --check` passed locally (no Cargo, Go, formatting, or mixed gate was run, per validation constraints). This change needs the serial parent dispatch for the mixed production shard-distributor gate. A fresh successful run is still needed to record the actual selected metadata and demonstrate TTL expiry plus reassignment under the new deterministic selection.
