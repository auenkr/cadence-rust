# Frontend domains SELECT parity

Branch `parity-fe-domains`, based on master `8ed6350530b4d89acb1278e6645bb82344f7c7d5`.

The starting benchmark recorded 30 prepared `domains` SELECTs per 60-second Go arm and 24 per Rust Frontend arm in each of three pairs. The same artifact shows 24 `domain_metadata` reads in each arm, including six prepared Rust reads in the mixed arm. Its busiest-300 shape list preserves 24 Go first-page `ORDER BY id LIMIT ?` reads, but omits the shape of the other six and omits the mixed arm's `domains` shapes. The two artifact fields have different selection rules. [Starting benchmark](https://github.com/auenkr/cadence-rust/actions/runs/44682430142)

The benchmark collector now records `mysql_general_log.domain_select_shapes` separately, with no busiest-shape cutoff. It reads HEX-encoded log arguments so binary UUIDs cannot split a log row, normalizes argument values, and preserves the page size. This runs after the general log is turned off. A mock collector check recovered both a repeated page scan and a binary ID point lookup, with shape counts summing to the per-table total.

## Reproduction

Dispatch the benchmark only on the public repository, with the full source SHA. Use `Benchmark/agg.py` to check the CPU model and completions, and repeat any paired comparison run on EPYC 7763. Download the `sdk-frontend-benchmark` artifact into `<root>/<run-id>/sdk-frontend-benchmark` for the aggregation script.

```sh
scripts/ci/gh-run.sh dispatch sdk-matching-benchmark.yml parity-fe-domains \
  service=frontend source_ref=<full-source-sha> rate=25 duration=60 warmup=10 \
  repetitions=3 mysql_tmpfs=true sql_log_secs=60 fresh_build=true
gh run list --workflow sdk-matching-benchmark.yml --branch parity-fe-domains --limit 3
python3 Benchmark/agg.py <root>
jq '.mysql_general_log | {domain_select_shapes, domains: .select_by_table["Execute domains"]}' <resources.json>
```

## Baseline query shapes

The diagnostic run succeeded on AMD EPYC 9V45 at source and workflow head `1c2b9836483cde51f203dc6771fe6fb3a23a4551`. All six arms completed 1,500 SDK workflows without errors. Rust binary SHA256 `31fd89bf963887b3c9c690bbc6e8b06e4ffe0b97df156f46b4a03c8699b21e51`. [Three-pair diagnostic benchmark](https://github.com/auenkr/cadence-rust/actions/runs/44723509027)

| MySQL Execute shape after `FROM domains` | Go, pairs 00/01/02 | Rust Frontend arm, pairs 00/01/02 |
|---|---:|---:|
| `WHERE shard_id=? ORDER BY id LIMIT 200` (Go) | 24/24/25 | 18/18/18 |
| `WHERE shard_id = ? ORDER BY id LIMIT 200` (Rust, with timeout hint) | 0/0/0 | 6/6/6 |
| `WHERE shard_id=? AND name = ?` (Go) | 6/6/6 | 0/0/0 |
| Total | 30/30/31 | 24/24/24 |

The lone additional Go scan in pair 02 coincided with one additional `domain_metadata` read. Each mixed arm scanned 24 times across the four services. All recorded scans used page size 200 and the first-page predicate.

A one-pair attribution run succeeded on Intel Xeon 6975P-C at source and workflow head `98443137514af84f8e6dcfd967c3e41420876fa8` using the same Rust binary. In `00-go-resources.json`, `mysql_general_log.domain_name_lookups` grouped the six reads under `172.18.0.4`, the Frontend container IP in `00-go-identities.json`. The two keys were `cadence-system` and `cadence-batcher`, with three reads apiece. The Rust arm's lookup map was empty. [Client attribution benchmark](https://github.com/auenkr/cadence-rust/actions/runs/44727856976)

## Source path and fix

The Go system Worker starts a scheduler SDK worker per eligible domain. At its first 1-minute refresh, it starts the workers for the newly registered system domains. In the attribution pair the Go arm measured 11:31:58–11:32:58 UTC and started the two scheduler workers at 11:32:44, within the window. The mixed arm did the same at 11:34:32 within its 11:33:45–11:34:46 window. See the paired `*-services.log`, `*.start`, and `*.end` files.

- `go/service/worker/scheduler/client_worker.go` calls SDK `worker.Start` from `defaultCreateWorker`. In Go SDK v1.4.0-rc.3, `internal/internal_worker.go`, the workflow, activity, and locally dispatched activity workers each call `verifyDomainExist`, which issues `DescribeDomain` by name.
- Go Frontend's `api/domain_handlers.go` delegates `DescribeDomain` to `go/common/domain/handler.go`, which calls persistence `GetDomain` directly. The MySQL `getDomainByNameQuery` gives the observed predicate. Go Worker startup's direct `ensureDomainExists` queries come from the Worker IP and precede measurement.
- Rust used the cached `describe` for the public RPC. A warm cache skipped all six persistence reads and could return a stale description following an external update. Public `DescribeDomain` now uses the configured persistent domain manager for gRPC, Thrift, and HTTP. Internal workflow routing keeps the existing cache-backed lookup. The regression test externally updates a domain with a warm Rust cache and verifies public reads by name and ID see the persisted version.

## Validation

Source and workflow head `9d4acef70bb5e45934498a854921ac2148efe5b0`.

- Workspace Rust CI succeeded with `clean_target=true`, Clippy followed by workspace tests, and `public_describe_reads_persisted_state_even_with_a_warm_cache` passing. [Rust CI](https://github.com/auenkr/cadence-rust/actions/runs/44729413142)
- MySQL Frontend mixed gate succeeded with `GATE SUMMARY failed: []`. Fresh binary SHA256 `8c68b76aa4f78480cfa1034750fedeae63c7b59b135856cdc84a2413be955a06`. [MySQL gate](https://github.com/auenkr/cadence-rust/actions/runs/44729425965)
- PostgreSQL Frontend mixed gate succeeded with `GATE SUMMARY failed: []`. Fresh binary SHA256 `b5989ab7afacf8920859cd338856efb6e77e0a56a7a6c82ce75f55bb5160ddf0`. [PostgreSQL gate](https://github.com/auenkr/cadence-rust/actions/runs/44729444509)
