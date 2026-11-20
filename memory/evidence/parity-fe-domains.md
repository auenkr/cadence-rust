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

The diagnostic benchmark is running at source and workflow head `1c2b9836483cde51f203dc6771fe6fb3a23a4551`. [Diagnostic benchmark](https://github.com/auenkr/cadence-rust/actions/runs/44723509027)
