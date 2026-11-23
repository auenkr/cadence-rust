# CI gate sweep: master `96feb1b1848e511b0cd7fdc2f09b4a27a870680f`

Dispatched on branch `gate-sweep-96feb1b`, based on the exact master SHA above. All workflows completed successfully on 2026-11-23. Workflow dispatches with no explicitly supplied inputs used the workflow's declared defaults. No run needed a retry; no failure notes.

| Workflow | Inputs | Run | Conclusion |
|---|---|---|---|
| `rust-ci.yml` | Whole workspace (no package filter) | [45155604215](https://github.com/auenkr/cadence-rust/actions/runs/45155604215) | success |
| `matching-mixed-gate.yml` | Default MySQL lane | [45155604257](https://github.com/auenkr/cadence-rust/actions/runs/45155604257) | success |
| `matching-cassandra-mixed-gate.yml` | Default Cassandra lane | [45155602943](https://github.com/auenkr/cadence-rust/actions/runs/45155602943) | success |
| `matching-active-offer-gate.yml` | Defaults | [45155603536](https://github.com/auenkr/cadence-rust/actions/runs/45155603536) | success |
| `matching-dependency-oracles.yml` | Defaults | [45155604906](https://github.com/auenkr/cadence-rust/actions/runs/45155604906) | success |
| `matching-raw-runid-oracle.yml` | `format_oracle_files=false` | [45155604513](https://github.com/auenkr/cadence-rust/actions/runs/45155604513) | success |
| `matching-production-validation.yml` | Defaults | [45155605357](https://github.com/auenkr/cadence-rust/actions/runs/45155605357) | success |
| `matching-production-tls-mixed.yml` | Defaults | [45155606983](https://github.com/auenkr/cadence-rust/actions/runs/45155606983) | success |
| `frontend-mixed-gate.yml` | MySQL | [45155604436](https://github.com/auenkr/cadence-rust/actions/runs/45155604436) | success |
| `frontend-mixed-gate.yml` | PostgreSQL | [45155604397](https://github.com/auenkr/cadence-rust/actions/runs/45155604397) | success |
| `frontend-mixed-gate.yml` | Cassandra | [45155604601](https://github.com/auenkr/cadence-rust/actions/runs/45155604601) | success |
| `history-mixed-gate.yml` | MySQL | [45155604138](https://github.com/auenkr/cadence-rust/actions/runs/45155604138) | success |
| `history-mixed-gate.yml` | PostgreSQL | [45155604961](https://github.com/auenkr/cadence-rust/actions/runs/45155604961) | success |
| `history-mixed-gate.yml` | Cassandra | [45155604259](https://github.com/auenkr/cadence-rust/actions/runs/45155604259) | success |
| `history-es-visibility.yml` | Elasticsearch (default) | [45155605304](https://github.com/auenkr/cadence-rust/actions/runs/45155605304) | success |
| `history-es-visibility.yml` | OpenSearch (`es_flavor=opensearch`) | [45155603987](https://github.com/auenkr/cadence-rust/actions/runs/45155603987) | success |
| `history-restartgate.yml` | MySQL | [45155605017](https://github.com/auenkr/cadence-rust/actions/runs/45155605017) | success |
| `history-restartgate.yml` | PostgreSQL | [45155603948](https://github.com/auenkr/cadence-rust/actions/runs/45155603948) | success |
| `history-restartgate.yml` | Cassandra | [45155605529](https://github.com/auenkr/cadence-rust/actions/runs/45155605529) | success |
| `history-soak.yml` | MySQL | [45155604419](https://github.com/auenkr/cadence-rust/actions/runs/45155604419) | success |
| `history-soak.yml` | PostgreSQL | [45155605677](https://github.com/auenkr/cadence-rust/actions/runs/45155605677) | success |
| `history-soak.yml` | Cassandra | [45155605377](https://github.com/auenkr/cadence-rust/actions/runs/45155605377) | success |
| `frontend-soak.yml` | Default lanes and soak settings (`go-baseline`, `rust`; 70 minutes, 10 starts/s) | [45155605153](https://github.com/auenkr/cadence-rust/actions/runs/45155605153) | success |
| `frontend-matching-benchmark.yml` | `service=matching`, `source_ref=96feb1b1848e511b0cd7fdc2f09b4a27a870680f`; other defaults | [45155605244](https://github.com/auenkr/cadence-rust/actions/runs/45155605244) | success |

`matching-http-tls-interop.yml` was intentionally skipped per task instructions. The extra paired Frontend + Matching benchmark was dispatched and passed.
