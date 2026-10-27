# Compact downloaded-gate evidence collection

Use the downloaded artifact's root directory (the directory with commit.txt):

```sh
python3 tests/collect-matching-gate-evidence.py /path/to/artifact \
  --expected-sha FULL_40_CHARACTER_SOURCE_SHA \
  --run-url https://github.com/auenkr/cadence-rust/actions/runs/RUN_ID \
  --lane postgres --output /path/to/empty/evidence-directory
```

The helper checks source identity and binary hash format, retains build/gate
exits, runtime topology, actual JSON probe scenario records and selected compact
logs. Files over1MiB are hashed but not copied. Original result labels remain
unchanged. A collection report explicitly says serviceAcceptance:false; the
parent still reviews lane-specific behavior against the acceptance ledger.

Exit0: successful recorded gate collected; exit1: failed recorded gate collected;
exit2: missing/inconsistent identity or evidence. An existing nonempty output
directory is rejected to prevent mixing runs.

Lightweight local file-parsing verification (no Cargo/Go/runtime): actual
PostgreSQL40468419712 collected with exit0; failed adaptive40469125507 collected
with exit1; PostgreSQL source compared against the adaptive SHA was rejected
with exit2 before writing output. Reports were written to corresponding
`/tmp/matching-collector-*` directories. This utility changes no service logic.
