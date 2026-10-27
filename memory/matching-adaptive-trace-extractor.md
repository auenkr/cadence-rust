# Matching adaptive trace extractor

Use `python3 tests/extract-matching-adaptive-trace.py <downloaded-log> [<another-log> ...] -o <output.json>` to extract `matching_adaptive_trace` records. For example:

```sh
python3 tests/extract-matching-adaptive-trace.py \
  memory/evidence/matching-adaptive-trace-40469125507/matching-retry-boundaries.log \
  -o /tmp/matching-adaptive-trace.json
```

The tool emits compact JSON with task event groups (workflow/run/schedule IDs when logged), separate scaler tick samples, poll-only records lacking workflow identity, and other ungrouped records. Every event retains its exact timestamp and boundary when present, source line, normalized identity, and original `key=value` fields. Unknown fields remain in that raw field map. Missing timestamps and boundaries stay `null`; the extractor does not join events by time or infer causes, acknowledgements, or successful completion from token flags or absent records. Current reader-event boundaries are kept verbatim and need no hardcoded schema entry.

Validation performed for this utility only: Python AST parsing and a tiny temporary fixture exercising task grouping, scaler separation, missing boundary handling, poll-only separation, and unknown-field retention. No server, Cargo, Go, or runtime validation was run.
