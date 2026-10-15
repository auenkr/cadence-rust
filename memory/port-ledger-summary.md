# Go source port ledger

This ledger audits all Go source files and top-level AST symbols, including
generated bindings and tests. Every file requires explicit review and evidence.
Partial file mappings are not completion claims.

```json
{
  "filesByKind": {
    "generated": 312,
    "production": 1245,
    "test": 743
  },
  "symbolsByKind": {
    "type": 6425,
    "function": 58726,
    "var": 3669,
    "const": 5884
  },
  "filesByStatus": {
    "review_required": 2285,
    "partial": 15
  }
}
```

Machine-readable detail: `port-ledger.json`. Reviewed mappings: `port-map.json`.
Regenerate with `python3 rust/tools/port-ledger.py`. This uses Go only as an AST
audit tool; Rust build and runtime do not require Go.
