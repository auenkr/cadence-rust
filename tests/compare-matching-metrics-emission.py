#!/usr/bin/env python3
"""Compare live Go tally and Rust Matching sink series captured by CI."""
import json
import sys


def identity(row):
    return (
        row["name"],
        row["kind"],
        tuple(sorted(row.get("tags", {}).items())),
    )


go_rows = {identity(row) for row in json.load(open(sys.argv[1], encoding="utf-8"))}
rust_rows = {identity(row) for row in json.load(open(sys.argv[2], encoding="utf-8"))}
missing = sorted(go_rows - rust_rows)
extra = sorted(rust_rows - go_rows)
if missing or extra:
    print(json.dumps({"missing_from_rust": missing, "extra_in_rust": extra}, indent=2))
    raise SystemExit(1)
print(json.dumps({"series": len(go_rows), "result": "match"}, sort_keys=True))
