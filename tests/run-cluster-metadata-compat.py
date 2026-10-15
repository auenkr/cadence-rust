#!/usr/bin/env python3
"""Check failover version ownership against the original Go cluster metadata."""
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent.parent
fixtures = root / "tests/fixtures/cluster-metadata.json"
actual = json.loads(subprocess.check_output(
    ["go", "run", "../tests/cluster-metadata-go/main.go", str(fixtures)], cwd=root / "go"))
expected = json.loads(fixtures.read_text())
if actual != expected:
    raise RuntimeError("cluster metadata Go oracle results drifted")
subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                "-p", "cadence-common", "cluster_metadata_go_fixtures"], check=True)
print(f"All {len(expected)} cluster metadata cases match Go and Rust")
