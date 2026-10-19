#!/usr/bin/env python3
"""Run Go SQL DomainStore writes through the Rust read-only SQLite reader."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
go_overlay_target = root / "go/common/persistence/sql/rust_matching_sqlite_test.go"
go_test_source = root / "tests/matching-sqlite-go_test.go"
with tempfile.TemporaryDirectory(prefix="cadence-matching-domain-") as directory:
    database = Path(directory) / "domains.db"
    with sqlite3.connect(database) as connection:
        connection.executescript((root / "go/schema/sqlite/cadence/schema.sql").read_text())

    overlay = Path(directory) / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(go_overlay_target): str(go_test_source)}}))
    environment = {**os.environ, "CADENCE_MATCHING_SQLITE_DB": str(database)}
    go_test = [
        "go", "test", "-overlay", str(overlay), "./common/persistence/sql",
        "-run", "^TestRustMatchingDomainMetadataInterop$", "-count=1",
    ]
    subprocess.run(go_test, cwd=root / "go", env={**environment, "CADENCE_MATCHING_SQLITE_PHASE": "seed"}, check=True)
    subprocess.run([
        "cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
        "-p", "cadence-persistence",
        "matching_domain_metadata::tests::go_domain_store_database_interop",
        "--", "--ignored", "--exact",
    ], cwd=root, env=environment, check=True)
    subprocess.run(go_test, cwd=root / "go", env={**environment, "CADENCE_MATCHING_SQLITE_PHASE": "verify"}, check=True)

print("Verified Go DomainStore SQLite data with Rust and reopened it in Go")
