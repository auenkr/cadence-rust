#!/usr/bin/env python3
"""Compare history batch bytes with the pinned Go persistence serializer."""
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix="cadence-history-blobs-") as temporary:
    output = Path(temporary)
    subprocess.run(["go", "run", "../tests/history-blob-go/main.go", str(output)],
                   cwd=root / "go", check=True)
    for name in ["history_blob.bin", "history_blob_empty.bin"]:
        expected = root / "rust/crates/cadence-thrift/tests/fixtures" / name
        if output.joinpath(name).read_bytes() != expected.read_bytes():
            raise RuntimeError(f"Go serializer fixture drift: {name}")
subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                "-p", "cadence-thrift", "persistence_history_blob_matches_go"], check=True)
print("Go/Rust history blob compatibility passed")
