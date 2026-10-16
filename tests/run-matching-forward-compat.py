#!/usr/bin/env python3
"""Verify forwarding limiter boundaries against the original Go implementation."""
import json
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix="matching-forward-") as directory:
    overlay = Path(directory) / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {
        str(root / "go/common/quotas/rust_forward_test.go"):
        str(root / "tests/matching-forward-go_test.go")
    }}))
    subprocess.run(["go", "test", "-overlay", str(overlay), "./common/quotas",
                    "-run", "^TestRustMatchingForwardRate$", "-count=1"], cwd=root / "go", check=True)
subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                "-p", "cadence-server", "--lib", "matching_forward"], check=True)
