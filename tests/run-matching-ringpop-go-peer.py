#!/usr/bin/env python3
"""Run the pinned Go Ringpop provider against Rust Matching's shared listener."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
artifacts = Path(tempfile.mkdtemp(prefix="matching-ringpop-go-peer-"))
subprocess.run(
    [
        "cargo",
        "build",
        "--locked",
        "-p",
        "cadence-server",
        "--example",
        "matching_ringpop_go_peer_probe",
    ],
    cwd=root / "rust",
    check=True,
    timeout=600,
)

overlay_test = root / "tests/go-overlay/common/peerprovider/ringpopprovider/rust_provider_lifecycle_test.go"
overlay = artifacts / "overlay.json"
overlay.write_text(
    json.dumps(
        {
            "Replace": {
                str(root / "go/common/peerprovider/ringpopprovider/rust_provider_lifecycle_test.go"):
                    str(overlay_test)
            }
        }
    )
)
env = {
    **os.environ,
    "RUST_MATCHING_RINGPOP_PROBE": str(
        root / "rust/target/debug/examples/matching_ringpop_go_peer_probe"
    ),
}
subprocess.run(
    [
        "go",
        "test",
        "-overlay",
        str(overlay),
        "./common/peerprovider/ringpopprovider",
        "-run",
        "^TestRustMatchingRingpopProviderGoPeerLifecycle$",
        "-count=1",
        "-v",
    ],
    cwd=root / "go",
    check=True,
    timeout=240,
    env=env,
)
print(f"Mixed Go/Rust Ringpop provider lifecycle probe passed; artifacts: {artifacts}")
