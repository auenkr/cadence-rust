#!/usr/bin/env python3
"""Compare Go and Rust LookupHost on the current Linux host resolver."""
import json
import os
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
overlay_test = ROOT / "tests/go-overlay/common/peerprovider/ringpopprovider/bootstrap_oracle_test.go"
go_test_path = ROOT / "go/common/peerprovider/ringpopprovider/bootstrap_oracle_test.go"

with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as overlay:
    json.dump({"Replace": {str(go_test_path): str(overlay_test)}}, overlay)
    overlay.flush()
    go = subprocess.run(
        ["go", "test", f"-overlay={overlay.name}", "./common/peerprovider/ringpopprovider",
         "-run", "^TestSystemLookupHostOracle$", "-count=1", "-v"],
        cwd=ROOT / "go", text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
if go.returncode:
    sys.stdout.write(go.stdout)
    raise SystemExit(go.returncode)
line = next((line for line in go.stdout.splitlines() if line.startswith("SYSTEM_LOOKUP_HOST_ORACLE=")), None)
if line is None:
    sys.stdout.write(go.stdout)
    raise SystemExit("Go system lookup oracle emitted no result")

env = os.environ.copy()
env["CADENCE_GO_SYSTEM_LOOKUP_HOST_ORACLE"] = line.split("=", 1)[1]
env.setdefault("CARGO_BUILD_JOBS", "1")
rust = subprocess.run(
    ["cargo", "test", "--locked", "-p", "cadence-server", "--lib", "system_lookup_matches_go_system_oracle"],
    cwd=ROOT / "rust", env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
)
sys.stdout.write(go.stdout)
sys.stdout.write(rust.stdout)
raise SystemExit(rust.returncode)
