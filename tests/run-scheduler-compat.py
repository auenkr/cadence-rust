#!/usr/bin/env python3
"""Run the shared boundary fixtures against Rust and the actual Go scheduler.

Go's overlay adds the compatibility test to its original package without
changing the reference source tree. No Go Cadence server is required.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
fixture = root / "tests/fixtures/scheduler-boundaries.json"
subprocess.run([
    "cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
    "-p", "cadence-persistence", "scheduler_",
], check=True)
with tempfile.TemporaryDirectory(prefix="cadence-scheduler-compat-") as directory:
    overlay = Path(directory) / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {
        str(root / "go/service/worker/scheduler/rust_compat_test.go"):
        str(root / "tests/scheduler-boundaries-go_test.go"),
    }}))
    subprocess.run([
        "go", "test", "-overlay", str(overlay),
        "./service/worker/scheduler", "-run", "^TestRustScheduler(Boundary|CatchUp|Backfill|IdentityLimits|Overlap|FireAccounting)Fixtures$",
        "-count=1", "-v",
    ], cwd=root / "go", env={**os.environ, "CADENCE_SCHEDULER_FIXTURES": str(fixture), "CADENCE_SCHEDULER_CATCHUP_FIXTURES": str(root / "tests/fixtures/scheduler-catchup.json"), "CADENCE_SCHEDULER_BACKFILL_FIXTURES": str(root / "tests/fixtures/scheduler-backfill.json"), "CADENCE_SCHEDULER_IDENTITY_FIXTURES": str(root / "tests/fixtures/scheduler-identity-limits.json"), "CADENCE_SCHEDULER_OVERLAP_FIXTURES": str(root / "tests/fixtures/scheduler-overlap.json"), "CADENCE_SCHEDULER_ACCOUNTING_FIXTURES": str(root / "tests/fixtures/scheduler-fire-accounting.json")}, check=True)
