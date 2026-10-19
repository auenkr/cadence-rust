#!/usr/bin/env python3
"""Call Matching Thrift and Ringpop JSON through one Rust TChannel listener."""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

root = Path(__file__).resolve().parents[1]
artifacts = Path(tempfile.mkdtemp(prefix="combined-matching-ringpop-tchannel-"))
probe = root / "rust/target/debug/examples/matching_ringpop_tchannel_probe"
subprocess.run([
    "cargo", "build", "--locked", "-p", "cadence-server",
    "--example", "matching_ringpop_tchannel_probe",
], cwd=root / "rust", check=True, timeout=600)
reservation = socket.socket()
reservation.bind(("127.0.0.1", 0))
port = reservation.getsockname()[1]
reservation.close()
overlay = artifacts / "overlay.json"
overlay.write_text(json.dumps({"Replace": {
    str(root / "go/common/peerprovider/ringpopprovider/rust_probe_test.go"):
        str(root / "tests/combined-matching-ringpop-go_test.go"),
}}))
log_path = artifacts / "server.log"
with log_path.open("w") as log:
    server = subprocess.Popen([str(probe), "--addr", f"127.0.0.1:{port}"],
                             cwd=root, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        while True:
            if server.poll() is not None:
                raise RuntimeError(f"Rust listener exited; see {log_path}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Rust listener did not start; see {log_path}")
                time.sleep(0.1)
        env = {**os.environ, "CADENCE_COMBINED_TCHANNEL": f"127.0.0.1:{port}"}
        subprocess.run([
            "go", "test", "-overlay", str(overlay), "./common/peerprovider/ringpopprovider",
            "-run", "^TestRustCombinedMatchingRingpopTChannel$", "-count=1", "-v",
        ], cwd=root / "go", check=True, timeout=180, env=env)
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
print(f"Combined Matching/Ringpop TChannel oracle passed; artifacts: {artifacts}")
