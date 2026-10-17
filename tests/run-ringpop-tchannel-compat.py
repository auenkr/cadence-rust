#!/usr/bin/env python3
"""Exercise Rust TChannel JSON Ringpop boundary with the pinned Go tchannel client."""
import argparse
import json
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--server-example", type=Path, default=root / "rust/target/debug/examples/ringpop_tchannel_probe")
parser.add_argument("--artifacts-dir", type=Path)
args = parser.parse_args()

artifacts = args.artifacts_dir or Path(tempfile.mkdtemp(prefix="ringpop-tchannel-compat-"))
artifacts.mkdir(parents=True, exist_ok=True)

# Build the Rust example if needed.
if not args.server_example.exists():
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(root / "rust/Cargo.toml"),
         "-p", "cadence-server", "--example", "ringpop_tchannel_probe"],
        cwd=root / "rust", check=True, timeout=600,
    )

# Reserve a port.
listener = socket.socket()
listener.bind(("127.0.0.1", 0))
port = listener.getsockname()[1]
listener.close()

overlay = artifacts / "overlay.json"
overlay.write_text(json.dumps({"Replace": {
    str(root / "go/common/peerprovider/ringpopprovider/rust_probe_test.go"): str(root / "tests/ringpop-tchannel-go_test.go"),
}}))

log_path = artifacts / "ringpop-tchannel.log"
with log_path.open("w") as log:
    server = subprocess.Popen(
        [str(args.server_example.resolve()), "--addr", f"127.0.0.1:{port}"],
        cwd=root, stdout=log, stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.monotonic() + 30
        while True:
            if server.poll() is not None:
                raise RuntimeError(f"Rust example exited; see {log_path}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Rust Ringpop TChannel listener did not start; see {log_path}")
                time.sleep(0.1)
        env = {**os.environ, "CADENCE_RINGPOP_TCHANNEL": f"127.0.0.1:{port}"}
        subprocess.run(
            ["go", "test", "-overlay", str(overlay.resolve()),
             "./common/peerprovider/ringpopprovider",
             "-run", "^TestRustRingpopTChannel$", "-count=1", "-v"],
            cwd=root / "go", check=True, timeout=180, env=env,
        )
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()

print(f"Ringpop TChannel compatibility passed; artifacts: {artifacts}")
