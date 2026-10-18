#!/usr/bin/env python3
"""Call the actual RingpopNode handler through Rust TChannel with Go client."""
import argparse
import json, os, socket, subprocess, tempfile, time
from pathlib import Path

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--server-example", type=Path, default=root / "rust/target/debug/examples/ringpop_node_probe")
args = parser.parse_args()
artifacts = Path(tempfile.mkdtemp(prefix="ringpop-node-compat-"))
if not args.server_example.exists():
    subprocess.run(["cargo", "build", "--locked", "--manifest-path", str(root / "rust/Cargo.toml"), "-p", "cadence-server", "--example", "ringpop_node_probe"], cwd=root / "rust", check=True, timeout=600)
sock = socket.socket(); sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]; sock.close()
overlay = artifacts / "overlay.json"
overlay.write_text(json.dumps({"Replace": {str(root / "go/common/peerprovider/ringpopprovider/rust_node_probe_test.go"): str(root / "tests/ringpop-node-go_test.go")}}))
state_file = artifacts / "state.json"
ready_file = artifacts / "ready"
probe_env = {**os.environ, "RINGPOP_NODE_STATE_FILE": str(state_file), "RINGPOP_NODE_READY_FILE": str(ready_file)}
log = (artifacts / "server.log").open("w")
server = subprocess.Popen([str(args.server_example.resolve()), f"127.0.0.1:{port}"], cwd=root, env=probe_env, stdout=log, stderr=subprocess.STDOUT)
try:
    deadline = time.monotonic() + 30
    while True:
        if server.poll() is not None: raise RuntimeError(f"Rust probe exited; see {artifacts / 'server.log'}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.2): break
        except OSError:
            if time.monotonic() >= deadline: raise RuntimeError("RingpopNode TChannel listener did not start")
            time.sleep(.1)
    env = {**probe_env, "CADENCE_RINGPOP_TCHANNEL": f"127.0.0.1:{port}"}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./common/peerprovider/ringpopprovider", "-run", "^TestRustRealRingpopNode$", "-count=1", "-v"], cwd=root / "go", env=env, check=True, timeout=180)
    deadline = time.monotonic() + 10
    counts = json.loads(state_file.read_text())
    while (counts["join"] < 4 or counts["ping"] != 16) and time.monotonic() < deadline:
        time.sleep(.05)
        counts = json.loads(state_file.read_text())
    if counts["join"] < 4 or counts["ping"] != 16:
        raise RuntimeError(f"unexpected real handler counters: {counts}")
finally:
    server.terminate()
    try: server.wait(timeout=10)
    except subprocess.TimeoutExpired: server.kill(); server.wait()
    log.close()
print(f"Real RingpopNode Go client probe passed; evidence: {artifacts}")
