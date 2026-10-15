#!/usr/bin/env python3
"""Build and verify the Rust server with original Go code, IDLs and Go SDK.

All server/client processes are local. No Go Cadence server is started.
Run long checks in tmux session cadence-rust, per the repository instructions.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--skip-rust-checks", action="store_true",
                        help="skip workspace tests/clippy; still build and run compatibility checks")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    artifacts = (args.artifacts_dir or Path(tempfile.mkdtemp(prefix="cadence-rust-compat-"))).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    results = []
    print(f"Compatibility logs: {artifacts}", flush=True)

    def run(name, command, cwd=root, env=None, timeout=1200):
        log = artifacts / f"{name}.log"
        print(f"Running {name}", flush=True)
        started = time.monotonic()
        with log.open("w") as output:
            try:
                completed = subprocess.run(command, cwd=cwd, env=env, stdout=output,
                                           stderr=subprocess.STDOUT, timeout=timeout)
                code = completed.returncode
            except subprocess.TimeoutExpired:
                code = 124
        results.append({"check": name, "exitCode": code,
                        "seconds": round(time.monotonic() - started, 2), "log": str(log)})
        (artifacts / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        if code:
            print(log.read_text()[-12000:], file=sys.stderr)
            raise RuntimeError(f"{name} failed ({code}); see {log}")

    manifest = str(root / "rust/Cargo.toml")
    if not args.skip_rust_checks:
        run("workspace-tests", ["cargo", "test", "--manifest-path", manifest, "--workspace"])
        run("workspace-clippy", ["cargo", "clippy", "--manifest-path", manifest,
                                 "--workspace", "--all-targets", "--", "-D", "warnings"])
    run("scheduler-differential", [sys.executable, str(root / "tests/run-scheduler-compat.py")])
    run("server-build", ["cargo", "build", "--manifest-path", manifest, "-p", "cadence-server"])
    # Reserve all five ports together to avoid selecting the same port twice.
    reservations = [socket.socket() for _ in range(5)]
    try:
        for reservation in reservations:
            reservation.bind(("127.0.0.1", 0))
        http, frontend, history, matching, tchannel = [s.getsockname()[1] for s in reservations]
    finally:
        for reservation in reservations:
            reservation.close()
    env = {**os.environ, "CADENCE_ENABLE_SCHEDULER": "1", "CADENCE_ENABLE_SEMAPHORE": "1"}
    command = [str(root / "rust/target/debug/cadence-server"), "start", "--port", str(http),
               "--grpc-port", str(frontend), "--history-grpc-port", str(history),
               "--matching-grpc-port", str(matching), "--tchannel-port", str(tchannel)]
    (artifacts / "server.json").write_text(json.dumps({"command": command, "ports": {
        "http": http, "frontend": frontend, "history": history,
        "matching": matching, "tchannel": tchannel}}, indent=2) + "\n")
    with (artifacts / "server.log").open("w") as log:
        server = subprocess.Popen(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while True:
                if server.poll() is not None:
                    raise RuntimeError(f"server exited ({server.returncode}); see {artifacts / 'server.log'}")
                try:
                    with socket.create_connection(("127.0.0.1", frontend), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"server not ready; see {artifacts / 'server.log'}")
                    time.sleep(0.1)
            run("public-grpc", ["node", "tests/grpc-compat.cjs", f"127.0.0.1:{frontend}"], timeout=300)
            run("internal-grpc", ["node", "tests/internal-grpc-compat.cjs"], env={
                **env, "CADENCE_GRPC_PEER": f"127.0.0.1:{frontend}",
                "CADENCE_HISTORY_GRPC_PEER": f"127.0.0.1:{history}",
                "CADENCE_MATCHING_GRPC_PEER": f"127.0.0.1:{matching}",
                "CADENCE_ADMIN_GRPC_PORT": str(frontend)}, timeout=300)
            run("go-sdk-tchannel", ["go", "run", "."], cwd=root / "tests/go-sdk",
                env={**env, "CADENCE_TCHANNEL_PEER": f"127.0.0.1:{tchannel}"}, timeout=300)
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    print(f"All compatibility checks passed. Results: {artifacts / 'results.json'}", flush=True)


if __name__ == "__main__":
    main()
