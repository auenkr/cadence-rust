#!/usr/bin/env python3
"""Build and verify the Rust server with original Go code, IDLs and Go SDK.

All processes are local. Standalone Rust checks run without Go services; a separate
boundary probe boots original Go roles; the final mixed probe replaces Go Matching
with the Rust protobuf listener and tests the existing SDK through Go Frontend.
Run long checks in tmux session cadence-rust, per the repository instructions.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import socket
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request


def main():
    # Concurrent debug links of the generated RPC surface can exhaust this
    # development host; callers can explicitly choose a larger job count.
    os.environ.setdefault("CARGO_BUILD_JOBS", "1")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--go-mixed-server-bin", type=Path, help="reuse the test-only Go Fx routing harness build")
    parser.add_argument("--go-server-bin", type=Path, help="reuse an original Go server build for the final delivery probe")
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
    run("matching-gc-differential", [sys.executable, str(root / "tests/run-matching-gc-compat.py")], timeout=300)
    run("matching-poller-differential", [sys.executable, str(root / "tests/run-matching-poller-compat.py")], timeout=300)
    run("matching-queue-differential", [sys.executable, str(root / "tests/run-matching-queue-compat.py")])
    run("matching-storage-differential", [sys.executable, str(root / "tests/run-matching-storage-compat.py")])
    run("matching-differential", [sys.executable, str(root / "tests/run-matching-compat.py")])
    run("scheduler-differential", [sys.executable, str(root / "tests/run-scheduler-compat.py")])
    run("version-history-differential", [sys.executable, str(root / "tests/run-version-history-compat.py")])
    run("history-blob-differential", [sys.executable, str(root / "tests/run-history-blob-compat.py")])
    run("cluster-metadata-differential", [sys.executable, str(root / "tests/run-cluster-metadata-compat.py")])
    run("active-active-differential", [sys.executable, str(root / "tests/run-active-active-compat.py")])
    run("domain-update-differential", [sys.executable, str(root / "tests/run-domain-update-compat.py")])
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
    # Prove the copied executable can boot from a directory with no Go tree.
    server_workdir = artifacts / "standalone"
    server_workdir.mkdir(exist_ok=True)
    binary = server_workdir / "cadence-server"
    shutil.copy2(root / "rust/target/debug/cadence-server", binary)
    run("service-listeners", [sys.executable, str(root / "tests/check-service-listeners.py"), str(binary)], cwd=server_workdir)
    run("go-config-compat", [str(binary), "check-config", "--config",
                             str(root / "go/config/development.yaml")], cwd=server_workdir)
    run("cluster-config-cli", [sys.executable, str(root / "tests/check-cluster-config-cli.py"), str(binary)], cwd=server_workdir)
    command = [str(binary), "start", "--port", str(http),
               "--grpc-port", str(frontend), "--history-grpc-port", str(history),
               "--matching-grpc-port", str(matching), "--tchannel-port", str(tchannel)]
    # First boot the copied binary with no configuration file or Go checkout.
    with (artifacts / "embedded-startup.log").open("w") as log:
        embedded = subprocess.Popen(command, cwd=server_workdir, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while True:
                if embedded.poll() is not None:
                    raise RuntimeError("embedded-default server exited")
                try:
                    with socket.create_connection(("127.0.0.1", frontend), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("embedded-default server did not start")
                    time.sleep(0.1)
        finally:
            embedded.terminate()
            try:
                embedded.wait(timeout=5)
            except subprocess.TimeoutExpired:
                embedded.kill()
                embedded.wait()
    results.append({"check": "embedded-startup", "exitCode": 0,
                    "log": str(artifacts / "embedded-startup.log")})
    # Exercise real configured-cluster version offsets, keeping all servers Rust.
    config = server_workdir / "compat.yaml"
    text = (root / "rust/config/development.yaml").read_text()
    text = text.replace("  clusterGroup:\n", "  clusterGroup:\n    remote:\n      enabled: true\n      initialFailoverVersion: 2\n      rpcAddress: localhost:1\n      rpcName: cadence-frontend\n      rpcTransport: grpc\n", 1)
    config.write_text(text)
    command += ["--config", str(config)]
    (artifacts / "server.json").write_text(json.dumps({"command": command, "workingDirectory": str(server_workdir), "ports": {
        "http": http, "frontend": frontend, "history": history,
        "matching": matching, "tchannel": tchannel}}, indent=2) + "\n")
    with (artifacts / "server.log").open("w") as log:
        server = subprocess.Popen(command, cwd=server_workdir, env=env, stdout=log, stderr=subprocess.STDOUT)
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
            # These drivers intentionally exercise rapid successive failovers.
            # Keep Go's one-minute production default; override only this server.
            override = {"configName": "frontend.failoverCoolDown", "configValues": [
                {"value": {"EncodingType": 1, "Data": base64.b64encode(b'"0s"').decode()}}]}
            with urllib.request.urlopen(urllib.request.Request(
                    f"http://127.0.0.1:{http}/api/v1/admin/dynamic-config/update",
                    data=json.dumps(override).encode(), headers={"Content-Type": "application/json"}), timeout=5) as response:
                assert response.status == 200
            (artifacts / "test-policy-overrides.json").write_text(json.dumps(override, indent=2) + "\n")
            run("service-workflow-probe", ["node", "tests/service-workflow-compat.cjs",
                f"127.0.0.1:{frontend}", f"127.0.0.1:{matching}"], timeout=120)
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
    go_probe = [sys.executable, str(root / "tests/run-go-service-baseline.py"),
        "--matching-delivery", "--artifacts-dir", str(artifacts / "go-service-delivery")]
    if args.go_server_bin:
        go_probe += ["--binary", str(args.go_server_bin.resolve())]
    run("go-services-matching-delivery", go_probe, timeout=2400)
    mixed_probe = [sys.executable, str(root / "tests/run-go-service-baseline.py"),
        "--rust-matching", "--artifacts-dir", str(artifacts / "mixed-matching")]
    if args.go_mixed_server_bin:
        mixed_probe += ["--binary", str(args.go_mixed_server_bin.resolve())]
    run("go-services-rust-matching", mixed_probe, timeout=2400)
    print(f"All compatibility checks passed. Results: {artifacts / 'results.json'}", flush=True)


if __name__ == "__main__":
    main()
