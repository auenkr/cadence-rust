#!/usr/bin/env python3
"""Verify selected Rust roles do not bind other services' RPC ports."""
import argparse
from contextlib import ExitStack
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("binary", type=Path)
args = parser.parse_args()
binary = args.binary.resolve()
for role in ["matching", "history"]:
    with ExitStack() as stack, tempfile.TemporaryDirectory(prefix=f"cadence-{role}-listeners-") as directory:
        sockets = [stack.enter_context(socket.socket()) for _ in range(6)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
        http, frontend, history, matching, tchannel, matching_tchannel = [listener.getsockname()[1] for listener in sockets]
        # Keep all unselected RPC ports occupied. Accidental listener startup
        # fails the process rather than being hidden by an unused open port.
        sockets[0].close()
        selected = matching if role == "matching" else history
        sockets[3 if role == "matching" else 2].close()
        if role == "matching": sockets[5].close()
        with (Path(directory) / "server.log").open("w+") as log:
            process = subprocess.Popen([str(binary), "start", "--services", role,
                "--port", str(http), "--grpc-port", str(frontend),
                "--history-grpc-port", str(history), "--matching-grpc-port", str(matching),
                "--tchannel-port", str(tchannel), "--matching-tchannel-port", str(matching_tchannel)], cwd=directory, stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(f"{role} exited: {log.read()}")
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{http}/health", timeout=1) as response:
                            assert response.status == 200
                        with socket.create_connection(("127.0.0.1", selected), timeout=1):
                            pass
                        if role == "matching":
                            with socket.create_connection(("127.0.0.1", matching_tchannel), timeout=1): pass
                        break
                    except (OSError, urllib.error.URLError):
                        if time.monotonic() >= deadline:
                            raise RuntimeError(f"{role} listener startup timed out")
                        time.sleep(0.1)
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{http}/api/v1/domains/list", timeout=1)
                    raise AssertionError(f"{role} exposes frontend HTTP routes")
                except urllib.error.HTTPError as error:
                    assert error.code == 404
                # Allow asynchronous failed binds to surface before success.
                time.sleep(0.25)
                assert process.poll() is None, f"{role} exited after startup"
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
    print(f"Verified isolated {role} listeners")
