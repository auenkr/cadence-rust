#!/usr/bin/env python3
"""Exercise native Rust Matching Thrift with the pinned Go generated clients."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--server-bin', type=Path, default=root / 'rust/target/debug/cadence-server')
parser.add_argument('--artifacts-dir', type=Path)
args = parser.parse_args()
artifacts = args.artifacts_dir or Path(tempfile.mkdtemp(prefix='matching-thrift-compat-'))
artifacts.mkdir(parents=True, exist_ok=True)
reservations = [socket.socket() for _ in range(6)]
for listener in reservations:
    listener.bind(('127.0.0.1', 0))
http, frontend, history, matching, tchannel, matching_tchannel = [s.getsockname()[1] for s in reservations]
for listener in reservations:
    listener.close()
command = [str(args.server_bin.resolve()), 'start', '--port', str(http), '--grpc-port', str(frontend),
           '--history-grpc-port', str(history), '--matching-grpc-port', str(matching), '--tchannel-port', str(tchannel),
           '--matching-tchannel-port', str(matching_tchannel), '--data-dir', str(artifacts / 'data')]
overlay = artifacts / 'overlay.json'
overlay.write_text(json.dumps({'Replace': {str(root / 'go/service/matching/wrappers/thrift/rust_native_test.go'): str(root / 'tests/matching-thrift-go_test.go')}}))
with (artifacts / 'server.log').open('w') as log:
    server = subprocess.Popen(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        while True:
            if server.poll() is not None:
                raise RuntimeError(f'Rust server exited: {artifacts / "server.log"}')
            try:
                with socket.create_connection(('127.0.0.1', matching_tchannel), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Rust Matching listener did not start')
                time.sleep(0.1)
        subprocess.run(['go', 'test', '-overlay', str(overlay.resolve()), './service/matching/wrappers/thrift',
                        '-run', '^TestRustMatchingNativeThrift$', '-count=1', '-v'], cwd=root / 'go', check=True, timeout=180,
                       env={**os.environ, 'CADENCE_FRONTEND_TCHANNEL': f'127.0.0.1:{tchannel}',
                            'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{matching_tchannel}',
                            'CADENCE_MATCHING_HTTP': f'http://127.0.0.1:{http}/matching'})
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
print(f'Native Matching Thrift compatibility passed; artifacts: {artifacts}')
