#!/usr/bin/env python3
"""Boot the original Go services with SQLite and check the shared service probe.

Run in tmux cadence-rust. Default mode is an all-Go baseline.
--matching-delivery checks the Rust Matching RPC/History boundary.
--rust-matching replaces Go Matching with Rust using test-only Fx discovery.
"""
import argparse
import json
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifacts-dir', type=Path)
parser.add_argument('--rust-matching', action='store_true', help='route original Go Frontend/History/Worker to Rust Matching via test-only Fx discovery')
parser.add_argument('--matching-delivery', action='store_true', help='run Rust durable queue delivery against original Go History')
parser.add_argument('--matching-restart', action='store_true', help='restart Rust Matching over the same SQLite queue while Go services stay up')
parser.add_argument('--binary', type=Path, help='use an already built Go server')
parser.add_argument('--go-mixed-server-bin', type=Path, help='reuse a Go test binary built with the Rust Matching discovery overlay')
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
work = (args.artifacts_dir or Path(tempfile.mkdtemp(prefix='cadence-go-baseline-'))).resolve()
work.mkdir(parents=True, exist_ok=True)
print(f'Go baseline artifacts: {work}', flush=True)
binary = args.go_mixed_server_bin.resolve() if args.go_mixed_server_bin else (args.binary.resolve() if args.binary else work / 'cadence-server')
if args.go_mixed_server_bin and not (args.rust_matching or args.matching_restart):
    parser.error('--go-mixed-server-bin requires --rust-matching or --matching-restart')
if not args.binary and not args.go_mixed_server_bin:
    with (work / 'build.log').open('w') as output:
        if args.rust_matching or args.matching_restart:
            overlay = work / 'overlay.json'
            overlay.write_text(json.dumps({'Replace': {str(root / 'go/cmd/server/cadence/rust_matching_peer_test.go'): str(root / 'tests/matching-peer-go_test.go')}}))
            build = ['go', 'test', '-c', '-p', '1', '-ldflags=-s -w', '-overlay', str(overlay), '-o', str(binary), './cadence']
        else:
            build = ['go', 'build', '-p', '1', '-ldflags=-s -w', '-o', str(binary), '.']
        subprocess.run(build, cwd=root / 'go/cmd/server', stdout=output, stderr=subprocess.STDOUT, check=True, timeout=1200)
if args.matching_delivery or args.rust_matching or args.matching_restart:
    # Compile before booting Go or starting workflow deadlines. The probe's
    # per-workflow timeout must measure delivery, not linker resource contention.
    with (work / 'rust-build.log').open('w') as output:
        subprocess.run(['cargo', 'test', '--manifest-path', str(root / 'rust/Cargo.toml'),
            '-p', 'cadence-server', '--test', 'matching_delivery', '--no-run'],
            cwd=root, stdout=output, stderr=subprocess.STDOUT, check=True, timeout=1200)
config = work / 'config'
config.mkdir(exist_ok=True)
keys = ['FRONTEND_PORT', 'MATCHING_PORT', 'HISTORY_PORT', 'WORKER_PORT',
        'FRONTEND_PORT_GRPC', 'MATCHING_PORT_GRPC', 'HISTORY_PORT_GRPC',
        'FRONTEND_PORT_PPROF', 'MATCHING_PORT_PPROF', 'HISTORY_PORT_PPROF', 'WORKER_PORT_PPROF']
if args.rust_matching or args.matching_restart:
    keys.append('RUST_MATCHING_PORT_GRPC')
reservations = [socket.socket() for _ in keys]
try:
    for listener in reservations:
        listener.bind(('127.0.0.1', 0))
    ports = {key: str(listener.getsockname()[1]) for key, listener in zip(keys, reservations)}
finally:
    for listener in reservations:
        listener.close()
(work / 'ports.json').write_text(json.dumps(ports, indent=2) + '\n')
(config / 'base.yaml').write_text((root / 'go/config/base.yaml').read_text())
text = (root / 'go/config/development.yaml').read_text()
for key, value in ports.items():
    text = re.sub(r'\$\{' + key + r':\d+\}', value, text)
text = text.replace('"127.0.0.1:7933", "127.0.0.1:7934", "127.0.0.1:7935"',
    ', '.join(f'"127.0.0.1:{ports[key]}"' for key in ['FRONTEND_PORT', 'HISTORY_PORT', 'WORKER_PORT' if args.rust_matching or args.matching_restart else 'MATCHING_PORT']))
text = text.replace('localhost:7833', f'localhost:{ports["FRONTEND_PORT_GRPC"]}')
text = text.replace('"config/dynamicconfig/development.yaml"', json.dumps(str(root / 'go/config/dynamicconfig/development.yaml')))
# Keep archival outputs in this baseline's artifacts directory as well.
text = text.replace('/tmp/cadence_archival/development', str(work / 'history-archive'))
text = text.replace('/tmp/cadence_vis_archival/development', str(work / 'visibility-archive'))
text = text.replace('/tmp/blobstore', str(work / 'blobstore'))
(config / 'development.yaml').write_text(text)
text = (root / 'go/config/development_sqlite.yaml').read_text()
text = text.replace('"cadence.db"', json.dumps(str(work / 'cadence.db')))
text = text.replace('"cadence_visibility.db"', json.dumps(str(work / 'cadence_visibility.db')))
(config / 'development_sqlite.yaml').write_text(text)
command = [str(binary), '--config', str(config), '--zone', 'sqlite', 'start',
           '--auto-setup', '--services', 'frontend,history,matching,worker']
environment = None
if args.rust_matching or args.matching_restart:
    import os
    command = [str(binary), '-test.run=^TestRustMatchingGoServices$', '-test.timeout=15m']
    environment = {**os.environ, 'CADENCE_GO_CONFIG': str(config), 'CADENCE_RUST_MATCHING_ADDR': f'127.0.0.1:{ports["RUST_MATCHING_PORT_GRPC"]}'}
with (work / 'server.log').open('w') as log:
    server = subprocess.Popen(command, cwd=work, stdout=log, stderr=subprocess.STDOUT, env=environment)
    try:
        deadline = time.monotonic() + 90
        while True:
            if server.poll() is not None:
                raise RuntimeError(f'Go baseline exited; see {work / "server.log"}')
            try:
                with socket.create_connection(('127.0.0.1', int(ports['FRONTEND_PORT_GRPC'])), timeout=0.5):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Go baseline startup timed out')
                time.sleep(0.1)
        with (work / 'probe.log').open('w') as output:
            if args.rust_matching or args.matching_restart:
                probe = ['node', str(root / 'tests/matching-service-compat.cjs'),
                    f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}', f'127.0.0.1:{ports["HISTORY_PORT_GRPC"]}',
                    f'127.0.0.1:{ports["RUST_MATCHING_PORT_GRPC"]}', str(work / 'rust-matching'), f'127.0.0.1:{ports["FRONTEND_PORT"]}',
                    'restart' if args.matching_restart else '', str(server.pid)]
            elif args.matching_delivery:
                probe = ['node', str(root / 'tests/matching-history-compat.cjs'),
                    f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}', f'127.0.0.1:{ports["HISTORY_PORT_GRPC"]}']
            else:
                probe = ['node', str(root / 'tests/service-workflow-compat.cjs'),
                    f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}', f'127.0.0.1:{ports["MATCHING_PORT_GRPC"]}']
            result = subprocess.run(probe, cwd=root, stdout=output, stderr=subprocess.STDOUT, timeout=900 if args.matching_restart else (600 if args.matching_delivery or args.rust_matching else 120))
        (work / 'results.json').write_text(json.dumps({'probeExitCode': result.returncode,
            'topology': 'Go Frontend/History/Worker with Rust Matching RPC' if args.rust_matching or args.matching_restart else ('Go services with Rust queue/History delivery boundary' if args.matching_delivery else 'all-Go'),
            'goServicesPid': server.pid, 'goServicesAliveAfterProbe': server.poll() is None,
            'goMatchingStarted': not (args.rust_matching or args.matching_restart), 'serverCommand': command}, indent=2) + '\n')
        if result.returncode:
            raise RuntimeError(f'Go baseline probe failed; see {work / "probe.log"}')
    finally:
        server.terminate()
        try:
            server.wait(timeout=20)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=10)
print(f'Service baseline passed: {work / "results.json"}', flush=True)
