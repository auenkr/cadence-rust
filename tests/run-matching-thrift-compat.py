#!/usr/bin/env python3
"""Exercise native Rust Matching Thrift with the pinned Go generated clients."""
import argparse
import json
import os
from pathlib import Path
import re
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
reservations = [socket.socket() for _ in range(7)]
for listener in reservations:
    listener.bind(('127.0.0.1', 0))
http, frontend, history, matching, tchannel, matching_tchannel, matching_http = [s.getsockname()[1] for s in reservations]
for listener in reservations:
    listener.close()
config = (root / 'rust/config/development.yaml').read_text()
matching_section = re.search(r'(?ms)^  matching:\n    rpc:\n(.*?)(?=^    [a-z])', config)
if matching_section is None:
    raise RuntimeError('could not locate matching.rpc in development config')
methods = ['Meta::health', 'MatchingService::AddActivityTask', 'MatchingService::AddDecisionTask',
           'MatchingService::CancelOutstandingPoll', 'MatchingService::DescribeTaskList',
           'MatchingService::GetTaskListsByDomain', 'MatchingService::ListTaskListPartitions',
           'MatchingService::PollForActivityTask', 'MatchingService::PollForDecisionTask',
           'MatchingService::QueryWorkflow', 'MatchingService::RespondQueryTaskCompleted']
http_config = f'      http:\n        port: {matching_http}\n        procedures:\n' + ''.join(f'          - {method}\n' for method in methods)
config = config[:matching_section.end()] + http_config + config[matching_section.end():]
config_path = artifacts / 'server.yaml'
config_path.write_text(config)
tls_config_path = artifacts / 'server-tls.yaml'
tls_config_path.write_text(config.replace(
    f'        port: {matching_http}',
    f'        port: {matching_http}\n        tls: {{enabled: true}}',
    1,
))
command = [str(args.server_bin.resolve()), 'start', '--config', str(config_path), '--port', str(http), '--grpc-port', str(frontend),
           '--history-grpc-port', str(history), '--matching-grpc-port', str(matching), '--tchannel-port', str(tchannel),
           '--matching-tchannel-port', str(matching_tchannel), '--data-dir', str(artifacts / 'data')]
overlay = artifacts / 'overlay.json'
overlay.write_text(json.dumps({'Replace': {str(root / 'go/service/matching/wrappers/thrift/rust_native_test.go'): str(root / 'tests/matching-thrift-go_test.go')}}))
tls_start = subprocess.run(
    [str(args.server_bin.resolve()), 'start', '--config', str(tls_config_path)],
    cwd=root, capture_output=True, text=True, timeout=10,
)
if tls_start.returncode == 0 or 'inbound TLS is unsupported' not in tls_start.stderr:
    raise AssertionError(f'Matching TLS did not fail closed: {tls_start.stdout}\n{tls_start.stderr}')
with (artifacts / 'server.log').open('w') as log:
    server = subprocess.Popen(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        while True:
            if server.poll() is not None:
                raise RuntimeError(f'Rust server exited: {artifacts / "server.log"}')
            try:
                with socket.create_connection(('127.0.0.1', matching_tchannel), timeout=0.2):
                    pass
                try:
                    with socket.create_connection(('127.0.0.1', matching_http), timeout=0.2):
                        pass
                    break
                except OSError:
                    pass
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Rust Matching listener did not start')
                time.sleep(0.1)
        subprocess.run(['go', 'test', '-overlay', str(overlay.resolve()), './service/matching/wrappers/thrift',
                        '-run', '^TestRustMatchingNativeThrift$', '-count=1', '-v'], cwd=root / 'go', check=True, timeout=180,
                       env={**os.environ, 'CADENCE_FRONTEND_TCHANNEL': f'127.0.0.1:{tchannel}',
                            'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{matching_tchannel}',
                            'CADENCE_MATCHING_HTTP': f'http://127.0.0.1:{matching_http}/matching'})
        import urllib.error
        import urllib.request
        headers = {'Rpc-Service': 'cadence-matching', 'Rpc-Encoding': 'thrift'}
        request = urllib.request.Request(f'http://127.0.0.1:{matching_http}/matching', data=b'\0',
            headers={**headers, 'Rpc-Procedure': 'Meta::health'})
        with urllib.request.urlopen(request, timeout=3) as response:
            if response.status != 200:
                raise AssertionError(f'allowlisted procedure returned {response.status}')
        request = urllib.request.Request(f'http://127.0.0.1:{matching_http}/matching', data=b'\0',
            headers={**headers, 'Rpc-Procedure': 'WorkflowService::ListDomains'})
        try:
            urllib.request.urlopen(request, timeout=3)
            raise AssertionError('configured procedure allowlist accepted WorkflowService::ListDomains')
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
        for service in ('cadence-matching', 'cadence-frontend'):
            request = urllib.request.Request(f'http://127.0.0.1:{http}/matching', data=b'\0',
                headers={'Rpc-Service': service, 'Rpc-Encoding': 'thrift',
                         'Rpc-Procedure': 'MatchingService::ListTaskListPartitions'})
            try:
                urllib.request.urlopen(request, timeout=3)
                raise AssertionError(f'JSON listener exposed Matching Thrift route as {service}')
            except urllib.error.HTTPError as error:
                if error.code != 404:
                    raise
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
print(f'Native Matching Thrift compatibility passed; artifacts: {artifacts}')
