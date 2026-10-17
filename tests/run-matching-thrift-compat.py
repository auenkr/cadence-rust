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
    f'        port: {matching_http}\n        TLSMode: enforced\n        tls:\n          enabled: true\n          certFile: {artifacts / "tls-cert.pem"}\n          keyFile: {artifacts / "tls-key.pem"}',
    1,
))
subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout',
                str(artifacts / 'tls-key.pem'), '-out', str(artifacts / 'tls-cert.pem'),
                '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'],
               check=True, capture_output=True)
command = [str(args.server_bin.resolve()), 'start', '--config', str(config_path), '--port', str(http), '--grpc-port', str(frontend),
           '--history-grpc-port', str(history), '--matching-grpc-port', str(matching), '--tchannel-port', str(tchannel),
           '--matching-tchannel-port', str(matching_tchannel), '--data-dir', str(artifacts / 'data')]
overlay = artifacts / 'overlay.json'
overlay.write_text(json.dumps({'Replace': {str(root / 'go/service/matching/wrappers/thrift/rust_native_test.go'): str(root / 'tests/matching-thrift-go_test.go')}}))
def assert_start_fails(test_config, message):
    test_command = command.copy()
    test_command[test_command.index('--config') + 1] = str(test_config)
    result = subprocess.run(test_command, cwd=root, capture_output=True, text=True, timeout=15)
    if result.returncode == 0 or message not in result.stdout + result.stderr:
        raise AssertionError(f'expected startup failure containing {message!r}: {result.stdout}\n{result.stderr}')

missing_tls_path = artifacts / 'missing-tls.yaml'
missing_tls_path.write_text(config.replace(
    f'        port: {matching_http}',
    f'        port: {matching_http}\n        TLSMode: enforced', 1))
assert_start_fails(missing_tls_path, 'requires tls.enabled')
bad_cert_path = artifacts / 'bad-cert.yaml'
bad_cert_path.write_text(tls_config_path.read_text().replace(str(artifacts / 'tls-cert.pem'), str(artifacts / 'missing-cert.pem')))
assert_start_fails(bad_cert_path, 'loading Matching HTTP TLS certificate/key')
invalid_cert = artifacts / 'invalid-cert.pem'
invalid_cert.write_text('not a PEM certificate')
invalid_cert_path = artifacts / 'invalid-cert.yaml'
invalid_cert_path.write_text(tls_config_path.read_text().replace(str(artifacts / 'tls-cert.pem'), str(invalid_cert)))
assert_start_fails(invalid_cert_path, 'loading Matching HTTP TLS certificate/key')
wrong_key = artifacts / 'wrong-key.pem'
subprocess.run(['openssl', 'genrsa', '-out', str(wrong_key), '2048'], check=True, capture_output=True)
mismatched_path = artifacts / 'mismatched-key.yaml'
mismatched_path.write_text(tls_config_path.read_text().replace(str(artifacts / 'tls-key.pem'), str(wrong_key)))
assert_start_fails(mismatched_path, 'loading Matching HTTP TLS certificate/key')
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
tls_command = command.copy()
tls_command[tls_command.index('--config') + 1] = str(tls_config_path)
with (artifacts / 'server-tls.log').open('w') as log:
    tls_server = subprocess.Popen(tls_command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        while True:
            if tls_server.poll() is not None:
                raise RuntimeError(f'Rust TLS server exited: {artifacts / "server-tls.log"}')
            try:
                with socket.create_connection(('127.0.0.1', matching_http), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Rust Matching TLS listener did not start')
                time.sleep(0.1)
        subprocess.run(['go', 'test', '-overlay', str(overlay.resolve()), './service/matching/wrappers/thrift',
                        '-run', '^TestRustMatchingTLS$', '-count=1', '-v'], cwd=root / 'go', check=True, timeout=60,
                       env={**os.environ, 'CADENCE_MATCHING_TLS_HTTP': f'https://localhost:{matching_http}/matching',
                            'CADENCE_MATCHING_TLS_CERT': str(artifacts / 'tls-cert.pem')})
        with socket.create_connection(('127.0.0.1', matching_http), timeout=2) as plaintext:
            plaintext.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
            plaintext.settimeout(2)
            response = plaintext.recv(256)
            if not response or response[0] != 0x15:
                raise AssertionError(f'plaintext did not receive a TLS alert: {response!r}')
    finally:
        tls_server.terminate()
        try:
            tls_server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            tls_server.kill()
            tls_server.wait()
print(f'Native Matching Thrift compatibility passed; artifacts: {artifacts}')
