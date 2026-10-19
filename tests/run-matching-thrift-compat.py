#!/usr/bin/env python3
"""Exercise native Rust Matching Thrift with the pinned Go generated clients."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.request

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--server-bin', type=Path, default=root / 'rust/target/debug/cadence-server')
parser.add_argument('--artifacts-dir', type=Path)
args = parser.parse_args()
artifacts = (args.artifacts_dir or Path(tempfile.mkdtemp(prefix='matching-thrift-compat-'))).resolve()
artifacts.mkdir(parents=True, exist_ok=True)

# Original YARPC v1.88.0 classifier tests are the oracle for short reads.
subprocess.run(['go', 'test', 'go.uber.org/yarpc/transport/internal/tls/muxlistener',
                '-run', '^TestIsTLSClientHelloRecord$', '-count=1', '-v'],
               cwd=root / 'go', check=True, timeout=60)

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
drain_probe = artifacts / 'matching-drain.test'
subprocess.run([
    'go', 'test', '-c', '-overlay', str(overlay), '-o', str(drain_probe),
    './service/matching/wrappers/thrift',
], cwd=root / 'go', check=True, timeout=180)
def assert_start_fails(test_config, message):
    test_command = command.copy()
    test_command[test_command.index('--config') + 1] = str(test_config)
    result = subprocess.run(test_command, cwd=root, capture_output=True, text=True, timeout=15)
    if result.returncode == 0 or message not in result.stdout + result.stderr:
        raise AssertionError(f'expected startup failure containing {message!r}: {result.stdout}\n{result.stderr}')

def assert_runtime_listener_failure_cleans_up():
    occupied = socket.socket()
    occupied.bind(('127.0.0.1', 0))
    occupied.listen()
    test_command = command.copy()
    test_command[test_command.index('--matching-tchannel-port') + 1] = str(occupied.getsockname()[1])
    try:
        result = subprocess.run(test_command, cwd=root, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            raise AssertionError('server unexpectedly started with Matching TChannel port occupied')
        if 'address already in use' not in (result.stdout + result.stderr).lower():
            raise AssertionError(f'server failed before the occupied Matching listener: {result.stdout}\\n{result.stderr}')
    finally:
        occupied.close()

def request_tls_health(port, certificate):
    context = ssl.create_default_context(cafile=str(certificate))
    with socket.create_connection(('127.0.0.1', port), timeout=3) as raw:
        with context.wrap_socket(raw, server_hostname='localhost') as client:
            client.settimeout(3)
            client.sendall(b'POST /matching HTTP/1.1\r\nHost: localhost\r\n'
                           b'Rpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\n'
                           b'Rpc-Procedure: Meta::health\r\nContent-Length: 1\r\n\r\n\0')
            response = client.recv(1024)
            if b'200' not in response:
                raise AssertionError(f'TLS health request returned {response!r}')

invalid_mode_path = artifacts / 'invalid-mode.yaml'
invalid_mode_path.write_text(config.replace(
    f'        port: {matching_http}', f'        port: {matching_http}\n        TLSMode: Enforced', 1))
assert_start_fails(invalid_mode_path, 'unknown TLSMode')
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
assert_runtime_listener_failure_cleans_up()
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
        # Listener shutdown must cancel connection tasks waiting in sniffing.
        shutdown_probe = socket.create_connection(('127.0.0.1', matching_http), timeout=3)
        drain_value = base64.b64encode(b'"5s"').decode('ascii')
        update = urllib.request.Request(
            f'http://127.0.0.1:{http}/api/v1/admin/dynamic-config/update',
            data=json.dumps({
                'configName': 'matching.shutdownDrainDuration',
                'configValues': [{
                    'filters': [],
                    'value': {'encodingType': 'JSON', 'data': drain_value},
                }],
            }).encode(),
            headers={'Content-Type': 'application/json'},
        )
        with urllib.request.urlopen(update, timeout=3) as response:
            if response.status != 200:
                raise AssertionError(f'dynamic drain config update returned {response.status}')
        server.send_signal(signal.SIGTERM)
        time.sleep(0.1)
        try:
            server.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
        env = {**os.environ, 'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{matching_tchannel}'}
        subprocess.run(
            [str(drain_probe), '-test.run=^TestRustMatchingDrainHealth$', '-test.count=1', '-test.v'],
            cwd=root / 'go', env=env, check=True, timeout=5,
        )
        if server.poll() is not None:
            raise AssertionError('server exited before the Matching drain health RPC completed')
        server.wait(timeout=10)
        shutdown_probe.settimeout(3)
        try:
            if shutdown_probe.recv(1) != b'':
                raise AssertionError('listener shutdown did not close a pending sniff connection')
        except ConnectionResetError:
            pass  # Aborting the connection task may produce TCP RST rather than FIN.
        shutdown_probe.close()
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
                            'CADENCE_MATCHING_TLS_CERT': str(artifacts / 'tls-cert.pem'),
                            'CADENCE_MATCHING_EXPECT_PLAINTEXT_REJECT': '1',
                            'CADENCE_MATCHING_PORT': str(matching_http)})
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

permissive_path = artifacts / 'server-permissive.yaml'
permissive_path.write_text(tls_config_path.read_text().replace('TLSMode: enforced', 'TLSMode: permissive'))
with (artifacts / 'server-permissive.log').open('w') as log:
    permissive_command = command.copy()
    permissive_command[permissive_command.index('--config') + 1] = str(permissive_path)
    permissive_server = subprocess.Popen(permissive_command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        while True:
            if permissive_server.poll() is not None:
                raise RuntimeError(f'Rust permissive listener exited: {artifacts / "server-permissive.log"}')
            try:
                with socket.create_connection(('127.0.0.1', matching_http), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Rust permissive Matching listener did not start')
                time.sleep(0.1)
        # A silent connection must not hold up the listener while a different
        # client completes a request.
        silent = socket.create_connection(('127.0.0.1', matching_http), timeout=3)
        try:
            with socket.create_connection(('127.0.0.1', matching_http), timeout=3) as probe:
                probe.sendall(b'POST /matching HTTP/1.1\r\nHost: localhost\r\n'
                              b'Rpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\n'
                              b'Rpc-Procedure: Meta::health\r\nContent-Length: 1\r\n\r\n\0')
                probe.settimeout(3)
                if b'200' not in probe.recv(1024):
                    raise AssertionError('listener did not serve plaintext behind a silent peer')
            request_tls_health(matching_http, artifacts / 'tls-cert.pem')
        finally:
            silent.close()

        # YARPC does not reject accepted connections at an application-level
        # pending limit. Keep more than the candidate's old 128 peers silent,
        # then confirm a later request still progresses.
        pending = [socket.create_connection(('127.0.0.1', matching_http), timeout=3)
                   for _ in range(129)]
        try:
            with socket.create_connection(('127.0.0.1', matching_http), timeout=3) as probe:
                probe.sendall(b'POST /matching HTTP/1.1\r\nHost: localhost\r\n'
                              b'Rpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\n'
                              b'Rpc-Procedure: Meta::health\r\nContent-Length: 1\r\n\r\n\0')
                probe.settimeout(3)
                if b'200' not in probe.recv(1024):
                    raise AssertionError('pending connection load blocked a later HTTP request')
        finally:
            for connection in pending:
                connection.close()

        # YARPC performs one Read for its five-byte sniff. Force a short read,
        # then finish the TLS record: Go classifies this as plaintext, so the
        # request must not complete a TLS handshake. This guards parity for the
        # deliberately fragmented case instead of asserting a Rust-only upgrade.
        raw = socket.create_connection(('127.0.0.1', matching_http), timeout=3)
        raw.settimeout(3)
        incoming, outgoing = ssl.MemoryBIO(), ssl.MemoryBIO()
        context = ssl.create_default_context(cafile=str(artifacts / 'tls-cert.pem'))
        client = context.wrap_bio(incoming, outgoing, server_side=False, server_hostname='localhost')
        try:
            try:
                client.do_handshake()
            except ssl.SSLWantReadError:
                pass
            hello = outgoing.read()
            raw.sendall(hello[:1])
            time.sleep(0.1)  # Ensure the listener's single read returns short.
            raw.sendall(hello[1:])
            response = raw.recv(4096)
            if b'HTTP/' not in response:
                raise AssertionError(f'short-read TLS sniff did not follow plaintext path: {response!r}')
        finally:
            raw.close()

        # A client that sends a TLS record header and stalls in the handshake
        # must not block a subsequent plaintext request.
        stalled_tls = socket.create_connection(('127.0.0.1', matching_http), timeout=3)
        stalled_tls.sendall(b'\x16\x03\x03\x00\x20')
        try:
            with socket.create_connection(('127.0.0.1', matching_http), timeout=3) as probe:
                probe.sendall(b'POST /matching HTTP/1.1\r\nHost: localhost\r\n'
                              b'Rpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\n'
                              b'Rpc-Procedure: Meta::health\r\nContent-Length: 1\r\n\r\n\0')
                probe.settimeout(3)
                if b'200' not in probe.recv(1024):
                    raise AssertionError('listener did not serve plaintext behind a stalled TLS handshake')
            request_tls_health(matching_http, artifacts / 'tls-cert.pem')
        finally:
            stalled_tls.close()
        # A failed TLS handshake and a peer that closes during sniffing both
        # release their per-connection work so later clients remain serviceable.
        for partial in (b'\x16\x03', b'\x16\x03\x03\x00\x01\xff'):
            failed = socket.create_connection(('127.0.0.1', matching_http), timeout=3)
            failed.sendall(partial)
            failed.close()
        with socket.create_connection(('127.0.0.1', matching_http), timeout=3) as probe:
            probe.sendall(b'POST /matching HTTP/1.1\r\nHost: localhost\r\n'
                          b'Rpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\n'
                          b'Rpc-Procedure: Meta::health\r\nContent-Length: 1\r\n\r\n\0')
            probe.settimeout(3)
            if b'200' not in probe.recv(1024):
                raise AssertionError('listener did not recover after sniff/handshake errors')
        timed_out = socket.create_connection(('127.0.0.1', matching_http), timeout=3)
        time.sleep(16)  # YARPC muxlistener's pinned sniff deadline is 15 seconds.
        timed_out.settimeout(2)
        if timed_out.recv(1) != b'':
            raise AssertionError('silent sniff peer was not closed at the 15 second deadline')
        timed_out.close()
        with socket.create_connection(('127.0.0.1', matching_http), timeout=3) as probe:
            probe.sendall(b'POST /matching HTTP/1.1\r\nHost: localhost\r\n'
                          b'Rpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\n'
                          b'Rpc-Procedure: Meta::health\r\nContent-Length: 1\r\n\r\n\0')
            probe.settimeout(3)
            if b'200' not in probe.recv(1024):
                raise AssertionError('listener did not recover after sniff timeout')
        subprocess.run(['go', 'test', '-overlay', str(overlay.resolve()), './service/matching/wrappers/thrift',
                        '-run', '^(TestRustMatchingNativeThrift|TestRustMatchingTLS)$', '-count=1', '-v'], cwd=root / 'go', check=True, timeout=180,
                       env={**os.environ, 'CADENCE_MATCHING_TLS_HTTP': f'https://localhost:{matching_http}/matching',
                            'CADENCE_MATCHING_PLAINTEXT_HTTP': f'http://127.0.0.1:{matching_http}/matching',
                            'CADENCE_MATCHING_HTTP': f'http://127.0.0.1:{matching_http}/matching',
                            'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{matching_tchannel}',
                            'CADENCE_FRONTEND_TCHANNEL': f'127.0.0.1:{tchannel}',
                            'CADENCE_MATCHING_TLS_CERT': str(artifacts / 'tls-cert.pem')})
    finally:
        permissive_server.terminate()
        try:
            permissive_server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            permissive_server.kill()
            permissive_server.wait()

for mode, config_text in (
        ('default', tls_config_path.read_text().replace('        TLSMode: enforced\n', '')),
        ('disabled', tls_config_path.read_text().replace('TLSMode: enforced', 'TLSMode: disabled'))):
    plain_mode_path = artifacts / f'server-{mode}-tls.yaml'
    plain_mode_path.write_text(config_text)
    with (artifacts / f'server-{mode}-tls.log').open('w') as log:
        plain_mode_command = command.copy()
        plain_mode_command[plain_mode_command.index('--config') + 1] = str(plain_mode_path)
        plain_mode_server = subprocess.Popen(plain_mode_command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while True:
                if plain_mode_server.poll() is not None:
                    raise RuntimeError(f'Rust {mode} TLS listener exited: {artifacts / f"server-{mode}-tls.log"}')
                try:
                    with socket.create_connection(('127.0.0.1', matching_http), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f'Rust {mode} TLS Matching listener did not start')
                    time.sleep(0.1)
            subprocess.run(['go', 'test', '-overlay', str(overlay.resolve()), './service/matching/wrappers/thrift',
                            '-run', '^TestRustMatchingPlainHTTP$', '-count=1', '-v'], cwd=root / 'go', check=True, timeout=60,
                           env={**os.environ, 'CADENCE_MATCHING_PLAINTEXT_HTTP': f'http://127.0.0.1:{matching_http}/matching'})
        finally:
            plain_mode_server.terminate()
            try:
                plain_mode_server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                plain_mode_server.kill()
                plain_mode_server.wait()
print(f'Native Matching Thrift compatibility passed; artifacts: {artifacts}')
