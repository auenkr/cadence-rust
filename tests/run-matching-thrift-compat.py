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
import sqlite3
import ssl
import subprocess
import tempfile
import time
import urllib.request

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--server-bin', type=Path, default=root / 'rust/target/debug/cadence-server')
parser.add_argument('--artifacts-dir', type=Path)
parser.add_argument('--history-bin', type=Path, help='cadence-history binary; enables the real-stack MySQL stage')
parser.add_argument('--mysql-addr', default=os.environ.get('CADENCE_MYSQL_ADDR', '127.0.0.1:3306'))
parser.add_argument('--mysql-user', default='root')
parser.add_argument('--mysql-password', default=os.environ.get('CADENCE_MYSQL_PASSWORD', 'cadence'))
args = parser.parse_args()
artifacts = (args.artifacts_dir or Path(tempfile.mkdtemp(prefix='matching-thrift-compat-'))).resolve()
artifacts.mkdir(parents=True, exist_ok=True)

# Original YARPC v1.88.0 classifier tests are the oracle for short reads.
subprocess.run(['go', 'test', 'go.uber.org/yarpc/transport/internal/tls/muxlistener',
                '-run', '^TestIsTLSClientHelloRecord$', '-count=1', '-v'],
               cwd=root / 'go', check=True, timeout=60)

reservations = [socket.socket() for _ in range(8)]
for listener in reservations:
    listener.bind(('127.0.0.1', 0))
http, frontend, history, matching, tchannel, matching_tchannel, matching_http, history_tchannel = [s.getsockname()[1] for s in reservations]
for listener in reservations:
    listener.close()
config = (root / 'rust/config/development.yaml').read_text()
# The production main assembly starts Matching's Ringpop provider for explicit
# config. Keep this component probe a deterministic one-node ring on the same
# randomized TChannel port it binds, so no unrelated Go/Rust Ringpop processes
# are needed for the focused persistence and native Thrift assertions.
config = re.sub(
    r'(?m)^  bootstrapHosts:.*$',
    f'  broadcastAddress: 127.0.0.1\n  bootstrapHosts: ["127.0.0.1:{matching_tchannel}"]',
    config,
    count=1,
)
# Real Rust History joins the ring on its configured TChannel port (Go config shape: services.history.rpc.port).
config = config.replace('${HISTORY_PORT:7934}', str(history_tchannel), 1)
domain_database = (artifacts / 'go-cadence.sqlite').resolve()
oracle_database = (artifacts / 'go-oracle.sqlite').resolve()
for path in (domain_database, oracle_database):
    for suffix in ('', '-wal', '-shm'):
        Path(str(path) + suffix).unlink(missing_ok=True)
    with sqlite3.connect(path) as database:
        database.executescript((root / 'go/schema/sqlite/cadence/schema.sql').read_text())
config = config.replace('defaultStore: cass-default', 'defaultStore: matching-domain-metadata', 1)
config = config.replace(
    '\nringpop:\n',
    f'\n    matching-domain-metadata:\n      sql:\n        pluginName: sqlite\n'
    f'        databaseName: {domain_database}\n\nringpop:\n',
    1,
)
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
# Matching-only stage has no Frontend admin API: set the drain window through the
# file-based dynamic config that Matching reads at startup.
stage_dynamic = artifacts / 'matching-dynamic-config.yaml'
stage_dynamic.write_text('matching.shutdownDrainDuration:\n  - value: "30s"\n')
config = config.replace('filepath: "config/dynamicconfig/development.yaml"', 'filepath: ' + json.dumps(str(stage_dynamic)), 1)
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
           '--matching-tchannel-port', str(matching_tchannel), '--data-dir', str(artifacts / 'data'),
           '--services', 'matching']
overlay = artifacts / 'overlay.json'
overlay.write_text(json.dumps({'Replace': {
    str(root / 'go/service/matching/wrappers/thrift/rust_native_test.go'): str(root / 'tests/matching-thrift-go_test.go'),
    str(root / 'go/common/persistence/sql/rust_matching_sqlite_test.go'): str(root / 'tests/matching-sqlite-go_test.go'),
    str(root / 'go/common/peerprovider/ringpopprovider/rust_history_stub_test.go'): str(root / 'tests/matching-history-ring-stub-go_test.go'),
}}))
oracle_sql_env = {**os.environ, 'CADENCE_MATCHING_SQLITE_DB': str(oracle_database),
                  'CADENCE_MATCHING_SQLITE_PHASE': 'seed'}
subprocess.run(['go', 'test', '-overlay', str(overlay), './common/persistence/sql',
                '-run', '^TestRustMatchingDomainMetadataInterop$', '-count=1'],
               cwd=root / 'go', env=oracle_sql_env, check=True, timeout=180)
domain_sql_env = {**os.environ, 'CADENCE_MATCHING_SQLITE_DB': str(domain_database),
                  'CADENCE_MATCHING_SQLITE_PHASE': 'live-seed'}
subprocess.run(['go', 'test', '-overlay', str(overlay), './common/persistence/sql',
                '-run', '^TestRustMatchingDomainMetadataInterop$', '-count=1'],
               cwd=root / 'go', env=domain_sql_env, check=True, timeout=180)
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

def assert_embedded_start_succeeds():
    test_command = command.copy()
    config_index = test_command.index('--config')
    del test_command[config_index:config_index + 2]
    # cadence-server hosts every service by default and runs the production
    # Frontend (0c90d7c7e), which needs a real History peer; the old in-memory
    # Frontend/History shim is gone. Restrict the no-config probe to Matching.
    test_command += ['--services', 'matching']
    embedded_data = artifacts / 'embedded-data'
    test_command[test_command.index('--data-dir') + 1] = str(embedded_data)
    log_path = artifacts / 'embedded-startup.log'
    with log_path.open('w') as log:
        process = subprocess.Popen(test_command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f'no-config embedded Matching exited; see {log_path}')
                try:
                    for port in (matching_tchannel,):
                        with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                            pass
                    break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError(f'no-config embedded Matching did not start; see {log_path}')
            # The Go thrift workflow test needs a Frontend+History; it runs
            # against the explicit-config cluster below.
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

def assert_runtime_listener_failure_cleans_up():
    occupied = socket.socket()
    occupied.bind(('127.0.0.1', 0))
    occupied.listen()
    test_command = command.copy()
    test_command[test_command.index('--matching-tchannel-port') + 1] = str(occupied.getsockname()[1])
    test_command[test_command.index('--data-dir') + 1] = str(artifacts / 'listener-failure-data')
    try:
        result = subprocess.run(test_command, cwd=root, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            raise AssertionError('server unexpectedly started with Matching TChannel port occupied')
        if 'address already in use' not in (result.stdout + result.stderr).lower():
            raise AssertionError(f'server failed before the occupied Matching listener: {result.stdout}\\n{result.stderr}')
    finally:
        occupied.close()

def assert_matching_grpc_bind_fails_before_membership_startup():
    occupied = socket.socket()
    occupied.bind(('127.0.0.1', 0))
    occupied.listen()
    test_command = command.copy()
    test_command[test_command.index('--matching-grpc-port') + 1] = str(occupied.getsockname()[1])
    test_command[test_command.index('--data-dir') + 1] = str(artifacts / 'grpc-listener-failure-data')
    try:
        result = subprocess.run(test_command, cwd=root, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            raise AssertionError('server unexpectedly started with Matching gRPC port occupied')
        if 'address already in use' not in (result.stdout + result.stderr).lower():
            raise AssertionError(f'server failed before the occupied Matching gRPC listener: {result.stdout}\n{result.stderr}')
        if 'starting Matching Ringpop membership' in (result.stdout + result.stderr):
            raise AssertionError('Ringpop membership startup ran despite the Matching gRPC bind failure')
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
unsupported_nosql_path = artifacts / 'unsupported-nosql.yaml'
unsupported_nosql_path.write_text((root / 'rust/config/development.yaml').read_text().replace(
    'pluginName: "cassandra"', 'pluginName: "unsupported-test"', 1))
assert_start_fails(unsupported_nosql_path, 'does not support plugin "unsupported-test"')
import contextlib


@contextlib.contextmanager
def history_ring_member(tag):
    """Join the Rust Matching ring as a real Go cadence-history member.

    Explicit-config Matching resolves History through ringpop (serviceName label
    cadence-history + grpc port label), as in a real cluster; a one-node ring has
    no History host and polls would fail with "Not enough hosts to serve the request".
    """
    ready, release = artifacts / f'history-stub-{tag}.ready', artifacts / f'history-stub-{tag}.release'
    ready.unlink(missing_ok=True)
    release.unlink(missing_ok=True)
    with (artifacts / f'history-stub-{tag}.log').open('w') as stub_log:
        stub = subprocess.Popen(
            ['go', 'test', '-overlay', str(overlay.resolve()), './common/peerprovider/ringpopprovider',
             '-run', '^TestRustMatchingHistoryRingStub$', '-count=1', '-v'],
            cwd=root / 'go', stdout=stub_log, stderr=subprocess.STDOUT, start_new_session=True,
            env={**os.environ, 'CADENCE_RINGPOP_BOOTSTRAP': f'127.0.0.1:{matching_tchannel}',
                 'CADENCE_HISTORY_GRPC_PORT': str(history),
                 'RINGPOP_STUB_READY_FILE': str(ready), 'RINGPOP_STUB_RELEASE_FILE': str(release)})
        try:
            deadline = time.monotonic() + 150
            while not ready.exists():
                if stub.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError(f'Go cadence-history ring stub did not join: {artifacts / f"history-stub-{tag}.log"}')
                time.sleep(0.2)
            yield
        finally:
            release.write_text('release\n')
            try:
                stub.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pass
            finally:
                # Reap the whole group so the compiled Go test child is never orphaned.
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(stub.pid, signal.SIGKILL)
                stub.wait()


assert_embedded_start_succeeds()
assert_runtime_listener_failure_cleans_up()
assert_matching_grpc_bind_fails_before_membership_startup()
main_ring = contextlib.ExitStack()
with (artifacts / 'server.log').open('w') as log:
    server = subprocess.Popen(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        ready_ports = (matching, matching_tchannel, matching_http)
        while True:
            if server.poll() is not None:
                raise RuntimeError(f'Rust server exited: {artifacts / "server.log"}')
            try:
                for port in ready_ports:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                        pass
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f'Rust service listeners did not all start: {ready_ports}')
                time.sleep(0.1)
        # Matching-only sqlite stage: Matching resolves History through the ring,
        # so a Go cadence-history ring member stands in. Workflow-driving tests
        # (TestRustMatchingNativeThrift) run against the real SQL stack below.
        main_ring.enter_context(history_ring_member('main'))
        if (artifacts / 'data' / 'matching.db').exists():
            raise RuntimeError('explicit production config created private --data-dir/matching.db instead of using defaultStore')
        ready = artifacts / 'failover-poll.ready'
        failover_written = artifacts / 'go-failover.ready'
        poll_env = {**os.environ, 'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{matching_tchannel}',
                    'CADENCE_MATCHING_FAILOVER_READY': str(ready),
                    'CADENCE_MATCHING_FAILOVER_WRITTEN': str(failover_written),
                    'CADENCE_MATCHING_SQLITE_DB': str(domain_database)}
        poll_probe = subprocess.Popen(
            ['go', 'test', '-overlay', str(overlay.resolve()), './service/matching/wrappers/thrift',
             '-run', '^TestRustMatchingDomainCacheFailoverProbe$', '-count=1', '-v'],
            cwd=root / 'go', env=poll_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            deadline = time.monotonic() + 90
            while not ready.exists():
                if poll_probe.poll() is not None:
                    raise RuntimeError(f'failover poll probe exited before readiness: {poll_probe.stdout.read()}')
                if time.monotonic() >= deadline:
                    raise RuntimeError('failover poll probe did not register its long poll')
                time.sleep(0.1)
            def start_held_domain_update(phase):
                ready_path = artifacts / f'{phase}.ready'
                release_path = artifacts / f'{phase}.release'
                ready_path.unlink(missing_ok=True)
                release_path.unlink(missing_ok=True)
                # Pin the pre-update WAL snapshot so SQLite cannot checkpoint
                # and recycle the Go writer's committed frames before Rust
                # reads them. The held Go process alone has no reader, so its
                # automatic checkpoint can legitimately leave no WAL bytes.
                wal_reader = sqlite3.connect(domain_database, timeout=30)
                wal_reader.execute('BEGIN')
                wal_reader.execute('SELECT id FROM domains LIMIT 1').fetchall()
                held = subprocess.Popen(
                    ['go', 'test', '-overlay', str(overlay.resolve()), './common/persistence/sql',
                     '-run', '^TestRustMatchingDomainMetadataInterop$', '-count=1', '-v'],
                    cwd=root / 'go',
                    env={**domain_sql_env, 'CADENCE_MATCHING_SQLITE_PHASE': phase,
                         'CADENCE_MATCHING_SQLITE_HOLD_READY': str(ready_path),
                         'CADENCE_MATCHING_SQLITE_HOLD_RELEASE': str(release_path)},
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                deadline = time.monotonic() + 90
                while not ready_path.exists():
                    if held.poll() is not None:
                        raise RuntimeError(f'held Go SQLite update exited before commit: {held.stdout.read()}')
                    if time.monotonic() >= deadline:
                        held.kill()
                        output, _ = held.communicate()
                        raise RuntimeError(f'held Go SQLite update did not commit: {output}')
                    time.sleep(0.1)
                if held.poll() is not None:
                    raise RuntimeError(f'Go SQLite writer exited before WAL inspection: {held.stdout.read()}')
                wal_path = Path(str(domain_database) + '-wal')
                if not wal_path.exists() or wal_path.stat().st_size == 0:
                    release_path.write_text('release')
                    output, _ = held.communicate(timeout=10)
                    wal_reader.close()
                    raise RuntimeError(f'Go SQLite update did not leave committed WAL bytes: {output}')
                return held, release_path, wal_reader

            def release_held_update(held, release_path, wal_reader):
                release_path.write_text('release')
                output, _ = held.communicate(timeout=15)
                wal_reader.close()
                if held.returncode != 0:
                    raise RuntimeError(f'held Go SQLite update failed: {output}')

            ordinary_writer, ordinary_release, ordinary_wal_reader = start_held_domain_update('live-ordinary-held')
            try:
                time.sleep(2)
                if poll_probe.poll() is not None:
                    raise RuntimeError(f'ordinary metadata refresh released a poll: {poll_probe.stdout.read()}')
                if ordinary_writer.poll() is not None:
                    raise RuntimeError('Go ordinary update writer exited before Rust cache observation')
            finally:
                release_held_update(ordinary_writer, ordinary_release, ordinary_wal_reader)
            failover_writer, failover_release, failover_wal_reader = start_held_domain_update('live-failover-held')
            try:
                failover_written.write_text('updated')
                try:
                    output, _ = poll_probe.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    poll_probe.kill()
                    output, _ = poll_probe.communicate()
                    raise RuntimeError(f'Rust Matching did not release the poll after Go failover: {output}')
                if poll_probe.returncode != 0:
                    raise RuntimeError(f'Go worker poll did not observe Rust failover behavior: {output}')
                if failover_writer.poll() is not None:
                    raise RuntimeError('Go failover update writer exited before Rust cache observation')
            finally:
                release_held_update(failover_writer, failover_release, failover_wal_reader)
        finally:
            if poll_probe.poll() is None:
                poll_probe.kill()
                poll_probe.wait(timeout=5)
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
        # Keep a wide margin for process scheduling and the Go dispatcher
        # startup between signal delivery and the health RPC. The second
        # signal below ends the process after the drain behavior is proved.
        server.send_signal(signal.SIGTERM)
        env = {**os.environ, 'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{matching_tchannel}'}
        subprocess.run(
            [str(drain_probe), '-test.run=^TestRustMatchingDrainHealth$', '-test.count=1', '-test.v'],
            cwd=root / 'go', env=env, check=True, timeout=5,
        )
        if server.poll() is not None:
            raise AssertionError('server exited before the Matching drain health RPC completed')
        # A second signal forces shutdown, so send it only after proving the
        # configured graceful-drain window accepted a generated-client RPC.
        try:
            server.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
        server.wait(timeout=10)
        shutdown_probe.settimeout(3)
        try:
            if shutdown_probe.recv(1) != b'':
                raise AssertionError('listener shutdown did not close a pending sniff connection')
        except ConnectionResetError:
            pass  # Aborting the connection task may produce TCP RST rather than FIN.
        shutdown_probe.close()
    finally:
        main_ring.close()
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
            try:
                response = plaintext.recv(256)
            except ConnectionResetError:
                response = b''
            if response and response[0] != 0x15:
                raise AssertionError(f'plaintext received a non-alert response from the enforced TLS listener: {response!r}')
    finally:
        tls_server.terminate()
        try:
            tls_server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            tls_server.kill()
            tls_server.wait()

permissive_path = artifacts / 'server-permissive.yaml'
permissive_path.write_text(tls_config_path.read_text().replace('TLSMode: enforced', 'TLSMode: permissive'))
permissive_ring = contextlib.ExitStack()
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
                        '-run', '^TestRustMatchingTLS$', '-count=1', '-v'], cwd=root / 'go', check=True, timeout=180,
                       env={**os.environ, 'CADENCE_MATCHING_TLS_HTTP': f'https://localhost:{matching_http}/matching',
                            'CADENCE_MATCHING_PLAINTEXT_HTTP': f'http://127.0.0.1:{matching_http}/matching',
                            'CADENCE_MATCHING_HTTP': f'http://127.0.0.1:{matching_http}/matching',
                            'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{matching_tchannel}',
                            'CADENCE_FRONTEND_TCHANNEL': f'127.0.0.1:{tchannel}',
                            'CADENCE_MATCHING_TLS_CERT': str(artifacts / 'tls-cert.pem')})
    finally:
        permissive_ring.close()
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
def run_sql_stack():
    """Real Rust Frontend+Matching (cadence-server) and cadence-history on one MySQL store."""
    work = artifacts / 'sql-stack'
    work.mkdir(parents=True, exist_ok=True)
    names = ['FRONTEND_PORT', 'FRONTEND_PORT_GRPC', 'MATCHING_PORT', 'MATCHING_PORT_GRPC', 'HISTORY_PORT', 'HISTORY_PORT_GRPC']
    socks = [socket.socket() for _ in range(32)]
    for sock in socks:
        sock.bind(('127.0.0.1', 0))
    free = [sock.getsockname()[1] for sock in socks]
    for sock in socks:
        sock.close()
    fe_t, fe_g, m_t, m_g, h_t, h_g = free[:6]
    fixed = dict(zip(names, free[:6]))
    spare = iter(free[6:])
    cfg = (root / 'rust/config/development.yaml').read_text()
    cfg = re.sub(r'\$\{(\w+):\d+\}', lambda m: str(fixed.get(m.group(1)) or next(spare)), cfg)
    host, _, port = args.mysql_addr.partition(':')
    def store(name, db, conns):
        return (f'    {name}:\n      sql:\n        pluginName: mysql\n        databaseName: {db}\n'
                f'        connectAddr: "{args.mysql_addr}"\n        connectProtocol: tcp\n        user: {args.mysql_user}\n'
                f'        password: "{args.mysql_password}"\n        nShards: 1\n        maxConns: {conns}\n        maxIdleConns: {conns}\n')
    persistence = ('persistence:\n  defaultStore: mysql-default\n  visibilityStore: mysql-visibility\n  numHistoryShards: 4\n  datastores:\n'
                   + store('mysql-default', 'cadence', 20) + store('mysql-visibility', 'cadence_visibility', 2) + '\n')
    cfg, n = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', lambda m: persistence, cfg, count=1)
    assert n == 1
    # Ring bootstrap through Matching (starts first); Frontend and History join it.
    cfg, n = re.subn(r'(?m)^  bootstrapHosts:.*$', f'  broadcastAddress: 127.0.0.1\n  bootstrapHosts: ["127.0.0.1:{m_t}"]', cfg, count=1)
    assert n == 1
    dyn = (root / 'go/config/dynamicconfig/development.yaml').read_text().rstrip('\n')
    dyn += '\n\nsystem.enableGRPCOutbound:\n  - value: true\n'
    (work / 'dynamic-config.yaml').write_text(dyn + '\n')
    cfg = cfg.replace('filepath: "config/dynamicconfig/development.yaml"', 'filepath: ' + json.dumps(str(work / 'dynamic-config.yaml')))
    for old, new in (('/tmp/cadence_archival/development', 'history-archive'), ('/tmp/cadence_vis_archival/development', 'visibility-archive'), ('/tmp/blobstore', 'blobstore')):
        cfg = cfg.replace(old, str(work / new))
    cfg = cfg.replace('localhost:7833', f'127.0.0.1:{fe_g}')
    section = re.search(r'(?ms)^  matching:\n    rpc:\n(.*?)(?=^    [a-z])', cfg)
    assert section is not None
    tls = (f'      http:\n        port: {matching_http}\n        TLSMode: permissive\n        tls:\n          enabled: true\n'
           f'          certFile: {artifacts / "tls-cert.pem"}\n          keyFile: {artifacts / "tls-key.pem"}\n        procedures:\n'
           + ''.join(f'          - {method}\n' for method in methods))
    cfg = cfg[:section.end()] + tls + cfg[section.end():]
    (work / 'rust.yaml').write_text(cfg)
    procs = []
    def launch(name, cmd, env=None):
        log = (work / f'{name}.log').open('ab')
        procs.append(subprocess.Popen([str(c) for c in cmd], cwd=work, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True))
        return procs[-1]
    def wait_port(port, secs, proc):
        end = time.monotonic() + secs
        while time.monotonic() < end:
            if proc.poll() is not None:
                return False
            try:
                socket.create_connection(('127.0.0.1', port), timeout=0.5).close()
                return True
            except OSError:
                time.sleep(0.5)
        return False
    server_bin = str(args.server_bin.resolve())
    try:
        matching_proc = launch('rust-matching', [server_bin, 'start', '--config', work / 'rust.yaml', '--root', work, '--services', 'matching',
            '--port', '0', '--history-grpc-port', h_g, '--matching-grpc-port', m_g, '--matching-tchannel-port', m_t, '--data-dir', work / 'matching-data'])
        frontend_proc = launch('rust-frontend', [server_bin, 'start', '--config', work / 'rust.yaml', '--root', work, '--services', 'frontend',
            '--history-grpc-port', h_g, '--grpc-port', fe_g, '--tchannel-port', fe_t, '--data-dir', work / 'frontend-data'],
            env={**os.environ, 'CADENCE_HISTORY_TCHANNEL_ADDR': f'127.0.0.1:{h_t}', 'CADENCE_MATCHING_TCHANNEL_ADDR': f'127.0.0.1:{m_t}'})
        history_proc = launch('rust-history', [args.history_bin.resolve(), '--config', work / 'rust.yaml', '--root', work, 'start', '--services', 'history'])
        for label, port, proc in (('history grpc', h_g, history_proc), ('matching grpc', m_g, matching_proc), ('matching http', matching_http, matching_proc),
                                  ('frontend tchannel', fe_t, frontend_proc), ('frontend grpc', fe_g, frontend_proc)):
            if not wait_port(port, 240, proc):
                raise RuntimeError(f'real-stack {label} port {port} did not open; see {work}')
        time.sleep(20)  # ring convergence
        subprocess.run(['go', 'test', '-overlay', str(overlay.resolve()), './service/matching/wrappers/thrift',
                        '-run', '^(TestRustMatchingNativeThrift|TestRustMatchingTLS)$', '-count=1', '-v'], cwd=root / 'go', check=True, timeout=300,
                       env={**os.environ, 'CADENCE_MATCHING_TLS_HTTP': f'https://localhost:{matching_http}/matching',
                            'CADENCE_MATCHING_PLAINTEXT_HTTP': f'http://127.0.0.1:{matching_http}/matching',
                            'CADENCE_MATCHING_HTTP': f'http://127.0.0.1:{matching_http}/matching',
                            'CADENCE_MATCHING_TCHANNEL': f'127.0.0.1:{m_t}',
                            'CADENCE_FRONTEND_TCHANNEL': f'127.0.0.1:{fe_t}',
                            'CADENCE_MATCHING_TLS_CERT': str(artifacts / 'tls-cert.pem')})
        for proc in procs:
            if proc.poll() is not None:
                raise RuntimeError(f'real-stack process exited early with {proc.returncode}; see {work}')
    finally:
        for proc in procs:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
        for proc in procs:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()


if args.history_bin:
    run_sql_stack()
print(f'Native Matching Thrift compatibility passed; artifacts: {artifacts}')
