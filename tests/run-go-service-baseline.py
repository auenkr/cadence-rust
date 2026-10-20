#!/usr/bin/env python3
"""Boot the original Go services with SQLite and check the shared service probe.

Run in tmux cadence-rust. Default mode is an all-Go baseline.
--matching-delivery checks the Rust Matching RPC/History boundary.
--rust-matching replaces Go Matching with Rust using test-only Fx discovery.
--history-transport selects Rust Matching's outbound History transport in the
production Ringpop lane (grpc or tchannel).
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifacts-dir', type=Path)
parser.add_argument('--rust-matching', action='store_true', help='route original Go Frontend/History/Worker to Rust Matching via test-only Fx discovery')
parser.add_argument('--matching-delivery', action='store_true', help='run Rust durable queue delivery against original Go History')
parser.add_argument('--matching-restart', action='store_true', help='restart Rust Matching over the same SQLite queue while Go services stay up')
parser.add_argument('--binary', type=Path, help='use an already built Go server')
parser.add_argument('--go-mixed-server-bin', type=Path, help='reuse a Go test binary built with the Rust Matching discovery overlay')
parser.add_argument('--rust-production-binary', type=Path, help='run an already built Rust server binary with the original Go Ringpop provider (requires production Matching Ringpop assembly)')
parser.add_argument('--history-transport', choices=('grpc', 'tchannel'), default='grpc',
    help='Rust Matching outbound History transport for --rust-production-binary (default: grpc)')
parser.add_argument('--mysql', action='store_true', help='run production Rust Matching and Go services on the same MySQL schema')
parser.add_argument('--evidence-dir', type=Path, help='copy the selected run evidence into this directory before the artifacts directory is removed')
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
work = (args.artifacts_dir or Path(tempfile.mkdtemp(prefix='cadence-go-baseline-'))).resolve()
work.mkdir(parents=True, exist_ok=True)
print(f'Go baseline artifacts: {work}', flush=True)
sys.path.insert(0, str(Path(__file__).resolve().parent))
import matching_evidence


def mysql_rows(sql):
    """Run one read-only MySQL statement against the shared database."""
    return matching_evidence.mysql_rows(
        sql,
        container=os.environ.get('CADENCE_MYSQL_DOCKER_CONTAINER'),
        password=mysql_password,
        database=mysql_database,
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
    )


def check_go_schema_only():
    """Confirm the shared database holds Go's schema and no private tables."""
    return matching_evidence.check_go_schema_only(
        root / 'go/schema/mysql/v8/cadence/schema.sql',
        container=os.environ.get('CADENCE_MYSQL_DOCKER_CONTAINER'),
        password=mysql_password,
        database=mysql_database,
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
    )
production_matching = args.rust_production_binary is not None
if production_matching and not (args.rust_matching or args.matching_restart):
    parser.error('--rust-production-binary requires --rust-matching or --matching-restart')
if production_matching and args.go_mixed_server_bin:
    parser.error('--rust-production-binary cannot use the Go test-binary discovery overlay')
if production_matching and (not args.rust_production_binary.is_file() or not os.access(args.rust_production_binary, os.X_OK)):
    parser.error(f'--rust-production-binary must name an existing executable: {args.rust_production_binary}')
if args.history_transport != 'grpc' and not production_matching:
    parser.error('--history-transport applies only with --rust-production-binary')
if args.mysql and not production_matching:
    parser.error('--mysql requires --rust-production-binary')
mysql_addr = os.environ.get('CADENCE_MYSQL_ADDR', '127.0.0.1:3306')
mysql_user = os.environ.get('CADENCE_MYSQL_USER', 'root')
mysql_password = os.environ.get('CADENCE_MYSQL_PASSWORD', 'cadence')
mysql_database = os.environ.get('CADENCE_MYSQL_DATABASE', 'cadence')
mysql_visibility_database = os.environ.get('CADENCE_MYSQL_VISIBILITY_DATABASE', 'cadence_visibility')
mysql_host, _, mysql_port = mysql_addr.rpartition(':')
if not mysql_host or not mysql_port.isdigit():
    parser.error('--mysql requires CADENCE_MYSQL_ADDR in host:port form')
binary = args.go_mixed_server_bin.resolve() if args.go_mixed_server_bin else (args.binary.resolve() if args.binary else work / 'cadence-server')
if args.go_mixed_server_bin and not (args.rust_matching or args.matching_restart):
    parser.error('--go-mixed-server-bin requires --rust-matching or --matching-restart')
if not args.binary and not args.go_mixed_server_bin:
    with (work / 'build.log').open('w') as output:
        if production_matching:
            build = ['go', 'build', '-p', '1', '-ldflags=-s -w', '-o', str(binary), '.']
        elif args.rust_matching or args.matching_restart:
            overlay = work / 'overlay.json'
            overlay.write_text(json.dumps({'Replace': {str(root / 'go/cmd/server/cadence/rust_matching_peer_test.go'): str(root / 'tests/matching-peer-go_test.go')}}))
            build = ['go', 'test', '-c', '-p', '1', '-ldflags=-s -w', '-overlay', str(overlay), '-o', str(binary), './cadence']
        else:
            build = ['go', 'build', '-p', '1', '-ldflags=-s -w', '-o', str(binary), '.']
        subprocess.run(build, cwd=root / 'go/cmd/server', stdout=output, stderr=subprocess.STDOUT, check=True, timeout=1200)
matching_test_executable = None
if args.matching_delivery or ((args.rust_matching or args.matching_restart) and not production_matching):
    # Compile before booting Go or starting workflow deadlines. The probe's
    # per-workflow timeout must measure delivery, not linker resource contention.
    build = subprocess.run(['cargo', 'test', '--manifest-path', str(root / 'rust/Cargo.toml'),
        '-p', 'cadence-server', '--test', 'matching_delivery', '--no-run', '--message-format=json'],
        cwd=root, capture_output=True, text=True, timeout=1200)
    (work / 'rust-build.log').write_text(build.stdout + build.stderr)
    if build.returncode:
        raise subprocess.CalledProcessError(build.returncode, build.args)
    artifacts = []
    for line in build.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        target = message.get('target', {})
        executable = message.get('executable')
        if (message.get('reason') == 'compiler-artifact' and target.get('name') == 'matching_delivery'
                and 'test' in target.get('kind', []) and executable):
            artifacts.append(Path(executable).resolve())
    if len(artifacts) != 1:
        raise RuntimeError(f'Cargo reported {len(artifacts)} matching_delivery test executables; expected exactly one. See {work / "rust-build.log"}')
    matching_test_executable = artifacts[0]
    if not matching_test_executable.is_file() or not os.access(matching_test_executable, os.X_OK):
        raise RuntimeError(f'Cargo-reported matching_delivery test executable is missing or not executable: {matching_test_executable}')
config = work / 'config'
config.mkdir(exist_ok=True)
dynamic_config = work / 'dynamic-config.yaml'
dynamic_text = (root / 'go/config/dynamicconfig/development.yaml').read_text()
if args.rust_matching or args.matching_restart:
    dynamic_text += (
        '\nsystem.enableTasklistIsolation:\n'
        '  - value: true\n'
        'system.allIsolationGroups:\n'
        '  - value: ["zone-a", "zone-b"]\n'
    )
dynamic_config.write_text(dynamic_text)
keys = ['FRONTEND_PORT', 'MATCHING_PORT', 'HISTORY_PORT', 'WORKER_PORT',
        'FRONTEND_PORT_GRPC', 'MATCHING_PORT_GRPC', 'HISTORY_PORT_GRPC',
        'FRONTEND_PORT_PPROF', 'MATCHING_PORT_PPROF', 'HISTORY_PORT_PPROF', 'WORKER_PORT_PPROF']
if args.rust_matching or args.matching_restart:
    keys.append('RUST_MATCHING_PORT_GRPC')
if production_matching:
    keys.append('RUST_MATCHING_PORT')
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
text = text.replace('"config/dynamicconfig/development.yaml"', json.dumps(str(dynamic_config)))
# Keep archival outputs in this baseline's artifacts directory as well.
text = text.replace('/tmp/cadence_archival/development', str(work / 'history-archive'))
text = text.replace('/tmp/cadence_vis_archival/development', str(work / 'visibility-archive'))
text = text.replace('/tmp/blobstore', str(work / 'blobstore'))
(config / 'development.yaml').write_text(text)
if args.mysql:
    text = (root / 'go/config/development_mysql.yaml').read_text()
    text = re.sub(r'(?m)^(\s*databaseName:)\s*"cadence"\s*$', r'\1 ' + json.dumps(mysql_database), text, count=1)
    text = re.sub(r'(?m)^(\s*databaseName:)\s*"cadence_visibility"\s*$', r'\1 ' + json.dumps(mysql_visibility_database), text, count=1)
    text = re.sub(r'(?m)^(\s*connectAddr:)\s*"127\.0\.0\.1:3306"\s*$', r'\1 ' + json.dumps(mysql_addr), text)
    text = re.sub(r'(?m)^(\s*user:)\s*"root"\s*$', r'\1 ' + json.dumps(mysql_user), text)
    text = re.sub(r'(?m)^(\s*password:)\s*"cadence"\s*$', r'\1 ' + json.dumps(mysql_password), text)
    (config / 'development_mysql.yaml').write_text(text)
else:
    text = (root / 'go/config/development_sqlite.yaml').read_text()
    text = text.replace('"cadence.db"', json.dumps(str(work / 'cadence.db')))
    text = text.replace('"cadence_visibility.db"', json.dumps(str(work / 'cadence_visibility.db')))
    (config / 'development_sqlite.yaml').write_text(text)
if production_matching:
    rust_config = work / 'rust-matching.yaml'
    rust_dynamic_config = work / 'rust-dynamic-config.yaml'
    rust_dynamic_text = dynamic_text + (
        '\nsystem.enableGRPCOutbound:\n'
        f'  - value: {str(args.history_transport == "grpc").lower()}\n'
    )
    rust_dynamic_config.write_text(rust_dynamic_text)
    rust_text = (root / 'rust/config/development.yaml').read_text()
    database_name = json.dumps(str(work / 'cadence.db'))
    if args.mysql:
        persistence_config = (
            'persistence:\n'
            '  defaultStore: mysql-default\n'
            '  visibilityStore: mysql-visibility\n'
            '  numHistoryShards: 4\n'
            '  datastores:\n'
            '    mysql-default:\n'
            '      sql:\n'
            '        pluginName: mysql\n'
            '        databaseName: ' + json.dumps(mysql_database) + '\n'
            '        connectAddr: ' + json.dumps(mysql_addr) + '\n'
            '        connectProtocol: tcp\n'
            '        user: ' + json.dumps(mysql_user) + '\n'
            '        password: ' + json.dumps(mysql_password) + '\n'
            '        nShards: 1\n'
            '        maxConns: 20\n'
            '        maxIdleConns: 20\n'
            '    mysql-visibility:\n'
            '      sql:\n'
            '        pluginName: mysql\n'
            '        databaseName: ' + json.dumps(mysql_visibility_database) + '\n'
            '        connectAddr: ' + json.dumps(mysql_addr) + '\n'
            '        connectProtocol: tcp\n'
            '        user: ' + json.dumps(mysql_user) + '\n'
            '        password: ' + json.dumps(mysql_password) + '\n'
            '        nShards: 1\n'
            '        maxConns: 2\n'
            '        maxIdleConns: 2\n'
        )
    else:
        persistence_config = (
        'persistence:\n'
        '  defaultStore: sqlite-default\n'
        '  visibilityStore: sqlite-visibility\n'
        '  numHistoryShards: 4\n'
        '  datastores:\n'
        '    sqlite-default:\n'
        '      sql:\n'
        '        pluginName: sqlite\n'
        '        databaseName: ' + database_name + '\n'
        '        nShards: 1\n'
        '    sqlite-visibility:\n'
        '      sql:\n'
        '        pluginName: sqlite\n'
        '        databaseName: ' + json.dumps(str(work / 'cadence_visibility.db')) + '\n'
        )
    rust_text, replaced = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', persistence_config, rust_text, count=1)
    if replaced != 1:
        raise RuntimeError('could not replace Rust Matching persistence config')
    seeds = ', '.join(json.dumps(f'127.0.0.1:{ports[key]}') for key in
                      ['FRONTEND_PORT', 'HISTORY_PORT', 'WORKER_PORT'])
    rust_text, replaced = re.subn(r'(?m)^  bootstrapHosts: \[.*\]$', f'  bootstrapHosts: [{seeds}]', rust_text, count=1)
    if replaced != 1:
        raise RuntimeError('could not configure Rust Ringpop bootstrap hosts')
    # The compatibility probe launches Rust from the repository root. Keep
    # this path anchored to the isolated run directory so the transport and
    # isolation settings written above are the ones Rust actually loads.
    rust_text = rust_text.replace('filepath: "config/dynamicconfig/development.yaml"',
        'filepath: ' + json.dumps(str(rust_dynamic_config)))
    rust_config.write_text(rust_text)
    (work / 'rust-matching-command.json').write_text(json.dumps([
        str(args.rust_production_binary.resolve()), 'start', '--config', str(rust_config), '--root', str(work),
        '--services', 'matching', '--port', '0', '--history-grpc-port', ports['HISTORY_PORT_GRPC'],
        '--matching-grpc-port', ports['RUST_MATCHING_PORT_GRPC'],
        '--matching-tchannel-port', ports['RUST_MATCHING_PORT'],
        '--data-dir', str(work / 'rust-matching-data')], indent=2) + '\n')
command = [str(binary), '--config', str(config), '--zone', 'mysql' if args.mysql else 'sqlite', 'start',
           '--auto-setup', '--services', 'frontend,history,worker' if production_matching else 'frontend,history,matching,worker']
environment = None
if (args.rust_matching or args.matching_restart) and not production_matching:
    import os
    command = [str(binary), '-test.run=^TestRustMatchingGoServices$', '-test.timeout=15m']
    environment = {**os.environ, 'CADENCE_GO_CONFIG': str(config), 'CADENCE_RUST_MATCHING_ADDR': f'127.0.0.1:{ports["RUST_MATCHING_PORT_GRPC"]}'}
results = {
    'topology': ('Go Frontend/History/Worker with Rust Matching via production Ringpop' if production_matching else ('Go Frontend/History/Worker with Rust Matching RPC' if args.rust_matching or args.matching_restart else ('Go services with Rust queue/History delivery boundary' if args.matching_delivery else 'all-Go'))),
    'goMatchingStarted': not (production_matching or args.rust_matching or args.matching_restart),
    'rustProductionBinary': str(args.rust_production_binary.resolve()) if production_matching else None,
    'ringpopGate': 'original Go PeerProvider is active; the production probe must discover Rust Matching through Go Frontend before SDK operations' if production_matching else 'test-only static PeerProvider overlay in Rust Matching compatibility mode',
    'historyTransport': args.history_transport if production_matching else None,
    'rustDynamicConfigFile': str(rust_dynamic_config) if production_matching else None,
    'historyRoutingGate': ('Rust Matching resolves History peers through its joined Ringpop view and uses the selected gRPC/Thrift-TChannel client' if production_matching else None),
    'acceptanceEligible': not production_matching,
    'serverCommand': command,
    'artifactsDirectory': str(work),
}


def copy_evidence(destination, results):
    """Copy the selected run evidence into a durable directory."""
    config_files = sorted(config.glob('*.yaml'))
    if production_matching:
        config_files += [Path(rust_config), Path(rust_dynamic_config)]
    binaries = {'goBinary': Path(binary)}
    if production_matching:
        binaries['rustProductionBinary'] = Path(args.rust_production_binary)
    return matching_evidence.copy_evidence(
        destination,
        work=work,
        config_files=config_files,
        repository=root,
        binaries=binaries,
        results=results,
    )


environment_note = {key: value for key, value in os.environ.items()
                   if key.startswith('CADENCE_') and 'PASSWORD' not in key.upper()}
(work / 'command.txt').write_text(
    'env ' + ' '.join(f'{key}={value}' for key, value in sorted(environment_note.items())) + '\n'
    + ' '.join([shlex.quote(part) for part in [sys.executable, *sys.argv]]) + '\n')
exit_code = 0
failure = None
try:
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
                if production_matching:
                    probe = ['node', str(root / 'tests/matching-service-compat.cjs'),
                        f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}', f'127.0.0.1:{ports["HISTORY_PORT_GRPC"]}',
                        f'127.0.0.1:{ports["RUST_MATCHING_PORT_GRPC"]}', str(work / 'rust-matching'), f'127.0.0.1:{ports["FRONTEND_PORT"]}',
                        'restart' if args.matching_restart else '', str(server.pid), 'production']
                elif args.rust_matching or args.matching_restart:
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
                probe_environment = None
                if matching_test_executable and (args.rust_matching or args.matching_restart):
                    probe_environment = {**os.environ, 'CADENCE_MATCHING_DELIVERY_TEST_EXECUTABLE': str(matching_test_executable)}
                if production_matching:
                    probe_environment = {**os.environ,
                        'CADENCE_RUST_MATCHING_PRODUCTION_BINARY': str(args.rust_production_binary.resolve()),
                        'CADENCE_RUST_MATCHING_PRODUCTION_CONFIG': str(rust_config),
                        'CADENCE_RUST_MATCHING_PRODUCTION_TCHANNEL': ports['RUST_MATCHING_PORT'],
                        'CADENCE_RUST_MATCHING_PRODUCTION_GRPC_PORT': ports['RUST_MATCHING_PORT_GRPC'],
                        'CADENCE_RUST_MATCHING_PRODUCTION_HISTORY_GRPC_PORT': ports['HISTORY_PORT_GRPC'],
                        'CADENCE_RUST_MATCHING_PRODUCTION_DATA_DIR': str(work / 'rust-matching-data'),
                        'CADENCE_MATCHING_SQLITE_DB': str(work / 'cadence.db')}
                    if args.mysql:
                        probe_environment.update({
                            'CADENCE_MATCHING_MYSQL': '1',
                            'CADENCE_MYSQL_HOST': mysql_host,
                            'CADENCE_MYSQL_PORT': mysql_port,
                            'CADENCE_MYSQL_USER': mysql_user,
                            'CADENCE_MYSQL_PASSWORD': mysql_password,
                            'CADENCE_MYSQL_DATABASE': mysql_database,
                        })
                        if os.environ.get('CADENCE_MYSQL_DOCKER_CONTAINER'):
                            probe_environment['CADENCE_MYSQL_DOCKER_CONTAINER'] = os.environ['CADENCE_MYSQL_DOCKER_CONTAINER']
                result = subprocess.run(probe, cwd=root, stdout=output, stderr=subprocess.STDOUT,
                    env=probe_environment, timeout=1200 if (args.matching_restart and production_matching) else (900 if args.matching_restart else (600 if args.matching_delivery or args.rust_matching else 120)))
            results['probeExitCode'] = result.returncode
            results['goServicesPid'] = server.pid
            results['goServicesAliveAfterProbe'] = server.poll() is None
            observations_path = work / 'rust-matching' / 'db-observations.json'
            if not observations_path.is_file():
                observations_path = work / 'db-observations.json'
            if observations_path.is_file():
                observations = json.loads(observations_path.read_text())
                results['taskGarbageCollection'] = observations.get('taskGarbageCollection')
                results['domainMetadata'] = observations.get('domainMetadata')
                results['decoderFixtureCases'] = observations.get('decoderFixtureCases')
            if args.mysql:
                try:
                    results['goSchemaOnly'] = check_go_schema_only()
                except Exception as error:  # the schema observation must not mask the probe failure
                    results['goSchemaOnly'] = {'error': f'{type(error).__name__}: {error}'}
            (work / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
            if result.returncode:
                raise RuntimeError(f'Go baseline probe failed; see {work / "probe.log"}')
        finally:
            server.terminate()
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
except BaseException as error:
    exit_code = 1
    failure = f'{type(error).__name__}: {error}'
    raise
finally:
    results['runExitCode'] = exit_code
    results['failure'] = failure
    (work / 'run.exit').write_text(f'{exit_code}\n')
    (work / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    if args.evidence_dir:
        copied = copy_evidence(args.evidence_dir, results)
        print(f'Selected evidence copied to {args.evidence_dir}: {copied}', flush=True)
if production_matching:
    print(f'Preliminary production process probe completed; it is not Matching acceptance evidence: {work / "results.json"}', flush=True)
else:
    print(f'Service baseline passed: {work / "results.json"}', flush=True)
