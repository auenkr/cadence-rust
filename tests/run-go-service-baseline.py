#!/usr/bin/env python3
"""Boot original Go services and check the shared service probe.

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
import socket
import subprocess
import tempfile
import time
import uuid

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
parser.add_argument('--matching-shard-distributor-address', help='configure production Rust Matching to use this shard-manager gRPC endpoint')
parser.add_argument('--matching-shard-distributor-namespace', help='configure one production Matching shard-manager namespace')
parser.add_argument('--matching-onboarding-percentage', type=int, help='set matching.percentageOnboardedToShardManager in Rust dynamic config')
parser.add_argument('--matching-failure-recovery', action='store_true', help='run a two-instance production Matching failure/recovery probe')
parser.add_argument('--matching-idle-check-seconds', type=int, help='override the Matching idle-task-list check interval in seconds')
parser.add_argument('--cassandra', action='store_true', help='run production Rust Matching and original Go services against the same existing Go Cassandra schemas')
parser.add_argument('--cassandra-hosts', default=os.environ.get('CADENCE_CASSANDRA_CONTACT_POINTS', '127.0.0.1'))
parser.add_argument('--cassandra-port', type=int, default=int(os.environ.get('CADENCE_CASSANDRA_PORT', '9042')))
parser.add_argument('--cassandra-keyspace', default=os.environ.get('CADENCE_CASSANDRA_KEYSPACE', 'cadence'))
parser.add_argument('--cassandra-visibility-keyspace', default=os.environ.get('CADENCE_CASSANDRA_VISIBILITY_KEYSPACE', 'cadence_visibility'))
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
work = (args.artifacts_dir or Path(tempfile.mkdtemp(prefix='cadence-go-baseline-'))).resolve()
work.mkdir(parents=True, exist_ok=True)
print(f'Go baseline artifacts: {work}', flush=True)
probe_id = str(uuid.uuid4())
production_matching = args.rust_production_binary is not None
if production_matching and not (args.rust_matching or args.matching_restart):
    parser.error('--rust-production-binary requires --rust-matching or --matching-restart')
if production_matching and args.go_mixed_server_bin:
    parser.error('--rust-production-binary cannot use the Go test-binary discovery overlay')
if production_matching and (not args.rust_production_binary.is_file() or not os.access(args.rust_production_binary, os.X_OK)):
    parser.error(f'--rust-production-binary must name an existing executable: {args.rust_production_binary}')
if args.history_transport != 'grpc' and not production_matching:
    parser.error('--history-transport applies only with --rust-production-binary')
sd_options = (args.matching_shard_distributor_address, args.matching_shard_distributor_namespace, args.matching_onboarding_percentage)
if any(value is not None for value in sd_options) and not production_matching:
    parser.error('shard-distributor options apply only with --rust-production-binary')
if any(value is not None for value in sd_options) and not all(value is not None for value in sd_options):
    parser.error('shard-distributor address, namespace, and onboarding percentage must be configured together')
if args.matching_onboarding_percentage is not None and not 0 <= args.matching_onboarding_percentage <= 100:
    parser.error('--matching-onboarding-percentage must be between 0 and 100')
if args.matching_failure_recovery and not production_matching:
    parser.error('--matching-failure-recovery requires --rust-production-binary')
if args.matching_idle_check_seconds is not None and args.matching_idle_check_seconds < 1:
    parser.error('--matching-idle-check-seconds must be positive')
if args.matching_idle_check_seconds is not None and not production_matching:
    parser.error('--matching-idle-check-seconds requires --rust-production-binary')
if args.cassandra and not (production_matching and args.matching_failure_recovery):
    parser.error('--cassandra requires --rust-production-binary and --matching-failure-recovery')
if args.cassandra and not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', args.cassandra_keyspace):
    parser.error('--cassandra-keyspace must be a CQL identifier')
if args.cassandra and not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', args.cassandra_visibility_keyspace):
    parser.error('--cassandra-visibility-keyspace must be a CQL identifier')
if args.cassandra and (not args.cassandra_hosts or any(char in args.cassandra_hosts for char in '\r\n"')):
    parser.error('--cassandra-hosts must be a non-empty comma-separated host list without YAML control characters')
if args.cassandra and not 1 <= args.cassandra_port <= 65535:
    parser.error('--cassandra-port must be between 1 and 65535')
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
if args.cassandra:
    dynamic_text += '\nsystem.enableDistributedSemaphore:\n  - value: true\n'
if args.matching_onboarding_percentage is not None:
    dynamic_text += (
        '\nmatching.percentageOnboardedToShardManager:\n'
        f'  - value: {args.matching_onboarding_percentage}\n'
    )
if args.matching_idle_check_seconds is not None:
    dynamic_text += (
        '\nmatching.idleTasklistCheckInterval:\n'
        f'  - value: {args.matching_idle_check_seconds}s\n'
        '    constraints:\n'
        f'      domainName: "service-compat-{probe_id}"\n'
        f'      taskListName: "production-idle-{probe_id}"\n'
    )
dynamic_config.write_text(dynamic_text)
keys = ['FRONTEND_PORT', 'MATCHING_PORT', 'HISTORY_PORT', 'WORKER_PORT',
        'FRONTEND_PORT_GRPC', 'MATCHING_PORT_GRPC', 'HISTORY_PORT_GRPC',
        'FRONTEND_PORT_PPROF', 'MATCHING_PORT_PPROF', 'HISTORY_PORT_PPROF', 'WORKER_PORT_PPROF']
if args.rust_matching or args.matching_restart:
    keys.append('RUST_MATCHING_PORT_GRPC')
if production_matching:
    keys.append('RUST_MATCHING_PORT')
    if args.matching_failure_recovery:
        keys.extend(['RUST_MATCHING_SECONDARY_PORT', 'RUST_MATCHING_SECONDARY_PORT_GRPC'])
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
if args.cassandra:
    go_config = text
    go_config = re.sub(r'(?m)^(\s+hosts:)\s*.*$',
        lambda match: f'{match.group(1)} {json.dumps(args.cassandra_hosts)}\n{match.group(1)[:-len("hosts:")]}port: {args.cassandra_port}',
        go_config)
    go_config = go_config.replace('keyspace: "cadence"', f'keyspace: "{args.cassandra_keyspace}"')
    go_config = go_config.replace('keyspace: "cadence_visibility"', f'keyspace: "{args.cassandra_visibility_keyspace}"')
    go_config = go_config.replace('"config/dynamicconfig/development.yaml"', json.dumps(str(dynamic_config)))
    (config / 'development_cassandra.yaml').write_text(go_config)
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
    sqlite_persistence = (
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
    if args.cassandra:
        cassandra_persistence = (
            'persistence:\n'
            '  defaultStore: cass-default\n'
            '  visibilityStore: cass-visibility\n'
            '  numHistoryShards: 4\n'
            '  datastores:\n'
            '    cass-default:\n'
            '      nosql:\n'
            '        pluginName: cassandra\n'
            f'        hosts: {json.dumps(args.cassandra_hosts)}\n'
            f'        port: {args.cassandra_port}\n'
            f'        keyspace: {args.cassandra_keyspace}\n'
            '        connectTimeout: 2s\n'
            '        timeout: 5s\n'
            '        consistency: LOCAL_QUORUM\n'
            '        serialConsistency: LOCAL_SERIAL\n'
            '    cass-visibility:\n'
            '      nosql:\n'
            '        pluginName: cassandra\n'
            f'        hosts: {json.dumps(args.cassandra_hosts)}\n'
            f'        port: {args.cassandra_port}\n'
            f'        keyspace: {args.cassandra_visibility_keyspace}\n'
        )
        persistence_text = cassandra_persistence
        rust_text = rust_text.replace('enableDistributedSemaphore: false', 'enableDistributedSemaphore: true')
        if 'enableDistributedSemaphore:' not in rust_text:
            rust_text = rust_text.replace('persistence:\n', 'enableDistributedSemaphore: true\npersistence:\n', 1)
    else:
        persistence_text = sqlite_persistence
    rust_text, replaced = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', persistence_text, rust_text, count=1)
    if replaced != 1:
        raise RuntimeError('could not replace Rust Matching persistence config')
    seeds = ', '.join(json.dumps(f'127.0.0.1:{ports[key]}') for key in
                      ['FRONTEND_PORT', 'HISTORY_PORT', 'WORKER_PORT'])
    if args.matching_failure_recovery:
        seeds += ', ' + ', '.join(json.dumps(f'127.0.0.1:{ports[key]}') for key in
                                  ['RUST_MATCHING_PORT', 'RUST_MATCHING_SECONDARY_PORT'])
    rust_text, replaced = re.subn(r'(?m)^  bootstrapHosts: \[.*\]$', f'  bootstrapHosts: [{seeds}]', rust_text, count=1)
    if replaced != 1:
        raise RuntimeError('could not configure Rust Ringpop bootstrap hosts')
    # The compatibility probe launches Rust from the repository root. Keep
    # this path anchored to the isolated run directory so the transport and
    # isolation settings written above are the ones Rust actually loads.
    rust_text = rust_text.replace('filepath: "config/dynamicconfig/development.yaml"',
        'filepath: ' + json.dumps(str(rust_dynamic_config)))
    if args.matching_shard_distributor_address is not None:
        rust_text, replaced = re.subn(
            r'(?m)^(  hostPort: ).*$',
            r'\g<1>' + json.dumps(args.matching_shard_distributor_address),
            rust_text,
            count=1,
        )
        if replaced != 1:
            raise RuntimeError('could not configure Rust shardDistributorClient.hostPort')
        rust_text, replaced = re.subn(
            r'(?m)^(    - namespace: ).*$',
            r'\g<1>' + json.dumps(args.matching_shard_distributor_namespace),
            rust_text,
            count=1,
        )
        if replaced != 1:
            raise RuntimeError('could not configure Rust shard-distributor namespace')
    rust_config.write_text(rust_text)
    (work / 'rust-matching-command.json').write_text(json.dumps([
        str(args.rust_production_binary.resolve()), 'start', '--config', str(rust_config), '--root', str(work),
        '--services', 'matching', '--port', '0', '--history-grpc-port', ports['HISTORY_PORT_GRPC'],
        '--matching-grpc-port', ports['RUST_MATCHING_PORT_GRPC'],
        '--matching-tchannel-port', ports['RUST_MATCHING_PORT'],
        '--data-dir', str(work / 'rust-matching-data')], indent=2) + '\n')
go_zone = 'cassandra' if args.cassandra else 'sqlite'
command = [str(binary), '--config', str(config), '--zone', go_zone, 'start',
           '--auto-setup', '--services', 'frontend,history,worker' if production_matching else 'frontend,history,matching,worker']
environment = None
if (args.rust_matching or args.matching_restart) and not production_matching:
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
                    **({'CADENCE_MATCHING_SQLITE_DB': str(work / 'cadence.db')} if not args.cassandra else {}),
                    'CADENCE_MATCHING_TEST_ID': probe_id,
                    'CADENCE_MATCHING_CASSANDRA_GATE': '1' if args.cassandra else '0',
                    'CADENCE_CASSANDRA_CONTACT_POINTS': args.cassandra_hosts,
                    'CADENCE_CASSANDRA_PORT': str(args.cassandra_port),
                    'CADENCE_CASSANDRA_KEYSPACE': args.cassandra_keyspace,
                    'CADENCE_MATCHING_SHARD_DISTRIBUTOR_CONTROL': os.environ.get('CADENCE_MATCHING_SHARD_DISTRIBUTOR_CONTROL', ''),
                    'CADENCE_MATCHING_SHARD_DISTRIBUTOR_NAMESPACE': args.matching_shard_distributor_namespace or '',
                    'CADENCE_MATCHING_ONBOARDING_PERCENTAGE': str(args.matching_onboarding_percentage if args.matching_onboarding_percentage is not None else '')}
            if production_matching and args.matching_failure_recovery:
                probe_environment.update({
                    'CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL': ports['RUST_MATCHING_SECONDARY_PORT'],
                    'CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT': ports['RUST_MATCHING_SECONDARY_PORT_GRPC'],
                    'CADENCE_MATCHING_FAILURE_RECOVERY': '1',
                })
            result = subprocess.run(probe, cwd=root, stdout=output, stderr=subprocess.STDOUT,
                env=probe_environment, timeout=1800 if args.matching_failure_recovery else (900 if args.matching_restart else (600 if args.matching_delivery or args.rust_matching else 120)))
        topology = (
            'Go Frontend/History/Worker and Rust Matching on shared Cassandra via production Ringpop'
            if args.cassandra else
            'Go Frontend/History/Worker with Rust Matching via production Ringpop'
            if production_matching else
            'Go Frontend/History/Worker with Rust Matching RPC'
            if args.rust_matching or args.matching_restart else
            'Go services with Rust queue/History delivery boundary'
            if args.matching_delivery else 'all-Go'
        )
        (work / 'results.json').write_text(json.dumps({'probeExitCode': result.returncode,
            'topology': topology,
            'persistence': ({'backend': 'cassandra', 'hosts': args.cassandra_hosts, 'port': args.cassandra_port,
                'defaultKeyspace': args.cassandra_keyspace, 'visibilityKeyspace': args.cassandra_visibility_keyspace}
                if args.cassandra else {'backend': 'sqlite'} if production_matching else None),
            'goServicesPid': server.pid, 'goServicesAliveAfterProbe': server.poll() is None,
            'goMatchingStarted': not (production_matching or args.rust_matching or args.matching_restart),
            'rustProductionBinary': str(args.rust_production_binary.resolve()) if production_matching else None,
            'ringpopGate': 'original Go PeerProvider is active; the production probe must discover Rust Matching through Go Frontend before SDK operations' if production_matching else 'test-only static PeerProvider overlay in Rust Matching compatibility mode',
            'historyTransport': args.history_transport if production_matching else None,
            'rustDynamicConfigFile': str(rust_dynamic_config) if production_matching else None,
            'historyRoutingGate': ('Rust Matching resolves History peers through its joined Ringpop view and uses the selected gRPC/Thrift-TChannel client' if production_matching else None),
            'acceptanceEligible': bool(args.cassandra) if production_matching else True,
            'serverCommand': command}, indent=2) + '\n')
        if result.returncode:
            raise RuntimeError(f'Go baseline probe failed; see {work / "probe.log"}')
    finally:
        server.terminate()
        try:
            server.wait(timeout=20)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=10)
if production_matching:
    if args.cassandra:
        print(f'Cassandra mixed-service Matching gate passed: {work / "results.json"}', flush=True)
    else:
        print(f'Preliminary production process probe completed; it is not Matching acceptance evidence: {work / "results.json"}', flush=True)
else:
    print(f'Service baseline passed: {work / "results.json"}', flush=True)
