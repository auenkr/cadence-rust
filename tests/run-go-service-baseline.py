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
parser.add_argument('--rust-production-binary', type=Path, help='run an already built Rust server binary with the original Go Ringpop provider (requires production Matching Ringpop assembly)')
parser.add_argument('--history-transport', choices=('grpc', 'tchannel'), default='grpc',
    help='Rust Matching outbound History transport for --rust-production-binary (default: grpc)')
parser.add_argument('--postgres', action='store_true', help='run production Rust Matching and original Go services against the same Go-configured PostgreSQL schema; requires CADENCE_POSTGRES_* settings')
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
work = (args.artifacts_dir or Path(tempfile.mkdtemp(prefix='cadence-go-baseline-'))).resolve()
work.mkdir(parents=True, exist_ok=True)
print(f'Go baseline artifacts: {work}', flush=True)
production_matching = args.rust_production_binary is not None
if production_matching and not (args.rust_matching or args.matching_restart):
    parser.error('--rust-production-binary requires --rust-matching or --matching-restart')
if production_matching and args.go_mixed_server_bin:
    parser.error('--rust-production-binary cannot use the Go test-binary discovery overlay')
if production_matching and (not args.rust_production_binary.is_file() or not os.access(args.rust_production_binary, os.X_OK)):
    parser.error(f'--rust-production-binary must name an existing executable: {args.rust_production_binary}')
if args.history_transport != 'grpc' and not production_matching:
    parser.error('--history-transport applies only with --rust-production-binary')
if args.postgres and not production_matching:
    parser.error('--postgres requires --rust-production-binary')
postgres_addr = os.environ.get('CADENCE_POSTGRES_ADDR', '127.0.0.1:5432')
postgres_user = os.environ.get('CADENCE_POSTGRES_USER', 'postgres')
postgres_password = os.environ.get('CADENCE_POSTGRES_PASSWORD', 'cadence')
postgres_database = os.environ.get('CADENCE_POSTGRES_DATABASE', 'cadence')
postgres_visibility_database = os.environ.get('CADENCE_POSTGRES_VISIBILITY_DATABASE', 'cadence_visibility')
postgres_sslmode = os.environ.get('CADENCE_POSTGRES_SSLMODE', '')
postgres_ca = os.environ.get('CADENCE_POSTGRES_CA_FILE', '')
postgres_cert = os.environ.get('CADENCE_POSTGRES_CERT_FILE', '')
postgres_key = os.environ.get('CADENCE_POSTGRES_KEY_FILE', '')
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
if args.postgres:
    text = (root / 'go/config/development_postgres.yaml').read_text()
    # Replace each datasource explicitly so password and database values do
    # not collide with repeated YAML scalar values.
    text = re.sub(r'(?m)^(\s*databaseName:)\s*"cadence"\s*$', r'\1 ' + json.dumps(postgres_database), text, count=1)
    text = re.sub(r'(?m)^(\s*databaseName:)\s*"cadence_visibility"\s*$', r'\1 ' + json.dumps(postgres_visibility_database), text, count=1)
    text = re.sub(r'(?m)^(\s*connectAddr:)\s*"127\.0\.0\.1:5432"\s*$', r'\1 ' + json.dumps(postgres_addr), text)
    text = re.sub(r'(?m)^(\s*user:)\s*"postgres"\s*$', r'\1 ' + json.dumps(postgres_user), text)
    text = re.sub(r'(?m)^(\s*password:)\s*"cadence"\s*$', r'\1 ' + json.dumps(postgres_password), text)
    if postgres_sslmode:
        tls = ('        tls:\n'
               '          enabled: true\n'
               '          sslmode: ' + json.dumps(postgres_sslmode) + '\n')
        for field, value in [('caFile', postgres_ca), ('certFile', postgres_cert), ('keyFile', postgres_key)]:
            if value:
                tls += f'          {field}: {json.dumps(value)}\n'
        text = re.sub(r'(?m)^(\s*maxConns:.*)$', r'\1\n' + tls, text)
    (config / 'development_postgres.yaml').write_text(text)
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
    if args.postgres:
        tls_config = ''
        if postgres_sslmode:
            tls_config = ('        tls:\n'
                          '          enabled: true\n'
                          '          sslmode: ' + json.dumps(postgres_sslmode) + '\n')
            for field, value in [('caFile', postgres_ca), ('certFile', postgres_cert), ('keyFile', postgres_key)]:
                if value:
                    tls_config += f'          {field}: {json.dumps(value)}\n'
        persistence_text = (
            'persistence:\n'
            '  defaultStore: postgres-default\n'
            '  visibilityStore: postgres-visibility\n'
            '  numHistoryShards: 4\n'
            '  datastores:\n'
            '    postgres-default:\n'
            '      sql:\n'
            '        pluginName: postgres\n'
            '        databaseName: ' + json.dumps(postgres_database) + '\n'
            '        connectAddr: ' + json.dumps(postgres_addr) + '\n'
            '        user: ' + json.dumps(postgres_user) + '\n'
            '        password: ' + json.dumps(postgres_password) + '\n'
            '        nShards: 1\n'
            '        maxConns: 20\n' + tls_config +
            '    postgres-visibility:\n'
            '      sql:\n'
            '        pluginName: postgres\n'
            '        databaseName: ' + json.dumps(postgres_visibility_database) + '\n'
            '        connectAddr: ' + json.dumps(postgres_addr) + '\n'
            '        user: ' + json.dumps(postgres_user) + '\n'
            '        password: ' + json.dumps(postgres_password) + '\n'
            '        nShards: 1\n'
            '        maxConns: 2\n' + tls_config
        )
    else:
        persistence_text = (
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
    rust_text, replaced = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', persistence_text, rust_text, count=1)
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
command = [str(binary), '--config', str(config), '--zone', 'postgres' if args.postgres else 'sqlite', 'start',
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
                pg_host, pg_port = postgres_addr.rsplit(':', 1)
                probe_environment = {**os.environ,
                    'CADENCE_RUST_MATCHING_PRODUCTION_BINARY': str(args.rust_production_binary.resolve()),
                    'CADENCE_RUST_MATCHING_PRODUCTION_CONFIG': str(rust_config),
                    'CADENCE_RUST_MATCHING_PRODUCTION_TCHANNEL': ports['RUST_MATCHING_PORT'],
                    'CADENCE_RUST_MATCHING_PRODUCTION_GRPC_PORT': ports['RUST_MATCHING_PORT_GRPC'],
                    'CADENCE_RUST_MATCHING_PRODUCTION_HISTORY_GRPC_PORT': ports['HISTORY_PORT_GRPC'],
                    'CADENCE_RUST_MATCHING_PRODUCTION_DATA_DIR': str(work / 'rust-matching-data'),
                    'CADENCE_MATCHING_SQLITE_DB': str(work / 'cadence.db'),
                    'CADENCE_MATCHING_POSTGRES': 'true' if args.postgres else '',
                    'CADENCE_POSTGRES_HOST': pg_host if args.postgres else '',
                    'CADENCE_POSTGRES_PORT': pg_port if args.postgres else '',
                    'CADENCE_POSTGRES_USER': postgres_user if args.postgres else '',
                    'CADENCE_POSTGRES_PASSWORD': postgres_password if args.postgres else '',
                    'CADENCE_POSTGRES_DATABASE': postgres_database if args.postgres else '',
                    'CADENCE_POSTGRES_SSLMODE': postgres_sslmode,
                    'CADENCE_POSTGRES_CA_FILE': postgres_ca,
                    'CADENCE_POSTGRES_CERT_FILE': postgres_cert,
                    'CADENCE_POSTGRES_KEY_FILE': postgres_key}
            result = subprocess.run(probe, cwd=root, stdout=output, stderr=subprocess.STDOUT,
                env=probe_environment, timeout=900 if args.matching_restart else (600 if args.matching_delivery or args.rust_matching else 120))
        (work / 'results.json').write_text(json.dumps({'probeExitCode': result.returncode,
            'topology': ('Go Frontend/History/Worker with Rust Matching via production Ringpop' if production_matching else ('Go Frontend/History/Worker with Rust Matching RPC' if args.rust_matching or args.matching_restart else ('Go services with Rust queue/History delivery boundary' if args.matching_delivery else 'all-Go'))),
            'goServicesPid': server.pid, 'goServicesAliveAfterProbe': server.poll() is None,
            'goMatchingStarted': not (production_matching or args.rust_matching or args.matching_restart),
            'rustProductionBinary': str(args.rust_production_binary.resolve()) if production_matching else None,
            'ringpopGate': 'original Go PeerProvider is active; the production probe must discover Rust Matching through Go Frontend before SDK operations' if production_matching else 'test-only static PeerProvider overlay in Rust Matching compatibility mode',
            'historyTransport': args.history_transport if production_matching else None,
            'rustDynamicConfigFile': str(rust_dynamic_config) if production_matching else None,
            'historyRoutingGate': ('Rust Matching resolves History peers through its joined Ringpop view and uses the selected gRPC/Thrift-TChannel client' if production_matching else None),
            'acceptanceEligible': not production_matching,
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
    print(f'Preliminary production process probe completed; it is not Matching acceptance evidence: {work / "results.json"}', flush=True)
else:
    print(f'Service baseline passed: {work / "results.json"}', flush=True)

if args.postgres:
    # Start every Go service against the same schema after Rust Matching has
    # exited. This confirms that Go can reopen the schema after Rust has used
    # its task and domain tables, and leaves a second SDK workflow transcript.
    evidence_file = work / 'rust-matching' / 'rust-postgres-evidence.json'
    if not evidence_file.is_file():
        raise RuntimeError(f'Rust probe did not save PostgreSQL evidence: {evidence_file}')
    rust_evidence = json.loads(evidence_file.read_text())
    pg_host, pg_port = postgres_addr.rsplit(':', 1)
    pg_environment = {**os.environ,
        'PGHOST': pg_host, 'PGPORT': pg_port, 'PGUSER': postgres_user,
        'PGPASSWORD': postgres_password, 'PGDATABASE': postgres_database,
        'PGSSLMODE': postgres_sslmode or 'disable'}
    if postgres_ca:
        pg_environment['PGSSLROOTCERT'] = postgres_ca
    if postgres_cert:
        pg_environment['PGSSLCERT'] = postgres_cert
    if postgres_key:
        pg_environment['PGSSLKEY'] = postgres_key
    sql_checks = {
        'goDomainRow': "SELECT count(*) FROM domains WHERE id=decode(replace(:'domain_id','-',''),'hex') AND name=:'domain'",
        'rustTaskListRow': "SELECT count(*) FROM task_lists WHERE domain_id=decode(replace(:'domain_id','-',''),'hex') AND name=:'task_list' AND task_type=:'task_type'::smallint AND range_id > 0 AND octet_length(data) > 0 AND data_encoding IN ('thriftrw','thriftrw_snappy')",
        'goDomainMetadataRow': "SELECT count(*) FROM domain_metadata WHERE notification_version > 0",
        'goDomainBlob': "SELECT count(*) FROM domains WHERE id=decode(replace(:'domain_id','-',''),'hex') AND octet_length(data) > 0 AND data_encoding IN ('thriftrw','thriftrw_snappy')",
    }
    sql_evidence = {}
    for name, statement in sql_checks.items():
        check = subprocess.run(['psql', '-X', '-A', '-t', '-w', '-v', 'ON_ERROR_STOP=1',
            '-v', f'domain_id={rust_evidence["domainId"]}', '-v', f'domain={rust_evidence["domain"]}',
            '-v', f'task_list={rust_evidence["taskList"]}', '-v',
            f'task_type={rust_evidence.get("taskType", 0)}', '-f', '-'],
            input=statement + '\n', env=pg_environment, capture_output=True, text=True, timeout=30)
        (work / f'{name}.log').write_text(check.stdout + check.stderr)
        if check.returncode:
            raise RuntimeError(f'PostgreSQL {name} check failed; see {work / f"{name}.log"}')
        count = int(check.stdout.strip())
        if count < 1:
            raise RuntimeError(f'PostgreSQL {name} check found no shared record')
        sql_evidence[name] = count
    (work / 'postgres-record-verification.json').write_text(json.dumps({
        'domainRow': sql_evidence['goDomainRow'],
        'rustMatchingTaskListRow': sql_evidence['rustTaskListRow'],
        'goDomainMetadataRow': sql_evidence['goDomainMetadataRow'],
        'goDomainBlob': sql_evidence['goDomainBlob'],
        'database': postgres_database,
        'evidence': str(evidence_file),
    }, indent=2) + '\n')
    verify_command = [str(binary), '--config', str(config), '--zone', 'postgres', 'start',
        '--services', 'frontend,history,matching,worker']
    verify_log = work / 'go-verification-server.log'
    with verify_log.open('w') as log:
        verifier = subprocess.Popen(verify_command, cwd=work, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 90
            while True:
                if verifier.poll() is not None:
                    raise RuntimeError(f'Go verification services exited; see {verify_log}')
                try:
                    with socket.create_connection(('127.0.0.1', int(ports['FRONTEND_PORT_GRPC'])), timeout=0.5):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f'Go verification startup timed out; see {verify_log}')
                    time.sleep(0.1)
            pending = rust_evidence.get('goMatchingPendingTask')
            if not pending:
                raise RuntimeError('Rust PostgreSQL probe did not leave a pending task for Go Matching consumption')
            consume_environment = {**os.environ,
                'CADENCE_MIXED_MATCHING_DOMAIN': rust_evidence['domain'],
                'CADENCE_TCHANNEL_PEER': f'127.0.0.1:{ports["FRONTEND_PORT"]}',
                'CADENCE_MIXED_RESTART_WORKFLOW_ID': pending['workflowId'],
                'CADENCE_MIXED_RESTART_TASK_LIST': pending['taskList']}
            with (work / 'go-matching-consume-sdk.log').open('w') as output:
                consume = subprocess.run(['go', 'run', '.'], cwd=root / 'tests/go-sdk',
                    env=consume_environment, stdout=output, stderr=subprocess.STDOUT,
                    timeout=180)
            if consume.returncode:
                raise RuntimeError(f'Go Matching could not consume the Rust-produced PostgreSQL task; see {work / "go-matching-consume-sdk.log"}')
            consume_rows = subprocess.run(['psql', '-X', '-A', '-t', '-w', '-v', 'ON_ERROR_STOP=1',
                '-v', f'domain_id={rust_evidence["domainId"]}', '-v', f'task_list={pending["taskList"]}',
                '-v', f'task_type={pending.get("taskType", 0)}', '-f', '-'],
                input="SELECT count(*) FROM tasks WHERE domain_id=decode(replace(:'domain_id','-',''),'hex') AND task_list_name=:'task_list' AND task_type=:'task_type'::smallint;\n",
                env=pg_environment, capture_output=True, text=True, timeout=30)
            (work / 'go-consumed-task-row-count.log').write_text(consume_rows.stdout + consume_rows.stderr)
            if consume_rows.returncode or consume_rows.stdout.strip() != '0':
                raise RuntimeError(f'Go Matching completion left {consume_rows.stdout.strip()!r} pending task rows; see {work / "go-consumed-task-row-count.log"}')
            with (work / 'go-verification-probe.log').open('w') as output:
                verification = subprocess.run(['node', str(root / 'tests/matching-postgres-go-verify.cjs'),
                    f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}', rust_evidence['domain'],
                    rust_evidence['workflowId'], rust_evidence['runId'], rust_evidence['taskList'],
                    pending['workflowId'], pending['runId'], pending['taskList']],
                    cwd=root, stdout=output, stderr=subprocess.STDOUT, timeout=300)
            if not verification.returncode:
                # Go's task-ID block must match the range row it persisted: block =
                # ((range-1)*100000+1, range*100000). Read the rows after the Go call,
                # since DescribeTaskList can lease and advance a range.
                verified = json.loads((work / 'go-verification-probe.log').read_text().strip().splitlines()[-1])
                for key, name in (('taskList', rust_evidence['taskList']), ('consumedTaskList', pending['taskList'])):
                    range_query = subprocess.run(['psql', '-X', '-A', '-t', '-w', '-v', 'ON_ERROR_STOP=1',
                        '-v', f'domain_id={rust_evidence["domainId"]}', '-v', f'task_list={name}',
                        '-v', 'task_type=0', '-f', '-'],
                        input="SELECT range_id FROM task_lists WHERE domain_id=decode(replace(:'domain_id','-',''),'hex') AND name=:'task_list' AND task_type=:'task_type'::smallint;\n",
                        env=pg_environment, capture_output=True, text=True, timeout=30)
                    (work / f'range-id-{key}.log').write_text(range_query.stdout + range_query.stderr)
                    if range_query.returncode or not range_query.stdout.strip().isdigit():
                        raise RuntimeError(f'could not read decision task-list range for {name}; see {work / f"range-id-{key}.log"}')
                    range_id = int(range_query.stdout.strip())
                    block = verified['taskBlocks'][key]
                    if block != {'startId': str((range_id - 1) * 100000 + 1), 'endId': str(range_id * 100000)}:
                        raise RuntimeError(f'Go task-ID block {block} does not match persisted range {range_id} for {name}')
            (work / 'go-verification-results.json').write_text(json.dumps({
                'exitCode': verification.returncode,
                'topology': 'original Go Frontend/History/Matching/Worker reopened PostgreSQL after Rust Matching mixed-service run',
                'serverCommand': verify_command,
                'schemaWasSeededByGoAutoSetup': True,
                'goMatchingConsumedRustProducedPendingTask': True,
                'rustProducedPendingTaskRowsAfterGoConsumption': 0,
                'exactlyOnceEvidence': 'one workflow completion event and zero remaining task rows',
                'goMatchingConsumeSDKLog': str(work / 'go-matching-consume-sdk.log'),
                'evidenceLog': str(work / 'go-verification-probe.log'),
            }, indent=2) + '\n')
            if verification.returncode:
                raise RuntimeError(f'Go verification probe failed; see {work / "go-verification-probe.log"}')
        finally:
            verifier.terminate()
            try:
                verifier.wait(timeout=20)
            except subprocess.TimeoutExpired:
                verifier.kill()
                verifier.wait(timeout=10)
    print(f'Go PostgreSQL verification completed: {work / "go-verification-results.json"}', flush=True)
