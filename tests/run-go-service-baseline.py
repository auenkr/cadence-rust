#!/usr/bin/env python3
"""Boot the original Go services with SQLite and check the shared service probe.

Run in tmux cadence-rust. Default mode is an all-Go baseline.
--matching-delivery checks the Rust Matching RPC/History boundary.
--rust-matching replaces Go Matching with Rust using test-only Fx discovery.
--history-transport selects Rust Matching's outbound History transport in the
production Ringpop lane (grpc or tchannel).
"""
import argparse
import base64
import gzip
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

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
parser.add_argument('--matching-exporter', choices=('prometheus', 'statsd', 'm3'),
    help='configure and capture one Rust Matching exporter in the production Ringpop lane')
parser.add_argument('--mysql', action='store_true', help='run production Rust Matching and Go services on the same MySQL schema')
parser.add_argument('--postgres', action='store_true', help='run production Rust Matching and original Go services against the same Go-configured PostgreSQL schema; requires CADENCE_POSTGRES_* settings')
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


def validate_matching_exporter(exporter, work, repository, expected_domain):
    """Decode captured exporter output and require SDK-driven Matching metrics."""
    required = ('cadence_requests_per_tl', 'cadence_tasklist_request', 'poll_success_per_tl',
                'cadence_client_requests')
    # The production Ringpop probe sends inbound Matching RPCs through gRPC;
    # system.enableGRPCOutbound selects a separate Rust-to-History path.
    caller = 'cadence-frontend'
    # The mixed SDK probe's Matching requests enter through the production
    # gRPC listener (the outbound History transport setting is independent).
    transport = 'grpc'
    shutdown_path = work / 'rust-matching' / 'matching-shutdown.json'
    if not shutdown_path.is_file():
        shutdown_path = work / 'matching-shutdown.json'
    shutdown = json.loads(shutdown_path.read_text()) if shutdown_path.is_file() else None
    if not shutdown or shutdown.get('exitCode') != 0 or shutdown.get('signal') != 'SIGTERM':
        raise RuntimeError('Rust Matching did not provide a successful SIGTERM shutdown record')
    if exporter == 'prometheus':
        snapshots_path = work / 'prometheus-scrape-samples.jsonl.gz'
        snapshots = gzip.open(snapshots_path, 'rt', encoding='utf-8') if snapshots_path.is_file() else None
        if snapshots is not None:
            scrape_iter = (json.loads(line)['body'] for line in snapshots if line.strip())
        else:
            legacy_path = work / 'prometheus-scrape-samples.jsonl'
            scrape_iter = ((json.loads(line)['body'] for line in legacy_path.open(encoding='utf-8') if line.strip())
                           if legacy_path.is_file() else iter([(work / 'prometheus-scrape.txt').read_text(encoding='utf-8')]))
        sample_count = 0
        metric_values = {metric: [] for metric in required}
        request_lines = set()
        prometheus_labels = {
            label: re.sub(r'[^A-Za-z0-9_]', '_', value)
            for label, value in {
                'caller': caller,
                'transport': transport,
                'cadence_service': 'cadence-matching',
                'domain': expected_domain,
                'operation': 'RespondQueryTaskCompleted',
            }.items()
        }
        for scrape in scrape_iter:
            for line in scrape.splitlines():
                if not line or line.startswith('#'):
                    continue
                sample_count += 1
                for metric in required:
                    if metric in line:
                        match = re.search(r'\s([-+0-9.eE]+)$', line)
                        if match and float(match.group(1)) > 0:
                            metric_values[metric].append(float(match.group(1)))
                if ('cadence_requests_per_tl' in line
                    and f'caller="{prometheus_labels["caller"]}"' in line
                    and f'transport="{prometheus_labels["transport"]}"' in line
                    and f'cadence_service="{prometheus_labels["cadence_service"]}"' in line
                    and f'domain="{prometheus_labels["domain"]}"' in line
                    and f'operation="{prometheus_labels["operation"]}"' in line):
                    match = re.search(r'\s([-+0-9.eE]+)$', line)
                    if match and float(match.group(1)) > 0:
                        request_lines.add(line)
        if snapshots is not None:
            snapshots.close()
        if sample_count == 0:
            raise RuntimeError('Prometheus scrape succeeded but returned no metric samples')
        for metric in required:
            if not metric_values[metric]:
                raise RuntimeError(f'Prometheus metric {metric} has no positive SDK-driven sample')
        request_lines = sorted(request_lines)
        if not request_lines:
            raise RuntimeError('Prometheus lacks a positive query request sample with the expected domain/caller/transport/service labels')
        semantic = {'format': exporter, 'sampleCount': sample_count,
                    'requiredMetrics': list(required), 'expectedDomain': expected_domain,
                    'prometheusExpectedLabels': prometheus_labels,
                    'positiveRequiredMetricSamples': {key: len(value) for key, value in metric_values.items()},
                    'requestSamples': request_lines}
    elif exporter == 'statsd':
        captured = [json.loads(line) for line in (work / 'statsd-packets.json').read_text().splitlines() if line.strip()]
        packets = [item['base64'] for item in captured]
        if not captured:
            raise RuntimeError('StatsD capture received no UDP packets')
        decoded = [base64.b64decode(packet).decode('utf-8') for packet in packets]
        lines = [line for packet in decoded for line in packet.splitlines() if line]
        shutdown_packets = [item for item in captured if item['capturedAtUnixNs'] >= shutdown['sigtermSentUnixNs']]
        if not shutdown_packets:
            raise RuntimeError('StatsD emitted no UDP packets after SIGTERM; shutdown flush was not observed')
        shutdown_lines = [line for item in shutdown_packets
                          for line in base64.b64decode(item['base64']).decode('utf-8').splitlines() if line]
        for metric in required:
            values = [float(match.group(1)) for line in shutdown_lines if metric in line
                      for match in [re.search(r':([-+0-9.eE]+)\|[cgm]', line)] if match]
            if not values or max(values) <= 0:
                raise RuntimeError(f'StatsD SIGTERM flush lacks positive SDK-driven metric {metric}')
        requests = [line for line in lines if 'cadence_requests_per_tl' in line]
        rendered = '\n'.join(lines)
        for metric in required:
            if metric not in rendered:
                raise RuntimeError(f'StatsD output lacks SDK-driven Matching metric {metric}')
            values = [float(match.group(1)) for line in lines if metric in line
                      for match in [re.search(r':([-+0-9.eE]+)\|[cgm]', line)] if match]
            if not values or max(values) <= 0:
                raise RuntimeError(f'StatsD metric {metric} has no positive SDK-driven sample')
        qualified = [line for line in requests if f'.caller.{caller}' in line
                     and f'.transport.{transport}' in line
                     and '.cadence_service.cadence-matching' in line
                     and f'.domain.{expected_domain}' in line
                     and '.operation.RespondQueryTaskCompleted' in line]
        qualified_values = [float(match.group(1)) for line in qualified
                            for match in [re.search(r':([-+0-9.eE]+)\|[cgm]', line)] if match]
        if not qualified_values or max(qualified_values) <= 0:
            raise RuntimeError('StatsD lacks a positive query request sample with the expected domain/caller/transport/service labels')
        semantic = {'format': exporter, 'packetCount': len(packets), 'lineCount': len(lines),
                    'requiredMetrics': list(required), 'expectedDomain': expected_domain,
                    'requestLines': requests, 'qualifiedQueryLines': qualified,
                    'shutdownPacketCount': len(shutdown_packets), 'shutdownPacketLines': shutdown_lines,
                    'shutdownQuerySample': any(line in shutdown_lines for line in qualified)}
        if not semantic['shutdownQuerySample']:
            raise RuntimeError('StatsD SIGTERM flush did not contain the qualified SDK query sample')
    else:
        captured = [json.loads(line) for line in (work / 'm3-packets.json').read_text().splitlines() if line.strip()]
        packets = [base64.b64decode(item['base64']) for item in captured]
        if not captured:
            raise RuntimeError('M3 capture received no UDP packets')
        decoder = subprocess.run(
            ['go', 'run', './tools/matching-metrics-m3-oracle', 'decode-hex'],
            cwd=repository / 'go', input=''.join(packet.hex() + '\n' for packet in packets),
            capture_output=True, text=True, check=False, timeout=120)
        (work / 'm3-decoder-go.log').write_text(
            'command: go run ./tools/matching-metrics-m3-oracle decode-hex\n'
            f'exit: {decoder.returncode}\nstdout:\n{decoder.stdout}\nstderr:\n{decoder.stderr}')
        if decoder.returncode:
            raise RuntimeError(f'pinned Go M3 decoder failed; see {work / "m3-decoder-go.log"}')
        (work / 'm3-decoded.jsonl').write_text(decoder.stdout)
        batches = [json.loads(line) for line in decoder.stdout.splitlines() if line.strip()]
        if not batches:
            raise RuntimeError('M3 Go Compact decoder returned no metric batches')
        shutdown_packets = [packet for packet, item in zip(packets, captured)
                            if item['capturedAtUnixNs'] >= shutdown['sigtermSentUnixNs']]
        if not shutdown_packets:
            raise RuntimeError('M3 emitted no UDP packets after SIGTERM; shutdown flush was not observed')
        shutdown_decoder = subprocess.run(
            ['go', 'run', './tools/matching-metrics-m3-oracle', 'decode-hex'],
            cwd=repository / 'go', input=''.join(packet.hex() + '\n' for packet in shutdown_packets),
            capture_output=True, text=True, check=False, timeout=120)
        (work / 'm3-shutdown-decoder-go.log').write_text(
            'command: go run ./tools/matching-metrics-m3-oracle decode-hex (SIGTERM packets)\n'
            f'exit: {shutdown_decoder.returncode}\nstdout:\n{shutdown_decoder.stdout}\nstderr:\n{shutdown_decoder.stderr}')
        if shutdown_decoder.returncode:
            raise RuntimeError(f'pinned Go M3 shutdown decoder failed; see {work / "m3-shutdown-decoder-go.log"}')
        shutdown_batches = [json.loads(line) for line in shutdown_decoder.stdout.splitlines() if line.strip()]
        shutdown_metric_values = []
        for batch in shutdown_batches:
            for metric in batch.get('metrics', []):
                value = metric['value']
                if isinstance(value, dict):
                    shutdown_metric_values.append((metric['name'], value.get('count', value.get('gauge', value.get('timer', 0)))))
        for required_metric in required:
            if not any(required_metric in name and value > 0 for name, value in shutdown_metric_values):
                raise RuntimeError(f'M3 SIGTERM flush lacks positive SDK-driven metric {required_metric}')
        metrics = []
        for batch in batches:
            common = {tag['name']: tag['value'] for tag in batch.get('commonTags', [])}
            for metric in batch.get('metrics', []):
                tags = {**common, **{tag['name']: tag['value'] for tag in metric.get('tags', [])}}
                metrics.append((metric['name'], tags, metric['value']))
        for required_metric in required:
            matching_values = [value for name, _, value in metrics if required_metric in name]
            if not matching_values:
                raise RuntimeError(f'M3 output lacks SDK-driven Matching metric {required_metric}')
            if not any(value.get('count', value.get('gauge', value.get('timer', 0))) > 0
                       for value in matching_values if isinstance(value, dict)):
                raise RuntimeError(f'M3 metric {required_metric} has no positive SDK-driven value')
        request_metrics = [(name, tags, value) for name, tags, value in metrics
                           if 'cadence_requests_per_tl' in name
                           and tags.get('caller') == caller
                           and tags.get('transport') == transport
                           and tags.get('cadence_service') == 'cadence-matching'
                           and tags.get('domain') == expected_domain
                           and tags.get('operation') == 'RespondQueryTaskCompleted']
        positive_requests = [(name, tags, value) for name, tags, value in request_metrics
                             if isinstance(value, dict)
                             and value.get('count', value.get('gauge', value.get('timer', 0))) > 0]
        if not positive_requests:
            raise RuntimeError('M3 lacks a positive query request sample with the expected domain/caller/transport/service labels')
        shutdown_metrics = []
        for batch in shutdown_batches:
            common = {tag['name']: tag['value'] for tag in batch.get('commonTags', [])}
            for metric in batch.get('metrics', []):
                tags = {**common, **{tag['name']: tag['value'] for tag in metric.get('tags', [])}}
                value = metric['value']
                if ('cadence_requests_per_tl' in metric['name'] and tags.get('caller') == caller
                    and tags.get('transport') == transport
                    and tags.get('cadence_service') == 'cadence-matching'
                    and tags.get('domain') == expected_domain
                    and tags.get('operation') == 'RespondQueryTaskCompleted'
                    and isinstance(value, dict)
                    and value.get('count', value.get('gauge', value.get('timer', 0))) > 0):
                    shutdown_metrics.append({'name': metric['name'], 'tags': tags, 'value': value})
        if not shutdown_metrics:
            raise RuntimeError('M3 SIGTERM flush did not contain the qualified SDK query sample')
        _, tags, value = positive_requests[0]
        semantic = {'format': exporter, 'packetCount': len(packets), 'batchCount': len(batches),
                    'metricCount': len(metrics), 'requiredMetrics': list(required),
                    'expectedDomain': expected_domain,
                    'requestMetric': {'name': positive_requests[0][0], 'tags': tags, 'value': value},
                    'shutdownPacketCount': len(shutdown_packets),
                    'shutdownQuerySamples': shutdown_metrics,
                    'timestampsPreserved': True}
    (work / 'matching-exporter-semantics.json').write_text(json.dumps(semantic, indent=2) + '\n')
    return semantic
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
if args.postgres and not production_matching:
    parser.error('--postgres requires --rust-production-binary')
if args.mysql and args.postgres:
    parser.error('--mysql and --postgres select different production databases; choose one')
postgres_addr = os.environ.get('CADENCE_POSTGRES_ADDR', '127.0.0.1:5432')
postgres_user = os.environ.get('CADENCE_POSTGRES_USER', 'postgres')
postgres_password = os.environ.get('CADENCE_POSTGRES_PASSWORD', 'cadence')
postgres_database = os.environ.get('CADENCE_POSTGRES_DATABASE', 'cadence')
postgres_visibility_database = os.environ.get('CADENCE_POSTGRES_VISIBILITY_DATABASE', 'cadence_visibility')
postgres_sslmode = os.environ.get('CADENCE_POSTGRES_SSLMODE', '')
postgres_ca = os.environ.get('CADENCE_POSTGRES_CA_FILE', '')
postgres_cert = os.environ.get('CADENCE_POSTGRES_CERT_FILE', '')
postgres_key = os.environ.get('CADENCE_POSTGRES_KEY_FILE', '')
if args.matching_exporter and not production_matching:
    parser.error('--matching-exporter requires --rust-production-binary')
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
if args.postgres:
    text = (root / 'go/config/development_postgres.yaml').read_text()
    text = re.sub(r'(?m)^(\s*databaseName:)\s*"cadence"\s*$', r'\1 ' + json.dumps(postgres_database), text, count=1)
    text = re.sub(r'(?m)^(\s*databaseName:)\s*"cadence_visibility"\s*$', r'\1 ' + json.dumps(postgres_visibility_database), text, count=1)
    text = re.sub(r'(?m)^(\s*connectAddr:)\s*"127\.0\.0\.1:5432"\s*$', r'\1 ' + json.dumps(postgres_addr), text)
    text = re.sub(r'(?m)^(\s*user:)\s*"postgres"\s*$', r'\1 ' + json.dumps(postgres_user), text)
    text = re.sub(r'(?m)^(\s*password:)\s*"cadence"\s*$', r'\1 ' + json.dumps(postgres_password), text)
    if postgres_sslmode:
        tls = '        tls:\n          enabled: true\n          sslmode: ' + json.dumps(postgres_sslmode) + '\n'
        for field, value in [('caFile', postgres_ca), ('certFile', postgres_cert), ('keyFile', postgres_key)]:
            if value: tls += f'          {field}: {json.dumps(value)}\n'
        text = re.sub(r'(?m)^(\s*maxConns:.*)$', r'\1\n' + tls, text)
    (config / 'development_postgres.yaml').write_text(text)
elif args.mysql:
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
    exporter_address = None
    if args.matching_exporter:
        # Keep exporters confined to Matching. The original Go services remain
        # untouched and no Go Matching process is started in this lane.
        if args.matching_exporter == 'prometheus':
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                exporter_address = f'127.0.0.1:{listener.getsockname()[1]}'
            matching_metrics_yaml = (
                '    metrics:\n'
                '      prometheus:\n'
                '        listenAddress: ' + json.dumps(exporter_address) + '\n'
                '        timerType: histogram\n'
            )
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener:
                listener.bind(('127.0.0.1', 0))
                exporter_address = f'127.0.0.1:{listener.getsockname()[1]}'
            matching_metrics_yaml = (
                f'    metrics:\n      {args.matching_exporter}:\n'
                '        hostPort: ' + json.dumps(exporter_address) + '\n'
            )
            if args.matching_exporter == 'm3':
                matching_metrics_yaml += '        service: cadence_matching\n        env: test\n'
            else:
                # Go documents 1432 bytes as its default safe UDP payload for
                # local traffic. Keep aggregates pending until SIGTERM through
                # reportingInterval; split their shutdown flush into safe packets.
                matching_metrics_yaml += '        flushInterval: 1h\n        flushBytes: 1432\n'
        matching_metrics_yaml = matching_metrics_yaml.replace(
            '    metrics:\n', '    metrics:\n      reportingInterval: 1h\n', 1)
        rust_text, replaced_metrics = re.subn(
            r'(?ms)^  matching:\n(.*?)(?=^  (?:frontend|history|worker):|\Z)',
            lambda match: '  matching:\n' + re.sub(
                r'(?ms)^    metrics:\n.*?(?=^    [a-zA-Z][\w-]*:|\Z)',
                '', match.group(1)) + matching_metrics_yaml,
            rust_text, count=1)
        if replaced_metrics != 1:
            raise RuntimeError('could not configure isolated Rust Matching exporter')
    database_name = json.dumps(str(work / 'cadence.db'))
    if args.postgres:
        tls_config = ''
        if postgres_sslmode:
            tls_config = '        tls:\n          enabled: true\n          sslmode: ' + json.dumps(postgres_sslmode) + '\n'
            for field, value in [('caFile', postgres_ca), ('certFile', postgres_cert), ('keyFile', postgres_key)]:
                if value: tls_config += f'          {field}: {json.dumps(value)}\n'
        persistence_config = (
            'persistence:\n  defaultStore: postgres-default\n  visibilityStore: postgres-visibility\n  numHistoryShards: 4\n  datastores:\n'
            '    postgres-default:\n      sql:\n        pluginName: postgres\n'
            '        databaseName: ' + json.dumps(postgres_database) + '\n'
            '        connectAddr: ' + json.dumps(postgres_addr) + '\n'
            '        user: ' + json.dumps(postgres_user) + '\n        password: ' + json.dumps(postgres_password) + '\n'
            '        nShards: 1\n        maxConns: 20\n' + tls_config +
            '    postgres-visibility:\n      sql:\n        pluginName: postgres\n'
            '        databaseName: ' + json.dumps(postgres_visibility_database) + '\n'
            '        connectAddr: ' + json.dumps(postgres_addr) + '\n'
            '        user: ' + json.dumps(postgres_user) + '\n        password: ' + json.dumps(postgres_password) + '\n'
            '        nShards: 1\n        maxConns: 2\n' + tls_config)
    elif args.mysql:
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
command = [str(binary), '--config', str(config), '--zone', 'mysql' if args.mysql else ('postgres' if args.postgres else 'sqlite'), 'start',
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
                udp_capture = None
                udp_thread = None
                udp_packets = []
                udp_stop = threading.Event()
                prometheus_scrape_count = [0]
                prometheus_last_scrape = [None]
                prometheus_thread = None
                if args.matching_exporter in ('statsd', 'm3'):
                    udp_capture = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    udp_capture.bind(('127.0.0.1', int(exporter_address.rsplit(':', 1)[1])))
                    udp_capture.settimeout(0.1)

                    def collect_udp():
                        while True:
                            try:
                                packet, _ = udp_capture.recvfrom(65535)
                                udp_packets.append((packet, time.time_ns()))
                            except socket.timeout:
                                if udp_stop.is_set():
                                    break

                    udp_thread = threading.Thread(target=collect_udp, daemon=True)
                    udp_thread.start()
                elif args.matching_exporter == 'prometheus':
                    prometheus_capture = gzip.open(
                        work / 'prometheus-scrape-samples.jsonl.gz', 'wt', encoding='utf-8')
                    def scrape_prometheus():
                        while not udp_stop.is_set():
                            try:
                                with urllib.request.urlopen(f'http://{exporter_address}/metrics', timeout=1) as response:
                                    scrape = response.read().decode('utf-8')
                                    if any(line and not line.startswith('#') for line in scrape.splitlines()):
                                        prometheus_capture.write(json.dumps({
                                            'capturedAtUnixNs': time.time_ns(), 'body': scrape}) + '\n')
                                        prometheus_capture.flush()
                                        prometheus_scrape_count[0] += 1
                                        prometheus_last_scrape[0] = scrape
                            except Exception:
                                pass
                            udp_stop.wait(0.2)

                    prometheus_thread = threading.Thread(target=scrape_prometheus, daemon=True)
                    prometheus_thread.start()
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
                    if args.postgres:
                        pg_host, pg_port = postgres_addr.rsplit(':', 1)
                        probe_environment.update({
                            'CADENCE_MATCHING_POSTGRES': '1', 'CADENCE_POSTGRES_HOST': pg_host,
                            'CADENCE_POSTGRES_PORT': pg_port, 'CADENCE_POSTGRES_USER': postgres_user,
                            'CADENCE_POSTGRES_PASSWORD': postgres_password, 'CADENCE_POSTGRES_DATABASE': postgres_database,
                            'CADENCE_POSTGRES_SSLMODE': postgres_sslmode, 'CADENCE_POSTGRES_CA_FILE': postgres_ca,
                            'CADENCE_POSTGRES_CERT_FILE': postgres_cert, 'CADENCE_POSTGRES_KEY_FILE': postgres_key})
                    elif args.mysql:
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
                if udp_capture:
                    udp_stop.set()
                    udp_thread.join(timeout=2)
                    udp_capture.close()
                    (work / f'{args.matching_exporter}-packets.json').write_text(
                        ''.join(json.dumps({'base64': base64.b64encode(packet).decode('ascii'),
                                            'capturedAtUnixNs': captured_at}) + '\n'
                                for packet, captured_at in udp_packets))
                elif args.matching_exporter == 'prometheus':
                    udp_stop.set()
                    prometheus_thread.join(timeout=2)
                    prometheus_capture.close()
                    if prometheus_scrape_count[0]:
                        (work / 'prometheus-scrape.txt').write_text(prometheus_last_scrape[0])
                    else:
                        (work / 'prometheus-scrape-error.txt').write_text(
                            'no successful scrape with metric samples during the SDK probe\n')
            results['probeExitCode'] = result.returncode
            results['goServicesPid'] = server.pid
            results['goServicesAliveAfterProbe'] = server.poll() is None
            results['matchingExporter'] = args.matching_exporter
            results['matchingExporterAddress'] = exporter_address if production_matching else None
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
            if args.matching_exporter:
                probe_result = None
                for line in reversed((work / 'probe.log').read_text().splitlines()):
                    try:
                        candidate = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(candidate, dict) and isinstance(candidate.get('domain'), str):
                        probe_result = candidate
                        break
                if probe_result is None:
                    raise RuntimeError('SDK probe did not report its registered domain for exporter label validation')
                semantics = validate_matching_exporter(
                    args.matching_exporter, work, root, probe_result['domain'])
                results['matchingExporterSemantics'] = semantics
                (work / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
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
