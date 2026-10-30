#!/usr/bin/env python3
"""Evidence helpers for the Go/Rust mixed-service baseline harness.

The production MySQL gate produces a large artifacts directory that must not
be committed. These helpers copy the selected evidence into a durable
directory: the probe log, the result documents, the exact command, hashes of
the binaries and configurations, the sanitized configurations, the build and
lint outputs and the compact database observations.

Databases, binaries, keys and password configurations are never copied.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess


def sha256_file(path):
    """Return the SHA256 of one file, or None when it does not exist."""
    path = Path(path)
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


# The observation is bounded so a stalled database or container cannot hang
# the gate.
MYSQL_OBSERVATION_TIMEOUT_SECONDS = 30


def mysql_rows(sql, *, container=None, password='cadence', database='cadence',
               host='127.0.0.1', port='3306', user='root'):
    """Run one read-only MySQL statement and return its rows.

    The mixed production gate uses the same MySQL 8 container the Go services
    and Rust Matching share, so the observation runs inside that container and
    never needs a host client.
    """
    if container:
        command = ['docker', 'exec', container, 'mysql', '--batch', '--skip-column-names',
                   '-uroot', f'-p{password}', database, '-e', sql]
    else:
        command = ['mysql', '--batch', '--skip-column-names', '--host', host,
                   '--port', str(port), '--user', user, f'--password={password}',
                   database, '-e', sql]
    result = subprocess.run(command, capture_output=True, text=True, timeout=MYSQL_OBSERVATION_TIMEOUT_SECONDS)
    if result.returncode:
        raise RuntimeError(f'MySQL observation failed: {result.stderr.strip() or sql}')
    return [line.split('\t') for line in result.stdout.splitlines() if line.strip()]


# Go's MySQL plugin creates these two admin tables while it applies the
# schema. They belong to Go's own setup, not to Rust Matching.
GO_SETUP_TABLES = ('schema_update_history', 'schema_version')


def go_schema_tables(schema_path):
    """Return the table names of Go's real MySQL schema."""
    schema = Path(schema_path).read_text()
    return sorted({name.lower() for name in re.findall(r'(?im)^CREATE TABLE `?(\w+)`?\s*\(', schema)})


def check_go_schema_only(schema_path, **connection):
    """Confirm the shared database holds Go's schema and no private tables.

    Rust Matching must reuse Go's schema instead of migrating its own, so
    every table in the shared database has to exist in Go's schema file or in
    the admin tables Go's own setup creates.
    """
    present = sorted(row[0].lower() for row in mysql_rows('SHOW TABLES', **connection))
    expected = go_schema_tables(schema_path)
    allowed = sorted(set(expected) | set(GO_SETUP_TABLES))
    unexpected = [table for table in present if table not in allowed]
    missing = [table for table in expected if table not in present]
    if unexpected:
        raise RuntimeError(f"the shared MySQL database holds tables Go's schema does not define: {unexpected}")
    return {
        'presentTables': present,
        'goSchemaTables': expected,
        'goSetupTables': sorted(table for table in present if table in GO_SETUP_TABLES),
        'missingGoTables': missing,
    }


def sanitize_config(text):
    """Remove credentials from a generated configuration file."""
    return re.sub(r'(?im)^(\s*(?:password|cert|key|caFile|serverName)\s*:\s*).*$', r'\1"***"', text)


def git_state(repository):
    """Return the source revision the run used."""
    def git(*arguments):
        return subprocess.run(['git', '-C', str(repository), *arguments],
                              capture_output=True, text=True).stdout.strip()
    return {
        'repository': str(repository),
        'head': git('rev-parse', 'HEAD'),
        'branch': git('rev-parse', '--abbrev-ref', 'HEAD'),
        'status': [line for line in git('status', '--porcelain').splitlines() if line],
    }


def copy_evidence(destination, *, work, config_files, repository, binaries, results):
    """Copy the selected run evidence into a durable directory.

    Only the probe log, the result documents, the command, hashes of the
    binaries and configurations, the sanitized configurations, the build and
    lint outputs and the compact database observations are copied. Databases,
    binaries, keys and password configurations are never copied.
    """
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    work = Path(work)
    copied = []
    for name in ('probe.log', 'results.json', 'run.exit', 'command.txt', 'db-observations.json',
                 'go-sdk.log', 'restart-sdk.log', 'graceful-drain-sdk.log', 'server.log',
                 'rust-build.log', 'build.log', 'prometheus-scrape.txt',
                 'prometheus-scrape-samples.jsonl.gz',
                 'prometheus-scrape-error.txt', 'statsd-packets.json', 'm3-packets.json',
                 'm3-decoder-go.log', 'm3-shutdown-decoder-go.log', 'm3-decoded.jsonl',
                 'matching-exporter-semantics.json', 'matching-tls-handshakes.json'):
        source = work / name
        if source.is_file():
            shutil.copy2(source, destination / name)
            copied.append(name)
    for pattern in ('*clippy*', '*lint*'):
        for source in sorted(work.glob(pattern)):
            if source.is_file():
                shutil.copy2(source, destination / source.name)
                copied.append(source.name)
    # The probe writes its database observations and its service logs into its
    # own working directory below the artifacts directory.
    probe_dir = work / 'rust-matching'
    if probe_dir.is_dir():
        for name in ('db-observations.json', 'gc-backlog-sdk.log', 'go-sdk.log',
                     'graceful-drain-sdk.log', 'restart-sdk.log', 'matching-shutdown.json',
                     'matching-tls-handshakes.json'):
            source = probe_dir / name
            if source.is_file():
                shutil.copy2(source, destination / name)
                copied.append(name)
        server_logs = destination / 'rust-server-logs'
        for source in sorted(probe_dir.glob('rust-server-*.log')):
            server_logs.mkdir(exist_ok=True)
            shutil.copy2(source, server_logs / source.name)
            copied.append(f'rust-server-logs/{source.name}')
        for source in sorted(probe_dir.glob('rust-process-evidence-*.json')):
            shutil.copy2(source, destination / source.name)
            copied.append(source.name)
    config_dir = destination / 'config-redacted'
    config_dir.mkdir(exist_ok=True)
    for source in config_files:
        source = Path(source)
        if source.is_file():
            (config_dir / source.name).write_text(sanitize_config(source.read_text()))
            copied.append(f'config-redacted/{source.name}')
    (destination / 'source.json').write_text(json.dumps(git_state(repository), indent=2) + '\n')
    copied.append('source.json')
    (destination / 'binaries.json').write_text(json.dumps({
        name: {'path': str(path), 'sha256': sha256_file(path)} for name, path in binaries.items()
    }, indent=2) + '\n')
    copied.append('binaries.json')
    (destination / 'config-hashes.json').write_text(json.dumps({
        str(path): sha256_file(path) for path in config_files
    }, indent=2) + '\n')
    copied.append('config-hashes.json')
    (destination / 'results-copy.json').write_text(json.dumps(results, indent=2) + '\n')
    copied.append('results-copy.json')
    (destination / 'EVIDENCE.md').write_text(
        '# Matching production mixed-run evidence\n\n'
        f'- artifacts directory: `{work}`\n'
        f'- evidence directory: `{destination}`\n'
        f'- probe exit code: `{results.get("probeExitCode")}`\n'
        f'- result: `{results.get("result")}`\n'
        f'- task garbage collection: `{json.dumps(results.get("taskGarbageCollection"))}`\n'
        f'- domain metadata: `{json.dumps(results.get("domainMetadata"))}`\n'
        f'- Go schema only: `{json.dumps(results.get("goSchemaOnly"))}`\n')
    copied.append('EVIDENCE.md')
    return copied
