#!/usr/bin/env python3
"""Retain compact evidence from a downloaded Matching Actions artifact.

Exit 0 means a successful recorded gate was collected, 1 means a failed gate
was collected, and 2 means required evidence is missing or inconsistent.
Collection never constitutes Matching service acceptance.
"""

import argparse
import json
from pathlib import Path
import re
import shutil
import sys

from matching_evidence import sha256_file


def collect(args):
    source = args.artifact.resolve()
    destination = args.output.resolve()
    if destination == source or source in destination.parents:
        raise ValueError('output must be outside the downloaded artifact')
    if destination.exists() and any(destination.iterdir()):
        raise ValueError('output directory must be empty')
    head = (source / 'commit.txt').read_text().strip()
    if not re.fullmatch(r'[0-9a-f]{40}', args.expected_sha) or head != args.expected_sha:
        raise ValueError(f'source mismatch: expected {args.expected_sha}, recorded {head}')
    binary = (source / 'binary.sha256').read_text().strip()
    if not re.fullmatch(r'[0-9a-f]{64}\s+\*?\S+', binary):
        raise ValueError('missing or malformed Rust binary SHA256 record')
    build = next((source / name for name in ('build.exit', 'cargo-build.exit')
                  if (source / name).is_file()), None)
    if build is None:
        raise ValueError('missing recorded build exit')
    build_exit = int(build.read_text().strip())
    gate_exit = int((source / 'gate.exit').read_text().strip())
    runtime_path = source / 'gate/run/results.json'
    runtime = json.loads(runtime_path.read_text())
    probe_path = source / 'gate/run/probe.log'
    probe = probe_path.read_text()
    scenarios = []
    for line in probe.splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict) and 'result' in row:
            scenarios.append(row)
    # Keep this as an evidence inventory: requested command-line flags are not
    # results, and log-file presence alone does not prove an SDK scenario passed.
    work = source / 'gate/run/rust-matching'
    sdk_names = ('go-sdk.log', 'restart-sdk.log', 'graceful-drain-sdk.log',
                 'adaptive-scaler-sdk.log', 'adaptive-scaler-retry-only-sdk.log',
                 'production-idle-recovery-sdk.log', 'production-crash-recovery-sdk.log',
                 'gc-backlog-sdk.log')
    sdk_evidence = {name: {'status': 'present' if (work / name).is_file() else 'missing'}
                    for name in sdk_names}
    cleanup = {}
    for path in sorted((source / 'gate').glob('*container-cleanup.txt')):
        content = path.read_text(errors='replace').strip()
        cleanup[path.name] = {
            'status': 'present' if content else 'empty',
            'recordedOutput': content or None,
            'sha256': sha256_file(path),
        }
    recovery_names = ('matching-shutdown.json', 'production-failure-recovery-checkpoints.json',
                      'db-observations.json')
    recovery_evidence = {}
    for name in recovery_names:
        path = work / name
        if not path.is_file():
            recovery_evidence[name] = {'status': 'missing'}
            continue
        try:
            recovery_evidence[name] = {'status': 'present', 'recorded': json.loads(path.read_text())}
        except (ValueError, OSError):
            recovery_evidence[name] = {'status': 'present-unparsed', 'sha256': sha256_file(path)}
    if gate_exit == 0:
        if build_exit != 0 or runtime.get('runExitCode') != 0 or runtime.get('probeExitCode') != 0:
            raise ValueError('successful gate contradicts recorded build/runtime/probe exits')
        if runtime.get('goMatchingStarted') is not False or runtime.get('rustServices') != ['matching']:
            raise ValueError('successful gate lacks Rust-only Matching topology evidence')
        if set(runtime.get('goServices', [])) != {'frontend', 'history', 'worker'}:
            raise ValueError('successful gate lacks original Go peer service topology')
        if not scenarios:
            raise ValueError('successful gate has no actual probe scenario result')
    selected = [source / 'commit.txt', source / 'binary.sha256', build,
                source / 'gate.exit', runtime_path, probe_path]
    for relative in ('command.txt', 'gate/run/run.exit'):
        if (source / relative).is_file():
            selected.append(source / relative)
    selected.extend(sorted((source / 'gate').glob('*container-cleanup.txt')))
    for name in ('adaptive-scaler-sdk.log', 'adaptive-scaler-retry-only-sdk.log',
                 'go-sdk.log', 'restart-sdk.log', 'graceful-drain-sdk.log',
                 'production-idle-recovery-sdk.log', 'production-crash-recovery-sdk.log',
                 'production-failure-recovery-checkpoints.json',
                 'gc-backlog-sdk.log', 'db-observations.json', 'matching-shutdown.json'):
        path = source / 'gate/run/rust-matching' / name
        if path.is_file():
            selected.append(path)
    # Keep large logs in the downloaded artifact; retain their content hashes.
    hashes = {str(path.relative_to(source)): sha256_file(path) for path in selected}
    destination.mkdir(parents=True, exist_ok=True)
    retained = []
    for path in selected:
        if path.stat().st_size > 1024 * 1024:
            continue
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        retained.append(str(relative))
    report = {'runUrl': args.run_url, 'lane': args.lane, 'headSha': head,
              'binarySha256': binary.split()[0], 'buildExit': build_exit,
              'gateExit': gate_exit, 'runtimeResults': runtime,
              'observedProbeResults': scenarios,
              'sdkEvidenceFiles': sdk_evidence,
              'recoveryEvidence': recovery_evidence,
              'databaseCleanup': cleanup,
              'retainedFiles': retained,
              'originalFileSha256': hashes, 'serviceAcceptance': False,
              'reviewRequired': 'Inspect actual lane-specific result rows, SDK/recovery evidence, cleanup output, and source parity ledger. Missing files and requested flags are not execution evidence.'}
    (destination / 'collection.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'headSha': head, 'gateExit': gate_exit, 'output': str(destination),
                      'serviceAcceptance': False}))
    return int(build_exit != 0 or gate_exit != 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('artifact', type=Path)
    parser.add_argument('--expected-sha', required=True)
    parser.add_argument('--run-url', required=True)
    parser.add_argument('--lane', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    try:
        return collect(args)
    except (OSError, ValueError, TypeError) as error:
        print(f'evidence collection failed: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
