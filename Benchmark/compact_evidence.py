"""Export summaries/provenance to Git; leave raw samples and logs in CI artifacts."""
import copy
import json
from pathlib import Path


def compact_report(report):
    result = copy.deepcopy(report)
    for run in result.get('runs', {}).values():
        run.pop('client_outstanding_timeline', None)
        resources = run.get('resources', {})
        resources.pop('timeline', None)
        resources.pop('mysql_samples', None)
    result['retention_note'] = 'Raw samples, time series and logs remain in temporary CI artifacts; this summary cannot regenerate percentiles.'
    return result


def export_compact(source, destination):
    source, destination = Path(source), Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    provenance = {'exit_codes': {}, 'binary_hashes': {}, 'commands': {}, 'configurations': {}}
    for path in sorted(source.rglob('*')):
        if not path.is_file():
            continue
        relative = str(path.relative_to(source))
        name = path.name
        if name == 'report.json':
            report = compact_report(json.loads(path.read_text()))
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(report, indent=2) + '\n')
        elif name in ('report.md', 'manifest.json', 'artifacts.json'):
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(path.read_text())
        elif path.suffix == '.exit':
            provenance['exit_codes'][relative] = path.read_text().strip()
        elif path.suffix == '.sha256':
            provenance['binary_hashes'][relative] = path.read_text().strip()
        elif name in ('commands.json', 'command.txt', 'rust-matching-command.json', 'commit.txt', 'source-sha.txt', 'head.txt'):
            provenance['commands'][relative] = path.read_text().strip()
        elif path.suffix in ('.yaml', '.yml') or ('-config-' in name or '-dynamic-' in name) and path.suffix == '.log':
            provenance['configurations'][relative] = path.read_text()
    (destination / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
