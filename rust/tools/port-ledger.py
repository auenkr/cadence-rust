#!/usr/bin/env python3
"""Create a full Go-source symbol ledger using Go's AST, with reviewed mappings.

Go is used for audit tooling only. No Rust build/runtime dependency is introduced.
Unknown files and mappings are deliberately not treated as completed ports.
"""
from pathlib import Path
from collections import Counter
import subprocess, json
ROOT=Path(__file__).resolve().parents[2]
raw=subprocess.check_output(['go','run','tests/port-audit/main.go','go'],cwd=ROOT)
files=json.loads(raw)
map_path=ROOT/'memory/port-map.json'
mappings=json.loads(map_path.read_text()) if map_path.exists() else {}
for file in files:
 mapping=mappings.get(file['goPath'],{})
 file['status']=mapping.get('status','review_required')
 file['rustSources']=mapping.get('rustSources',[])
 file['evidence']=mapping.get('evidence',[])
ledger={'scope':'All Go source files and top-level AST symbols, including generated bindings and tests. File mappings do not establish semantic parity.', 'counts':{'filesByKind':dict(Counter(f['kind'] for f in files)),'symbolsByKind':dict(Counter(s['kind'] for f in files for s in f['symbols'])),'filesByStatus':dict(Counter(f['status'] for f in files))},'files':files}
(ROOT/'memory/port-ledger.json').write_text(json.dumps(ledger,separators=(',',':'))+'\n')
(ROOT/'memory/port-ledger-summary.md').write_text('# Go source port ledger\n\nThis ledger audits all Go source files and top-level AST symbols, including\ngenerated bindings and tests. Every file requires explicit review and evidence.\nPartial file mappings are not completion claims.\n\n```json\n'+json.dumps(ledger['counts'],indent=2)+'\n```\n\nMachine-readable detail: `port-ledger.json`. Reviewed mappings: `port-map.json`.\nRegenerate with `python3 rust/tools/port-ledger.py`. This uses Go only as an AST\naudit tool; Rust build and runtime do not require Go.\n')
print(json.dumps(ledger['counts']))
