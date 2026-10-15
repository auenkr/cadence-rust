#!/usr/bin/env python3
"""Audit public and internal RPC adapters against upstream IDLs. Not an engine parity claim."""
from pathlib import Path
import re
ROOT=Path(__file__).resolve().parents[2]
source='\n'.join(p.read_text() for p in (ROOT/'rust/crates/cadence-server/src').glob('*.rs'))
def snake(value):
 value=re.sub(r'([A-Z]+)([A-Z][a-z])',r'\1_\2',value)
 return re.sub(r'([a-z0-9])([A-Z])',r'\1_\2',value).lower()
implementations={name:set(re.findall(r'async fn (\w+)',body)) for name,body in re.findall(r'impl p::(\w+)_server::\w+ for Frontend\s*\{(.*?)^\}',source,re.M|re.S)}
lines=['# Migration inventory','', 'This audit counts concrete public gRPC adapters, not complete Go behavior.', 'An adapter can still depend on incomplete engine behavior. Generated default', 'UNIMPLEMENTED stubs do not count as implemented.','']
total=implemented=0
for path in sorted((ROOT/'go/idls/proto/uber/cadence/api/v1').glob('service_*.proto')):
 proto=path.read_text(); service=re.search(r'service (\w+)',proto)[1]
 methods=re.findall(r'\brpc (\w+)\s*\(',proto)
 supported=[m for m in methods if snake(m) in implementations.get(snake(service),set())]
 missing=[m for m in methods if m not in supported]
 total+=len(methods);implemented+=len(supported)
 lines+= [f'## {service}', '',f'Adapters present: {len(supported)} / {len(methods)}.','', 'Present: '+(', '.join(supported) or 'none')+'.','', 'Unported: '+(', '.join(missing) or 'none')+'.','']
lines[2:2]=[f'Public gRPC adapters: {implemented} / {total}.','']
# Internal service bindings and adapters are audited separately from frontend adapters.
aliases={'a':'admin','admin':'admin','h':'history','history':'history','m':'matching','matching':'matching','sd':'sharddistributor','sharddistributor':'sharddistributor'}
internal_impl={}
for alias, service_mod, body in re.findall(r'impl (\w+)::(\w+)_server::\w+ for \w+\s*\{(.*?)^\}',source,re.M|re.S):
 package=aliases.get(alias)
 if package: internal_impl[(package,service_mod)]=set(re.findall(r'async fn (\w+)',body))
paths=[ROOT/'go/idls/proto/uber/cadence/admin/v1/service.proto',ROOT/'go/proto/internal/uber/cadence/history/v1/service.proto',ROOT/'go/proto/internal/uber/cadence/matching/v1/service.proto']
paths+=sorted((ROOT/'rust/proto/shard-manager').rglob('*.proto'))
internal_total=internal_present=0
for path in paths:
 proto=path.read_text()
 package=re.search(r'package uber\.cadence\.(\w+)\.v1;',proto)[1]
 for service,body in re.findall(r'service (\w+)\s*\{(.*?)^\}',proto,re.M|re.S):
  methods=re.findall(r'\brpc (\w+)\s*\(',body)
  present=[m for m in methods if snake(m) in internal_impl.get((package,snake(service)),set())]
  missing=[m for m in methods if m not in present]
  internal_total+=len(methods);internal_present+=len(present)
  lines += [f'## {service} (internal/administrative)', '', f'Adapters present: {len(present)} / {len(methods)}.', '', 'Definitions: '+str(path.relative_to(ROOT))+'.', '', 'Present: '+(', '.join(present) or 'none')+'.','', 'Unported: '+(', '.join(missing) or 'none')+'.','']
lines[4:4]=[f'Internal/administrative gRPC adapters: {internal_present} / {internal_total}.','']
lines+=['## Other required work','',
'- Internal history and matching gRPC APIs, admin APIs, shard distributor, service discovery and membership, ownership/failover, worker service internals.',
'- Production SQL and NoSQL persistence backends, Go schema compatibility, transactions, visibility backends, indexing queues, and crash durability. Current JSON snapshots are development persistence.',
'- Buffered history events and their event-ID relationships, closing decisions with unhandled events, task and workflow timeout engines, complete workflow retry/cron behavior, reset/restart, sticky task execution, local activity dispatch, and closed-workflow queries.',
'- Domain UUID isolation after deletion/recreation, complete global/active-active domain validation and failover, XDC and replication, archival, batch workflows, schedules, semaphores, scanners and repair/diagnostics.',
'- Full dynamic configuration, authentication/authorization, rate limiting, metrics and tracing, operational tooling, lifecycle and health semantics. Current service registry labels do not establish separate history/matching/worker service implementations.',
'- Complete unknown-field/enum compatibility and comprehensive Go/Java SDK coverage, including caching, replay and recovery. Core SDK workflows passing do not establish complete Cadence compatibility.',
'', '## Completion criteria','',
'Every Go capability must have a Rust implementation and evidence of equivalent', 'behavior. All public and internal RPC methods and transports, persistence schemas', 'and backends, workflow histories, failure/recovery paths, SDK replay and lifecycle', 'behavior must be covered. The running server must require no Go Cadence process.',
'Until those checks pass, this rewrite remains incomplete.','']
(ROOT/'memory/migration-inventory.md').write_text('\n'.join(lines))
print(f'{implemented}/{total} public gRPC adapters; inventory written')
